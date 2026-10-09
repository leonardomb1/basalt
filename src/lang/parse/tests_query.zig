//! Parser tests for queries: aggregates, joins, set operations, table functions,
//! subqueries and how each lowers to pipeline stages.

const aggStageIndex = @import("testing_util.zig").aggStageIndex;
const ast = @import("../ast.zig");
const containsTrueLit = @import("testing_util.zig").containsTrueLit;
const Diagnostic = @import("../sql_parser.zig").Diagnostic;
const firstPipelineStages = @import("testing_util.zig").firstPipelineStages;
const parseSource = @import("../sql_parser.zig").parseSource;
const parseTest = @import("testing_util.zig").parseTest;
const std = @import("std");
const testing = std.testing;

test "sql: terminal select from csv becomes read+write stdout" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT id, amount FROM 'in.csv' WHERE status = 'paid' LIMIT 2;");
    try testing.expectEqual(@as(usize, 2), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqual(ast.Kind.batch, prog.stmts[0].kind.kind);
    const pl = prog.stmts[1].output;
    try testing.expectEqual(@as(usize, 5), pl.stages.len);
    try testing.expect(pl.stages[0].node == .read);
    try testing.expect(pl.stages[1].node == .filter);
    try testing.expect(pl.stages[2].node == .select);
    try testing.expect(pl.stages[3].node == .limit);
    try testing.expect(pl.stages[4].node == .write);
    try testing.expectEqualStrings("stdout", pl.stages[4].node.write.connector);
}

test "sql: an aggregate refuses a second argument instead of dropping it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT SUM(x, id) AS s FROM 'in.csv';", &diag));
    try testing.expectEqualStrings("`sum` takes one argument, not 2", diag.msg);
    try testing.expectEqual(@as(u32, 8), diag.col);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT round(stddev(x, 2), 2) AS s FROM 'in.csv';", &diag));
    try testing.expectEqualStrings("`stddev` takes one argument, not 2", diag.msg);
    _ = try parseSource(a, "SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS b, SUM(x) AS c FROM 'in.csv';", &diag);
}

test "sql: a scalar function before OVER says it is not a window function; every aggregate is one" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    inline for (.{ "upper", "coalesce", "abs" }) |f| {
        try testing.expectError(error.ParseFailed, parseSource(a, "SELECT g, " ++ f ++ "(x) OVER (PARTITION BY g) AS s FROM 'in.csv';", &diag));
        try testing.expect(std.mem.startsWith(u8, diag.msg, "`" ++ f ++ "` is not a window function"));
        try testing.expectEqual(@as(u32, 11), diag.col);
    }
    inline for (.{ "median", "stddev", "variance", "bool_and", "bit_or", "count_if", "first_value", "last_value" }) |f|
        _ = try parseSource(a, "SELECT g, " ++ f ++ "(x) OVER (PARTITION BY g) AS s FROM 'in.csv';", &diag);
    _ = try parseSource(a, "SELECT g, nth_value(x, 2) OVER (PARTITION BY g ORDER BY x) AS s, ntile(4) OVER (ORDER BY x) AS q, percent_rank() OVER (ORDER BY x) AS p, cume_dist() OVER (ORDER BY x) AS c FROM 'in.csv';", &diag);
    _ = try parseSource(a, "SELECT g, median(x) AS m FROM 'in.csv' GROUP BY g;", &diag);
    _ = try parseSource(a, "SELECT g, x, SUM(x) OVER (PARTITION BY g ORDER BY x) AS s FROM 'in.csv';", &diag);
}

test "sql: a table function lowers each call to a binding, its parameters bound to the arguments" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS
        \\  SELECT id, x FROM 'o.csv' WHERE x >= $minx;
        \\SELECT id FROM paid(8);
    , &diag);
    try testing.expect(prog.stmts[1].func.body == .table);
    const b = prog.stmts[2].binding;
    try testing.expect(std.mem.startsWith(u8, b.name, "__tvf") and std.mem.endsWith(u8, b.name, "_paid"));
    const f = b.pipeline.stages[1].node.filter;
    try testing.expectEqual(@as(i64, 8), f.binary.r.int_lit);
    try testing.expectEqualStrings(b.name, prog.stmts[3].output.stages[0].node.ref);
}

test "sql: two calls in one query get their own bindings, the body's CTEs included" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE FUNCTION byg(k STRING) RETURNS TABLE AS
        \\  WITH s AS (SELECT g, x FROM 'o.csv') SELECT x FROM s WHERE g = $k;
        \\SELECT * FROM byg('a') l JOIN byg('b') r ON l.x = r.x;
    , &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |st| if (st == .binding) try names.append(st.binding.name);
    try testing.expectEqual(@as(usize, 4), names.items.len);
    for (names.items, 0..) |n, i| {
        try testing.expect(!std.mem.eql(u8, n, "s"));
        for (names.items[0..i]) |m| try testing.expect(!std.mem.eql(u8, m, n));
    }
    var args = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |st| if (st == .binding and std.mem.endsWith(u8, st.binding.name, "_byg")) {
        const stages = st.binding.pipeline.stages;
        const own_s = try std.mem.concat(a, u8, &.{ st.binding.name[0 .. st.binding.name.len - "byg".len], "s" });
        try testing.expectEqualStrings(own_s, stages[0].node.ref);
        try args.append(stages[1].node.filter.binary.r.str_lit);
    };
    try testing.expectEqual(@as(usize, 2), args.items.len);
    try testing.expectEqualStrings("a", args.items[0]);
    try testing.expectEqualStrings("b", args.items[1]);
}

test "sql: a table function call is checked where it is written" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const decl = "CREATE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS SELECT id, x FROM 'o.csv' WHERE x >= $minx;\n";
    const cases = [_]struct { q: []const u8, msg: []const u8 }{
        .{ .q = "SELECT * FROM paid(1, 2);", .msg = "`paid` expects 0 to 1 argument(s), got 2" },
        .{ .q = "SELECT * FROM paid('x');", .msg = "`paid`: argument 1 (`minx`) expects INT, got STRING" },
        .{ .q = "SELECT * FROM paid(id);", .msg = "`paid`: argument 1 (`minx`) must be a constant — a literal, a `$param`, or an expression over them" },
        .{ .q = "SELECT * FROM nope(1);", .msg = "unknown table function `nope` — declare it with CREATE FUNCTION nope(...) RETURNS TABLE AS SELECT ..." },
        .{ .q = "CREATE OR REPLACE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS SELECT * FROM paid($minx);\nSELECT * FROM paid(1);", .msg = "in `paid` called at 3:15: table function `paid` nests more than 16 calls deep — does it reach itself?" },
    };
    for (cases) |c| {
        var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        try testing.expectError(error.ParseFailed, parseSource(a, try std.mem.concat(a, u8, &.{ decl, c.q }), &diag));
        try testing.expectEqualStrings(c.msg, diag.msg);
    }
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "CREATE FUNCTION bad() RETURNS TABLE AS SELECT id FROM 'o.csv' WHERE;", &diag));
    try testing.expectEqual(@as(u32, 1), diag.line);
    try testing.expectError(error.ParseFailed, parseSource(a, "CREATE FUNCTION bad() RETURNS TABLE AS 1 + 1;", &diag));
    try testing.expectEqualStrings("`bad`: a table function's body is a query — SELECT ... or WITH ...", diag.msg);
}

test "sql: JOIN LATERAL refuses a body the join could not apply per row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_]struct { body: []const u8, says: []const u8 }{
        .{ .body = "SELECT x FROM 't.csv' WHERE k = $p LIMIT 1", .says = "a LIMIT" },
        .{ .body = "SELECT DISTINCT x FROM 't.csv' WHERE k = $p", .says = "DISTINCT" },
        .{ .body = "SELECT k, COUNT(*) AS n FROM 't.csv' WHERE k = $p GROUP BY k", .says = "GROUP BY" },
        .{ .body = "SELECT x FROM 't.csv' WHERE k > $p", .says = "may only appear as `column = $p`" },
        .{ .body = "SELECT x FROM 't.csv' WHERE k = $p OR x = 1", .says = "may only appear as `column = $p`" },
        .{ .body = "SELECT x, upper($p) AS u FROM 't.csv' WHERE k = $p", .says = "SELECT item on its own" },
        .{ .body = "SELECT x FROM 't.csv'", .says = "never says `column = $p`" },
    };
    for (cases) |c| {
        var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const src = try std.fmt.allocPrint(a, "CREATE FUNCTION f(p) RETURNS TABLE AS {s}; SELECT o.id FROM 'o.csv' o CROSS JOIN LATERAL f(o.id) i;", .{c.body});
        try testing.expectError(error.ParseFailed, parseSource(a, src, &diag));
        if (std.mem.indexOf(u8, diag.msg, c.says) == null) {
            std.debug.print("for `{s}` got: {s}\n", .{ c.body, diag.msg });
            return error.TestUnexpectedResult;
        }
    }
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "CREATE FUNCTION f(p) RETURNS TABLE AS SELECT x FROM 't.csv' WHERE k = $p; SELECT o.id FROM 'o.csv' o CROSS JOIN f(o.id) i;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "JOIN LATERAL") != null);
}

test "sql: LOAD INTO with GROUP BY becomes aggregate" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\SELECT status, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total
        \\FROM 'in.csv'
        \\GROUP BY status;
    );
    const pl = prog.stmts[1].output;
    try testing.expectEqual(@as(usize, 3), pl.stages.len);
    const agg = pl.stages[1].node.aggregate;
    try testing.expectEqual(@as(usize, 2), agg.aggs.len);
    try testing.expectEqual(ast.AggFunc.count, agg.aggs[0].func);
    try testing.expect(agg.aggs[0].arg == null);
    try testing.expectEqual(ast.AggFunc.sum, agg.aggs[1].func);
    try testing.expectEqual(@as(usize, 1), agg.by.len);
    try testing.expectEqualStrings("csv", pl.stages[2].node.write.connector);
}

test "sql: CTE + LEFT JOIN with alias stripping" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH paid AS (
        \\  SELECT id, amount FROM 'in.csv' WHERE status = 'paid'
        \\)
        \\SELECT t.id, t.note, p.amount
        \\FROM 'in.csv' t
        \\LEFT JOIN paid p ON t.id = p.id;
    );
    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .binding);
    try testing.expectEqualStrings("paid", prog.stmts[1].binding.name);
    const pl = prog.stmts[2].output;
    try testing.expect(pl.stages[1].node == .join);
    const j = pl.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.left, j.kind);
    try testing.expectEqualStrings("paid", j.binding);
    try testing.expectEqual(@as(usize, 1), j.left_keys.len);
    try testing.expectEqualStrings("id", j.left_keys[0].parts[0]);
    try testing.expectEqualStrings("id", j.right_keys[0].parts[0]);
    const sel = pl.stages[2].node.select;
    try testing.expectEqualStrings("id", sel[0].field.parts[0]);
    try testing.expectEqual(@as(usize, 1), sel[0].field.parts.len);
}

test "sql: a name used for two tables in one FROM is refused where it repeats" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\SELECT r.b FROM (SELECT a FROM 'x.csv') r JOIN (SELECT b FROM 'y.csv') r ON r.a = r.b;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`r` names two tables") != null);
    try testing.expectEqual(@as(u32, 72), diag.col);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\WITH p AS (SELECT a FROM 'x.csv') SELECT a FROM 'x.csv' p JOIN p ON a = a;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`p` names two tables") != null);

    _ = try parseTest(a, "WITH p AS (SELECT a FROM 'x.csv') SELECT a FROM p JOIN p ON a = a;");
}

test "sql: a path or a connection's table on a JOIN's right side reads in a binding of its own, AS optional" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (host = 'h', database = 'd');
        \\SELECT * FROM 'x.xlsx' AS xl JOIN sr.db.t AS t ON t.id = xl.id JOIN 'y.csv' y WITH (delimiter = ';') ON y.k = xl.k;
    );
    const t_read = prog.stmts[2].binding;
    try testing.expectEqualStrings("__derived1_t", t_read.name);
    try testing.expectEqualStrings("sr", t_read.pipeline.stages[0].node.read.connector);
    const y_read = prog.stmts[3].binding;
    try testing.expectEqualStrings("delimiter", y_read.pipeline.stages[0].hints[0].key);
    const st = prog.stmts[4].output.stages;
    try testing.expectEqualStrings("csv", st[0].node.read.connector);
    try testing.expectEqualStrings("t", st[1].node.join.alias);
    try testing.expectEqualStrings("id", st[1].node.join.right_keys[0].parts[0]);
    try testing.expectEqualStrings("y", st[2].node.join.alias);
}

test "sql: ON beyond keys — the right side alone narrows it, the rest filters an inner join after and an outer join's pairs" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\SELECT * FROM 'a.csv' AS a JOIN 'b.csv' AS b ON 1 = 1 AND b.del <> '*' AND b.id = a.id AND a.v = 'x';
    );
    const narrowed = prog.stmts[2].binding;
    try testing.expectEqualStrings("__derived2_on", narrowed.name);
    try testing.expectEqualStrings("__derived1_b", narrowed.pipeline.stages[0].node.ref);
    const f = narrowed.pipeline.stages[1].node.filter;
    try testing.expectEqualStrings("del", f.binary.r.binary.l.field.parts[0]);
    const st = prog.stmts[3].output.stages;
    try testing.expectEqualStrings("__derived2_on", st[1].node.join.binding);
    try testing.expectEqualStrings("b", st[1].node.join.alias);
    try testing.expectEqual(@as(usize, 1), st[1].node.join.left_keys.len);
    try testing.expect(st[2].node == .filter);

    const lj = try parseTest(a, "SELECT * FROM 'a.csv' a LEFT JOIN 'b.csv' b ON b.id = a.id AND a.v = 'x';");
    const ljj = lj.stmts[2].output.stages[1].node.join;
    try testing.expectEqualStrings("v", ljj.residual.?.binary.l.field.parts[ljj.residual.?.binary.l.field.parts.len - 1]);
    try testing.expect(lj.stmts[2].output.stages[2].node != .filter);
    const rj = try parseTest(a, "SELECT * FROM 'a.csv' a RIGHT JOIN 'b.csv' b ON b.id = a.id AND b.del <> '*';");
    try testing.expect(rj.stmts[2].output.stages[1].node.join.residual != null);
    try testing.expectEqualStrings("__derived1_b", rj.stmts[2].output.stages[1].node.join.binding);

    const only_right = try parseTest(a, "SELECT * FROM 'a.csv' a JOIN 'b.csv' b ON b.del <> '*';");
    const orj = only_right.stmts[3].output.stages[1].node.join;
    try testing.expect(orj.keyless() and orj.residual == null);
    try testing.expectEqualStrings("b", orj.alias);
}

test "sql: an ON with no `=` key is the whole condition of a nested-loop join, of any kind" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const inner = try parseTest(a, "SELECT * FROM 'a.csv' a JOIN 'b.csv' b ON a.x < b.y AND a.v = 'x';");
    const ist = inner.stmts[2].output.stages;
    for (ist[2..]) |st| try testing.expect(st.node != .filter);
    const ij = ist[1].node.join;
    try testing.expect(ij.keyless());
    try testing.expectEqual(ast.BinOp.@"and", ij.residual.?.binary.op);
    try testing.expectEqual(ast.BinOp.lt, ij.residual.?.binary.l.binary.op);
    try testing.expectEqual(ast.BinOp.eq, ij.residual.?.binary.r.binary.op);

    const between = try parseTest(a, "SELECT * FROM 'a.csv' a LEFT JOIN 'b.csv' b ON a.d BETWEEN b.lo AND b.hi;");
    const bj = between.stmts[2].output.stages[1].node.join;
    try testing.expect(bj.keyless() and bj.kind == .left);
    try testing.expectEqual(ast.BinOp.ge, bj.residual.?.binary.l.binary.op);
    try testing.expectEqual(ast.BinOp.le, bj.residual.?.binary.r.binary.op);

    const ors = try parseTest(a, "SELECT * FROM 'a.csv' a FULL JOIN 'b.csv' b ON a.k = b.k OR a.alt = b.k;");
    const oj = ors.stmts[2].output.stages[1].node.join;
    try testing.expect(oj.keyless() and oj.kind == .full);
    try testing.expectEqual(ast.BinOp.@"or", oj.residual.?.binary.op);

    const right = try parseTest(a, "SELECT * FROM 'a.csv' a RIGHT JOIN 'b.csv' b ON a.x > b.y AND b.del <> '*';");
    const rj = right.stmts[2].output.stages[1].node.join;
    try testing.expect(rj.keyless() and rj.kind == .right);
    try testing.expectEqualStrings("__derived1_b", rj.binding);

    const keyed = try parseTest(a, "SELECT * FROM 'a.csv' a JOIN 'b.csv' b ON a.k = b.k AND a.x < b.y;");
    const kst = keyed.stmts[2].output.stages;
    try testing.expect(!kst[1].node.join.keyless());
    try testing.expect(kst[1].node.join.residual == null);
    try testing.expect(kst[2].node == .filter);

    const cross = try parseTest(a, "SELECT * FROM 'a.csv' a CROSS JOIN 'b.csv' b WHERE a.x < b.y;");
    const cst = cross.stmts[2].output.stages;
    try testing.expect(!cst[1].node.join.keyless());
    try testing.expect(cst[2].node == .filter);
}

test "sql: a computed key in ON — each side computes its own column, dropped after the join" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\SELECT * FROM 'a.csv' AS a JOIN 'b.csv' AS b ON trim(b.code) = cast(a.code AS string) AND b.del <> '*';
    );
    const rb = prog.stmts[2].binding.pipeline.stages;
    try testing.expect(rb[1].node == .filter);
    try testing.expect(rb[2].node.select[0] == .star);
    try testing.expectEqualStrings("code", rb[2].node.select[1].computed.expr.call.args[0].field.parts[0]);
    const st = prog.stmts[3].output.stages;
    try testing.expect(st[1].node.select[0] == .star);
    const lk = st[1].node.select[1].computed.name;
    const j = st[2].node.join;
    try testing.expectEqualStrings(lk, j.left_keys[0].parts[0]);
    try testing.expectEqualStrings(rb[2].node.select[1].computed.name, j.right_keys[0].parts[0]);
    try testing.expectEqual(@as(usize, 2), st[3].node.select[0].star_except.len);
}

test "sql: an `=` of bare columns in ON is left for the plan to place, its key columns dropped after the join" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\WITH base AS (SELECT * FROM 'c.csv')
        \\SELECT * FROM 's.xlsx' INNER JOIN base ON TRIM(cc_code) = CAST(cr AS varchar);
    );
    const st = prog.stmts[2].output.stages;
    const j = st[1].node.join;
    try testing.expectEqual(@as(usize, 0), j.left_keys.len);
    try testing.expectEqual(@as(usize, 1), j.deferred.len);
    try testing.expectEqualStrings("cc_code", j.deferred[0].a.call.args[0].field.parts[0]);
    const drop = st[2].node.select[0].star_except;
    try testing.expectEqualStrings(j.deferred[0].left_name, drop[0]);
    try testing.expectEqualStrings(j.deferred[0].right_name, drop[1]);
}

test "sql: USING keys on renamed right columns and lists the names first; NATURAL is refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a, "SELECT * FROM 'a.csv' a RIGHT JOIN 'b.csv' b USING (k, d);");
    const st = prog.stmts[3].output.stages;
    const j = st[1].node.join;
    try testing.expectEqualStrings("k", j.left_keys[0].parts[0]);
    try testing.expectEqualStrings("d", j.left_keys[1].parts[0]);
    try testing.expect(std.mem.startsWith(u8, j.right_keys[0].parts[0], "__uk"));
    try testing.expectEqualStrings("b", j.alias);
    const sel = st[2].node.select;
    try testing.expectEqualStrings("k", sel[0].computed.name);
    try testing.expectEqualStrings("coalesce", sel[0].computed.expr.call.name);
    try testing.expectEqual(@as(usize, 4), sel[2].star_except.len);

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM 'a.csv' a NATURAL JOIN 'b.csv' b;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "USING (a, b)") != null);
}

test "sql: multi-key ON, RIGHT/FULL/CROSS join kinds, and the plain-column rule" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id, day, v FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t FULL OUTER JOIN r ON t.id = r.id AND r.day = t.day;
    );
    const j = prog.stmts[2].output.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.full, j.kind);
    try testing.expectEqual(@as(usize, 2), j.left_keys.len);
    try testing.expectEqualStrings("id", j.left_keys[0].parts[0]);
    try testing.expectEqualStrings("id", j.right_keys[0].parts[0]);
    try testing.expectEqualStrings("day", j.left_keys[1].parts[0]);
    try testing.expectEqualStrings("day", j.right_keys[1].parts[0]);

    const rp = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t RIGHT JOIN r ON t.id = r.id;
    );
    try testing.expectEqual(ast.JoinKind.right, rp.stmts[2].output.stages[1].node.join.kind);

    const cp = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t CROSS JOIN r;
    );
    const cj = cp.stmts[2].output.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.cross, cj.kind);
    try testing.expectEqual(@as(usize, 0), cj.left_keys.len);

    const up = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM 'in.csv' CROSS JOIN UNNEST(tags) AS tag;
    );
    try testing.expect(up.stmts[1].output.stages[1].node == .explode);

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const ck = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t JOIN r ON lower(t.id) = r.id;
    );
    try testing.expectEqualStrings("id", ck.stmts[2].output.stages[2].node.join.right_keys[0].parts[0]);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t CROSS JOIN r ON t.id = r.id;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no ON or USING clause") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t JOIN r;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "expected `ON") != null);
}

test "sql: plain UNION [ALL] lines up by position; INTERSECT binds tighter than UNION and EXCEPT" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const all = try parseTest(a, "SELECT k FROM 'x.csv' UNION ALL SELECT k FROM 'y.csv';");
    const ua = all.stmts[all.stmts.len - 1].output.stages;
    try testing.expect(ua[0].node.union_.positional);
    try testing.expect(ua[0].node.union_.branches[0].pipeline != null);
    try testing.expect(ua[1].node != .distinct);

    const dis = try parseTest(a, "SELECT k FROM 'x.csv' UNION SELECT k FROM 'y.csv';");
    const ud = dis.stmts[dis.stmts.len - 1].output.stages;
    try testing.expect(ud[1].node == .distinct);

    const mixed = try parseTest(a, "SELECT k FROM 'x.csv' UNION SELECT k FROM 'y.csv' UNION ALL SELECT k FROM 'z.csv';");
    const um = mixed.stmts[mixed.stmts.len - 1].output.stages;
    try testing.expectEqual(@as(usize, 2), um[0].node.union_.branches.len);
    try testing.expect(um[1].node != .distinct);

    const prec = try parseTest(a, "SELECT k FROM 'x.csv' UNION SELECT k FROM 'y.csv' INTERSECT SELECT k FROM 'z.csv';");
    const up = prec.stmts[prec.stmts.len - 1].output.stages;
    try testing.expectEqual(ast.SetOp.union_all, up[0].node.union_.set);
    try testing.expect(up[1].node == .distinct);
    const inner = up[0].node.union_.branches[1].pipeline.?.stages[0].node.ref;
    const right = for (prec.stmts) |st| {
        if (st == .binding and std.mem.eql(u8, st.binding.name, inner)) break st.binding.pipeline.stages;
    } else return error.TestUnexpectedResult;
    try testing.expectEqual(ast.SetOp.intersect, right[0].node.union_.set);

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' EXCEPT ALL SELECT k FROM 'y.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`EXCEPT ALL` is not supported yet") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' INTERSECT BY NAME SELECT k FROM 'y.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`BY NAME` applies to a chain of UNIONs only") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' UNION ALL BY NAME SELECT k FROM 'y.csv' UNION ALL SELECT k FROM 'z.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "all `BY NAME` or all by position") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' UNION ALL SELECT k FROM 'y.csv' ANCHOR SCHEMA first;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`ANCHOR` applies to `UNION ALL BY NAME`") != null);
}

test "sql: UNION ALL BY NAME with tag literal and anchor" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\LOAD INTO sr.CT2_UNIFIED USING stream_load
        \\  UPSERT ON (CT2_EMPRESA, R_E_C_N_O_)
        \\  SPLIT BY (R_E_C_N_O_)
        \\AS
        \\SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
        \\UNION ALL BY NAME
        \\SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
        \\ANCHOR SCHEMA erp.dbo.CT2010;
    );
    const pl = prog.stmts[3].output;
    try testing.expectEqual(@as(usize, 2), pl.stages.len);
    const u = pl.stages[0].node.union_;
    try testing.expectEqual(@as(usize, 2), u.branches.len);
    try testing.expectEqualStrings("01", u.branches[0].tag.?);
    try testing.expectEqualStrings("02", u.branches[1].tag.?);
    try testing.expectEqualStrings("tag", pl.stages[0].hints[0].key);
    try testing.expectEqualStrings("CT2_EMPRESA", pl.stages[0].hints[0].value.ident);
    try testing.expectEqualStrings("canon", pl.stages[0].hints[1].key);
    try testing.expectEqualStrings("CT2010", pl.stages[0].hints[1].value.ident);
    const w = pl.stages[1].node.write;
    try testing.expectEqual(@as(usize, 2), w.mode.upsert.keys.len);
    try testing.expectEqualStrings("split", pl.stages[1].hints[0].key);
}

test "sql: a computed GROUP BY key keeps the columns a CASE inside an aggregate reads" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const prog = try parseTest(ar.allocator(),
        \\SELECT CAST(DATE_TRUNC('month', d) AS DATE) AS m,
        \\       SUM(CASE WHEN status = 'x' THEN 1 ELSE 0 END) AS c
        \\FROM 't.csv' GROUP BY CAST(DATE_TRUNC('month', d) AS DATE);
    );
    const stages = prog.stmts[1].output.stages;
    try std.testing.expect(stages[1].node == .select);
    var has_status = false;
    for (stages[1].node.select) |it| {
        if (it == .field and std.mem.eql(u8, it.field.last(), "status")) has_status = true;
    }
    try std.testing.expect(has_status);
}

test "sql: an aggregate inside a scalar expression lifts to the aggregate stage" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT g, ROUND(AVG(v), 2) AS m FROM 'x.csv' GROUP BY g;");
    const st = firstPipelineStages(prog);

    const ai = aggStageIndex(st);
    const agg = st[ai].node.aggregate;
    try testing.expectEqual(@as(usize, 1), agg.aggs.len);
    try testing.expectEqualStrings("avg(v)", agg.aggs[0].name);
    try testing.expectEqual(ast.AggFunc.avg, agg.aggs[0].func);

    const sel = st[ai + 1].node.select;
    try testing.expectEqual(@as(usize, 2), sel.len);
    try testing.expectEqualStrings("g", sel[0].field.last());
    try testing.expectEqualStrings("m", sel[1].computed.name);
    const arg0 = sel[1].computed.expr.call.args[0];
    try testing.expectEqualStrings("avg(v)", arg0.field.last());
}

test "sql: one aggregate mentioned twice becomes a single output column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT SUM(v) AS s, SUM(v) * 2 AS d FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    const agg = st[aggStageIndex(st)].node.aggregate;
    try testing.expectEqual(@as(usize, 1), agg.aggs.len);
    try testing.expectEqualStrings("s", agg.aggs[0].name);
}

test "sql: HAVING may name an aggregate the SELECT list omits" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT g, AVG(v) AS m FROM 'x.csv' GROUP BY g HAVING COUNT(*) > 1;");
    const st = firstPipelineStages(prog);

    const ai = aggStageIndex(st);
    const agg = st[ai].node.aggregate;
    try testing.expectEqual(@as(usize, 2), agg.aggs.len);
    try testing.expectEqualStrings("count(*)", agg.aggs[1].name);

    const sel = st[ai + 2].node.select;
    try testing.expectEqual(@as(usize, 2), sel.len);
    try testing.expectEqualStrings("g", sel[0].field.last());
    try testing.expectEqualStrings("m", sel[1].field.last());
}

test "sql: a grouping key may be aliased in the SELECT list" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT g AS grp, COUNT(*) AS n FROM 'x.csv' GROUP BY g;");
    const st = firstPipelineStages(prog);
    const ai = aggStageIndex(st);
    try testing.expectEqualSlices(u8, "g", st[ai].node.aggregate.by[0].last());

    const sel = st[ai + 1].node.select;
    try testing.expectEqualStrings("grp", sel[0].computed.name);
    try testing.expectEqualStrings("g", sel[0].computed.expr.field.last());
    try testing.expectEqualStrings("n", sel[1].field.last());
}

test "sql: an ungrouped column is refused, not silently dropped" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "SELECT g, v, COUNT(*) AS n FROM 'x.csv' GROUP BY g;", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`v`") != null);
}

test "sql: IN (SELECT ...) lifts to a semi join stage beside the filter" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const prog = try parseSource(a, "SELECT k FROM 'f.csv' WHERE v < 10 AND k IN (SELECT k FROM 'd.csv') AND v > 2;", &diag);
    try testing.expect(prog.stmts[1] == .binding);
    const bname = prog.stmts[1].binding.name;
    const stages = prog.stmts[2].output.stages;
    try testing.expect(stages[1].node == .filter);
    try testing.expect(!containsTrueLit(stages[1].node.filter));
    try testing.expect(stages[2].node == .join);
    try testing.expectEqual(ast.JoinKind.semi, stages[2].node.join.kind);
    try testing.expectEqualStrings(bname, stages[2].node.join.binding);
    try testing.expectEqualStrings("k", stages[2].node.join.left_keys[0].parts[0]);
    try testing.expectEqualStrings("k", stages[2].node.join.right_keys[0].parts[0]);

    const prog2 = try parseSource(a, "SELECT k FROM 'f.csv' WHERE k NOT IN (SELECT k FROM 'd.csv');", &diag);
    const st2 = prog2.stmts[2].output.stages;
    try testing.expect(st2[1].node == .join);
    try testing.expectEqual(ast.JoinKind.anti, st2[1].node.join.kind);
}

test "sql: IN (SELECT ...) under OR / outside WHERE / multi-column are parse errors" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'f.csv' WHERE v = 1 OR k IN (SELECT k FROM 'd.csv');", &diag));
    try testing.expectEqualStrings("IN (SELECT ...) is only supported as a top-level AND condition of WHERE — not under OR, NOT or CASE", diag.msg);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT (k IN (SELECT k FROM 'd.csv')) AS f FROM 'f.csv';", &diag));
    try testing.expectEqualStrings("IN (SELECT ...) is only supported in a WHERE clause", diag.msg);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'f.csv' WHERE k IN (SELECT k, v FROM 'd.csv');", &diag));
    try testing.expectEqualStrings("the subquery of IN must produce exactly one named column", diag.msg);
    const prog = try parseSource(a, "SELECT k FROM 'f.csv' WHERE k IN (1, 2, 3);", &diag);
    try testing.expect(prog.stmts[1].output.stages[1].node == .filter);
}

test "sql: scalar subquery desugars to a query LET ahead of the statement" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const prog = try parseSource(a, "SELECT k FROM 'f.csv' WHERE v > (SELECT max(v) FROM 'f.csv');", &diag);
    try testing.expect(prog.stmts[1] == .let_const);
    const l = prog.stmts[1].let_const;
    try testing.expect(l.expr == null);
    try testing.expect(l.query != null);
    const filt = prog.stmts[2].output.stages[1].node.filter;
    try testing.expectEqualStrings(l.name, filt.binary.r.field.parts[0]);

    const prog2 = try parseSource(a, "LET hi = (SELECT max(v) FROM 'f.csv');\nSELECT k FROM 'f.csv' WHERE v > $hi;", &diag);
    try testing.expect(prog2.stmts[1] == .let_const);
    try testing.expect(prog2.stmts[1].let_const.query != null);
    try testing.expectEqualStrings("hi", prog2.stmts[1].let_const.name);
}

test "sql: a select item's alias may omit AS, but not swallow the clause after it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT category, SUM(value) total, MAX(value) \"top\" FROM 'in.csv' GROUP BY category;");
    try testing.expect(prog.stmts.len == 2);
    const agg = prog.stmts[1].output.stages[1].node.aggregate;
    try testing.expectEqual(@as(usize, 2), agg.aggs.len);
    try testing.expectEqualStrings("total", agg.aggs[0].name);
    try testing.expectEqualStrings("top", agg.aggs[1].name);
    try testing.expectEqualStrings("category", agg.by[0].last());

    const clauses = try parseTest(a, "SELECT id FROM 'in.csv' WHERE id > 1 ORDER BY id LIMIT 2;");
    const st = clauses.stmts[1].output.stages;
    try testing.expect(st[1].node == .filter);
    try testing.expectEqual(@as(usize, 1), st[2].node.select.len);
    try testing.expectEqualStrings("id", st[2].node.select[0].field.last());
    try testing.expect(st[3].node == .sort);
    try testing.expectEqualStrings("id", st[3].node.sort.keys[0].field.last());
    try testing.expectEqual(@as(u64, 2), st[4].node.limit.count);

    const bare = try parseTest(a, "SELECT 1 x, 'a' y;");
    const items = bare.stmts[1].output.stages[1].node.select;
    try testing.expectEqualStrings("x", items[0].computed.name);
    try testing.expectEqualStrings("y", items[1].computed.name);
}
