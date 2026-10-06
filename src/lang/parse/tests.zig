//! The parser's tests: whole scripts through `parseSource`, each checking the AST
//! or the error a construct gives.

const Diagnostic = @import("../sql_parser.zig").Diagnostic;
const Parser = @import("../sql_parser.zig").Parser;
const aggStageIndex = @import("testing_util.zig").aggStageIndex;
const ast = @import("../ast.zig");
const binOpText = @import("../sql_parser.zig").binOpText;
const containsTrueLit = @import("testing_util.zig").containsTrueLit;
const firstPipelineStages = @import("testing_util.zig").firstPipelineStages;
const hintStr = @import("testing_util.zig").hintStr;
const parseExprStr = @import("../sql_parser.zig").parseExprStr;
const parseSource = @import("../sql_parser.zig").parseSource;
const parseSourceOpts = @import("../sql_parser.zig").parseSourceOpts;
const parseTest = @import("testing_util.zig").parseTest;
const parseTypeStr = @import("../sql_parser.zig").parseTypeStr;
const std = @import("std");
const testing = std.testing;
const types = @import("../types.zig");
test "sql expr: bitwise precedence slots between comparisons and additive" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const or_e = try parseExprStr(a, "1 | 2 & 3", &diag);
    try testing.expectEqual(ast.BinOp.bit_or, or_e.binary.op);
    try testing.expectEqual(@as(i64, 1), or_e.binary.l.int_lit);
    try testing.expectEqual(ast.BinOp.bit_and, or_e.binary.r.binary.op);

    const cmp = try parseExprStr(a, "flags & 4 = 4", &diag);
    try testing.expectEqual(ast.BinOp.eq, cmp.binary.op);
    try testing.expectEqual(ast.BinOp.bit_and, cmp.binary.l.binary.op);

    const sh = try parseExprStr(a, "1 + 1 << 2", &diag);
    try testing.expectEqual(ast.BinOp.shl, sh.binary.op);
    try testing.expectEqual(ast.BinOp.add, sh.binary.l.binary.op);

    const xo = try parseExprStr(a, "a ^ b | c", &diag);
    try testing.expectEqual(ast.BinOp.bit_or, xo.binary.op);
    try testing.expectEqual(ast.BinOp.bit_xor, xo.binary.l.binary.op);
    const ml = try parseExprStr(a, "2 * 3 & 1", &diag);
    try testing.expectEqual(ast.BinOp.bit_and, ml.binary.op);
    try testing.expectEqual(ast.BinOp.mul, ml.binary.l.binary.op);

    const bn = try parseExprStr(a, "~x & 1", &diag);
    try testing.expectEqual(ast.BinOp.bit_and, bn.binary.op);
    try testing.expectEqual(ast.UnOp.bit_not, bn.binary.l.unary.op);
    const an = try parseExprStr(a, "x & 1 and y", &diag);
    try testing.expectEqual(ast.BinOp.@"and", an.binary.op);
    try testing.expectEqual(ast.BinOp.bit_and, an.binary.l.binary.op);

    const cc = try parseExprStr(a, "a || b", &diag);
    try testing.expectEqualStrings("concat", cc.call.name);

    try testing.expectEqualStrings("&", binOpText(.bit_and));
    try testing.expectEqualStrings("<<", binOpText(.shl));
    try testing.expectEqualStrings(">>", binOpText(.shr));
}

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

test "sql: EXPLAIN is a statement anywhere in a script, not only the first" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'h', database = 'erp');
        \\EXPLAIN WITH paid AS (SELECT id, amount FROM 'in.csv' WHERE status = 'paid')
        \\SELECT id FROM paid;
    );
    try testing.expectEqual(ast.ExplainMode.none, prog.explain);
    try testing.expectEqual(@as(usize, 4), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .connection);
    try testing.expect(prog.stmts[2] == .binding);
    try testing.expectEqualStrings("paid", prog.stmts[2].binding.name);
    try testing.expect(prog.stmts[3] == .explain);
    const ex = prog.stmts[3].explain;
    try testing.expectEqual(ast.ExplainMode.plan, ex.mode);
    const last = ex.pipeline.stages[ex.pipeline.stages.len - 1];
    try testing.expect(last.node == .write);
    try testing.expectEqualStrings("stdout", last.node.write.connector);
}

test "sql: EXPLAIN ANALYZE and EXPLAIN LOAD INTO as later statements" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\SELECT id FROM 'in.csv';
        \\EXPLAIN ANALYZE SELECT id FROM 'in.csv';
        \\EXPLAIN LOAD INTO 'out.csv' AS SELECT id FROM 'in.csv';
    );
    try testing.expectEqual(ast.ExplainMode.none, prog.explain);
    try testing.expectEqual(@as(usize, 4), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .output);
    try testing.expect(prog.stmts[2] == .explain);
    try testing.expectEqual(ast.ExplainMode.analyze, prog.stmts[2].explain.mode);
    try testing.expect(prog.stmts[3] == .explain);
    try testing.expectEqual(ast.ExplainMode.plan, prog.stmts[3].explain.mode);
    const w = prog.stmts[3].explain.pipeline.stages[prog.stmts[3].explain.pipeline.stages.len - 1];
    try testing.expectEqualStrings("csv", w.node.write.connector);
    try testing.expectEqualStrings("out.csv", w.node.write.target);
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

test "sql: an aggregate with no window form says so before OVER" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    inline for (.{ "median", "stddev" }) |f| {
        try testing.expectError(error.ParseFailed, parseSource(a, "SELECT g, " ++ f ++ "(x) OVER (PARTITION BY g) AS s FROM 'in.csv';", &diag));
        try testing.expect(std.mem.startsWith(u8, diag.msg, "`" ++ f ++ "` is not a window function"));
        try testing.expectEqual(@as(u32, 11), diag.col);
    }
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

test "sql: a lambda's parameter is its own node in the body, never a column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a, "SELECT json_filter(tags, t -> t = name AND t <> 'x') AS v FROM 'in.csv';", &diag);
    const call = prog.stmts[1].output.stages[1].node.select[0].computed.expr.call;
    const l = call.args[1].lambda;
    try testing.expectEqualStrings("t", l.params[0]);
    const lhs = l.body.binary.l.binary;
    try testing.expectEqualStrings("t", lhs.l.lambda_var);
    try testing.expectEqualStrings("name", lhs.r.field.parts[0]);
    const p2 = try parseSource(a, "SELECT json_any(tags, t -> t = 1) AS v, t FROM 'in.csv';", &diag);
    try testing.expect(p2.stmts[1].output.stages[1].node.select[1] == .field);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT a - > b AS z FROM 'in.csv';", &diag));

    const p3 = try parseSource(a, "SELECT json_reduce(tags, 0, (acc, t, i) -> acc + t + i) AS v FROM 'in.csv';", &diag);
    const l3 = p3.stmts[1].output.stages[1].node.select[0].computed.expr.call.args[2].lambda;
    try testing.expectEqual(@as(usize, 3), l3.params.len);
    try testing.expectEqualStrings("acc", l3.body.binary.l.binary.l.lambda_var);
    try testing.expectEqualStrings("i", l3.body.binary.r.lambda_var);
    const p4 = try parseSource(a, "SELECT json_any(tags, (t) -> t = 1) AS v FROM 'in.csv';", &diag);
    try testing.expectEqual(@as(usize, 1), p4.stmts[1].output.stages[1].node.select[0].computed.expr.call.args[1].lambda.params.len);
    const p5 = try parseSource(a, "SELECT coalesce((a), b) AS v FROM 'in.csv';", &diag);
    try testing.expect(p5.stmts[1].output.stages[1].node.select[0].computed.expr.call.args[0].* == .field);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT json_reduce(tags, 0, (x, x) -> x) AS v FROM 'in.csv';", &diag));
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

test "sql: EXPLAIN COSTS is rejected in either position" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "EXPLAIN COSTS SELECT id FROM 'in.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no cost model") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\SELECT id FROM 'in.csv';
        \\EXPLAIN COSTS SELECT id FROM 'in.csv';
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no cost model") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\SELECT id FROM 'in.csv';
        \\EXPLAIN CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'h', database = 'erp');
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "after EXPLAIN") != null);
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

test "sql: ON beyond keys — the right side alone narrows it, the rest of an inner join filters after" {
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

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM 'a.csv' a LEFT JOIN 'b.csv' b ON b.id = a.id AND a.v = 'x';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "only an inner join") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM 'a.csv' a JOIN 'b.csv' b ON b.del <> '*';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "at least one") != null);
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
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no ON clause") != null);

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

test "sql: long IN lists and AND/OR chains are balanced; deeper nesting is an error, not a stack overflow" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var src = std.array_list.Managed(u8).init(a);
    try src.appendSlice("SELECT id FROM 'x.csv' WHERE id IN (");
    for (0..5000) |i| try src.writer().print("{s}{d}", .{ if (i == 0) "" else ",", i });
    try src.appendSlice(") AND (");
    for (0..5000) |i| try src.writer().print("{s}v = {d}", .{ if (i == 0) "" else " OR ", i });
    try src.appendSlice(");");
    const prog = try parseTest(a, src.items);
    const pred = prog.stmts[prog.stmts.len - 1].output.stages[1].node.filter;
    try testing.expect(!Parser.deeperThan(pred, 40));

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    src.clearRetainingCapacity();
    try src.appendSlice("SELECT v");
    for (0..2000) |_| try src.appendSlice(" + v");
    try src.appendSlice(" AS s FROM 'x.csv';");
    try testing.expectError(error.ParseFailed, parseSource(a, src.items, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "more than 1000 levels deep") != null);

    src.clearRetainingCapacity();
    try src.appendSlice("SELECT ");
    for (0..5000) |_| try src.append('(');
    try src.append('v');
    for (0..5000) |_| try src.append(')');
    try src.appendSlice(" FROM 'x.csv';");
    try testing.expectError(error.ParseFailed, parseSource(a, src.items, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "levels of parentheses") != null);

    src.clearRetainingCapacity();
    try src.appendSlice("SELECT COUNT(*) AS c FROM ");
    for (0..200) |_| try src.appendSlice("(SELECT * FROM ");
    try src.appendSlice("'x.csv'");
    for (0..200) |_| try src.appendSlice(") q");
    try src.append(';');
    try testing.expectError(error.ParseFailed, parseSource(a, src.items, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "queries nest more than") != null);
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

test "sql: PUSHDOWN fragment becomes a where hint on the read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT filial, valor FROM erp.dbo.SC5010
        \\  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
        \\WHERE valor > 0;
    );
    const pl = prog.stmts[2].output;
    try testing.expect(pl.stages[0].node == .read);
    try testing.expectEqualStrings("where", pl.stages[0].hints[0].key);
    try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", pl.stages[0].hints[0].value.str);
    try testing.expect(pl.stages[1].node == .filter);
}

test "sql: FOR EACH ROW OF json path with CASE dispatch" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE ENDPOINT '/fanout';
        \\PARAM job JSON FROM BODY;
        \\CREATE CONNECTION crm TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\FOR EACH ROW OF ($job.tables) AS (name, pk)
        \\  PARALLEL ON ERROR CONTINUE
        \\  CASE
        \\    WHEN pk IS EMPTY THEN
        \\      LOAD INTO sr.'crm_${lower(name)}' USING stream_load AS
        \\      SELECT * FROM crm.QUERY($$SELECT * FROM ${name}$$);
        \\    ELSE
        \\      LOAD INTO sr.'crm_${lower(name)}' USING stream_load
        \\        UPSERT ON ('${pk}') AS
        \\      SELECT *, now() AS extraction_timestamp
        \\      FROM crm.QUERY($$SELECT * FROM ${name}$$);
        \\  END CASE
        \\END FOR;
    );
    try testing.expect(prog.stmts[0].kind.kind == .http);
    try testing.expect(prog.stmts[1] == .param);
    try testing.expect(prog.stmts[1].param.is_json);
    const fe = prog.stmts[4].for_each;
    try testing.expectEqual(@as(usize, 2), fe.var_names.len);
    try testing.expect(fe.source == .json_path);
    try testing.expectEqualStrings("job", fe.source.json_path.parts[0]);
    try testing.expectEqual(@as(usize, 1), fe.body.len);
    const m = fe.body[0].match;
    try testing.expect(m.subject == null);
    try testing.expectEqual(@as(usize, 2), m.arms.len);
    try testing.expect(m.arms[0].guard != null);
    try testing.expect(m.arms[1].is_default);
    const arm1 = m.arms[0].body[0].output;
    try testing.expectEqual(@as(usize, 2), arm1.stages.len);
    const arm2 = m.arms[1].body[0].output;
    try testing.expectEqual(@as(usize, 3), arm2.stages.len);
    try testing.expectEqualStrings("crm_${lower(name)}", arm2.stages[2].node.write.target);
}

test "sql: EACH TABLE OF (SELECT ...) parses an in-engine discovery pipeline" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION cat TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM EACH TABLE OF (
        \\  SELECT name, substr(name, 4, 2) AS emp FROM cat.tables WHERE name LIKE 'CT2%'
        \\) AS (table_name, emp);
    );
    const pl = prog.stmts[2].output;
    const u = pl.stages[0].node.union_;
    try testing.expectEqual(@as(usize, 0), u.discover_query.len);
    try testing.expectEqual(@as(usize, 0), u.discover_json.len);
    try testing.expectEqualStrings("cat", u.discover_conn);
    const disc = u.discover_pipeline orelse return error.TestExpectedPipeline;
    try testing.expect(disc.stages[0].node == .read);
    try testing.expectEqualStrings("cat", disc.stages[0].node.read.connector);
    try testing.expect(disc.stages[1].node == .filter);
    try testing.expect(disc.stages[2].node == .select);
    try testing.expectEqual(@as(usize, 2), disc.stages[2].node.select.len);
    try testing.expectEqualStrings("emp", pl.stages[0].hints[0].value.ident);
}

test "sql: EACH TABLE OF (SELECT ...) over a non-connection source needs IN <conn>" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM EACH TABLE OF (SELECT name, emp FROM 'cat.csv') AS (table_name, emp);
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "add `IN <conn>`") != null);

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM EACH TABLE OF (SELECT name, emp FROM 'cat.csv') IN erp AS (table_name, emp);
    );
    const u = prog.stmts[2].output.stages[0].node.union_;
    try testing.expectEqualStrings("erp", u.discover_conn);
    try testing.expect(u.discover_pipeline != null);
}

test "sql: FOR EACH ROW OF (SELECT ...) parses an in-engine discovery pipeline" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\FOR EACH ROW OF (SELECT name, lower(name) AS slug FROM 'catalog.csv' WHERE active = '1') AS (name, slug)
        \\  LOAD INTO 'out_${slug}.csv' AS SELECT * FROM '${slug}.csv';
        \\END FOR;
    );
    const fe = prog.stmts[1].for_each;
    try testing.expectEqual(@as(usize, 2), fe.var_names.len);
    try testing.expect(fe.source == .pipeline);
    const disc = fe.source.pipeline;
    try testing.expectEqual(@as(usize, 3), disc.stages.len);
    try testing.expect(disc.stages[0].node == .read);
    try testing.expectEqualStrings("csv", disc.stages[0].node.read.connector);
    try testing.expect(disc.stages[1].node == .filter);
    try testing.expectEqualStrings("slug", disc.stages[2].node.select[1].computed.name);
    try testing.expectEqual(@as(usize, 1), fe.body.len);
}

test "sql: connection credential convention injects env calls" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\SELECT 1 AS one FROM 'in.csv';
    );
    const conn = prog.stmts[1].connection;
    var saw_user = false;
    var saw_pass = false;
    for (conn.config) |attr| {
        if (std.mem.eql(u8, attr.key, "user")) {
            saw_user = true;
            try testing.expectEqualStrings("env", attr.value.call.name);
            try testing.expectEqualStrings("ERP_USER", attr.value.call.args[0].str_lit);
        }
        if (std.mem.eql(u8, attr.key, "password")) {
            saw_pass = true;
            try testing.expectEqualStrings("ERP_PASS", attr.value.call.args[0].str_lit);
        }
    }
    try testing.expect(saw_user and saw_pass);
}

test "sql: truncated expression is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "SELECT * FROM x.QUERY($$q$$) WHERE a >", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expectEqualStrings("expected an expression, found end of input", diag.msg);
    try testing.expectEqual(@as(u32, 1), diag.line);
    try testing.expectEqual(@as(u32, 39), diag.col);
}

test "sql: ACCEPT INTO BUFFER declaration and FROM BUFFER source" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE ENDPOINT '/eventos' DOC 'telemetria'
        \\  ACCEPT BODY (device_id STRING NOT NULL, v INT)
        \\  INTO BUFFER 'ev' AT '/var/lib/basalt/wal' SEGMENT 16 MB RETAIN 24 HOURS;
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\LOAD INTO sr.eventos USING stream_load AS
        \\SELECT device_id, CAST(v AS INT) AS v
        \\FROM BUFFER 'ev' FLUSH EVERY 5 SECONDS OR 50000 ROWS
        \\WHERE device_id IS NOT NULL;
    );
    const kd = prog.stmts[0].kind;
    try testing.expect(kd.kind == .http);
    const buf = kd.buffer.?;
    try testing.expectEqualStrings("ev", buf.name);
    try testing.expectEqualStrings("/var/lib/basalt/wal", buf.dir);
    try testing.expectEqual(@as(u64, 16 << 20), buf.segment_bytes);
    try testing.expectEqual(@as(u32, 24), buf.retain_hours.?);
    try testing.expectEqual(@as(usize, 2), buf.schema.len);
    try testing.expect(buf.schema[0].not_null);

    const pl = prog.stmts[2].output;
    const rd = pl.stages[0].node.read;
    try testing.expectEqualStrings("buffer", rd.connector);
    try testing.expectEqualStrings("ev", rd.form.buffer.name);
    try testing.expectEqualStrings("", rd.form.buffer.dir);
    try testing.expectEqualStrings("flush_secs", pl.stages[0].hints[0].key);
    try testing.expectEqual(@as(i64, 5), pl.stages[0].hints[0].value.int);
    try testing.expectEqualStrings("flush_rows", pl.stages[0].hints[1].key);
    try testing.expectEqual(@as(i64, 50000), pl.stages[0].hints[1].value.int);
    try testing.expect(pl.stages[1].node == .filter);
}

test "sql: reflection lowering — IDENTIFIER / || / PUSHDOWN(expr) -> ${...} templates" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\PARAM tables JSON;
        \\CREATE CONNECTION fluig TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\FOR EACH ROW OF ($tables) AS (name, where)
        \\  PARALLEL ON ERROR CONTINUE
        \\  LOAD INTO sr.IDENTIFIER('fluig_' || lower($name))
        \\    USING stream_load UPSERT AS
        \\  SELECT *, now() AS extraction_timestamp
        \\  FROM fluig.dbo.IDENTIFIER($name)
        \\  PUSHDOWN($where);
        \\END FOR;
    );
    const fe = prog.stmts[4].for_each;
    const body = fe.body[0].output;
    const rd = body.stages[0].node.read;
    try testing.expectEqualStrings("dbo", rd.form.table.parts[0]);
    try testing.expectEqualStrings("${name}", rd.form.table.parts[1]);
    try testing.expectEqualStrings("where", body.stages[0].hints[0].key);
    try testing.expectEqualStrings("${where}", body.stages[0].hints[0].value.str);
    const w = body.stages[body.stages.len - 1].node.write;
    try testing.expectEqualStrings("fluig_${lower(name)}", w.target);
    try testing.expect(w.mode == .upsert);
    try testing.expectEqual(@as(usize, 0), w.mode.upsert.keys.len);
}

test "sql: IDENTIFIER(expr) in a column position -> a field named by a template" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\LOAD INTO '/tmp/o.csv' AS
        \\SELECT IDENTIFIER($c), COUNT(*) AS n FROM 'in.csv'
        \\GROUP BY IDENTIFIER($c) ORDER BY IDENTIFIER('k_' || $c) DESC;
    , &diag);
    const stages = prog.stmts[prog.stmts.len - 1].output.stages;
    var saw_agg = false;
    var saw_sort = false;
    for (stages) |st| try testing.expect(st.node != .select);
    for (stages) |st| switch (st.node) {
        .aggregate => |ag| {
            saw_agg = true;
            try testing.expectEqual(@as(usize, 1), ag.by.len);
            try testing.expectEqual(@as(usize, 1), ag.by[0].parts.len);
            try testing.expectEqualStrings("${c}", ag.by[0].parts[0]);
            try testing.expectEqual(@as(usize, 1), ag.aggs.len);
            try testing.expectEqualStrings("n", ag.aggs[0].name);
        },
        .sort => |so| {
            saw_sort = true;
            try testing.expectEqualStrings("k_${c}", so.keys[0].field.parts[0]);
            try testing.expect(so.keys[0].desc);
        },
        else => {},
    };
    try testing.expect(saw_agg and saw_sort);
}

test "sql: DESCRIBE lowers to a describe-mode EXPLAIN that asks a table for no rows; SHOW TABLES to a catalog query" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\DESCRIBE erp.public.orders;
        \\DESCRIBE 'x.csv' WITH (delimiter = ';');
        \\DESC SELECT 1 AS x;
        \\SHOW TABLES FROM erp.public LIKE 'ord%';
    , &diag);
    const t = prog.stmts[2].explain;
    try testing.expectEqual(ast.ExplainMode.describe, t.mode);
    try testing.expectEqualStrings("1 = 0", t.pipeline.stages[0].node.read.where);
    try testing.expect(t.pipeline.stages[t.pipeline.stages.len - 1].node == .write);
    try testing.expectEqualStrings("delimiter", prog.stmts[3].explain.pipeline.stages[0].hints[0].key);
    try testing.expectEqual(ast.ExplainMode.describe, prog.stmts[4].explain.mode);
    const show = prog.stmts[5].output.stages[0].node.read;
    try testing.expectEqualStrings("erp", show.connector);
    try testing.expect(std.mem.indexOf(u8, show.form.query, "INFORMATION_SCHEMA.TABLES") != null);
    try testing.expect(std.mem.indexOf(u8, show.form.query, "TABLE_SCHEMA = 'public'") != null);
    try testing.expect(std.mem.indexOf(u8, show.form.query, "LIKE 'ord%'") != null);

    try testing.expectError(error.ParseFailed, parseSource(a, "SHOW TABLES FROM nope;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "not a connection") != null);
}

test "sql: http reads — GET/POST calls, connection clauses, resources" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'https://x/v1')
        \\  RETRY 3 ON (429) WITH (items = 'data');
        \\PARAM code STRING DEFAULT 'br';
        \\CREATE RESOURCE rc.countries AS GET('/countries', region = 'europe')
        \\  PAGINATE BY page (param = 'p');
        \\SELECT * FROM rc.GET('/alpha/' || $code, fields = 'name');
        \\SELECT * FROM rc.POST('/search', body = $$ {"q": 1} $$);
        \\SELECT * FROM rc.countries WITH (items = 'rows');
        \\SHOW TABLES FROM rc LIKE 'c%';
    , &diag);
    const conn = prog.stmts[1].connection;
    try testing.expectEqual(@as(i64, 3), conn.hints[0].value.int);
    try testing.expectEqualStrings("data", hintStr(conn.hints, "items").?);

    const get = prog.stmts[3].output.stages[0];
    try testing.expectEqualStrings("rc", get.node.read.connector);
    try testing.expectEqualStrings("/alpha/${code}", get.node.read.form.path);
    try testing.expectEqualStrings("name", hintStr(get.hints, "query:fields").?);

    const post = prog.stmts[4].output.stages[0];
    try testing.expectEqualStrings("/search", post.node.read.form.path);
    try testing.expectEqualStrings("post", hintStr(post.hints, "method").?);
    try testing.expectEqualStrings(" {\"q\": 1} ", hintStr(post.hints, "body").?);

    const res = prog.stmts[5].output.stages[0];
    try testing.expectEqualStrings("/countries", res.node.read.form.path);
    try testing.expectEqualStrings("europe", hintStr(res.hints, "query:region").?);
    try testing.expectEqualStrings("page", hintStr(res.hints, "paginate").?);
    try testing.expectEqualStrings("rows", res.hints[res.hints.len - 1].value.str);

    const show = prog.stmts[6].output.stages;
    try testing.expectEqualStrings("range", show[0].node.read.connector);
    try testing.expect(show[1].node == .select);
    try testing.expect(show[2].node == .filter);

    const bad = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'u'); SELECT * FROM rc.nope;", .msg = "no such resource" },
        .{ .src = "CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'u'); SELECT * FROM rc.GET('/x', body = 'b');", .msg = "GET takes no body" },
        .{ .src = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h') RETRY 2;", .msg = "http` connections only" },
        .{ .src = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h'); CREATE RESOURCE pg.t AS GET('/t');", .msg = "postgres connection" },
        .{ .src = "CREATE RESOURCE nope.t AS GET('/t');", .msg = "not a connection" },
    };
    for (bad) |b| {
        try testing.expectError(error.ParseFailed, parseSource(a, b.src, &diag));
        try testing.expect(std.mem.indexOf(u8, diag.msg, b.msg) != null);
    }
}

test "sql: UNNEST(JSON_EACH(col)) is a json explode" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a, "SELECT id, tag FROM 'x.csv' CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag;");
    const ex = prog.stmts[1].output.stages[1].node.explode;
    try testing.expectEqualStrings("tags", ex.field);
    try testing.expect(ex.json);
    try testing.expect(ex.delim == null);
}

test "sql: a statement function may open with the statement CASE; the scalar CASE stays an expression" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE FUNCTION pick(x) AS CASE $x WHEN 'a' THEN 1 ELSE CASE WHEN $x = 'b' THEN 2 ELSE 0 END END;
        \\CREATE FUNCTION branch(x) AS
        \\  CASE $x
        \\    WHEN 'a' THEN PRINT 'a'; FOR EACH ROW OF (SELECT 1 AS n) AS (n) PRINT $n; END FOR;
        \\    ELSE PRINT CASE WHEN $x = '' THEN 'blank' ELSE $x END;
        \\  END CASE;
        \\END;
        \\SELECT pick('b') AS p;
    , &diag);
    try testing.expect(prog.stmts[1].func.body == .expr);
    try testing.expect(prog.stmts[1].func.body.expr.* == .match);
    try testing.expect(prog.stmts[2].func.body == .stmts);
    try testing.expect(prog.stmts[2].func.body.stmts[0] == .match);
}

test "sql: EXCEPT accepts IDENTIFIER(expr), lowered to a template name" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a, "LOAD INTO '/tmp/o.csv' AS SELECT * EXCEPT (id, IDENTIFIER($ex)) FROM 'in.csv';", &diag);
    const items = prog.stmts[1].output.stages[1].node.select;
    try testing.expectEqualStrings("id", items[0].star_except[0]);
    try testing.expectEqualStrings("${ex}", items[0].star_except[1]);
}

test "sql: FROM IDENTIFIER(expr) -> a computed path read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\LOAD INTO '/tmp/o.csv' AS
        \\SELECT * FROM IDENTIFIER('dir/' || name || '.csv');
    );
    const rd = prog.stmts[1].output.stages[0].node.read;
    try testing.expectEqualStrings("csv", rd.connector);
    try testing.expectEqualStrings("dir/${name}.csv", rd.form.path);
}

test "sql: FROM IDENTIFIER without a literal extension is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "LOAD INTO '/tmp/o.csv' AS SELECT * FROM IDENTIFIER('dir/' || name);", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expect(std.mem.indexOf(u8, diag.msg, "literal extension") != null);
}

test "sql: LOAD INTO IDENTIFIER(expr) -> a computed path write" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\LOAD INTO IDENTIFIER('dir/' || name || '.csv') AS
        \\SELECT * FROM 'in.csv';
    );
    const stages = prog.stmts[1].output.stages;
    const w = stages[stages.len - 1].node.write;
    try testing.expectEqualStrings("csv", w.connector);
    try testing.expectEqualStrings("dir/${name}.csv", w.target);
}

test "sql: LOAD INTO IDENTIFIER without a literal extension is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "LOAD INTO IDENTIFIER('dir/' || name) AS SELECT * FROM 'in.csv';", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expect(std.mem.indexOf(u8, diag.msg, "literal extension") != null);
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

test "sql: IDENTIFIER in an UPSERT key -> per-row computed key column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\PARAM tables JSON;
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\FOR EACH ROW OF ($tables) AS (name, pk)
        \\  LOAD INTO sr.T
        \\    USING stream_load
        \\    UPSERT ON (IDENTIFIER(if($pk = '', $name || 'id', $pk))) AS
        \\  SELECT * FROM 'x.csv';
        \\END FOR;
    );
    const w = prog.stmts[3].for_each.body[0].output.stages[1].node.write;
    try testing.expectEqual(@as(usize, 1), w.mode.upsert.keys.len);
    try testing.expectEqualStrings("${if((pk == ''), concat(name, 'id'), pk)}", w.mode.upsert.keys[0]);
}

test "sql: CREATE FUNCTION — expression, typed/DEFAULT params, statement body, CALL" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE FUNCTION margin(rev FLOAT, cost FLOAT) AS (rev - cost) / rev;
        \\CREATE OR REPLACE FUNCTION round_to(x, n INT DEFAULT 2) AS round(x, n);
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\CREATE FUNCTION sync_table(name, filter) AS
        \\  LOAD INTO sr.IDENTIFIER('bronze_' || lower($name)) USING stream_load UPSERT AS
        \\  SELECT * FROM erp.dbo.IDENTIFIER($name) PUSHDOWN($filter);
        \\END;
        \\CALL sync_table('SC5010', $$D_E_L_E_T_ <> '*'$$);
    );

    const margin = prog.stmts[1].func;
    try testing.expect(margin.body == .expr);
    try testing.expect(!margin.replace);
    try testing.expectEqual(@as(usize, 2), margin.params.len);
    try testing.expectEqualStrings("rev", margin.params[0].name);
    try testing.expectEqual(types.TypeKind.float, margin.params[0].ty.?.kind);
    try testing.expect(margin.params[0].default == null);

    const round_to = prog.stmts[2].func;
    try testing.expect(round_to.replace);
    try testing.expect(round_to.params[0].ty == null);
    try testing.expectEqual(types.TypeKind.int, round_to.params[1].ty.?.kind);
    try testing.expectEqual(@as(i64, 2), round_to.params[1].default.?.int_lit);

    const sync = prog.stmts[5].func;
    try testing.expect(sync.body == .stmts);
    try testing.expectEqual(@as(usize, 1), sync.body.stmts.len);
    const pipe = sync.body.stmts[0].output;
    try testing.expectEqualStrings("${name}", pipe.stages[0].node.read.form.table.parts[1]);
    try testing.expectEqualStrings("${filter}", pipe.stages[0].hints[0].value.str);
    try testing.expectEqualStrings("bronze_${lower(name)}", pipe.stages[pipe.stages.len - 1].node.write.target);

    const call = prog.stmts[6].call;
    try testing.expectEqualStrings("sync_table", call.name);
    try testing.expectEqual(@as(usize, 2), call.args.len);
    try testing.expectEqualStrings("SC5010", call.args[0].str_lit);
    try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", call.args[1].str_lit);
}

test "sql: THROW — bare, WHEN-guarded, and inside a CASE arm" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\PARAM tbl STRING DEFAULT '';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\THROW 'unreachable branch';
        \\CASE WHEN $tbl = 'zz' THEN THROW 'no zz allowed'; END CASE;
        \\SELECT 1 AS n FROM 'x.csv';
    );

    const guarded = prog.stmts[2].throw;
    try testing.expectEqualStrings("tbl is required (e.g. -p tbl=SC5)", guarded.message.str_lit);
    try testing.expect(guarded.when.?.* == .is_null);
    try testing.expectEqual(ast.Expr.NullTest.is_empty, guarded.when.?.is_null.kind);
    try testing.expect(!guarded.when.?.is_null.negated);

    const bare = prog.stmts[3].throw;
    try testing.expectEqualStrings("unreachable branch", bare.message.str_lit);
    try testing.expect(bare.when == null);

    const armed = prog.stmts[4].match.arms[0].body[0].throw;
    try testing.expectEqualStrings("no zz allowed", armed.message.str_lit);
    try testing.expect(armed.when == null);
}

test "sql: a non-DEFAULT parameter may not follow a defaulted one" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a,
        \\CREATE FUNCTION f(a INT DEFAULT 1, b INT) AS a + b;
        \\SELECT f(1) AS v FROM 'x.csv';
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "without DEFAULT") != null);
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

test "sql: BETWEEN lowers to >= AND <=" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT * FROM 'x.csv' WHERE v BETWEEN 10 AND 30;");
    const st = firstPipelineStages(prog);
    var f: ?*const ast.Expr = null;
    for (st) |s| if (s.node == .filter) {
        f = s.node.filter;
    };
    const e = f.?;
    try testing.expectEqual(ast.BinOp.@"and", e.binary.op);
    try testing.expectEqual(ast.BinOp.ge, e.binary.l.binary.op);
    try testing.expectEqual(@as(i64, 10), e.binary.l.binary.r.int_lit);
    try testing.expectEqual(ast.BinOp.le, e.binary.r.binary.op);
    try testing.expectEqual(@as(i64, 30), e.binary.r.binary.r.int_lit);
}

test "sql: NOT BETWEEN negates the whole range" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT * FROM 'x.csv' WHERE v NOT BETWEEN 10 AND 30;");
    const st = firstPipelineStages(prog);
    var f: ?*const ast.Expr = null;
    for (st) |s| if (s.node == .filter) {
        f = s.node.filter;
    };
    try testing.expectEqual(ast.UnOp.not, f.?.unary.op);
    try testing.expectEqual(ast.BinOp.@"and", f.?.unary.e.binary.op);
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

test "sql: a double-quoted name is a column reference, not a string" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT \"Exchange rate\" AS taxa FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    const sel = st[1].node.select;
    try testing.expectEqualStrings("taxa", sel[0].computed.name);
    try testing.expectEqualStrings("Exchange rate", sel[0].computed.expr.field.last());
    try testing.expectEqualStrings("Exchange rate", firstPipelineStages(try parseTest(a, "SELECT 'Exchange rate' AS lit FROM 'x.csv';"))[1].node.select[0].computed.expr.str_lit);
}

test "sql: a quoted name that spells a keyword is still a column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT \"select\" AS s FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    try testing.expectEqualStrings("select", st[1].node.select[0].computed.expr.field.last());
}

test "sql: a doubled quote inside a name is one literal quote" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT \"a\"\"b\" AS c FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    try testing.expectEqualStrings("a\"b", st[1].node.select[0].computed.expr.field.last());
}

test "sql: an empty quoted name is a lex error, not an empty column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT \"\" FROM 'x.csv';", &diag));
    try testing.expect(std.mem.startsWith(u8, diag.msg, "invalid token `\"\""));
    try testing.expectEqual(@as(u32, 8), diag.col);
}

test "sql: PUSHDOWN on a discovered union becomes a per-branch where hint" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\SELECT * FROM EACH TABLE OF (erp.QUERY($$SELECT name, x FROM sys.tables$$))
        \\  AS (table_name, EMPRESA)
        \\  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
        \\  ANCHOR SCHEMA SC5010;
    );
    const st = firstPipelineStages(prog);
    var saw_where = false;
    var saw_canon = false;
    for (st[0].hints) |h| {
        if (std.mem.eql(u8, h.key, "where")) {
            saw_where = true;
            try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", h.value.str);
        }
        if (std.mem.eql(u8, h.key, "canon")) saw_canon = true;
    }
    try testing.expect(saw_where);
    try testing.expect(saw_canon);
}

test "sql: ANCHOR SCHEMA before PUSHDOWN parses the same" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\SELECT * FROM EACH TABLE OF (erp.QUERY($$SELECT name, x FROM sys.tables$$))
        \\  AS (table_name, EMPRESA)
        \\  ANCHOR SCHEMA SC5010
        \\  PUSHDOWN($$1 = 1$$);
    );
    const st = firstPipelineStages(prog);
    var n: usize = 0;
    for (st[0].hints) |h| {
        if (std.mem.eql(u8, h.key, "where")) {
            n += 1;
            try testing.expectEqualStrings("1 = 1", h.value.str);
        }
        if (std.mem.eql(u8, h.key, "canon")) {
            n += 1;
            try testing.expectEqualStrings("SC5010", h.value.ident);
        }
    }
    try testing.expectEqual(@as(usize, 2), n);
}

test "sql: PRINT takes a literal or a `||` chain over params" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\PARAM tbl STRING DEFAULT 'sc5';
        \\PRINT 'starting';
        \\PRINT 'loading ' || $tbl;
        \\SELECT id FROM 'x.csv';
    );
    try testing.expectEqualStrings("starting", prog.stmts[2].print.expr.str_lit);

    const cat = prog.stmts[3].print.expr;
    try testing.expect(cat.* == .call);
    try testing.expectEqualStrings("concat", cat.call.name);
    try testing.expectEqualStrings("loading ", cat.call.args[0].str_lit);
    try testing.expectEqualStrings("tbl", cat.call.args[1].field.last());
}

test "sql: PRINT is a statement inside a FOR EACH body and a statement function" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE FUNCTION note(msg) AS
        \\  PRINT 'note: ' || $msg;
        \\END;
        \\FOR EACH ROW OF (SELECT name FROM 'catalog.csv') AS (name)
        \\  PRINT 'company ' || $name;
        \\  LOAD INTO 'out.csv' AS SELECT id FROM 'x.csv';
        \\END FOR;
    );
    const note = prog.stmts[1].func;
    try testing.expect(note.body == .stmts);
    try testing.expect(note.body.stmts[0] == .print);

    const fe = prog.stmts[2].for_each;
    try testing.expectEqual(@as(usize, 2), fe.body.len);
    try testing.expect(fe.body[0] == .print);
    try testing.expectEqualStrings("name", fe.body[0].print.expr.call.args[1].field.last());
    try testing.expect(fe.body[1] == .output);
}

test "sql: PRINT without an expression is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "PRINT;\nSELECT id FROM 'x.csv';", &diag));
    try testing.expectEqualStrings("expected an expression, found ';'", diag.msg);
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

test "parse with recovery: every statement that fails is recorded, the rest still parse; a broken block ends it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    var errs = std.array_list.Managed(Diagnostic).init(a);

    const prog = try parseSourceOpts(a,
        \\SELECT 1 AS x;
        \\SELECT 1 +;
        \\SELECT 2 AS y;
        \\SELECT (1 AS w;
        \\SELECT 3 AS z;
    , &diag, .{ .errors = &errs });
    try testing.expectEqual(@as(usize, 2), errs.items.len);
    try testing.expectEqual(@as(u32, 2), errs.items[0].line);
    try testing.expectEqual(@as(u32, 4), errs.items[1].line);
    var outputs: usize = 0;
    for (prog.stmts) |s| if (s == .output) {
        outputs += 1;
    };
    try testing.expectEqual(@as(usize, 3), outputs);

    errs.clearRetainingCapacity();
    _ = try parseSourceOpts(a,
        \\SELECT 1 AS x;
        \\FOR EACH ROW OF (SELECT 1 AS n) AS (n)
        \\  SELECT 1 +;
        \\  SELECT 2 AS y;
        \\END FOR;
        \\SELECT 3 +;
    , &diag, .{ .errors = &errs });
    try testing.expectEqual(@as(usize, 1), errs.items.len);

    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT 1 +; SELECT 2 AS y;", &diag));
}

test "known tables: a name the session holds reads as a binding reference, FROM and JOIN" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM enrich;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "unknown source `enrich`") != null);

    const prog = try parseSourceOpts(a,
        \\WITH r AS (SELECT range AS id FROM RANGE(3))
        \\SELECT * FROM enrich JOIN r ON enrich.id = r.id;
        \\SELECT * FROM r JOIN enrich ON r.id = enrich.id;
    , &diag, .{ .known_tables = &.{"enrich"} });
    var refs: usize = 0;
    for (prog.stmts) |s| if (s == .output) {
        if (s.output.stages[0].node == .ref and std.mem.eql(u8, s.output.stages[0].node.ref, "enrich")) refs += 1;
        for (s.output.stages) |st| if (st.node == .join and std.mem.eql(u8, st.node.join.binding, "enrich")) {
            refs += 1;
        };
    };
    try testing.expectEqual(@as(usize, 2), refs);
}

test "parseTypeStr: params, aliases, junk" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try testing.expect(parseTypeStr(a, "decimal(10,2)").?.scale == 2);
    try testing.expect(parseTypeStr(a, "varchar(20)").?.kind == .string);
    try testing.expect(parseTypeStr(a, "timestamp").?.kind == .timestamp);
    try testing.expect(parseTypeStr(a, "nope") == null);
    try testing.expect(parseTypeStr(a, "int int") == null);
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
