//! Parser tests for expressions and names: precedence, BETWEEN, lambdas, quoted
//! identifiers, types, and the depth guard on long chains.

const ast = @import("../ast.zig");
const binOpText = @import("../sql_parser.zig").binOpText;
const Diagnostic = @import("../sql_parser.zig").Diagnostic;
const firstPipelineStages = @import("testing_util.zig").firstPipelineStages;
const parseExprStr = @import("../sql_parser.zig").parseExprStr;
const Parser = @import("../sql_parser.zig").Parser;
const parseSource = @import("../sql_parser.zig").parseSource;
const parseTest = @import("testing_util.zig").parseTest;
const parseTypeStr = @import("../sql_parser.zig").parseTypeStr;
const std = @import("std");
const testing = std.testing;

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
