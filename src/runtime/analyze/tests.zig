//! Analysis tests over whole programs: a script parsed and analyzed offline, each
//! checking the plan, the schemas it resolves, or the error a construct gives.

const analyze = @import("../analyze.zig").analyze;
const analyzeCsv = @import("testing_util.zig").analyzeCsv;
const ast = @import("../../lang/ast.zig");
const Diag = @import("../analyze.zig").Diag;
const expectAnalyzeErr = @import("testing_util.zig").expectAnalyzeErr;
const ParamMap = std.StringHashMap(*const ast.Expr);
const parse = @import("../analyze.zig").parse;
const parser = @import("../../lang/sql_parser.zig");
const pushdown = @import("../pushdown.zig");
const Stage = @import("../analyze.zig").Stage;
const std = @import("std");
const substFilterParams = @import("../analyze.zig").substFilterParams;
const types = @import("../../lang/types.zig");

test "analyze a CSV map pipeline: structure, offline schema, physical" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const src = try std.fmt.allocPrint(
        a,
        "LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= 50;",
        .{in},
    );
    const prog = try parse(a, src);
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);

    try std.testing.expectEqualStrings("batch", plan.kind);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);
    const o = plan.outputs[0];
    try std.testing.expectEqualStrings("csv", o.source.connector);
    try std.testing.expect(o.source.schema != null);
    try std.testing.expectEqual(@as(usize, 2), o.source.schema.?.fields.len);
    try std.testing.expectEqual(@as(usize, 2), o.stages.len);
    try std.testing.expectEqualStrings("filter", o.stages[0].kind);
    try std.testing.expectEqualStrings("select", o.stages[1].kind);
    try std.testing.expect(!o.physical.has_breaker);
    try std.testing.expect(!o.physical.splittable);
    try std.testing.expect(o.physical.morsel_parallel);
}

test "physical plan: which SQL shapes report a key-range split" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');\n";

    const cases = [_]struct { q: []const u8, split: bool, breaker: ?bool = null }{
        .{ .q = "SELECT a FROM pg.t WHERE b > 0", .split = true, .breaker = false },
        .{ .q = "SELECT g, COUNT(*) AS n FROM pg.t GROUP BY g", .split = true },
        .{ .q = "SELECT g, COUNT(*) AS n FROM pg.t GROUP BY g ORDER BY n DESC LIMIT 5", .split = true },
        .{ .q = "SELECT a FROM pg.t ORDER BY a DESC LIMIT 10", .split = false },
        .{ .q = "SELECT * FROM pg.orders ORDER BY id", .split = false, .breaker = true },
        .{ .q = "SELECT DISTINCT a FROM pg.t", .split = false },
        .{ .q = "SELECT g, COUNT(*) AS n FROM pg.QUERY($$SELECT * FROM t$$) GROUP BY g", .split = false },
        .{ .q = "SELECT * FROM pg.QUERY($$SELECT 1 AS x$$)", .split = false, .breaker = false },
    };

    for (cases) |c| {
        const src = try std.fmt.allocPrint(a, "{s}LOAD INTO '/tmp/o.csv' AS {s};", .{ conn, c.q });
        const prog = try parse(a, src);
        var diag = Diag{};
        const plan = try analyze(a, prog, &diag);
        try std.testing.expectEqual(c.split, plan.outputs[0].physical.splittable);
        if (c.breaker) |b| try std.testing.expectEqual(b, plan.outputs[0].physical.has_breaker);
    }
}

test "physical plan: which file reads divide into morsels" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const cases = [_]struct { from: []const u8, morsel: bool }{
        .{ .from = "'/data/x.parquet'", .morsel = true },
        .{ .from = "'https://h/x.parquet'", .morsel = true },
        .{ .from = "'s3://bkt/x.parquet'", .morsel = true },
        .{ .from = "'/data/x.csv'", .morsel = true },
        .{ .from = "'https://h/x.csv'", .morsel = false },
        .{ .from = "'az://acct/c/x.csv'", .morsel = false },
        .{ .from = "RANGE(10)", .morsel = false },
    };

    for (cases) |c| {
        const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/o.csv' AS SELECT * FROM {s};", .{c.from});
        const prog = try parse(a, src);
        var diag = Diag{};
        const plan = try analyze(a, prog, &diag);
        try std.testing.expectEqual(c.morsel, plan.outputs[0].physical.morsel_parallel);
    }
}

test "analyze a SQL table pipeline: the schema stays unresolved offline" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (
        \\  host = 'h', user = 'u', password = 'p', database = 'd'
        \\);
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.orders WHERE amount > 0;
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expect(plan.outputs[0].source.schema == null);
}

test "analyze pushdown preview: a CTE, derived table or table function at the head sends its WHERE to the source" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');\n";
    const queries = [_][]const u8{
        "LOAD INTO '/tmp/x.csv' AS WITH o AS (SELECT id, amount FROM pg.orders WHERE amount > 0) SELECT id FROM o;",
        "LOAD INTO '/tmp/x.csv' AS SELECT id FROM (SELECT id, amount FROM pg.orders WHERE amount > 0) d;",
        "CREATE FUNCTION pos(lo INT) RETURNS TABLE AS SELECT id, amount FROM pg.orders WHERE amount > $lo;\nLOAD INTO '/tmp/x.csv' AS SELECT id FROM pos(0);",
        "LOAD INTO '/tmp/x.csv' AS SELECT id FROM (SELECT id, amount AS amt FROM pg.orders) d WHERE amt > 0;",
    };
    for (queries) |q| {
        var diag = Diag{};
        const plan = try analyze(a, try parse(a, try std.mem.concat(a, u8, &.{ conn, q })), &diag);
        const src = plan.outputs[0].source;
        try std.testing.expectEqualStrings("postgres", src.connector);
        try std.testing.expectEqualStrings("(\"amount\" > 0)", src.pushdown);
        try std.testing.expect(std.mem.indexOf(u8, src.detail, "(via binding ") != null);
    }
    var diag = Diag{};
    const w = try analyze(a, try parse(a, conn ++
        "LOAD INTO '/tmp/x.csv' AS WITH r AS (SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM pg.orders WHERE amount > 0) SELECT id FROM r WHERE rn = 1;"), &diag);
    try std.testing.expectEqualStrings("", w.outputs[0].source.pushdown);
}

test "analyze pushdown preview: a chain of CTEs over one read sends every WHERE it can, and lists its stages" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');\n";

    var diag = Diag{};
    const plain = try analyze(a, try parse(a, conn ++
        "LOAD INTO '/tmp/x.csv' AS WITH a AS (SELECT id, amt, k FROM pg.t WHERE amt > 1), b AS (SELECT id, k FROM a WHERE id < 100) SELECT id FROM b WHERE k = 'x';"), &diag);
    const src = plain.outputs[0].source;
    try std.testing.expectEqualStrings("(\"amt\" > 1) AND (\"id\" < 100) AND (\"k\" = 'x')", src.pushdown);
    try std.testing.expect(std.mem.indexOf(u8, src.detail, "(via binding b, a)") != null);
    var filters: usize = 0;
    for (plain.outputs[0].stages) |st| {
        if (std.mem.eql(u8, st.kind, "filter")) filters += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), filters);

    const computed = try analyze(a, try parse(a, conn ++
        "LOAD INTO '/tmp/x.csv' AS WITH a AS (SELECT id, amt * 2 AS dbl FROM pg.t), b AS (SELECT id FROM a WHERE dbl > 5 AND id < 100) SELECT id FROM b;"), &diag);
    try std.testing.expectEqualStrings("(\"id\" < 100)", computed.outputs[0].source.pushdown);
}

test "analyze: a condition in ON on a connection's table on the right is the WHERE that side sends" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = Diag{};
    const plan = try analyze(a, try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS SELECT o.id, c.name FROM pg.orders AS o INNER JOIN pg.customers AS c ON 1 = 1 AND c.active = 1 AND c.deleted <> '*' AND c.cid = o.cid;
    ), &diag);
    var join: ?Stage = null;
    for (plan.outputs[0].stages) |st| {
        if (std.mem.eql(u8, st.kind, "join")) join = st;
    }
    try std.testing.expect(std.mem.indexOf(u8, join.?.right_pushdown, "(\"active\" = 1)") != null);
    try std.testing.expect(std.mem.indexOf(u8, join.?.right_pushdown, "text comparisons decided by the collation at run time") != null);
}

test "analyze pushdown preview: raw PUSHDOWN AND-ed with the translated filter" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS
        \\SELECT filial FROM erp.dbo.T PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
        \\WHERE valor > 0 AND status = 'ok';
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqualStrings(
        "(D_E_L_E_T_ <> '*') AND ((([valor] > 0) AND ([status] = 'ok')))",
        plan.outputs[0].source.pushdown,
    );
}

test "analyze pushdown preview: an untranslatable filter is not pushed (stays engine-side)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS SELECT * FROM pg.t WHERE amount + 1 > 5;
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqualStrings("", plan.outputs[0].source.pushdown);
}

test "type flow fills out_schema for resolved sources" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT id, CAST(amount AS INT) * 2 AS d FROM '{s}';", .{in});
    var diag = Diag{};
    const plan = try analyze(a, try parse(a, src), &diag);
    const sel = plan.outputs[0].stages[0];
    try std.testing.expect(sel.out_schema != null);
    try std.testing.expectEqual(@as(usize, 2), sel.out_schema.?.fields.len);
    try std.testing.expectEqual(types.TypeKind.int, sel.out_schema.?.fields[1].ty.kind);
}

test "type flow catches a type error in an expression" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,name\n1,x\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT * FROM '{s}' WHERE NOT name;", .{in});
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), &diag));
    try std.testing.expectEqualStrings("`not` needs a bool operand", diag.msg);
}

test "analyze rejects unknown connection" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a, "LOAD INTO '/tmp/x.csv' AS SELECT * FROM nope.t;");
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "unknown connection") != null);
}

test "analyze checks the stages after a join against the joined schema" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const csv_data = "id,name,amount\n1,x,10\n";

    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, csv_data, "WITH r AS (SELECT id AS rid, name AS rname FROM '$IN') SELECT SUM(CAST(nope AS INT)) AS x FROM '$IN' JOIN r ON id = rid", &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "nope") != null);

    diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, csv_data, "WITH r AS (SELECT id AS rid, name AS rname FROM '$IN') SELECT id FROM '$IN' JOIN r ON id = rid WHERE missing = 'x'", &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "missing") != null);

    diag = Diag{};
    const plan = try analyzeCsv(a, csv_data, "WITH r AS (SELECT id AS rid, name AS rname FROM '$IN') SELECT rname, SUM(CAST(amount AS INT)) AS total FROM '$IN' JOIN r ON id = rid WHERE rname <> '' GROUP BY rname", &diag);
    const stages = plan.outputs[0].stages;
    var join_schema: ?types.Schema = null;
    for (stages) |st| {
        if (std.mem.eql(u8, st.kind, "join")) join_schema = st.out_schema;
    }
    const js = join_schema orelse return error.TestUnexpectedResult;
    try std.testing.expect(js.indexOf("rname") != null);
    try std.testing.expect(js.indexOf("amount") != null);
    try std.testing.expect(stages[stages.len - 1].out_schema != null);
}

test "analyze rejects a program with nothing to run" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a, "PARAM x INT DEFAULT 1;");
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, prog, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "nothing to run") != null);
}

test "analyze rejects `* rename` onto a duplicate column name" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try expectAnalyzeErr(ar.allocator(), "id,name\n1,x\n", "SELECT * RENAME (id AS name) FROM '$IN'", "`* rename` produces duplicate column `name`");
}

test "analyze rejects `is empty` on a non-string operand" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try expectAnalyzeErr(ar.allocator(), "id,amount\n1,100\n", "SELECT * FROM '$IN' WHERE CAST(amount AS INT) IS EMPTY", "`is empty` needs a string operand (got int)");
}

test "analyze: an undeclared `$name` is refused by name, never read as the column it spells" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, "id,tag\n1,x\n", "SELECT $tag AS t FROM '$IN'", &diag));
    try std.testing.expectEqualStrings("unknown `$tag`: no PARAM, LET or loop variable of that name", diag.msg);
    var grouped = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, "id,g\n1,x\n", "SELECT $tag AS t, g, COUNT(*) AS n FROM '$IN' GROUP BY g", &grouped));
    try std.testing.expectEqualStrings("unknown `$tag`: no PARAM, LET or loop variable of that name", grouped.msg);
    const conn = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', user = 'u', password = 'p', database = 'd');\n";
    var sql_diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, conn ++ "LOAD INTO '/tmp/x.csv' AS SELECT id FROM pg.orders WHERE day >= $since;"), &sql_diag));
    try std.testing.expectEqualStrings("unknown `$since`: no PARAM, LET or loop variable of that name", sql_diag.msg);
    var ok = Diag{};
    _ = try analyze(a, try parse(a, conn ++
        \\PARAM since DATE DEFAULT '2026-01-01';
        \\LET hi = (SELECT max(id) AS m FROM pg.orders);
        \\LOAD INTO '/tmp/x.csv' AS SELECT id, $since AS s FROM pg.orders WHERE day >= $since AND id <= $hi;
        \\FOR EACH ROW OF (SELECT 'a' AS r) AS (r)
        \\  LOAD INTO '/tmp/y.csv' AS SELECT id FROM pg.orders WHERE region = $r;
        \\END FOR;
    ), &ok);
}

test "analyze: a numeric aggregate refuses a non-numeric argument at plan time" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyzeCsv(a, "id,d\n1,2024-01-01\n", "SELECT SUM(d) AS s FROM '$IN'", &diag));
    try std.testing.expectEqualStrings("`sum` needs a numeric argument, got date", diag.msg);
    var ok = Diag{};
    _ = try analyzeCsv(a, "id,s\n1,x\n", "SELECT SUM(s) AS n, MIN(s) AS lo FROM '$IN'", &ok);
}

test "analyze rejects `?.` safe navigation on a plain column reference" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try expectAnalyzeErr(ar.allocator(), "id,name\n1,x\n", "SELECT name?.foo AS v FROM '$IN'", "`?.` (safe navigation) only applies to JSON-param paths");
}

test "check accepts a filter over a statement-level LET (and rejects a LET/PARAM clash)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const src = try std.fmt.allocPrint(a,
        \\PARAM floor INT DEFAULT 10;
        \\LET cutoff = $floor * 5;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}' WHERE CAST(amount AS INT) >= $cutoff;
    , .{in});
    const prog = try parse(a, src);
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);
    try std.testing.expectEqualStrings("filter", plan.outputs[0].stages[0].kind);
    const clash = try std.fmt.allocPrint(a,
        \\PARAM cutoff INT DEFAULT 10;
        \\LET cutoff = 5;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    const cprog = try parse(a, clash);
    var cdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, cprog, &cdiag));
    try std.testing.expect(std.mem.indexOf(u8, cdiag.msg, "declared twice") != null);
}

test "check rejects a script whose THROW guard fires, and passes one whose WHEN is false" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,amount\n1,100\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const fires = try std.fmt.allocPrint(a,
        \\PARAM tbl STRING DEFAULT '';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    var fdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, fires), &fdiag));
    try std.testing.expectEqualStrings("tbl is required (e.g. -p tbl=SC5)", fdiag.msg);

    const holds = try std.fmt.allocPrint(a,
        \\PARAM tbl STRING DEFAULT 'SC5';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    var hdiag = Diag{};
    const plan = try analyze(a, try parse(a, holds), &hdiag);
    try std.testing.expectEqual(@as(usize, 1), plan.outputs.len);

    const bare = try std.fmt.allocPrint(a,
        \\LET tag = 'zz';
        \\THROW 'unreachable branch: ' || $tag;
        \\LOAD INTO '/tmp/x.csv' AS SELECT id FROM '{s}';
    , .{in});
    var bdiag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, bare), &bdiag));
    try std.testing.expectEqualStrings("unreachable branch: zz", bdiag.msg);
}

test "analyze: an unknown name is underlined where it is written, not at its statement" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { src: []const u8, line: u32, col: u32, end_col: u32, msg: []const u8 = "" };
    const cases = [_]Case{
        .{ .src = "LOAD INTO '/tmp/x.csv' AS\nSELECT range\nFROM RANGE(3)\nWHERE nosuch > 1;", .line = 4, .col = 7, .end_col = 13, .msg = "unknown field `nosuch`" },
        .{ .src = "SELECT nope FROM RANGE(3);", .line = 1, .col = 8, .end_col = 12 },
        .{ .src = "SELECT range,\n       upper(nope) AS u\nFROM RANGE(3);", .line = 2, .col = 14, .end_col = 18 },
        .{ .src = "SELECT frobnicate(range) AS f FROM RANGE(3);", .line = 1, .col = 8, .end_col = 18 },
        .{ .src = "SELECT substr(range) AS s FROM RANGE(3);", .line = 1, .col = 8, .end_col = 14 },
        .{ .src = "SELECT range FROM RANGE(3) ORDER BY zz;", .line = 1, .col = 37, .end_col = 39 },
        .{ .src = "SELECT \"no such\" FROM RANGE(3);", .line = 1, .col = 8, .end_col = 17 },
    };
    for (cases) |c| {
        var diag = Diag{};
        try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, c.src), &diag));
        errdefer std.debug.print("case: {s} -> {s} at {?}..{?}\n", .{ c.src, diag.msg, diag.pos, diag.end });
        try std.testing.expectEqual(c.line, diag.pos.?.line);
        try std.testing.expectEqual(c.col, diag.pos.?.col);
        try std.testing.expectEqual(c.line, diag.end.?.line);
        try std.testing.expectEqual(c.end_col, diag.end.?.col);
        if (c.msg.len > 0) try std.testing.expectEqualStrings(c.msg, diag.msg);
    }
}

test "analyze: string builtins refuse a non-INT position but coerce scalars to text" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,name\n1,ann\n" });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });

    const bad = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT substr(id, 'a', 'b') AS s FROM '{s}';", .{in});
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, bad), &diag));
    try std.testing.expectEqualStrings("`substr` start must be an INT, got string", diag.msg);

    const ok = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS SELECT concat(id, '-', name) AS c, lpad(id, 5, '0') AS p, length(id) AS n FROM '{s}';", .{in});
    var diag2 = Diag{};
    _ = try analyze(a, try parse(a, ok), &diag2);
}

test "analyze: CREATE FUNCTION cannot shadow a builtin or an aggregate" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    inline for (.{ "upper", "sum" }) |name| {
        const src = try std.fmt.allocPrint(a, "CREATE FUNCTION {s}(x) AS 999; SELECT 1 AS a;", .{name});
        var diag = Diag{};
        try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), &diag));
        try std.testing.expectEqualStrings("`" ++ name ++ "` is a built-in function and cannot be redefined", diag.msg);
    }
}

test "analyze: a window stage resolves its schema — the input plus one typed column per function" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parse(a,
        \\LOAD INTO '/tmp/x.csv' AS
        \\SELECT y, LAG(rev) OVER (ORDER BY y) AS prev, SUM(rev) OVER (ORDER BY y) AS run
        \\FROM (SELECT 'a' AS y, CAST(1 AS DECIMAL(10,2)) AS rev) m;
    );
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    var found = false;
    for (plan.outputs[0].stages) |st| {
        if (!std.mem.eql(u8, st.kind, "window")) continue;
        found = true;
        const s = st.out_schema orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(usize, 4), s.fields.len);
        try std.testing.expectEqualStrings("prev", s.fields[2].name);
        try std.testing.expect(s.fields[2].ty.kind == .decimal and s.fields[2].ty.nullable);
        try std.testing.expectEqualStrings("run", s.fields[3].name);
        try std.testing.expect(s.fields[3].ty.kind == .float and s.fields[3].ty.nullable);
    }
    try std.testing.expect(found);
}

test "a $param or LET in a filter reaches the pushdown as its value, never as a column" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE CONNECTION db TYPE sqlserver OPTIONS (host = 'h');
        \\LET c = 1;
        \\PARAM n INT DEFAULT 5;
        \\EXPLAIN SELECT * FROM db.dbo.t WHERE b >= $c AND k < $n;
    , &pdiag);
    var diag = Diag{};
    const plan = try analyze(a, prog, &diag);
    try std.testing.expectEqualStrings("(([b] >= 1) AND ([k] < 5))", plan.outputs[0].source.pushdown);

    var params = ParamMap.init(a);
    const five = try a.create(ast.Expr);
    five.* = .{ .int_lit = 5 };
    try params.put("n", five);
    const stages = prog.stmts[prog.stmts.len - 1].explain.pipeline.stages;
    const bound = try substFilterParams(a, stages, &params);
    try std.testing.expect(bound.ptr != stages.ptr);
    const sql = (try pushdown.serialWhere(a, .postgres, bound)).?;
    try std.testing.expect(std.mem.indexOf(u8, sql, "\"k\" < 5") != null);
    var none = ParamMap.init(a);
    try std.testing.expect((try substFilterParams(a, stages, &none)).ptr == stages.ptr);
}
