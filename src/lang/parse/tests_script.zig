//! Parser tests for scripts: statements, EXPLAIN and DESCRIBE, FOR EACH discovery,
//! functions, THROW and PRINT, IDENTIFIER and PUSHDOWN lowering, connections and
//! error recovery.

const ast = @import("../ast.zig");
const Diagnostic = @import("../sql_parser.zig").Diagnostic;
const firstPipelineStages = @import("testing_util.zig").firstPipelineStages;
const hintStr = @import("testing_util.zig").hintStr;
const parseSource = @import("../sql_parser.zig").parseSource;
const parseSourceOpts = @import("../sql_parser.zig").parseSourceOpts;
const parseTest = @import("testing_util.zig").parseTest;
const std = @import("std");
const testing = std.testing;
const types = @import("../types.zig");

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
