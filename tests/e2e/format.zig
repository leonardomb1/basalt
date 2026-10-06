//! End-to-end reads of each format: CSV dialects, zip members, Excel, parquet logical
//! and nested types, WAL buffers, and the explode functions.

const std = @import("std");
const basalt = @import("basalt");
const Wal = basalt.wal.Wal;
const parser = basalt.sql_parser;
const fixtures = basalt.fixtures;
const Diag = basalt.env.Diag;
const ParamArg = basalt.env.ParamArg;
const run = basalt.runtime.run;
const runToString = @import("harness.zig").runToString;
const runScript = @import("harness.zig").runScript;

test "read a semicolon latin-1 file end to end" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "SIT;N\r\nLIQUIDA\xC7\xC3O;2\r\nCANCELADA;1\r\n",
        "SELECT SIT, N FROM '$IN' WITH (delimiter = ';', encoding = 'latin1') ORDER BY N DESC",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("SIT,N\nLIQUIDAÇÃO,2\nCANCELADA,1\n", out);
}

test "an unreadable extension is refused by run, not just check" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.json", .data = "{\"a\":1}\n{\"a\":2}\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.json" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT COUNT(*) AS c FROM '{s}';", .{ out_path, in_path });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "cannot read") != null);
}

test "WITH (format = 'csv') reads a file whose extension says nothing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.dat", .data = "id\n1\n2\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.dat" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT id FROM '{s}' WITH (format = 'csv');", .{ out_path, in_path });
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id\n1\n2\n", out);
}

const fx_zip = fixtures.two_members_zip;

test "read a zip member end to end with the :: reference" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = fx_zip });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT id, v FROM '{s}/t.zip :: a.csv' ORDER BY id;",
        .{ out_path, base },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,v\n1,x\n2,y\n", out);
}

test "a zip holding several files refuses to guess which one" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = fx_zip });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT * FROM '{s}/t.zip';", .{ out_path, base });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "a.csv") != null);
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "b.csv") != null);
}

test "explode splits a delimited column into rows" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const out = try runToString(
        alloc,
        &tmp,
        "id,tags\n1,\"a,b,c\"\n2,x\n3,\n",
        "SELECT * FROM '$IN' CROSS JOIN UNNEST(tags) AS tag",
    );
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,b\n1,c\n2,x\n", out);
}

test "JSON_EACH explodes a JSON array; json_get walks keys and indexes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const in =
        \\id,doc
        \\1,"{""name"":{""common"":""Brazil""},""capital"":[""Brasília""],""tags"":[""a"",1,null,{""k"":true}]}"
        \\2,"{""name"":{},""tags"":[]}"
        \\3,"{""tags"":null}"
        \\
    ;
    const got = try runToString(alloc, &tmp, in,
        \\SELECT id, json_get(doc, 'name.common') AS name, json_get(doc, 'capital[0]') AS cap, json_get(doc, '$.capital.0') AS cap2
        \\FROM '$IN'
    );
    defer alloc.free(got);
    try std.testing.expectEqualStrings("id,name,cap,cap2\n1,Brazil,Brasília,Brasília\n2,,,\n3,,,\n", got);

    var tmp2 = std.testing.tmpDir(.{});
    defer tmp2.cleanup();
    const tags = try runToString(alloc, &tmp2, in,
        \\SELECT id, tag FROM (SELECT id, json_get(doc, 'tags') AS tags FROM '$IN')
        \\CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag
    );
    defer alloc.free(tags);
    try std.testing.expectEqualStrings("id,tag\n1,a\n1,1\n1,\n1,\"{\"\"k\"\":true}\"\n", tags);
}

test "FROM BUFFER replays WAL segments as a source (batch mode)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const wal_dir = try std.fs.path.join(alloc, &.{ base, "wal" });
    defer alloc.free(wal_dir);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    {
        var w = try Wal.open(alloc, wal_dir, "ev", 1 << 20);
        defer w.close();
        try w.append("{\"device_id\":\"a\",\"v\":1}");
        try w.append("{\"device_id\":\"b\",\"v\":2}");
        try w.rotate();
        try w.append("{\"device_id\":\"c\",\"v\":3}");
        try w.sync();
    }

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT device_id, CAST(v AS INT) AS v FROM BUFFER 'ev' AT '{s}';",
        .{ out_path, wal_dir },
    );
    defer alloc.free(script);

    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("device_id,v\na,1\nb,2\nc,3\n", out);
}

test "an Excel workbook reads as a table: typed columns, a named sheet and range, and no writing one" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "f.xlsx", .data = fixtures.openpyxl_xlsx });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const cases = [_]struct { q: []const u8, want: []const u8 }{
        .{ .q = "SELECT id, nome, dia FROM '$X' WHERE dia >= '2024-01-01' AND valor > 0 ORDER BY id", .want = "id,nome,dia\n1,São Paulo,2026-10-03\n" },
        .{ .q = "SELECT codigo, qtd * 2 AS q2 FROM '$X' WITH (sheet = 'Notas', range = 'A3:B5')", .want = "codigo,q2\nx1,10\nx2,14\n" },
    };
    for (cases) |c| {
        const xp = try std.fs.path.join(alloc, &.{ base, "f.xlsx" });
        defer alloc.free(xp);
        const q = try std.mem.replaceOwned(u8, alloc, c.q, "$X", xp);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
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
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const w = try std.fmt.allocPrint(parena.allocator(), "LOAD INTO '{s}/out.xlsx' AS SELECT 1 AS a;", .{base});
    const prog = try parser.parseSource(parena.allocator(), w, &pdiag);
    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, run(alloc, prog, .{}, &rdiag));
    try std.testing.expect(std.mem.indexOf(u8, rdiag.msg, "does not write them") != null);
}

test "csv: a column of ISO dates is inferred as DATE (empty cells are null)" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const got = try runToString(alloc, &tmp, "d\n2026-01-31\n\n2026-02-01\n", "SELECT date_add('day', 1, d) AS n FROM '$IN' WHERE d >= '2026-01-01'");
    defer alloc.free(got);
    try std.testing.expectEqualStrings("n\n2026-02-01\n2026-02-02\n", got);
}

const fx_logical = fixtures.logical_types_parquet;

test "LogicalType-only parquet timestamps and times read back as wall-clock values" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "lt.parquet", .data = fx_logical });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT id, ts, ts_utc, t, date_trunc('hour', ts) AS h FROM '{s}/lt.parquet' ORDER BY id;",
        .{ out_path, base },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings(
        "id,ts,ts_utc,t,h\n" ++
            "1,2025-12-31 23:59:59.999999,2025-06-01 08:30:00,01:02:00,2025-12-31 23:00:00\n" ++
            "2,2026-01-01 12:00:00,2026-01-01 12:00:00.123456,23:59:59.999999,2026-01-01 12:00:00\n" ++
            "3,,2026-03-01 00:00:00,00:00:00,\n" ++
            "4,1969-12-31 23:59:59.500000,,,1969-12-31 23:00:00\n",
        out,
    );
}

test "a parquet LIST column unnests through JSON_EACH, one row per element" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "l.parquet", .data = fixtures.lists_v2_parquet });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}' AS SELECT id, x FROM '{s}/l.parquet' CROSS JOIN UNNEST(JSON_EACH(xs)) AS x ORDER BY id;",
        .{ out_path, base },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings("id,x\n1,1\n1,2\n4,\n4,5\n", out);
}

test "copying a parquet with nested columns keeps every one of them" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "src.parquet", .data = fixtures.lists_v1_parquet });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const script = try std.fmt.allocPrint(
        alloc,
        "LOAD INTO '{s}/copy.parquet' AS SELECT * FROM '{s}/src.parquet';\nLOAD INTO '{s}' AS SELECT id, recs, m FROM '{s}/copy.parquet' ORDER BY id;",
        .{ base, base, out_path, base },
    );
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings(
        "id,recs,m\n" ++
            "1,\"[{\"\"a\"\":1,\"\"b\"\":\"\"x\"\"}]\",\"{\"\"k\"\":1}\"\n" ++
            "2,,\n" ++
            "3,[],{}\n" ++
            "4,\"[{\"\"a\"\":2,\"\"b\"\":\"\"y\"\"}]\",\"{\"\"z\"\":2}\"\n",
        out,
    );
}
