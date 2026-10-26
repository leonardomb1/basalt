//! End-to-end reads of each format: CSV dialects, zip members, Excel (and writing
//! it), parquet logical and nested types, and the explode functions.

const std = @import("std");
const basalt = @import("basalt");
const fixtures = basalt.fixtures;
const Diag = basalt.env.Diag;
const ParamArg = basalt.env.ParamArg;
const runToString = @import("harness.zig").runToString;
const exec = @import("harness.zig").exec;
const runScript = @import("harness.zig").runScript;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;
const expectRunRefuses = @import("harness.zig").expectRunRefuses;
const checkAndRun = @import("harness.zig").checkAndRun;

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

test "a CSV cell that is not UTF-8 fails the run naming file, line and column, serial and in lanes; latin-1 reads it" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var input = std.array_list.Managed(u8).init(alloc);
    defer input.deinit();
    try input.appendSlice("id,cidade\n");
    for (0..5000) |i| try input.writer().print("{d},S\u{e3}o Paulo \u{20ac}\n", .{i});
    try tmp.dir.writeFile(.{ .sub_path = "ok.csv", .data = input.items });
    try input.appendSlice("5000,Bel\xe9m\n");
    try tmp.dir.writeFile(.{ .sub_path = "bad.csv", .data = input.items });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT COUNT(*) AS n, MAX(cidade) AS c FROM '$B/ok.csv';", 4, &.{});
    try @import("harness.zig").expectFile(&tmp, "out.csv", "n,c\n5000,S\u{e3}o Paulo \u{20ac}\n");
    const want = "bad.csv` line 5002, column `cidade`: not valid UTF-8 (byte 0xe9 at offset 3); read it WITH (encoding = 'latin1') or 'cp1252' if it is not UTF-8";
    for ([_]usize{ 1, 4 }) |threads|
        try expectRunRefuses(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT COUNT(*) AS n, MAX(cidade) AS c FROM '$B/bad.csv';", threads, want);
    try tmp.dir.writeFile(.{ .sub_path = "l1.csv", .data = "id,cidade\n1,Bel\xe9m\n" });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT cidade FROM '$B/l1.csv' WITH (encoding = 'latin1');", 1, &.{});
    try @import("harness.zig").expectFile(&tmp, "out.csv", "cidade\nBel\u{e9}m\n");
}

test "an unreadable extension is refused by run, not just check" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.yaml", .data = "<a>1</a>\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.yaml" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS SELECT COUNT(*) AS c FROM '{s}';", .{ out_path, in_path });
    defer alloc.free(script);

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, exec(alloc, script, .{}, &rdiag));
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

    var rdiag: Diag = .{};
    try std.testing.expectError(error.PlanFailed, exec(alloc, script, .{}, &rdiag));
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
        var rdiag: Diag = .{};
        _ = try exec(alloc, script, .{}, &rdiag);
        const got = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
        defer alloc.free(got);
        try std.testing.expectEqualStrings(c.want, got);
    }
}

test "an Excel workbook is written and reads back typed, a named sheet, Excel's limits refused" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,s,d,ts,t,big,dec\n" ++
        "1,\"a,b \"\"q\"\"\",2024-02-29,2024-02-29 23:59:59.5,08:15:00,9007199254740993,12.5\n" ++
        "2,,1899-12-31,1970-01-01 00:00:00,23:59:59.999,-3,\n" ++
        "3,\"line\nbreak _x0041_\",9999-12-31,2026-10-16 12:00:00,00:00:00,4,0.01\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc,
        \\LOAD INTO '$B/out.xlsx' WITH (sheet = 'Vendas') AS
        \\SELECT id, s, CAST(d AS DATE) AS d, CAST(ts AS TIMESTAMP) AS ts, CAST(t AS TIME) AS t, big, CAST(dec AS DECIMAL(10,2)) AS dec, id % 2 = 0 AS even FROM '$B/in.csv';
        \\LOAD INTO '$B/out.csv' AS SELECT * FROM '$B/out.xlsx' WITH (sheet = 'Vendas');
    , "$B", base);
    defer alloc.free(script);
    const out = try runScript(alloc, &tmp, script, &.{});
    defer alloc.free(out);
    try std.testing.expectEqualStrings(
        "id,s,d,ts,t,big,dec,even\n" ++
            "1,\"a,b \"\"q\"\"\",2024-02-29,2024-02-29 23:59:59.500000,08:15:00,9007199254740993,12.5,false\n" ++
            "2,,1899-12-31,1970-01-01 00:00:00,23:59:59.999000,-3,,true\n" ++
            "3,\"line\nbreak _x0041_\",9999-12-31,2026-10-16 12:00:00,00:00:00,4,0.01,false\n",
        out,
    );

    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xlsx' WITH (sheet = 'a/b') AS SELECT 1 AS a;", "cannot hold any of");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xlsx' WITH (sheet = 3) AS SELECT 1 AS a;", "quoted worksheet name");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xlsx' WITH (delimiter = ';') AS SELECT 1 AS a;", "does not apply to an Excel file");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.csv' WITH (sheet = 'x') AS SELECT 1 AS a;", "`sheet` is an option of an Excel file");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xlsx' APPEND AS SELECT 1 AS a;", "an Excel workbook is a zip archive");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xlsx.gz' AS SELECT 1 AS a;", "written without a `.gz`");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xlsm' AS SELECT 1 AS a;", "macro-enabled `.xlsm`");

    const named = try std.mem.replaceOwned(u8, alloc,
        \\LOAD INTO '$B/o.dat' WITH (format = 'xlsx', sheet = 'Dados') AS SELECT id, s FROM '$B/in.csv' WHERE id = 1;
        \\LOAD INTO '$B/out.csv' AS SELECT * FROM '$B/o.dat' WITH (format = 'xlsx', sheet = 'Dados');
    , "$B", base);
    defer alloc.free(named);
    const back = try runScript(alloc, &tmp, named, &.{});
    defer alloc.free(back);
    try std.testing.expectEqualStrings("id,s\n1,\"a,b \"\"q\"\"\"\n", back);
}

test "an Excel sheet takes 1,048,575 data rows under its header and refuses the next" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const cases = [_]struct { n: []const u8, ok: bool }{ .{ .n = "1048575", .ok = true }, .{ .n = "1048576", .ok = false } };
    for (cases) |c| {
        var parena = std.heap.ArenaAllocator.init(alloc);
        defer parena.deinit();
        const script = try std.fmt.allocPrint(parena.allocator(), "LOAD INTO '{s}/big.xlsx' AS SELECT range AS x FROM range(0, {s});", .{ base, c.n });
        var rdiag: Diag = .{};
        const got = exec(alloc, script, .{ .log = .{ .quiet = true } }, &rdiag);
        if (c.ok) {
            _ = try got;
            const check = try std.fmt.allocPrint(parena.allocator(), "LOAD INTO '{s}/n.csv' AS SELECT COUNT(*) AS n, MAX(x) AS hi FROM '{s}/big.xlsx';", .{ base, base });
            var rd2: Diag = .{};
            _ = try exec(alloc, check, .{ .log = .{ .quiet = true } }, &rd2);
            const n = try tmp.dir.readFileAlloc(alloc, "n.csv", 1 << 10);
            defer alloc.free(n);
            try std.testing.expectEqualStrings("n,hi\n1048575,1048574\n", n);
        } else {
            try std.testing.expectError(error.XlsxTooManyRows, got);
            try std.testing.expect(std.mem.indexOf(u8, basalt.runtime.failLabel(error.XlsxTooManyRows), "1048575 data rows") != null);
        }
    }
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

test "a parquet UINT64 is DECIMAL(20,0): it joins and compares against an INT, and keeps a value past 2^63-1" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "u.parquet", .data = fixtures.unsigned_parquet });
    try tmp.dir.writeFile(.{ .sub_path = "b.parquet", .data = fixtures.unsigned_big_parquet });
    try tmp.dir.writeFile(.{ .sub_path = "k.csv", .data = "k,tag\n4,four\n9223372036854775807,max\n5,five\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const Case = struct { q: []const u8, want: []const u8 };
    const cases = comptime [_]Case{
        .{
            .q = "SELECT tag, COUNT(*) AS n FROM '{0s}/u.parquet' JOIN '{0s}/k.csv' ON u64 = k GROUP BY tag ORDER BY tag",
            .want = "tag,n\nfour,2\nmax,1\n",
        },
        .{
            .q = "SELECT u64 FROM '{0s}/u.parquet' WHERE u64 = 4 AND u64 < 9223372036854775807 ORDER BY u32",
            .want = "u64\n4\n4\n",
        },
        .{
            .q = "SELECT big, big > 9223372036854775807 AS past, big + 0 AS same FROM '{0s}/b.parquet' WHERE big > 9223372036854775807",
            .want = "big,past,same\n18446744073709551615,true,18446744073709551615\n",
        },
    };
    inline for (cases) |c| {
        const q = try std.fmt.allocPrint(alloc, c.q, .{base});
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
        defer alloc.free(script);
        const out = try runScript(alloc, &tmp, script, &[_]ParamArg{});
        defer alloc.free(out);
        try std.testing.expectEqualStrings(c.want, out);
    }
}

test "a parquet folder mixing a UINT64 and an INT64 file is refused naming the retyped column, in either order" {
    const alloc = std.testing.allocator;
    const Order = struct { uint: []const u8, int: []const u8, why: []const u8 };
    const orders = [_]Order{
        .{ .uint = "a.parquet", .int = "b.parquet", .why = "column `big` is int there, not decimal(20,0)" },
        .{ .uint = "b.parquet", .int = "a.parquet", .why = "column `big` is decimal(20,0) there, not int" },
    };
    for (orders) |o| for ([_][]const u8{ "SELECT big FROM '$B/d/'", "SELECT SUM(big) AS s FROM '$B/d/'" }) |sel| for ([_]usize{ 1, 4 }) |threads| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.makeDir("d");
        const upath = try std.fmt.allocPrint(alloc, "d/{s}", .{o.uint});
        defer alloc.free(upath);
        try tmp.dir.writeFile(.{ .sub_path = upath, .data = fixtures.unsigned_big_parquet });
        const base = try tmp.dir.realpathAlloc(alloc, ".");
        defer alloc.free(base);
        const mk = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/d/{s}' AS SELECT CAST(7 AS INT) AS big;", .{ base, o.int });
        defer alloc.free(mk);
        var mdiag: Diag = .{};
        _ = try exec(alloc, mk, .{ .log = .{ .quiet = true } }, &mdiag);
        const q = try std.mem.replaceOwned(u8, alloc, sel, "$B", base);
        defer alloc.free(q);
        const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS {s};", .{ base, q });
        defer alloc.free(script);
        var rdiag: Diag = .{};
        const msg = if (exec(alloc, script, .{ .threads = threads, .log = .{ .quiet = true } }, &rdiag)) |_|
            return error.TestExpectedError
        else |e| if (rdiag.msg.len > 0) rdiag.msg else basalt.eval.takeFailure(e) orelse "";
        const want = try std.fmt.allocPrint(alloc, "`b.parquet` in folder `{s}/d/` does not match `a.parquet`, its first file: {s}", .{ base, o.why });
        defer alloc.free(want);
        if (std.mem.indexOf(u8, msg, want) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ want, msg });
            return error.TestUnexpectedResult;
        }
    };
}

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

test "a set operation writes a NULL from any branch into parquet, the column nullable when one branch's is" {
    const alloc = std.testing.allocator;
    const script =
        \\LOAD INTO '$B/last.parquet' AS SELECT 1 AS k, unhex('') AS b UNION ALL SELECT 2, CAST(NULL AS BYTES);
        \\LOAD INTO '$B/first.parquet' AS SELECT 1 AS k, CAST(NULL AS BYTES) AS b UNION ALL SELECT 2, unhex('ff');
        \\LOAD INTO '$B/mid.parquet' AS SELECT 1 AS k, unhex('01') AS b UNION ALL SELECT 2, CAST(NULL AS BYTES) UNION ALL SELECT 3, unhex('02');
        \\LOAD INTO '$B/except.parquet' AS SELECT 1 AS k, CAST(NULL AS INT) AS v EXCEPT SELECT 2, 7;
        \\LOAD INTO '$B/out.csv' AS
        \\  SELECT * FROM (SELECT 'last' AS f, k, hex(b) AS h, b IS NULL AS n FROM '$B/last.parquet'
        \\  UNION ALL SELECT 'first', k, hex(b), b IS NULL FROM '$B/first.parquet'
        \\  UNION ALL SELECT 'mid', k, hex(b), b IS NULL FROM '$B/mid.parquet'
        \\  UNION ALL SELECT 'except', k, CAST(v AS STRING), v IS NULL FROM '$B/except.parquet') t ORDER BY f, k;
    ;
    for ([_]usize{ 1, 4 }) |threads| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try checkAndRun(alloc, &tmp, script, threads, &.{});
        try @import("harness.zig").expectFile(&tmp, "out.csv", "f,k,h,n\n" ++
            "except,1,,true\n" ++
            "first,1,,true\n" ++
            "first,2,FF,false\n" ++
            "last,1,\"\",false\n" ++
            "last,2,,true\n" ++
            "mid,1,01,false\n" ++
            "mid,2,,true\n" ++
            "mid,3,02,false\n");
    }
}

/// A stored zip whose one member, `p.parquet`, holds `x`: the refusal comes from the
/// member's name, before anything reads it.
const parquet_member_zip = "\x50\x4b\x03\x04\x14\x00\x00\x00\x00\x00\x00\x00\x21\x5c\x83\x16\xdc\x8c\x01\x00\x00\x00\x01\x00\x00\x00\x09\x00\x00\x00\x70\x2e\x70\x61\x72\x71\x75\x65\x74\x78\x50\x4b\x01\x02\x14\x03\x14\x00\x00\x00\x00\x00\x00\x00\x21\x5c\x83\x16\xdc\x8c\x01\x00\x00\x00\x01\x00\x00\x00\x09\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01\x00\x00\x00\x00\x70\x2e\x70\x61\x72\x71\x75\x65\x74\x50\x4b\x05\x06\x00\x00\x00\x00\x01\x00\x01\x00\x37\x00\x00\x00\x28\x00\x00\x00\x00\x00";

test "a parquet inside a zip is refused by run as by check" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "pz.zip", .data = parquet_member_zip });
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.csv' AS SELECT * FROM '$B/pz.zip :: p.parquet';", "parquet needs random access");
}

test "a CSV header's quoted names lose their quotes, a delimiter inside one included, serial or in lanes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "q.csv", .data = "\"Valor Total\",\"a,b\",c\n1,2,3\n4,5,6\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}/out.csv' AS SELECT \"Valor Total\" AS v, \"a,b\" AS ab FROM '{s}/q.csv' ORDER BY v;", .{ base, base });
    defer alloc.free(script);
    for ([_]usize{ 1, 4 }) |threads| {
        var rdiag: Diag = .{};
        _ = try exec(alloc, script, .{ .threads = threads, .log = .{ .quiet = true } }, &rdiag);
        const got = try tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 16);
        defer alloc.free(got);
        try std.testing.expectEqualStrings("v,ab\n1,2\n4,5\n", got);
    }
}
