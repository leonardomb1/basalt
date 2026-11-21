//! End-to-end DELETE, UPDATE and INSERT on single files: a CSV or NDJSON file keeps
//! every byte outside the records changed, an Excel sheet every row it keeps and
//! every other part of its workbook, any other format is written again whole; and
//! what a change to a file refuses.

const std = @import("std");
const basalt = @import("basalt");
const harness = @import("harness.zig");

const testing = std.testing;
const gpa = testing.allocator;

fn load(tmp: *testing.TmpDir, script: []const u8) !void {
    try harness.checkAndRun(gpa, tmp, script, 1, &.{});
}

fn rows(a: std.mem.Allocator, tmp: *testing.TmpDir, query: []const u8) ![]const u8 {
    try load(tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/out.csv' AS {s};", .{query}));
    return tmp.dir.readFileAlloc(a, "out.csv", 1 << 20);
}

test "DELETE and UPDATE of a CSV file keep every byte outside the records they change: BOM, CRLF, Latin-1, a quoted line break, a blank line" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "\xef\xbb\xbfid;nome;obs\r\n1;\"Ana\";\"linha\r\ncom quebra\"\r\n\r\n2;Jo\xe3o;x\r\n3;\"Caio \"\"C\"\"\";\r\n4;Z\xe9;fim" });
    try load(&tmp, "DELETE FROM '$B/c.csv' WITH (delimiter = ';', encoding = 'latin1') WHERE id = 1;");
    try load(&tmp, "UPDATE '$B/c.csv' WITH (delimiter = ';', encoding = 'latin1') SET obs = 'um;dois', nome = nome || '\u{00e7}' WHERE id = 2;");
    try harness.expectFile(&tmp, "c.csv", "\xef\xbb\xbfid;nome;obs\r\n\r\n2;Jo\xe3o\xe7;\"um;dois\"\r\n3;\"Caio \"\"C\"\"\";\r\n4;Z\xe9;fim");
    try load(&tmp, "DELETE FROM '$B/c.csv' WITH (delimiter = ';', encoding = 'latin1') WHERE id = 99;");
    try harness.expectFile(&tmp, "c.csv", "\xef\xbb\xbfid;nome;obs\r\n\r\n2;Jo\xe3o\xe7;\"um;dois\"\r\n3;\"Caio \"\"C\"\"\";\r\n4;Z\xe9;fim");
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "id,v\n2,x\n3,y\n" });
    try tmp.dir.writeFile(.{ .sub_path = "dup.csv", .data = "id,v\n2,x\n2,y\n" });
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id,nome\n1,a\n2,b\n3,c\n" });
    try load(&tmp, "UPDATE '$B/t.csv' t SET nome = s.v FROM '$B/s.csv' s WHERE t.id = s.id AND t.id NOT IN (SELECT id FROM '$B/s.csv' WHERE v = 'y');");
    try harness.expectFile(&tmp, "t.csv", "id,nome\n1,a\n2,x\n3,c\n");
    try harness.expectRunRefuses(gpa, &tmp, "UPDATE '$B/t.csv' t SET nome = s.v FROM '$B/dup.csv' s WHERE t.id = s.id;", 1, "a row matches more than one row of the FROM source");
    try harness.expectFile(&tmp, "t.csv", "id,nome\n1,a\n2,x\n3,c\n");
}

test "UPDATE of a CSV file writes only the fields its SET names: a FLOAT keeps all its digits, a decimal its trailing zero, a quoted field its quotes" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "u.csv", .data = "id,status,f,big,note\r\n1,a,0.1234567890123456789,12345678901234567890,\"x, y\"\r\n2,b,21168.20,1,\"\"\r\n3,c,1e5,2,plain" });
    try load(&tmp, "UPDATE '$B/u.csv' SET status = 'x, z' WHERE id <= 2;");
    try harness.expectFile(&tmp, "u.csv", "id,status,f,big,note\r\n1,\"x, z\",0.1234567890123456789,12345678901234567890,\"x, y\"\r\n2,\"x, z\",21168.20,1,\"\"\r\n3,c,1e5,2,plain");
    try load(&tmp, "UPDATE '$B/u.csv' SET big = big + 1, status = 'q' WHERE id = 3;");
    try harness.expectFile(&tmp, "u.csv", "id,status,f,big,note\r\n1,\"x, z\",0.1234567890123456789,12345678901234567890,\"x, y\"\r\n2,\"x, z\",21168.20,1,\"\"\r\n3,q,1e5,3,plain");
}

test "DELETE and UPDATE of an NDJSON file: a deleted line goes with its indent and ending, an update changes only the keys it sets" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "n.ndjson", .data = "{\"id\":1,\"v\":\"a\"}\n  {\"v\":\"b\",\"id\":2}  \r\n\n{\"id\":3,\"v\":\"c\",\"x\":[1,2],\"w\":{\"k\":\"\\u00e9\"}}\n{\"id\":4}\n" });
    try load(&tmp, "DELETE FROM '$B/n.ndjson' WHERE id = 2;");
    try load(&tmp, "UPDATE '$B/n.ndjson' SET v = upper(v) WHERE id = 3;");
    try load(&tmp, "UPDATE '$B/n.ndjson' SET v = 'novo' WHERE id = 4;");
    try harness.expectFile(&tmp, "n.ndjson", "{\"id\":1,\"v\":\"a\"}\n\n{\"id\":3,\"v\":\"C\",\"x\":[1,2],\"w\":{\"k\":\"\\u00e9\"}}\n{\"id\":4,\"v\":\"novo\"}\n");
}

test "DELETE and UPDATE of a Parquet, Avro, Arrow or JSON-array file write it again whole, rows in their places" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "p.parquet", "a.avro", "r.arrow" }) |f| {
        try load(&tmp, try std.fmt.allocPrint(a, "LOAD INTO '$B/{s}' AS SELECT range AS id, 'v' || range AS v FROM range(4);", .{f}));
        try load(&tmp, try std.fmt.allocPrint(a, "DELETE FROM '$B/{s}' WHERE id = 1;", .{f}));
        try load(&tmp, try std.fmt.allocPrint(a, "UPDATE '$B/{s}' SET v = 'Z' WHERE id > 1;", .{f}));
        try testing.expectEqualStrings("id,v\n0,v0\n2,Z\n3,Z\n", try rows(a, &tmp, try std.fmt.allocPrint(a, "SELECT * FROM '$B/{s}'", .{f})));
    }
    try tmp.dir.writeFile(.{ .sub_path = "arr.json", .data = "[{\"id\":1,\"v\":\"a\"},\n {\"id\":2,\"v\":\"b\"}]\n" });
    try load(&tmp, "DELETE FROM '$B/arr.json' WHERE id = 1;");
    try harness.expectFile(&tmp, "arr.json", "[\n{\"id\":2,\"v\":\"b\"}\n]\n");
}

test "DELETE, UPDATE and INSERT of a Parquet file on lanes encode its row groups in parallel, rows in their places" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/big.parquet' AS SELECT range AS id, range % 7 AS g, 'r' || range AS v FROM range(300000);");
    try harness.checkAndRun(gpa, &tmp, "DELETE FROM '$B/big.parquet' WHERE g = 3;", 4, &.{});
    try harness.checkAndRun(gpa, &tmp, "UPDATE '$B/big.parquet' SET v = upper(v) WHERE id % 1000 = 0;", 4, &.{});
    try harness.checkAndRun(gpa, &tmp, "INSERT INTO '$B/big.parquet' SELECT 999999 AS id, 'z' AS v;", 4, &.{});
    try testing.expectEqualStrings("n,s,u,first,last\n257144,38572342856,257,R0,z\n", try rows(a, &tmp, "SELECT count(*) AS n, sum(id) AS s, count(CASE WHEN v LIKE 'R%' THEN 1 END) AS u, min(v) AS first, max(v) AS last FROM '$B/big.parquet'"));
    try testing.expectEqualStrings("id,v\n0,R0\n1,r1\n2,r2\n4,r4\n", try rows(a, &tmp, "SELECT id, v FROM '$B/big.parquet' LIMIT 4"));
}

test "INSERT into a file: a CSV's columns by name in its delimiter, encoding and line ending, each value the type the file's column reads as; NDJSON, gzip, Parquet and a JSON array" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "i.csv", .data = "\xef\xbb\xbfid;nome;valor\r\n1;Jo\xe3o;1.50\r\n" });
    try load(&tmp, "INSERT INTO '$B/i.csv' WITH (delimiter = ';', encoding = 'latin1') SELECT 'Concei\u{00e7}\u{00e3}o' AS nome, 2 AS id;");
    try harness.expectFile(&tmp, "i.csv", "\xef\xbb\xbfid;nome;valor\r\n1;Jo\xe3o;1.50\r\n2;Concei\xe7\xe3o;\r\n");
    try tmp.dir.writeFile(.{ .sub_path = "q.csv", .data = "id,q,d\n1,17,2024-01-01\n" });
    try load(&tmp, "INSERT INTO '$B/q.csv' SELECT 2 AS id, CAST(40.00 AS DECIMAL(10,2)) AS q, CAST('2024-02-01 00:00:00' AS TIMESTAMP) AS d;");
    try harness.expectFile(&tmp, "q.csv", "id,q,d\n1,17,2024-01-01\n2,40,2024-02-01\n");
    try tmp.dir.writeFile(.{ .sub_path = "q.ndjson", .data = "{\"id\":1,\"q\":17}\n" });
    try load(&tmp, "INSERT INTO '$B/q.ndjson' SELECT 2 AS id, CAST(40.00 AS DECIMAL(10,2)) AS q;");
    try harness.expectFile(&tmp, "q.ndjson", "{\"id\":1,\"q\":17}\n{\"id\":2,\"q\":40}\n");
    try tmp.dir.writeFile(.{ .sub_path = "i.ndjson", .data = "{\"id\":1}\n" });
    try load(&tmp, "INSERT INTO '$B/i.ndjson' SELECT 2 AS id, 'x' AS v;");
    try harness.expectFile(&tmp, "i.ndjson", "{\"id\":1}\n{\"id\":2,\"v\":\"x\"}\n");
    try tmp.dir.writeFile(.{ .sub_path = "j.json", .data = "[{\"id\":1,\"v\":\"a\"}]\n" });
    try load(&tmp, "INSERT INTO '$B/j.json' SELECT 2 AS id;");
    try harness.expectFile(&tmp, "j.json", "[\n{\"id\":1,\"v\":\"a\"},\n{\"id\":2,\"v\":null}\n]\n");
    try load(&tmp, "LOAD INTO '$B/g.csv.gz' AS SELECT 1 AS id, 'a' AS v;");
    try load(&tmp, "INSERT INTO '$B/g.csv.gz' SELECT 2 AS id;");
    try testing.expectEqualStrings("id,v\n1,a\n2,\n", try rows(a, &tmp, "SELECT * FROM '$B/g.csv.gz'"));
    try load(&tmp, "LOAD INTO '$B/p.parquet' AS SELECT 1 AS id, CAST(1.5 AS DECIMAL(10,2)) AS valor;");
    try load(&tmp, "INSERT INTO '$B/p.parquet' (valor, id) SELECT 2.25, 2;");
    try testing.expectEqualStrings("id,valor\n1,1.50\n2,2.25\n", try rows(a, &tmp, "SELECT * FROM '$B/p.parquet'"));
}

test "INSERT into a file refuses a file that is not there, one cut short, a column it lacks, a value its column's type cannot hold exactly and a format a rewrite would lose" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "torn.csv", .data = "id,v\n1,a" });
    try tmp.dir.writeFile(.{ .sub_path = "x.xml", .data = "<r><row><id>1</id></row></r>" });
    try load(&tmp, "LOAD INTO '$B/p.parquet' AS SELECT 1 AS id;");
    try harness.expectRunRefuses(gpa, &tmp, "INSERT INTO '$B/none.csv' SELECT 1 AS id;", 1, "there is no such file");
    try harness.expectRunRefuses(gpa, &tmp, "INSERT INTO '$B/torn.csv' SELECT 2 AS id;", 1, "does not end in a line break");
    try harness.expectFile(&tmp, "torn.csv", "id,v\n1,a");
    try harness.expectRunRefuses(gpa, &tmp, "INSERT INTO '$B/p.parquet' SELECT 2 AS id, 3 AS extra;", 1, "the rows have column `extra`, which the file lacks");
    try tmp.dir.writeFile(.{ .sub_path = "q.csv", .data = "id,q\n1,17\n" });
    try harness.expectRunRefuses(gpa, &tmp, "INSERT INTO '$B/q.csv' SELECT 3 AS id, 40.5 AS q;", 1, "column `q` reads as int from the file, and 40.5 is not one exactly");
    try harness.expectFile(&tmp, "q.csv", "id,q\n1,17\n");
    try harness.expectRefusedAlike(gpa, &tmp, "INSERT INTO '$B/x.xml' SELECT 1 AS id;", "an XML file is written in basalt's own shape");
    try harness.expectRefusedAlike(gpa, &tmp, "DELETE FROM '$B/x.xml' WHERE id = 1;", "an XML file is written in basalt's own shape");
}

/// A workbook of two sheets, `Dados` (a styled header from the shared strings, then
/// four rows) and `Resumo`, its second sheet's cell `second`.
fn workbook(tmp: *testing.TmpDir, name: []const u8, second: []const u8) !void {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var aw = std.Io.Writer.Allocating.init(a);
    const z = try basalt.zipwrite.Writer.init(a, &aw.writer);
    defer z.deinit();
    const decl = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    const ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    const rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    try z.addStored("[Content_Types].xml", decl ++ "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/></Types>");
    try z.addStored("_rels/.rels", decl ++ "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"" ++ rel ++ "/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>");
    try z.addStored("xl/workbook.xml", decl ++ "<workbook xmlns=\"" ++ ns ++ "\" xmlns:r=\"" ++ rel ++ "\"><sheets><sheet name=\"Dados\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"Resumo\" sheetId=\"2\" r:id=\"rId2\"/></sheets></workbook>");
    try z.addStored("xl/_rels/workbook.xml.rels", decl ++ "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"" ++ rel ++ "/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId2\" Type=\"" ++ rel ++ "/worksheet\" Target=\"worksheets/sheet2.xml\"/><Relationship Id=\"rId3\" Type=\"" ++ rel ++ "/styles\" Target=\"styles.xml\"/><Relationship Id=\"rId4\" Type=\"" ++ rel ++ "/sharedStrings\" Target=\"sharedStrings.xml\"/></Relationships>");
    try z.addStored("xl/styles.xml", decl ++ "<styleSheet xmlns=\"" ++ ns ++ "\"><fonts count=\"2\"><font><sz val=\"11\"/></font><font><b/><sz val=\"11\"/></font></fonts><fills count=\"1\"><fill><patternFill patternType=\"none\"/></fill></fills><borders count=\"1\"><border/></borders><cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs><cellXfs count=\"2\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/><xf numFmtId=\"0\" fontId=\"1\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyFont=\"1\"/></cellXfs></styleSheet>");
    try z.addStored("xl/sharedStrings.xml", decl ++ "<sst xmlns=\"" ++ ns ++ "\" count=\"7\" uniqueCount=\"7\"><si><t>id</t></si><si><t>nome</t></si><si><t>valor</t></si><si><t>Ana</t></si><si><t>Bia</t></si><si><t>Caio</t></si><si><t>Dora</t></si></sst>");
    var s1 = std.Io.Writer.Allocating.init(a);
    try s1.writer.writeAll(decl ++ "<worksheet xmlns=\"" ++ ns ++ "\"><dimension ref=\"A1:C5\"/><cols><col min=\"2\" max=\"2\" width=\"20\" customWidth=\"1\"/></cols><sheetData><row r=\"1\"><c r=\"A1\" s=\"1\" t=\"s\"><v>0</v></c><c r=\"B1\" s=\"1\" t=\"s\"><v>1</v></c><c r=\"C1\" s=\"1\" t=\"s\"><v>2</v></c></row>");
    for (0..4) |i| try s1.writer.print("<row r=\"{d}\" spans=\"1:3\"><c r=\"A{d}\"><v>{d}</v></c><c r=\"B{d}\" t=\"s\"><v>{d}</v></c><c r=\"C{d}\"><v>{d}</v></c></row>", .{ i + 2, i + 2, i + 1, i + 2, i + 3, i + 2, (i + 1) * 10 });
    try s1.writer.writeAll("</sheetData><autoFilter ref=\"A1:C5\"/></worksheet>");
    try z.addStored("xl/worksheets/sheet1.xml", s1.written());
    try z.addStored("xl/worksheets/sheet2.xml", try std.mem.concat(a, u8, &.{ decl, "<worksheet xmlns=\"", ns, "\"><sheetData><row r=\"1\">", second, "</row></sheetData></worksheet>" }));
    try z.finish();
    try tmp.dir.writeFile(.{ .sub_path = name, .data = aw.written() });
}

/// A workbook whose sheet `Dados` holds a formula, with the calculation chain that
/// names its cell, and a second sheet `Fixa`.
fn chainedWorkbook(tmp: *testing.TmpDir, name: []const u8) !void {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var aw = std.Io.Writer.Allocating.init(a);
    const z = try basalt.zipwrite.Writer.init(a, &aw.writer);
    defer z.deinit();
    const decl = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
    const ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
    const rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    const ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.";
    try z.addStored("[Content_Types].xml", decl ++ "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\"><Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/><Default Extension=\"xml\" ContentType=\"application/xml\"/><Override PartName=\"/xl/workbook.xml\" ContentType=\"" ++ ct ++ "sheet.main+xml\"/><Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"" ++ ct ++ "worksheet+xml\"/><Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"" ++ ct ++ "worksheet+xml\"/><Override PartName=\"/xl/calcChain.xml\" ContentType=\"" ++ ct ++ "calcChain+xml\"/></Types>");
    try z.addStored("_rels/.rels", decl ++ "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"" ++ rel ++ "/officeDocument\" Target=\"xl/workbook.xml\"/></Relationships>");
    try z.addStored("xl/workbook.xml", decl ++ "<workbook xmlns=\"" ++ ns ++ "\" xmlns:r=\"" ++ rel ++ "\"><sheets><sheet name=\"Dados\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"Fixa\" sheetId=\"4\" r:id=\"rId2\"/></sheets></workbook>");
    try z.addStored("xl/_rels/workbook.xml.rels", decl ++ "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"" ++ rel ++ "/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId2\" Type=\"" ++ rel ++ "/worksheet\" Target=\"worksheets/sheet2.xml\"/><Relationship Id=\"rId7\" Type=\"" ++ rel ++ "/calcChain\" Target=\"calcChain.xml\"/></Relationships>");
    try z.addStored("xl/worksheets/sheet1.xml", decl ++ "<worksheet xmlns=\"" ++ ns ++ "\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>v</t></is></c><c r=\"B1\" t=\"inlineStr\"><is><t>w</t></is></c></row><row r=\"2\"><c r=\"A2\"><v>1</v></c><c r=\"B2\"><f>A2*2</f><v>2</v></c></row></sheetData></worksheet>");
    try z.addStored("xl/worksheets/sheet2.xml", decl ++ "<worksheet xmlns=\"" ++ ns ++ "\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"inlineStr\"><is><t>k</t></is></c></row><row r=\"2\"><c r=\"A2\"><v>42</v></c></row></sheetData></worksheet>");
    try z.addStored("xl/calcChain.xml", decl ++ "<calcChain xmlns=\"" ++ ns ++ "\"><c r=\"B2\" i=\"1\"/></calcChain>");
    try z.finish();
    try tmp.dir.writeFile(.{ .sub_path = name, .data = aw.written() });
}

fn member(a: std.mem.Allocator, tmp: *testing.TmpDir, file: []const u8, name: []const u8) ![]const u8 {
    const path = try tmp.dir.realpathAlloc(a, file);
    const m = try basalt.zipsrc.openMember(a, path, name);
    defer m.close();
    var aw = std.Io.Writer.Allocating.init(a);
    _ = try m.reader.streamRemaining(&aw.writer);
    return aw.written();
}

test "LOAD INTO a workbook with a sheet named writes that sheet anew or adds it, keeps every other sheet and drops the calculation chain; dates follow a 1904 workbook's calendar" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try load(&tmp, "LOAD INTO '$B/r.xlsx' WITH (sheet = 'Vendas') AS SELECT 1 AS id, 'a' AS t;");
    try load(&tmp, "LOAD INTO '$B/r.xlsx' WITH (sheet = 'Resumo') AS SELECT 2 AS id, CAST('2026-01-31' AS DATE) AS d;");
    try load(&tmp, "LOAD INTO '$B/r.xlsx' WITH (sheet = 'vendas') AS SELECT 9 AS id, 'z' AS t;");
    try testing.expectEqualStrings("id,t\n9,z\n", try rows(a, &tmp, "SELECT * FROM '$B/r.xlsx' WITH (sheet = 'Vendas')"));
    try testing.expectEqualStrings("id,d\n2,2026-01-31\n", try rows(a, &tmp, "SELECT * FROM '$B/r.xlsx' WITH (sheet = 'Resumo')"));
    const wbx = try member(a, &tmp, "r.xlsx", "xl/workbook.xml");
    try testing.expect(std.mem.indexOf(u8, wbx, "<sheet name=\"Vendas\" sheetId=\"1\"") != null);
    try testing.expect(std.mem.indexOf(u8, wbx, "<sheet name=\"Resumo\" sheetId=\"2\"") != null);
    try harness.expectRunRefuses(gpa, &tmp, "INSERT INTO '$B/r.xlsx' WITH (sheet = '1') SELECT 1 AS id;", 1, "no sheet `1` (sheets: Vendas, Resumo)");

    try chainedWorkbook(&tmp, "c.xlsx");
    try load(&tmp, "LOAD INTO '$B/c.xlsx' WITH (sheet = 'Dados') AS SELECT 5 AS v;");
    try testing.expectEqualStrings("v\n5\n", try rows(a, &tmp, "SELECT * FROM '$B/c.xlsx' WITH (sheet = 'Dados')"));
    try testing.expectEqualStrings("k\n42\n", try rows(a, &tmp, "SELECT * FROM '$B/c.xlsx' WITH (sheet = 'Fixa')"));
    try load(&tmp, "LOAD INTO '$B/c.xlsx' WITH (sheet = 'Nova') AS SELECT 6 AS n;");
    try testing.expectEqualStrings("n\n6\n", try rows(a, &tmp, "SELECT * FROM '$B/c.xlsx' WITH (sheet = 'Nova')"));
    const c_path = try tmp.dir.realpathAlloc(a, "c.xlsx");
    for (try basalt.zipsrc.names(a, c_path)) |n| try testing.expect(std.mem.indexOf(u8, n, "calcChain") == null);
    for ([_][]const u8{ "[Content_Types].xml", "xl/_rels/workbook.xml.rels" }) |p| try testing.expect(std.mem.indexOf(u8, try member(a, &tmp, "c.xlsx", p), "calcChain") == null);
    const cwb = try member(a, &tmp, "c.xlsx", "xl/workbook.xml");
    try testing.expect(std.mem.indexOf(u8, cwb, "<sheet name=\"Nova\" sheetId=\"5\" r:id=\"rId3\"/>") != null);
    try testing.expect(std.mem.indexOf(u8, try member(a, &tmp, "c.xlsx", "[Content_Types].xml"), "<Override PartName=\"/xl/worksheets/sheet3.xml\"") != null);

    try tmp.dir.writeFile(.{ .sub_path = "e.xlsx", .data = basalt.fixtures.edge_xlsx });
    try load(&tmp, "LOAD INTO '$B/e.xlsx' WITH (sheet = 'Novo') AS SELECT CAST('2026-01-31' AS DATE) AS d;");
    try testing.expectEqualStrings("d\n2026-01-31\n", try rows(a, &tmp, "SELECT * FROM '$B/e.xlsx' WITH (sheet = 'Novo')"));
    try testing.expect(std.mem.indexOf(u8, try member(a, &tmp, "e.xlsx", "xl/worksheets/sheet1.xml"), "<v>44591</v>") != null);
    try testing.expect(std.mem.indexOf(u8, try member(a, &tmp, "e.xlsx", "xl/workbook.xml"), "<x:sheet name=\"Novo\" sheetId=\"3\" r:id=\"rId10\"/>") != null);
    try harness.expectRunRefuses(gpa, &tmp, "LOAD INTO '$B/e.xlsx' WITH (sheet = 'grafico') AS SELECT 1 AS a;", 1, "a chart sheet of the workbook has that name");
}

test "DELETE, UPDATE and INSERT on an Excel sheet keep its other sheets, shared strings and styles, and every row they leave" {
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try workbook(&tmp, "w.xlsx", "<c r=\"A1\" t=\"inlineStr\"><is><t>resumo</t></is></c>");
    const keep = [_][]const u8{ "xl/workbook.xml", "xl/sharedStrings.xml", "xl/styles.xml", "xl/worksheets/sheet2.xml" };
    var before: [keep.len][]const u8 = undefined;
    for (&before, keep) |*b, k| b.* = try member(a, &tmp, "w.xlsx", k);
    try load(&tmp, "DELETE FROM '$B/w.xlsx' WHERE id = 2;");
    try load(&tmp, "UPDATE '$B/w.xlsx' SET valor = valor + 1, nome = 'Caio Novo' WHERE id = 3;");
    for (before, keep) |b, k| try testing.expectEqualStrings(b, try member(a, &tmp, "w.xlsx", k));
    try testing.expectEqualStrings("id,nome,valor\n1,Ana,10\n3,Caio Novo,31\n4,Dora,40\n", try rows(a, &tmp, "SELECT * FROM '$B/w.xlsx'"));
    const sheet = try member(a, &tmp, "w.xlsx", "xl/worksheets/sheet1.xml");
    try testing.expect(std.mem.indexOf(u8, sheet, "<dimension ref=\"A1:C4\"/><cols><col min=\"2\" max=\"2\" width=\"20\" customWidth=\"1\"/></cols>") != null);
    try testing.expect(std.mem.indexOf(u8, sheet, "<row r=\"4\" spans=\"1:3\"><c r=\"A4\"><v>4</v></c><c r=\"B4\" t=\"s\"><v>6</v></c><c r=\"C4\"><v>40</v></c></row>") != null);
    try testing.expect(std.mem.indexOf(u8, sheet, "<autoFilter ref=\"A1:C4\"/>") != null);
    try load(&tmp, "INSERT INTO '$B/w.xlsx' WITH (sheet = 'Dados') SELECT 5 AS id, 'Eva' AS nome;");
    try testing.expectEqualStrings("id,nome,valor\n1,Ana,10\n3,Caio Novo,31\n4,Dora,40\n5,Eva,\n", try rows(a, &tmp, "SELECT * FROM '$B/w.xlsx'"));
    try testing.expectEqualStrings(before[3], try member(a, &tmp, "w.xlsx", "xl/worksheets/sheet2.xml"));

    try workbook(&tmp, "f.xlsx", "<c r=\"A1\"><f>SUM(Dados!C2:C5)</f><v>100</v></c>");
    try harness.expectRunRefuses(gpa, &tmp, "DELETE FROM '$B/f.xlsx' WHERE id = 2;", 1, "a formula in another sheet names sheet `Dados`");
    try harness.expectRefusedAlike(gpa, &tmp, "DELETE FROM '$B/w.xlsx' WITH (range = 'A1:C3') WHERE id = 2;", "drop `range`");
}
