//! End-to-end XML: rows by default and by `row`, nesting as JSON, a folder, the
//! writer's round trip with names FOR XML encodes, and what is refused.

const std = @import("std");
const checkAndRun = @import("harness.zig").checkAndRun;
const expectRefusedAlike = @import("harness.zig").expectRefusedAlike;

fn out(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

test "children and attributes are columns, a nested child JSON, a repeated one an array in every row" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "o.xml", .data = 
        \\<?xml version="1.0"?>
        \\<orders>
        \\  <order id="1"><customer>Ana &amp; Co</customer><total currency="BRL">10.50</total><items><item sku="A">2</item><item sku="B">1</item></items></order>
        \\  <order id="2"><customer></customer><items><item sku="C">5</item></items></order>
        \\</orders>
    });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id, customer, customer IS NULL AS cnull, total, \"total@currency\" AS cur, json_get(items, 'item') AS it FROM '$B/o.xml' ORDER BY id;", 1, &.{});
    const got = try out(alloc, &tmp);
    defer alloc.free(got);
    try std.testing.expectEqualStrings(
        "id,customer,cnull,total,cur,it\n" ++
            "1,Ana & Co,false,10.5,BRL,\"[{\"\"@sku\"\":\"\"A\"\",\"\"#text\"\":\"\"2\"\"},{\"\"@sku\"\":\"\"B\"\",\"\"#text\"\":\"\"1\"\"}]\"\n" ++
            "2,\"\",false,,,\"[{\"\"@sku\"\":\"\"C\"\",\"\"#text\"\":\"\"5\"\"}]\"\n",
        got,
    );
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT sku, SUM(item) AS q FROM '$B/o.xml' WITH (row = 'item') GROUP BY sku ORDER BY sku;", 1, &.{});
    const items = try out(alloc, &tmp);
    defer alloc.free(items);
    try std.testing.expectEqualStrings("sku,q\nA,2\nB,1\nC,5\n", items);
}

test "XML written, gzipped and read back: NULL left out, the empty string kept, names encoded" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,Valor Total,note\n1,2.50,\"a<b & \"\"c\"\"\"\n2,,\"\"\n3,7,\n" });
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/o.xml.gz' WITH (root = 'Itens', row = 'Item') AS SELECT * FROM '$B/in.csv';", 4, &.{});
    try checkAndRun(alloc, &tmp, "LOAD INTO '$B/out.csv' AS SELECT id, \"Valor Total\", note, note IS NULL AS nnull FROM '$B/o.xml.gz' ORDER BY id;", 1, &.{});
    const got = try out(alloc, &tmp);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("id,Valor Total,note,nnull\n1,2.5,\"a<b & \"\"c\"\"\",false\n2,,\"\",false\n3,7,,true\n", got);
}

test "an XML file is not appended to; root and row apply to an XML target only" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.xml' APPEND AS SELECT 1 AS a;", "root element closes on its last line");
    try expectRefusedAlike(alloc, &tmp, "LOAD INTO '$B/o.csv' WITH (row = 'r') AS SELECT 1 AS a;", "`row` is an option of an XML file");
    try expectRefusedAlike(alloc, &tmp, "SELECT * FROM '$B/x.xml' WITH (items = 'a');", "does not apply to an XML read");
}
