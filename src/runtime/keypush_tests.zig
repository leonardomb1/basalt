//! Key pushdown end to end, through `run`: the SQL a join's taking side sends to a
//! PostgreSQL server (the in-process fake of `db/postgres_fake.zig`, which records
//! every query), and the row groups a Parquet side skips.

const std = @import("std");
const FakePg = @import("../db/postgres_fake.zig").FakePg;
const Table = @import("../db/postgres_fake.zig").Table;
const column = @import("../exec/column.zig");
const env_mod = @import("env.zig");
const obs = @import("obs.zig");
const parser = @import("../lang/sql_parser.zig");
const pqwrite = @import("../format/parquet/write.zig");
const run = @import("run.zig").run;
const types = @import("../lang/types.zig");

const testing = std.testing;

const item_rows = [_][]const ?[]const u8{
    &.{ "1", " A1 ", "alpha", "x" },
    &.{ "2", "B2", "beta", "y" },
    &.{ "3", "C3 ", "gamma", "z" },
};
const tables = [_]Table{.{
    .name = "items",
    .cols = &.{ .{ .name = "id", .oid = 20 }, .{ .name = "code" }, .{ .name = "name" }, .{ .name = "extra" } },
    .rows = &item_rows,
}};

const Ran = struct {
    err: ?anyerror,
    diag: env_mod.Diag,
    out: []const u8,
    summary: obs.Summary,
};

/// Runs `body` (its `$B` the tmp dir) as `LOAD INTO '$B/out.csv' AS <body>`, after
/// a `CREATE CONNECTION pg` to the fake server on `srv_port`.
fn runPg(a: std.mem.Allocator, tmp: *std.testing.TmpDir, srv_port: u16, body: []const u8, threads: usize) !Ran {
    const base = try tmp.dir.realpathAlloc(a, ".");
    const conn = try std.fmt.allocPrint(a, "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');\n", .{srv_port});
    const q = try std.mem.replaceOwned(u8, a, body, "$B", base);
    const script = try std.fmt.allocPrint(a, "{s}LOAD INTO '{s}/out.csv' AS {s};", .{ conn, base, q });
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var ran = Ran{ .err = null, .diag = .{}, .out = "", .summary = .{ .run_id = 0 } };
    _ = run(testing.allocator, prog, .{ .threads = threads, .log = .{ .quiet = true }, .summary_out = &ran.summary }, &ran.diag) catch |e| {
        ran.err = e;
        ran.diag.msg = try a.dupe(u8, ran.diag.msg);
        return ran;
    };
    ran.out = try tmp.dir.readFileAlloc(a, "out.csv", 1 << 20);
    return ran;
}

/// The queries the server got that read `items`.
fn itemQueries(a: std.mem.Allocator, srv: *FakePg) ![]const []const u8 {
    const all = try srv.received(testing.allocator);
    defer testing.allocator.free(all);
    var out = std.array_list.Managed([]const u8).init(a);
    for (all) |q| if (std.mem.indexOf(u8, q, "public.items") != null) try out.append(try a.dupe(u8, q));
    return out.toOwnedSlice();
}

fn expectLast(qs: []const []const u8, want: []const u8) !void {
    try testing.expect(qs.len >= 2);
    try testing.expectEqualStrings(want, qs[qs.len - 1]);
    try testing.expect(std.mem.endsWith(u8, qs[qs.len - 2], " WHERE 1 = 0"));
}

test "a SQL right side asks for the left side's keys after its WHERE 1 = 0 probe, its columns narrowed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "cr,qty\n1,10\n3,30\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    const r = try runPg(a, &tmp, srv.port(), "SELECT s.qty, c.name FROM '$B/s.csv' s JOIN pg.public.items c ON c.id = s.cr", 1);
    try testing.expect(r.err == null);
    try testing.expectEqualStrings("qty,name\n10,alpha\n30,gamma\n", r.out);
    try expectLast(try itemQueries(a, srv), "SELECT \"id\", \"name\" FROM public.items WHERE \"id\" IN (1, 3)");
}

test "a computed key narrows the right side's columns to those the key and the output read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "cr\nA1\nC3\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    const r = try runPg(a, &tmp, srv.port(), "SELECT s.cr, c.name FROM '$B/s.csv' s JOIN pg.public.items c ON trim(c.code) = CAST(s.cr AS varchar)", 1);
    try testing.expect(r.err == null);
    try testing.expectEqualStrings("cr,name\nA1,alpha\nC3,gamma\n", r.out);
    const qs = try itemQueries(a, srv);
    try testing.expect(std.mem.startsWith(u8, qs[qs.len - 1], "SELECT \"code\", \"name\" FROM public.items WHERE "));
    try testing.expect(std.mem.endsWith(u8, qs[qs.len - 1], " IN ('A1', 'C3')"));
    try testing.expect(std.mem.endsWith(u8, qs[qs.len - 2], " WHERE 1 = 0"));

    const u = try runPg(a, &tmp, srv.port(), "SELECT cr, name FROM '$B/s.csv' JOIN pg.public.items ON trim(code) = CAST(cr AS varchar)", 1);
    try testing.expect(u.err == null);
    try testing.expectEqualStrings("cr,name\nA1,alpha\nC3,gamma\n", u.out);
    const uq = try itemQueries(a, srv);
    try testing.expect(std.mem.startsWith(u8, uq[uq.len - 1], "SELECT \"code\", \"name\" FROM public.items WHERE "));
}

test "a SQL left side asks for the indexed right side's keys" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "cr,qty\n2,20\n3,30\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    const r = try runPg(a, &tmp, srv.port(), "SELECT b.name, s.qty FROM pg.public.items b JOIN '$B/s.csv' s ON b.id = s.cr", 1);
    try testing.expect(r.err == null);
    try testing.expectEqualStrings("name,qty\nbeta,20\ngamma,30\n", r.out);
    try expectLast(try itemQueries(a, srv), "SELECT \"id\", \"name\" FROM public.items WHERE \"id\" IN (2, 3)");
}

test "key_pushdown = false sends the plain query, and an empty side sends WHERE 1 = 0" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "cr,qty\n1,10\n3,30\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    const off = try runPg(a, &tmp, srv.port(), "SELECT s.qty, c.name FROM '$B/s.csv' s JOIN pg.public.items c ON c.id = s.cr WITH (key_pushdown = false)", 1);
    try testing.expect(off.err == null);
    try testing.expectEqualStrings("qty,name\n10,alpha\n30,gamma\n", off.out);
    const qs = try itemQueries(a, srv);
    try testing.expectEqualStrings("SELECT \"id\", \"name\" FROM public.items", qs[qs.len - 1]);
    for (qs) |q| try testing.expect(std.mem.indexOf(u8, q, " IN (") == null);

    const empty = try runPg(a, &tmp, srv.port(), "WITH s AS (SELECT * FROM '$B/s.csv' WHERE qty > 100) SELECT s.qty, c.name FROM s JOIN pg.public.items c ON c.id = s.cr", 1);
    try testing.expect(empty.err == null);
    try testing.expectEqualStrings("qty,name\n", empty.out);
    try expectLast(try itemQueries(a, srv), "SELECT \"id\", \"name\" FROM public.items WHERE 1 = 0");
}

test "a failed late query is the server's words, permanent; a dropped connection is transient" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "cr,qty\n1,10\n3,30\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();
    srv.fail_on = " IN (";
    srv.fail_msg = "canceling statement due to statement timeout";

    const body = "SELECT s.qty, c.name FROM '$B/s.csv' s JOIN pg.public.items c ON c.id = s.cr";
    const r = try runPg(a, &tmp, srv.port(), body, 1);
    try testing.expect(r.err != null);
    try testing.expect(std.mem.indexOf(u8, r.diag.msg, "canceling statement due to statement timeout") != null);
    try testing.expect(!r.diag.retryable and !env_mod.isTransient(r.err.?));

    srv.fail_on = null;
    srv.drop_on = " IN (";
    const d = try runPg(a, &tmp, srv.port(), body, 1);
    try testing.expect(d.err != null);
    try testing.expect(d.diag.retryable or env_mod.isTransient(d.err.?));
}

const pq_schema = types.Schema{ .fields = &.{
    .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "v", .ty = types.Type.init(.string).asNullable() },
} };

/// A Parquet file of ids `first`, `first + 1`, …, one row group each.
fn writeGroups(a: std.mem.Allocator, path: []const u8, first: i64, groups: usize) !void {
    var w = try pqwrite.Writer.open(a, path, pq_schema, .snappy, .truncate);
    w.group_rows = 1;
    var ids = column.Builder.init(a, pq_schema.fields[0].ty);
    var vs = column.Builder.init(a, pq_schema.fields[1].ty);
    for (0..groups) |g| {
        const id = first + @as(i64, @intCast(g));
        try ids.append(.{ .int = id });
        try vs.append(.{ .string = try std.fmt.allocPrint(a, "v{d}", .{id}) });
    }
    var cols = [_]column.Column{ try ids.finish(), try vs.finish() };
    try w.writeBatch(a, .{ .schema = &pq_schema, .columns = &cols, .len = groups });
    try w.close();
}

test "a Parquet right side skips the row groups no left key falls in" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeGroups(a, try std.fs.path.join(a, &.{ base, "r.parquet" }), 1, 8);
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "k,q\n1,a\n6,b\n" });
    try tmp.dir.writeFile(.{ .sub_path = "none.csv", .data = "k,q\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    const r = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/s.csv' s JOIN '$B/r.parquet' r ON r.id = s.k", 1);
    try testing.expect(r.err == null);
    try testing.expectEqualStrings("q,v\na,v1\nb,v6\n", r.out);
    try testing.expectEqual(@as(u64, 8), r.summary.pushdown.row_groups);
    try testing.expectEqual(@as(u64, 6), r.summary.pushdown.row_groups_skipped);

    const left = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/s.csv' s LEFT JOIN '$B/r.parquet' r ON r.id = s.k", 1);
    try testing.expectEqualStrings("q,v\na,v1\nb,v6\n", left.out);
    try testing.expectEqual(@as(u64, 6), left.summary.pushdown.row_groups_skipped);

    const off = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/s.csv' s JOIN '$B/r.parquet' r ON r.id = s.k WITH (key_pushdown = false)", 1);
    try testing.expectEqualStrings("q,v\na,v1\nb,v6\n", off.out);
    try testing.expectEqual(@as(u64, 0), off.summary.pushdown.row_groups_skipped);

    const computed = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/s.csv' s JOIN '$B/r.parquet' r ON r.id + 0 = s.k", 1);
    try testing.expectEqualStrings("q,v\na,v1\nb,v6\n", computed.out);
    try testing.expectEqual(@as(u64, 0), computed.summary.pushdown.row_groups_skipped);

    try writeGroups(a, try std.fs.path.join(a, &.{ base, "big.parquet" }), 1, 200);
    const flipped = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/s.csv' s JOIN '$B/big.parquet' r ON r.id = s.k ORDER BY s.q", 1);
    try testing.expect(flipped.err == null);
    try testing.expectEqualStrings("q,v\na,v1\nb,v6\n", flipped.out);
    try testing.expectEqual(@as(u64, 198), flipped.summary.pushdown.row_groups_skipped);

    const right = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/s.csv' s RIGHT JOIN '$B/r.parquet' r ON r.id = s.k", 1);
    try testing.expectEqual(@as(u64, 0), right.summary.pushdown.row_groups_skipped);

    const empty = try runPg(a, &tmp, srv.port(), "SELECT s.q, r.v FROM '$B/none.csv' s JOIN '$B/r.parquet' r ON r.id = CAST(s.k AS bigint)", 1);
    try testing.expect(empty.err == null);
    try testing.expectEqual(@as(u64, 8), empty.summary.pushdown.row_groups_skipped);
}

test "a Parquet left side, a file or a folder, serial or in lanes, skips the row groups no right key falls in" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeGroups(a, try std.fs.path.join(a, &.{ base, "l.parquet" }), 1, 8);
    try tmp.dir.makeDir("d");
    try writeGroups(a, try std.fs.path.join(a, &.{ base, "d", "a.parquet" }), 1, 4);
    try writeGroups(a, try std.fs.path.join(a, &.{ base, "d", "b.parquet" }), 5, 4);
    try tmp.dir.writeFile(.{ .sub_path = "s.csv", .data = "k,q\n2,a\n7,b\n" });
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    for ([_][]const u8{ "l.parquet", "d/" }) |src| {
        for ([_]usize{ 1, 4 }) |threads| {
            const body = try std.fmt.allocPrint(a, "SELECT l.v, s.q FROM '$B/{s}' l JOIN '$B/s.csv' s ON l.id = s.k", .{src});
            const r = try runPg(a, &tmp, srv.port(), body, threads);
            try testing.expect(r.err == null);
            try testing.expectEqualStrings("v,q\nv2,a\nv7,b\n", r.out);
            try testing.expectEqual(@as(u64, 6), r.summary.pushdown.row_groups_skipped);
            try testing.expectEqual(threads > 1, r.summary.threads > 1);
        }
    }

    const kept = try runPg(a, &tmp, srv.port(), "SELECT l.v FROM '$B/l.parquet' l LEFT JOIN '$B/s.csv' s ON l.id = s.k", 1);
    try testing.expectEqual(@as(u64, 0), kept.summary.pushdown.row_groups_skipped);
}
