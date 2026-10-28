//! The path oracle: for each pipeline shape, the path a plain `EXPLAIN` names is
//! the path `EXPLAIN ANALYZE` takes, over local CSV, Parquet and record files and
//! the in-process PostgreSQL fake (`testing/postgres_fake.zig`). Paths compare by
//! their head, the text before any count or `unless`; a row whose source vetoes
//! its path at run time (a quoted newline, one row group) names the static head
//! apart. Both outputs are captured from fd 2, and fd 1 is held too, so nothing
//! reaches the test runner's channel.

const std = @import("std");
const FakePg = @import("../../testing/postgres_fake.zig").FakePg;
const Table = @import("../../testing/postgres_fake.zig").Table;
const column = @import("../../exec/column.zig");
const env_mod = @import("../env.zig");
const msgfmt = @import("../../format/msgfmt.zig");
const parser = @import("../../lang/sql_parser.zig");
const pqwrite = @import("../../format/parquet/write.zig");
const run = @import("../run.zig").run;
const types = @import("../../lang/types.zig");

const testing = std.testing;

const Case = struct {
    q: []const u8,
    want: []const u8,
    static: ?[]const u8 = null,
    threads: usize = 4,
    select: bool = false,
};

const cases = [_]Case{
    .{ .q = "SELECT id, v FROM '$B/in.csv' WHERE v > 2", .want = "csv parallel map" },
    .{ .q = "SELECT g, SUM(v) AS s FROM '$B/in.csv' GROUP BY g", .want = "csv parallel aggregate" },
    .{ .q = "SELECT g, SUM(v) AS s FROM '$B/in.csv' GROUP BY g ORDER BY s DESC LIMIT 2", .want = "csv parallel aggregate" },
    .{ .q = "SELECT DISTINCT COUNT(*) AS n FROM '$B/in.csv' GROUP BY g", .want = "csv parallel aggregate" },
    .{ .q = "SELECT DISTINCT g FROM '$B/in.csv'", .want = "csv parallel distinct" },
    .{ .q = "SELECT id, v FROM '$B/in.csv' ORDER BY v DESC, id LIMIT 5", .want = "csv parallel top-n" },
    .{ .q = "SELECT id FROM '$B/in.csv' ORDER BY v DESC, id LIMIT 5", .want = "serial" },
    .{ .q = "SELECT id FROM '$B/in.csv' ORDER BY v, id", .want = "the window's input read into memory first (csv parallel map)" },
    .{ .q = "SELECT id, SUM(v) OVER (PARTITION BY g) AS s FROM '$B/in.csv'", .want = "the window's input read into memory first (csv parallel map)" },
    .{ .q = "SELECT * FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM '$B/in.csv') r WHERE rn <= 2", .want = "serial" },
    .{ .q = "SELECT id, x FROM '$B/in.csv' CROSS JOIN UNNEST(SPLIT(g, ',')) AS x", .want = "csv parallel map" },
    .{ .q = "SELECT id, x FROM '$B/p.parquet' CROSS JOIN UNNEST(SPLIT(g, ',')) AS x WHERE v > 2", .want = "parquet parallel map" },
    .{ .q = "SELECT id FROM '$B/in.csv' LIMIT 5", .want = "serial" },
    .{ .q = "SELECT t.id, k.label FROM '$B/in.csv' t JOIN '$B/k.csv' k ON t.g = k.g", .want = "csv parallel map+join" },
    .{ .q = "SELECT t.id, k.label FROM '$B/in.csv' t RIGHT JOIN '$B/k.csv' k ON t.g = k.g", .want = "csv parallel map+join" },
    .{ .q = "SELECT k.label, COUNT(*) AS n FROM '$B/in.csv' t JOIN '$B/k.csv' k ON t.g = k.g GROUP BY k.label", .want = "csv parallel aggregate over joins" },
    .{ .q = "SELECT k.label, COUNT(*) AS n FROM '$B/in.csv' t RIGHT JOIN '$B/k.csv' k ON t.g = k.g GROUP BY k.label", .want = "serial" },
    .{ .q = "SELECT COUNT(*) AS n FROM (SELECT g, SUM(v) AS s FROM '$B/in.csv' GROUP BY g) d", .want = "__derived1_d read into memory first (csv parallel aggregate)" },
    .{ .q = "WITH c AS (SELECT id, v FROM '$B/in.csv' WHERE v > 1) SELECT id FROM c", .want = "csv parallel map" },
    .{ .q = "WITH t AS (SELECT g, SUM(v) AS s FROM '$B/in.csv' GROUP BY g) SELECT g, s FROM t ORDER BY g", .want = "t read into memory first (csv parallel aggregate)" },
    .{ .q = "SELECT * FROM '$B/q.csv'", .want = "serial", .static = "csv parallel map" },
    .{ .q = "SELECT g, SUM(v) AS s FROM '$B/in.csv' GROUP BY g", .want = "serial", .threads = 1 },
    .{ .q = "SELECT s.v, c.g FROM '$B/in.csv' s JOIN pg.public.t c ON c.id = s.id", .want = "serial", .static = "csv parallel map+join" },
    .{ .q = "SELECT * FROM lines('$B/l.txt')", .want = "csv parallel map" },
    .{ .q = "SELECT id, v FROM '$B/p.parquet' WHERE v > 2", .want = "parquet parallel map" },
    .{ .q = "SELECT g, SUM(v) AS s FROM '$B/p.parquet' GROUP BY g", .want = "parquet parallel aggregate" },
    .{ .q = "SELECT id, v FROM '$B/p.parquet' ORDER BY v DESC, id LIMIT 5", .want = "parquet parallel top-n" },
    .{ .q = "SELECT DISTINCT g FROM '$B/p.parquet'", .want = "parquet parallel distinct" },
    .{ .q = "SELECT id FROM '$B/p.parquet' ORDER BY v, id", .want = "the window's input read into memory first (parquet parallel map)" },
    .{ .q = "SELECT id, v FROM '$B/p1.parquet'", .want = "serial", .static = "parquet parallel map" },
    .{ .q = "SELECT g, SUM(v) AS s FROM '$B/pf/' GROUP BY g", .want = "parquet parallel aggregate" },
    .{ .q = "SELECT id, v FROM '$B/pf/' WHERE v > 2", .want = "parquet parallel map" },
    .{ .q = "SELECT id, v FROM pg.public.t WHERE v > 1", .want = "postgres split-parallel" },
    .{ .q = "SELECT id, x FROM pg.public.t CROSS JOIN UNNEST(SPLIT(g, ',')) AS x", .want = "postgres split-parallel" },
    .{ .q = "SELECT id, v FROM pg.public.t", .want = "serial", .threads = 1 },
    .{ .q = "SELECT t.id, k.label FROM pg.public.t t JOIN '$B/k.csv' k ON t.g = k.g", .want = "postgres split-parallel map+join" },
    .{ .q = "SELECT k.label, COUNT(*) AS n FROM pg.public.t t JOIN '$B/k.csv' k ON t.g = k.g GROUP BY k.label", .want = "serial" },
    .{ .q = "SELECT id FROM pg.public.t ORDER BY id LIMIT 3", .want = "serial; ORDER BY ... LIMIT runs in the source" },
    .{ .q = "SELECT id FROM pg.public.t LIMIT 3", .want = "serial; ORDER BY ... LIMIT runs in the source" },
    .{ .q = "SELECT id FROM '$B/in.csv' ORDER BY v, id", .want = "serial", .threads = 1 },
    .{ .q = "SELECT id, v FROM pg.public.t ORDER BY v", .want = "serial" },
    .{ .q = "SELECT id, v FROM pg.QUERY($$SELECT * FROM t$$)", .want = "serial" },
    .{ .q = "SELECT 'a' AS tag, t.* FROM pg.public.t t UNION ALL BY NAME SELECT 'b' AS tag, t.* FROM pg.public.t2 t", .want = "serial" },
};

const split_cases = [_]Case{
    .{ .q = "SELECT g, COUNT(*) AS n FROM pg.public.t GROUP BY g", .want = "postgres split-parallel aggregate" },
    .{ .q = "SELECT DISTINCT COUNT(*) AS n FROM pg.public.t GROUP BY g", .want = "postgres split-parallel aggregate" },
};

const select_cases = [_]Case{
    .{ .q = "SELECT 'a' AS tag, t.* FROM pg.public.t t UNION ALL BY NAME SELECT 'b' AS tag, t.* FROM pg.public.t2 t", .want = "union split-parallel", .select = true },
};

fn pgRows(a: std.mem.Allocator) ![]const []const ?[]const u8 {
    const rows = try a.alloc([]const ?[]const u8, 40);
    for (rows, 0..) |*r, i| {
        const row = try a.alloc(?[]const u8, 3);
        row[0] = try msgfmt.allocPrint(a, "{d}", .{i + 1});
        row[1] = "abcde"[i % 5 ..][0..1];
        row[2] = try msgfmt.allocPrint(a, "{d}", .{i % 7});
        r.* = row;
    }
    return rows;
}

const pq_schema = types.Schema{ .fields = &.{
    .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "g", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "v", .ty = types.Type.init(.int).asNullable() },
} };

/// A Parquet file of `rows` rows cut into row groups of `group` rows.
fn writeParquet(a: std.mem.Allocator, path: []const u8, rows: usize, group: usize) !void {
    var w = try pqwrite.Writer.open(a, path, pq_schema, .snappy, .truncate);
    w.group_rows = group;
    var ids = column.Builder.init(a, pq_schema.fields[0].ty);
    var gs = column.Builder.init(a, pq_schema.fields[1].ty);
    var vs = column.Builder.init(a, pq_schema.fields[2].ty);
    for (0..rows) |i| {
        try ids.append(.{ .int = @intCast(i) });
        try gs.append(.{ .string = "abcde"[i % 5 ..][0..1] });
        try vs.append(.{ .int = @intCast(i % 10) });
    }
    var cols = [_]column.Column{ try ids.finish(), try gs.finish(), try vs.finish() };
    try w.writeBatch(a, .{ .schema = &pq_schema, .columns = &cols, .len = rows });
    try w.close();
}

/// fd `fd` pointed at `name` in `dir` until `end`.
const Capture = struct {
    file: std.fs.File,
    saved: std.posix.fd_t,
    fd: std.posix.fd_t,

    fn begin(dir: std.fs.Dir, fd: std.posix.fd_t, name: []const u8) !Capture {
        const file = try dir.createFile(name, .{ .read = true, .truncate = true });
        errdefer file.close();
        const saved = try std.posix.dup(fd);
        errdefer std.posix.close(saved);
        try std.posix.dup2(file.handle, fd);
        return .{ .file = file, .saved = saved, .fd = fd };
    }

    fn end(self: *Capture, a: std.mem.Allocator) ![]u8 {
        std.posix.dup2(self.saved, self.fd) catch {};
        std.posix.close(self.saved);
        defer self.file.close();
        try self.file.seekTo(0);
        return self.file.readToEndAlloc(a, 1 << 20);
    }
};

/// The `Path:` text `script` prints, run with `threads` lanes.
fn pathOf(a: std.mem.Allocator, dir: std.fs.Dir, script: []const u8, threads: usize) ![]const u8 {
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var diag = env_mod.Diag{};
    var out = try Capture.begin(dir, std.posix.STDOUT_FILENO, "stdout.txt");
    var err = try Capture.begin(dir, std.posix.STDERR_FILENO, "stderr.txt");
    const res = run(testing.allocator, prog, .{ .threads = threads, .log = .{ .quiet = true, .summary = .none } }, &diag);
    const printed = try err.end(a);
    _ = try out.end(a);
    _ = res catch |e| {
        std.debug.print("{s}\n{s}\n{s}\n", .{ script, diag.msg, printed });
        return e;
    };
    const at = std.mem.indexOf(u8, printed, "Path: ") orelse {
        std.debug.print("{s}\n{s}\n", .{ script, printed });
        return error.TestUnexpectedResult;
    };
    const rest = printed[at + "Path: ".len ..];
    return rest[0 .. std.mem.indexOfScalar(u8, rest, '\n') orelse rest.len];
}

/// A path's text up to its first count or condition; a prelude keeps its inner
/// path's head.
fn head(a: std.mem.Allocator, t: []const u8) ![]const u8 {
    const pre = " read into memory first (";
    if (std.mem.indexOf(u8, t, pre)) |i| {
        const end = std.mem.lastIndexOf(u8, t, "), then serial") orelse t.len;
        return msgfmt.allocPrint(a, "{s}{s}{s})", .{ t[0..i], pre, try head(a, t[i + pre.len .. end]) });
    }
    var end = t.len;
    for ([_][]const u8{ ", ", " (" }) |cut| {
        if (std.mem.indexOf(u8, t, cut)) |i| end = @min(end, i);
    }
    return t[0..end];
}

/// Whether `c` runs and explains as it says, each mismatch printed.
fn caseHolds(a: std.mem.Allocator, tmp: *std.testing.TmpDir, base: []const u8, port: u16, c: Case, split: bool) !bool {
    const conn = try msgfmt.allocPrint(a, "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');\n", .{port});
    const q = try std.mem.replaceOwned(u8, a, c.q, "$B", base);
    const into = if (c.select) "" else try msgfmt.allocPrint(a, "LOAD INTO '{s}/never.csv'{s} AS ", .{ base, if (split) " SPLIT BY (id)" else "" });
    const ran = try head(a, try pathOf(a, tmp.dir, try msgfmt.allocPrint(a, "{s}EXPLAIN ANALYZE {s}{s};", .{ conn, into, q }), c.threads));
    const said = try head(a, try pathOf(a, tmp.dir, try msgfmt.allocPrint(a, "{s}EXPLAIN {s}{s};", .{ conn, into, q }), c.threads));
    const ok = std.mem.eql(u8, c.want, ran) and std.mem.eql(u8, c.static orelse c.want, said);
    if (!ok) std.debug.print("{s}\n  took: {s} (want {s})\n  said: {s} (want {s})\n", .{ c.q, ran, c.want, said, c.static orelse c.want });
    return ok;
}

test "path oracle: EXPLAIN names the path EXPLAIN ANALYZE takes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");

    var input = std.array_list.Managed(u8).init(a);
    try input.appendSlice("id,g,v\n");
    for (0..6000) |i| try input.writer().print("{d},{c},{d}\n", .{ i, "abcde"[i % 5], i % 10 });
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input.items });
    try tmp.dir.writeFile(.{ .sub_path = "k.csv", .data = "g,label\na,A\nb,B\nc,C\n" });
    try tmp.dir.writeFile(.{ .sub_path = "q.csv", .data = "id,note\n1,\"two\nlines\"\n2,plain\n" });
    try tmp.dir.writeFile(.{ .sub_path = "l.txt", .data = "one\ntwo\nthree\nfour\n" });
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "p.parquet" }), 1000, 100);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "p1.parquet" }), 100, 1000);
    try tmp.dir.makeDir("pf");
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "pf", "a.parquet" }), 100, 1000);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "pf", "b.parquet" }), 100, 1000);

    const cols = [_]@import("../../testing/postgres_fake.zig").Col{ .{ .name = "id", .oid = 20 }, .{ .name = "g" }, .{ .name = "v", .oid = 20 } };
    const rows = try pgRows(a);
    const tables = [_]Table{
        .{ .name = "t", .cols = &cols, .rows = rows, .key = "id", .est_rows = 3_000_000 },
        .{ .name = "t2", .cols = &cols, .rows = rows, .key = "id", .est_rows = 3_000_000 },
    };
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    var held = true;
    for (cases) |c| held = try caseHolds(a, &tmp, base, srv.port(), c, false) and held;
    for (split_cases) |c| held = try caseHolds(a, &tmp, base, srv.port(), c, true) and held;
    for (select_cases) |c| held = try caseHolds(a, &tmp, base, srv.port(), c, false) and held;
    try testing.expect(held);
}

test "a key range typed otherwise than the probe fails its lane, on every split path" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "k.csv", .data = "g,label\na,A\nb,B\nc,C\n" });

    const cols = [_]@import("../../testing/postgres_fake.zig").Col{ .{ .name = "id", .oid = 20 }, .{ .name = "g" }, .{ .name = "v", .oid = 20 } };
    const tables = [_]Table{.{ .name = "t", .cols = &cols, .rows = try pgRows(a), .key = "id", .est_rows = 3_000_000 }};
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();
    srv.retype_on = " >= ";
    srv.retype_oid = 701;

    const queries = [_][]const u8{
        "SELECT id, v FROM pg.public.t WHERE v > 1",
        "SELECT t.id, k.label FROM pg.public.t t JOIN '$B/k.csv' k ON t.g = k.g",
        "SELECT g, COUNT(*) AS n FROM pg.public.t GROUP BY g",
    };
    for (queries) |q| {
        const script = try msgfmt.allocPrint(a, "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');\nLOAD INTO '{s}/out.csv' SPLIT BY (id) AS {s};", .{ srv.port(), base, try std.mem.replaceOwned(u8, a, q, "$B", base) });
        var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, script, &pd);
        var diag = env_mod.Diag{};
        var out = try Capture.begin(tmp.dir, std.posix.STDOUT_FILENO, "stdout.txt");
        var err = try Capture.begin(tmp.dir, std.posix.STDERR_FILENO, "stderr.txt");
        const res = run(testing.allocator, prog, .{ .threads = 4, .log = .{ .quiet = true, .summary = .none } }, &diag);
        const printed = try err.end(a);
        _ = try out.end(a);
        if (res) |_| {
            std.debug.print("{s}\n{s}\n", .{ q, printed });
            return error.TestExpectedError;
        } else |e| {
            testing.expectEqual(error.SplitSchemaChanged, e) catch |x| {
                std.debug.print("{s}\n{s}\n{s}\n", .{ q, diag.msg, printed });
                return x;
            };
            try testing.expect(std.mem.indexOf(u8, diag.msg, "as float, the probe as") != null);
        }
    }
}

test "a split-parallel union shown on stdout is one result, as the serial union is" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const cols = [_]@import("../../testing/postgres_fake.zig").Col{ .{ .name = "id", .oid = 20 }, .{ .name = "g" }, .{ .name = "v", .oid = 20 } };
    const rows = (try pgRows(a))[0..3];
    const tables = [_]Table{
        .{ .name = "t", .cols = &cols, .rows = rows, .key = "id", .est_rows = 3_000_000 },
        .{ .name = "t2", .cols = &cols, .rows = rows, .key = "id", .est_rows = 3_000_000 },
    };
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();

    const script = try msgfmt.allocPrint(a, "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');\nSELECT 'a' AS tag, t.* FROM pg.public.t t UNION ALL BY NAME SELECT 'b' AS tag, t.* FROM pg.public.t2 t;", .{srv.port()});
    for ([_]usize{ 1, 4 }) |threads| {
        var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, script, &pd);
        var diag = env_mod.Diag{};
        var out = try Capture.begin(tmp.dir, std.posix.STDOUT_FILENO, "stdout.txt");
        var err = try Capture.begin(tmp.dir, std.posix.STDERR_FILENO, "stderr.txt");
        const res = run(testing.allocator, prog, .{ .threads = threads, .log = .{ .quiet = true, .summary = .none } }, &diag);
        const printed = try err.end(a);
        const shown = try out.end(a);
        _ = res catch |e| {
            std.debug.print("{s}\n{s}\n", .{ diag.msg, printed });
            return e;
        };
        testing.expect(std.mem.count(u8, shown, "tag  id") == 1 and std.mem.count(u8, shown, " rows)") == 1 and
            std.mem.indexOf(u8, shown, "a    1") != null and std.mem.indexOf(u8, shown, "b    1") != null) catch |e| {
            std.debug.print("-j {d}:\n{s}\n", .{ threads, shown });
            return e;
        };
    }
}
