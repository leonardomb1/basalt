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
    .{ .q = "SELECT id, list_cosine_similarity(CAST(concat('[', v, ',1]') AS LIST(DOUBLE)), CAST('[1,2]' AS LIST(DOUBLE))) AS s FROM '$B/p.parquet' ORDER BY s DESC NULLS LAST, id LIMIT 5", .want = "parquet parallel top-n" },
    .{ .q = "SELECT id FROM '$B/in.csv' ORDER BY v, id", .want = "csv parallel sort" },
    .{ .q = "SELECT id, SUM(v) OVER (PARTITION BY g) AS s FROM '$B/in.csv'", .want = "csv parallel window" },
    .{ .q = "SELECT * FROM (SELECT id, ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) AS rn FROM '$B/in.csv') r WHERE rn <= 2", .want = "serial" },
    .{ .q = "SELECT id, x FROM '$B/in.csv' CROSS JOIN UNNEST(SPLIT(g, ',')) AS x", .want = "csv parallel map" },
    .{ .q = "SELECT id, x FROM '$B/p.parquet' CROSS JOIN UNNEST(SPLIT(g, ',')) AS x WHERE v > 2", .want = "parquet parallel map" },
    .{ .q = "SELECT id FROM '$B/in.csv' LIMIT 5", .want = "serial" },
    .{ .q = "SELECT g, string_agg(id, ',') AS s FROM '$B/in.csv' GROUP BY g", .want = "serial" },
    .{ .q = "SELECT g, any_value(id) AS a FROM '$B/p.parquet' GROUP BY g", .want = "serial" },
    .{ .q = "SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS m FROM '$B/p.parquet' GROUP BY g", .want = "parquet parallel aggregate" },
    .{ .q = "SELECT g, COUNT(*) AS n FROM '$B/in.csv' GROUP BY ROLLUP(g)", .want = "serial" },
    .{ .q = "SELECT * FROM '$B/in.csv' PIVOT (SUM(v) FOR g IN ('a'))", .want = "serial" },
    .{ .q = "SELECT id, g FROM '$B/in.csv' QUALIFY ROW_NUMBER() OVER (PARTITION BY g ORDER BY id) = 1", .want = "serial" },
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
    .{ .q = "SELECT id FROM '$B/p.parquet' ORDER BY v, id", .want = "parquet parallel sort" },
    .{ .q = "SELECT COUNT(*) AS n FROM '$B/p.parquet'", .want = "serial" },
    .{ .q = "SELECT COUNT(*) AS n FROM '$B/p.parquet' WHERE v > 0", .want = "parquet parallel aggregate" },
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

/// What `script` prints on stderr, run with `threads` lanes.
fn printedBy(a: std.mem.Allocator, dir: std.fs.Dir, script: []const u8, threads: usize, explain: bool) ![]const u8 {
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var diag = env_mod.Diag{};
    var out = try Capture.begin(dir, std.posix.STDOUT_FILENO, "stdout.txt");
    var err = try Capture.begin(dir, std.posix.STDERR_FILENO, "stderr.txt");
    const res = run(testing.allocator, prog, .{ .threads = threads, .explain = explain, .log = .{ .quiet = true, .summary = .none } }, &diag);
    const printed = try err.end(a);
    _ = try out.end(a);
    _ = res catch |e| {
        std.debug.print("{s}\n{s}\n{s}\n", .{ script, diag.msg, printed });
        return e;
    };
    return printed;
}

/// The `Path:` text `script` prints, run with `threads` lanes.
fn pathOf(a: std.mem.Allocator, dir: std.fs.Dir, script: []const u8, threads: usize) ![]const u8 {
    const printed = try printedBy(a, dir, script, threads, false);
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

const ReportCase = struct {
    script: []const u8,
    threads: usize = 1,
    explain: bool = false,
    says: []const []const u8 = &.{},
};

const report_cases = [_]ReportCase{
    .{ .script = "EXPLAIN ANALYZE SELECT id, v FROM pg.public.t WHERE v > 1;", .says = &.{"scan (#3)  postgres table public.t\n            sent: SELECT \"v\", \"id\" FROM \"public\".\"t\" WHERE (\"v\" > 1)\n"} },
    .{ .script = "EXPLAIN ANALYZE LOAD INTO '$B/never.csv' AS SELECT id, v FROM pg.public.t WHERE v > 1;", .threads = 4, .says = &.{ "outside the plan (actuals)", "postgres lookup", "opened and not read", "sent: once per key range: SELECT" } },
    .{ .script = "EXPLAIN ANALYZE LOAD INTO '$B/never.csv' SPLIT BY (id) AS SELECT g, COUNT(*) AS n FROM pg.public.t GROUP BY g;", .threads = 4, .says = &.{ "outside the plan (actuals)", "sent: once per key range: SELECT" } },
    .{ .script = "EXPLAIN ANALYZE LOAD INTO '$B/never.csv' SPLIT BY (id) AS SELECT t.id, k.g FROM pg.public.t t JOIN '$B/in.csv' k ON t.g = k.g;", .threads = 4, .says = &.{"sent: once per key range: SELECT"} },
    .{ .script = "EXPLAIN ANALYZE SELECT id FROM pg.public.t ORDER BY id LIMIT 3;", .threads = 4, .says = &.{"opened and not read"} },
    .{ .script = "EXPLAIN ANALYZE SELECT s.v, c.g FROM '$B/in.csv' s JOIN pg.public.t c ON c.id = s.id;", .says = &.{"schema probe"} },
    .{ .script = "EXPLAIN ANALYZE SELECT s.v, c.g FROM '$B/in.csv' s JOIN pg.public.t c ON c.id = s.id;", .threads = 4 },
    .{ .script = "EXPLAIN ANALYZE SELECT * FROM EACH TABLE OF (pg.QUERY($$SELECT \"tname\", \"tag\" FROM catalog$$));", .says = &.{ "postgres query, discovery", "union (#", "2 branches, from discovery", "postgres table t\n", "postgres table t2\n" } },
    .{ .script = "EXPLAIN ANALYZE LOAD INTO '$B/never.csv' AS SELECT * FROM EACH TABLE OF (pg.QUERY($$SELECT \"tname\", \"tag\" FROM catalog$$));", .threads = 4, .says = &.{"postgres query, discovery"} },
    .{ .script = "EXPLAIN ANALYZE SELECT * FROM EACH TABLE OF (SELECT tname, tag FROM pg.public.catalog);", .says = &.{"2 branches, from discovery"} },
    .{ .script = "EXPLAIN ANALYZE SELECT id FROM '$B/in.csv' WHERE v = (SELECT MAX(v) AS m FROM pg.public.t);", .says = &.{"scalar subquery (actuals)"} },
    .{ .script = "LET top = (SELECT MAX(v) AS m FROM pg.public.t);\nLOAD INTO '$B/never.csv' AS SELECT id FROM '$B/in.csv' WHERE v = $top;", .explain = true, .says = &.{"LET `top` (actuals)"} },
    .{ .script = "FOR EACH ROW OF (SELECT tname, tag FROM pg.public.catalog) AS (a, b)\n  LOAD INTO '$B/never.csv' AS SELECT id FROM pg.public.t;\nEND FOR;", .explain = true, .says = &.{"FOR EACH discovery (actuals)"} },
    .{ .script = "FOR EACH ROW OF (pg.QUERY($$SELECT \"tname\", \"tag\" FROM catalog$$)) AS (a, b)\n  LOAD INTO '$B/never.csv' AS SELECT id FROM pg.public.t;\nEND FOR;", .explain = true, .says = &.{ "FOR EACH discovery (actuals)", "postgres query, discovery", "FOR EACH 2 rows of 2 share this plan, its figures summed over them: a=t, b=a; a=t2, b=b" } },
    .{ .script = "EXPLAIN ANALYZE LOAD INTO '$B/never.xlsx' AS SELECT a.id FROM '$B/in.csv' a JOIN '$B/twice.csv' b ON a.g = b.g;", .says = &.{ "Sink: xlsx ", "(2.0x)", "project (#1)  id\n" } },
    .{ .script = "EXPLAIN ANALYZE SELECT g, string_agg(id, ',') AS s FROM pg.public.t GROUP BY g;", .threads = 4 },
    .{ .script = "EXPLAIN ANALYZE SELECT * FROM pg.public.t PIVOT (SUM(v) FOR g IN ('a'));", .threads = 4, .says = &.{"by the other columns"} },
    .{ .script = "EXPLAIN ANALYZE SELECT g, COUNT(*) AS n FROM pg.public.t GROUP BY ROLLUP(g);", .threads = 4 },
    .{ .script = "EXPLAIN ANALYZE SELECT id, v FROM pg.public.t UNPIVOT (x FOR w IN (g));", .threads = 4 },
    .{ .script = "DESCRIBE pg.public.t;", .explain = true, .says = &.{"DESCRIBE (actuals)"} },
    .{ .script = "CREATE FUNCTION f(a) AS\n  LOAD INTO '$B/never.csv' AS SELECT id FROM pg.public.t;\nEND;\nCALL f('x');", .explain = true, .says = &.{"CALL f(a=x)"} },
    .{ .script = "CASE WHEN 1 = 2 THEN PRINT 'no'; WHEN 1 = 1 THEN LOAD INTO '$B/never.csv' AS SELECT id FROM pg.public.t; END CASE;", .explain = true, .says = &.{"CASE: arm 2 of 2"} },
    .{ .script = "LOAD INTO '$B/d/' WITH (format = 'delta') AS SELECT id, v FROM '$B/in.csv';\nEXPLAIN ANALYZE SELECT id FROM '$B/d/';", .says = &.{ "log replay (#", "live files: ", "└─ log read (#", "├─ list (#", "entries: " } },
    .{ .script = "EXPLAIN ANALYZE LOAD INTO '$B/d/' WITH (merge_schema = true) UPSERT ON (id) USING changes (sequence_by = id, position = id) AS SELECT id, v FROM '$B/in.csv';", .says = &.{ "log replay (#", "Sink: delta " } },
    .{ .script = "EXPLAIN ANALYZE SELECT g, SUM(v) AS s FROM '$B/pf/' GROUP BY g;", .threads = 4, .says = &.{ "list (#", "footer (#", "reads: " } },
    .{ .script = "EXPLAIN ANALYZE SELECT id FROM '$B/p.parquet' WHERE v > 2;", .threads = 4, .says = &.{"footer (#"} },
};

fn count(hay: []const u8, needle: []const u8) usize {
    return std.mem.count(u8, hay, needle);
}

/// Whether the server got no query `printed` does not show: each on a `sent:` line of
/// its own as many times as it went out, a key range's holding the query shown once
/// for them all, and an explained loop's rows that share a plan showing it once.
fn reportShowsAll(a: std.mem.Allocator, printed: []const u8, got: []const []const u8) !bool {
    var ranges = std.array_list.Managed([]const u8).init(a);
    var lines = std.mem.splitScalar(u8, printed, '\n');
    while (lines.next()) |ln| {
        const mark = "sent: once per key range: ";
        const at = std.mem.indexOf(u8, ln, mark) orelse continue;
        try ranges.append(ln[at + mark.len ..]);
    }
    var seen = std.StringHashMap(usize).init(a);
    for (got) |q| (try seen.getOrPutValue(try @import("explain.zig").sentText(a, q), 0)).value_ptr.* += 1;
    var held = true;
    var it = seen.iterator();
    while (it.next()) |e| {
        const shown = count(printed, try msgfmt.allocPrint(a, "sent: {s}\n", .{e.key_ptr.*}));
        if (shown == e.value_ptr.*) continue;
        if (shown >= 1 and std.mem.indexOf(u8, printed, "share this plan") != null) continue;
        const in_range = shown == 0 and for (ranges.items) |r| {
            if (std.mem.indexOf(u8, e.key_ptr.*, r) != null) break true;
        } else false;
        if (in_range) continue;
        std.debug.print("sent {d}x, shown {d}x: {s}\n", .{ e.value_ptr.*, shown, e.key_ptr.* });
        held = false;
    }
    return held;
}

test "the report shows every query the server got, once, on its scan or in a tree of its own" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = "id,g,v\n1,a,1\n2,b,2\n3,c,3\n" });
    try tmp.dir.writeFile(.{ .sub_path = "twice.csv", .data = "g\na\na\nb\nb\nc\nc\n" });
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "p.parquet" }), 1000, 100);
    try tmp.dir.makeDir("pf");
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "pf", "a.parquet" }), 100, 50);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "pf", "b.parquet" }), 100, 50);

    const Col = @import("../../testing/postgres_fake.zig").Col;
    const cols = [_]Col{ .{ .name = "id", .oid = 20 }, .{ .name = "g" }, .{ .name = "v", .oid = 20 } };
    const rows = try pgRows(a);
    const cat_cols = [_]Col{ .{ .name = "tname" }, .{ .name = "tag" } };
    const cat_rows = [_][]const ?[]const u8{ &.{ "t", "a" }, &.{ "t2", "b" } };
    const tables = [_]Table{
        .{ .name = "t", .cols = &cols, .rows = rows, .key = "id", .est_rows = 3_000_000 },
        .{ .name = "t2", .cols = &cols, .rows = rows, .key = "id", .est_rows = 3_000_000 },
        .{ .name = "catalog", .cols = &cat_cols, .rows = &cat_rows },
    };
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();
    const conn = try msgfmt.allocPrint(a, "CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');\n", .{srv.port()});

    var held = true;
    for (report_cases) |c| {
        const before = (try srv.received(a)).len;
        const script = try msgfmt.allocPrint(a, "{s}{s}", .{ conn, try std.mem.replaceOwned(u8, a, c.script, "$B", base) });
        const printed = try printedBy(a, tmp.dir, script, c.threads, c.explain);
        const got = (try srv.received(a))[before..];
        var ok = try reportShowsAll(a, printed, got);
        for (c.says) |s| if (std.mem.indexOf(u8, printed, s) == null) {
            std.debug.print("missing: {s}\n", .{s});
            ok = false;
        };
        if (!ok) std.debug.print("{s}\n{s}\n", .{ c.script, printed });
        held = ok and held;
    }
    try testing.expect(held);
}

test "a union and each join after it ask their tables only for the columns the query takes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");

    const Col = @import("../../testing/postgres_fake.zig").Col;
    const head_cols = [_]Col{ .{ .name = "resp" }, .{ .name = "dept" }, .{ .name = "unused_h" }, .{ .name = "deleted" } };
    const head_rows = [_][]const ?[]const u8{&.{ "1", "d", "x", "" }};
    const emp_cols = [_]Col{ .{ .name = "mat" }, .{ .name = "nome" }, .{ .name = "depto" }, .{ .name = "unused_e" }, .{ .name = "deleted" } };
    const emp_rows = [_][]const ?[]const u8{&.{ "1", "n", "d", "x", "" }};
    const cat_cols = [_]Col{ .{ .name = "tname" }, .{ .name = "tag" } };
    const cat_rows = [_][]const ?[]const u8{ &.{ "h1", "a" }, &.{ "h2", "b" } };
    const tables = [_]Table{
        .{ .name = "h1", .cols = &head_cols, .rows = &head_rows },
        .{ .name = "h2", .cols = &head_cols, .rows = &head_rows },
        .{ .name = "emp", .cols = &emp_cols, .rows = &emp_rows },
        .{ .name = "catalog", .cols = &cat_cols, .rows = &cat_rows },
    };
    const srv = try FakePg.start(testing.allocator, &tables);
    defer srv.finish();
    const script = try msgfmt.allocPrint(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = '127.0.0.1', port = {d}, database = 'd', user = 'u', password = 'p');
        \\LOAD INTO '{s}/out.csv' AS
        \\WITH h AS (
        \\  SELECT resp, dept, grp FROM EACH TABLE OF (pg.QUERY($$SELECT "tname", "tag" FROM catalog$$)) AS (tname, grp)
        \\  WITH (schema_from = 'h1')
        \\  WHERE trim(resp) <> '' AND deleted <> '*'
        \\),
        \\s AS (
        \\  SELECT h.grp, e.mat, e.nome, m.nome AS boss
        \\  FROM h
        \\  INNER JOIN pg.public.emp AS e ON trim(e.depto) = trim(h.dept) AND e.deleted <> '*'
        \\  LEFT JOIN pg.public.emp AS m ON trim(m.mat) = trim(h.resp) AND m.deleted <> '*'
        \\)
        \\SELECT * FROM s;
    , .{ srv.port(), base });
    const printed = try printedBy(a, tmp.dir, script, 1, false);
    errdefer std.debug.print("{s}\n", .{printed});
    const got = try srv.received(a);
    var bad = false;
    var late_reads: usize = 0;
    for (got) |q| {
        if (std.mem.indexOf(u8, q, "1 = 0") != null or std.mem.indexOf(u8, q, "catalog") != null or std.mem.indexOf(u8, q, "pg_attribute") != null) continue;
        late_reads += 1;
        if (std.mem.indexOf(u8, q, "SELECT *") == null and std.mem.indexOf(u8, q, "unused_") == null) continue;
        std.debug.print("reads more than it takes: {s}\n", .{q});
        bad = true;
    }
    try testing.expect(!bad);
    try testing.expectEqual(@as(usize, 4), late_reads);
    var seen = std.StringHashMap(void).init(a);
    for (got) |q| if (std.mem.indexOf(u8, q, "1 = 0") != null) {
        if ((try seen.getOrPut(q)).found_existing) {
            std.debug.print("asked twice: {s}\n", .{q});
            bad = true;
        }
    };
    try testing.expect(!bad);
    try testing.expectEqualStrings("grp,mat,nome,boss\nb,1,n,n\na,1,n,n\n", try sortedOut(a, tmp.dir));
}

test "a parallel map and join reads from its head file only the columns the stages after the join take" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "p.parquet" }), 1000, 100);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "k.parquet" }), 10, 100);

    const q = try msgfmt.allocPrint(a, "SELECT p.id, k.v FROM '{s}/p.parquet' p JOIN '{s}/k.parquet' k ON p.v = k.id AND p.id > k.v * 100", .{ base, base });
    const printed = try printedBy(a, tmp.dir, try msgfmt.allocPrint(a, "EXPLAIN ANALYZE LOAD INTO '{s}/never.csv' AS {s};", .{ base, q }), 4, true);
    errdefer std.debug.print("{s}\n", .{printed});
    const at = std.mem.indexOf(u8, printed, "Path: ").? + "Path: ".len;
    try testing.expectEqualStrings("parquet parallel map+join", try head(a, printed[at .. at + std.mem.indexOfScalar(u8, printed[at..], '\n').?]));
    try testing.expect(std.mem.indexOf(u8, printed, "Columns read: 4 of 6") != null);

    _ = try printedBy(a, tmp.dir, try msgfmt.allocPrint(a, "LOAD INTO '{s}/out.csv' AS SELECT COUNT(*) AS n, SUM(id) AS s, SUM(v) AS t FROM ({s}) j;", .{ base, q }), 4, false);
    try testing.expectEqualStrings("n,s,t\n549,356400,1650\n", try tmp.dir.readFileAlloc(a, "out.csv", 1 << 10));
}

test "the joins held before a turned last join choose their sides by the rule every join takes, and turn their own last join, as EXPLAIN ANALYZE shows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "big.parquet" }), 300_000, 10_000);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "mid.parquet" }), 40_000, 1000);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "wide.parquet" }), 300_000, 10_000);
    try tmp.dir.writeFile(.{ .sub_path = "c.csv", .data = "cv,seg,nk\n0,x,0\n1,y,1\n2,x,1\n3,y,1\n4,x,1\n5,y,1\n6,x,1\n7,y,1\n8,x,1\n9,y,1\n" });
    try tmp.dir.writeFile(.{ .sub_path = "n.csv", .data = "nk,name\n0,n0\n1,n1\n" });

    const q3 = "SELECT o.g, COUNT(*) AS n, SUM(b.v) AS t FROM '$B/c.csv' c JOIN '$B/mid.parquet' o ON o.v = c.cv$W JOIN '$B/big.parquet' b ON b.id = o.id WHERE c.seg = 'x' GROUP BY o.g";
    const four = "SELECT o.g, COUNT(*) AS n, SUM(b.v) AS t FROM '$B/n.csv' n JOIN '$B/c.csv' c ON c.nk = n.nk WITH (join_order = 'written') JOIN '$B/wide.parquet' o ON o.v = c.cv$W JOIN '$B/big.parquet' b ON b.id = o.id WHERE n.name = 'n0' GROUP BY o.g";
    const Want = struct { q: []const u8, w: []const u8, held: usize };
    for ([_]Want{
        .{ .q = q3, .w = "", .held = 2 },
        .{ .q = q3, .w = " WITH (join_order = 'written')", .held = 1 },
        .{ .q = four, .w = "", .held = 2 },
        .{ .q = four, .w = " WITH (join_order = 'written')", .held = 1 },
    }) |c| {
        const q = try std.mem.replaceOwned(u8, a, try std.mem.replaceOwned(u8, a, c.q, "$W", c.w), "$B", base);
        const printed = try printedBy(a, tmp.dir, try msgfmt.allocPrint(a, "EXPLAIN ANALYZE LOAD INTO '{s}/never.csv' AS {s};", .{ base, q }), 4, true);
        errdefer std.debug.print("{s}\n", .{printed});
        try testing.expect(std.mem.indexOf(u8, printed, "read into memory first") != null);
        try testing.expectEqual(c.held, std.mem.count(u8, printed, "build: left"));
    }
    var want: ?[]const u8 = null;
    for ([_][]const u8{ " WITH (join_order = 'written')", "" }) |w| for ([_]usize{ 1, 4 }) |threads| {
        const q = try std.mem.replaceOwned(u8, a, try std.mem.replaceOwned(u8, a, four, "$W", w), "$B", base);
        _ = try printedBy(a, tmp.dir, try msgfmt.allocPrint(a, "LOAD INTO '{s}/out.csv' AS {s} ORDER BY o.g;", .{ base, q }), threads, false);
        const got = try tmp.dir.readFileAlloc(a, "out.csv", 1 << 16);
        if (want) |x| try testing.expectEqualStrings(x, got) else want = got;
    };
    try testing.expect(std.mem.count(u8, want.?, "\n") == 2);
}

/// `out.csv`'s header and its rows in reverse order, the order of the rows being a
/// union's, so a test reads one spelling of them.
fn sortedOut(a: std.mem.Allocator, dir: std.fs.Dir) ![]const u8 {
    const text = try dir.readFileAlloc(a, "out.csv", 1 << 20);
    const nl = std.mem.indexOfScalar(u8, text, '\n').?;
    var rows = std.array_list.Managed([]const u8).init(a);
    var it = std.mem.splitScalar(u8, std.mem.trimRight(u8, text[nl + 1 ..], "\n"), '\n');
    while (it.next()) |r| try rows.append(r);
    std.sort.pdq([]const u8, rows.items, {}, struct {
        fn gt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, y, x);
        }
    }.gt);
    var out = std.array_list.Managed(u8).init(a);
    try out.appendSlice(text[0 .. nl + 1]);
    for (rows.items) |r| {
        try out.appendSlice(r);
        try out.append('\n');
    }
    return out.items;
}

test "the report counts an HTTP read's pages on its scan, and shows no query value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const http_fake = @import("../../testing/http_fake.zig");
    var srv: http_fake.FakeServer = undefined;
    try srv.start(&.{
        http_fake.FakeServer.reply("200 OK", "", "[{\"id\":1}]"),
        http_fake.FakeServer.reply("200 OK", "", "[{\"id\":2}]"),
        http_fake.FakeServer.reply("200 OK", "", "[]"),
    });
    defer srv.stop();
    const url = try srv.url(a, "/items?token=hidden");
    const printed = try printedBy(a, tmp.dir, try msgfmt.allocPrint(a, "LET n = 1;\nEXPLAIN ANALYZE SELECT * FROM HTTP('{s}') WITH (paginate = 'page');", .{url}), 1, false);
    errdefer std.debug.print("{s}\n", .{printed});
    try testing.expect(std.mem.indexOf(u8, printed, "pages: 3") != null);
    try testing.expect(std.mem.indexOf(u8, printed, "token=...") != null);
    try testing.expect(std.mem.indexOf(u8, printed, "hidden") == null);
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
