//! The aggregate's results across `-j`: the same queries over a CSV, a Parquet
//! file, a Parquet folder and an NDJSON file cut into many small morsels give
//! byte-identical files at -j 4 and 16, float sums included, and exact ones the
//! same at -j 1, whose one lane runs the serial aggregate. Keys run just past the
//! lane table's and the parallel combine's bounds, single and multi-column, so the
//! parallel merge and the partitioned write run on small inputs.

const std = @import("std");
const column = @import("../../exec/column.zig");
const env_mod = @import("../env.zig");
const msgfmt = @import("../../format/msgfmt.zig");
const parser = @import("../../lang/sql_parser.zig");
const pqwrite = @import("../../format/parquet/write.zig");
const run = @import("../run.zig").run;
const types = @import("../../lang/types.zig");

const testing = std.testing;

const rows = 40_000;

const schema = types.Schema{ .fields = &.{
    .{ .name = "id", .ty = types.Type.init(.int) },
    .{ .name = "k", .ty = types.Type.init(.int) },
    .{ .name = "g", .ty = types.Type.init(.string) },
    .{ .name = "f", .ty = types.Type.init(.float) },
} };

fn key(i: usize) i64 {
    return @intCast((i * 7919) % 17_000);
}

fn group(i: usize) []const u8 {
    return "abcdefg"[i % 7 ..][0..1];
}

/// Mixed magnitudes, so a sum's last bits follow the order it adds in.
fn value(i: usize) f64 {
    const x: f64 = @floatFromInt(i);
    return @as(f64, @floatFromInt(i % 13)) * 1.0e9 + x * 0.001 + 1.0 / (x + 3.0);
}

fn writeCsv(dir: std.fs.Dir, name: []const u8, a: std.mem.Allocator) !void {
    var out = std.array_list.Managed(u8).init(a);
    try out.appendSlice("id,k,g,f\n");
    for (0..rows) |i| try out.writer().print("{d},{d},{s},{d}\n", .{ i, key(i), group(i), value(i) });
    try dir.writeFile(.{ .sub_path = name, .data = out.items });
}

fn writeNdjson(dir: std.fs.Dir, name: []const u8, a: std.mem.Allocator) !void {
    var out = std.array_list.Managed(u8).init(a);
    for (0..rows / 4) |i| try out.writer().print("{{\"id\":{d},\"k\":{d},\"g\":\"{s}\",\"f\":{e}}}\n", .{ i, key(i), group(i), value(i) });
    try dir.writeFile(.{ .sub_path = name, .data = out.items });
}

fn writeParquet(a: std.mem.Allocator, path: []const u8, lo: usize, hi: usize) !void {
    var w = try pqwrite.Writer.open(a, path, schema, .snappy, .truncate);
    w.group_rows = 997;
    var cols: [4]column.Builder = undefined;
    for (&cols, schema.fields) |*c, f| c.* = column.Builder.init(a, f.ty);
    for (lo..hi) |i| {
        try cols[0].append(.{ .int = @intCast(i) });
        try cols[1].append(.{ .int = key(i) });
        try cols[2].append(.{ .string = group(i) });
        try cols[3].append(.{ .float = value(i) });
    }
    var done: [4]column.Column = undefined;
    for (&done, &cols) |*d, *c| d.* = try c.finish();
    try w.writeBatch(a, .{ .schema = &schema, .columns = &done, .len = hi - lo });
    try w.close();
}

/// A query, and whether its every column is exact, so one lane's serial aggregate
/// gives it byte for byte too.
const Query = struct { q: []const u8, exact: bool = false };

const queries = [_]Query{
    .{ .q = "SELECT k, g, SUM(f) AS s, AVG(f) AS m, COUNT(*) AS n, MIN(f) AS lo FROM $IN GROUP BY k, g ORDER BY k, g" },
    .{ .q = "SELECT COUNT(*) AS n, SUM(f) AS s, AVG(f) AS m, VARIANCE(f) AS v, STDDEV(f) AS d FROM $IN" },
    .{ .q = "SELECT g, SUM(f) AS s, COUNT(DISTINCT k) AS dk, MEDIAN(f) AS md FROM $IN GROUP BY g HAVING COUNT(*) > 10 ORDER BY g" },
    .{ .q = "SELECT k, SUM(f) AS s FROM $IN GROUP BY k ORDER BY s DESC, k LIMIT 7" },
    .{ .q = "SELECT l.label, SUM(t.f) AS s, COUNT(*) AS n FROM $IN t JOIN '$B/labels.csv' l ON t.g = l.g GROUP BY l.label ORDER BY l.label" },
    .{ .q = "SELECT id % 3 AS r, SUM(f) AS s FROM $IN WHERE k > 100 GROUP BY r ORDER BY r" },
    .{ .q = "SELECT COUNT(*) AS n, SUM(s) AS t FROM (SELECT g, SUM(f) AS s FROM $IN GROUP BY g) d" },
    .{ .q = "SELECT CAST(k AS VARCHAR) AS ks, CAST(f AS DECIMAL(18,3)) AS fd, COUNT(*) AS n FROM $IN GROUP BY ks, fd ORDER BY ks, fd", .exact = true },
    .{ .q = "SELECT SUM(CAST(f AS DECIMAL(18,3))) AS ds, COUNT(DISTINCT g) AS dg FROM $IN", .exact = true },
    .{ .q = "SELECT k, g, COUNT(*) AS n, MIN(f) AS lo, MAX(id) AS hi, COUNT(DISTINCT id % 5) AS d, MEDIAN(f) AS md FROM $IN GROUP BY k, g HAVING COUNT(*) > 1 ORDER BY k, g", .exact = true },
    .{ .q = "SELECT l.label, COUNT(*) AS n, MAX(t.f) AS mx FROM $IN t JOIN '$B/labels.csv' l ON t.g = l.g GROUP BY l.label ORDER BY l.label", .exact = true },
    .{ .q = "SELECT k, MAX(f) AS mx FROM $IN GROUP BY k ORDER BY mx DESC, k LIMIT 7", .exact = true },
    .{ .q = "SELECT COUNT(*) AS n, MAX(c) AS t FROM (SELECT g, COUNT(*) AS c FROM $IN GROUP BY g) d", .exact = true },
};

/// The file `q` writes over `input` at `threads`, morsels of `morsel_bytes`.
fn resultOf(a: std.mem.Allocator, dir: std.fs.Dir, base: []const u8, input: []const u8, q: []const u8, threads: usize, opts: env_mod.RunOptions) ![]const u8 {
    const sel = try std.mem.replaceOwned(u8, a, try std.mem.replaceOwned(u8, a, q, "$IN", input), "$B", base);
    const out = try msgfmt.allocPrint(a, "out_{d}.csv", .{threads});
    const script = try msgfmt.allocPrint(a, "LOAD INTO '{s}/{s}' AS {s};", .{ base, out, sel });
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, script, &pd);
    var diag = env_mod.Diag{};
    var o = opts;
    o.threads = threads;
    o.log = .{ .quiet = true, .summary = .none };
    _ = run(testing.allocator, prog, o, &diag) catch |e| {
        std.debug.print("{s}\n{s}\n", .{ script, diag.msg });
        return e;
    };
    return dir.readFileAlloc(a, out, 1 << 24);
}

fn sameAtEveryJ(a: std.mem.Allocator, dir: std.fs.Dir, base: []const u8, inputs: []const []const u8, opts: env_mod.RunOptions) !void {
    var held = true;
    for (inputs) |input| for (queries) |c| {
        const four = try resultOf(a, dir, base, input, c.q, 4, opts);
        const sixteen = try resultOf(a, dir, base, input, c.q, 16, opts);
        if (!std.mem.eql(u8, four, sixteen)) {
            held = false;
            std.debug.print("{s} over {s}: -j 16 differs from -j 4\n", .{ c.q, input });
        }
        if (!c.exact) continue;
        if (!std.mem.eql(u8, four, try resultOf(a, dir, base, input, c.q, 1, opts))) {
            held = false;
            std.debug.print("{s} over {s}: -j 1 differs from -j 4\n", .{ c.q, input });
        }
    };
    try testing.expect(held);
}

test "an aggregate over many morsels is byte-identical at -j 4 and 16, and at -j 1 where exact" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");

    try writeCsv(tmp.dir, "in.csv", a);
    try tmp.dir.writeFile(.{ .sub_path = "labels.csv", .data = "g,label\na,A\nb,B\nc,C\nd,D\ne,E\n" });
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "in.parquet" }), 0, rows);
    try tmp.dir.makeDir("pf");
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "pf", "a.parquet" }), 0, rows / 3);
    try writeParquet(a, try std.fs.path.join(a, &.{ base, "pf", "b.parquet" }), rows / 3, rows);
    try writeNdjson(tmp.dir, "in.ndjson", a);

    const inputs = [_][]const u8{
        try msgfmt.allocPrint(a, "'{s}/in.csv'", .{base}),
        try msgfmt.allocPrint(a, "'{s}/in.parquet'", .{base}),
        try msgfmt.allocPrint(a, "'{s}/pf/'", .{base}),
        try msgfmt.allocPrint(a, "'{s}/in.ndjson'", .{base}),
    };
    try sameAtEveryJ(a, tmp.dir, base, &inputs, .{ .morsel_bytes = 8 << 10 });
}

test "a grouped aggregate past --op-memory partitions its rows to disk and gives -j 1's result, floats and DISTINCT included" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeCsv(tmp.dir, "in.csv", a);
    const opts = env_mod.RunOptions{ .morsel_bytes = 16 << 10, .op_memory = 1 << 20, .spill_dir = base };

    const q = try msgfmt.allocPrint(a, "SELECT k, COUNT(*) AS n, MIN(id) AS lo, SUM(f) AS s, AVG(f) AS m FROM '{s}/in.csv' GROUP BY k ORDER BY k", .{base});
    const one = try resultOf(a, tmp.dir, base, "", q, 1, opts);
    const four = try resultOf(a, tmp.dir, base, "", q, 4, opts);
    try testing.expectEqualStrings(one, four);
    try testing.expect(std.mem.count(u8, one, "\n") == 17_001);

    const dq = try msgfmt.allocPrint(a, "SELECT k, COUNT(DISTINCT g) AS gs FROM '{s}/in.csv' GROUP BY k ORDER BY k", .{base});
    var roomy = opts;
    roomy.op_memory = 4 << 20;
    try testing.expectEqualStrings(try resultOf(a, tmp.dir, base, "", dq, 1, roomy), try resultOf(a, tmp.dir, base, "", dq, 4, roomy));

    const unordered = try msgfmt.allocPrint(a, "SELECT k, SUM(f) AS s FROM '{s}/in.csv' GROUP BY k", .{base});
    const four_u = try resultOf(a, tmp.dir, base, "", unordered, 4, opts);
    const sixteen_u = try resultOf(a, tmp.dir, base, "", unordered, 16, opts);
    try testing.expectEqualStrings(four_u, sixteen_u);

    const whole = try msgfmt.allocPrint(a, "SELECT COUNT(DISTINCT k) AS ks, SUM(f) AS s FROM '{s}/in.csv'", .{base});
    try testing.expectEqualStrings(try resultOf(a, tmp.dir, base, "", whole, 1, opts), try resultOf(a, tmp.dir, base, "", whole, 4, opts));
}
