//! The aggregate's results at every `-j`: the same queries over a CSV, a Parquet
//! file and a Parquet folder cut into many small morsels give byte-identical files
//! at -j 1, 4 and 16, float sums included. Keys run just past the lane table's and
//! the parallel combine's bounds, single and multi-column, so the parallel merge and
//! the partitioned write run on small inputs.

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

const queries = [_][]const u8{
    "SELECT k, g, SUM(f) AS s, AVG(f) AS m, COUNT(*) AS n, MIN(f) AS lo FROM $IN GROUP BY k, g ORDER BY k, g",
    "SELECT COUNT(*) AS n, SUM(f) AS s, AVG(f) AS m, VARIANCE(f) AS v, STDDEV(f) AS d FROM $IN",
    "SELECT g, SUM(f) AS s, COUNT(DISTINCT k) AS dk, MEDIAN(f) AS md FROM $IN GROUP BY g HAVING COUNT(*) > 10 ORDER BY g",
    "SELECT k, SUM(f) AS s FROM $IN GROUP BY k ORDER BY s DESC, k LIMIT 7",
    "SELECT CAST(k AS VARCHAR) AS ks, CAST(f AS DECIMAL(18,3)) AS fd, COUNT(*) AS n FROM $IN GROUP BY ks, fd ORDER BY ks, fd",
    "SELECT SUM(CAST(f AS DECIMAL(18,3))) AS ds, COUNT(DISTINCT g) AS dg FROM $IN",
    "SELECT l.label, SUM(t.f) AS s, COUNT(*) AS n FROM $IN t JOIN '$B/labels.csv' l ON t.g = l.g GROUP BY l.label ORDER BY l.label",
    "SELECT id % 3 AS r, SUM(f) AS s FROM $IN WHERE k > 100 GROUP BY r ORDER BY r",
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
    for (inputs) |input| for (queries) |q| {
        const one = try resultOf(a, dir, base, input, q, 1, opts);
        for ([_]usize{ 4, 16 }) |j| {
            const many = try resultOf(a, dir, base, input, q, j, opts);
            if (!std.mem.eql(u8, one, many)) {
                held = false;
                std.debug.print("{s} over {s}: -j {d} differs from -j 1\n", .{ q, input, j });
            }
        }
    };
    try testing.expect(held);
}

test "an aggregate over many morsels is byte-identical at -j 1, 4 and 16" {
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

    const inputs = [_][]const u8{
        try msgfmt.allocPrint(a, "'{s}/in.csv'", .{base}),
        try msgfmt.allocPrint(a, "'{s}/in.parquet'", .{base}),
        try msgfmt.allocPrint(a, "'{s}/pf/'", .{base}),
    };
    try sameAtEveryJ(a, tmp.dir, base, &inputs, .{ .morsel_bytes = 16 << 10 });
}

test "an aggregate past --op-memory runs serially and spills, the same at -j 1 and 4" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    try writeCsv(tmp.dir, "in.csv", a);

    const q = try msgfmt.allocPrint(a, "SELECT k, COUNT(*) AS n, MIN(id) AS lo FROM '{s}/in.csv' GROUP BY k ORDER BY k", .{base});
    const opts = env_mod.RunOptions{ .morsel_bytes = 16 << 10, .op_memory = 64 << 10, .spill_dir = base };
    const one = try resultOf(a, tmp.dir, base, "", q, 1, opts);
    const four = try resultOf(a, tmp.dir, base, "", q, 4, opts);
    try testing.expectEqualStrings(one, four);
    try testing.expect(std.mem.count(u8, one, "\n") == 17_001);
}
