//! Prints kcov's merged coverage of src/ per folder: `coverage_summary <coverage.json> <src dir>`.
//! kcov counts every instrumented line, test blocks included, so the figures run a
//! little above the share of product code the tests reach.

const std = @import("std");

const File = struct { file: []const u8, covered_lines: []const u8, total_lines: []const u8 };
const Report = struct { files: []const File, percent_covered: []const u8 };

pub fn main() !void {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const args = try std.process.argsAlloc(a);
    if (args.len != 3) return error.Usage;
    const text = try std.fs.cwd().readFileAlloc(a, args[1], 64 << 20);
    const report = try std.json.parseFromSliceLeaky(Report, a, text, .{ .ignore_unknown_fields = true });
    const src = args[2];

    var folders = std.StringArrayHashMap([2]u64).init(a);
    var covered: u64 = 0;
    var total: u64 = 0;
    for (report.files) |f| {
        if (!std.mem.startsWith(u8, f.file, src)) continue;
        const rel = std.mem.trimLeft(u8, f.file[src.len..], "/");
        const folder = if (std.mem.indexOfScalar(u8, rel, '/')) |i| rel[0..i] else "(root)";
        const c = try std.fmt.parseInt(u64, f.covered_lines, 10);
        const t = try std.fmt.parseInt(u64, f.total_lines, 10);
        const e = try folders.getOrPut(folder);
        if (!e.found_existing) e.value_ptr.* = .{ 0, 0 };
        e.value_ptr[0] += c;
        e.value_ptr[1] += t;
        covered += c;
        total += t;
    }

    const Sort = struct {
        keys: []const []const u8,
        pub fn lessThan(ctx: @This(), x: usize, y: usize) bool {
            return std.mem.lessThan(u8, ctx.keys[x], ctx.keys[y]);
        }
    };
    folders.sort(Sort{ .keys = folders.keys() });

    var buf: [4096]u8 = undefined;
    var w = std.fs.File.stdout().writer(&buf);
    const out = &w.interface;
    for (folders.keys(), folders.values()) |k, v| {
        if (v[1] == 0) continue;
        try out.print("  src/{s: <10} {d: >5.1}%  {d: >6} / {d}\n", .{ k, pct(v[0], v[1]), v[0], v[1] });
    }
    try out.print("  {s: <14} {d: >5.1}%  {d: >6} / {d}\n", .{ "total", pct(covered, total), covered, total });
    try out.flush();
}

fn pct(c: u64, t: u64) f64 {
    return if (t == 0) 0 else 100.0 * @as(f64, @floatFromInt(c)) / @as(f64, @floatFromInt(t));
}
