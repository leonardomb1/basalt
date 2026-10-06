//! Test helpers shared by the tests of analyze.zig's parts.

const Diag = @import("../analyze.zig").Diag;
const Plan = @import("../analyze.zig").Plan;
const analyze = @import("../analyze.zig").analyze;
const ast = @import("../../lang/ast.zig");
const parse = @import("../analyze.zig").parse;
const std = @import("std");
/// Analyze `LOAD INTO ... AS <query over a 2-col CSV>` offline and expect an error
/// whose message contains `msg`. `$IN` in the query is the input CSV's path.
pub fn expectAnalyzeErr(a: std.mem.Allocator, csv_data: []const u8, query: []const u8, msg: []const u8) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = csv_data });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const q = try std.mem.replaceOwned(u8, a, query, "$IN", in);
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS {s};", .{q});
    var diag = Diag{};
    try std.testing.expectError(error.AnalyzeFailed, analyze(a, try parse(a, src), &diag));
    if (std.mem.indexOf(u8, diag.msg, msg) == null) {
        std.debug.print("expected a message containing `{s}`, got `{s}`\n", .{ msg, diag.msg });
        return error.TestUnexpectedResult;
    }
}

pub fn analyzeCsv(a: std.mem.Allocator, csv_data: []const u8, query: []const u8, diag: *Diag) !Plan {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = csv_data });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const in = try std.fs.path.join(a, &.{ base, "in.csv" });
    const q = try std.mem.replaceOwned(u8, a, query, "$IN", in);
    const src = try std.fmt.allocPrint(a, "LOAD INTO '/tmp/x.csv' AS {s};", .{q});
    return analyze(a, try parse(a, src), diag);
}

pub fn tfld(a: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const parts = try a.alloc([]const u8, 1);
    parts[0] = name;
    const e = try a.create(ast.Expr);
    e.* = .{ .field = .{ .parts = parts } };
    return e;
}
