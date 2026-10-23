//! Test helpers shared by the tests of eval.zig's parts.

pub const Value = @import("../eval.zig").Value;
const Batch = @import("../batch.zig").Batch;
const TypeCtx = @import("../eval.zig").TypeCtx;
const compareValues = @import("support.zig").compareValues;
const constEval = @import("vec.zig").constEval;
const evalRow = @import("row.zig").evalRow;
const parser = @import("../../lang/sql_parser.zig");
const std = @import("std");
const vec = @import("vec.zig");

pub fn evalLit(a: std.mem.Allocator, src: []const u8) !Value {
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, src, &diag);
    return constEval(a, e, &[_][]const u8{}, &[_]Value{});
}

/// Each expression over `batch` as a column and row by row: the same value, kind and
/// scale per row, or the same first error. `vectorized`: the node must not decline
/// to the row path.
pub fn expectVecMatchesRows(a: std.mem.Allocator, batch: Batch, srcs: []const []const u8, vectorized: bool) !void {
    for (srcs) |src| expectOne(a, batch, src, vectorized) catch |e| {
        std.debug.print("mismatch in `{s}`\n", .{src});
        return e;
    };
}

fn expectOne(a: std.mem.Allocator, batch: Batch, src: []const u8, vectorized: bool) !void {
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, src, &diag);
    var ctx = TypeCtx{ .schema = batch.schema.*, .arena = a };
    const ty = try ctx.typeOf(e);
    if (vectorized) if (vec.evalVecNode(a, e, batch)) |_| {} else |err| try std.testing.expect(err != error.Unsupported);
    const col = vec.evalColumn(a, e, batch, ty) catch |err| {
        const first = for (0..batch.len) |i| {
            _ = evalRow(a, e, batch, i) catch |row_err| break row_err;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(first, err);
        return;
    };
    if (!ty.unknown) try std.testing.expectEqual(ty.kind, col.ty.kind);
    if (vectorized and !ty.unknown and ty.kind == .decimal) {
        try std.testing.expectEqual(ty.precision, col.ty.precision);
        try std.testing.expectEqual(ty.scale, col.ty.scale);
    }
    for (0..batch.len) |i| {
        const want = try evalRow(a, e, batch, i);
        const got = col.getValue(i);
        try std.testing.expectEqual(want.isNull(), got.isNull());
        if (want.isNull()) continue;
        switch (want) {
            .string, .bytes => |s| try std.testing.expectEqualStrings(s, switch (got) {
                .string, .bytes => |g| g,
                else => return error.TestUnexpectedResult,
            }),
            .float => |x| if (std.math.isNan(x)) try std.testing.expect(std.math.isNan(got.float)) else try std.testing.expectEqual(x, got.float),
            .decimal => |d| {
                try std.testing.expectEqual(d.unscaled, got.decimal.unscaled);
                try std.testing.expectEqual(d.scale, got.decimal.scale);
            },
            else => try std.testing.expect(compareValues(want, got).? == .eq),
        }
    }
}
