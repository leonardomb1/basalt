//! Test helpers shared by the tests of sql_parser.zig's parts.

const Diagnostic = @import("../sql_parser.zig").Diagnostic;
const ast = @import("../ast.zig");
const parseSource = @import("../sql_parser.zig").parseSource;
const std = @import("std");
pub fn parseTest(a: std.mem.Allocator, src: []const u8) !ast.Program {
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parseSource(a, src, &diag) catch |e| {
        if (e == error.ParseFailed) std.debug.print("parse error {d}:{d}: {s}\n", .{ diag.line, diag.col, diag.msg });
        return e;
    };
}

pub fn hintStr(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key)) return switch (h.value) {
            .str, .ident => |s| s,
            else => null,
        };
    }
    return null;
}

pub fn firstPipelineStages(prog: ast.Program) []const ast.Stage {
    for (prog.stmts) |s| {
        if (s == .output) return s.output.stages;
    }
    unreachable;
}

/// Index of the aggregate stage, counted from the front: the pipeline ends in a write.
pub fn aggStageIndex(st: []const ast.Stage) usize {
    for (st, 0..) |s, i| {
        if (s.node == .aggregate) return i;
    }
    unreachable;
}

/// Whether a `bool_lit true` survives in the tree; a leftover sentinel would read as
/// an always-true conjunct.
pub fn containsTrueLit(e: *const ast.Expr) bool {
    return switch (e.*) {
        .bool_lit => |b| b,
        .binary => |b| containsTrueLit(b.l) or containsTrueLit(b.r),
        .unary => |u| containsTrueLit(u.e),
        else => false,
    };
}
