//! Test helpers shared by the tests of lanes.zig's parts.

const ast = @import("../../lang/ast.zig");
const std = @import("std");
pub fn testStages(arena: std.mem.Allocator, src: []const u8) ![]const ast.Stage {
    var diag: @import("../../lang/sql_parser.zig").Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try @import("../../lang/sql_parser.zig").parseSource(arena, src, &diag);
    for (prog.stmts) |s| if (s == .output) return s.output.stages;
    return error.NoOutput;
}
