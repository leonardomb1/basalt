//! Test helpers shared by the tests of eval.zig's parts.

pub const Value = @import("../eval.zig").Value;
const constEval = @import("vec.zig").constEval;
const parser = @import("../../lang/sql_parser.zig");
const std = @import("std");
pub fn evalLit(a: std.mem.Allocator, src: []const u8) !Value {
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, src, &diag);
    return constEval(a, e, &[_][]const u8{}, &[_]Value{});
}
