const std = @import("std");
const root = @import("root.zig");

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    try root.cli.run(gpa.allocator());
}

test {
    std.testing.refAllDeclsRecursive(root);
}
