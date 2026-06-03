const std = @import("std");
const root = @import("root.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    try root.cli.run(gpa.allocator());
}

test {
    // Pull every module's tests into the `zig build test` run.
    std.testing.refAllDeclsRecursive(root);
}
