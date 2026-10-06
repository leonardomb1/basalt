//! The basalt executable's entry point. Debug builds run on a leak-checking
//! DebugAllocator, release builds on the SMP allocator; everything else is
//! `cli.run`. The test block pulls every declaration under root.zig into the
//! test build.

const std = @import("std");
const builtin = @import("builtin");
const root = @import("root.zig");

pub fn main() !void {
    var debug_gpa = std.heap.DebugAllocator(.{}){};
    defer if (builtin.mode == .Debug) {
        _ = debug_gpa.deinit();
    };
    const gpa = if (builtin.mode == .Debug) debug_gpa.allocator() else std.heap.smp_allocator;
    try root.cli.run(gpa);
}

test {
    std.testing.refAllDeclsRecursive(root);
}
