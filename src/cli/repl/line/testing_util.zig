//! Test helpers shared by the tests of line.zig's parts.

const Buffer = @import("buffer.zig").Buffer;
const std = @import("std");
pub fn testBuffer(text: []const u8, cursor: usize) !Buffer {
    var b = Buffer.init(std.testing.allocator);
    try b.set(text);
    b.cursor = cursor;
    return b;
}
