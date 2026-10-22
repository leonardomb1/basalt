//! RFC 3986 percent-encoding helpers shared by the URL, signing and form code.

const std = @import("std");

/// Letters, digits and `-._~`, which no percent-encoding escapes.
pub fn isUnreserved(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~';
}

/// `%XX` escapes decoded; a malformed escape is kept, and `s` is returned as is when
/// it holds no `%`.
pub fn decode(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, s, '%') == null) return s;
    return std.Uri.percentDecodeInPlace(try arena.dupe(u8, s));
}

/// `s` appended to `buf` with all but unreserved characters as `%XX`.
pub fn encode(buf: *std.array_list.Managed(u8), s: []const u8) error{OutOfMemory}!void {
    for (s) |b| {
        if (isUnreserved(b)) try buf.append(b) else try buf.writer().print("%{X:0>2}", .{b});
    }
}

test "encode escapes all but unreserved characters" {
    var buf = std.array_list.Managed(u8).init(std.testing.allocator);
    defer buf.deinit();
    try buf.appendSlice("k=");
    try encode(&buf, "a b/é~-._+%");
    try std.testing.expectEqualStrings("k=a%20b%2F%C3%A9~-._%2B%25", buf.items);
}
