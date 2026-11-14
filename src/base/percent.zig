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

/// `s` written to `w` with every byte but the unreserved ones, and those in `keep`
/// (a path's `/`), as uppercase `%XX`, as SigV4 and Shared Key sign it.
pub fn writeEncoded(w: anytype, s: []const u8, comptime keep: []const u8) !void {
    for (s) |b| {
        if (isUnreserved(b) or std.mem.indexOfScalar(u8, keep, b) != null) try w.writeByte(b) else try w.print("%{X:0>2}", .{b});
    }
}

/// `s` appended to `buf` with all but unreserved characters as `%XX`.
pub fn encode(buf: *std.array_list.Managed(u8), s: []const u8) error{OutOfMemory}!void {
    try writeEncoded(buf.writer(), s, "");
}

/// `s` percent-encoded into a new slice, the bytes in `keep` left as they are.
pub fn encodeAlloc(arena: std.mem.Allocator, s: []const u8, comptime keep: []const u8) error{OutOfMemory}![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    try writeEncoded(out.writer(), s, keep);
    return out.toOwnedSlice();
}

test "encode escapes all but unreserved characters" {
    var buf = std.array_list.Managed(u8).init(std.testing.allocator);
    defer buf.deinit();
    try buf.appendSlice("k=");
    try encode(&buf, "a b/é~-._+%");
    try std.testing.expectEqualStrings("k=a%20b%2F%C3%A9~-._%2B%25", buf.items);
}

test "encodeAlloc keeps only the bytes it is told to" {
    const a = std.testing.allocator;
    const path = try encodeAlloc(a, "dir a/k=v+1", "/");
    defer a.free(path);
    try std.testing.expectEqualStrings("dir%20a/k%3Dv%2B1", path);
    const kv = try encodeAlloc(a, "dir a/k=v+1", "/=");
    defer a.free(kv);
    try std.testing.expectEqualStrings("dir%20a/k=v%2B1", kv);
}
