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
