//! Character sets bytes are read in and written out of: UTF-8, ISO-8859-1, windows-1252
//! and EBCDIC code page 037. `decode` turns bytes into UTF-8 text, repairing invalid
//! UTF-8 and mapping each single-byte character; `encode` turns UTF-8 text into a
//! charset's bytes, refusing a character it cannot hold rather than substituting one.
//!
//! The five bytes windows-1252 leaves unassigned (0x81, 0x8D, 0x8F, 0x90, 0x9D) are
//! their C1 controls, as Microsoft's own table, SQL Server, WHATWG, ICU and MySQL read
//! them: lossless, so every byte decodes and encodes back to itself.

const std = @import("std");
const celltext = @import("../exec/celltext.zig");

pub const Charset = enum {
    utf8,
    latin1,
    cp1252,
    cp037,

    /// A charset by name, case, `-` and `_` aside: `utf8`, `latin1` / `iso-8859-1`,
    /// `cp1252` / `windows-1252`, `cp037` / `ebcdic`.
    pub fn parse(s: []const u8) ?Charset {
        for ([_]struct { []const u8, Charset }{
            .{ "utf8", .utf8 },     .{ "latin1", .latin1 },      .{ "iso88591", .latin1 },
            .{ "cp1252", .cp1252 }, .{ "windows1252", .cp1252 }, .{ "cp037", .cp037 },
            .{ "ebcdic", .cp037 },  .{ "ibm037", .cp037 },
        }) |c| if (eqNorm(s, c[0])) return c[1];
        return null;
    }

    /// The byte a space is in this charset, for padding a field.
    pub fn space(cs: Charset) u8 {
        return if (cs == .cp037) 0x40 else ' ';
    }
};

pub const names = "utf8, latin1 / iso-8859-1, cp1252 / windows-1252, cp037 / ebcdic";

fn eqNorm(a: []const u8, b: []const u8) bool {
    var i: usize = 0;
    var j: usize = 0;
    while (true) {
        while (i < a.len and (a[i] == '-' or a[i] == '_')) i += 1;
        while (j < b.len and (b[j] == '-' or b[j] == '_')) j += 1;
        if (i == a.len or j == b.len) return i == a.len and j == b.len;
        if (std.ascii.toLower(a[i]) != std.ascii.toLower(b[j])) return false;
        i += 1;
        j += 1;
    }
}

const cp1252_high = [32]u21{
    0x20AC, 0x0081, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
    0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008D, 0x017D, 0x008F,
    0x0090, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0x009D, 0x017E, 0x0178,
};

const cp037_to_unicode = [256]u21{
    0x0000, 0x0001, 0x0002, 0x0003, 0x009C, 0x0009, 0x0086, 0x007F,
    0x0097, 0x008D, 0x008E, 0x000B, 0x000C, 0x000D, 0x000E, 0x000F,
    0x0010, 0x0011, 0x0012, 0x0013, 0x009D, 0x0085, 0x0008, 0x0087,
    0x0018, 0x0019, 0x0092, 0x008F, 0x001C, 0x001D, 0x001E, 0x001F,
    0x0080, 0x0081, 0x0082, 0x0083, 0x0084, 0x000A, 0x0017, 0x001B,
    0x0088, 0x0089, 0x008A, 0x008B, 0x008C, 0x0005, 0x0006, 0x0007,
    0x0090, 0x0091, 0x0016, 0x0093, 0x0094, 0x0095, 0x0096, 0x0004,
    0x0098, 0x0099, 0x009A, 0x009B, 0x0014, 0x0015, 0x009E, 0x001A,
    0x0020, 0x00A0, 0x00E2, 0x00E4, 0x00E0, 0x00E1, 0x00E3, 0x00E5,
    0x00E7, 0x00F1, 0x00A2, 0x002E, 0x003C, 0x0028, 0x002B, 0x007C,
    0x0026, 0x00E9, 0x00EA, 0x00EB, 0x00E8, 0x00ED, 0x00EE, 0x00EF,
    0x00EC, 0x00DF, 0x0021, 0x0024, 0x002A, 0x0029, 0x003B, 0x00AC,
    0x002D, 0x002F, 0x00C2, 0x00C4, 0x00C0, 0x00C1, 0x00C3, 0x00C5,
    0x00C7, 0x00D1, 0x00A6, 0x002C, 0x0025, 0x005F, 0x003E, 0x003F,
    0x00F8, 0x00C9, 0x00CA, 0x00CB, 0x00C8, 0x00CD, 0x00CE, 0x00CF,
    0x00CC, 0x0060, 0x003A, 0x0023, 0x0040, 0x0027, 0x003D, 0x0022,
    0x00D8, 0x0061, 0x0062, 0x0063, 0x0064, 0x0065, 0x0066, 0x0067,
    0x0068, 0x0069, 0x00AB, 0x00BB, 0x00F0, 0x00FD, 0x00FE, 0x00B1,
    0x00B0, 0x006A, 0x006B, 0x006C, 0x006D, 0x006E, 0x006F, 0x0070,
    0x0071, 0x0072, 0x00AA, 0x00BA, 0x00E6, 0x00B8, 0x00C6, 0x00A4,
    0x00B5, 0x007E, 0x0073, 0x0074, 0x0075, 0x0076, 0x0077, 0x0078,
    0x0079, 0x007A, 0x00A1, 0x00BF, 0x00D0, 0x00DD, 0x00DE, 0x00AE,
    0x005E, 0x00A3, 0x00A5, 0x00B7, 0x00A9, 0x00A7, 0x00B6, 0x00BC,
    0x00BD, 0x00BE, 0x005B, 0x005D, 0x00AF, 0x00A8, 0x00B4, 0x00D7,
    0x007B, 0x0041, 0x0042, 0x0043, 0x0044, 0x0045, 0x0046, 0x0047,
    0x0048, 0x0049, 0x00AD, 0x00F4, 0x00F6, 0x00F2, 0x00F3, 0x00F5,
    0x007D, 0x004A, 0x004B, 0x004C, 0x004D, 0x004E, 0x004F, 0x0050,
    0x0051, 0x0052, 0x00B9, 0x00FB, 0x00FC, 0x00F9, 0x00FA, 0x00FF,
    0x005C, 0x00F7, 0x0053, 0x0054, 0x0055, 0x0056, 0x0057, 0x0058,
    0x0059, 0x005A, 0x00B2, 0x00D4, 0x00D6, 0x00D2, 0x00D3, 0x00D5,
    0x0030, 0x0031, 0x0032, 0x0033, 0x0034, 0x0035, 0x0036, 0x0037,
    0x0038, 0x0039, 0x00B3, 0x00DB, 0x00DC, 0x00D9, 0x00DA, 0x009F,
};

/// The code point one byte of a single-byte charset stands for.
fn codePoint(cs: Charset, c: u8) u21 {
    return switch (cs) {
        .utf8, .latin1 => c,
        .cp1252 => if (c < 0x80 or c > 0x9F) c else cp1252_high[c - 0x80],
        .cp037 => cp037_to_unicode[c],
    };
}

/// One byte's character as UTF-8, at most three bytes.
pub const Seq = struct { len: u8, bytes: [3]u8 };

/// Each byte of single-byte charset `cs` as its UTF-8 sequence, built at compile time.
pub fn utf8Table(comptime cs: Charset) *const [256]Seq {
    comptime std.debug.assert(cs != .utf8);
    return &struct {
        const table: [256]Seq = blk: {
            @setEvalBranchQuota(20_000);
            var t: [256]Seq = undefined;
            for (&t, 0..) |*e, b| {
                var buf: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(codePoint(cs, @intCast(b)), &buf) catch unreachable;
                e.* = .{ .len = n, .bytes = buf[0..3].* };
            }
            break :blk t;
        };
    }.table;
}

fn asciiCompatible(comptime cs: Charset) bool {
    return cs != .cp037;
}

/// The length of `src` in `cs` decoded to UTF-8.
pub fn decodedLen(comptime cs: Charset, src: []const u8) usize {
    const t = utf8Table(cs);
    var n: usize = 0;
    for (src) |b| n += if (asciiCompatible(cs) and b < 0x80) 1 else t[b].len;
    return n;
}

/// `src` in `cs` as UTF-8 into `dst`, which holds at least `decodedLen(cs, src)`;
/// returns that length.
pub fn decodeInto(comptime cs: Charset, dst: []u8, src: []const u8) usize {
    const t = utf8Table(cs);
    var o: usize = 0;
    for (src) |b| {
        if (asciiCompatible(cs) and b < 0x80) {
            dst[o] = b;
            o += 1;
        } else {
            const e = t[b];
            @memcpy(dst[o..][0..e.len], e.bytes[0..e.len]);
            o += e.len;
        }
    }
    return o;
}

/// Whether every byte is below 0x80, sixteen bytes at a time.
pub fn isAscii(bytes: []const u8) bool {
    const V = @Vector(16, u8);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + 16 <= bytes.len) : (i += 16) acc |= @as(V, bytes[i..][0..16].*);
    var tail: u8 = @reduce(.Or, acc);
    for (bytes[i..]) |b| tail |= b;
    return tail < 0x80;
}

/// `bytes` in `cs` as UTF-8: invalid UTF-8 repaired, each single-byte character mapped;
/// `bytes` itself when it needs no change.
pub fn decode(arena: std.mem.Allocator, cs: Charset, bytes: []const u8) ![]const u8 {
    switch (cs) {
        .utf8 => {
            if (std.unicode.utf8ValidateSlice(bytes)) return bytes;
            var out = try std.array_list.Managed(u8).initCapacity(arena, bytes.len + 8);
            var runs = celltext.Utf8Runs{ .s = bytes };
            while (runs.next()) |t| try out.appendSlice(t);
            return out.items;
        },
        inline else => |c| {
            if (asciiCompatible(c) and isAscii(bytes)) return bytes;
            const out = try arena.alloc(u8, decodedLen(c, bytes));
            _ = decodeInto(c, out, bytes);
            return out;
        },
    }
}

/// UTF-8 `text` as `cs` bytes; `text` itself for UTF-8. A character `cs` cannot hold
/// is `error.Unencodable`.
pub fn encode(arena: std.mem.Allocator, cs: Charset, text: []const u8) ![]const u8 {
    if (cs == .utf8) return text;
    if (cs != .cp037) {
        for (text) |c| {
            if (c >= 0x80) break;
        } else return text;
    }
    var out = try std.array_list.Managed(u8).initCapacity(arena, text.len);
    var it = (std.unicode.Utf8View.init(text) catch return error.Unencodable).iterator();
    while (it.nextCodepoint()) |cp| out.appendAssumeCapacity(try byteOf(cs, cp));
    return out.items;
}

fn byteOf(cs: Charset, cp: u21) !u8 {
    switch (cs) {
        .utf8 => unreachable,
        .latin1 => return if (cp <= 0xFF) @intCast(cp) else error.Unencodable,
        .cp1252 => {
            if (cp < 0x80 or (cp >= 0xA0 and cp <= 0xFF)) return @intCast(cp);
            for (cp1252_high, 0..) |m, i| if (m == cp) return @intCast(0x80 + i);
            return error.Unencodable;
        },
        .cp037 => {
            for (cp037_to_unicode, 0..) |m, i| if (m == cp) return @intCast(i);
            return error.Unencodable;
        },
    }
}

test "charsets decode and encode back, refusing what they cannot hold" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("Jos\u{e9} \u{20ac}", try decode(a, .cp1252, "Jos\xe9 \x80"));
    try std.testing.expectEqualStrings("Jos\xe9 \x80", try encode(a, .cp1252, "Jos\u{e9} \u{20ac}"));
    try std.testing.expectError(error.Unencodable, encode(a, .latin1, "\u{20ac}"));
    try std.testing.expectEqualStrings("ABC 123", try decode(a, .cp037, "\xc1\xc2\xc3\x40\xf1\xf2\xf3"));
    try std.testing.expectEqualStrings("\xc1\xc2\xc3\x40\xf1\xf2\xf3", try encode(a, .cp037, "ABC 123"));
    try std.testing.expectEqualStrings("a\u{FFFD}", try decode(a, .utf8, "a\xff"));
    try std.testing.expectEqual(Charset.cp037, Charset.parse("EBCDIC").?);
    try std.testing.expectEqual(Charset.latin1, Charset.parse("ISO-8859-1").?);
}

test "decode by table equals the per-byte encoder it replaced, every byte and a random corpus" {
    const Orig = struct {
        fn oracleCodePoint(cs: Charset, c: u8) u21 {
            return switch (cs) {
                .utf8, .latin1 => c,
                .cp1252 => if (c < 0x80 or c > 0x9F) c else cp1252_high[c - 0x80],
                .cp037 => cp037_to_unicode[c],
            };
        }
        fn oracleDecode(arena: std.mem.Allocator, cs: Charset, bytes: []const u8) ![]const u8 {
            if (cs == .utf8) {
                if (std.unicode.utf8ValidateSlice(bytes)) return bytes;
                var out = try std.array_list.Managed(u8).initCapacity(arena, bytes.len + 8);
                var runs = celltext.Utf8Runs{ .s = bytes };
                while (runs.next()) |t| try out.appendSlice(t);
                return out.items;
            }
            if (cs != .cp037) {
                for (bytes) |c| {
                    if (c >= 0x80) break;
                } else return bytes;
            }
            var out = try std.array_list.Managed(u8).initCapacity(arena, bytes.len * 3);
            var buf: [4]u8 = undefined;
            for (bytes) |c| {
                const n = std.unicode.utf8Encode(oracleCodePoint(cs, c), &buf) catch unreachable;
                out.appendSliceAssumeCapacity(buf[0..n]);
            }
            return out.items;
        }
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var all: [256]u8 = undefined;
    for (&all, 0..) |*b, i| b.* = @intCast(i);
    var prng = std.Random.DefaultPrng.init(0x1252);
    const r = prng.random();
    var buf: [96]u8 = undefined;
    for ([_]Charset{ .utf8, .latin1, .cp1252, .cp037 }) |cs| {
        for (0..256) |i| try std.testing.expectEqualStrings(try Orig.oracleDecode(a, cs, all[i .. i + 1]), try decode(a, cs, all[i .. i + 1]));
        try std.testing.expectEqualStrings(try Orig.oracleDecode(a, cs, &all), try decode(a, cs, &all));
        try std.testing.expectEqualStrings("", try decode(a, cs, ""));
        for (0..3000) |it| {
            const n = r.uintLessThan(usize, buf.len + 1);
            for (buf[0..n]) |*b| b.* = if (it % 2 == 0) r.int(u8) else r.uintLessThan(u8, 0x80);
            try std.testing.expectEqualStrings(try Orig.oracleDecode(a, cs, buf[0..n]), try decode(a, cs, buf[0..n]));
        }
    }
}

test "cp1252 decodes every byte as Microsoft's bestfit1252 table, the five unassigned bytes to C1 (D5)" {
    const Orig = struct {
        fn tdsCp1252High(b: u8) u21 {
            return switch (b) {
                0x80 => 0x20AC,
                0x82 => 0x201A,
                0x83 => 0x0192,
                0x84 => 0x201E,
                0x85 => 0x2026,
                0x86 => 0x2020,
                0x87 => 0x2021,
                0x88 => 0x02C6,
                0x89 => 0x2030,
                0x8A => 0x0160,
                0x8B => 0x2039,
                0x8C => 0x0152,
                0x8E => 0x017D,
                0x91 => 0x2018,
                0x92 => 0x2019,
                0x93 => 0x201C,
                0x94 => 0x201D,
                0x95 => 0x2022,
                0x96 => 0x2013,
                0x97 => 0x2014,
                0x98 => 0x02DC,
                0x99 => 0x2122,
                0x9A => 0x0161,
                0x9B => 0x203A,
                0x9C => 0x0153,
                0x9E => 0x017E,
                0x9F => 0x0178,
                else => b,
            };
        }
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = utf8Table(.cp1252);
    for (0..256) |i| {
        const b: u8 = @intCast(i);
        var want: [4]u8 = undefined;
        const n = try std.unicode.utf8Encode(if (b < 0x80) b else Orig.tdsCp1252High(b), &want);
        try std.testing.expectEqualSlices(u8, want[0..n], t[b].bytes[0..t[b].len]);
        try std.testing.expectEqualStrings(want[0..n], try decode(a, .cp1252, &.{b}));
    }
    var out: [16]u8 = undefined;
    try std.testing.expectEqualStrings("\xc2\x81a", out[0..decodeInto(.cp1252, &out, "\x81a")]);
    try std.testing.expectEqual(@as(usize, 3), decodedLen(.cp1252, "\x81a"));
    try std.testing.expectEqualStrings("\xc2\x81\xc2\x8d\xc2\x8f\xc2\x90\xc2\x9d", try decode(a, .cp1252, "\x81\x8d\x8f\x90\x9d"));
    try std.testing.expectEqualStrings("\x81\x8d\x8f\x90\x9d", try encode(a, .cp1252, "\u{81}\u{8d}\u{8f}\u{90}\u{9d}"));
    try std.testing.expectError(error.Unencodable, encode(a, .cp1252, "\u{80}"));
    try std.testing.expectError(error.Unencodable, encode(a, .cp1252, "\u{FFFD}"));
}

test "every single-byte charset decodes each byte and encodes it back to itself (D5)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var all: [256]u8 = undefined;
    for (&all, 0..) |*b, i| b.* = @intCast(i);
    for ([_]Charset{ .latin1, .cp1252, .cp037 }) |cs| {
        const text = try decode(a, cs, &all);
        try std.testing.expect(std.unicode.utf8ValidateSlice(text));
        try std.testing.expectEqualSlices(u8, &all, try encode(a, cs, text));
    }
}

test "isAscii agrees with a byte loop at every length and position of a high byte" {
    var buf: [80]u8 = undefined;
    @memset(&buf, 'a');
    for (0..buf.len + 1) |len| {
        try std.testing.expect(isAscii(buf[0..len]));
        for (0..len) |at| {
            buf[at] = 0x80;
            try std.testing.expect(!isAscii(buf[0..len]));
            buf[at] = 0x7F;
            try std.testing.expect(isAscii(buf[0..len]));
            buf[at] = 'a';
        }
    }
}
