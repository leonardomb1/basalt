//! Hex text, strict: exactly the digits `[0-9a-fA-F]`. `std.fmt.parseInt(.., 16)`
//! also takes a sign and `_`; a prefix such as SQL Server's `0x` or postgres's `'\x`
//! is stripped or written by its caller. `json.zig` `hex4` reads its `\u` digits with `digit`.

const std = @import("std");

pub const Case = enum { lower, upper };

pub const DecodeError = error{ OddLength, InvalidDigit };

/// The value of hex digit `c`, or null.
pub fn digit(c: u8) ?u4 {
    return switch (c) {
        '0'...'9' => @intCast(c - '0'),
        'a'...'f' => @intCast(c - 'a' + 10),
        'A'...'F' => @intCast(c - 'A' + 10),
        else => null,
    };
}

/// Two digits per byte of `bytes` into `out`, which holds at least `2 * bytes.len`.
pub const lower_digits = "0123456789abcdef";

pub fn encode(out: []u8, bytes: []const u8, comptime case: Case) []u8 {
    const digits = if (case == .lower) lower_digits else "0123456789ABCDEF";
    for (bytes, 0..) |b, i| {
        out[i * 2] = digits[b >> 4];
        out[i * 2 + 1] = digits[b & 0x0F];
    }
    return out[0 .. bytes.len * 2];
}

/// The bytes of `text` into `out`, which holds at least `text.len / 2`.
pub fn decode(out: []u8, text: []const u8) DecodeError![]u8 {
    if (text.len % 2 != 0) return error.OddLength;
    for (out[0 .. text.len / 2], 0..) |*o, i| {
        const hi = digit(text[i * 2]) orelse return error.InvalidDigit;
        const lo = digit(text[i * 2 + 1]) orelse return error.InvalidDigit;
        o.* = (@as(u8, hi) << 4) | lo;
    }
    return out[0 .. text.len / 2];
}

/// The unsigned `T` that `s` spells in hex digits only; null when `s` is empty, holds
/// anything but a digit, or overflows `T`. Leading zeros are allowed.
pub fn int(comptime T: type, s: []const u8) ?T {
    if (s.len == 0) return null;
    var v: T = 0;
    for (s) |c| {
        const d = digit(c) orelse return null;
        v = std.math.mul(T, v, 16) catch return null;
        v = std.math.add(T, v, d) catch return null;
    }
    return v;
}

const testing = std.testing;

test "hex: digit takes exactly 0-9, a-f and A-F" {
    for (0..256) |i| {
        const c: u8 = @intCast(i);
        const want: ?u4 = if (std.fmt.charToDigit(c, 16)) |d| @intCast(d) else |_| null;
        try testing.expectEqual(want, digit(c));
    }
}

test "hex: encode in both cases and decode it back" {
    var prng = std.Random.DefaultPrng.init(0x4e58);
    const r = prng.random();
    var src: [64]u8 = undefined;
    var txt: [128]u8 = undefined;
    var back: [64]u8 = undefined;
    for (0..2000) |_| {
        const n = r.uintAtMost(usize, src.len);
        r.bytes(src[0..n]);
        inline for (.{ Case.lower, Case.upper }) |case| {
            const t = encode(&txt, src[0..n], case);
            try testing.expectEqual(n * 2, t.len);
            const want = std.fmt.bytesToHex(src, if (case == .lower) .lower else .upper);
            try testing.expectEqualStrings(want[0 .. n * 2], t);
            try testing.expectEqualSlices(u8, src[0..n], try decode(&back, t));
        }
    }
    try testing.expectEqualStrings("0001abff", encode(&txt, &.{ 0x00, 0x01, 0xAB, 0xFF }, .lower));
    try testing.expectEqualStrings("0001ABFF", encode(&txt, &.{ 0x00, 0x01, 0xAB, 0xFF }, .upper));
    try testing.expectEqualStrings("", encode(&txt, "", .lower));
}

test "hex: decode tells an odd length from a bad digit" {
    var out: [8]u8 = undefined;
    try testing.expectEqualSlices(u8, &.{ 0xDE, 0xAD, 0xbe, 0xef }, try decode(&out, "DEADbeef"));
    try testing.expectEqualSlices(u8, "", try decode(&out, ""));
    try testing.expectError(error.OddLength, decode(&out, "abc"));
    try testing.expectError(error.OddLength, decode(&out, "+"));
    for ([_][]const u8{ "+f", "-0", "_1", "0x", " 1", "1 ", "gg", "0\x00" }) |t|
        try testing.expectError(error.InvalidDigit, decode(&out, t));
}

test "hex: int refuses a sign, `_`, `0x`, empty text and overflow; keeps leading zeros" {
    try testing.expectEqual(@as(?u21, 0x41), int(u21, "41"));
    try testing.expectEqual(@as(?u21, 0x41), int(u21, "0041"));
    try testing.expectEqual(@as(?u21, 0x41), int(u21, "00000000000041"));
    try testing.expectEqual(@as(?u21, 0x10FFFF), int(u21, "10ffff"));
    try testing.expectEqual(@as(?u21, 0x1FFFFF), int(u21, "1FFFFF"));
    try testing.expectEqual(@as(?u21, null), int(u21, "200000"));
    try testing.expectEqual(@as(?u8, 0xFF), int(u8, "ff"));
    try testing.expectEqual(@as(?u8, null), int(u8, "100"));
    try testing.expectEqual(@as(?u64, std.math.maxInt(u64)), int(u64, "ffffffffffffffff"));
    try testing.expectEqual(@as(?u64, null), int(u64, "10000000000000000"));
    for ([_][]const u8{ "", "+f", "-0", "4_1", "_41", "0x41", "+41", " 41", "41 ", "4g" }) |t|
        try testing.expectEqual(@as(?u21, null), int(u21, t));
}

test "hex: pins std.fmt.parseInt's looser grammar, as today's Unhex and XML char refs call it" {
    try testing.expectEqual(@as(u8, 0x0F), try std.fmt.parseInt(u8, "+f", 16));
    try testing.expectEqual(@as(u8, 0x00), try std.fmt.parseInt(u8, "-0", 16));
    try testing.expectEqual(@as(u21, 0x41), try std.fmt.parseInt(u21, "4_1", 16));
    try testing.expectEqual(@as(u21, 0x41), try std.fmt.parseInt(u21, "+41", 16));
    try testing.expectError(error.InvalidCharacter, std.fmt.parseInt(u21, "0x41", 16));
    for ([_][]const u8{ "+f", "-0", "4_1", "+41", "0x41" }) |t| try testing.expectEqual(@as(?u21, null), int(u21, t));
}

test "hex: on digits-only text int agrees with std.fmt.parseInt" {
    var prng = std.Random.DefaultPrng.init(0x1e7);
    const r = prng.random();
    const alphabet = "0123456789abcdefABCDEF";
    var buf: [20]u8 = undefined;
    for (0..5000) |_| {
        const n = r.intRangeAtMost(usize, 1, buf.len);
        for (buf[0..n]) |*c| c.* = alphabet[r.uintLessThan(usize, alphabet.len)];
        const s = buf[0..n];
        try testing.expectEqual(std.fmt.parseInt(u64, s, 16) catch null, int(u64, s));
        try testing.expectEqual(std.fmt.parseInt(u21, s, 16) catch null, int(u21, s));
        try testing.expectEqual(std.fmt.parseInt(u8, s, 16) catch null, int(u8, s));
    }
}

test "hex: pins json hex4, four strict digits" {
    for ([_][]const u8{ "0041", "ffff", "D83D", "dE00" }) |t| {
        const v = int(u16, t).?;
        try testing.expectEqual(std.fmt.parseInt(u16, t, 16) catch unreachable, v);
    }
    try testing.expectEqual(@as(?u16, null), int(u16, "+041"));
    try testing.expectEqual(@as(?u16, null), int(u16, "0_41"));
}
