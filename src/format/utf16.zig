//! UTF-16LE over byte buffers (wire bodies are byte-aligned, so std's `[]const u16`
//! helpers do not apply), and UTF-16 unit counts of UTF-8 text.
//!
//! UCS-2 is not UTF-16: the halves of a surrogate pair are joined (encoding them
//! singly once made every emoji `??`). Only an unpaired half meets the `Lone`
//! policy: `?` (SQL Server's TDS reader today), U+FFFD, or an error (std's rule).
//! A trailing odd byte is not a code unit and is dropped.

const std = @import("std");

pub const Lone = enum { question, replacement, fail };

pub const LoneError = error{LoneSurrogate};

fn Decoded(comptime lone: Lone) type {
    return if (lone == .fail) LoneError!usize else usize;
}

/// Room `decodeLe` needs for `n` bytes of UTF-16LE: three per code unit (a pair's
/// four bytes are two units' worth, U+FFFD is three).
pub fn decodedBound(n: usize) usize {
    return n / 2 * 3;
}

/// `bytes` as UTF-8 into `out`, which holds `decodedBound(bytes.len)`; returns the
/// length written. Eight units that are all ASCII, the common run, are narrowed at
/// once. Moved from TDS `utf16Encode`.
pub fn decodeLe(comptime lone: Lone, out: []u8, bytes: []const u8) Decoded(lone) {
    const V = @Vector(16, u8);
    const ascii_mask: V = @bitCast([_]u8{ 0x80, 0xFF } ** 8);
    const lows = [8]i32{ 0, 2, 4, 6, 8, 10, 12, 14 };
    var o: usize = 0;
    var k: usize = 0;
    while (k + 1 < bytes.len) {
        if (k + 16 <= bytes.len) {
            const v: V = bytes[k..][0..16].*;
            if (@reduce(.Or, v & ascii_mask) == 0) {
                out[o..][0..8].* = @shuffle(u8, v, undefined, lows);
                o += 8;
                k += 16;
                continue;
            }
        }
        var cp: u21 = @as(u16, bytes[k]) | (@as(u16, bytes[k + 1]) << 8);
        k += 2;
        if (cp < 0x80) {
            out[o] = @intCast(cp);
            o += 1;
            continue;
        }
        if (std.unicode.utf16IsHighSurrogate(@intCast(cp))) {
            const lo: u16 = if (k + 1 < bytes.len) @as(u16, bytes[k]) | (@as(u16, bytes[k + 1]) << 8) else 0;
            if (!std.unicode.utf16IsLowSurrogate(lo)) {
                if (lone == .fail) return error.LoneSurrogate;
                o += lonely(lone, out[o..]);
                continue;
            }
            cp = 0x10000 + ((cp & 0x03ff) << 10) | (lo & 0x03ff);
            k += 2;
        }
        o += std.unicode.utf8Encode(cp, out[o..]) catch blk: {
            if (lone == .fail) return error.LoneSurrogate;
            break :blk lonely(lone, out[o..]);
        };
    }
    return o;
}

fn lonely(comptime lone: Lone, out: []u8) usize {
    switch (lone) {
        .question => {
            out[0] = '?';
            return 1;
        },
        .replacement => {
            out[0..3].* = "\u{FFFD}".*;
            return 3;
        },
        .fail => unreachable,
    }
}

/// `decodeLe` into a slice of `a` sized to fit.
pub fn decodeLeAlloc(comptime lone: Lone, a: std.mem.Allocator, bytes: []const u8) (if (lone == .fail) (std.mem.Allocator.Error || LoneError) else std.mem.Allocator.Error)![]u8 {
    const out = try a.alloc(u8, decodedBound(bytes.len));
    errdefer a.free(out);
    const n = if (lone == .fail) try decodeLe(lone, out, bytes) else decodeLe(lone, out, bytes);
    return a.realloc(out, n);
}

/// UTF-8 `s` as UTF-16LE bytes into `out`, which holds `2 * s.len`; returns the
/// length written. Invalid UTF-8, a surrogate half spelled in UTF-8 included, is
/// refused as std's `utf8ToUtf16Le` refuses it. Leading ASCII is widened 16 at once.
pub fn encodeLe(out: []u8, s: []const u8) error{InvalidUtf8}!usize {
    const V = @Vector(16, u8);
    const zero: V = @splat(0);
    const high: V = @splat(0x80);
    const widen = comptime blk: {
        var m: [32]i32 = undefined;
        for (0..16) |i| {
            m[i * 2] = i;
            m[i * 2 + 1] = -1;
        }
        break :blk m;
    };
    var o: usize = 0;
    var k: usize = 0;
    while (k + 16 <= s.len) {
        const v: V = s[k..][0..16].*;
        if (@reduce(.Or, v & high) != 0) break;
        out[o..][0..32].* = @shuffle(u8, v, zero, widen);
        o += 32;
        k += 16;
    }
    const view = try std.unicode.Utf8View.init(s[k..]);
    var it = view.iterator();
    while (it.nextCodepoint()) |cp| {
        if (cp < 0x10000) {
            std.mem.writeInt(u16, out[o..][0..2], @intCast(cp), .little);
            o += 2;
        } else {
            const v = cp - 0x10000;
            std.mem.writeInt(u16, out[o..][0..2], @intCast(0xD800 + (v >> 10)), .little);
            std.mem.writeInt(u16, out[o + 2 ..][0..2], @intCast(0xDC00 + (v & 0x3FF)), .little);
            o += 4;
        }
    }
    return o;
}

/// `encodeLe` into a slice of `a` sized to fit.
pub fn encodeLeAlloc(a: std.mem.Allocator, s: []const u8) error{ InvalidUtf8, OutOfMemory }![]u8 {
    const out = try a.alloc(u8, 2 * s.len);
    errdefer a.free(out);
    return a.realloc(out, try encodeLe(out, s));
}

/// The longest prefix of UTF-16LE `bytes` within `max` bytes that keeps whole units
/// and does not end on the high half of a pair it cut (TDS `appendBatch`'s cut).
pub fn cutLe(bytes: []const u8, max: usize) usize {
    var n = @min(bytes.len, max) & ~@as(usize, 1);
    if (n < bytes.len and n >= 2) {
        const last = std.mem.readInt(u16, bytes[n - 2 ..][0..2], .little);
        if (std.unicode.utf16IsHighSurrogate(last)) n -= 2;
    }
    return n;
}

/// UTF-16 units of UTF-8 `s`: one per lead byte, two for a four-byte lead. On
/// invalid UTF-8 a stray continuation byte counts 0 (xlsx `utf16Len`).
pub fn unitLen(s: []const u8) usize {
    var n: usize = 0;
    for (s) |c| {
        if (c & 0xC0 != 0x80) n += 1;
        if (c >= 0xF0) n += 1;
    }
    return n;
}

/// Byte offset of UTF-16 offset `u`, a JavaScript editor's cursor. Inside a surrogate
/// pair or past the end clamps back to a boundary; non-UTF-8 bytes count one unit.
pub fn unitToByte(text: []const u8, u: usize) usize {
    var i: usize = 0;
    var units: usize = 0;
    while (i < text.len) {
        const n = std.unicode.utf8ByteSequenceLength(text[i]) catch 1;
        const cp_units: usize = if (n == 4) 2 else 1;
        if (units + cp_units > u) break;
        units += cp_units;
        i = @min(text.len, i + n);
    }
    return i;
}

/// UTF-16 units of `text[0..b]`, the inverse of `unitToByte`; a byte that leads no
/// UTF-8 sequence counts one unit.
pub fn byteToUnit(text: []const u8, b: usize) usize {
    var i: usize = 0;
    var units: usize = 0;
    while (i < @min(b, text.len)) {
        const n = std.unicode.utf8ByteSequenceLength(text[i]) catch 1;
        units += if (n == 4) 2 else 1;
        i += n;
    }
    return units;
}

const testing = std.testing;

fn unitsLe(a: std.mem.Allocator, units: []const u16) ![]u8 {
    const out = try a.alloc(u8, units.len * 2);
    for (units, 0..) |c, i| std.mem.writeInt(u16, out[i * 2 ..][0..2], c, .little);
    return out;
}

fn reference(a: std.mem.Allocator, comptime lone: Lone, bytes: []const u8) !?[]u8 {
    var out = std.array_list.Managed(u8).init(a);
    var k: usize = 0;
    var tmp: [4]u8 = undefined;
    while (k + 1 < bytes.len) : (k += 2) {
        const u = std.mem.readInt(u16, bytes[k..][0..2], .little);
        var cp: u21 = u;
        if (std.unicode.utf16IsHighSurrogate(u) and k + 3 < bytes.len and
            std.unicode.utf16IsLowSurrogate(std.mem.readInt(u16, bytes[k + 2 ..][0..2], .little)))
        {
            cp = 0x10000 + ((@as(u21, u) & 0x3ff) << 10) + (std.mem.readInt(u16, bytes[k + 2 ..][0..2], .little) & 0x3ff);
            k += 2;
        } else if (std.unicode.utf16IsHighSurrogate(u) or std.unicode.utf16IsLowSurrogate(u)) {
            switch (lone) {
                .question => try out.append('?'),
                .replacement => try out.appendSlice("\u{FFFD}"),
                .fail => return null,
            }
            continue;
        }
        try out.appendSlice(tmp[0 .. std.unicode.utf8Encode(cp, &tmp) catch unreachable]);
    }
    return out.items;
}

fn randomUnits(r: std.Random, units: *std.array_list.Managed(u16), n: usize, surrogates: bool) !void {
    const pool = [_]u21{ 'a', 'Z', '0', ' ', '~', 0x7F, 0x80, 0xE7, 0xE3, 0x7FF, 0x800, 0x20AC, 0x4E2D, 0xFFFD, 0xFFFF, 0x10000, 0x1F600, 0x10FFFF };
    units.clearRetainingCapacity();
    for (0..n) |_| {
        if (surrogates and r.uintLessThan(u8, 6) == 0) {
            try units.append(r.intRangeAtMost(u16, 0xD800, 0xDFFF));
            continue;
        }
        const cp: u21 = if (r.uintLessThan(u8, 4) != 0) r.intRangeAtMost(u21, 0x20, 0x7E) else pool[r.uintLessThan(usize, pool.len)];
        if (cp >= 0x10000) {
            const v = cp - 0x10000;
            try units.append(@intCast(0xD800 + (v >> 10)));
            try units.append(@intCast(0xDC00 + (v & 0x3FF)));
        } else try units.append(@intCast(cp));
    }
}

test "utf16: decodeLe pins TDS utf16ToUtf8: BMP text, joined pairs, an unpaired half is `?`" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("hi\u{e9}", try decodeLeAlloc(.question, a, "h\x00i\x00\xe9\x00"));
    try testing.expectEqualStrings("a😀b", try decodeLeAlloc(.question, a, "a\x00\x3d\xd8\x00\xde" ++ "b\x00"));
    const round = try std.unicode.utf8ToUtf16LeAlloc(a, "𝄞x𝕏");
    try testing.expectEqualStrings("𝄞x𝕏", try decodeLeAlloc(.question, a, std.mem.sliceAsBytes(round)));
    try testing.expectEqualStrings("?a", try decodeLeAlloc(.question, a, "\x3d\xd8" ++ "a\x00"));
    try testing.expectEqualStrings("a?", try decodeLeAlloc(.question, a, "a\x00\x3d\xd8"));
    try testing.expectEqualStrings("?", try decodeLeAlloc(.question, a, "\x00\xde"));
    try testing.expectEqualStrings("??", try decodeLeAlloc(.question, a, "\x3d\xd8\x3d\xd8"));
    try testing.expectEqualStrings("?😀", try decodeLeAlloc(.question, a, "\x3d\xd8\x3d\xd8\x00\xde"));
    try testing.expectEqualStrings("a", try decodeLeAlloc(.question, a, "a\x00b"));
    try testing.expectEqualStrings("", try decodeLeAlloc(.question, a, "a"));
    try testing.expectEqualStrings("", try decodeLeAlloc(.question, a, ""));
}

test "utf16: replacement puts one U+FFFD per unpaired half; fail refuses it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("\u{FFFD}a", try decodeLeAlloc(.replacement, a, "\x3d\xd8" ++ "a\x00"));
    try testing.expectEqualStrings("a\u{FFFD}", try decodeLeAlloc(.replacement, a, "a\x00\x3d\xd8"));
    try testing.expectEqualStrings("\u{FFFD}", try decodeLeAlloc(.replacement, a, "\x00\xde"));
    try testing.expectEqualStrings("\u{FFFD}\u{FFFD}", try decodeLeAlloc(.replacement, a, "\x00\xde\x3d\xd8"));
    try testing.expectEqualStrings("a😀b", try decodeLeAlloc(.fail, a, "a\x00\x3d\xd8\x00\xde" ++ "b\x00"));
    for ([_][]const u8{ "\x3d\xd8a\x00", "a\x00\x3d\xd8", "\x00\xde", "\x3d\xd8\x3d\xd8\x00\xde" }) |b|
        try testing.expectError(error.LoneSurrogate, decodeLeAlloc(.fail, a, b));
    var big: [64]u8 = undefined;
    @memset(&big, 0);
    for (0..32) |i| big[i * 2] = 'x';
    big[62] = 0x00;
    big[63] = 0xDE;
    try testing.expectError(error.LoneSurrogate, decodeLe(.fail, try a.alloc(u8, decodedBound(big.len)), &big));
}

test "utf16: every policy matches a scalar reference at every alignment, std where it applies" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x16_16);
    const r = prng.random();
    var units = std.array_list.Managed(u16).init(a);
    for (0..20_000) |i| {
        _ = ar.reset(.retain_capacity);
        units = std.array_list.Managed(u16).init(a);
        try randomUnits(r, &units, r.uintLessThan(usize, 48), i % 2 == 0);
        const whole = try unitsLe(a, units.items);
        const cut = r.uintAtMost(usize, 1);
        const bytes = whole[0 .. whole.len - @min(whole.len, cut)];
        const out = try a.alloc(u8, decodedBound(bytes.len));
        try testing.expectEqualStrings((try reference(a, .question, bytes)).?, out[0..decodeLe(.question, out, bytes)]);
        try testing.expectEqualStrings((try reference(a, .replacement, bytes)).?, out[0..decodeLe(.replacement, out, bytes)]);
        const strict = try reference(a, .fail, bytes);
        const std_units = units.items[0 .. bytes.len / 2];
        if (std.unicode.utf16LeToUtf8Alloc(a, std_units)) |want| {
            try testing.expectEqualStrings(want, strict.?);
            try testing.expectEqualStrings(want, out[0..try decodeLe(.fail, out, bytes)]);
            try testing.expectEqualStrings(want, out[0..decodeLe(.question, out, bytes)]);
        } else |_| {
            try testing.expect(strict == null);
            try testing.expectError(error.LoneSurrogate, decodeLe(.fail, out, bytes));
        }
    }
}

test "utf16: encodeLe matches std's utf8ToUtf16LeAlloc as SMB and NTLM write it, and round-trips" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xe2c0de);
    const r = prng.random();
    var units = std.array_list.Managed(u16).init(a);
    for (0..10_000) |_| {
        _ = ar.reset(.retain_capacity);
        units = std.array_list.Managed(u16).init(a);
        try randomUnits(r, &units, r.uintLessThan(usize, 48), false);
        const text = try std.unicode.utf16LeToUtf8Alloc(a, units.items);
        const wide = try std.unicode.utf8ToUtf16LeAlloc(a, text);
        const want = try unitsLe(a, wide);
        const got = try encodeLeAlloc(a, text);
        try testing.expectEqualSlices(u8, want, got);
        try testing.expectEqualSlices(u8, std.mem.sliceAsBytes(wide), got);
        try testing.expectEqualStrings(text, try decodeLeAlloc(.fail, a, got));
        try testing.expectEqual(unitLen(text), got.len / 2);
    }
}

test "utf16: encodeLe refuses exactly the UTF-8 std refuses" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xbad8);
    const r = prng.random();
    const pool = [_][]const u8{ "a", "abcdefghijklmnop", "\xc3\xa7", "\xe2\x82\xac", "\xf0\x9f\x98\x80", "\xc3", "\x80", "\xff", "\xed\xa0\x80", "\xf4\x90\x80\x80", "\xc0\xaf", "\xe0\x80\xaf" };
    for (0..10_000) |_| {
        _ = ar.reset(.retain_capacity);
        var s = std.array_list.Managed(u8).init(a);
        for (0..r.uintLessThan(usize, 12)) |_| try s.appendSlice(pool[r.uintLessThan(usize, pool.len)]);
        const out = try a.alloc(u8, 2 * s.items.len);
        if (std.unicode.utf8ToUtf16LeAlloc(a, s.items)) |wide| {
            try testing.expectEqualSlices(u8, try unitsLe(a, wide), out[0..try encodeLe(out, s.items)]);
        } else |e| {
            try testing.expectEqual(error.InvalidUtf8, e);
            try testing.expectError(error.InvalidUtf8, encodeLe(out, s.items));
        }
    }
    try testing.expectError(error.InvalidUtf8, encodeLeAlloc(a, "abcdefghijklmnopqrstuvwxyz\xff"));
    try testing.expectEqualSlices(u8, "", try encodeLeAlloc(a, ""));
}

test "utf16: the allocating forms free what they do not return" {
    const gpa = testing.allocator;
    const le = try encodeLeAlloc(gpa, "a😀çabcdefghijklmnopq");
    defer gpa.free(le);
    try testing.expectEqual(@as(usize, 2 * 21), le.len);
    const back = try decodeLeAlloc(.fail, gpa, le);
    defer gpa.free(back);
    try testing.expectEqualStrings("a😀çabcdefghijklmnopq", back);
    const q = try decodeLeAlloc(.question, gpa, "\x3d\xd8a\x00");
    defer gpa.free(q);
    try testing.expectEqualStrings("?a", q);
    const empty = try encodeLeAlloc(gpa, "");
    defer gpa.free(empty);
    try testing.expectEqual(@as(usize, 0), empty.len);
    try testing.expectError(error.InvalidUtf8, encodeLeAlloc(gpa, "abc\xff"));
    try testing.expectError(error.LoneSurrogate, decodeLeAlloc(.fail, gpa, "a\x00\x3d\xd8"));
}

test "utf16: cutLe pins TDS INSERT BULK's cut before a split pair" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const emoji = "\xf0\x9f\x98\x80";
    for (3997..4001) |lead| {
        const s = try a.alloc(u8, lead + emoji.len * 3);
        @memset(s[0..lead], 'x');
        for (0..3) |k| @memcpy(s[lead + k * emoji.len ..][0..emoji.len], emoji);
        const le = try encodeLeAlloc(a, s);
        const want: usize = switch (lead) {
            3997, 3999 => 7998,
            else => 8000,
        };
        const n = cutLe(le, 8000);
        try testing.expectEqual(want, n);
        _ = try decodeLeAlloc(.fail, a, le[0..n]);
    }
    try testing.expectEqual(@as(usize, 4), cutLe("a\x00b\x00", 8000));
    try testing.expectEqual(@as(usize, 2), cutLe("a\x00b\x00", 3));
    try testing.expectEqual(@as(usize, 2), cutLe("a\x00\x3d\xd8\x00\xde", 4));
    try testing.expectEqual(@as(usize, 6), cutLe("a\x00\x3d\xd8\x00\xde", 6));
    try testing.expectEqual(@as(usize, 0), cutLe("\x3d\xd8\x00\xde", 2));
    try testing.expectEqual(@as(usize, 0), cutLe("", 2));
}

test "utf16: unitLen, unitToByte and byteToUnit agree on valid UTF-8" {
    const cases = [_][]const u8{ "", "abc", "ação", "a😀b", "𝄞x𝕏", "中文", "\u{10FFFF}" };
    for (cases) |s| {
        try testing.expectEqual(unitLen(s), byteToUnit(s, s.len));
        try testing.expectEqual(s.len, unitToByte(s, unitLen(s)));
    }
    const text = "SELECT 'é😀' AS x";
    try testing.expectEqual(@as(usize, 8), unitToByte(text, 8));
    try testing.expectEqual(@as(usize, 10), unitToByte(text, 9));
    try testing.expectEqual(@as(usize, 10), unitToByte(text, 10));
    try testing.expectEqual(@as(usize, 14), unitToByte(text, 11));
    try testing.expectEqual(text.len, unitToByte(text, 1000));
    for ([_]usize{ 0, 8, 10, 14, text.len }) |b| try testing.expectEqual(b, unitToByte(text, byteToUnit(text, b)));
    try testing.expectEqual(@as(usize, 17), byteToUnit(text, text.len));
}

test "utf16: pins where xlsx unitLen and cmd_complete byteToUnit differ on invalid UTF-8" {
    try testing.expectEqual(@as(usize, 0), unitLen("\x80"));
    try testing.expectEqual(@as(usize, 1), byteToUnit("\x80", 1));
    try testing.expectEqual(@as(usize, 2), unitLen("\xf8"));
    try testing.expectEqual(@as(usize, 1), byteToUnit("\xf8", 1));
    try testing.expectEqual(@as(usize, 3), unitLen("\xf0a"));
    try testing.expectEqual(@as(usize, 2), byteToUnit("\xf0a", 2));
    try testing.expectEqual(@as(usize, 2), unitLen("\xc3a"));
    try testing.expectEqual(@as(usize, 1), byteToUnit("\xc3a", 2));
    try testing.expectEqual(@as(usize, 2), unitToByte("\xf0a", 2));
    try testing.expectEqual(@as(usize, 0), unitToByte("\xf0a", 1));
}

test "utf16: unitLen counts std's units on random valid text" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x0717);
    const r = prng.random();
    var units = std.array_list.Managed(u16).init(a);
    for (0..5000) |_| {
        try randomUnits(r, &units, r.uintLessThan(usize, 40), false);
        const text = try std.unicode.utf16LeToUtf8Alloc(a, units.items);
        try testing.expectEqual(units.items.len, unitLen(text));
        try testing.expectEqual(units.items.len, byteToUnit(text, text.len));
        try testing.expectEqual(text.len, unitToByte(text, units.items.len));
    }
}
