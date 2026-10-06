//! UTF-8 string helpers: character counts and offsets, substr, padding, reversal,
//! case mapping, accent stripping and LIKE matching.

const max_str_bytes = @import("functions.zig").max_str_bytes;
const pow10f = @import("../value.zig").pow10f;
const std = @import("std");

pub inline fn charWidth(s: []const u8, i: usize) usize {
    const b = s[i];
    if (b < 0x80) return 1;
    const n = std.unicode.utf8ByteSequenceLength(b) catch return 1;
    if (i + n > s.len) return 1;
    _ = std.unicode.utf8Decode(s[i..][0..n]) catch return 1;
    return n;
}

pub fn isAscii(s: []const u8) bool {
    var i: usize = 0;
    while (i + 8 <= s.len) : (i += 8) {
        if (std.mem.readInt(u64, s[i..][0..8], .little) & 0x8080808080808080 != 0) return false;
    }
    while (i < s.len) : (i += 1) {
        if (s[i] >= 0x80) return false;
    }
    return true;
}

pub fn charCount(s: []const u8) usize {
    if (isAscii(s)) return s.len;
    var i: usize = 0;
    var n: usize = 0;
    while (i < s.len) : (n += 1) i += charWidth(s, i);
    return n;
}

pub fn charOffset(s: []const u8, k: usize) usize {
    var i: usize = 0;
    var c: usize = 0;
    while (c < k and i < s.len) : (c += 1) i += charWidth(s, i);
    return i;
}

pub fn substrChars(arena: std.mem.Allocator, s: []const u8, start1: i64, len_opt: ?i64) ![]const u8 {
    var start: usize = 0;
    if (start1 > 1) start = charOffset(s, @intCast(start1 - 1));
    var end: usize = s.len;
    if (len_opt) |l| {
        if (l <= 0) return "";
        end = start + charOffset(s[start..], @intCast(l));
    }
    return arena.dupe(u8, s[start..end]);
}

/// Half away from zero (2.5 to 3, -2.5 to -3), not banker's rounding. Engines
/// disagree, so `round` stays out of the pushdown whitelist in runtime/pushdown.zig.
pub fn roundHalfAway(x: f64, digits: i64) f64 {
    if (digits == 0) return @round(x);
    const s = pow10f(@intCast(@min(@abs(digits), 22)));
    return if (digits > 0) @round(x * s) / s else @round(x / s) * s;
}

/// Postgres `lpad`/`rpad`: pads to exactly `n` characters and truncates a longer
/// `s` to its first `n`; an empty `fill` leaves a short `s` unchanged.
pub fn padChars(arena: std.mem.Allocator, s: []const u8, n: i64, fill: []const u8, left: bool) ![]const u8 {
    if (n <= 0) return "";
    const want: usize = @intCast(n);
    if (want > max_str_bytes) return error.CastFailed;
    const have = charCount(s);
    if (have >= want) return arena.dupe(u8, s[0..charOffset(s, want)]);
    if (fill.len == 0) return arena.dupe(u8, s);
    var pad = std.array_list.Managed(u8).init(arena);
    var fi: usize = 0;
    var k: usize = 0;
    while (k < want - have) : (k += 1) {
        if (fi == fill.len) fi = 0;
        const w = charWidth(fill, fi);
        try pad.appendSlice(fill[fi..][0..w]);
        fi += w;
    }
    if (pad.items.len + s.len > max_str_bytes) return error.CastFailed;
    return std.mem.concat(arena, u8, if (left) &.{ pad.items, s } else &.{ s, pad.items });
}

/// Postgres `left`/`right`: a negative `n` means all but the last/first |n|
/// characters, rather than clamping to empty.
pub fn endSlice(s: []const u8, n: i64, left: bool) []const u8 {
    const slen: i64 = @intCast(charCount(s));
    var take: i64 = if (n >= 0) n else slen + n;
    if (take < 0) take = 0;
    if (take > slen) take = slen;
    const k: usize = @intCast(take);
    return if (left) s[0..charOffset(s, k)] else s[charOffset(s, @intCast(slen - take))..];
}

pub fn reverseChars(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    const out = try arena.alloc(u8, s.len);
    var i: usize = 0;
    while (i < s.len) {
        const w = charWidth(s, i);
        @memcpy(out[s.len - i - w ..][0..w], s[i..][0..w]);
        i += w;
    }
    return out;
}

/// One-to-one case mapping for ASCII, Latin-1, Latin Extended-A, Greek and
/// Cyrillic; anything else, and length-changing mappings like `ß`, stay as is.
pub fn caseMap(cp: u21, up: bool) u21 {
    if (cp < 0x80) return if (up) std.ascii.toUpper(@intCast(cp)) else std.ascii.toLower(@intCast(cp));
    if (up) {
        if ((cp >= 0xE0 and cp <= 0xFE and cp != 0xF7)) return cp - 0x20;
        if (cp == 0xFF) return 0x178;
        if (cp >= 0x100 and cp <= 0x17F) return latinExtA(cp, true);
        if (cp >= 0x3B1 and cp <= 0x3C9 and cp != 0x3C2) return cp - 0x20;
        if (cp == 0x3C2) return 0x3A3;
        if (cp == 0x3AC) return 0x386;
        if (cp >= 0x3AD and cp <= 0x3AF) return cp - 0x25;
        if (cp == 0x3CC) return 0x38C;
        if (cp == 0x3CD or cp == 0x3CE) return cp - 0x3F;
        if (cp >= 0x430 and cp <= 0x44F) return cp - 0x20;
        if (cp >= 0x450 and cp <= 0x45F) return cp - 0x50;
    } else {
        if ((cp >= 0xC0 and cp <= 0xDE and cp != 0xD7)) return cp + 0x20;
        if (cp == 0x178) return 0xFF;
        if (cp >= 0x100 and cp <= 0x17F) return latinExtA(cp, false);
        if (cp >= 0x391 and cp <= 0x3A9 and cp != 0x3A2) return cp + 0x20;
        if (cp == 0x386) return 0x3AC;
        if (cp >= 0x388 and cp <= 0x38A) return cp + 0x25;
        if (cp == 0x38C) return 0x3CC;
        if (cp == 0x38E or cp == 0x38F) return cp + 0x3F;
        if (cp >= 0x410 and cp <= 0x42F) return cp + 0x20;
        if (cp >= 0x400 and cp <= 0x40F) return cp + 0x50;
    }
    return cp;
}

/// Latin Extended-A pairs on adjacent code points: even/odd through U+0137 and from
/// U+014A, odd/even across U+0139-U+0148 and U+0179-U+017E; the rest have no partner.
fn latinExtA(cp: u21, up: bool) u21 {
    if (cp == 0x130 or cp == 0x131 or cp == 0x138 or cp == 0x149 or cp == 0x17F or cp == 0x178) return cp;
    const odd_upper = (cp >= 0x139 and cp <= 0x148) or (cp >= 0x179 and cp <= 0x17E);
    const is_upper = if (odd_upper) cp % 2 == 1 else cp % 2 == 0;
    if (up and !is_upper) return if (odd_upper) cp - 1 else cp - 1;
    if (!up and is_upper) return cp + 1;
    return cp;
}

const unaccent_base = "AAAAAA*CEEEEIIIIDNOOOOO-OUUUUY**" ++ "aaaaaa*ceeeeiiiidnooooo-ouuuuy*y" ++
    "AaAaAaCcCcCcCcDdDdEeEeEeEeEeGgGgGgGgHhHhIiIiIiIiIi**JjKkkLlLlLlLlLlNnNnNnnNnOoOoOo**RrRrRrSsSsSsSsTtTtTtUuUuUuUuUuUuWwYyYZzZzZzs";

comptime {
    std.debug.assert(unaccent_base.len == 0x180 - 0xC0);
}

/// What `unaccent` writes for `cp`, or null to keep it. In `unaccent_base`, `*`
/// marks a ligature spelled here and `-` a non-letter; combining accents are dropped.
pub fn unaccentCp(cp: u21) ?[]const u8 {
    if (cp >= 0x300 and cp <= 0x36F) return "";
    if (cp < 0xC0 or cp >= 0x180) return null;
    const b = unaccent_base[cp - 0xC0];
    if (b == '-') return null;
    if (b != '*') return unaccent_base[cp - 0xC0 ..][0..1];
    return switch (cp) {
        0xC6 => "AE",
        0xE6 => "ae",
        0xDE => "TH",
        0xFE => "th",
        0xDF => "ss",
        0x132 => "IJ",
        0x133 => "ij",
        0x152 => "OE",
        0x153 => "oe",
        else => unreachable,
    };
}

pub fn isWordChar(cp: u21, w: usize) bool {
    if (cp < 0x80) return std.ascii.isAlphanumeric(@intCast(cp));
    if (w == 1) return false;
    return cp == 0xDF or caseMap(cp, true) != cp or caseMap(cp, false) != cp;
}

pub fn caseMapInto(out: *std.array_list.Managed(u8), s: []const u8, up: bool) !void {
    var i: usize = 0;
    while (i < s.len) {
        const w = charWidth(s, i);
        if (w == 1) {
            try out.append(if (s[i] < 0x80) (if (up) std.ascii.toUpper(s[i]) else std.ascii.toLower(s[i])) else s[i]);
        } else {
            const cp = std.unicode.utf8Decode(s[i..][0..w]) catch unreachable;
            var buf: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(caseMap(cp, up), &buf) catch unreachable;
            try out.appendSlice(buf[0..n]);
        }
        i += w;
    }
}

/// SQL `LIKE`. `%` is tested before the literal compare: otherwise a `%` in the
/// text matched a `%` in the pattern literally, and `'50% off' LIKE '50%'` was false.
pub fn likeMatch(s: []const u8, pat: []const u8) bool {
    var si: usize = 0;
    var pi: usize = 0;
    var star: ?usize = null;
    var smark: usize = 0;
    while (si < s.len) {
        if (pi < pat.len and pat[pi] == '%') {
            star = pi;
            smark = si;
            pi += 1;
        } else if (pi < pat.len and pat[pi] == '_') {
            si += charWidth(s, si);
            pi += 1;
        } else if (pi < pat.len and pat[pi] == s[si]) {
            si += 1;
            pi += 1;
        } else if (star) |st| {
            pi = st + 1;
            smark += charWidth(s, smark);
            si = smark;
        } else return false;
    }
    while (pi < pat.len and pat[pi] == '%') pi += 1;
    return pi == pat.len;
}
