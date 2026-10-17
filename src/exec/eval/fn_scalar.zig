//! The built-in scalar functions written once as math: each struct names its
//! arguments' kinds and an `apply` over non-null values, and `scalar.zig` builds
//! the row form and the vector kernel from it.

const Cx = @import("scalar.zig").Cx;
const EvalError = @import("../eval.zig").EvalError;
const caseMap = @import("strings.zig").caseMap;
const caseMapInto = @import("strings.zig").caseMapInto;
const charCount = @import("strings.zig").charCount;
const charOffset = @import("strings.zig").charOffset;
const charWidth = @import("strings.zig").charWidth;
const endSlice = @import("strings.zig").endSlice;
const failWith = @import("support.zig").failWith;
const isAscii = @import("strings.zig").isAscii;
const isWordChar = @import("strings.zig").isWordChar;
const likeMatch = @import("strings.zig").likeMatch;
const max_str_bytes = @import("functions.zig").max_str_bytes;
const padChars = @import("strings.zig").padChars;
const parseHexI64 = @import("support.zig").parseHexI64;
const reverseChars = @import("strings.zig").reverseChars;
const std = @import("std");
const substrChars = @import("strings.zig").substrChars;
const trimSpace = @import("support.zig").trim;
const unaccentCp = @import("strings.zig").unaccentCp;

pub fn CaseMap(comptime up: bool) type {
    return struct {
        pub const args = .{.str};
        pub fn apply(cx: Cx, s: []const u8) EvalError![]const u8 {
            if (isAscii(s)) {
                const o = try cx.extend(s.len);
                for (o, s) |*ch, x| ch.* = if (up) std.ascii.toUpper(x) else std.ascii.toLower(x);
            } else try caseMapInto(cx.buf, s, up);
            return cx.written();
        }
    };
}

pub const Trim = struct {
    pub const args = .{.str};
    pub fn apply(_: Cx, s: []const u8) EvalError![]const u8 {
        return trimSpace(s);
    }
};

/// `length` counts characters, `strlen` bytes (DuckDB's split); a BYTES value is
/// bytes either way.
pub fn Length(comptime chars: bool) type {
    return struct {
        pub const args = .{.str};
        pub fn apply(cx: Cx, s: []const u8) EvalError!i64 {
            return @intCast(if (chars and !cx.bytes) charCount(s) else s.len);
        }
    };
}

pub const Affix = enum { starts_with, ends_with, contains, like };

pub fn Match(comptime how: Affix) type {
    return struct {
        pub const args = .{ .str, .str };
        pub fn apply(_: Cx, s: []const u8, p: []const u8) EvalError!bool {
            return switch (how) {
                .starts_with => std.mem.startsWith(u8, s, p),
                .ends_with => std.mem.endsWith(u8, s, p),
                .contains => std.mem.indexOf(u8, s, p) != null,
                .like => likeMatch(s, p),
            };
        }
    };
}

pub const Substr = struct {
    pub const args = .{ .str, .int, .opt_int };
    pub fn apply(cx: Cx, s: []const u8, start: i64, len: ?i64) EvalError![]const u8 {
        return substrChars(cx.arena, s, start, len);
    }
};

pub const Replace = struct {
    pub const args = .{ .str, .str, .str };
    pub fn apply(cx: Cx, s: []const u8, from: []const u8, to: []const u8) EvalError![]const u8 {
        if (from.len == 0) return s;
        _ = std.mem.replace(u8, s, from, to, try cx.extend(std.mem.replacementSize(u8, s, from, to)));
        return cx.written();
    }
};

/// Postgres `left`/`right`: a negative `n` means all but the last/first |n|.
pub fn End(comptime left: bool) type {
    return struct {
        pub const args = .{ .str, .int };
        pub fn apply(_: Cx, s: []const u8, n: i64) EvalError![]const u8 {
            return endSlice(s, n, left);
        }
    };
}

pub const Strpos = struct {
    pub const args = .{ .str, .str };
    pub fn apply(_: Cx, s: []const u8, sub: []const u8) EvalError!i64 {
        if (sub.len == 0) return 1;
        const at = std.mem.indexOf(u8, s, sub) orelse return 0;
        return @as(i64, @intCast(charCount(s[0..at]))) + 1;
    }
};

pub const Reverse = struct {
    pub const args = .{.str};
    pub fn apply(cx: Cx, s: []const u8) EvalError![]const u8 {
        return reverseChars(cx.arena, s);
    }
};

pub fn Pad(comptime left: bool) type {
    return struct {
        pub const args = .{ .str, .int, .opt_str };
        pub fn apply(cx: Cx, s: []const u8, n: i64, fill: ?[]const u8) EvalError![]const u8 {
            return padChars(cx.arena, s, n, fill orelse " ", left);
        }
    };
}

pub const Repeat = struct {
    pub const args = .{ .str, .int };
    pub fn apply(cx: Cx, s: []const u8, n: i64) EvalError![]const u8 {
        if (n <= 0) return "";
        const total = @as(u128, @intCast(n)) * @as(u128, s.len);
        if (total > max_str_bytes) return failWith(error.CastFailed, "repeat: the result would be {d} bytes, past the {d}-byte string limit", .{ total, max_str_bytes });
        const o = try cx.extend(@intCast(total));
        var i: usize = 0;
        while (i < o.len) : (i += s.len) @memcpy(o[i..][0..s.len], s);
        return cx.written();
    }
};

pub const SplitPart = struct {
    pub const args = .{ .str, .str, .int };
    pub fn apply(_: Cx, s: []const u8, delim: []const u8, n: i64) EvalError![]const u8 {
        var want = n;
        if (delim.len == 0) return if (want == 1 or want == -1) s else "";
        if (want < 0) want += @as(i64, @intCast(std.mem.count(u8, s, delim))) + 2;
        if (want < 1) return "";
        var it = std.mem.splitSequence(u8, s, delim);
        var k: i64 = 0;
        while (it.next()) |part| {
            k += 1;
            if (k == want) return part;
        }
        return "";
    }
};

/// Postgres' `translate`, by characters: each character of `from` becomes the one
/// at the same place in `to`, or is deleted when `to` is shorter.
pub const Translate = struct {
    pub const args = .{ .str, .str, .str };
    pub fn apply(cx: Cx, str: []const u8, from: []const u8, to: []const u8) EvalError![]const u8 {
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const ch = str[i..][0..w];
            i += w;
            var at: usize = 0;
            var k: usize = 0;
            const hit = while (k < from.len) {
                const fw = charWidth(from, k);
                if (std.mem.eql(u8, from[k..][0..fw], ch)) break at;
                k += fw;
                at += 1;
            } else null;
            const idx = hit orelse {
                try cx.buf.appendSlice(ch);
                continue;
            };
            const off = charOffset(to, idx);
            if (off < to.len) try cx.buf.appendSlice(to[off..][0..charWidth(to, off)]);
        }
        return cx.written();
    }
};

pub const Initcap = struct {
    pub const args = .{.str};
    pub fn apply(cx: Cx, str: []const u8) EvalError![]const u8 {
        var in_word = false;
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const ch = str[i..][0..w];
            i += w;
            const cp: u21 = if (w == 1) ch[0] else std.unicode.utf8Decode(ch) catch unreachable;
            const word = isWordChar(cp, w);
            if (!word) {
                try cx.buf.appendSlice(ch);
            } else {
                var b: [4]u8 = undefined;
                const n = std.unicode.utf8Encode(caseMap(cp, !in_word), &b) catch unreachable;
                try cx.buf.appendSlice(b[0..n]);
            }
            in_word = word;
        }
        return cx.written();
    }
};

pub const Unaccent = struct {
    pub const args = .{.str};
    pub fn apply(cx: Cx, str: []const u8) EvalError![]const u8 {
        if (isAscii(str)) return str;
        var i: usize = 0;
        while (i < str.len) {
            const w = charWidth(str, i);
            const base = if (w == 1) null else unaccentCp(std.unicode.utf8Decode(str[i..][0..w]) catch unreachable);
            try cx.buf.appendSlice(base orelse str[i..][0..w]);
            i += w;
        }
        return cx.written();
    }
};

pub const Ascii = struct {
    pub const args = .{.str};
    pub fn apply(_: Cx, str: []const u8) EvalError!i64 {
        if (str.len == 0) return 0;
        const w = charWidth(str, 0);
        return if (w == 1) str[0] else std.unicode.utf8Decode(str[0..w]) catch unreachable;
    }
};

pub const Chr = struct {
    pub const args = .{.exact_int};
    pub fn apply(cx: Cx, x: i64) EvalError![]const u8 {
        const bad = "chr: {d} is not a character";
        const cp = std.math.cast(u21, x) orelse return failWith(error.CastFailed, bad, .{x});
        if (cp == 0) return failWith(error.CastFailed, bad, .{x});
        var b: [4]u8 = undefined;
        const n = std.unicode.utf8Encode(cp, &b) catch return failWith(error.CastFailed, bad, .{x});
        try cx.buf.appendSlice(b[0..n]);
        return cx.written();
    }
};

pub const ToBase64 = struct {
    pub const args = .{.str};
    pub fn apply(cx: Cx, data: []const u8) EvalError![]const u8 {
        const enc = std.base64.standard.Encoder;
        return enc.encode(try cx.extend(enc.calcSize(data.len)), data);
    }
};

/// RFC 3986 percent-encoding: all but letters, digits and `-._~` become `%XX`.
/// Decoding leaves `+` alone and passes a stray `%` through, as DuckDB does.
pub fn UrlCode(comptime encode: bool) type {
    return struct {
        pub const args = .{.str};
        pub fn apply(cx: Cx, str: []const u8) EvalError![]const u8 {
            if (encode) {
                for (str) |b| {
                    if (std.ascii.isAlphanumeric(b) or b == '-' or b == '.' or b == '_' or b == '~') {
                        try cx.buf.append(b);
                    } else try cx.buf.writer().print("%{X:0>2}", .{b});
                }
                return cx.written();
            }
            var i: usize = 0;
            while (i < str.len) : (i += 1) {
                if (str[i] == '%' and i + 2 < str.len) {
                    if (std.fmt.parseInt(u8, str[i + 1 .. i + 3], 16)) |b| {
                        try cx.buf.append(b);
                        i += 2;
                        continue;
                    } else |_| {}
                }
                try cx.buf.append(str[i]);
            }
            return cx.written();
        }
    };
}

pub const BitCount = struct {
    pub const args = .{.exact_int};
    pub fn apply(_: Cx, x: i64) EvalError!i64 {
        return @popCount(x);
    }
};

pub const ToHex = struct {
    pub const args = .{.exact_int};
    pub fn apply(cx: Cx, x: i64) EvalError![]const u8 {
        try cx.buf.writer().print("{x}", .{@as(u64, @bitCast(x))});
        return cx.written();
    }
};

pub const FromHex = struct {
    pub const args = .{.str};
    pub fn apply(_: Cx, s: []const u8) EvalError!i64 {
        return parseHexI64(s);
    }
};

/// Null for a negative number, which has no real root.
pub const Sqrt = struct {
    pub const args = .{.num};
    pub fn apply(_: Cx, x: f64) EvalError!?f64 {
        if (x < 0) return null;
        return @sqrt(x);
    }
};

pub const Power = struct {
    pub const args = .{ .num, .num };
    pub fn apply(_: Cx, a: f64, b: f64) EvalError!f64 {
        return std.math.pow(f64, a, b);
    }
};

/// NULL when any part is (`concat_ws` skips them instead).
pub const Concat = struct {
    pub const variadic = true;
    pub const bytes_if_any = true;
    pub fn apply(cx: Cx, parts: []const []const u8) EvalError![]const u8 {
        for (parts) |p| try cx.buf.appendSlice(p);
        return cx.written();
    }
};
