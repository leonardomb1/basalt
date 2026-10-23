//! The built-in scalar functions written once as math: each struct names its
//! arguments' kinds and an `apply` over non-null values, and `scalar.zig` builds
//! the row form and the vector kernel from it.

const Cx = @import("scalar.zig").Cx;
const Decimal = @import("../value.zig").Decimal;
const pow10_i128 = @import("../value.zig").pow10_i128;
const Numeric = @import("scalar.zig").Numeric;
const Overload = @import("scalar.zig").Overload;
const Type = @import("../../lang/types.zig").Type;
const TypeKind = @import("../../lang/types.zig").TypeKind;
const ast = @import("../../lang/ast.zig");
const roundDecimal = @import("cast.zig").roundDecimal;
const roundHalfAway = @import("strings.zig").roundHalfAway;
const roundOutScale = @import("cast.zig").roundOutScale;
const time = @import("time.zig");
const TimeUnit = time.TimeUnit;
const clip = @import("support.zig").clip;
const regex = @import("../regex.zig");
const json = @import("../json.zig");
const cachedRegex = @import("support.zig").cachedRegex;
const cachedRegexOpts = @import("support.zig").cachedRegexOpts;
const regexErr = @import("support.zig").regexErr;
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
const percent = @import("../../net/percent.zig");
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
                try percent.encode(cx.buf, str);
                return cx.written();
            }
            const at = cx.buf.items.len;
            try cx.buf.appendSlice(str);
            const out = std.Uri.percentDecodeInPlace(cx.buf.items[at..]);
            std.mem.copyForwards(u8, cx.buf.items[at..], out);
            cx.buf.shrinkRetainingCapacity(at + out.len);
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

/// The INT `minInt` has no absolute value.
pub const Abs = Numeric(struct {
    pub fn int(x: i64) EvalError!i64 {
        if (x == std.math.minInt(i64)) return error.IntOverflow;
        return if (x < 0) -x else x;
    }
    pub fn float(x: f64) f64 {
        return @abs(x);
    }
    pub fn dec(d: Decimal) Decimal {
        return .{ .unscaled = if (d.unscaled < 0) -d.unscaled else d.unscaled, .scale = d.scale };
    }
});

/// `floor` (`down`) and `ceil`: an INT as it is, a DECIMAL exactly to scale 0.
pub fn Whole(comptime down: bool) type {
    return Numeric(struct {
        pub fn int(x: i64) i64 {
            return x;
        }
        pub fn float(x: f64) f64 {
            return if (down) @floor(x) else @ceil(x);
        }
        pub fn dec(d: Decimal) Decimal {
            if (d.scale >= pow10_i128.len) return .{ .unscaled = 0, .scale = 0 };
            const p = pow10_i128[d.scale];
            var q = @divTrunc(d.unscaled, p);
            if (@rem(d.unscaled, p) != 0) {
                if (down and d.unscaled < 0) q -= 1;
                if (!down and d.unscaled > 0) q += 1;
            }
            return .{ .unscaled = q, .scale = 0 };
        }
        pub fn decType(t: Type) Type {
            return Type.decimal(@min(38, (t.precision -| t.scale) + 1), 0);
        }
    });
}

/// A FLOAT's sign is a FLOAT (NaN stays NaN), a DECIMAL's a DECIMAL(1,0), and any
/// other value's an INT.
pub const Sign = Numeric(struct {
    pub fn int(x: i64) i64 {
        return std.math.sign(x);
    }
    pub fn float(x: f64) f64 {
        if (std.math.isNan(x)) return x;
        return if (x > 0) 1 else if (x < 0) -1 else 0;
    }
    pub fn dec(d: Decimal) Decimal {
        return .{ .unscaled = std.math.sign(d.unscaled), .scale = 0 };
    }
    pub fn decType(_: Type) Type {
        return Type.decimal(1, 0);
    }
    pub fn other(x: f64) i64 {
        return if (x > 0) 1 else if (x < 0) -1 else 0;
    }
});

/// `round(x[, digits])`, half away from zero: an INT without digits as it is, with
/// them a FLOAT; a DECIMAL exactly, at the scale `roundOutScale` gives each value.
pub const Round = Overload(struct {
    pub const variants = .{ RoundInt, RoundFloat, RoundDec };
    pub fn pick(c: ast.Expr.Call, k: TypeKind) ?usize {
        return switch (k) {
            .int => if (c.args.len == 1) 0 else 1,
            .float => 1,
            .decimal => 2,
            else => null,
        };
    }
});

const RoundInt = struct {
    pub const args = .{ .exact_int, .opt_int };
    pub fn apply(_: Cx, x: i64, _: ?i64) EvalError!i64 {
        return x;
    }
};

const RoundFloat = struct {
    pub const args = .{ .num, .opt_int };
    pub fn apply(_: Cx, x: f64, digits: ?i64) EvalError!f64 {
        return roundHalfAway(x, digits orelse 0);
    }
};

const RoundDec = struct {
    pub const args = .{ .dec, .opt_int };
    pub const value_scale = true;
    pub fn resultType(c: ast.Expr.Call, arg: Type) Type {
        return Type.decimal(arg.precision, roundOutScale(c, arg.scale));
    }
    /// Digits past ±39 round as ±39 do (nothing, or to zero), so no count overflows.
    pub fn apply(cx: Cx, d: Decimal, digits: ?i64) EvalError!Decimal {
        return roundDecimal(d, std.math.clamp(digits orelse 0, -39, 39), roundOutScale(cx.call, d.scale)) orelse error.IntOverflow;
    }
};

/// `date_trunc`: a DATE or TIMESTAMP down to the start of its unit, as a TIMESTAMP.
pub const DateTrunc = struct {
    pub const args = .{ .unit, .temporal };
    pub const result = .timestamp;
    pub fn apply(_: Cx, u: TimeUnit, us: i64) EvalError!i64 {
        return time.truncMicros(us, u);
    }
};

pub const Extract = struct {
    pub const args = .{ .unit, .temporal };
    pub fn apply(_: Cx, u: TimeUnit, us: i64) EvalError!i64 {
        return time.extractField(us, u);
    }
};

pub const DateDiff = struct {
    pub const args = .{ .unit, .temporal, .temporal };
    pub const eval_all = true;
    pub fn apply(_: Cx, u: TimeUnit, a: i64, b: i64) EvalError!i64 {
        return time.dateDiff(a, b, u);
    }
};

/// `date_add(unit, n, t)`: a DATE stays a DATE (a unit finer than a day is a
/// TypeMismatch), a TIMESTAMP a TIMESTAMP.
pub const DateAdd = Overload(struct {
    pub const on = 2;
    pub const variants = .{ DateAddDays, DateAddMicros };
    pub fn pick(_: ast.Expr.Call, k: TypeKind) ?usize {
        return switch (k) {
            .date => 0,
            .timestamp => 1,
            else => null,
        };
    }
});

const DateAddDays = struct {
    pub const args = .{ .unit, .int, .day };
    pub const eval_all = true;
    pub fn apply(_: Cx, u: TimeUnit, n: i64, d: i32) EvalError!i32 {
        return time.addDays(d, u, n);
    }
};

const DateAddMicros = struct {
    pub const args = .{ .unit, .int, .micros };
    pub const eval_all = true;
    pub const result = .timestamp;
    pub fn apply(_: Cx, u: TimeUnit, n: i64, us: i64) EvalError!i64 {
        return time.addMicros(us, u, n);
    }
};

pub const MakeDate = struct {
    pub const args = .{ .int, .int, .int };
    pub const eval_all = true;
    pub fn apply(_: Cx, y: i64, m: i64, d: i64) EvalError!i32 {
        const bad = "make_date: year {d}, month {d}, day {d} is not a date";
        if (y < -1_000_000_000 or y > 1_000_000_000 or m < 1 or m > 12 or d < 1 or d > time.daysInMonth(y, @intCast(m)))
            return failWith(error.CastFailed, bad, .{ y, m, d });
        const days = time.daysFromCivil(y, @intCast(m), @intCast(d));
        return std.math.cast(i32, days) orelse failWith(error.CastFailed, bad, .{ y, m, d });
    }
};

/// Whole seconds since the epoch, rounded down.
pub const Epoch = struct {
    pub const args = .{.temporal};
    pub fn apply(_: Cx, us: i64) EvalError!i64 {
        return @divFloor(us, 1_000_000);
    }
};

/// Seconds since the epoch as a TIMESTAMP, a fraction kept to the microsecond as
/// PostgreSQL's `to_timestamp` keeps it; a value not a FLOAT or DECIMAL is read as
/// an INT.
pub const ToTimestamp = Overload(struct {
    pub const variants = .{ ToTimestampFloat, ToTimestampDec, ToTimestampInt };
    pub fn pick(_: ast.Expr.Call, k: TypeKind) ?usize {
        return switch (k) {
            .float => 0,
            .decimal => 1,
            else => 2,
        };
    }
});

const ToTimestampFloat = struct {
    pub const args = .{.num};
    pub const result = .timestamp;
    pub fn apply(_: Cx, x: f64) EvalError!i64 {
        const us = @round(x * 1_000_000);
        if (!std.math.isFinite(us) or @abs(us) > 9.2e18) return error.IntOverflow;
        return @intFromFloat(us);
    }
};

const ToTimestampDec = struct {
    pub const args = .{.dec};
    pub const result = .timestamp;
    pub fn apply(_: Cx, d: Decimal) EvalError!i64 {
        var q: i128 = d.unscaled;
        var s: u8 = d.scale;
        while (s < 6) : (s += 1) q = std.math.mul(i128, q, 10) catch return error.IntOverflow;
        while (s > 6) : (s -= 1) q = @divTrunc(q + (if (s == 7) @as(i128, if (q < 0) -5 else 5) else 0), 10);
        return std.math.cast(i64, q) orelse error.IntOverflow;
    }
};

const ToTimestampInt = struct {
    pub const args = .{.int};
    pub const result = .timestamp;
    pub fn apply(_: Cx, x: i64) EvalError!i64 {
        return time.mulI64(x, 1_000_000);
    }
};

/// A literal format is checked when the script is.
pub const Strftime = struct {
    pub const args = .{ .temporal, .str };
    pub const typed_last = true;
    pub fn apply(cx: Cx, us: i64, fmt: []const u8) EvalError![]const u8 {
        try time.strftimeInto(cx.buf, us, fmt);
        return cx.written();
    }
};

/// `try_strptime` (`lenient`) gives NULL where `strptime` fails.
pub fn Strptime(comptime lenient: bool) type {
    return struct {
        pub const args = .{ .str, .str };
        pub const result = .timestamp;
        pub fn apply(_: Cx, text: []const u8, fmt: []const u8) EvalError!?i64 {
            return time.strptimeFmt(text, fmt) orelse {
                if (lenient) return null;
                return failWith(error.CastFailed, "strptime: '{s}' is not a date in '{s}' (try_strptime gives null)", .{ clip(text), clip(fmt) });
            };
        }
    };
}

pub const Power = struct {
    pub const args = .{ .num, .num };
    pub fn apply(_: Cx, a: f64, b: f64) EvalError!f64 {
        return std.math.pow(f64, a, b);
    }
};

/// The first match of `p` in `s`, its groups in `caps`. A pattern comes from the
/// thread's cache, so a kernel compiles a constant one once per batch.
fn regexFind(s: []const u8, p: []const u8, caps: *regex.Captures) EvalError!?[2]usize {
    const re = cachedRegex(p) catch |e| return regexErr(e);
    return re.find(s, 0, caps) catch |e| regexErr(e);
}

pub const RegexpMatches = struct {
    pub const args = .{ .str, .str };
    pub fn apply(_: Cx, s: []const u8, p: []const u8) EvalError!bool {
        var caps: regex.Captures = undefined;
        return (try regexFind(s, p, &caps)) != null;
    }
};

/// Null where the pattern does not match, unlike DuckDB's '', which a load cannot
/// tell from an empty field.
pub const RegexpExtract = struct {
    pub const args = .{ .str, .str, .opt_int };
    pub fn apply(_: Cx, s: []const u8, p: []const u8, group: ?i64) EvalError!?[]const u8 {
        var caps: regex.Captures = undefined;
        if (try regexFind(s, p, &caps) == null) return null;
        const span = caps[@intCast(group orelse 0)] orelse return null;
        return s[span[0]..span[1]];
    }
};

/// The kernel of `regexp_replace`, whose row form evaluates its arguments in its own
/// order (`regexpReplaceText`).
pub const RegexpReplace = struct {
    pub const args = .{ .str, .str, .str, .opt_str };
    pub fn apply(cx: Cx, s: []const u8, p: []const u8, rep: []const u8, flags: ?[]const u8) EvalError![]const u8 {
        return regexpReplaceText(cx.arena, s, p, rep, flags);
    }
};

/// `s` with the first match of `p` (every one, flag `g`; ignoring case, flag `i`)
/// replaced by `rep`.
pub fn regexpReplaceText(arena: std.mem.Allocator, s: []const u8, p: []const u8, rep: []const u8, flag_text: ?[]const u8) EvalError![]const u8 {
    var flags = regex.Flags{};
    if (flag_text) |t| flags = regex.parseFlags(t) orelse return error.CastFailed;
    const re = cachedRegexOpts(p, .{ .icase = flags.icase }) catch |e| return regexErr(e);
    const replaceFn = if (flags.global) &regex.replaceAllRe else &regex.replaceFirstRe;
    return replaceFn(arena, re, s, rep) catch |e| regexErr(e);
}

/// The value at a path of a JSON document, as text; NULL where the path leads
/// nowhere or to a JSON null.
pub const JsonGet = struct {
    pub const args = .{ .str, .str };
    pub const eval_all = true;
    pub fn apply(cx: Cx, doc: []const u8, path: []const u8) EvalError!?[]const u8 {
        try json.validate(cx.arena, doc);
        const leaf = (try json.path(cx.arena, doc, path)) orelse return null;
        return switch (try json.cell(cx.arena, leaf)) {
            .null => null,
            .text => |t| t,
        };
    }
};

/// The number of elements of a JSON array; any other document is invalid here.
pub const JsonLength = struct {
    pub const args = .{.str};
    pub fn apply(cx: Cx, doc: []const u8) EvalError!i64 {
        try json.validate(cx.arena, doc);
        if (json.rootKind(doc) != .array) return error.InvalidJson;
        var items = json.Elements.root(doc);
        var n: i64 = 0;
        while (items.next()) |_| n += 1;
        return n;
    }
};

/// The characters of a text as a JSON array of strings.
pub const Chars = struct {
    pub const args = .{.str};
    pub fn apply(cx: Cx, str: []const u8) EvalError![]const u8 {
        var out = std.Io.Writer.Allocating.init(cx.arena);
        const w = &out.writer;
        w.writeByte('[') catch return error.OutOfMemory;
        var i: usize = 0;
        while (i < str.len) {
            const cw = charWidth(str, i);
            if (i > 0) w.writeByte(',') catch return error.OutOfMemory;
            std.json.Stringify.encodeJsonString(str[i..][0..cw], .{}, w) catch return error.OutOfMemory;
            i += cw;
            if (out.written().len > max_str_bytes) return failWith(error.CastFailed, "chars: the array passes {d} bytes", .{max_str_bytes});
        }
        w.writeByte(']') catch return error.OutOfMemory;
        return out.written();
    }
};

/// `md5` and `sha256` as lowercase hex.
pub fn Digest(comptime H: type) type {
    return struct {
        pub const args = .{.str};
        pub fn apply(cx: Cx, data: []const u8) EvalError![]const u8 {
            var d: [H.digest_length]u8 = undefined;
            H.hash(data, &d, .{});
            const hex = std.fmt.bytesToHex(d, .lower);
            @memcpy(try cx.extend(hex.len), &hex);
            return cx.written();
        }
    };
}

pub const XxHash64 = struct {
    pub const args = .{.str};
    pub fn apply(_: Cx, data: []const u8) EvalError!i64 {
        return @bitCast(std.hash.XxHash64.hash(0, data));
    }
};

/// Surrounding whitespace is ignored.
pub const FromBase64 = struct {
    pub const args = .{.str};
    pub const result = .bytes;
    pub fn apply(cx: Cx, s: []const u8) EvalError![]const u8 {
        const text = std.mem.trim(u8, s, " \t\r\n");
        const dec = std.base64.standard.Decoder;
        const bad = "from_base64: '{s}' is not base64";
        const out = try cx.extend(dec.calcSizeForSlice(text) catch return failWith(error.CastFailed, bad, .{clip(text)}));
        dec.decode(out, text) catch return failWith(error.CastFailed, bad, .{clip(text)});
        return cx.written();
    }
};

/// Postgres' `concat_ws`: NULL parts are skipped, where `concat` is NULL when any
/// part is; a NULL separator is NULL.
pub const ConcatWs = struct {
    pub const variadic = true;
    pub const skip_null = true;
    pub fn apply(cx: Cx, parts: []const []const u8) EvalError![]const u8 {
        for (parts[1..], 0..) |p, i| {
            if (i > 0) try cx.buf.appendSlice(parts[0]);
            try cx.buf.appendSlice(p);
        }
        return cx.written();
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
