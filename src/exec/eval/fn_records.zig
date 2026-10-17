//! Functions over raw record bytes, for formats built in SQL: byte access, charset
//! decode and encode, field decoders (`*_at(rec, pos, len, …)`) and field encoders
//! (`fmt_*`), each written once as math for `scalar.zig`'s factory.
//!
//! Byte positions count from 0. A field out of the record's range, or one that does
//! not read as its kind, is an error naming the field and its bytes, never a guess or
//! a NULL; a field of spaces decodes as NULL, and a date field may name another NULL
//! marker (`'000000'`). Two-digit years read 69–99 as 19xx and 00–68 as 20xx, as
//! POSIX `%y` does, and only years in that window are written as two digits. An
//! encoder writes a NULL as spaces, as fixed-width files spell one (binary encoders
//! leave it NULL), refuses a value too wide for its field, and never rounds: a
//! decimal with more places than its field has nonzero digits there is refused.
//! `read_bytes` and `file_size` read a file by range wherever it is (`bytes.zig`).

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const Type = @import("../../lang/types.zig").Type;
const Decimal = @import("../value.zig").Decimal;
const EvalError = @import("../eval.zig").EvalError;
const TypeCtx = @import("../eval.zig").TypeCtx;
const TypeError = @import("../eval.zig").TypeError;
const Cx = @import("scalar.zig").Cx;
const failWith = @import("support.zig").failWith;
const time = @import("time.zig");
const charset = @import("../../format/charset.zig");
const bytes = @import("../../format/bytes.zig");
const vecmod = @import("vec.zig");
const column = @import("../column.zig");
const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;

const max_digits = 38;

fn fieldOf(comptime name: []const u8, rec: []const u8, pos: i64, len: i64) EvalError![]const u8 {
    if (pos < 0 or len < 0 or pos + len > rec.len)
        return failWith(error.CastFailed, "`" ++ name ++ "`: bytes {d}..{d} are outside the record's {d} bytes", .{ pos, pos + len, rec.len });
    const p: usize = @intCast(pos);
    return rec[p .. p + @as(usize, @intCast(len))];
}

fn bad(comptime name: []const u8, raw: []const u8, comptime what: []const u8) EvalError {
    var buf: [64]u8 = undefined;
    return failWith(error.CastFailed, "`" ++ name ++ "`: `{s}` is not " ++ what, .{shown(&buf, raw)});
}

/// `raw` as a message shows it: printable ASCII as is, anything else as `\xNN`.
fn shown(buf: *[64]u8, raw: []const u8) []const u8 {
    var w = std.Io.Writer.fixed(buf);
    for (raw) |c| {
        if (c >= 0x20 and c < 0x7f) w.writeByte(c) catch break else w.print("\\x{X:0>2}", .{c}) catch break;
    }
    return w.buffered();
}

fn blank(s: []const u8) bool {
    for (s) |c| if (c != ' ') return false;
    return true;
}

fn litInt(c: ast.Expr.Call, i: usize) ?i64 {
    if (i >= c.args.len) return null;
    return switch (c.args[i].*) {
        .int_lit => |x| x,
        else => null,
    };
}

fn litStr(c: ast.Expr.Call, i: usize) ?[]const u8 {
    if (i >= c.args.len) return null;
    return switch (c.args[i].*) {
        .str_lit => |s| s,
        else => null,
    };
}

fn wantLitInt(self: *TypeCtx, c: ast.Expr.Call, i: usize, what: []const u8, lo: i64, hi: i64) TypeError!void {
    const x = litInt(c, i) orelse return self.err("`{s}` needs its {s} as an integer literal", .{ c.name, what });
    if (x < lo or x > hi) return self.err("`{s}` {s} must be {d} to {d}, not {d}", .{ c.name, what, lo, hi, x });
}

fn wantCharset(self: *TypeCtx, c: ast.Expr.Call, i: usize) TypeError!void {
    if (i >= c.args.len) return;
    const s = litStr(c, i) orelse return;
    if (charset.Charset.parse(s) == null) return self.err("`{s}`: unknown charset `{s}` (" ++ charset.names ++ ")", .{ c.name, s });
}

fn charsetArg(comptime name: []const u8, s: ?[]const u8) EvalError!charset.Charset {
    const n = s orelse return .utf8;
    return charset.Charset.parse(n) orelse failWith(error.CastFailed, "`" ++ name ++ "`: unknown charset `{s}`", .{n});
}

pub const ByteLen = struct {
    pub const args = .{.str};
    pub const signature = "(bytes)";
    pub fn apply(_: Cx, b: []const u8) EvalError!i64 {
        return @intCast(b.len);
    }
};

pub const ByteSlice = struct {
    pub const args = .{ .str, .int, .int };
    pub const result = .bytes;
    pub const signature = "(bytes, pos, len)";
    pub fn apply(_: Cx, b: []const u8, pos: i64, len: i64) EvalError![]const u8 {
        return fieldOf("byte_slice", b, pos, len);
    }
};

pub const ByteAt = struct {
    pub const args = .{ .str, .int };
    pub const signature = "(bytes, pos)";
    pub fn apply(_: Cx, b: []const u8, pos: i64) EvalError!i64 {
        return (try fieldOf("byte_at", b, pos, 1))[0];
    }
};

pub const ByteFind = struct {
    pub const args = .{ .str, .str, .opt_int };
    pub const signature = "(bytes, needle[, from])";
    pub const may_null = true;
    pub fn apply(_: Cx, b: []const u8, needle: []const u8, from: ?i64) EvalError!?i64 {
        const start = from orelse 0;
        if (start < 0 or start > b.len) return failWith(error.CastFailed, "`byte_find`: start {d} is outside the {d} bytes", .{ start, b.len });
        const at = std.mem.indexOfPos(u8, b, @intCast(start), needle) orelse return null;
        return @intCast(at);
    }
};

pub const Hex = struct {
    pub const args = .{.str};
    pub const signature = "(bytes)";
    pub fn apply(cx: Cx, b: []const u8) EvalError![]const u8 {
        const out = try cx.extend(b.len * 2);
        for (b, 0..) |c, i| _ = std.fmt.bufPrint(out[i * 2 ..][0..2], "{X:0>2}", .{c}) catch unreachable;
        return cx.written();
    }
};

pub const Unhex = struct {
    pub const args = .{.str};
    pub const result = .bytes;
    pub const signature = "(hex text)";
    pub fn apply(cx: Cx, t: []const u8) EvalError![]const u8 {
        if (t.len % 2 != 0) return bad("unhex", t, "an even number of hex digits");
        const out = try cx.extend(t.len / 2);
        for (out, 0..) |*o, i| o.* = std.fmt.parseInt(u8, t[i * 2 ..][0..2], 16) catch return bad("unhex", t, "hex digits");
        return cx.written();
    }
};

pub const Decode = struct {
    pub const args = .{ .str, .str };
    pub const signature = "(bytes, charset)";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try wantCharset(self, c, 1);
    }
    pub fn apply(cx: Cx, b: []const u8, cs: []const u8) EvalError![]const u8 {
        return charset.decode(cx.arena, try charsetArg("decode", cs), b);
    }
};

pub const Encode = struct {
    pub const args = .{ .str, .str };
    pub const result = .bytes;
    pub const signature = "(text, charset)";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try wantCharset(self, c, 1);
    }
    pub fn apply(cx: Cx, t: []const u8, cs: []const u8) EvalError![]const u8 {
        return charset.encode(cx.arena, try charsetArg("encode", cs), t) catch |e| switch (e) {
            error.Unencodable => bad("encode", t, "text the charset holds"),
            else => error.OutOfMemory,
        };
    }
};

pub const TextAt = struct {
    pub const args = .{ .str, .int, .int, .opt_str };
    pub const signature = "(rec, pos, len[, charset])";
    pub const may_null = true;
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try wantCharset(self, c, 3);
    }
    pub fn apply(cx: Cx, rec: []const u8, pos: i64, len: i64, cs: ?[]const u8) EvalError!?[]const u8 {
        const which = try charsetArg("text_at", cs);
        const raw = try fieldOf("text_at", rec, pos, len);
        const t = std.mem.trimRight(u8, try charset.decode(cx.arena, which, raw), " ");
        return if (t.len == 0) null else t;
    }
};

/// An integer field: spaces around it allowed, one leading sign, then only digits;
/// leading zeros are digits like any other.
fn digitsInt(comptime name: []const u8, raw: []const u8) EvalError!?i64 {
    const t = std.mem.trim(u8, raw, " ");
    if (t.len == 0) return null;
    var i: usize = 0;
    const neg = t[0] == '-';
    if (t[0] == '-' or t[0] == '+') i = 1;
    if (i == t.len) return bad(name, raw, "an integer");
    var v: i64 = 0;
    for (t[i..]) |c| {
        if (c < '0' or c > '9') return bad(name, raw, "an integer");
        v = std.math.mul(i64, v, 10) catch return bad(name, raw, "an integer in range");
        v = std.math.add(i64, v, c - '0') catch return bad(name, raw, "an integer in range");
    }
    return if (neg) -v else v;
}

pub const IntAt = struct {
    pub const args = .{ .str, .int, .int };
    pub const signature = "(rec, pos, len)";
    pub const may_null = true;
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64) EvalError!?i64 {
        return digitsInt("int_at", try fieldOf("int_at", rec, pos, len));
    }
};

/// DECIMAL(len, scale) from a call's literal `len` and `scale` arguments.
fn decimalType(c: ast.Expr.Call, len_at: usize, scale_at: usize, digits: fn (i64) i64) Type {
    const len = litInt(c, len_at) orelse max_digits;
    const scale = litInt(c, scale_at) orelse 0;
    const p = std.math.clamp(digits(len), 1, max_digits);
    return Type.decimal(@intCast(p), @intCast(std.math.clamp(scale, 0, p)));
}

fn sameDigits(len: i64) i64 {
    return len;
}

fn checkDecimal(self: *TypeCtx, c: ast.Expr.Call, len_at: usize, scale_at: usize) TypeError!void {
    try wantLitInt(self, c, len_at, "length", 1, max_digits + 2);
    try wantLitInt(self, c, scale_at, "scale", 0, max_digits);
}

/// A decimal field: implied decimals (`scale` of the digits are places) without
/// `sep`, else `sep` the decimal point and the other of `.` and `,` a grouping mark.
fn digitsDecimal(comptime name: []const u8, raw: []const u8, scale: i64, sep: ?[]const u8) EvalError!?Decimal {
    const t = std.mem.trim(u8, raw, " ");
    if (t.len == 0) return null;
    if (scale < 0 or scale > max_digits) return failWith(error.CastFailed, "`" ++ name ++ "`: scale {d} is out of range", .{scale});
    const point: ?u8 = if (sep) |s| (if (s.len == 1 and (s[0] == '.' or s[0] == ',')) s[0] else return failWith(error.CastFailed, "`" ++ name ++ "`: the separator is `.` or `,`, not `{s}`", .{s})) else null;
    var i: usize = 0;
    const neg = t[0] == '-';
    if (t[0] == '-' or t[0] == '+') i = 1;
    var v: i128 = 0;
    var places: i64 = 0;
    var after = false;
    var digits: usize = 0;
    for (t[i..]) |c| {
        if (point) |pt| {
            if (c == pt) {
                if (after) return bad(name, raw, "a decimal");
                after = true;
                continue;
            }
            if (c == (if (pt == ',') @as(u8, '.') else ',') and !after) continue;
        }
        if (c < '0' or c > '9') return bad(name, raw, "a decimal");
        digits += 1;
        if (digits > max_digits) return bad(name, raw, "a decimal of at most 38 digits");
        v = v * 10 + (c - '0');
        if (after) places += 1;
    }
    if (digits == 0) return bad(name, raw, "a decimal");
    if (point == null) places = scale;
    while (places > scale) : (places -= 1) {
        if (@rem(v, 10) != 0) return bad(name, raw, "a decimal with at most its scale's places");
        v = @divTrunc(v, 10);
    }
    while (places < scale) : (places += 1) v *= 10;
    return .{ .unscaled = if (neg) -v else v, .scale = @intCast(scale) };
}

pub const DecimalAt = struct {
    pub const args = .{ .str, .int, .int, .int, .opt_str };
    pub const signature = "(rec, pos, len, scale[, separator])";
    pub const may_null = true;
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkDecimal(self, c, 2, 3);
    }
    pub fn resultType(c: ast.Expr.Call) Type {
        return decimalType(c, 2, 3, sameDigits);
    }
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64, scale: i64, sep: ?[]const u8) EvalError!?Decimal {
        return digitsDecimal("decimal_at", try fieldOf("decimal_at", rec, pos, len), scale, sep);
    }
};

const Civil = struct { y: i64 = 1970, mo: u32 = 1, d: u32 = 1, h: u32 = 0, mi: u32 = 0, s: u32 = 0 };

const Part = enum { yyyy, yy, mo, dd, hh, mi, ss };

/// The next part of a date pattern at `p[i..]`, null for a literal character.
fn partAt(p: []const u8, i: usize) ?struct { part: Part, width: usize } {
    const r = p[i..];
    if (std.mem.startsWith(u8, r, "YYYY")) return .{ .part = .yyyy, .width = 4 };
    if (std.mem.startsWith(u8, r, "YY")) return .{ .part = .yy, .width = 2 };
    if (std.mem.startsWith(u8, r, "MM")) return .{ .part = .mo, .width = 2 };
    if (std.mem.startsWith(u8, r, "DD")) return .{ .part = .dd, .width = 2 };
    if (std.mem.startsWith(u8, r, "hh")) return .{ .part = .hh, .width = 2 };
    if (std.mem.startsWith(u8, r, "mm")) return .{ .part = .mi, .width = 2 };
    if (std.mem.startsWith(u8, r, "ss")) return .{ .part = .ss, .width = 2 };
    return null;
}

fn checkPattern(self: *TypeCtx, c: ast.Expr.Call, at: usize, comptime allowed: []const Part) TypeError!void {
    const p = litStr(c, at) orelse return self.err("`{s}` needs its pattern as a string literal", .{c.name});
    var i: usize = 0;
    var any = false;
    while (i < p.len) {
        const pa = partAt(p, i) orelse {
            if (std.ascii.isAlphabetic(p[i])) return self.err("`{s}` pattern `{s}`: `{c}` is not a part (YYYY YY MM DD hh mm ss) nor punctuation", .{ c.name, p, p[i] });
            i += 1;
            continue;
        };
        if (std.mem.indexOfScalar(Part, allowed, pa.part) == null) return self.err("`{s}` pattern `{s}` has a part this kind does not hold", .{ c.name, p });
        any = true;
        i += pa.width;
    }
    if (!any) return self.err("`{s}` pattern `{s}` has no date or time part", .{ c.name, p });
    if (litInt(c, 2)) |len| if (len != p.len) return self.err("`{s}` reads {d} bytes, but pattern `{s}` is {d} long", .{ c.name, len, p, p.len });
}

fn readCivil(comptime name: []const u8, raw: []const u8, p: []const u8) EvalError!Civil {
    if (raw.len != p.len) return bad(name, raw, "as long as its pattern");
    var cv = Civil{};
    var i: usize = 0;
    while (i < p.len) {
        const pa = partAt(p, i) orelse {
            if (raw[i] != p[i]) return bad(name, raw, "in its pattern");
            i += 1;
            continue;
        };
        var v: u32 = 0;
        for (raw[i .. i + pa.width]) |ch| {
            if (ch < '0' or ch > '9') return bad(name, raw, "in its pattern");
            v = v * 10 + (ch - '0');
        }
        switch (pa.part) {
            .yyyy => cv.y = v,
            .yy => cv.y = if (v >= 69) 1900 + @as(i64, v) else 2000 + @as(i64, v),
            .mo => cv.mo = v,
            .dd => cv.d = v,
            .hh => cv.h = v,
            .mi => cv.mi = v,
            .ss => cv.s = v,
        }
        i += pa.width;
    }
    if (cv.mo < 1 or cv.mo > 12 or cv.d < 1 or cv.d > time.daysInMonth(cv.y, cv.mo) or cv.h > 23 or cv.mi > 59 or cv.s > 59)
        return bad(name, raw, "a valid date or time");
    return cv;
}

fn writeCivil(comptime name: []const u8, cx: Cx, cv: Civil, p: []const u8) EvalError![]const u8 {
    var i: usize = 0;
    while (i < p.len) {
        const pa = partAt(p, i) orelse {
            try cx.buf.append(p[i]);
            i += 1;
            continue;
        };
        const v: i64 = switch (pa.part) {
            .yyyy => if (cv.y < 0 or cv.y > 9999) return failWith(error.CastFailed, "`" ++ name ++ "`: year {d} does not fit YYYY", .{cv.y}) else cv.y,
            .yy => if (cv.y < 1969 or cv.y > 2068) return failWith(error.CastFailed, "`" ++ name ++ "`: year {d} would read back as another as YY, which holds 1969 to 2068", .{cv.y}) else @mod(cv.y, 100),
            .mo => cv.mo,
            .dd => cv.d,
            .hh => cv.h,
            .mi => cv.mi,
            .ss => cv.s,
        };
        const out = try cx.extend(pa.width);
        _ = std.fmt.bufPrint(out, "{d:0>[1]}", .{ @as(u64, @intCast(v)), pa.width }) catch unreachable;
        i += pa.width;
    }
    return cx.written();
}

fn nullMarked(raw: []const u8, marker: ?[]const u8) bool {
    if (blank(raw)) return true;
    const m = marker orelse return false;
    return std.mem.eql(u8, raw, m);
}

const date_parts = &[_]Part{ .yyyy, .yy, .mo, .dd };
const time_parts = &[_]Part{ .hh, .mi, .ss };
const stamp_parts = &[_]Part{ .yyyy, .yy, .mo, .dd, .hh, .mi, .ss };

pub const DateAt = struct {
    pub const args = .{ .str, .int, .int, .str, .opt_str };
    pub const signature = "(rec, pos, len, pattern[, null marker])";
    pub const may_null = true;
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkPattern(self, c, 3, date_parts);
    }
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64, p: []const u8, marker: ?[]const u8) EvalError!?i32 {
        const raw = try fieldOf("date_at", rec, pos, len);
        if (nullMarked(raw, marker)) return null;
        const cv = try readCivil("date_at", raw, p);
        return @intCast(time.daysFromCivil(cv.y, cv.mo, cv.d));
    }
};

pub const TimeAt = struct {
    pub const args = .{ .str, .int, .int, .str, .opt_str };
    pub const result = .time;
    pub const signature = "(rec, pos, len, pattern[, null marker])";
    pub const may_null = true;
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkPattern(self, c, 3, time_parts);
    }
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64, p: []const u8, marker: ?[]const u8) EvalError!?i64 {
        const raw = try fieldOf("time_at", rec, pos, len);
        if (nullMarked(raw, marker)) return null;
        const cv = try readCivil("time_at", raw, p);
        return (@as(i64, cv.h) * 3600 + @as(i64, cv.mi) * 60 + cv.s) * 1_000_000;
    }
};

pub const TimestampAt = struct {
    pub const args = .{ .str, .int, .int, .str, .opt_str };
    pub const result = .timestamp;
    pub const signature = "(rec, pos, len, pattern[, null marker])";
    pub const may_null = true;
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkPattern(self, c, 3, stamp_parts);
    }
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64, p: []const u8, marker: ?[]const u8) EvalError!?i64 {
        const raw = try fieldOf("timestamp_at", rec, pos, len);
        if (nullMarked(raw, marker)) return null;
        const cv = try readCivil("timestamp_at", raw, p);
        return (time.daysFromCivil(cv.y, cv.mo, cv.d) * 86_400 + @as(i64, cv.h) * 3600 + @as(i64, cv.mi) * 60 + cv.s) * 1_000_000;
    }
};

/// A zoned decimal's last byte: its digit and whether it is negative, in ASCII
/// overpunch (`{A`…`I` positive, `}J`…`R` negative) or EBCDIC zones (C/F positive,
/// D negative); null when it is neither.
fn overpunch(c: u8) ?struct { digit: u8, neg: bool } {
    return switch (c) {
        '0'...'9' => .{ .digit = c - '0', .neg = false },
        '{' => .{ .digit = 0, .neg = false },
        'A'...'I' => .{ .digit = c - 'A' + 1, .neg = false },
        '}' => .{ .digit = 0, .neg = true },
        'J'...'R' => .{ .digit = c - 'J' + 1, .neg = true },
        else => if (c & 0x0F <= 9) switch (c >> 4) {
            0xC, 0xF => .{ .digit = c & 0x0F, .neg = false },
            0xD => .{ .digit = c & 0x0F, .neg = true },
            else => null,
        } else null,
    };
}

pub const ZonedAt = struct {
    pub const args = .{ .str, .int, .int, .int };
    pub const signature = "(rec, pos, len, scale)";
    pub const may_null = true;
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkDecimal(self, c, 2, 3);
    }
    pub fn resultType(c: ast.Expr.Call) Type {
        return decimalType(c, 2, 3, sameDigits);
    }
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64, scale: i64) EvalError!?Decimal {
        const raw = try fieldOf("zoned_at", rec, pos, len);
        if (blank(raw)) return null;
        if (raw.len > max_digits) return bad("zoned_at", raw, "a zoned decimal of at most 38 digits");
        var v: i128 = 0;
        for (raw[0 .. raw.len - 1]) |c| {
            const d: u8 = if (c >= '0' and c <= '9') c - '0' else if (c >= 0xF0 and c <= 0xF9) c - 0xF0 else return bad("zoned_at", raw, "a zoned decimal");
            v = v * 10 + d;
        }
        const last = overpunch(raw[raw.len - 1]) orelse return bad("zoned_at", raw, "a zoned decimal");
        v = v * 10 + last.digit;
        return .{ .unscaled = if (last.neg) -v else v, .scale = @intCast(scale) };
    }
};

fn packedDigits(len: i64) i64 {
    return len * 2 - 1;
}

pub const PackedAt = struct {
    pub const args = .{ .str, .int, .int, .int };
    pub const signature = "(rec, pos, len, scale)";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try wantLitInt(self, c, 2, "length", 1, (max_digits + 1) / 2);
        try wantLitInt(self, c, 3, "scale", 0, max_digits);
    }
    pub fn resultType(c: ast.Expr.Call) Type {
        return decimalType(c, 2, 3, packedDigits);
    }
    pub fn apply(_: Cx, rec: []const u8, pos: i64, len: i64, scale: i64) EvalError!Decimal {
        const raw = try fieldOf("packed_at", rec, pos, len);
        if (raw.len == 0 or raw.len * 2 - 1 > max_digits) return bad("packed_at", raw, "a packed decimal of at most 38 digits");
        var v: i128 = 0;
        for (raw, 0..) |c, i| {
            const hi = c >> 4;
            const lo = c & 0x0F;
            if (hi > 9) return bad("packed_at", raw, "a packed decimal");
            v = v * 10 + hi;
            if (i + 1 < raw.len) {
                if (lo > 9) return bad("packed_at", raw, "a packed decimal");
                v = v * 10 + lo;
            } else {
                const neg = switch (lo) {
                    0xC, 0xF => false,
                    0xD => true,
                    else => return bad("packed_at", raw, "a packed decimal (its sign nibble is not C, D or F)"),
                };
                if (neg) v = -v;
            }
        }
        return .{ .unscaled = v, .scale = @intCast(scale) };
    }
};

/// A binary integer of type `T` in `endian` byte order at `pos`.
pub fn BinIntAt(comptime T: type, comptime endian: std.builtin.Endian) type {
    return struct {
        pub const args = .{ .str, .int };
        pub const signature = "(rec, pos)";
        pub fn apply(_: Cx, rec: []const u8, pos: i64) EvalError!i64 {
            const n = @sizeOf(T);
            const raw = try fieldOf("binary integer", rec, pos, n);
            const x = std.mem.readInt(T, raw[0..n], endian);
            return std.math.cast(i64, x) orelse failWith(error.CastFailed, "binary integer {d} does not fit an INT", .{x});
        }
    };
}

/// A binary IEEE float of type `T` in `endian` byte order at `pos`.
pub fn BinFloatAt(comptime T: type, comptime endian: std.builtin.Endian) type {
    return struct {
        pub const args = .{ .str, .int };
        pub const signature = "(rec, pos)";
        pub fn apply(_: Cx, rec: []const u8, pos: i64) EvalError!f64 {
            const n = @sizeOf(T);
            const raw = try fieldOf("binary float", rec, pos, n);
            const I = std.meta.Int(.unsigned, n * 8);
            return @floatCast(@as(T, @bitCast(std.mem.readInt(I, raw[0..n], endian))));
        }
    };
}

fn spaces(cx: Cx, width: i64, sp: u8) EvalError![]const u8 {
    if (width < 0) return failWith(error.CastFailed, "a field width cannot be negative ({d})", .{width});
    @memset(try cx.extend(@intCast(width)), sp);
    return cx.written();
}

fn tooWide(comptime name: []const u8, needed: usize, width: i64) EvalError {
    return failWith(error.CastFailed, "`" ++ name ++ "`: the value needs {d} bytes, more than its field's {d}", .{ needed, width });
}

pub const FmtText = struct {
    pub const args = .{ .str, .int, .opt_str };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(text, width[, charset])";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try wantCharset(self, c, 2);
    }
    pub fn apply(cx: Cx, v: ?[]const u8, width: i64, cs: ?[]const u8) EvalError![]const u8 {
        const which = try charsetArg("fmt_text", cs);
        const t = v orelse return spaces(cx, width, which.space());
        const b = charset.encode(cx.arena, which, t) catch |e| switch (e) {
            error.Unencodable => return bad("fmt_text", t, "text the charset holds"),
            else => return error.OutOfMemory,
        };
        if (width < 0 or b.len > width) return tooWide("fmt_text", b.len, width);
        try cx.buf.appendSlice(b);
        @memset(try cx.extend(@as(usize, @intCast(width)) - b.len), which.space());
        return cx.written();
    }
};

/// `digits` (no sign) zero-padded to `width`, a `-` first when negative.
fn padDigits(comptime name: []const u8, cx: Cx, digits: []const u8, neg: bool, width: i64) EvalError![]const u8 {
    const need = digits.len + @intFromBool(neg);
    if (width < 0 or need > width) return tooWide(name, need, width);
    if (neg) try cx.buf.append('-');
    @memset(try cx.extend(@as(usize, @intCast(width)) - need), '0');
    try cx.buf.appendSlice(digits);
    return cx.written();
}

pub const FmtInt = struct {
    pub const args = .{ .int, .int };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(int, width)";
    pub fn apply(cx: Cx, v: ?i64, width: i64) EvalError![]const u8 {
        const x = v orelse return spaces(cx, width, ' ');
        var buf: [24]u8 = undefined;
        const digits = std.fmt.bufPrint(&buf, "{d}", .{@abs(x)}) catch unreachable;
        return padDigits("fmt_int", cx, digits, x < 0, width);
    }
};

/// `d` at `scale` places exactly: more places with nonzero digits there is an error.
fn exactAt(comptime name: []const u8, d: Decimal, scale: i64) EvalError!i128 {
    if (scale < 0 or scale > max_digits) return failWith(error.CastFailed, "`" ++ name ++ "`: scale {d} is out of range", .{scale});
    var v = d.unscaled;
    var s: i64 = d.scale;
    while (s > scale) : (s -= 1) {
        if (@rem(v, 10) != 0) return failWith(error.CastFailed, "`" ++ name ++ "`: the value has more than {d} decimal places; round it first", .{scale});
        v = @divTrunc(v, 10);
    }
    while (s < scale) : (s += 1) v = std.math.mul(i128, v, 10) catch return failWith(error.CastFailed, "`" ++ name ++ "`: the value is too large", .{});
    return v;
}

pub const FmtDecimal = struct {
    pub const args = .{ .dec, .int, .int, .opt_str };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(decimal, width, scale[, separator])";
    pub fn apply(cx: Cx, v: ?Decimal, width: i64, scale: i64, sep: ?[]const u8) EvalError![]const u8 {
        const d = v orelse return spaces(cx, width, ' ');
        const u = try exactAt("fmt_decimal", d, scale);
        var buf: [48]u8 = undefined;
        var digits = std.fmt.bufPrint(&buf, "{d}", .{@abs(u)}) catch unreachable;
        if (sep) |s| {
            if (s.len != 1 or (s[0] != '.' and s[0] != ',')) return failWith(error.CastFailed, "`fmt_decimal`: the separator is `.` or `,`, not `{s}`", .{s});
            const places: usize = @intCast(scale);
            var whole: [50]u8 = undefined;
            const padded = if (digits.len <= places) blk: {
                @memset(whole[0 .. places + 1 - digits.len], '0');
                @memcpy(whole[places + 1 - digits.len .. places + 1], digits);
                break :blk whole[0 .. places + 1];
            } else digits;
            var withsep: [52]u8 = undefined;
            const ip = padded.len - places;
            @memcpy(withsep[0..ip], padded[0..ip]);
            withsep[ip] = s[0];
            @memcpy(withsep[ip + 1 .. ip + 1 + places], padded[ip..]);
            digits = withsep[0 .. padded.len + 1];
            return padDigits("fmt_decimal", cx, if (places == 0) digits[0 .. digits.len - 1] else digits, u < 0, width);
        }
        return padDigits("fmt_decimal", cx, digits, u < 0, width);
    }
};

fn civilOfDays(days: i64) Civil {
    const c = time.civilFromDays(days);
    return .{ .y = c.y, .mo = c.m, .d = c.d };
}

fn civilOfMicros(us: i64) Civil {
    const secs = @divFloor(us, 1_000_000);
    var cv = civilOfDays(@divFloor(secs, 86_400));
    const day: u32 = @intCast(@mod(secs, 86_400));
    cv.h = day / 3600;
    cv.mi = day % 3600 / 60;
    cv.s = day % 60;
    return cv;
}

pub const FmtDate = struct {
    pub const args = .{ .day, .str };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(date, pattern)";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkPattern(self, c, 1, date_parts);
    }
    pub fn apply(cx: Cx, v: ?i32, p: []const u8) EvalError![]const u8 {
        const d = v orelse return spaces(cx, @intCast(p.len), ' ');
        return writeCivil("fmt_date", cx, civilOfDays(d), p);
    }
};

pub const FmtTime = struct {
    pub const args = .{ .micros, .str };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(time, pattern)";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkPattern(self, c, 1, time_parts);
    }
    pub fn apply(cx: Cx, v: ?i64, p: []const u8) EvalError![]const u8 {
        const t = v orelse return spaces(cx, @intCast(p.len), ' ');
        return writeCivil("fmt_time", cx, civilOfMicros(t), p);
    }
};

pub const FmtTimestamp = struct {
    pub const args = .{ .micros, .str };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(timestamp, pattern)";
    pub fn check(self: *TypeCtx, c: ast.Expr.Call) TypeError!void {
        try checkPattern(self, c, 1, stamp_parts);
    }
    pub fn apply(cx: Cx, v: ?i64, p: []const u8) EvalError![]const u8 {
        const t = v orelse return spaces(cx, @intCast(p.len), ' ');
        return writeCivil("fmt_timestamp", cx, civilOfMicros(t), p);
    }
};

pub const FmtZoned = struct {
    pub const args = .{ .dec, .int, .int };
    pub const result = .bytes;
    pub const null_first = true;
    pub const signature = "(decimal, width, scale)";
    pub fn apply(cx: Cx, v: ?Decimal, width: i64, scale: i64) EvalError![]const u8 {
        const d = v orelse return spaces(cx, width, ' ');
        const u = try exactAt("fmt_zoned", d, scale);
        var buf: [48]u8 = undefined;
        const digits = std.fmt.bufPrint(&buf, "{d}", .{@abs(u)}) catch unreachable;
        if (width < 1 or digits.len > width) return tooWide("fmt_zoned", digits.len, width);
        const w: usize = @intCast(width);
        const out = try cx.extend(w);
        @memset(out, '0');
        @memcpy(out[w - digits.len ..], digits);
        const last = out[w - 1] - '0';
        out[w - 1] = if (u < 0) (if (last == 0) '}' else 'J' + last - 1) else (if (last == 0) '{' else 'A' + last - 1);
        return cx.written();
    }
};

pub const FmtPacked = struct {
    pub const args = .{ .dec, .int, .int };
    pub const result = .bytes;
    pub const signature = "(decimal, width, scale)";
    pub fn apply(cx: Cx, d: Decimal, width: i64, scale: i64) EvalError![]const u8 {
        const u = try exactAt("fmt_packed", d, scale);
        if (width < 1 or width * 2 - 1 > max_digits) return failWith(error.CastFailed, "`fmt_packed`: a width of {d} bytes is out of range", .{width});
        const w: usize = @intCast(width);
        var buf: [48]u8 = undefined;
        const digits = std.fmt.bufPrint(&buf, "{d}", .{@abs(u)}) catch unreachable;
        if (digits.len > w * 2 - 1) return tooWide("fmt_packed", (digits.len + 2) / 2, width);
        var nib: [80]u8 = undefined;
        const nd = w * 2 - 1;
        @memset(nib[0..nd], 0);
        for (digits, 0..) |ch, i| nib[nd - digits.len + i] = ch - '0';
        nib[nd] = if (u < 0) 0xD else 0xC;
        const out = try cx.extend(w);
        for (out, 0..) |*o, i| o.* = (nib[i * 2] << 4) | nib[i * 2 + 1];
        return cx.written();
    }
};

/// A binary integer of type `T` in `endian` byte order; out of `T`'s range is an error.
pub fn FmtBinInt(comptime T: type, comptime endian: std.builtin.Endian) type {
    return struct {
        pub const args = .{.int};
        pub const result = .bytes;
        pub const signature = "(int)";
        pub fn apply(cx: Cx, v: i64) EvalError![]const u8 {
            const x = std.math.cast(T, v) orelse return failWith(error.CastFailed, "{d} does not fit a {s}", .{ v, @typeName(T) });
            std.mem.writeInt(T, (try cx.extend(@sizeOf(T)))[0..@sizeOf(T)], x, endian);
            return cx.written();
        }
    };
}

/// A binary IEEE float of type `T` in `endian` byte order.
pub fn FmtBinFloat(comptime T: type, comptime endian: std.builtin.Endian) type {
    return struct {
        pub const args = .{.num};
        pub const result = .bytes;
        pub const signature = "(number)";
        pub fn apply(cx: Cx, v: f64) EvalError![]const u8 {
            const n = @sizeOf(T);
            const I = std.meta.Int(.unsigned, n * 8);
            std.mem.writeInt(I, (try cx.extend(n))[0..n], @bitCast(@as(T, @floatCast(v))), endian);
            return cx.written();
        }
    };
}

/// A file opened for `read_bytes` or `file_size`, or the error naming it.
fn openFile(comptime name: []const u8, arena: std.mem.Allocator, path: []const u8) EvalError!bytes.Bytes {
    return bytes.Bytes.open(arena, path) catch |e| switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        else => failWith(error.CastFailed, "`" ++ name ++ "`: cannot open `{s}` ({s})", .{ path, @errorName(e) }),
    };
}

fn rangeOf(arena: std.mem.Allocator, src: bytes.Bytes, path: []const u8, off: i64, len: i64) EvalError![]const u8 {
    if (off < 0 or len < 0) return failWith(error.CastFailed, "`read_bytes`: offset {d} and length {d} cannot be negative", .{ off, len });
    return src.range(arena, @intCast(off), @intCast(len)) catch |e| switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.ByteRangeOutOfFile => failWith(error.CastFailed, "`read_bytes`: bytes {d}..{d} are outside `{s}`'s {d} bytes", .{ off, off + len, path, src.size() }),
        else => failWith(error.CastFailed, "`read_bytes`: reading `{s}` failed ({s})", .{ path, @errorName(e) }),
    };
}

pub const ReadBytes = struct {
    pub const args = .{ .str, .int, .int };
    pub const result = .bytes;
    pub const io = true;
    pub const signature = "(path, offset, len)";
    pub fn apply(cx: Cx, path: []const u8, off: i64, len: i64) EvalError![]const u8 {
        const src = try openFile("read_bytes", cx.arena, path);
        defer src.close();
        return rangeOf(cx.arena, src, path, off, len);
    }

    /// The kernel: a path the same for the batch is opened once for all its rows.
    pub fn vec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) vecmod.VecError!vecmod.Vec {
        const p = vecmod.asStr(try vecmod.evalVec(arena, c.args[0], batch)) orelse return error.Unsupported;
        const path = switch (p) {
            .scalar => |x| x,
            .col => return error.Unsupported,
        };
        const n = batch.len;
        const offs = (try vecmod.asNum(arena, try vecmod.evalVec(arena, c.args[1], batch), n)) orelse return error.Unsupported;
        const lens = (try vecmod.asNum(arena, try vecmod.evalVec(arena, c.args[2], batch), n)) orelse return error.Unsupported;
        if (!vecmod.isIntNum(offs) or !vecmod.isIntNum(lens)) return error.Unsupported;
        const src = openFile("read_bytes", arena, path) catch return error.Unsupported;
        defer src.close();
        var app = try column.BytesAppender.init(arena, n);
        var valid = try Bitmap.initFull(arena, n);
        var any_null = false;
        for (0..n) |i| {
            if (!vecmod.numValid(offs, i) or !vecmod.numValid(lens, i)) {
                try app.pushNull();
                valid.setValid(i, false);
                any_null = true;
                continue;
            }
            const got = rangeOf(arena, src, path, vecmod.numAt(i64, offs, i), vecmod.numAt(i64, lens, i)) catch return error.Unsupported;
            try app.values.appendSlice(got);
            try app.endRow();
        }
        return vecmod.mkCol(Type.init(.bytes).withNull(any_null), n, valid, .{ .bytes = try app.finish() });
    }
};

pub const FileSize = struct {
    pub const args = .{.str};
    pub const io = true;
    pub const signature = "(path)";
    pub fn apply(cx: Cx, path: []const u8) EvalError!i64 {
        const src = try openFile("file_size", cx.arena, path);
        defer src.close();
        return @intCast(src.size());
    }
};

const testing = std.testing;
const types = @import("../../lang/types.zig");
const evalVecNode = @import("vec.zig").evalVecNode;
const evalRow = @import("row.zig").evalRow;

fn testBatch(a: std.mem.Allocator) !Batch {
    const recs = [_]?[]const u8{ "1 0001234500150325Ana   \xc3\xa9", "2 0000000000000000      xy", null, "3-0000000150311299Bo    zz" };
    var app = try column.BytesAppender.init(a, recs.len);
    var valid = try Bitmap.initFull(a, recs.len);
    for (recs, 0..) |r, i| {
        if (r) |s| {
            try app.values.appendSlice(s);
            try app.endRow();
        } else {
            try app.pushNull();
            valid.setValid(i, false);
        }
    }
    const decs = try a.dupe(Decimal, &.{ .{ .unscaled = 12345, .scale = 2 }, .{ .unscaled = -5, .scale = 2 }, .{ .unscaled = 0, .scale = 2 }, .{ .unscaled = 100, .scale = 2 } });
    var dvalid = try Bitmap.initFull(a, 4);
    dvalid.setValid(2, false);
    const days = try a.dupe(i32, &.{ 20162, 0, 19000, 10957 });
    const cols = try a.dupe(column.Column, &.{
        .{ .ty = types.Type.init(.bytes).asNullable(), .len = 4, .validity = valid, .data = .{ .bytes = try app.finish() } },
        .{ .ty = types.Type.decimal(10, 2).asNullable(), .len = 4, .validity = dvalid, .data = .{ .dec = decs } },
        .{ .ty = types.Type.init(.date), .len = 4, .validity = try Bitmap.initFull(a, 4), .data = .{ .i32 = days } },
    });
    const schema = try a.create(types.Schema);
    schema.* = .{ .fields = try a.dupe(types.Schema.Field, &.{
        .{ .name = "r", .ty = types.Type.init(.bytes).asNullable() },
        .{ .name = "d", .ty = types.Type.decimal(10, 2).asNullable() },
        .{ .name = "dt", .ty = types.Type.init(.date) },
    }) };
    return .{ .schema = schema, .columns = cols, .len = 4 };
}

test "record functions run vectorized and agree with the row form" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const batch = try testBatch(a);
    const parser = @import("../../lang/sql_parser.zig");
    const exprs = [_][]const u8{
        "int_at(r, 0, 1)",                "decimal_at(r, 2, 10, 2)",  "date_at(r, 12, 6, 'DDMMYY', '000000')",
        "text_at(r, 18, 6)",              "byte_len(r)",              "byte_at(r, 1)",
        "hex(byte_slice(r, 0, 2))",       "byte_find(r, 'z')",        "decode(byte_slice(r, 24, 2), 'latin1')",
        "fmt_decimal(d, 8, 2)",           "fmt_date(dt, 'YYYYMMDD')", "fmt_int(byte_len(r), 4)",
        "fmt_text(text_at(r, 18, 6), 8)", "u16le_at(r, 2)",           "hex(fmt_u32be(byte_len(r)))",
        "zoned_at('0000012{', 0, 8, 2)",  "hex(fmt_packed(d, 4, 2))", "fmt_zoned(d, 6, 2)",
        "r || x'0D0A'",
    };
    for (exprs) |src| {
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const e = try parser.parseExprStr(a, src, &diag);
        const v = evalVecNode(a, e, batch) catch |err| {
            std.debug.print("{s}: no kernel ({s})\n", .{ src, @errorName(err) });
            return err;
        };
        const col = switch (v) {
            .col => |c| c,
            .scalar => continue,
        };
        for (0..batch.len) |i| {
            const row = try evalRow(a, e, batch, i);
            const got = col.getValue(i);
            if (row.isNull() or got.isNull()) {
                try testing.expect(row.isNull() and got.isNull());
                continue;
            }
            const rs = try @import("format.zig").valueToString(a, row);
            const gs = try @import("format.zig").valueToString(a, got);
            testing.expectEqualStrings(rs, gs) catch |err| {
                std.debug.print("{s} row {d}\n", .{ src, i });
                return err;
            };
        }
    }
}

test "decoders read fields and encoders write them back" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = @import("testing_util.zig").evalLit;
    try testing.expectEqual(@as(i64, 1234), (try lit(a, "int_at('  001234', 0, 8)")).int);
    try testing.expectEqual(@as(i64, -7), (try lit(a, "int_at('-7', 0, 2)")).int);
    try testing.expect((try lit(a, "int_at('    ', 0, 4)")).isNull());
    try testing.expectEqual(@as(i128, 123450), (try lit(a, "decimal_at('1.234,50', 0, 8, 2, ',')")).decimal.unscaled);
    try testing.expectEqual(@as(i32, 10957), (try lit(a, "date_at('010100', 0, 6, 'DDMMYY')")).date);
    try testing.expectEqual(@as(i32, -1), (try lit(a, "date_at('311269', 0, 6, 'DDMMYY')")).date);
    try testing.expectEqual(@as(i32, 36159), (try lit(a, "date_at('311268', 0, 6, 'DDMMYY')")).date);
    try testing.expectEqual(@as(i128, -12340), (try lit(a, "zoned_at('0001234}', 0, 8, 2)")).decimal.unscaled);
    try testing.expectEqual(@as(i128, -1234), (try lit(a, "packed_at(x'01234D', 0, 3, 0)")).decimal.unscaled);
    try testing.expectEqualStrings("0001234}", (try lit(a, "fmt_zoned(CAST(-123.40 AS DECIMAL(8,2)), 8, 2)")).bytes);
    try testing.expectEqualStrings("\x01\x23\x4D", (try lit(a, "fmt_packed(-1234, 3, 0)")).bytes);
    try testing.expectEqualStrings("-0042", (try lit(a, "fmt_int(-42, 5)")).bytes);
    try testing.expectEqualStrings("     ", (try lit(a, "fmt_int(NULL, 5)")).bytes);
    try testing.expectEqualStrings("31/12/1999 23:59:58", (try lit(a, "fmt_timestamp(CAST('1999-12-31 23:59:58' AS TIMESTAMP), 'DD/MM/YYYY hh:mm:ss')")).bytes);
    try testing.expectEqualStrings("\x00\x00\x01\x02", (try lit(a, "fmt_u32be(258)")).bytes);
    try testing.expectEqual(@as(i64, 258), (try lit(a, "u32be_at(fmt_u32be(258), 0)")).int);
    try testing.expectEqual(@as(f64, 1.5), (try lit(a, "f64le_at(fmt_f64le(1.5), 0)")).float);
    try testing.expectError(error.CastFailed, lit(a, "fmt_u8(256)"));
    try testing.expectError(error.CastFailed, lit(a, "int_at('1 2', 0, 3)"));
}
