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
const civil = @import("../civil.zig");
const charset = @import("../../format/charset.zig");
const hex = @import("../../format/hex.zig");
const bytes = @import("../../format/bytes.zig");
const vecmod = @import("vec.zig");
const column = @import("../column.zig");
const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;

const decimal = @import("../decimal.zig");
const max_digits = decimal.max_digits;

fn fieldOf(comptime name: []const u8, rec: []const u8, pos: i64, len: i64) EvalError![]const u8 {
    if (pos < 0 or len < 0 or pos > rec.len or len > @as(i64, @intCast(rec.len)) - pos)
        return failWith(error.CastFailed, "`" ++ name ++ "`: {d} bytes at {d} are outside the record's {d} bytes", .{ len, pos, rec.len });
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
        _ = hex.encode(try cx.extend(b.len * 2), b, .upper);
        return cx.written();
    }
};

pub const Unhex = struct {
    pub const args = .{.str};
    pub const result = .bytes;
    pub const signature = "(hex text)";
    pub fn apply(cx: Cx, t: []const u8) EvalError![]const u8 {
        if (t.len % 2 != 0) return bad("unhex", t, "an even number of hex digits");
        _ = hex.decode(try cx.extend(t.len / 2), t) catch return bad("unhex", t, "hex digits");
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
    return decimal.fieldDecimal(t, point, scale) catch |e| switch (e) {
        error.NotDecimal => bad(name, raw, "a decimal"),
        error.TooManyDigits => bad(name, raw, "a decimal of at most 38 digits"),
        error.TooManyPlaces => bad(name, raw, "a decimal with at most its scale's places"),
    };
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

const Civil = struct { days: i64, clock: i64 };

/// The date and time of day `raw` spells in pattern `p`, a part it leaves out
/// taken from 1970-01-01 00:00:00.
fn readCivil(comptime name: []const u8, raw: []const u8, p: []const u8) EvalError!Civil {
    if (raw.len != p.len) return bad(name, raw, "as long as its pattern");
    var cv = civil.Fields{ .y = 1970, .m = 1, .d = 1 };
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
            .yy => cv.y = civil.pivotYY(v),
            .mo => cv.m = v,
            .dd => cv.d = v,
            .hh => cv.h = v,
            .mi => cv.mi = v,
            .ss => cv.s = v,
        }
        i += pa.width;
    }
    const days = civil.civilDays(cv.y, cv.m, cv.d) orelse return bad(name, raw, "a valid date or time");
    const clock = civil.clockMicros(cv.h, cv.mi, cv.s, 0) orelse return bad(name, raw, "a valid date or time");
    return .{ .days = days, .clock = clock };
}

fn writeCivil(comptime name: []const u8, cx: Cx, cv: civil.Fields, p: []const u8) EvalError![]const u8 {
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
            .mo => cv.m,
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
        return @intCast((try readCivil("date_at", raw, p)).days);
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
        return (try readCivil("time_at", raw, p)).clock;
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
        return cv.days * civil.us_per_day + cv.clock;
    }
};

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
        const v = decimal.zonedDecimal(raw) catch |e| return switch (e) {
            error.TooManyDigits => bad("zoned_at", raw, "a zoned decimal of at most 38 digits"),
            error.NotZoned => bad("zoned_at", raw, "a zoned decimal"),
        };
        return .{ .unscaled = v, .scale = @intCast(scale) };
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
        const v = decimal.packedDecimal(raw) catch |e| return switch (e) {
            error.TooManyDigits => bad("packed_at", raw, "a packed decimal of at most 38 digits"),
            error.NotPacked => bad("packed_at", raw, "a packed decimal"),
            error.BadSign => bad("packed_at", raw, "a packed decimal (its sign nibble is not C, D or F)"),
        };
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
    return decimal.exactAtScale(d, scale) catch |e| switch (e) {
        error.MorePlaces => failWith(error.CastFailed, "`" ++ name ++ "`: the value has more than {d} decimal places; round it first", .{scale}),
        error.TooLarge => failWith(error.CastFailed, "`" ++ name ++ "`: the value is too large", .{}),
    };
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
        const digits = std.fmt.bufPrint(&buf, "{d}", .{@abs(u)}) catch unreachable;
        if (sep) |s| {
            if (s.len != 1 or (s[0] != '.' and s[0] != ',')) return failWith(error.CastFailed, "`fmt_decimal`: the separator is `.` or `,`, not `{s}`", .{s});
            var withsep: [52]u8 = undefined;
            return padDigits("fmt_decimal", cx, decimal.withPoint(&withsep, digits, @intCast(scale), s[0]), u < 0, width);
        }
        return padDigits("fmt_decimal", cx, digits, u < 0, width);
    }
};

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
        return writeCivil("fmt_date", cx, civil.fieldsOfDays(d), p);
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
        return writeCivil("fmt_time", cx, civil.fieldsOfMicros(t), p);
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
        return writeCivil("fmt_timestamp", cx, civil.fieldsOfMicros(t), p);
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
        decimal.zonedInto(try cx.extend(w), digits, u < 0);
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
        decimal.packedInto(try cx.extend(w), digits, u < 0);
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
        .{ .ty = types.Type.decimal(10, 2).asNullable(), .len = 4, .validity = dvalid, .data = column.decimalData(decs) },
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
    try testing.expectError(error.CastFailed, lit(a, "byte_slice('abc', 1, 9223372036854775807)"));
    try testing.expectError(error.CastFailed, lit(a, "text_at('abc', 9223372036854775807, 1)"));
    try testing.expectError(error.CastFailed, lit(a, "byte_slice('abc', 2, 2)"));
    try testing.expectEqualStrings("c", (try lit(a, "byte_slice('abc', 2, 1)")).bytes);
}

test "record functions check their literal lengths, scales, charsets and patterns" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../../lang/sql_parser.zig");
    const schema = types.Schema{ .fields = &.{
        .{ .name = "r", .ty = types.Type.init(.bytes) },
        .{ .name = "s", .ty = types.Type.init(.string) },
        .{ .name = "n", .ty = types.Type.init(.int) },
        .{ .name = "dt", .ty = types.Type.init(.date) },
        .{ .name = "t", .ty = types.Type.init(.time) },
        .{ .name = "ts", .ty = types.Type.init(.timestamp) },
    } };
    const cases = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "decimal_at(r, 0, n, 2)", .msg = "`decimal_at` needs its length as an integer literal" },
        .{ .src = "decimal_at(r, 0, 41, 2)", .msg = "`decimal_at` length must be 1 to 40, not 41" },
        .{ .src = "decimal_at(r, 0, 5, 39)", .msg = "`decimal_at` scale must be 0 to 38, not 39" },
        .{ .src = "zoned_at(r, 0, 5, n)", .msg = "`zoned_at` needs its scale as an integer literal" },
        .{ .src = "packed_at(r, 0, 20, 0)", .msg = "`packed_at` length must be 1 to 19, not 20" },
        .{ .src = "packed_at(r, 0, 4, 40)", .msg = "`packed_at` scale must be 0 to 38, not 40" },
        .{ .src = "encode(s, 'klingon')", .msg = "`encode`: unknown charset `klingon` (" ++ charset.names ++ ")" },
        .{ .src = "text_at(r, 0, 3, 'klingon')", .msg = "`text_at`: unknown charset `klingon` (" ++ charset.names ++ ")" },
        .{ .src = "fmt_text(s, 3, 'klingon')", .msg = "`fmt_text`: unknown charset `klingon` (" ++ charset.names ++ ")" },
        .{ .src = "date_at(r, 0, 6, s)", .msg = "`date_at` needs its pattern as a string literal" },
        .{ .src = "date_at(r, 0, 6, 'DDMMXX')", .msg = "`date_at` pattern `DDMMXX`: `X` is not a part (YYYY YY MM DD hh mm ss) nor punctuation" },
        .{ .src = "date_at(r, 0, 6, 'DDMMhh')", .msg = "`date_at` pattern `DDMMhh` has a part this kind does not hold" },
        .{ .src = "date_at(r, 0, 5, 'DDMMYY')", .msg = "`date_at` reads 5 bytes, but pattern `DDMMYY` is 6 long" },
        .{ .src = "time_at(r, 0, 2, '--')", .msg = "`time_at` pattern `--` has no date or time part" },
        .{ .src = "time_at(r, 0, 4, 'YYYY')", .msg = "`time_at` pattern `YYYY` has a part this kind does not hold" },
        .{ .src = "timestamp_at(r, 0, 2, 'zz')", .msg = "`timestamp_at` pattern `zz`: `z` is not a part (YYYY YY MM DD hh mm ss) nor punctuation" },
        .{ .src = "fmt_date(dt, 'hhmm')", .msg = "`fmt_date` pattern `hhmm` has a part this kind does not hold" },
        .{ .src = "fmt_time(t, 'MM')", .msg = "`fmt_time` pattern `MM` has a part this kind does not hold" },
        .{ .src = "fmt_timestamp(ts, '/')", .msg = "`fmt_timestamp` pattern `/` has no date or time part" },
    };
    for (cases) |cs| {
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        var ctx = TypeCtx{ .schema = schema, .arena = a };
        try testing.expectError(error.TypeError, ctx.typeOf(try parser.parseExprStr(a, cs.src, &diag)));
        testing.expectEqualStrings(cs.msg, ctx.msg) catch |e| {
            std.debug.print("in `{s}`\n", .{cs.src});
            return e;
        };
    }
}

test "record decoders: time and timestamp fields, overpunch and EBCDIC zones, refusals by message" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = @import("testing_util.zig").evalLit;
    const takeFailure = @import("support.zig").takeFailure;

    try testing.expectEqual(@as(i64, (23 * 3600 + 59 * 60) * 1_000_000), (try lit(a, "time_at('2359', 0, 4, 'hhmm')")).time);
    try testing.expectEqual(@as(i64, (19724 * 86_400 + 3 * 3600 + 4 * 60 + 5) * 1_000_000), (try lit(a, "timestamp_at('20240102 030405', 0, 15, 'YYYYMMDD hhmmss')")).timestamp);
    try testing.expect((try lit(a, "time_at('    ', 0, 4, 'hhmm')")).isNull());
    try testing.expect((try lit(a, "timestamp_at('00000000', 0, 8, 'YYYYMMDD', '00000000')")).isNull());
    try testing.expect((try lit(a, "zoned_at('   ', 0, 3, 0)")).isNull());

    const decs = [_]struct { src: []const u8, unscaled: i128 }{
        .{ .src = "zoned_at('12A', 0, 3, 0)", .unscaled = 121 },
        .{ .src = "zoned_at('12R', 0, 3, 0)", .unscaled = -129 },
        .{ .src = "zoned_at(x'F1F2C3', 0, 3, 0)", .unscaled = 123 },
        .{ .src = "zoned_at(x'F1F2F3', 0, 3, 0)", .unscaled = 123 },
        .{ .src = "zoned_at(x'F1F2D3', 0, 3, 0)", .unscaled = -123 },
        .{ .src = "decimal_at('1.230', 0, 5, 2, '.')", .unscaled = 123 },
        .{ .src = "decimal_at('+1.5', 0, 4, 3, '.')", .unscaled = 1500 },
    };
    for (decs) |d| {
        errdefer std.debug.print("in `{s}`\n", .{d.src});
        try testing.expectEqual(d.unscaled, (try lit(a, d.src)).decimal.unscaled);
    }

    const refused = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "zoned_at(x'F1F2E3', 0, 3, 0)", .msg = "`zoned_at`: `\\xF1\\xF2\\xE3` is not a zoned decimal" },
        .{ .src = "zoned_at('1x3', 0, 3, 0)", .msg = "`zoned_at`: `1x3` is not a zoned decimal" },
        .{ .src = "decimal_at('1.235', 0, 5, 2, '.')", .msg = "`decimal_at`: `1.235` is not a decimal with at most its scale's places" },
        .{ .src = "decimal_at('1.2.3', 0, 5, 2, '.')", .msg = "`decimal_at`: `1.2.3` is not a decimal" },
        .{ .src = "decimal_at('12', 0, 2, 2, ';')", .msg = "`decimal_at`: the separator is `.` or `,`, not `;`" },
        .{ .src = "decimal_at('12', 0, 2, 39)", .msg = "`decimal_at`: scale 39 is out of range" },
        .{ .src = "decimal_at('-', 0, 1, 0)", .msg = "`decimal_at`: `-` is not a decimal" },
        .{ .src = "date_at('31-02-24', 0, 8, 'DD-MM-YY')", .msg = "`date_at`: `31-02-24` is not a valid date or time" },
        .{ .src = "date_at('31/12/24', 0, 8, 'DD-MM-YY')", .msg = "`date_at`: `31/12/24` is not in its pattern" },
        .{ .src = "date_at('3a-12-24', 0, 8, 'DD-MM-YY')", .msg = "`date_at`: `3a-12-24` is not in its pattern" },
        .{ .src = "date_at('311224', 0, 6, 'DDMMYYYY')", .msg = "`date_at`: `311224` is not as long as its pattern" },
        .{ .src = "time_at('2460', 0, 4, 'hhmm')", .msg = "`time_at`: `2460` is not a valid date or time" },
        .{ .src = "encode('\u{20AC}', 'latin1')", .msg = "`encode`: `\\xE2\\x82\\xAC` is not text the charset holds" },
        .{ .src = "int_at('+', 0, 1)", .msg = "`int_at`: `+` is not an integer" },
        .{ .src = "int_at('99999999999999999999', 0, 20)", .msg = "`int_at`: `99999999999999999999` is not an integer in range" },
    };
    for (refused) |r| {
        errdefer std.debug.print("in `{s}`\n", .{r.src});
        try testing.expectError(error.CastFailed, lit(a, r.src));
        try testing.expectEqualStrings(r.msg, takeFailure(error.CastFailed).?);
    }
}

test "record encoders: separators, padding, NULL as spaces, and every refusal by message" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = @import("testing_util.zig").evalLit;
    const takeFailure = @import("support.zig").takeFailure;

    const written = [_]struct { src: []const u8, want: []const u8 }{
        .{ .src = "fmt_decimal(CAST(1.5 AS DECIMAL(4,2)), 6, 2, ',')", .want = "001,50" },
        .{ .src = "fmt_decimal(0.05, 5, 2, '.')", .want = "00.05" },
        .{ .src = "fmt_decimal(-0.05, 6, 2, '.')", .want = "-00.05" },
        .{ .src = "fmt_decimal(15, 4, 0, '.')", .want = "0015" },
        .{ .src = "fmt_decimal(CAST(NULL AS DECIMAL(4,2)), 4, 2)", .want = "    " },
        .{ .src = "fmt_time(CAST('23:59:58' AS TIME), 'hh:mm:ss')", .want = "23:59:58" },
        .{ .src = "fmt_time(CAST(NULL AS TIME), 'hhmm')", .want = "    " },
        .{ .src = "fmt_date(CAST(NULL AS DATE), 'YYYYMMDD')", .want = "        " },
        .{ .src = "fmt_date(CAST('1969-07-20' AS DATE), 'DDMMYY')", .want = "200769" },
        .{ .src = "fmt_timestamp(CAST(NULL AS TIMESTAMP), 'YYYY')", .want = "    " },
        .{ .src = "fmt_zoned(CAST(NULL AS DECIMAL(4,2)), 3, 2)", .want = "   " },
        .{ .src = "fmt_text(NULL, 3)", .want = "   " },
        .{ .src = "fmt_text('ab', 4, 'latin1')", .want = "ab  " },
    };
    for (written) |w| {
        errdefer std.debug.print("in `{s}`\n", .{w.src});
        try testing.expectEqualStrings(w.want, (try lit(a, w.src)).bytes);
    }

    const refused = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "fmt_decimal(1.234, 6, 2)", .msg = "`fmt_decimal`: the value has more than 2 decimal places; round it first" },
        .{ .src = "fmt_decimal(1.5, 6, 39)", .msg = "`fmt_decimal`: scale 39 is out of range" },
        .{ .src = "fmt_decimal(1.5, 6, 2, ';')", .msg = "`fmt_decimal`: the separator is `.` or `,`, not `;`" },
        .{ .src = "fmt_decimal(-1.5, 4, 2, '.')", .msg = "`fmt_decimal`: the value needs 5 bytes, more than its field's 4" },
        .{ .src = "fmt_text('abcde', 3)", .msg = "`fmt_text`: the value needs 5 bytes, more than its field's 3" },
        .{ .src = "fmt_text('\u{20AC}', 3, 'latin1')", .msg = "`fmt_text`: `\\xE2\\x82\\xAC` is not text the charset holds" },
        .{ .src = "fmt_int(123, 2)", .msg = "`fmt_int`: the value needs 3 bytes, more than its field's 2" },
        .{ .src = "fmt_int(NULL, -1)", .msg = "a field width cannot be negative (-1)" },
        .{ .src = "fmt_zoned(12345, 3, 0)", .msg = "`fmt_zoned`: the value needs 5 bytes, more than its field's 3" },
        .{ .src = "fmt_packed(1, 20, 0)", .msg = "`fmt_packed`: a width of 20 bytes is out of range" },
        .{ .src = "fmt_packed(12345, 2, 0)", .msg = "`fmt_packed`: the value needs 3 bytes, more than its field's 2" },
        .{ .src = "fmt_date(CAST('2070-01-01' AS DATE), 'YYMMDD')", .msg = "`fmt_date`: year 2070 would read back as another as YY, which holds 1969 to 2068" },
    };
    for (refused) |r| {
        errdefer std.debug.print("in `{s}`\n", .{r.src});
        try testing.expectError(error.CastFailed, lit(a, r.src));
        try testing.expectEqualStrings(r.msg, takeFailure(error.CastFailed).?);
    }
}

test "read_bytes and file_size: ranges of a file, NULL offsets per row, and the errors naming the file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = @import("testing_util.zig").evalLit;
    const takeFailure = @import("support.zig").takeFailure;
    const parser = @import("../../lang/sql_parser.zig");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "f.bin", .data = "hello" });
    const path = try tmp.dir.realpathAlloc(a, "f.bin");
    const missing = try std.fs.path.join(a, &.{ std.fs.path.dirname(path).?, "nope.bin" });

    const q = struct {
        fn f(al: std.mem.Allocator, comptime fmt: []const u8, args: anytype) ![]const u8 {
            return std.fmt.allocPrint(al, fmt, args);
        }
    }.f;
    try testing.expectEqualStrings("ell", (try lit(a, try q(a, "read_bytes('{s}', 1, 3)", .{path}))).bytes);
    try testing.expectEqual(@as(i64, 5), (try lit(a, try q(a, "file_size('{s}')", .{path}))).int);

    const refused = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = try q(a, "read_bytes('{s}', 3, 5)", .{path}), .msg = try q(a, "`read_bytes`: bytes 3..8 are outside `{s}`'s 5 bytes", .{path}) },
        .{ .src = try q(a, "read_bytes('{s}', 0 - 1, 2)", .{path}), .msg = "`read_bytes`: offset -1 and length 2 cannot be negative" },
        .{ .src = try q(a, "read_bytes('{s}', 0, 1)", .{missing}), .msg = try q(a, "`read_bytes`: cannot open `{s}` (FileNotFound)", .{missing}) },
        .{ .src = try q(a, "file_size('{s}')", .{missing}), .msg = try q(a, "`file_size`: cannot open `{s}` (FileNotFound)", .{missing}) },
    };
    for (refused) |r| {
        errdefer std.debug.print("in `{s}`\n", .{r.src});
        try testing.expectError(error.CastFailed, lit(a, r.src));
        try testing.expectEqualStrings(r.msg, takeFailure(error.CastFailed).?);
    }

    const batch = try testBatch(a);
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, try q(a, "read_bytes('{s}', int_at(r, 0, 1), 1)", .{path}), &diag);
    const col = (try evalVecNode(a, e, batch)).col;
    const want = [_]?[]const u8{ "e", "l", null, "l" };
    for (want, 0..) |w, i| {
        const got = col.getValue(i);
        const row = try evalRow(a, e, batch, i);
        if (w) |s| {
            try testing.expectEqualStrings(s, got.bytes);
            try testing.expectEqualStrings(s, row.bytes);
        } else {
            try testing.expect(got.isNull());
            try testing.expect(row.isNull());
        }
    }
}

test "unhex takes hex digits only, with no sign or `_`, and hex prints every byte as two upper-case digits" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lit = @import("testing_util.zig").evalLit;
    const takeFailure = @import("support.zig").takeFailure;
    try testing.expectEqualStrings("\x0F\xAB", (try lit(a, "unhex('0fAb')")).bytes);
    try testing.expectEqualStrings("", (try lit(a, "unhex('')")).bytes);
    const refused = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "unhex('+f')", .msg = "`unhex`: `+f` is not hex digits" },
        .{ .src = "unhex('-0')", .msg = "`unhex`: `-0` is not hex digits" },
        .{ .src = "unhex('_1')", .msg = "`unhex`: `_1` is not hex digits" },
        .{ .src = "unhex('0g')", .msg = "`unhex`: `0g` is not hex digits" },
        .{ .src = "unhex('abc')", .msg = "`unhex`: `abc` is not an even number of hex digits" },
    };
    for (refused) |r| {
        errdefer std.debug.print("in `{s}`\n", .{r.src});
        try testing.expectError(error.CastFailed, lit(a, r.src));
        try testing.expectEqualStrings(r.msg, takeFailure(error.CastFailed).?);
    }
    var want: [512]u8 = undefined;
    for (0..256) |i| _ = try std.fmt.bufPrint(want[i * 2 ..][0..2], "{X:0>2}", .{@as(u8, @intCast(i))});
    const got = try lit(a, try std.fmt.allocPrint(a, "hex(unhex('{s}'))", .{&want}));
    try testing.expectEqualStrings(&want, switch (got) {
        .string, .bytes => |s| s,
        else => return error.TestUnexpectedResult,
    });
}

/// The pattern reader and the fields the encoders print, as they were before
/// `civil.Fields`, kept as the oracle the differential test checks against.
const OldCivil = struct {
    y: i64 = 1970,
    mo: u32 = 1,
    d: u32 = 1,
    h: u32 = 0,
    mi: u32 = 0,
    s: u32 = 0,

    fn read(raw: []const u8, p: []const u8) ?OldCivil {
        if (raw.len != p.len) return null;
        var cv = OldCivil{};
        var i: usize = 0;
        while (i < p.len) {
            const pa = partAt(p, i) orelse {
                if (raw[i] != p[i]) return null;
                i += 1;
                continue;
            };
            var v: u32 = 0;
            for (raw[i .. i + pa.width]) |ch| {
                if (ch < '0' or ch > '9') return null;
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
        if (cv.mo < 1 or cv.mo > 12 or cv.d < 1 or cv.d > civil.daysInMonth(cv.y, cv.mo) or cv.h > 23 or cv.mi > 59 or cv.s > 59) return null;
        return cv;
    }

    fn ofMicros(us: i64) OldCivil {
        const secs = @divFloor(us, 1_000_000);
        const c = civil.civilFromDays(@divFloor(secs, 86_400));
        const day: u32 = @intCast(@mod(secs, 86_400));
        return .{ .y = c.y, .mo = c.m, .d = c.d, .h = day / 3600, .mi = day % 3600 / 60, .s = day % 60 };
    }
};

test "date_at, time_at and timestamp_at read, and the fmt_ encoders see, what the code they replaced did" {
    var rnd = std.Random.DefaultPrng.init(0xda7e);
    const r = rnd.random();
    const patterns = [_][]const u8{ "YYYYMMDD", "DD/MM/YY", "YYYY-MM-DD hh:mm:ss", "hhmmss", "hh:mm", "YYMMDDhhmm" };
    var buf: [32]u8 = undefined;
    var accepted: usize = 0;
    for (0..100_000) |i| {
        const p = patterns[i % patterns.len];
        for (p, 0..) |ch, k| buf[k] = if (std.ascii.isAlphabetic(ch)) '0' + r.uintLessThan(u8, if (r.uintLessThan(u8, 3) == 0) 10 else 3) else ch;
        if (r.uintLessThan(u8, 50) == 0) buf[r.uintLessThan(usize, p.len)] = 'x';
        const raw = buf[0..p.len];
        errdefer std.debug.print("`{s}` in `{s}`\n", .{ raw, p });
        const old = OldCivil.read(raw, p);
        const got_date = DateAt.apply(undefined, raw, 0, @intCast(raw.len), p, null);
        const got_time = TimeAt.apply(undefined, raw, 0, @intCast(raw.len), p, null);
        const got_stamp = TimestampAt.apply(undefined, raw, 0, @intCast(raw.len), p, null);
        if (old) |cv| {
            accepted += 1;
            const days = civil.daysFromCivil(cv.y, cv.mo, cv.d);
            try testing.expectEqual(@as(?i32, @intCast(days)), try got_date);
            try testing.expectEqual(@as(?i64, (@as(i64, cv.h) * 3600 + @as(i64, cv.mi) * 60 + cv.s) * 1_000_000), try got_time);
            try testing.expectEqual(@as(?i64, (days * 86_400 + @as(i64, cv.h) * 3600 + @as(i64, cv.mi) * 60 + cv.s) * 1_000_000), try got_stamp);
        } else {
            try testing.expectError(error.CastFailed, got_date);
            try testing.expectError(error.CastFailed, got_time);
            try testing.expectError(error.CastFailed, got_stamp);
        }
    }
    try testing.expect(accepted > 10_000);
    for (0..100_000) |i| {
        const us: i64 = if (i == 0) std.math.minInt(i64) else if (i == 1) std.math.maxInt(i64) else r.int(i64) >> @intCast(i % 20);
        const o = OldCivil.ofMicros(us);
        const f = civil.fieldsOfMicros(us);
        try testing.expectEqual(o, OldCivil{ .y = f.y, .mo = f.m, .d = f.d, .h = f.h, .mi = f.mi, .s = f.s });
        const days = us >> 37;
        const od = civil.civilFromDays(days);
        const fd = civil.fieldsOfDays(days);
        try testing.expectEqual(OldCivil{ .y = od.y, .mo = od.m, .d = od.d }, OldCivil{ .y = fd.y, .mo = fd.m, .d = fd.d });
    }
}
