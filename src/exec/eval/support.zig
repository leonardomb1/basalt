//! What the evaluators share: the per-thread field and regex caches, the failure
//! note a failed cast leaves for its error message, and the order of values.

const Batch = @import("../batch.zig").Batch;
const EvalError = @import("../eval.zig").EvalError;
const Type = @import("../../lang/types.zig").Type;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const evalRow = @import("row.zig").evalRow;
const fmt_bound = @import("format.zig").fmt_bound;
const parseIsoDate = @import("time.zig").parseIsoDate;
const parseIsoTimestamp = @import("time.zig").parseIsoTimestamp;
const regex = @import("../regex.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const valueToString = @import("format.zig").valueToString;
const writeDecimal = @import("format.zig").writeDecimal;
const evalLit = @import("testing_util.zig").evalLit;
const formatTimestamp = @import("format.zig").formatTimestamp;

pub fn formatDecimal(arena: std.mem.Allocator, unscaled: i128, scale: u8) ![]const u8 {
    var buf: [fmt_bound]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeDecimal(&w, unscaled, scale) catch unreachable;
    return arena.dupe(u8, w.buffered());
}

threadlocal var field_memo: [8]usize = @splat(0);

/// Hashes length, first and last byte (names like `col36`..`col39` share the
/// first two) into the memo. A qualified name skips the memo, which is by name alone.
pub fn fieldIndex(schema: types.Schema, q: ast.QualName) ?usize {
    if (q.parts.len > 1) return schema.resolve(q.parts);
    const name = lastPart(q);
    if (name.len == 0) return schema.indexOf(name);
    const slot = (name.len *% 31 +% name[0] *% 7 +% name[name.len - 1]) & (field_memo.len - 1);
    const cached = field_memo[slot];
    if (cached < schema.fields.len and std.mem.eql(u8, schema.fields[cached].name, name)) return cached;
    const idx = schema.indexOf(name) orelse return null;
    field_memo[slot] = idx;
    return idx;
}

pub fn lastPart(q: ast.QualName) []const u8 {
    return q.parts[q.parts.len - 1];
}

pub fn isNum(v: Value) bool {
    return v == .int or v == .float or v == .decimal;
}

pub fn toF64(v: Value) f64 {
    return switch (v) {
        .int => |x| @floatFromInt(x),
        .float => |x| x,
        .decimal => |d| d.toF64(),
        else => 0,
    };
}

const RegexCache = struct {
    buf: [16 * 1024]u8 = undefined,
    src: []const u8 = &.{},
    re: regex.Regex = undefined,
    valid: bool = false,
};

threadlocal var regex_cache: RegexCache = .{};

threadlocal var fail_note: struct { err: ?anyerror = null, buf: [480]u8 = undefined, len: usize = 0 } = .{};

pub fn explain(e: anyerror, msg: []const u8) anyerror {
    const n = &fail_note;
    const k = @min(msg.len, n.buf.len);
    @memcpy(n.buf[0..k], msg[0..k]);
    n.len = k;
    n.err = e;
    return e;
}

pub fn failWith(e: EvalError, comptime fmt: []const u8, args: anytype) EvalError {
    const n = &fail_note;
    const msg = std.fmt.bufPrint(&n.buf, fmt, args) catch blk: {
        @memcpy(n.buf[n.buf.len - 3 ..], "...");
        break :blk n.buf[0..];
    };
    n.len = msg.len;
    n.err = e;
    return e;
}

/// A CAST failure naming the value and target, with the format a date or time is
/// read in, since that is what a file in another convention trips over.
pub fn castFailure(arena: std.mem.Allocator, v: Value, ty: Type) EvalError {
    const text = clip(valueToString(arena, v) catch "?");
    const want: []const u8 = switch (ty.kind) {
        .int => "an INT",
        .float => "a FLOAT",
        .bool => "a BOOL",
        .decimal => "a DECIMAL",
        .date => "a DATE (YYYY-MM-DD; strptime reads other formats)",
        .timestamp => "a TIMESTAMP (YYYY-MM-DD HH:MM:SS; strptime reads other formats)",
        .time => "a TIME (HH:MM[:SS])",
        else => @tagName(ty.kind),
    };
    if (ty.kind == .decimal)
        return failWith(error.CastFailed, "CAST: '{s}' is not a DECIMAL({d},{d})", .{ text, ty.precision, ty.scale });
    return failWith(error.CastFailed, "CAST: '{s}' is not {s}", .{ text, want });
}

pub inline fn forgetFailure() void {
    if (fail_note.err != null) fail_note.err = null;
}

pub fn takeFailure(e: anyerror) ?[]const u8 {
    const n = &fail_note;
    if (n.err == null or n.err.? != e) return null;
    n.err = null;
    return n.buf[0..n.len];
}

pub fn clip(s: []const u8) []const u8 {
    const max = 80;
    return if (s.len <= max) s else s[0..max];
}

const RegexMatch = struct { s: []const u8, span: ?[2]usize, caps: regex.Captures };

pub fn regexpFind(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!?RegexMatch {
    const v = try evalRow(arena, c.args[0], batch, row);
    if (v.isNull()) return null;
    const pat = try evalRow(arena, c.args[1], batch, row);
    if (pat.isNull()) return null;
    const re = cachedRegex(try valueToString(arena, pat)) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.BadPattern => return error.CastFailed,
        error.PatternTooComplex => return error.PatternTooComplex,
    };
    var m = RegexMatch{ .s = try valueToString(arena, v), .span = null, .caps = undefined };
    m.span = re.find(m.s, 0, &m.caps) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.BadPattern => return error.CastFailed,
        error.PatternTooComplex => return error.PatternTooComplex,
    };
    return m;
}

/// The entry is invalidated before compiling, so a failed compile never leaves the
/// old pattern under the new key, and `src` is copied into the entry's own buffer.
pub fn cachedRegex(pattern: []const u8) regex.Error!regex.Regex {
    const c = &regex_cache;
    if (c.valid and std.mem.eql(u8, c.src, pattern)) return c.re;
    var fba = std.heap.FixedBufferAllocator.init(&c.buf);
    c.valid = false;
    c.re = try regex.Regex.compile(fba.allocator(), pattern);
    c.src = fba.allocator().dupe(u8, pattern) catch return error.OutOfMemory;
    c.valid = true;
    return c.re;
}

pub fn orderF64(x: f64, y: f64) std.math.Order {
    const xn = std.math.isNan(x);
    const yn = std.math.isNan(y);
    if (xn or yn) {
        if (xn and yn) return .eq;
        return if (xn) .gt else .lt;
    }
    return std.math.order(x, y);
}

/// Bytes compare by content: a null answer here once made MIN/MAX over a bytes
/// column keep the first value forever.
pub fn compareValues(a: Value, b: Value) ?std.math.Order {
    if (isNum(a) and isNum(b)) {
        if (a == .int and b == .int) return std.math.order(a.int, b.int);
        return orderF64(toF64(a), toF64(b));
    }
    if (a == .string and b == .string) return std.mem.order(u8, a.string, b.string);
    if (a == .bytes and b == .bytes) return std.mem.order(u8, a.bytes, b.bytes);
    if (a == .bool and b == .bool) return std.math.order(@intFromBool(a.bool), @intFromBool(b.bool));
    if (a == .timestamp and b == .timestamp) return std.math.order(a.timestamp, b.timestamp);
    if (a == .date and b == .date) return std.math.order(a.date, b.date);
    if (a == .date and b == .string) return std.math.order(@as(i64, a.date), parseIsoDate(b.string) orelse return null);
    if (a == .string and b == .date) return std.math.order(parseIsoDate(a.string) orelse return null, @as(i64, b.date));
    if (a == .timestamp and b == .string) return std.math.order(a.timestamp, parseIsoTimestamp(b.string) orelse return null);
    if (a == .string and b == .timestamp) return std.math.order(parseIsoTimestamp(a.string) orelse return null, b.timestamp);
    if (a == .time and b == .time) return std.math.order(a.time, b.time);
    return null;
}

pub fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}

pub fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n");
}

/// `from_hex`: optional `0x`, either case, 16 digits of range so `to_hex` of a
/// negative round-trips. Junk or overflow is an error, never a null.
pub fn parseHexI64(s: []const u8) EvalError!i64 {
    var t = trim(s);
    if (t.len >= 2 and t[0] == '0' and (t[1] == 'x' or t[1] == 'X')) t = t[2..];
    if (t.len == 0) return error.CastFailed;
    for (t) |ch| if (!std.ascii.isHex(ch)) return error.CastFailed;
    const u = std.fmt.parseUnsigned(u64, t, 16) catch return error.CastFailed;
    return @bitCast(u);
}

pub fn toI64(v: Value) i64 {
    return switch (v) {
        .int => |x| x,
        .float => |x| @intFromFloat(x),
        .string => |s| std.fmt.parseInt(i64, std.mem.trim(u8, s, " "), 10) catch 0,
        else => 0,
    };
}

test "formatDecimal pads sub-unit magnitudes, zero, and negatives" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("-0.005", try formatDecimal(a, -5, 3));
    try std.testing.expectEqualStrings("0", try formatDecimal(a, 0, 0));
    try std.testing.expectEqualStrings("0.00", try formatDecimal(a, 0, 2));
    try std.testing.expectEqualStrings("7", try formatDecimal(a, 7, 0));
}

test "compareValues orders across numeric kinds and rejects mixed kinds" {
    try std.testing.expectEqual(std.math.Order.lt, compareValues(.{ .int = 1 }, .{ .float = 1.5 }).?);
    try std.testing.expectEqual(std.math.Order.eq, compareValues(.{ .float = 2.0 }, .{ .int = 2 }).?);
    try std.testing.expectEqual(std.math.Order.gt, compareValues(.{ .decimal = .{ .unscaled = 250, .scale = 2 } }, .{ .int = 2 }).?);
    try std.testing.expectEqual(std.math.Order.lt, compareValues(.{ .string = "a" }, .{ .string = "b" }).?);
    try std.testing.expectEqual(std.math.Order.lt, compareValues(.{ .bool = false }, .{ .bool = true }).?);
    try std.testing.expect(compareValues(.{ .string = "1" }, .{ .int = 1 }) == null);
    try std.testing.expect(compareValues(.{ .bool = true }, .{ .int = 1 }) == null);
    try std.testing.expect(compareValues(.{ .date = 1 }, .{ .timestamp = 1 }) == null);
}

test "strptime: widths, %y pivot, impossible days, try_ form" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ts = struct {
        fn f(al: std.mem.Allocator, src: []const u8) ![]const u8 {
            return formatTimestamp(al, (try evalLit(al, src)).timestamp);
        }
    }.f;

    try std.testing.expectEqualStrings("2026-10-03 00:00:00", try ts(a, "strptime('03/10/2026', '%d/%m/%Y')"));
    try std.testing.expectEqualStrings("2026-01-03 00:00:00", try ts(a, "strptime('3/1/2026', '%d/%m/%Y')"));
    try std.testing.expectEqualStrings("2024-02-29 13:05:09", try ts(a, "strptime('2024-02-29 13:05:09', '%Y-%m-%d %H:%M:%S')"));
    try std.testing.expectEqualStrings("1969-10-03 00:00:00", try ts(a, "strptime('03/10/69', '%d/%m/%y')"));
    try std.testing.expectEqualStrings("2068-10-03 00:00:00", try ts(a, "strptime('03/10/68', '%d/%m/%y')"));
    try std.testing.expectEqualStrings("2026-10-03 00:00:00", try ts(a, "strptime('100% 03/10/2026', '100%% %d/%m/%Y')"));

    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('31/02/2026', '%d/%m/%Y')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('03/10/2026 x', '%d/%m/%Y')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('2026-10-03', '%d/%m/%Y')"));
    try std.testing.expectError(error.CastFailed, evalLit(a, "strptime('03/10/2026 24:00:00', '%d/%m/%Y %H:%M:%S')"));
    try std.testing.expect((try evalLit(a, "try_strptime('31/02/2026', '%d/%m/%Y')")) == .null);
    _ = evalLit(a, "strptime('31/02/2026', '%d/%m/%Y')") catch {};
    try std.testing.expect(takeFailure(error.DivByZero) == null);
    try std.testing.expectEqualStrings("strptime: '31/02/2026' is not a date in '%d/%m/%Y' (try_strptime gives null)", takeFailure(error.CastFailed).?);
    try std.testing.expect(takeFailure(error.CastFailed) == null);
    try std.testing.expect((try evalLit(a, "try_strptime('', '%d/%m/%Y')")) == .null);
}

test "a failed CAST names its value and type, and TRY_CAST leaves no note" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectError(error.CastFailed, evalLit(a, "CAST('03/10/2026' AS DATE)"));
    try std.testing.expectEqualStrings("CAST: '03/10/2026' is not a DATE (YYYY-MM-DD; strptime reads other formats)", takeFailure(error.CastFailed).?);
    try std.testing.expectError(error.CastFailed, evalLit(a, "CAST('1,5' AS DECIMAL(10,2))"));
    try std.testing.expectEqualStrings("CAST: '1,5' is not a DECIMAL(10,2)", takeFailure(error.CastFailed).?);
    try std.testing.expect((try evalLit(a, "TRY_CAST('x' AS INT)")) == .null);
    try std.testing.expect(takeFailure(error.CastFailed) == null);
}

test "orderF64: a total order over NaN, so comparisons never hit unreachable" {
    const nan = std.math.nan(f64);
    try std.testing.expectEqual(std.math.Order.eq, orderF64(nan, nan));
    try std.testing.expectEqual(std.math.Order.gt, orderF64(nan, 1.0));
    try std.testing.expectEqual(std.math.Order.lt, orderF64(1.0, nan));
    try std.testing.expectEqual(std.math.Order.gt, orderF64(nan, std.math.inf(f64)));
    try std.testing.expectEqual(std.math.Order.eq, orderF64(0.0, -0.0));
    try std.testing.expectEqual(std.math.Order.lt, orderF64(-1.0, 1.0));

    const v_nan = Value{ .float = nan };
    try std.testing.expectEqual(std.math.Order.eq, compareValues(v_nan, v_nan).?);
    try std.testing.expectEqual(std.math.Order.gt, compareValues(v_nan, .{ .int = 9 }).?);
}

test "regexp_replace: the compiled-pattern cache keys on bytes, not on identity" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqualStrings("X-b-c", (try evalLit(a, "regexp_replace('a-b-c', 'a', 'X')")).string);
    try std.testing.expectEqualStrings("a-b-X", (try evalLit(a, "regexp_replace('a-b-c', 'c', 'X')")).string);
    try std.testing.expectEqualStrings("X-b-c", (try evalLit(a, "regexp_replace('a-b-c', 'a', 'X')")).string);

    try std.testing.expectEqualStrings("Xbc", (try evalLit(a, "regexp_replace('abc', '^a', 'X')")).string);
    try std.testing.expectEqualStrings("abX", (try evalLit(a, "regexp_replace('abc', 'c$', 'X')")).string);

    try std.testing.expectEqualStrings("b-a", (try evalLit(a, "regexp_replace('a-b', '(a)-(b)', '\\2-\\1')")).string);
    try std.testing.expectEqualStrings("b-a", (try evalLit(a, "regexp_replace('a-b', '(a)-(b)', '\\2-\\1')")).string);

    try std.testing.expectError(error.CastFailed, evalLit(a, "regexp_replace('abc', '(', 'X')"));
    try std.testing.expectEqualStrings("Xbc", (try evalLit(a, "regexp_replace('abc', '^a', 'X')")).string);

    try std.testing.expectEqualStrings("abc", (try evalLit(a, "regexp_replace('abc', 'zzz', 'X')")).string);

    var caps: regex.Captures = undefined;
    var pat = "(a)-(b)".*;
    var re = try cachedRegex(&pat);
    try std.testing.expectEqual(@as(?[2]usize, .{ 0, 3 }), try re.find("a-b", 0, &caps));
    pat[1] = 'b';
    pat[5] = 'a';
    re = try cachedRegex(&pat);
    try std.testing.expectEqual(@as(?[2]usize, null), try re.find("a-b", 0, &caps));
    try std.testing.expectEqual(@as(?[2]usize, .{ 0, 3 }), try re.find("b-a", 0, &caps));

    try std.testing.expectError(error.BadPattern, cachedRegex("("));
    re = try cachedRegex("(a)-(b)");
    try std.testing.expectEqual(@as(?[2]usize, .{ 0, 3 }), try re.find("a-b", 0, &caps));
    try std.testing.expectEqual(@as(?[2]usize, .{ 2, 3 }), caps[2]);
    try std.testing.expectEqual(@as(?[2]usize, null), try re.find("b-a", 0, &caps));
}

test "field resolution: the memo verifies its entry instead of trusting it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const int = types.Type.init(.int);
    const wide = types.Schema{ .fields = &.{
        .{ .name = "col36", .ty = int },
        .{ .name = "col37", .ty = int },
        .{ .name = "col38", .ty = int },
        .{ .name = "col39", .ty = int },
    } };
    const flipped = types.Schema{ .fields = &.{
        .{ .name = "col39", .ty = int },
        .{ .name = "col38", .ty = int },
        .{ .name = "col37", .ty = int },
        .{ .name = "col36", .ty = int },
    } };

    const q = struct {
        fn of(alloc: std.mem.Allocator, name: []const u8) ast.QualName {
            const parts = alloc.alloc([]const u8, 1) catch unreachable;
            parts[0] = name;
            return .{ .parts = parts };
        }
    };

    for (0..3) |_| {
        for ([_][]const u8{ "col36", "col37", "col38", "col39" }, 0..) |name, i| {
            try std.testing.expectEqual(i, fieldIndex(wide, q.of(a, name)).?);
            try std.testing.expectEqual(3 - i, fieldIndex(flipped, q.of(a, name)).?);
        }
    }

    try std.testing.expect(fieldIndex(wide, q.of(a, "nope")) == null);
    try std.testing.expectEqual(@as(usize, 0), fieldIndex(wide, q.of(a, "col36")).?);
    try std.testing.expect(fieldIndex(wide, q.of(a, "nope")) == null);

    const tiny = types.Schema{ .fields = &.{.{ .name = "z", .ty = int }} };
    try std.testing.expectEqual(@as(usize, 3), fieldIndex(wide, q.of(a, "col39")).?);
    try std.testing.expect(fieldIndex(tiny, q.of(a, "col39")) == null);
    try std.testing.expectEqual(@as(usize, 0), fieldIndex(tiny, q.of(a, "z")).?);
}
