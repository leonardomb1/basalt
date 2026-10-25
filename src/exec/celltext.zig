//! The cell factory: one typed writer for every sink that spells cells as text.
//! `write(w, c, r, p)` switches on the column's kind and writes the cell straight
//! from its typed storage, never boxing it into a `Value` nor allocating; `text`
//! gives a cell's plain text for a sink that transcodes or keeps it. The target's
//! policy `p` supplies only what differs between targets:
//!
//! - `null_text`, `bool_true`, `bool_false`: how null and the booleans are spelled;
//! - `string(w, s)`: how a string is escaped or quoted;
//! - optionally `bytes(w, s)` (else as a string), `float(w, x)` (else `{d}`),
//!   `quote_decimal` and `quote_temporal` (text around a decimal, or a date, time
//!   or timestamp), and `escape_scalars`, which passes every other cell's text
//!   through `string` too, a nested cell's as the empty text.
//!
//! A nested cell is otherwise null, as `Column.getValue` has it.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const format = @import("eval/format.zig");
const Batch = @import("batch.zig").Batch;
const Value = @import("value.zig").Value;

const Column = column.Column;

/// The widest text a non-string cell renders to: a float in plain notation, at 5e-324.
pub const scalar_bound = 340;

/// The policy for `text` with booleans spelled as values print.
pub const Plain = struct {
    pub const bool_true = "true";
    pub const bool_false = "false";
};

/// `s` cut into valid UTF-8 runs, each byte that starts no valid sequence given as
/// U+FFFD in its place, so a writer repairs text as it escapes it, as pyarrow reads
/// invalid bytes. A writer asks `std.unicode.utf8ValidateSlice` first and walks
/// runs only for text that fails it.
pub const Utf8Runs = struct {
    s: []const u8,
    i: usize = 0,

    pub const replacement = "\u{FFFD}";

    pub fn next(self: *Utf8Runs) ?[]const u8 {
        if (self.i >= self.s.len) return null;
        const start = self.i;
        while (self.i < self.s.len) {
            const n = seqLen(self.s[self.i..]);
            if (n == 0) break;
            self.i += n;
        }
        if (self.i > start) return self.s[start..self.i];
        self.i += 1;
        return replacement;
    }

    fn seqLen(s: []const u8) usize {
        if (s[0] < 0x80) return 1;
        const n = std.unicode.utf8ByteSequenceLength(s[0]) catch return 0;
        if (n > s.len) return 0;
        _ = std.unicode.utf8Decode(s[0..n]) catch return 0;
        return n;
    }
};

pub fn isNull(c: *const Column, r: usize) bool {
    return !c.validity.get(r) or c.ty.kind == .array or c.ty.kind == .@"struct";
}

pub inline fn write(w: anytype, c: *const Column, r: usize, p: anytype) !void {
    const P = @TypeOf(p);
    if (!c.validity.get(r)) return w.writeAll(P.null_text);
    switch (c.ty.kind) {
        .string => return p.string(w, c.data.bytes.at(r)),
        .bytes => return if (@hasDecl(P, "bytes")) p.bytes(w, c.data.bytes.at(r)) else p.string(w, c.data.bytes.at(r)),
        .array, .@"struct" => return if (escapes(P)) p.string(w, "") else w.writeAll(P.null_text),
        else => {},
    }
    if (escapes(P)) {
        var buf: [scalar_bound]u8 = undefined;
        var fw = std.Io.Writer.fixed(&buf);
        try scalar(&fw, c, r, p);
        return p.string(w, fw.buffered());
    }
    return scalar(w, c, r, p);
}

/// A non-null cell's text, unescaped: a string's own bytes, or the scalar rendered
/// into `buf`.
pub fn text(c: *const Column, r: usize, buf: *[scalar_bound]u8, p: anytype) []const u8 {
    switch (c.ty.kind) {
        .string, .bytes => return c.data.bytes.at(r),
        .array, .@"struct" => return "",
        else => {},
    }
    var fw = std.Io.Writer.fixed(buf);
    scalar(&fw, c, r, p) catch unreachable;
    return fw.buffered();
}

/// `x` as `{d}` spells it: its shortest round-tripping decimal, in plain notation.
/// A short decimal is written from its integer digits instead (`shortDecimal`);
/// anything else takes `{d}`.
pub fn writeFloat(w: anytype, x: f64) !void {
    const d = shortDecimal(x) orelse return w.print("{d}", .{x});
    var buf: [40]u8 = undefined;
    var i: usize = buf.len;
    var u = d.digits;
    if (d.places > 0) {
        for (0..d.places) |_| {
            i -= 1;
            buf[i] = '0' + @as(u8, @intCast(u % 10));
            u /= 10;
        }
        i -= 1;
        buf[i] = '.';
    }
    while (true) {
        i -= 1;
        buf[i] = '0' + @as(u8, @intCast(u % 10));
        u /= 10;
        if (u == 0) break;
    }
    if (d.neg) {
        i -= 1;
        buf[i] = '-';
    }
    return w.writeAll(buf[i..]);
}

const fast_places = 9;

const ShortDecimal = struct { digits: u64, places: usize, neg: bool };

/// `x` as an integer of digits and a count of decimal places, when it is the one
/// decimal at the fewest places (up to `fast_places`) that reads back as `x`: that
/// is the digit string the shortest round-trip form holds. A decimal reads back as
/// `c / 10^k` divided in f64, both exact below 2^53, which IEEE division rounds as
/// parsing does. Null for zero, NaN, infinities, magnitudes outside [1e-6, 1e15),
/// or two decimals at the fewest places reading back alike (the shortest form then
/// picks the nearer, which `{d}` works out).
fn shortDecimal(x: f64) ?ShortDecimal {
    const a = @abs(x);
    if (!(a >= 1e-6 and a < 1e15)) return null;
    var p: f64 = 1;
    for (0..fast_places + 1) |k| {
        const v = a * p;
        if (v >= 9007199254740991.0) return null;
        const c = @round(v);
        var hit: ?f64 = null;
        for ([_]f64{ c - 1, c, c + 1 }) |cand| {
            if (cand < 0 or cand / p != a) continue;
            if (hit != null) return null;
            hit = cand;
        }
        if (hit) |h| return .{ .digits = @intFromFloat(h), .places = k, .neg = x < 0 };
        p *= 10;
    }
    return null;
}

fn escapes(comptime P: type) bool {
    return @hasDecl(P, "escape_scalars") and P.escape_scalars;
}

/// Out of `scalar`'s line, so the loops it is inlined into stay as tight as before
/// decimals had two storages to read.
noinline fn decimalCell(w: anytype, c: *const Column, r: usize, q: []const u8) !void {
    try w.writeAll(q);
    const d = c.decAt(r);
    try format.writeDecimal(w, d.unscaled, d.scale);
    try w.writeAll(q);
}

inline fn scalar(w: anytype, c: *const Column, r: usize, p: anytype) !void {
    const P = @TypeOf(p);
    switch (c.ty.kind) {
        .bool => try w.writeAll(if (c.data.b[r]) P.bool_true else P.bool_false),
        .int => try format.writeInt(w, c.data.i64[r]),
        .float => if (@hasDecl(P, "float")) try p.float(w, c.data.f64[r]) else try writeFloat(w, c.data.f64[r]),
        .decimal => try decimalCell(w, c, r, if (@hasDecl(P, "quote_decimal")) P.quote_decimal else ""),
        .date, .time, .timestamp => {
            const q = if (@hasDecl(P, "quote_temporal")) P.quote_temporal else "";
            try w.writeAll(q);
            switch (c.ty.kind) {
                .date => try format.writeDate(w, c.data.i32[r]),
                .time => try format.writeTime(w, c.data.i64[r]),
                else => try format.writeTimestamp(w, c.data.i64[r]),
            }
            try w.writeAll(q);
        },
        .string, .bytes, .array, .@"struct" => unreachable,
    }
}

/// Tests' fixture: a one-column batch of `kind` over the values sinks spell
/// differently, its last row null; decimals carry scales other than the column's,
/// as a DECIMAL of no stated precision (PostgreSQL's NUMERIC) holds them.
pub fn goldenColumn(arena: std.mem.Allocator, kind: types.TypeKind) !Batch {
    const nested = kind == .array or kind == .@"struct";
    const ty = if (kind == .decimal) types.Type.decimal(0, 2).asNullable() else types.Type.init(kind).asNullable();
    var b = column.Builder.init(arena, if (nested) types.Type.init(.string).asNullable() else ty);
    switch (kind) {
        .bool => for ([_]bool{ true, false }) |x| try b.append(.{ .bool = x }),
        .int => for ([_]i64{ 0, -1, std.math.minInt(i64), std.math.maxInt(i64) }) |x| try b.append(.{ .int = x }),
        .float => for ([_]f64{ 0.1, 0, -0.0, 5e-324, 1e21, -2.25, std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64) }) |x| try b.append(.{ .float = x }),
        .decimal => for ([_]struct { i128, u8 }{ .{ 12345, 2 }, .{ -7, 4 }, .{ 5, 0 }, .{ 12345, 5 }, .{ -@as(i128, std.math.maxInt(u64)), 6 }, .{ 0, 2 } }) |x| try b.append(.{ .decimal = .{ .unscaled = x[0], .scale = x[1] } }),
        .string => for (golden_texts) |x| try b.append(.{ .string = x }),
        .bytes => for ([_][]const u8{ "", "plain", "\x01\x02 \\ \t\n\"", "\xc3\xb3" }) |x| try b.append(.{ .bytes = x }),
        .date => for ([_]i32{ 0, 19723, -719528, std.math.minInt(i32) + 1 }) |x| try b.append(.{ .date = x }),
        .time => for ([_]i64{ 0, 3_723_000_001, 86_399_999_999 }) |x| try b.append(.{ .time = x }),
        .timestamp => for ([_]i64{ 0, 1_704_067_200_123_456, -1, -62_135_596_800_000_000 }) |x| try b.append(.{ .timestamp = x }),
        .array, .@"struct" => try b.append(.{ .string = "x" }),
    }
    try b.append(.null);
    const cols = try arena.alloc(Column, 1);
    cols[0] = try b.finish();
    if (nested) cols[0].ty = ty;
    const schema = try arena.create(types.Schema);
    schema.* = .{ .fields = try arena.dupe(types.Schema.Field, &.{.{ .name = "c", .ty = ty }}) };
    return .{ .schema = schema, .columns = cols, .len = cols[0].len };
}

/// Strings `goldenColumn` holds: escapes, quotes, delimiters, the stream-load
/// separators, non-ASCII and text longer than a SIMD lane.
pub const golden_texts = [_][]const u8{
    "",                           "plain",       "tab\there",     "line\nbreak",               "cr\rlf\r\n",
    "say \"hi\"",                 "O'Brien",     "back\\slash\\", "a\\Nb",                     "\\N ",
    "NULL",                       "a\x01b\x02c", "a,b;c:d.e-1 0", "\xc3\xb3 \xf0\x9f\x98\x80", "\x00nul",
    "sixteen bytes and more\x01",
};

/// The plain spelling of 5e-324, which `goldenColumn` holds among its floats.
pub const golden_tiny = "0." ++ "0" ** 323 ++ "5";

/// Tests' fixture: three rows of an int, a string and a decimal, the middle row
/// null, to pin separators and row ends.
pub fn goldenRows(arena: std.mem.Allocator) !Batch {
    const tys = [_]types.Type{ types.Type.init(.int).asNullable(), types.Type.init(.string).asNullable(), types.Type.decimal(10, 2).asNullable() };
    const rows = [_][3]Value{
        .{ .{ .int = 1 }, .{ .string = "a b" }, .{ .decimal = .{ .unscaled = 250, .scale = 2 } } },
        .{ .null, .null, .null },
        .{ .{ .int = -2 }, .{ .string = "" }, .{ .decimal = .{ .unscaled = -1, .scale = 1 } } },
    };
    const cols = try arena.alloc(Column, tys.len);
    for (tys, cols, 0..) |ty, *col, c| {
        var b = column.Builder.init(arena, ty);
        for (rows) |row| try b.append(row[c]);
        col.* = try b.finish();
    }
    const fields = try arena.alloc(types.Schema.Field, tys.len);
    for (fields, tys, [_][]const u8{ "i", "s", "d" }) |*f, ty, n| f.* = .{ .name = n, .ty = ty };
    const schema = try arena.create(types.Schema);
    schema.* = .{ .fields = fields };
    return .{ .schema = schema, .columns = cols, .len = rows.len };
}

/// Tests' fixture: `goldenColumn` of every kind, in the order the golden tests list
/// them, then `goldenRows`.
pub fn goldenBatches(arena: std.mem.Allocator) ![]const Batch {
    const kinds = [_]types.TypeKind{ .bool, .int, .float, .decimal, .string, .bytes, .date, .time, .timestamp, .array };
    const out = try arena.alloc(Batch, kinds.len + 1);
    for (kinds, out[0..kinds.len]) |k, *b| b.* = try goldenColumn(arena, k);
    out[kinds.len] = try goldenRows(arena);
    return out;
}

/// Tests' check of one rendering per `goldenBatches` batch against its golden bytes.
pub fn expectGolden(want: []const []const u8, got: []const []const u8) !void {
    try std.testing.expectEqual(want.len, got.len);
    for (want, got) |w, g| try std.testing.expectEqualStrings(w, g);
}

test "writeFloat spells every float as {d} does: short decimals, near-ties, extremes, and random bits" {
    var prng = std.Random.DefaultPrng.init(0xf10a7);
    const rnd = prng.random();
    const fixed = [_]f64{ 0, -0.0, 0.1, 0.2, 0.3, 1.0 / 3.0, 17, 17.5, -2.25, 21168.23, 1e14 + 0.5, 999999999999999.9, 123456789.123, 5e-7, 1e-6, 0.000123, 9007199254740991.0, 1e15, 1e21, 5e-324, std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64), 0.15, 2.675, 1.005, 100, 1e-5 };
    var a: [400]u8 = undefined;
    var b: [400]u8 = undefined;
    var i: usize = 0;
    while (i < 300_000) : (i += 1) {
        const x: f64 = if (i < fixed.len) fixed[i] else switch (i % 4) {
            0 => @as(f64, @floatFromInt(rnd.intRangeAtMost(i64, -99_999_999_99, 99_999_999_99))) / std.math.pow(f64, 10, @floatFromInt(rnd.uintLessThan(u32, 10))),
            1 => @bitCast(rnd.int(u64)),
            2 => (rnd.float(f64) - 0.5) * std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -8, 16))),
            else => @as(f64, @floatFromInt(rnd.intRangeAtMost(i64, -10_000_000, 10_000_000))) / 100.0,
        };
        var fa = std.Io.Writer.fixed(&a);
        var fb = std.Io.Writer.fixed(&b);
        try writeFloat(&fa, x);
        try fb.print("{d}", .{x});
        try std.testing.expectEqualStrings(fb.buffered(), fa.buffered());
    }
}

test "writeInt, writeDate and writeTimestamp spell values as their format strings do" {
    var prng = std.Random.DefaultPrng.init(0xda7e);
    const rnd = prng.random();
    var a: [64]u8 = undefined;
    var b: [64]u8 = undefined;
    for (0..200_000) |i| {
        var fa = std.Io.Writer.fixed(&a);
        var fb = std.Io.Writer.fixed(&b);
        switch (i % 3) {
            0 => {
                const v: i64 = if (i < 3) std.math.minInt(i64) else rnd.int(i64) >> @intCast(rnd.uintLessThan(u8, 63));
                try format.writeInt(&fa, v);
                try fb.print("{d}", .{v});
            },
            1 => {
                const days = rnd.intRangeAtMost(i64, -1_000_000, 3_500_000);
                try format.writeDate(&fa, days);
                const c = @import("eval/time.zig").civilFromDays(days);
                if (c.y < 0) try fb.writeByte('-');
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ @abs(c.y), c.m, c.d });
            },
            else => {
                const us = rnd.intRangeAtMost(i64, -50_000_000_000_000_000, 250_000_000_000_000_000);
                const t = if (i % 2 == 0) @divFloor(us, 1_000_000) * 1_000_000 else us;
                try format.writeTimestamp(&fa, t);
                const days = @divFloor(t, 86_400_000_000);
                const rest: u64 = @intCast(t - days * 86_400_000_000);
                const secs = rest / 1_000_000;
                const frac = rest % 1_000_000;
                const c = @import("eval/time.zig").civilFromDays(days);
                if (c.y < 0) try fb.writeByte('-');
                try fb.print("{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{ @abs(c.y), c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60 });
                if (frac != 0) try fb.print(".{d:0>6}", .{frac});
            },
        }
        try std.testing.expectEqualStrings(fb.buffered(), fa.buffered());
    }
}
