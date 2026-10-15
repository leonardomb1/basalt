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

fn escapes(comptime P: type) bool {
    return @hasDecl(P, "escape_scalars") and P.escape_scalars;
}

inline fn scalar(w: anytype, c: *const Column, r: usize, p: anytype) !void {
    const P = @TypeOf(p);
    switch (c.ty.kind) {
        .bool => try w.writeAll(if (c.data.b[r]) P.bool_true else P.bool_false),
        .int => try w.print("{d}", .{c.data.i64[r]}),
        .float => if (@hasDecl(P, "float")) try p.float(w, c.data.f64[r]) else try w.print("{d}", .{c.data.f64[r]}),
        .decimal => {
            const q = if (@hasDecl(P, "quote_decimal")) P.quote_decimal else "";
            try w.writeAll(q);
            try format.writeDecimal(w, c.data.dec[r].unscaled, c.data.dec[r].scale);
            try w.writeAll(q);
        },
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

/// Tests' reference spelling of a value, as the sinks built it before `write`.
pub fn referenceText(arena: std.mem.Allocator, v: Value, bool_true: []const u8, bool_false: []const u8) ![]const u8 {
    return switch (v) {
        .null => "",
        .bool => |b| if (b) bool_true else bool_false,
        .date => |days| try format.formatDate(arena, days),
        .time => |t| try format.formatTime(arena, t),
        .timestamp => |micros| try format.formatTimestamp(arena, micros),
        else => try format.valueToString(arena, v),
    };
}

/// Strings a test batch draws from: escapes, quotes, delimiters, the stream-load
/// separators, non-ASCII and text longer than a SIMD lane.
pub const test_texts = [_][]const u8{
    "",                       "plain",               "a\x01b\x02c", "\x01\x02",
    "a\\Nb",                  "\\",                  "N",           "  padded   ",
    "\xc3\xb3 utf8",          "\\N ",                "NULL",        "a long value\x01 that spans\x02 more than sixteen bytes\x01",
    "0123456789abcdef",       "0123456789abcde\x02", "tab\there",   "line\nbreak",
    "cr\rlf\r\n",             "say \"hi\"",          "O'Brien",     "a,b;c:d.e",
    "\xf0\x9f\x98\x80 emoji", "back\\slash\\",       "\x00nul",
};

/// A batch of `rows` random rows over every scalar kind and a nested column, about
/// a sixth of each column null; decimals carry scales other than their column's.
pub fn testBatch(arena: std.mem.Allocator, seed: u64, rows: usize) !Batch {
    const kinds = [_]types.Type{
        types.Type.init(.int).asNullable(),
        types.Type.init(.float).asNullable(),
        types.Type.decimal(14, 2).asNullable(),
        types.Type.decimal(38, 6).asNullable(),
        types.Type.init(.date).asNullable(),
        types.Type.init(.time).asNullable(),
        types.Type.init(.timestamp).asNullable(),
        types.Type.init(.bool).asNullable(),
        types.Type.init(.string).asNullable(),
        types.Type.init(.bytes).asNullable(),
        types.Type.init(.array).asNullable(),
    };
    const floats = [_]f64{ 0, -0.0, 1.5, -2.25, 1e-300, 5e-324, -5e-324, 1.7976931348623157e308, -1.7976931348623157e308, std.math.inf(f64), -std.math.inf(f64), std.math.nan(f64), 123456.789, 1e21, 0.1 };
    var prng = std.Random.DefaultPrng.init(seed);
    const rnd = prng.random();
    const cols = try arena.alloc(Column, kinds.len);
    for (kinds, cols) |ty, *col| {
        const nested = ty.kind == .array;
        var b = column.Builder.init(arena, if (nested) types.Type.init(.string).asNullable() else ty);
        for (0..rows) |_| {
            if (rnd.uintLessThan(u8, 6) == 0) {
                try b.append(.null);
                continue;
            }
            try b.append(switch (ty.kind) {
                .int => .{ .int = switch (rnd.uintLessThan(u8, 3)) {
                    0 => std.math.minInt(i64),
                    1 => std.math.maxInt(i64),
                    else => rnd.int(i32),
                } },
                .float => .{ .float = if (rnd.boolean()) floats[rnd.uintLessThan(usize, floats.len)] else (rnd.float(f64) - 0.5) * 1e6 },
                .decimal => .{ .decimal = .{
                    .unscaled = switch (rnd.uintLessThan(u8, 4)) {
                        0 => rnd.int(i128) >> 6,
                        1 => @as(i128, std.math.maxInt(u64)) * (if (rnd.boolean()) @as(i128, 1) else -1),
                        2 => rnd.intRangeAtMost(i128, -999, 999),
                        else => rnd.int(i64),
                    },
                    .scale = rnd.uintAtMost(u8, 12),
                } },
                .date => .{ .date = if (rnd.uintLessThan(u8, 8) == 0) std.math.minInt(i32) + 1 else rnd.intRangeAtMost(i32, -800_000, 3_000_000) },
                .time => .{ .time = rnd.intRangeAtMost(i64, 0, 86_400_000_000 - 1) },
                .timestamp => .{ .timestamp = rnd.intRangeAtMost(i64, -100_000_000_000_000_000, 100_000_000_000_000_000) },
                .bool => .{ .bool = rnd.boolean() },
                .bytes => .{ .bytes = test_texts[rnd.uintLessThan(usize, test_texts.len)] },
                else => .{ .string = test_texts[rnd.uintLessThan(usize, test_texts.len)] },
            });
        }
        col.* = try b.finish();
        if (nested) col.ty = ty;
    }
    const fields = try arena.alloc(types.Schema.Field, kinds.len);
    for (fields, kinds) |*f, ty| f.* = .{ .name = "c", .ty = ty };
    const schema = try arena.create(types.Schema);
    schema.* = .{ .fields = fields };
    return .{ .schema = schema, .columns = cols, .len = rows };
}
