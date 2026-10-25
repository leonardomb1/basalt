//! JSON text, the one home for writing it: a string's inside (`chars`), a quoted
//! string (`string`), an object key (`key`), a `Value` as a JSON scalar (`scalar`),
//! the `celltext.write` policy for JSON rows (`Text`) and `clipUtf8`, which cuts text
//! for a message without splitting a codepoint.
//!
//! Text that is not valid UTF-8 is repaired as it is escaped, each byte that starts
//! no valid sequence given as U+FFFD, so every output parses as JSON. The control
//! bytes other than `\n`, `\r` and `\t` are written as `\u00XX` (std.json writes
//! `\b` and `\f`; both spellings decode to the same string). Non-finite floats are
//! null, floats take `celltext.writeFloat`, temporal values ride as ISO text and
//! bytes as base64 unless a `Style` says otherwise.

const std = @import("std");
const celltext = @import("celltext.zig");
const format = @import("eval/format.zig");
const decimal = @import("decimal.zig");
const Value = @import("value.zig").Value;

const Writer = std.Io.Writer;

pub const replacement = celltext.Utf8Runs.replacement;

/// `s` as the inside of a JSON string, repaired to UTF-8 first when it is not.
pub fn chars(w: *Writer, s: []const u8) Writer.Error!void {
    if (std.unicode.utf8ValidateSlice(s)) return escapeValid(w, s);
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| try escapeValid(w, t);
}

pub fn string(w: *Writer, s: []const u8) Writer.Error!void {
    try w.writeByte('"');
    try chars(w, s);
    try w.writeByte('"');
}

/// `s` as an object key: the quoted string and its colon.
pub fn key(w: *Writer, s: []const u8) Writer.Error!void {
    try string(w, s);
    try w.writeByte(':');
}

fn escapeValid(w: *Writer, s: []const u8) Writer.Error!void {
    var start: usize = 0;
    for (s, 0..) |c, i| {
        const esc: []const u8 = switch (c) {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            0...8, 11, 12, 14...0x1f => "",
            else => continue,
        };
        try w.writeAll(s[start..i]);
        if (esc.len > 0) try w.writeAll(esc) else try w.print("\\u{x:0>4}", .{c});
        start = i + 1;
    }
    try w.writeAll(s[start..]);
}

/// How `scalar` spells the kinds whose JSON shape is a choice: a decimal as a bare
/// number or a string, bytes as base64 or as their own text (repaired).
pub const Style = struct {
    decimal: enum { number, string } = .string,
    bytes: enum { base64, text } = .base64,
};

/// The shape of the JSON writer and of Avro's nested JSON: decimals as strings,
/// bytes as base64.
pub const typed: Style = .{};

/// The shape nested values take today in `json_object`/`writeJsonValue`, Arrow's
/// `Cursor.json` and Parquet's `jsonValue`: decimals as bare numbers, bytes as text.
pub const nested: Style = .{ .decimal = .number, .bytes = .text };

/// `v` as one JSON scalar: null, a boolean, a number, or a string for text and
/// the temporal kinds.
pub fn scalar(w: *Writer, v: Value, comptime style: Style) Writer.Error!void {
    switch (v) {
        .null => try w.writeAll("null"),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .int => |x| try format.writeInt(w, x),
        .float => |x| if (std.math.isFinite(x)) try celltext.writeFloat(w, x) else try w.writeAll("null"),
        .decimal => |d| switch (style.decimal) {
            .number => try decimal.writeDecimal(w, d.unscaled, d.scale),
            .string => {
                try w.writeByte('"');
                try decimal.writeDecimal(w, d.unscaled, d.scale);
                try w.writeByte('"');
            },
        },
        .string => |s| try string(w, s),
        .bytes => |s| switch (style.bytes) {
            .text => try string(w, s),
            .base64 => {
                try w.writeByte('"');
                try std.base64.standard.Encoder.encodeWriter(w, s);
                try w.writeByte('"');
            },
        },
        .date => |x| {
            try w.writeByte('"');
            try format.writeDate(w, x);
            try w.writeByte('"');
        },
        .time => |x| {
            try w.writeByte('"');
            try format.writeTime(w, x);
            try w.writeByte('"');
        },
        .timestamp => |x| {
            try w.writeByte('"');
            try format.writeTimestamp(w, x);
            try w.writeByte('"');
        },
    }
}

/// The `celltext.write` policy for JSON: decimals and temporals ride as strings,
/// non-finite floats as null and bytes as base64.
pub const Text = struct {
    pub const null_text = "null";
    pub const bool_true = "true";
    pub const bool_false = "false";
    pub const quote_decimal = "\"";
    pub const quote_temporal = "\"";

    pub fn string(_: Text, out: *Writer, s: []const u8) !void {
        try out.writeByte('"');
        try chars(out, s);
        try out.writeByte('"');
    }

    pub fn bytes(_: Text, out: *Writer, s: []const u8) !void {
        try out.writeByte('"');
        try std.base64.standard.Encoder.encodeWriter(out, s);
        try out.writeByte('"');
    }

    pub fn float(_: Text, out: *Writer, x: f64) !void {
        if (std.math.isFinite(x)) return out.print("{d}", .{x});
        return out.writeAll("null");
    }
};

/// The longest prefix of `s` of at most `max` bytes that does not end inside a
/// UTF-8 sequence: a cut that lands on a continuation byte backs up to the byte
/// that leads it (at most three), so valid text stays valid.
pub fn clipUtf8(s: []const u8, max: usize) []const u8 {
    if (s.len <= max) return s;
    var i = max;
    while (i > 0 and max - i < 3 and isContinuation(s[i])) i -= 1;
    if (isContinuation(s[i])) return s[0..max];
    return s[0..i];
}

fn isContinuation(c: u8) bool {
    return c & 0xC0 == 0x80;
}

const testing = std.testing;

fn render(buf: []u8, comptime f: anytype, args: anytype) ![]const u8 {
    var w = Writer.fixed(buf);
    try @call(.auto, f, .{&w} ++ args);
    return w.buffered();
}

fn repairedOf(a: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| try out.appendSlice(a, t);
    return out.items;
}

fn expectParsesTo(a: std.mem.Allocator, json_text: []const u8, want: []const u8) !void {
    const got = try std.json.parseFromSliceLeaky([]const u8, a, json_text, .{});
    try testing.expectEqualStrings(want, got);
    try @import("json.zig").validate(a, json_text);
}

test "jsontext chars escapes quotes, backslash and control bytes, repairs invalid UTF-8" {
    var buf: [128]u8 = undefined;
    try testing.expectEqualStrings("a\\\"b\\\\c\\nd\\u0001", try render(&buf, chars, .{"a\"b\\c\nd\x01"}));
    try testing.expectEqualStrings("caf\u{FFFD}\\t\\\"", try render(&buf, chars, .{"caf\xe9\t\""}));
    try testing.expectEqualStrings("\\u0008\\u000c\\u001f\x7f", try render(&buf, chars, .{"\x08\x0c\x1f\x7f"}));
    try testing.expectEqualStrings("\\u0000", try render(&buf, chars, .{"\x00"}));
    try testing.expectEqualStrings("", try render(&buf, chars, .{""}));
    try testing.expectEqualStrings("\"x\"", try render(&buf, string, .{"x"}));
    try testing.expectEqualStrings("\"k\\n\":", try render(&buf, key, .{"k\n"}));
}

test "jsontext chars: each invalid byte becomes one U+FFFD, valid neighbours are kept" {
    var buf: [128]u8 = undefined;
    const r = replacement;
    const cases = [_]struct { in: []const u8, out: []const u8 }{
        .{ .in = "\xff", .out = r },
        .{ .in = "a\xc3", .out = "a" ++ r },
        .{ .in = "\xc3\xb3\xc3", .out = "\xc3\xb3" ++ r },
        .{ .in = "\xc0\xaf", .out = r ++ r },
        .{ .in = "\xed\xa0\x80", .out = r ++ r ++ r },
        .{ .in = "\xf0\x9f\x98", .out = r ++ r ++ r },
        .{ .in = "\x80\"", .out = r ++ "\\\"" },
        .{ .in = "\xf4\x90\x80\x80", .out = r ++ r ++ r ++ r },
        .{ .in = "\xf0\x9f\x98\x80\xfe", .out = "\xf0\x9f\x98\x80" ++ r },
    };
    for (cases) |c| try testing.expectEqualStrings(c.out, try render(&buf, chars, .{c.in}));
}

test "jsontext string: every single byte and a fuzzed corpus parse back through std.json and json.validate" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var buf: [4096]u8 = undefined;
    for (0..256) |b| {
        const s = [_]u8{@intCast(b)};
        try expectParsesTo(a, try render(&buf, string, .{&s}), try repairedOf(a, &s));
    }
    var prng = std.Random.DefaultPrng.init(0x6a73_6f6e);
    const rand = prng.random();
    const alphabet = "a\"\\\n\r\t\x00\x01\x08\x0c\x1f\x7f\x80\xbf\xc0\xc3\xb3\xe2\x82\xac\xed\xa0\xf0\x9f\x98\x80\xf4\x90\xff ";
    var in: [64]u8 = undefined;
    for (0..2000) |_| {
        const n = rand.uintLessThan(usize, in.len);
        for (in[0..n]) |*c| c.* = if (rand.boolean()) alphabet[rand.uintLessThan(usize, alphabet.len)] else rand.int(u8);
        const out = try render(&buf, string, .{in[0..n]});
        try testing.expect(std.unicode.utf8ValidateSlice(out));
        try expectParsesTo(a, out, try repairedOf(a, in[0..n]));
    }
}

test "jsontext chars equals std.json on valid UTF-8 without \\b or \\f, and the deltas are pinned" {
    var b1: [512]u8 = undefined;
    var b2: [512]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(7);
    const rand = prng.random();
    const pieces = [_][]const u8{ "a", "z", "\"", "\\", "\n", "\r", "\t", "\x00", "\x01", "\x1f", "\x7f", "/", "\xc3\xb3", "\xe2\x82\xac", "\xf0\x9f\x98\x80" };
    var in: std.ArrayList(u8) = .empty;
    defer in.deinit(testing.allocator);
    for (0..1000) |_| {
        in.clearRetainingCapacity();
        for (0..rand.uintLessThan(usize, 24)) |_| try in.appendSlice(testing.allocator, pieces[rand.uintLessThan(usize, pieces.len)]);
        var w = Writer.fixed(&b2);
        try std.json.Stringify.encodeJsonStringChars(in.items, .{}, &w);
        try testing.expectEqualStrings(w.buffered(), try render(&b1, chars, .{in.items}));
    }
    var w = Writer.fixed(&b2);
    try std.json.Stringify.encodeJsonStringChars("\x08\x0c", .{}, &w);
    try testing.expectEqualStrings("\\b\\f", w.buffered());
    try testing.expectEqualStrings("\\u0008\\u000c", try render(&b1, chars, .{"\x08\x0c"}));
    w = Writer.fixed(&b2);
    try std.json.Stringify.encodeJsonStringChars("caf\xe9", .{}, &w);
    try testing.expectEqualStrings("caf\xe9", w.buffered());
    try testing.expectEqualStrings("caf\u{FFFD}", try render(&b1, chars, .{"caf\xe9"}));
}

test "jsontext chars equals json/write writeEscaped, the copy it was moved from" {
    const jsonwrite = @import("../format/json/write.zig");
    var b1: [512]u8 = undefined;
    var b2: [512]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(11);
    const rand = prng.random();
    var in: [48]u8 = undefined;
    for (0..2000) |_| {
        const n = rand.uintLessThan(usize, in.len);
        rand.bytes(in[0..n]);
        var w = Writer.fixed(&b2);
        try jsonwrite.writeEscaped(&w, in[0..n]);
        try testing.expectEqualStrings(w.buffered(), try render(&b1, chars, .{in[0..n]}));
    }
}

test "jsontext scalar: every Value kind in both styles" {
    var buf: [512]u8 = undefined;
    const Case = struct { v: Value, typed: []const u8, nested: []const u8 };
    const cases = [_]Case{
        .{ .v = .null, .typed = "null", .nested = "null" },
        .{ .v = .{ .bool = true }, .typed = "true", .nested = "true" },
        .{ .v = .{ .bool = false }, .typed = "false", .nested = "false" },
        .{ .v = .{ .int = 0 }, .typed = "0", .nested = "0" },
        .{ .v = .{ .int = std.math.minInt(i64) }, .typed = "-9223372036854775808", .nested = "-9223372036854775808" },
        .{ .v = .{ .int = std.math.maxInt(i64) }, .typed = "9223372036854775807", .nested = "9223372036854775807" },
        .{ .v = .{ .float = 0.1 }, .typed = "0.1", .nested = "0.1" },
        .{ .v = .{ .float = -0.0 }, .typed = "-0", .nested = "-0" },
        .{ .v = .{ .float = 1e21 }, .typed = "1000000000000000000000", .nested = "1000000000000000000000" },
        .{ .v = .{ .float = -2.25 }, .typed = "-2.25", .nested = "-2.25" },
        .{ .v = .{ .float = std.math.nan(f64) }, .typed = "null", .nested = "null" },
        .{ .v = .{ .float = std.math.inf(f64) }, .typed = "null", .nested = "null" },
        .{ .v = .{ .float = -std.math.inf(f64) }, .typed = "null", .nested = "null" },
        .{ .v = .{ .decimal = .{ .unscaled = 12345, .scale = 2 } }, .typed = "\"123.45\"", .nested = "123.45" },
        .{ .v = .{ .decimal = .{ .unscaled = -7, .scale = 4 } }, .typed = "\"-0.0007\"", .nested = "-0.0007" },
        .{ .v = .{ .decimal = .{ .unscaled = 5, .scale = 0 } }, .typed = "\"5\"", .nested = "5" },
        .{ .v = .{ .decimal = .{ .unscaled = 0, .scale = 2 } }, .typed = "\"0.00\"", .nested = "0.00" },
        .{ .v = .{ .decimal = .{ .unscaled = -99999999999999999999999999999999999999, .scale = 38 } }, .typed = "\"-0.99999999999999999999999999999999999999\"", .nested = "-0.99999999999999999999999999999999999999" },
        .{ .v = .{ .string = "" }, .typed = "\"\"", .nested = "\"\"" },
        .{ .v = .{ .string = "say \"hi\"\n" }, .typed = "\"say \\\"hi\\\"\\n\"", .nested = "\"say \\\"hi\\\"\\n\"" },
        .{ .v = .{ .string = "\xff" }, .typed = "\"" ++ replacement ++ "\"", .nested = "\"" ++ replacement ++ "\"" },
        .{ .v = .{ .bytes = "" }, .typed = "\"\"", .nested = "\"\"" },
        .{ .v = .{ .bytes = "plain" }, .typed = "\"cGxhaW4=\"", .nested = "\"plain\"" },
        .{ .v = .{ .bytes = "\x01\x02 \\ \t\n\"" }, .typed = "\"AQIgXCAJCiI=\"", .nested = "\"\\u0001\\u0002 \\\\ \\t\\n\\\"\"" },
        .{ .v = .{ .bytes = "\xff\x00" }, .typed = "\"/wA=\"", .nested = "\"" ++ replacement ++ "\\u0000\"" },
        .{ .v = .{ .date = 0 }, .typed = "\"1970-01-01\"", .nested = "\"1970-01-01\"" },
        .{ .v = .{ .date = 19723 }, .typed = "\"2024-01-01\"", .nested = "\"2024-01-01\"" },
        .{ .v = .{ .date = std.math.minInt(i32) }, .typed = "\"-5877641-06-23\"", .nested = "\"-5877641-06-23\"" },
        .{ .v = .{ .time = 0 }, .typed = "\"00:00:00\"", .nested = "\"00:00:00\"" },
        .{ .v = .{ .time = 3_723_000_001 }, .typed = "\"01:02:03.000001\"", .nested = "\"01:02:03.000001\"" },
        .{ .v = .{ .timestamp = 0 }, .typed = "\"1970-01-01 00:00:00\"", .nested = "\"1970-01-01 00:00:00\"" },
        .{ .v = .{ .timestamp = -1 }, .typed = "\"1969-12-31 23:59:59.999999\"", .nested = "\"1969-12-31 23:59:59.999999\"" },
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (cases) |c| {
        var w = Writer.fixed(&buf);
        try scalar(&w, c.v, typed);
        try testing.expectEqualStrings(c.typed, w.buffered());
        _ = try std.json.parseFromSliceLeaky(std.json.Value, a, w.buffered(), .{});
        w = Writer.fixed(&buf);
        try scalar(&w, c.v, nested);
        try testing.expectEqualStrings(c.nested, w.buffered());
        _ = try std.json.parseFromSliceLeaky(std.json.Value, a, w.buffered(), .{});
    }
}

test "jsontext scalar(nested) pins eval writeJsonValue on valid text, the copy Wave 1 moves" {
    const row = @import("eval/row.zig");
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const values = [_]Value{
        .null,                                            .{ .bool = true },                .{ .int = -42 },
        .{ .float = 0.1 },                                .{ .float = 5e-324 },             .{ .float = 123456789.125 },
        .{ .float = std.math.nan(f64) },                  .{ .float = -std.math.inf(f64) }, .{ .decimal = .{ .unscaled = -18446744073709551615, .scale = 6 } },
        .{ .decimal = .{ .unscaled = 250, .scale = 2 } }, .{ .string = "a\tb\"c\\" },       .{ .string = "\xc3\xb3 \xf0\x9f\x98\x80" },
        .{ .bytes = "plain \x01" },                       .{ .date = -719528 },             .{ .time = 86_399_999_999 },
        .{ .timestamp = 1_704_067_200_123_456 },          .{ .string = "not {json" },
    };
    for (values) |v| {
        var want = Writer.Allocating.init(a);
        try row.writeJsonValue(a, v, &want.writer);
        var got = Writer.Allocating.init(a);
        try scalar(&got.writer, v, nested);
        try testing.expectEqualStrings(want.written(), got.written());
    }
    var want = Writer.Allocating.init(a);
    try row.writeJsonValue(a, .{ .string = "caf\xe9" }, &want.writer);
    try testing.expectEqualStrings("\"caf\u{FFFD}\"", want.written());
    want = Writer.Allocating.init(a);
    try row.writeJsonValue(a, .{ .string = "\x08" }, &want.writer);
    try testing.expectEqualStrings("\"\\u0008\"", want.written());
}

test "jsontext Text renders the celltext golden batches as json/write Text does" {
    const jsonwrite = @import("../format/json/write.zig");
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for (try celltext.goldenBatches(a)) |batch| {
        for (batch.columns) |*col| {
            for (0..batch.len) |r| {
                var want = Writer.Allocating.init(a);
                try celltext.write(&want.writer, col, r, jsonwrite.Text{});
                var got = Writer.Allocating.init(a);
                try celltext.write(&got.writer, col, r, Text{});
                try testing.expectEqualStrings(want.written(), got.written());
                _ = try std.json.parseFromSliceLeaky(std.json.Value, a, got.written(), .{});
            }
        }
    }
}

test "jsontext clipUtf8 never splits a codepoint and keeps byte clips on ASCII" {
    try testing.expectEqualStrings("abc", clipUtf8("abc", 3));
    try testing.expectEqualStrings("abc", clipUtf8("abc", 80));
    try testing.expectEqualStrings("ab", clipUtf8("abc", 2));
    try testing.expectEqualStrings("", clipUtf8("abc", 0));
    try testing.expectEqualStrings("", clipUtf8("", 0));
    try testing.expectEqualStrings("a", clipUtf8("a\xc3\xb3", 2));
    try testing.expectEqualStrings("a\xc3\xb3", clipUtf8("a\xc3\xb3b", 3));
    try testing.expectEqualStrings("", clipUtf8("\xe2\x82\xac", 2));
    try testing.expectEqualStrings("", clipUtf8("\xf0\x9f\x98\x80", 3));
    try testing.expectEqualStrings("x", clipUtf8("x\xf0\x9f\x98\x80", 4));
    try testing.expectEqualStrings("x\xf0\x9f\x98\x80", clipUtf8("x\xf0\x9f\x98\x80y", 5));
    try testing.expectEqualStrings("\x80\x80\x80\x80", clipUtf8("\x80\x80\x80\x80\x80", 4));
    try testing.expectEqualStrings("\xff", clipUtf8("\xff\xff", 1));
    var prng = std.Random.DefaultPrng.init(3);
    const rand = prng.random();
    const pieces = [_][]const u8{ "a", "\xc3\xb3", "\xe2\x82\xac", "\xf0\x9f\x98\x80" };
    var in: std.ArrayList(u8) = .empty;
    defer in.deinit(testing.allocator);
    for (0..500) |_| {
        in.clearRetainingCapacity();
        for (0..rand.uintLessThan(usize, 40)) |_| try in.appendSlice(testing.allocator, pieces[rand.uintLessThan(usize, pieces.len)]);
        const max = rand.uintLessThan(usize, 100);
        const got = clipUtf8(in.items, max);
        try testing.expect(got.len <= max or got.len == in.items.len);
        try testing.expect(got.len + 3 >= @min(max, in.items.len));
        try testing.expect(std.unicode.utf8ValidateSlice(got));
    }
    var ascii: [300]u8 = undefined;
    @memset(&ascii, 'q');
    for ([_]usize{ 0, 1, 80, 256, 299, 300, 400 }) |n| try testing.expectEqualStrings(ascii[0..@min(ascii.len, n)], clipUtf8(&ascii, n));
}
