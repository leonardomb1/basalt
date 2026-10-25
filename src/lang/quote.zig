//! Basalt-DSL quoting, written once: a string literal between single quotes and a
//! quoted identifier between double quotes, each inner quote doubled. Output goes
//! to a `*std.Io.Writer` or appends to a `*std.array_list.Managed(u8)`.

const std = @import("std");

/// `s` as a DSL string literal: `it's` is `'it''s'`.
pub fn strLit(out: anytype, s: []const u8) !void {
    return quoted(out, s, '\'');
}

/// `s` as a quoted DSL identifier: `a"b` is `"a""b"`.
pub fn ident(out: anytype, s: []const u8) !void {
    return quoted(out, s, '"');
}

fn quoted(out: anytype, s: []const u8, comptime q: u8) !void {
    const T = @TypeOf(out);
    const sink = comptime if (T == *std.Io.Writer) .writer else if (T == *std.array_list.Managed(u8)) .list else @compileError("quote: unsupported sink " ++ @typeName(T));
    const put = struct {
        fn bytes(o: T, b: []const u8) !void {
            return if (sink == .writer) o.writeAll(b) else o.appendSlice(b);
        }
    };
    try put.bytes(out, &.{q});
    var start: usize = 0;
    while (std.mem.indexOfScalarPos(u8, s, start, q)) |i| {
        try put.bytes(out, s[start .. i + 1]);
        try put.bytes(out, &.{q});
        start = i + 1;
    }
    try put.bytes(out, s[start..]);
    try put.bytes(out, &.{q});
}

const testing = std.testing;

test "quote: strLit and ident match the byte-wise doubling they replace, on both sinks" {
    const Orig = struct {
        fn oracle(a: std.mem.Allocator, s: []const u8, q: u8) ![]const u8 {
            var out = std.array_list.Managed(u8).init(a);
            try out.append(q);
            for (s) |c| {
                if (c == q) try out.append(q);
                try out.append(c);
            }
            try out.append(q);
            return out.items;
        }
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const corpus = [_][]const u8{ "", "'", "\"", "''", "\"\"", "it's", "a\"b", "'a'", "x''y", "ação 😀", "\x00'\xff", "'\"'\"", "plain", "end'", "'start" };
    for (corpus) |s| {
        var list = std.array_list.Managed(u8).init(a);
        try strLit(&list, s);
        try testing.expectEqualStrings(try Orig.oracle(a, s, '\''), list.items);
        list.clearRetainingCapacity();
        try ident(&list, s);
        try testing.expectEqualStrings(try Orig.oracle(a, s, '"'), list.items);

        var aw = std.Io.Writer.Allocating.init(a);
        try strLit(&aw.writer, s);
        try testing.expectEqualStrings(try Orig.oracle(a, s, '\''), aw.written());
        aw.clearRetainingCapacity();
        try ident(&aw.writer, s);
        try testing.expectEqualStrings(try Orig.oracle(a, s, '"'), aw.written());
    }
    var prng = std.Random.DefaultPrng.init(0x9707e);
    const r = prng.random();
    var buf: [24]u8 = undefined;
    for (0..2000) |_| {
        const n = r.uintLessThan(usize, buf.len);
        for (buf[0..n]) |*c| c.* = switch (r.uintLessThan(u8, 4)) {
            0 => '\'',
            1 => '"',
            else => r.int(u8),
        };
        var list = std.array_list.Managed(u8).init(a);
        try strLit(&list, buf[0..n]);
        try testing.expectEqualStrings(try Orig.oracle(a, buf[0..n], '\''), list.items);
        list.clearRetainingCapacity();
        try ident(&list, buf[0..n]);
        try testing.expectEqualStrings(try Orig.oracle(a, buf[0..n], '"'), list.items);
    }
}
