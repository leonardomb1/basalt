//! Text, attribute values and element names written as XML 1.0, for every writer
//! that emits XML: XML files, Excel worksheets, the AAD token request.
//!
//! Text that is not valid UTF-8 is repaired first (`celltext.Utf8Runs`). A character
//! XML 1.0 cannot carry at all (a C0 control other than tab, LF and CR, U+FFFE,
//! U+FFFF) is U+FFFD in plain XML; OOXML instead spells it `_xHHHH_`, and then a
//! literal `_xHHHH_` is written `_x005F_xHHHH_` so it reads back as written. A CR is
//! `&#13;`, which a parser keeps rather than folding into a newline.
//!
//! An element name is encoded as SQL Server's `FOR XML` and .NET's
//! `XmlConvert.EncodeLocalName` do: each character a name cannot hold becomes
//! `_xHHHH_` (`Valor Total` is `Valor_x0020_Total`), and `decodeName` reverses it.

const std = @import("std");
const celltext = @import("../../exec/celltext.zig");
const token = @import("token.zig");

pub const Mode = struct { ooxml: bool = false, attr: bool = false };

/// `s` as element text, or with `attr` as an attribute value inside double quotes.
pub fn text(w: *std.Io.Writer, s: []const u8, mode: Mode) !void {
    if (std.unicode.utf8ValidateSlice(s)) return valid(w, s, mode);
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| try valid(w, t, mode);
}

fn valid(w: *std.Io.Writer, s: []const u8, mode: Mode) !void {
    var from: usize = 0;
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        var wide: usize = 0;
        var bad: ?u16 = null;
        const rep: []const u8 = switch (c) {
            '&' => "&amp;",
            '<' => "&lt;",
            '>' => "&gt;",
            '"' => if (mode.attr) "&quot;" else continue,
            '\r' => if (mode.ooxml) blk: {
                bad = c;
                break :blk "";
            } else "&#13;",
            '\t' => if (mode.attr) "&#9;" else continue,
            '\n' => if (mode.attr) "&#10;" else continue,
            '_' => if (mode.ooxml and token.ooxmlEscape(s[i..]) != null) "_x005F_" else continue,
            0...0x08, 0x0B, 0x0C, 0x0E...0x1F => blk: {
                bad = c;
                break :blk "";
            },
            0xEF => if (i + 2 < s.len and s[i + 1] == 0xBF and (s[i + 2] == 0xBE or s[i + 2] == 0xBF)) blk: {
                bad = if (s[i + 2] == 0xBE) 0xFFFE else 0xFFFF;
                wide = 2;
                break :blk "";
            } else continue,
            else => continue,
        };
        try w.writeAll(s[from..i]);
        if (bad) |b| {
            if (mode.ooxml) try w.print("_x{X:0>4}_", .{b}) else try w.writeAll(celltext.Utf8Runs.replacement);
        } else try w.writeAll(rep);
        i += wide;
        from = i + 1;
    }
    try w.writeAll(s[from..]);
}

/// `s` into a heap string as an attribute value, for a document built by format string.
pub fn attrAlloc(gpa: std.mem.Allocator, s: []const u8) ![]u8 {
    var aw = std.Io.Writer.Allocating.init(gpa);
    errdefer aw.deinit();
    try text(&aw.writer, s, .{ .attr = true });
    return aw.toOwnedSlice();
}

/// `s` as an element name: a character a name cannot hold, at its place, is `_xHHHH_`
/// (`_xHHHHHHHH_` past U+FFFF), and so is the `_` of a literal `_xHHHH_`.
pub fn name(w: *std.Io.Writer, s: []const u8) !void {
    var it = (std.unicode.Utf8View.init(s) catch return error.InvalidUtf8).iterator();
    var first = true;
    var at: usize = 0;
    while (it.nextCodepointSlice()) |cs| : (first = false) {
        const cp = std.unicode.utf8Decode(cs) catch unreachable;
        const literal = cp == '_' and decodedAt(s[at..]) != null;
        at += cs.len;
        if (!literal and (if (first) startChar(cp) else nameChar(cp))) {
            try w.writeAll(cs);
            continue;
        }
        if (cp > 0xFFFF) try w.print("_x{X:0>8}_", .{cp}) else try w.print("_x{X:0>4}_", .{cp});
    }
}

/// An element or attribute name with its `_xHHHH_` escapes decoded, as `name` wrote it.
pub fn decodeName(arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
    if (std.mem.indexOf(u8, raw, "_x") == null) return raw;
    var out = std.array_list.Managed(u8).init(arena);
    var i: usize = 0;
    while (i < raw.len) {
        if (decodedAt(raw[i..])) |d| {
            var buf: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(d.cp, &buf) catch {
                try out.append(raw[i]);
                i += 1;
                continue;
            };
            try out.appendSlice(buf[0..n]);
            i += d.len;
        } else {
            try out.append(raw[i]);
            i += 1;
        }
    }
    return out.items;
}

const Decoded = struct { cp: u21, len: usize };

fn decodedAt(s: []const u8) ?Decoded {
    if (s.len < 7 or s[0] != '_' or s[1] != 'x') return null;
    inline for (.{ 4, 8 }) |n| {
        if (s.len >= n + 3 and s[n + 2] == '_') {
            const hex = s[2 .. n + 2];
            const all_hex = for (hex) |h| {
                if (!std.ascii.isHex(h)) break false;
            } else true;
            if (all_hex) if (std.fmt.parseInt(u21, hex, 16)) |cp| return .{ .cp = cp, .len = n + 3 } else |_| {};
        }
    }
    return null;
}

fn startChar(cp: u21) bool {
    return switch (cp) {
        'A'...'Z', 'a'...'z', '_' => true,
        0xC0...0xD6, 0xD8...0xF6, 0xF8...0x2FF, 0x370...0x37D, 0x37F...0x1FFF, 0x200C...0x200D => true,
        0x2070...0x218F, 0x2C00...0x2FEF, 0x3001...0xD7FF, 0xF900...0xFDCF, 0xFDF0...0xFFFD, 0x10000...0xEFFFF => true,
        else => false,
    };
}

fn nameChar(cp: u21) bool {
    return startChar(cp) or switch (cp) {
        '-', '.', '0'...'9', 0xB7, 0x300...0x36F, 0x203F...0x2040 => true,
        else => false,
    };
}

test "text, attribute and OOXML escaping of markup, controls and invalid UTF-8" {
    var buf: [256]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try text(&w, "a<b>&\"c\"\r\n\t\x01_x0041_\xff", .{});
    try std.testing.expectEqualStrings("a&lt;b&gt;&amp;\"c\"&#13;\n\t\u{FFFD}_x0041_\u{FFFD}", w.buffered());
    w = std.Io.Writer.fixed(&buf);
    try text(&w, "\"x\"\t\n", .{ .attr = true });
    try std.testing.expectEqualStrings("&quot;x&quot;&#9;&#10;", w.buffered());
    w = std.Io.Writer.fixed(&buf);
    try text(&w, "\x01_x0041_\u{FFFF}\r", .{ .ooxml = true });
    try std.testing.expectEqualStrings("_x0001__x005F_x0041__xFFFF__x000D_", w.buffered());
}

test "names are encoded as FOR XML does and decode back" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_][2][]const u8{
        .{ "Valor Total", "Valor_x0020_Total" },
        .{ "1st", "_x0031_st" },
        .{ "a:b", "a_x003A_b" },
        .{ "ção-1.x", "ção-1.x" },
        .{ "_x0041_", "_x005F_x0041_" },
        .{ "sum(x)", "sum_x0028_x_x0029_" },
    };
    for (cases) |c| {
        var buf: [64]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        try name(&w, c[0]);
        try std.testing.expectEqualStrings(c[1], w.buffered());
        try std.testing.expectEqualStrings(c[0], try decodeName(a, c[1]));
    }
}
