//! `basalt complete`: what Tab offers at an offset, as JSON, with the UTF-16
//! conversions a JavaScript editor's positions need.

const Catalog = @import("catalog.zig").Catalog;
const Completer = @import("catalog.zig").Completer;
const DeclStore = @import("entry.zig").DeclStore;
const Offer = @import("catalog.zig").Offer;
const declOf = @import("entry.zig").declOf;
const loadSource = @import("args.zig").loadSource;
const nextVal = @import("args.zig").nextVal;
const splitStatements = @import("entry.zig").splitStatements;
const std = @import("std");
const suggestFor = @import("catalog.zig").suggestFor;
const unknownOption = @import("args.zig").unknownOption;

/// What Tab would offer at byte `--pos`, as JSON. Connections are asked for their
/// tables and columns only under `--connect`, as that is a round trip to each.
pub fn cmdComplete(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writer(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writer(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var pos: ?usize = null;
    var connect = false;
    var utf16 = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--command")) {
            i += 1;
        } else if (std.mem.eql(u8, arg, "--pos")) {
            const v = (try nextVal(args, &i, "--pos", stderr)) orelse return 2;
            pos = std.fmt.parseInt(usize, v, 10) catch {
                try stderr.print("error: invalid --pos `{s}`\n", .{v});
                return 2;
            };
        } else if (std.mem.eql(u8, arg, "--connect")) {
            connect = true;
        } else if (std.mem.eql(u8, arg, "--utf16")) {
            utf16 = true;
        } else if (try unknownOption(arg, "complete", stderr)) return 2;
    }
    const src = (try loadSource(a, "complete", args, stderr)) orelse return 1;
    const at = if (pos) |p| (if (utf16) utf16ToByte(src.text, p) else p) else src.text.len;
    if (at > src.text.len) {
        try stderr.print("error: --pos {d} is past the script's {d} bytes\n", .{ at, src.text.len });
        return 2;
    }

    var decls = DeclStore.init(alloc);
    defer decls.deinit();
    try declareFrom(&decls, a, src.text);
    var catalog = Catalog.init(alloc);
    defer catalog.deinit();
    const cx = Completer{ .gpa = alloc, .decls = &decls, .catalog = &catalog, .connect = connect };
    const offer = try suggestFor(a, &cx, src.text, at);
    try writeOfferIn(stdout, offer, at, src.text, utf16);
    try stdout.writeByte('\n');
    return 0;
}

/// The declarations a text makes, by its statements' first words, so a script still
/// being typed, which will not parse, still has its names in scope.
pub fn declareFrom(decls: *DeclStore, arena: std.mem.Allocator, text: []const u8) !void {
    for (try splitStatements(arena, text)) |st| {
        const id = declOf(st) orelse continue;
        if (id.kind == .endpoint) continue;
        try decls.put(id, st);
    }
}

/// Byte offset of UTF-16 offset `u`, a JavaScript editor's cursor. Inside a surrogate
/// pair or past the end clamps back to a boundary; non-UTF-8 bytes count one unit.
pub fn utf16ToByte(text: []const u8, u: usize) usize {
    var i: usize = 0;
    var units: usize = 0;
    while (i < text.len) {
        const n = std.unicode.utf8ByteSequenceLength(text[i]) catch 1;
        const cp_units: usize = if (n == 4) 2 else 1;
        if (units + cp_units > u) break;
        units += cp_units;
        i = @min(text.len, i + n);
    }
    return i;
}

pub fn byteToUtf16(text: []const u8, b: usize) usize {
    var i: usize = 0;
    var units: usize = 0;
    while (i < @min(b, text.len)) {
        const n = std.unicode.utf8ByteSequenceLength(text[i]) catch 1;
        units += if (n == 4) 2 else 1;
        i += n;
    }
    return units;
}

/// With `utf16`, offsets count UTF-16 units of `text`, as a JavaScript editor does.
pub fn writeOfferIn(w: *std.Io.Writer, offer: Offer, cursor: usize, text: []const u8, utf16: bool) !void {
    var o = offer;
    var c = cursor;
    if (utf16) {
        o.start = byteToUtf16(text, offer.start);
        c = byteToUtf16(text, cursor);
    }
    return writeOffer(w, o, c);
}

pub fn writeOffer(w: *std.Io.Writer, offer: Offer, cursor: usize) !void {
    try w.print("{{\"start\":{d},\"end\":{d},\"items\":[", .{ offer.start, cursor });
    for (offer.items, 0..) |c, k| {
        if (k > 0) try w.writeByte(',');
        try w.writeAll("{\"text\":");
        try std.json.Stringify.encodeJsonString(c.text, .{}, w);
        try w.print(",\"kind\":\"{s}\"", .{@tagName(c.kind)});
        if (c.detail.len > 0) {
            try w.writeAll(",\"detail\":");
            try std.json.Stringify.encodeJsonString(c.detail, .{}, w);
        }
        try w.writeByte('}');
    }
    try w.writeAll("]}");
}

test "utf16 offsets: a JavaScript editor's positions map to bytes and back" {
    const text = "SELECT 'é😀' AS x";
    try std.testing.expectEqual(@as(usize, 8), utf16ToByte(text, 8));
    try std.testing.expectEqual(@as(usize, 10), utf16ToByte(text, 9));
    try std.testing.expectEqual(@as(usize, 10), utf16ToByte(text, 10));
    try std.testing.expectEqual(@as(usize, 14), utf16ToByte(text, 11));
    try std.testing.expectEqual(text.len, utf16ToByte(text, 1000));
    for ([_]usize{ 0, 8, 10, 14, text.len }) |b| try std.testing.expectEqual(b, utf16ToByte(text, byteToUtf16(text, b)));
    try std.testing.expectEqual(@as(usize, 17), byteToUtf16(text, text.len));
}
