//! `basalt complete`: what Tab offers at an offset, as JSON, with the UTF-16
//! conversions a JavaScript editor's positions need.

const Catalog = @import("repl/catalog.zig").Catalog;
const Completer = @import("repl/catalog.zig").Completer;
const DeclStore = @import("entry.zig").DeclStore;
const Offer = @import("repl/catalog.zig").Offer;
const declOf = @import("entry.zig").declOf;
const loadSource = @import("args.zig").loadSource;
const loadExit = @import("args.zig").loadExit;
const nextVal = @import("args.zig").nextVal;
const splitStatements = @import("entry.zig").splitStatements;
const std = @import("std");
const suggestFor = @import("repl/catalog.zig").suggestFor;
const unknownOption = @import("args.zig").unknownOption;
const jsontext = @import("../exec/jsontext.zig");
const utf16 = @import("../format/utf16.zig");

/// What Tab would offer at byte `--pos`, as JSON. Connections are asked for their
/// tables and columns only under `--connect`, as that is a round trip to each.
pub fn cmdComplete(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writerStreaming(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writerStreaming(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var pos: ?usize = null;
    var connect = false;
    var units = false;
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
            units = true;
        } else if (try unknownOption(arg, "complete", stderr)) return 2;
    }
    const src = loadSource(a, "complete", args, stderr) catch |e| switch (e) {
        error.Usage, error.Unreadable => |le| return loadExit(le),
        else => return e,
    };
    const at = if (pos) |p| (if (units) utf16.unitToByte(src.text, p) else p) else src.text.len;
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
    try writeOfferIn(stdout, offer, at, src.text, units);
    try stdout.writeByte('\n');
    return 0;
}

/// The declarations a text makes, by its statements' first words, so a script still
/// being typed, which will not parse, still has its names in scope.
pub fn declareFrom(decls: *DeclStore, arena: std.mem.Allocator, text: []const u8) !void {
    for (try splitStatements(arena, text)) |st| {
        const id = declOf(st) orelse continue;
        try decls.put(id, st);
    }
}

/// With `units`, offsets count UTF-16 units of `text`, as a JavaScript editor does.
pub fn writeOfferIn(w: *std.Io.Writer, offer: Offer, cursor: usize, text: []const u8, units: bool) !void {
    var o = offer;
    var c = cursor;
    if (units) {
        o.start = utf16.byteToUnit(text, offer.start);
        c = utf16.byteToUnit(text, cursor);
    }
    return writeOffer(w, o, c);
}

pub fn writeOffer(w: *std.Io.Writer, offer: Offer, cursor: usize) !void {
    try w.print("{{\"start\":{d},\"end\":{d},\"items\":[", .{ offer.start, cursor });
    for (offer.items, 0..) |c, k| {
        if (k > 0) try w.writeByte(',');
        try w.writeAll("{\"text\":");
        try jsontext.string(w, c.text);
        try w.print(",\"kind\":\"{s}\"", .{@tagName(c.kind)});
        if (c.detail.len > 0) {
            try w.writeAll(",\"detail\":");
            try jsontext.string(w, c.detail);
        }
        try w.writeByte('}');
    }
    try w.writeAll("]}");
}

test "writeOfferIn: --utf16 offsets count UTF-16 units of the script" {
    const text = "SELECT 'é😀' AS x";
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeOfferIn(&w, .{ .start = 14 }, text.len, text, true);
    try std.testing.expectEqualStrings("{\"start\":11,\"end\":17,\"items\":[]}", w.buffered());
    w = std.Io.Writer.fixed(&buf);
    try writeOfferIn(&w, .{ .start = 14 }, text.len, text, false);
    try std.testing.expectEqualStrings("{\"start\":14,\"end\":20,\"items\":[]}", w.buffered());
}

test "writeOffer: a candidate that is not UTF-8 still makes a JSON offer" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const items = [_]@import("repl/complete.zig").Candidate{
        .{ .text = "caf\xe9", .kind = .column, .detail = "lat\xedn1" },
        .{ .text = "a\"b\\c\n\x01", .kind = .column },
    };
    var aw = std.Io.Writer.Allocating.init(a);
    try writeOffer(&aw.writer, .{ .items = &items }, 4);
    const Got = struct { start: usize, end: usize, items: []const struct { text: []const u8, kind: []const u8, detail: []const u8 = "" } };
    const got = try std.json.parseFromSliceLeaky(Got, a, aw.written(), .{});
    try std.testing.expectEqualStrings("caf\u{FFFD}", got.items[0].text);
    try std.testing.expectEqualStrings("lat\u{FFFD}n1", got.items[0].detail);
    try std.testing.expectEqualStrings("a\"b\\c\n\x01", got.items[1].text);
}
