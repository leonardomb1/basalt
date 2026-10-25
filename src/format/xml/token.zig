//! A streaming XML tokenizer: worksheets, object-store replies and XML files.
//!
//! Tokens come off a `std.Io.Reader` one at a time and are valid until the next
//! call, so a worksheet streams through in a buffer the size of its longest tag or
//! text run (at most `max_token`) rather than its whole body. Names are given
//! without their namespace prefix: the .NET OpenXML SDK writes `<x:row>` where Excel
//! writes `<row>`, and a reader matching the prefix found no rows at all in such a
//! file. Comments, processing instructions and DOCTYPEs are skipped; CDATA is text.
//! Tag attributes and text are returned raw; entities are decoded with `decode`.

const std = @import("std");
const hex = @import("../hex.zig");

pub const Error = error{ BadXml, XmlTokenTooLong, OutOfMemory, ReadFailed };

const max_token = 64 << 20;

pub const Tag = struct {
    name: []const u8,
    attrs: []const u8,
    self_closing: bool,

    /// The raw value of the attribute whose local name is `want`, or null. `r:id`
    /// answers to `id`.
    pub fn attr(self: Tag, want: []const u8) ?[]const u8 {
        var it = self.iterator();
        while (it.next()) |a| if (std.mem.eql(u8, localName(a.name), want)) return a.value;
        return null;
    }

    pub fn iterator(self: Tag) Attrs {
        return .{ .s = self.attrs };
    }
};

/// A tag's attributes in order, each name as written (prefix and all) and its value
/// raw; it stops at the first malformed one.
pub const Attrs = struct {
    s: []const u8,
    i: usize = 0,

    pub const Attr = struct { name: []const u8, value: []const u8 };

    pub fn next(self: *Attrs) ?Attr {
        const s = self.s;
        var i = self.i;
        while (i < s.len and isSpace(s[i])) i += 1;
        if (i >= s.len) return null;
        const ns = i;
        while (i < s.len and s[i] != '=' and !isSpace(s[i])) i += 1;
        const full = s[ns..i];
        while (i < s.len and isSpace(s[i])) i += 1;
        if (i >= s.len or s[i] != '=') return null;
        i += 1;
        while (i < s.len and isSpace(s[i])) i += 1;
        if (i >= s.len) return null;
        const q = s[i];
        if (q != '"' and q != '\'') return null;
        i += 1;
        const vs = i;
        while (i < s.len and s[i] != q) i += 1;
        if (i >= s.len) return null;
        self.i = i + 1;
        return .{ .name = full, .value = s[vs..i] };
    }
};

pub const Token = union(enum) {
    open: Tag,
    close: []const u8,
    text: []const u8,
    cdata: []const u8,
};

pub const Tokenizer = struct {
    r: *std.Io.Reader,
    buf: std.array_list.Managed(u8),
    lo: usize = 0,
    eof: bool = false,

    borrowed: bool = false,

    pub fn init(gpa: std.mem.Allocator, r: *std.Io.Reader) Tokenizer {
        return .{ .r = r, .buf = .init(gpa) };
    }

    /// Tokens of a whole document already in memory, read in place: nothing is
    /// copied or allocated, and tokens stay valid as long as `doc`.
    pub fn initSlice(doc: []const u8) Tokenizer {
        const buf = std.array_list.Managed(u8){ .items = @constCast(doc), .capacity = doc.len, .allocator = undefined };
        return .{ .r = undefined, .buf = buf, .eof = true, .borrowed = true };
    }

    pub fn deinit(self: *Tokenizer) void {
        if (!self.borrowed) self.buf.deinit();
    }

    fn fill(self: *Tokenizer) Error!bool {
        if (self.eof) return false;
        if (self.lo > 0 and self.lo * 2 >= self.buf.items.len) {
            const rest = self.buf.items.len - self.lo;
            std.mem.copyForwards(u8, self.buf.items[0..rest], self.buf.items[self.lo..]);
            self.buf.shrinkRetainingCapacity(rest);
            self.lo = 0;
        }
        if (self.buf.items.len - self.lo > max_token) return error.XmlTokenTooLong;
        try self.buf.ensureUnusedCapacity(64 << 10);
        const dst = self.buf.unusedCapacitySlice();
        const n = self.r.readSliceShort(dst) catch return error.ReadFailed;
        self.buf.items.len += n;
        if (n < dst.len) self.eof = true;
        return n > 0;
    }

    /// The next token, or null at the end of the document. Trailing text is read after
    /// `fill`, which may move what `have` pointed at.
    pub fn next(self: *Tokenizer) Error!?Token {
        while (true) {
            const have = self.buf.items[self.lo..];
            if (have.len == 0) {
                if (!try self.fill()) return null;
                continue;
            }
            if (have[0] != '<') {
                const lt = std.mem.indexOfScalar(u8, have, '<') orelse {
                    if (try self.fill()) continue;
                    const rest = self.buf.items[self.lo..];
                    self.lo = self.buf.items.len;
                    return .{ .text = rest };
                };
                self.lo += lt;
                return .{ .text = have[0..lt] };
            }
            if (try self.special(have)) |t| return t;
            if (have.len >= 2 and (have[1] == '?' or have[1] == '!')) continue;
            const gt = tagEnd(have) orelse {
                if (try self.fill()) continue;
                return error.BadXml;
            };
            const body = have[1..gt];
            self.lo += gt + 1;
            if (body.len > 0 and body[0] == '/') return .{ .close = localName(std.mem.trim(u8, body[1..], " \t\r\n")) };
            const self_closing = body.len > 0 and body[body.len - 1] == '/';
            const inner = if (self_closing) body[0 .. body.len - 1] else body;
            var ne: usize = 0;
            while (ne < inner.len and !isSpace(inner[ne])) ne += 1;
            return .{ .open = .{ .name = localName(inner[0..ne]), .attrs = inner[ne..], .self_closing = self_closing } };
        }
    }

    /// Consumes and skips `<?…?>`, `<!--…-->` and `<!DOCTYPE …>` (null with `lo`
    /// advanced, so `next` loops); returns `<![CDATA[…]]>`.
    fn special(self: *Tokenizer, have: []const u8) Error!?Token {
        if (have.len < 2 or (have[1] != '?' and have[1] != '!')) return null;
        const Close = struct { open: []const u8, close: []const u8 };
        const kinds = [_]Close{
            .{ .open = "<?", .close = "?>" },
            .{ .open = "<!--", .close = "-->" },
            .{ .open = "<![CDATA[", .close = "]]>" },
        };
        while (true) {
            const h = self.buf.items[self.lo..];
            for (kinds) |k| {
                if (h.len < k.open.len) break;
                if (!std.mem.startsWith(u8, h, k.open)) continue;
                const end = std.mem.indexOfPos(u8, h, k.open.len, k.close) orelse break;
                self.lo += end + k.close.len;
                if (std.mem.eql(u8, k.open, "<![CDATA[")) return .{ .cdata = h[k.open.len..end] };
                return null;
            } else {
                if (h.len >= 2 and h[1] == '!' and !std.mem.startsWith(u8, h, "<!-") and !std.mem.startsWith(u8, h, "<![")) {
                    const gt = std.mem.indexOfScalar(u8, h, '>') orelse {
                        if (try self.fill()) continue;
                        return error.BadXml;
                    };
                    self.lo += gt + 1;
                    return null;
                }
            }
            if (!try self.fill()) return error.BadXml;
        }
    }

    /// Skip to the end of the element just opened by `tag`, children included.
    pub fn skip(self: *Tokenizer, tag: Tag) Error!void {
        if (tag.self_closing) return;
        var depth: usize = 1;
        while (try self.next()) |t| switch (t) {
            .open => |o| {
                if (!o.self_closing) depth += 1;
            },
            .close => {
                depth -= 1;
                if (depth == 0) return;
            },
            else => {},
        };
        return error.BadXml;
    }
};

/// The text of each element whose local name is `name`, in document order and
/// entities decoded: a reply already in memory, such as an object store's listing.
/// An empty element is the empty text, and an element a cut-off reply leaves open
/// is not counted.
pub fn elementTexts(arena: std.mem.Allocator, doc: []const u8, name: []const u8) Error![][]const u8 {
    var tz = Tokenizer.initSlice(doc);
    var out = std.ArrayListUnmanaged([]const u8).empty;
    var text = std.array_list.Managed(u8).init(arena);
    var inside = false;
    while (tz.next() catch |e| switch (e) {
        error.BadXml => null,
        else => return e,
    }) |t| switch (t) {
        .open => |o| if (!inside and std.mem.eql(u8, o.name, name)) {
            if (o.self_closing) try out.append(arena, "") else inside = true;
        },
        .text => |raw| if (inside) try decodeInto(&text, raw, false),
        .cdata => |raw| if (inside) try text.appendSlice(raw),
        .close => |c| if (inside and std.mem.eql(u8, c, name)) {
            inside = false;
            try out.append(arena, try arena.dupe(u8, text.items));
            text.clearRetainingCapacity();
        },
    };
    return out.items;
}

/// The undecoded text of the first element whose local name is `name`, for a value
/// compared as written, such as an error code, where no allocator is at hand.
pub fn firstRawText(doc: []const u8, name: []const u8) ?[]const u8 {
    var tz = Tokenizer.initSlice(doc);
    while (tz.next() catch return null) |t| switch (t) {
        .open => |o| if (std.mem.eql(u8, o.name, name)) {
            if (o.self_closing) return "";
            const v = (tz.next() catch return null) orelse return null;
            return switch (v) {
                .text, .cdata => |raw| raw,
                .close => "",
                .open => null,
            };
        },
        else => {},
    };
    return null;
}

/// The `>` closing a tag that starts at `s[0]`, skipping any inside quoted
/// attribute values, where some writers leave a bare `>`.
fn tagEnd(s: []const u8) ?usize {
    var q: u8 = 0;
    for (s, 0..) |c, i| {
        if (q != 0) {
            if (c == q) q = 0;
        } else if (c == '"' or c == '\'') {
            q = c;
        } else if (c == '>') return i;
    }
    return null;
}

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

pub fn localName(full: []const u8) []const u8 {
    const colon = std.mem.indexOfScalar(u8, full, ':') orelse return full;
    return full[colon + 1 ..];
}

/// Append `raw` with XML entities decoded and, with `ooxml`, OOXML `_xHHHH_` escapes
/// after them (Excel writes a carriage return as `_x000D_`, an underscore as `_x005F_`).
pub fn decodeInto(out: *std.array_list.Managed(u8), raw: []const u8, ooxml: bool) Error!void {
    const start = out.items.len;
    var i: usize = 0;
    while (i < raw.len) {
        const c = raw[i];
        if (c != '&') {
            try out.append(c);
            i += 1;
            continue;
        }
        const semi = std.mem.indexOfScalarPos(u8, raw, i, ';') orelse return error.BadXml;
        const ent = raw[i + 1 .. semi];
        i = semi + 1;
        if (ent.len > 1 and ent[0] == '#') {
            const cp = (if (ent[1] == 'x' or ent[1] == 'X') hex.int(u21, ent[2..]) else decimalRef(ent[1..])) orelse return error.BadXml;
            try appendCodepoint(out, cp);
        } else if (std.mem.eql(u8, ent, "amp")) {
            try out.append('&');
        } else if (std.mem.eql(u8, ent, "lt")) {
            try out.append('<');
        } else if (std.mem.eql(u8, ent, "gt")) {
            try out.append('>');
        } else if (std.mem.eql(u8, ent, "quot")) {
            try out.append('"');
        } else if (std.mem.eql(u8, ent, "apos")) {
            try out.append('\'');
        } else return error.BadXml;
    }
    if (ooxml) unescapeOoxml(out, start);
}

/// OOXML `_xHHHH_` escapes over `out[start..]`, in place, for text whose XML
/// entities are already decoded.
pub fn unescapeOoxml(out: *std.array_list.Managed(u8), start: usize) void {
    if (std.mem.indexOf(u8, out.items[start..], "_x") == null) return;
    const s = out.items[start..];
    var w: usize = 0;
    var j: usize = 0;
    var tmp: [4]u8 = undefined;
    while (j < s.len) {
        if (ooxmlEscape(s[j..])) |cp| {
            const n = std.unicode.utf8Encode(cp, &tmp) catch 0;
            if (n > 0 and n <= 7) {
                @memcpy(s[w..][0..n], tmp[0..n]);
                w += n;
                j += 7;
                continue;
            }
        }
        s[w] = s[j];
        w += 1;
        j += 1;
    }
    out.shrinkRetainingCapacity(start + w);
}

/// The code point of the `_xHHHH_` escape `s` starts with, or null. A writer escapes
/// the `_` of any text this matches, so no literal text decodes as an escape.
pub fn ooxmlEscape(s: []const u8) ?u21 {
    if (s.len < 7 or s[0] != '_' or s[1] != 'x' or s[6] != '_') return null;
    return hex.int(u21, s[2..6]);
}

/// The code point a decimal character reference spells, digits only; null otherwise.
fn decimalRef(s: []const u8) ?u21 {
    if (s.len == 0) return null;
    for (s) |c| if (!std.ascii.isDigit(c)) return null;
    return std.fmt.parseInt(u21, s, 10) catch null;
}

fn appendCodepoint(out: *std.array_list.Managed(u8), cp: u21) Error!void {
    var tmp: [4]u8 = undefined;
    const n = std.unicode.utf8Encode(cp, &tmp) catch return error.BadXml;
    try out.appendSlice(tmp[0..n]);
}

test "tokenizer: prefixes, attributes, self-closing, skipped declarations, CDATA" {
    const doc =
        \\<?xml version="1.0"?><!-- c --><x:sheet xmlns:x="u"><x:row r="2"><x:c r="B2" t="s"/><v>a &amp; b</v></x:row><![CDATA[<raw>]]><t a='x>y'>z</t></x:sheet>
    ;
    var fr = std.Io.Reader.fixed(doc);
    var tk = Tokenizer.init(std.testing.allocator, &fr);
    defer tk.deinit();
    const sheet = (try tk.next()).?.open;
    try std.testing.expectEqualStrings("sheet", sheet.name);
    const row = (try tk.next()).?.open;
    try std.testing.expectEqualStrings("row", row.name);
    try std.testing.expectEqualStrings("2", row.attr("r").?);
    const c = (try tk.next()).?.open;
    try std.testing.expect(c.self_closing);
    try std.testing.expectEqualStrings("s", c.attr("t").?);
    try std.testing.expect(c.attr("missing") == null);
    try std.testing.expectEqualStrings("v", (try tk.next()).?.open.name);
    try std.testing.expectEqualStrings("a &amp; b", (try tk.next()).?.text);
    try std.testing.expectEqualStrings("v", (try tk.next()).?.close);
    try std.testing.expectEqualStrings("row", (try tk.next()).?.close);
    try std.testing.expectEqualStrings("<raw>", (try tk.next()).?.cdata);
    const t = (try tk.next()).?.open;
    try std.testing.expectEqualStrings("x>y", t.attr("a").?);
}

test "decode: entities, then OOXML _xHHHH_ escapes" {
    var out = std.array_list.Managed(u8).init(std.testing.allocator);
    defer out.deinit();
    try decodeInto(&out, "a &lt;b&gt; &#233;&#x00E3; line_x000D_two _x005F_x0041_", true);
    try std.testing.expectEqualStrings("a <b> éã line\rtwo _x0041_", out.items);
    out.clearRetainingCapacity();
    try decodeInto(&out, "keep_x000D_", false);
    try std.testing.expectEqualStrings("keep_x000D_", out.items);
    try std.testing.expectError(error.BadXml, decodeInto(&out, "bad &nope;", false));
}

test "decode: a hex character reference takes hex digits only, with no sign or `_`" {
    var out = std.array_list.Managed(u8).init(std.testing.allocator);
    defer out.deinit();
    try decodeInto(&out, "&#x41;&#X0042;&#x10FFFF;", false);
    try std.testing.expectEqualStrings("AB\u{10FFFF}", out.items);
    for ([_][]const u8{ "&#x4_1;", "&#x+41;", "&#x-0;", "&#x;", "&#x110000;", "&#xD800;", "&#x0x41;" }) |bad| {
        errdefer std.debug.print("in `{s}`\n", .{bad});
        try std.testing.expectError(error.BadXml, decodeInto(&out, bad, false));
    }
}

test "decode: a character reference is digits only, no sign or underscore" {
    var out = std.array_list.Managed(u8).init(std.testing.allocator);
    defer out.deinit();
    for ([_][]const u8{ "&#x4_1;", "&#x+41;", "&#x-41;", "&#x;", "&#6_5;", "&#+65;", "&#-65;", "&#;", "&#x 41;", "&#x110000000;" }) |bad| {
        out.clearRetainingCapacity();
        try std.testing.expectError(error.BadXml, decodeInto(&out, bad, false));
    }
    out.clearRetainingCapacity();
    try decodeInto(&out, "&#x41;&#X0062;&#99;&#0000100;", false);
    try std.testing.expectEqualStrings("Abcd", out.items);
}
