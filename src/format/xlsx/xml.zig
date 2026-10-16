//! A streaming XML tokenizer, just what reading an `.xlsx` needs.
//!
//! Tokens come off a `std.Io.Reader` one at a time and are valid until the next
//! call, so a worksheet streams through in a buffer the size of its longest tag or
//! text run (at most `max_token`) rather than its whole body. Names are given
//! without their namespace prefix: the .NET OpenXML SDK writes `<x:row>` where Excel
//! writes `<row>`, and a reader matching the prefix found no rows at all in such a
//! file. Comments, processing instructions and DOCTYPEs are skipped; CDATA is text.
//! Tag attributes and text are returned raw; entities are decoded with `decode`.

const std = @import("std");

pub const Error = error{ BadXml, XmlTokenTooLong, OutOfMemory, ReadFailed };

const max_token = 64 << 20;

pub const Tag = struct {
    name: []const u8,
    attrs: []const u8,
    self_closing: bool,

    /// The raw value of the attribute whose local name is `want`, or null. `r:id`
    /// answers to `id`.
    pub fn attr(self: Tag, want: []const u8) ?[]const u8 {
        var i: usize = 0;
        const s = self.attrs;
        while (i < s.len) {
            while (i < s.len and isSpace(s[i])) i += 1;
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
            const val = s[vs..i];
            i += 1;
            if (std.mem.eql(u8, localName(full), want)) return val;
        }
        return null;
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

    pub fn init(gpa: std.mem.Allocator, r: *std.Io.Reader) Tokenizer {
        return .{ .r = r, .buf = .init(gpa) };
    }

    pub fn deinit(self: *Tokenizer) void {
        self.buf.deinit();
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
        if (n == 0) {
            self.eof = true;
            return false;
        }
        self.buf.items.len += n;
        return true;
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
            const cp = (if (ent[1] == 'x' or ent[1] == 'X')
                std.fmt.parseInt(u21, ent[2..], 16)
            else
                std.fmt.parseInt(u21, ent[1..], 10)) catch return error.BadXml;
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
        if (j + 7 <= s.len and s[j] == '_' and s[j + 1] == 'x' and s[j + 6] == '_') {
            if (std.fmt.parseInt(u21, s[j + 2 .. j + 6], 16)) |cp| {
                const n = std.unicode.utf8Encode(cp, &tmp) catch 0;
                if (n > 0 and n <= 7) {
                    @memcpy(s[w..][0..n], tmp[0..n]);
                    w += n;
                    j += 7;
                    continue;
                }
            } else |_| {}
        }
        s[w] = s[j];
        w += 1;
        j += 1;
    }
    out.shrinkRetainingCapacity(start + w);
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
