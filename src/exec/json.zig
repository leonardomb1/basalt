//! JSON read in place. `json_get`, `JSON_EACH` and the JSON array functions
//! parse one document per row, and `std.json` built a whole tree for each —
//! a hash map per object, a list per array, a copy of every string — to hand
//! back one value or one array's elements. This reads the text where it lies,
//! in the On-Demand style of simdjson and sonic-rs: `validate` checks the
//! document without allocating, then `path` and `Elements` locate values as
//! slices of the original text, and only what is returned is decoded.
//!
//! What it accepts and refuses is `std.json`'s exactly — a cell that is not
//! JSON was an error and still is, duplicate keys included — and a document it
//! is not sure of (nesting deeper than `max_depth`, more open keys than
//! `max_keys`, a long escaped key) is handed to `std.json` to decide, so those
//! cases are exact by construction. The values it returns are `std.json`'s too
//! (`eval.jsonToValue`): a test holds the two against each other.

const std = @import("std");

pub const Error = error{ InvalidJson, OutOfMemory };

/// Nesting the validator follows itself; a deeper document goes to `std.json`.
const max_depth = 128;
/// Keys of the open objects compared for duplicates; more go to `std.json`.
const max_keys = 256;
/// The longest escaped key decoded on the stack to compare; longer goes to `std.json`.
const max_key_bytes = 256;

/// Whether `text` is one JSON document, as `std.json` decides.
pub fn validate(arena: std.mem.Allocator, text: []const u8) Error!void {
    var v = Validator{ .t = text };
    v.run() catch |e| switch (e) {
        error.Invalid => return error.InvalidJson,
        error.Fallback => {
            _ = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch |e2|
                return if (e2 == error.OutOfMemory) error.OutOfMemory else error.InvalidJson;
        },
    };
}

const Validator = struct {
    t: []const u8,
    i: usize = 0,
    depth: usize = 0,
    keys: [max_keys][]const u8 = undefined,
    nkeys: usize = 0,

    const E = error{ Invalid, Fallback };

    fn run(v: *Validator) E!void {
        v.ws();
        try v.value();
        v.ws();
        if (v.i != v.t.len) return error.Invalid;
    }

    fn ws(v: *Validator) void {
        while (v.i < v.t.len) switch (v.t[v.i]) {
            ' ', '\t', '\n', '\r' => v.i += 1,
            else => return,
        };
    }

    fn value(v: *Validator) E!void {
        if (v.i >= v.t.len) return error.Invalid;
        switch (v.t[v.i]) {
            '{' => try v.object(),
            '[' => try v.array(),
            '"' => _ = try v.string(),
            't' => try v.literal("true"),
            'f' => try v.literal("false"),
            'n' => try v.literal("null"),
            '-', '0'...'9' => try v.number(),
            else => return error.Invalid,
        }
    }

    fn literal(v: *Validator, word: []const u8) E!void {
        if (!std.mem.startsWith(u8, v.t[v.i..], word)) return error.Invalid;
        v.i += word.len;
    }

    /// `-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?` — what follows it is the
    /// enclosing value's to judge, so `01` and `1a` fail there.
    fn number(v: *Validator) E!void {
        const t = v.t;
        var i = v.i;
        if (t[i] == '-') i += 1;
        if (i >= t.len) return error.Invalid;
        if (t[i] == '0') {
            i += 1;
        } else if (t[i] >= '1' and t[i] <= '9') {
            while (i < t.len and isDigit(t[i])) i += 1;
        } else return error.Invalid;
        if (i < t.len and t[i] == '.') {
            i += 1;
            const s = i;
            while (i < t.len and isDigit(t[i])) i += 1;
            if (i == s) return error.Invalid;
        }
        if (i < t.len and (t[i] == 'e' or t[i] == 'E')) {
            i += 1;
            if (i < t.len and (t[i] == '+' or t[i] == '-')) i += 1;
            const s = i;
            while (i < t.len and isDigit(t[i])) i += 1;
            if (i == s) return error.Invalid;
        }
        v.i = i;
    }

    /// A string at `v.i`; returns its raw content, between the quotes.
    fn string(v: *Validator) E![]const u8 {
        const t = v.t;
        v.i += 1;
        const start = v.i;
        var high = false;
        while (true) {
            if (v.i >= t.len) return error.Invalid;
            switch (t[v.i]) {
                '"' => break,
                '\\' => {
                    v.i += 1;
                    if (v.i >= t.len) return error.Invalid;
                    switch (t[v.i]) {
                        '"', '\\', '/', 'b', 'f', 'n', 'r', 't' => v.i += 1,
                        'u' => {
                            const cp = hex4(t, v.i + 1) orelse return error.Invalid;
                            v.i += 5;
                            if (cp >= 0xD800 and cp <= 0xDBFF) {
                                if (v.i + 6 > t.len or t[v.i] != '\\' or t[v.i + 1] != 'u') return error.Invalid;
                                const lo = hex4(t, v.i + 2) orelse return error.Invalid;
                                if (lo < 0xDC00 or lo > 0xDFFF) return error.Invalid;
                                v.i += 6;
                            } else if (cp >= 0xDC00 and cp <= 0xDFFF) return error.Invalid;
                        },
                        else => return error.Invalid,
                    }
                },
                0...0x1F => return error.Invalid,
                0x80...0xFF => {
                    high = true;
                    v.i += 1;
                },
                else => v.i += 1,
            }
        }
        const raw = t[start..v.i];
        v.i += 1;
        if (high and !std.unicode.utf8ValidateSlice(raw)) return error.Invalid;
        return raw;
    }

    fn object(v: *Validator) E!void {
        if (v.depth == max_depth) return error.Fallback;
        v.depth += 1;
        defer v.depth -= 1;
        const t = v.t;
        v.i += 1;
        const base = v.nkeys;
        defer v.nkeys = base;
        v.ws();
        if (v.i < t.len and t[v.i] == '}') {
            v.i += 1;
            return;
        }
        while (true) {
            v.ws();
            if (v.i >= t.len or t[v.i] != '"') return error.Invalid;
            const key = try v.string();
            // `std.json` refuses a document with a key repeated in one object
            for (v.keys[base..v.nkeys]) |k| if (try keysEqual(k, key)) return error.Invalid;
            if (v.nkeys == max_keys) return error.Fallback;
            v.keys[v.nkeys] = key;
            v.nkeys += 1;
            v.ws();
            if (v.i >= t.len or t[v.i] != ':') return error.Invalid;
            v.i += 1;
            v.ws();
            try v.value();
            v.ws();
            if (v.i >= t.len) return error.Invalid;
            switch (t[v.i]) {
                ',' => v.i += 1,
                '}' => {
                    v.i += 1;
                    return;
                },
                else => return error.Invalid,
            }
        }
    }

    fn array(v: *Validator) E!void {
        if (v.depth == max_depth) return error.Fallback;
        v.depth += 1;
        defer v.depth -= 1;
        const t = v.t;
        v.i += 1;
        v.ws();
        if (v.i < t.len and t[v.i] == ']') {
            v.i += 1;
            return;
        }
        while (true) {
            v.ws();
            try v.value();
            v.ws();
            if (v.i >= t.len) return error.Invalid;
            switch (t[v.i]) {
                ',' => v.i += 1,
                ']' => {
                    v.i += 1;
                    return;
                },
                else => return error.Invalid,
            }
        }
    }

    /// Two raw keys naming the same key once decoded (`"a"` and `"a"`).
    fn keysEqual(a: []const u8, b: []const u8) E!bool {
        if (std.mem.eql(u8, a, b)) return true;
        const ea = std.mem.indexOfScalar(u8, a, '\\') != null;
        const eb = std.mem.indexOfScalar(u8, b, '\\') != null;
        if (!ea and !eb) return false;
        if (a.len > max_key_bytes or b.len > max_key_bytes) return error.Fallback;
        var ba: [max_key_bytes]u8 = undefined;
        var bb: [max_key_bytes]u8 = undefined;
        return std.mem.eql(u8, decodeInto(&ba, a), decodeInto(&bb, b));
    }
};

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

/// Four hex digits exactly — not `std.fmt.parseInt`, which takes a sign and `_`.
fn hex4(t: []const u8, at: usize) ?u21 {
    if (at + 4 > t.len) return null;
    var v: u21 = 0;
    for (t[at..][0..4]) |c| {
        const d: u21 = switch (c) {
            '0'...'9' => c - '0',
            'a'...'f' => c - 'a' + 10,
            'A'...'F' => c - 'A' + 10,
            else => return null,
        };
        v = v * 16 + d;
    }
    return v;
}

// --- walking a validated document --------------------------------------------
// Everything below assumes `validate` passed: it does not check the text again.

fn skipWs(t: []const u8, i_in: usize) usize {
    var i = i_in;
    while (i < t.len) switch (t[i]) {
        ' ', '\t', '\n', '\r' => i += 1,
        else => return i,
    };
    return i;
}

/// Just past the string whose opening quote is at `i`.
fn stringEnd(t: []const u8, i: usize) usize {
    var j = i + 1;
    while (true) {
        j = std.mem.indexOfAnyPos(u8, t, j, "\"\\").?;
        if (t[j] == '"') return j + 1;
        j += 2;
    }
}

/// Just past the value starting at `i`. A container is skipped by counting its
/// brackets outside strings, the way sonic-rs and jiter skip one.
fn valueEnd(t: []const u8, i: usize) usize {
    switch (t[i]) {
        '"' => return stringEnd(t, i),
        '{', '[' => {
            var depth: usize = 0;
            var j = i;
            while (true) {
                j = std.mem.indexOfAnyPos(u8, t, j, "\"{}[]").?;
                switch (t[j]) {
                    '"' => {
                        j = stringEnd(t, j);
                        continue;
                    },
                    '{', '[' => depth += 1,
                    else => {
                        depth -= 1;
                        if (depth == 0) return j + 1;
                    },
                }
                j += 1;
            }
        },
        else => {
            var j = i;
            while (j < t.len) switch (t[j]) {
                ',', ']', '}', ' ', '\t', '\n', '\r' => break,
                else => j += 1,
            };
            return j;
        },
    }
}

/// The value of member `key` of the object at `i`, keys compared decoded.
fn member(arena: std.mem.Allocator, t: []const u8, i: usize, key: []const u8) Error!?usize {
    var j = skipWs(t, i + 1);
    if (t[j] == '}') return null;
    while (true) {
        const ke = stringEnd(t, j);
        const raw = t[j + 1 .. ke - 1];
        j = skipWs(t, skipWs(t, ke) + 1);
        const hit = if (std.mem.indexOfScalar(u8, raw, '\\') == null)
            std.mem.eql(u8, raw, key)
        else
            std.mem.eql(u8, try decodeString(arena, raw), key);
        if (hit) return j;
        j = skipWs(t, valueEnd(t, j));
        if (t[j] == '}') return null;
        j = skipWs(t, j + 1);
    }
}

/// Element `n` of the array at `i`.
fn element(t: []const u8, i: usize, n: usize) ?usize {
    var it = Elements.at(t, i);
    var k: usize = 0;
    while (it.nextStart()) |s| : (k += 1) {
        if (k == n) return s;
        it.i = it.after(s);
    }
    return null;
}

/// The value `path` names in the validated document `t` — `a.b`, `a[0].b` or
/// `a.0.b`, a leading `$` allowed — as a slice of `t`; null when a key is
/// missing, an index is past the end, or a step lands on a scalar. The same
/// walk as `eval.jsonPath` over a `std.json` tree.
pub fn path(arena: std.mem.Allocator, t: []const u8, p_in: []const u8) Error!?[]const u8 {
    var p = p_in;
    if (std.mem.startsWith(u8, p, "$")) p = p[1..];
    var cur = skipWs(t, 0);
    var steps = std.mem.tokenizeAny(u8, p, ".[]");
    while (steps.next()) |step| {
        cur = switch (t[cur]) {
            '{' => (try member(arena, t, cur, step)) orelse return null,
            '[' => element(t, cur, std.fmt.parseInt(usize, step, 10) catch return null) orelse return null,
            else => return null,
        };
    }
    return t[cur..valueEnd(t, cur)];
}

/// What the validated document `t` is at its root.
pub fn rootKind(t: []const u8) enum { array, null, other } {
    return switch (t[skipWs(t, 0)]) {
        '[' => .array,
        'n' => .null,
        else => .other,
    };
}

/// The elements of the array at the root of the validated document `t`, each a
/// slice of `t`.
pub const Elements = struct {
    t: []const u8,
    i: usize,

    pub fn root(t: []const u8) Elements {
        return at(t, skipWs(t, 0));
    }

    fn at(t: []const u8, open: usize) Elements {
        return .{ .t = t, .i = skipWs(t, open + 1) };
    }

    pub fn next(self: *Elements) ?[]const u8 {
        const s = self.nextStart() orelse return null;
        const e = valueEnd(self.t, s);
        self.i = self.after(s);
        return self.t[s..e];
    }

    fn nextStart(self: *Elements) ?usize {
        if (self.i >= self.t.len or self.t[self.i] == ']') return null;
        return self.i;
    }

    /// Where the element after the one starting at `s` starts (or the `]`).
    fn after(self: *Elements, s: usize) usize {
        const j = skipWs(self.t, valueEnd(self.t, s));
        if (self.t[j] == ',') return skipWs(self.t, j + 1);
        self.i = j;
        return j;
    }
};

// --- values -------------------------------------------------------------------

/// A JSON value as a cell, as `eval.jsonToValue` makes one from a `std.json`
/// tree: strings unquoted, numbers and booleans as their text, objects and
/// arrays as compact JSON, null as null.
pub const Cell = union(enum) { null, text: []const u8 };

pub fn cell(arena: std.mem.Allocator, raw: []const u8) Error!Cell {
    return switch (raw[0]) {
        '"' => .{ .text = try decodeString(arena, raw[1 .. raw.len - 1]) },
        't' => .{ .text = "true" },
        'f' => .{ .text = "false" },
        'n' => .null,
        '{', '[' => .{ .text = try compact(arena, raw) },
        else => .{ .text = try numberText(arena, raw, .cell) },
    };
}

/// A number's kind, as `std.json` reads one: an integer when it is written as
/// one and fits an i64, a float when it is finite, else its text.
pub const Number = union(enum) { int: i64, float: f64, text: []const u8 };

pub fn number(raw: []const u8) Number {
    if (!std.mem.eql(u8, raw, "-0") and std.mem.indexOfAny(u8, raw, ".eE") == null) {
        return if (std.fmt.parseInt(i64, raw, 10)) |i| .{ .int = i } else |_| .{ .text = raw };
    }
    const f = std.fmt.parseFloat(f64, raw) catch return .{ .text = raw };
    return if (std.math.isFinite(f)) .{ .float = f } else .{ .text = raw };
}

/// `.cell` renders a float as `jsonToValue` does (`{d}`); `.json` as
/// `std.json.Stringify` writes one (`{}`).
const NumberStyle = enum { cell, json };

fn numberText(arena: std.mem.Allocator, raw: []const u8, style: NumberStyle) Error![]const u8 {
    return switch (number(raw)) {
        // written as an integer, it is already its own shortest text
        .int, .text => raw,
        .float => |f| switch (style) {
            .cell => try std.fmt.allocPrint(arena, "{d}", .{f}),
            .json => try std.fmt.allocPrint(arena, "{}", .{f}),
        },
    };
}

/// A string's raw content decoded: escapes resolved, `\u` pairs joined. The
/// content itself when it holds no escape.
pub fn decodeString(arena: std.mem.Allocator, content: []const u8) Error![]const u8 {
    if (std.mem.indexOfScalar(u8, content, '\\') == null) return content;
    const buf = try arena.alloc(u8, content.len);
    return decodeInto(buf, content);
}

/// Decode `content` into `buf`, which a decoded string never outgrows (every
/// escape is at least as long as what it stands for).
fn decodeInto(buf: []u8, content: []const u8) []const u8 {
    var o: usize = 0;
    var i: usize = 0;
    while (i < content.len) {
        const c = content[i];
        if (c != '\\') {
            buf[o] = c;
            o += 1;
            i += 1;
            continue;
        }
        const e = content[i + 1];
        i += 2;
        const lit: u8 = switch (e) {
            'b' => 0x08,
            'f' => 0x0C,
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            'u' => {
                var cp: u21 = hex4(content, i).?;
                i += 4;
                if (cp >= 0xD800 and cp <= 0xDBFF) {
                    const lo = hex4(content, i + 2).?;
                    i += 6;
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                }
                o += std.unicode.utf8Encode(cp, buf[o..]) catch unreachable;
                continue;
            },
            else => e, // `"`, `\`, `/`
        };
        buf[o] = lit;
        o += 1;
    }
    return buf[0..o];
}

/// The validated value `raw` written as `std.json.Stringify` writes the tree it
/// parses to: no insignificant whitespace, strings re-escaped, numbers in its
/// form. A string with no escape is written as it stands: it holds nothing
/// `Stringify` would escape.
pub fn compact(arena: std.mem.Allocator, raw: []const u8) Error![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    try compactInto(arena, raw, &aw.writer);
    return aw.written();
}

pub fn compactInto(arena: std.mem.Allocator, raw: []const u8, w: *std.Io.Writer) Error!void {
    var i: usize = 0;
    while (i < raw.len) {
        const c = raw[i];
        switch (c) {
            ' ', '\t', '\n', '\r' => i += 1,
            '{', '}', '[', ']', ':', ',' => {
                w.writeByte(c) catch return error.OutOfMemory;
                i += 1;
            },
            '"' => {
                const e = stringEnd(raw, i);
                const content = raw[i + 1 .. e - 1];
                if (std.mem.indexOfScalar(u8, content, '\\') == null) {
                    w.writeAll(raw[i..e]) catch return error.OutOfMemory;
                } else {
                    std.json.Stringify.encodeJsonString(try decodeString(arena, content), .{}, w) catch return error.OutOfMemory;
                }
                i = e;
            },
            't', 'f', 'n' => {
                const word: []const u8 = switch (c) {
                    't' => "true",
                    'f' => "false",
                    else => "null",
                };
                w.writeAll(word) catch return error.OutOfMemory;
                i += word.len;
            },
            else => {
                const e = valueEnd(raw, i);
                w.writeAll(try numberText(arena, raw[i..e], .json)) catch return error.OutOfMemory;
                i = e;
            },
        }
    }
}

// --- tests --------------------------------------------------------------------

const testing = std.testing;
const eval = @import("eval.zig");
const Value = @import("value.zig").Value;

test "json: accepts and refuses what std.json does" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_][]const u8{
        "{\"a\":1,\"a\":2}", "{\"a\":1,\"\\u0061\":2}", "{\"a\":{\"b\":1},\"b\":{\"b\":2}}", " [1, 2] \n",
        "\xef\xbb\xbf[1]",   "[1,]",                    "[01]",                              "[1.]",
        "[-0]",              "[1e5]",                   "[1E+2]",                            "[\"\\u00e9\"]",
        "[\"\\ud800\"]",     "[\"\\udc00\"]",           "[\"\\ud83d\\ude00\"]",              "[\"a\tb\"]",
        "[\"\xff\"]",        "[\"\xed\xa0\x80\"]",      "[\"\xc3\xa9\"]",                    "[99999999999999999999]",
        "[1.5e400]",         "[true,false,null]",       "[tru]",                             "{}",
        "{\"k\" :  [ ] }",   "nul",                     "[\"\\/\"]",                         "[\"\\x\"]",
        "1 2",               "",                        "  ",                                "\"x\"",
        "-",                 "[1e]",                    "[1e+]",                             "[.5]",
        "[\"\\u12\"]",       "[\"\\u+123\"]",           "[\"\\u_123\"]",                     "{\"a\" 1}",
        "{,}",               "[,1]",                    "{\"a\":1,}",                        "[[[[]]]]",
        "{\"\":0}",          "[\"\x7f\"]",              "truex",                             "[1]x",
    };
    for (cases) |c| {
        const want = if (std.json.parseFromSliceLeaky(std.json.Value, a, c, .{})) |_| true else |_| false;
        const got = if (validate(a, c)) |_| true else |_| false;
        if (want != got) {
            std.debug.print("validate disagrees on `{s}`: std.json {}, here {}\n", .{ c, want, got });
            return error.TestUnexpectedResult;
        }
    }
}

/// A random JSON document: nested values, escapes, awkward numbers and spacing.
fn genValue(r: std.Random, w: *std.Io.Writer, depth: usize) !void {
    const kind = r.uintLessThan(u8, if (depth > 4) 5 else 7);
    switch (kind) {
        0 => try w.writeAll(([_][]const u8{ "true", "false", "null" })[r.uintLessThan(usize, 3)]),
        1 => try w.writeAll(([_][]const u8{ "0", "-0", "7", "-12", "3.25", "1e5", "2.50", "1E-3", "99999999999999999999", "1.5e400", "-0.0", "123456789012" })[r.uintLessThan(usize, 12)]),
        2, 3 => try genString(r, w),
        4 => try w.writeAll(([_][]const u8{ "[]", "{}" })[r.uintLessThan(usize, 2)]),
        5 => {
            try w.writeByte('[');
            const n = r.uintLessThan(usize, 5);
            for (0..n) |k| {
                if (k > 0) try w.writeAll(([_][]const u8{ ",", " , ", ",\n" })[r.uintLessThan(usize, 3)]);
                try genValue(r, w, depth + 1);
            }
            try w.writeByte(']');
        },
        else => {
            try w.writeAll(([_][]const u8{ "{", "{ " })[r.uintLessThan(usize, 2)]);
            const n = r.uintLessThan(usize, 5);
            for (0..n) |k| {
                if (k > 0) try w.writeByte(',');
                // keys from a small set, so lookups hit and duplicates happen
                try w.writeAll(([_][]const u8{ "\"a\"", "\"b\"", "\"c\"", "\"\\u0061\"", "\"k\\n\"", "\"0\"" })[r.uintLessThan(usize, 6)]);
                try w.writeAll(([_][]const u8{ ":", " : " })[r.uintLessThan(usize, 2)]);
                try genValue(r, w, depth + 1);
            }
            try w.writeByte('}');
        },
    }
}

fn genString(r: std.Random, w: *std.Io.Writer) !void {
    try w.writeByte('"');
    const parts = [_][]const u8{ "x", "vip", " ", "\\n", "\\t", "\\\"", "\\\\", "\\/", "\\u00e9", "\\u0001", "\\ud83d\\ude00", "\xc3\xa9", "\xe2\x82\xac", "\\b", "\\f" };
    const n = r.uintLessThan(usize, 5);
    for (0..n) |_| try w.writeAll(parts[r.uintLessThan(usize, parts.len)]);
    try w.writeByte('"');
}

fn sameCell(a: std.mem.Allocator, mine: Cell, theirs: Value) !bool {
    _ = a;
    return switch (mine) {
        .null => theirs == .null,
        .text => |s| theirs == .string and std.mem.eql(u8, s, theirs.string),
    };
}

test "json: reads every generated document as std.json does, and every damaged one too" {
    var prng = std.Random.DefaultPrng.init(20261003);
    const r = prng.random();
    const paths = [_][]const u8{ "a", "b", "0", "$.a", "a.b", "a[0]", "[1]", "c.0.a", "k\n", "$[0].b", "x" };
    var checked: usize = 0;
    for (0..20000) |round| {
        var ar = std.heap.ArenaAllocator.init(testing.allocator);
        defer ar.deinit();
        const a = ar.allocator();
        var aw = std.Io.Writer.Allocating.init(a);
        try genValue(r, &aw.writer, 0);
        var doc: []u8 = try a.dupe(u8, aw.written());
        // a third of the documents get one byte damaged
        if (round % 3 == 0 and doc.len > 0) {
            const at = r.uintLessThan(usize, doc.len);
            doc[at] = ([_]u8{ '"', '\\', ',', ']', '}', '1', ' ', 0x01, 0xff, 'e' })[r.uintLessThan(usize, 10)];
        }

        const tree = std.json.parseFromSliceLeaky(std.json.Value, a, doc, .{}) catch null;
        const ok = if (validate(a, doc)) |_| true else |_| false;
        if (ok != (tree != null)) {
            std.debug.print("validate disagrees on `{s}`: std.json {}, here {}\n", .{ doc, tree != null, ok });
            return error.TestUnexpectedResult;
        }
        const t = tree orelse continue;
        checked += 1;
        for (paths) |p| {
            const want: Value = if (eval.jsonPath(t, p)) |leaf| try eval.jsonToValue(a, leaf) else .null;
            const mine: Cell = if (try path(a, doc, p)) |raw| try cell(a, raw) else .null;
            if (!try sameCell(a, mine, want)) {
                std.debug.print("path `{s}` of `{s}`: std.json {any}, here {any}\n", .{ p, doc, want, mine });
                return error.TestUnexpectedResult;
            }
        }
        if (t == .array) {
            var it = Elements.root(doc);
            for (t.array.items) |item| {
                const raw = it.next() orelse return error.TestUnexpectedResult;
                if (!try sameCell(a, try cell(a, raw), try eval.jsonToValue(a, item))) {
                    std.debug.print("element of `{s}` differs\n", .{doc});
                    return error.TestUnexpectedResult;
                }
            }
            try testing.expect(it.next() == null);
        }
        const want_text = try std.json.Stringify.valueAlloc(a, t, .{});
        const mine_text = try compact(a, doc[skipWs(doc, 0)..valueEnd(doc, skipWs(doc, 0))]);
        if (!std.mem.eql(u8, want_text, mine_text)) {
            std.debug.print("compact `{s}`: std.json `{s}`, here `{s}`\n", .{ doc, want_text, mine_text });
            return error.TestUnexpectedResult;
        }
    }
    // the generator must not be producing only failures
    try testing.expect(checked > 8000);
}
