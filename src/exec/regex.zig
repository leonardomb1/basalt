//! A small backtracking regular-expression matcher, enough for the pattern
//! vocabulary SQL queries actually use: anchors, `.`, character classes,
//! capturing and non-capturing groups, alternation and the three greedy
//! quantifiers. Deliberately a subset — no lookaround, no backreferences
//! inside the pattern, no lazy quantifiers — so it stays a few hundred lines
//! instead of pulling in a regex dependency.
//!
//! Compilation takes an allocator so callers can hand it a
//! `FixedBufferAllocator` over stack memory: matching a column costs no heap
//! traffic and nothing to free.

const std = @import("std");

pub const Error = error{ BadPattern, OutOfMemory };

pub const max_groups = 10;
pub const Captures = [max_groups]?[2]usize;

const Class = struct {
    neg: bool,
    /// 256-bit membership set; simpler and faster than a range list.
    bits: [32]u8,

    fn has(self: Class, c: u8) bool {
        const in = (self.bits[c >> 3] >> @intCast(c & 7)) & 1 != 0;
        return in != self.neg;
    }

    fn set(self: *Class, c: u8) void {
        self.bits[c >> 3] |= @as(u8, 1) << @intCast(c & 7);
    }
};

const Atom = union(enum) {
    lit: u8,
    any,
    class: Class,
    bol,
    eol,
    group: Group,
};

const Group = struct { alt: []const []const Item, cap: ?u8 };

const Item = struct { atom: Atom, min: u32 = 1, max: u32 = 1 };

/// Continuation: what remains to be matched once the current atom succeeds.
/// Modelling it explicitly is what lets a group backtrack into the sequence
/// that follows it.
const Cont = union(enum) {
    done,
    seq: struct { items: []const Item, i: usize, rep: u32, next: *const Cont },
    close: struct { cap: u8, start: usize, next: *const Cont },
};

pub const Regex = struct {
    alt: []const []const Item,
    ngroups: u8,

    pub fn compile(gpa: std.mem.Allocator, pattern: []const u8) Error!Regex {
        var p = Parser{ .src = pattern, .gpa = gpa };
        const alt = try p.parseAlt();
        if (p.i != pattern.len) return Error.BadPattern;
        return .{ .alt = alt, .ngroups = p.ngroup };
    }

    /// Leftmost match at or after `from`. Returns the matched span; `caps` is
    /// filled with group spans (index 0 is the whole match).
    pub fn find(self: Regex, s: []const u8, from: usize, caps: *Captures) ?[2]usize {
        var start = from;
        while (start <= s.len) : (start += 1) {
            caps.* = .{null} ** max_groups;
            const done: Cont = .done;
            for (self.alt) |branch| {
                if (step(branch, 0, 0, s, start, caps, &done)) |end| {
                    caps[0] = .{ start, end };
                    return .{ start, end };
                }
            }
        }
        return null;
    }
};

fn resume_(k: *const Cont, s: []const u8, pos: usize, caps: *Captures) ?usize {
    switch (k.*) {
        .done => return pos,
        .seq => |x| return step(x.items, x.i, x.rep, s, pos, caps, x.next),
        .close => |x| {
            const saved = caps[x.cap];
            caps[x.cap] = .{ x.start, pos };
            if (resume_(x.next, s, pos, caps)) |e| return e;
            caps[x.cap] = saved;
            return null;
        },
    }
}

/// Match `items[i..]` where the item at `i` has already matched `rep` times.
/// Greedy: one more repetition is always tried before settling for fewer.
fn step(items: []const Item, i: usize, rep: u32, s: []const u8, pos: usize, caps: *Captures, cont: *const Cont) ?usize {
    if (i == items.len) return resume_(cont, s, pos, caps);
    const it = items[i];
    if (rep < it.max) {
        const k = Cont{ .seq = .{ .items = items, .i = i, .rep = rep + 1, .next = cont } };
        if (matchAtom(it.atom, s, pos, caps, &k)) |e| return e;
    }
    if (rep >= it.min) return step(items, i + 1, 0, s, pos, caps, cont);
    return null;
}

fn matchAtom(a: Atom, s: []const u8, pos: usize, caps: *Captures, k: *const Cont) ?usize {
    switch (a) {
        .bol => return if (pos == 0) resume_(k, s, pos, caps) else null,
        .eol => return if (pos == s.len) resume_(k, s, pos, caps) else null,
        .lit => |c| return if (pos < s.len and s[pos] == c) resume_(k, s, pos + 1, caps) else null,
        .any => return if (pos < s.len) resume_(k, s, pos + 1, caps) else null,
        .class => |cl| return if (pos < s.len and cl.has(s[pos])) resume_(k, s, pos + 1, caps) else null,
        .group => |g| {
            for (g.alt) |branch| {
                if (g.cap) |ci| {
                    const closer = Cont{ .close = .{ .cap = ci, .start = pos, .next = k } };
                    if (step(branch, 0, 0, s, pos, caps, &closer)) |e| return e;
                } else {
                    if (step(branch, 0, 0, s, pos, caps, k)) |e| return e;
                }
            }
            return null;
        },
    }
}

const Parser = struct {
    src: []const u8,
    i: usize = 0,
    gpa: std.mem.Allocator,
    ngroup: u8 = 1,

    fn parseAlt(self: *Parser) Error![]const []const Item {
        var branches = std.array_list.Managed([]const Item).init(self.gpa);
        try branches.append(try self.parseSeq());
        while (self.i < self.src.len and self.src[self.i] == '|') {
            self.i += 1;
            try branches.append(try self.parseSeq());
        }
        return branches.toOwnedSlice();
    }

    fn parseSeq(self: *Parser) Error![]const Item {
        var items = std.array_list.Managed(Item).init(self.gpa);
        while (self.i < self.src.len and self.src[self.i] != '|' and self.src[self.i] != ')') {
            var it = Item{ .atom = try self.parseAtom() };
            if (self.i < self.src.len) switch (self.src[self.i]) {
                '*' => {
                    it.min = 0;
                    it.max = std.math.maxInt(u32);
                    self.i += 1;
                },
                '+' => {
                    it.min = 1;
                    it.max = std.math.maxInt(u32);
                    self.i += 1;
                },
                '?' => {
                    it.min = 0;
                    it.max = 1;
                    self.i += 1;
                },
                else => {},
            };
            try items.append(it);
        }
        return items.toOwnedSlice();
    }

    fn parseAtom(self: *Parser) Error!Atom {
        const c = self.src[self.i];
        self.i += 1;
        switch (c) {
            '^' => return .bol,
            '$' => return .eol,
            '.' => return .any,
            '(' => {
                var cap: ?u8 = null;
                if (self.i + 1 < self.src.len and self.src[self.i] == '?' and self.src[self.i + 1] == ':') {
                    self.i += 2;
                } else {
                    if (self.ngroup >= max_groups) return Error.BadPattern;
                    cap = self.ngroup;
                    self.ngroup += 1;
                }
                const alt = try self.parseAlt();
                if (self.i >= self.src.len or self.src[self.i] != ')') return Error.BadPattern;
                self.i += 1;
                return .{ .group = .{ .alt = alt, .cap = cap } };
            },
            '[' => return .{ .class = try self.parseClass() },
            '\\' => {
                if (self.i >= self.src.len) return Error.BadPattern;
                const e = self.src[self.i];
                self.i += 1;
                return escapeAtom(e);
            },
            else => return .{ .lit = c },
        }
    }

    fn parseClass(self: *Parser) Error!Class {
        var cl = Class{ .neg = false, .bits = .{0} ** 32 };
        if (self.i < self.src.len and self.src[self.i] == '^') {
            cl.neg = true;
            self.i += 1;
        }
        var first = true;
        while (self.i < self.src.len and (self.src[self.i] != ']' or first)) {
            first = false;
            var lo = self.src[self.i];
            self.i += 1;
            if (lo == '\\') {
                if (self.i >= self.src.len) return Error.BadPattern;
                const e = self.src[self.i];
                self.i += 1;
                switch (escapeAtom(e)) {
                    .class => |sub| {
                        for (0..256) |b| {
                            if (sub.has(@intCast(b))) cl.set(@intCast(b));
                        }
                        continue;
                    },
                    .lit => |l| lo = l,
                    else => return Error.BadPattern,
                }
            }
            if (self.i + 1 < self.src.len and self.src[self.i] == '-' and self.src[self.i + 1] != ']') {
                const hi = self.src[self.i + 1];
                self.i += 2;
                var b: usize = lo;
                while (b <= hi) : (b += 1) cl.set(@intCast(b));
            } else {
                cl.set(lo);
            }
        }
        if (self.i >= self.src.len) return Error.BadPattern;
        self.i += 1;
        return cl;
    }
};

fn escapeAtom(e: u8) Atom {
    var cl = Class{ .neg = false, .bits = .{0} ** 32 };
    switch (e) {
        'd', 'D' => {
            for ('0'..'9' + 1) |b| cl.set(@intCast(b));
            cl.neg = e == 'D';
        },
        'w', 'W' => {
            for ('a'..'z' + 1) |b| cl.set(@intCast(b));
            for ('A'..'Z' + 1) |b| cl.set(@intCast(b));
            for ('0'..'9' + 1) |b| cl.set(@intCast(b));
            cl.set('_');
            cl.neg = e == 'W';
        },
        's', 'S' => {
            for ([_]u8{ ' ', '\t', '\n', '\r', 11, 12 }) |b| cl.set(b);
            cl.neg = e == 'S';
        },
        'n' => return .{ .lit = '\n' },
        't' => return .{ .lit = '\t' },
        'r' => return .{ .lit = '\r' },
        else => return .{ .lit = e },
    }
    return .{ .class = cl };
}

/// Replace the first match of `pattern` in `s`, expanding `\1`..`\9` in
/// `repl` to the corresponding capture (`\0` is the whole match). Mirrors
/// `regexp_replace` without the global flag.
pub fn replaceFirst(
    out: std.mem.Allocator,
    scratch: std.mem.Allocator,
    s: []const u8,
    pattern: []const u8,
    repl: []const u8,
) Error![]const u8 {
    const re = try Regex.compile(scratch, pattern);
    var caps: Captures = undefined;
    const span = re.find(s, 0, &caps) orelse return s;

    var buf = std.array_list.Managed(u8).init(out);
    try buf.appendSlice(s[0..span[0]]);
    var i: usize = 0;
    while (i < repl.len) : (i += 1) {
        if (repl[i] == '\\' and i + 1 < repl.len and repl[i + 1] >= '0' and repl[i + 1] <= '9') {
            const g = repl[i + 1] - '0';
            if (caps[g]) |c| try buf.appendSlice(s[c[0]..c[1]]);
            i += 1;
        } else {
            try buf.append(repl[i]);
        }
    }
    try buf.appendSlice(s[span[1]..]);
    return buf.toOwnedSlice();
}

test "regex: literals, classes, anchors, groups" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    var re = try Regex.compile(a, "^ab+c$");
    try std.testing.expect(re.find("abbbc", 0, &caps) != null);
    try std.testing.expect(re.find("ac", 0, &caps) == null);

    re = try Regex.compile(a, "[a-z]+[0-9]");
    try std.testing.expect(re.find("xyz7", 0, &caps) != null);
    try std.testing.expect(re.find("XYZ7", 0, &caps) == null);

    re = try Regex.compile(a, "a|bc");
    try std.testing.expect(re.find("zzbc", 0, &caps) != null);
}

test "regex: the ClickBench host-extraction pattern" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const pat = "^https?://(?:www\\.)?([^/]+)/.*$";

    const got = try replaceFirst(
        std.testing.allocator,
        fba.allocator(),
        "http://www.example.com/a/b",
        pat,
        "\\1",
    );
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings("example.com", got);

    fba.reset();
    const got2 = try replaceFirst(
        std.testing.allocator,
        fba.allocator(),
        "https://sub.host.org/x",
        pat,
        "\\1",
    );
    defer std.testing.allocator.free(got2);
    try std.testing.expectEqualStrings("sub.host.org", got2);
}

test "regex: no match leaves the subject alone" {
    var buf: [8 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const got = try replaceFirst(std.testing.allocator, fba.allocator(), "plain", "^x(y)z$", "\\1");
    try std.testing.expectEqualStrings("plain", got);
}
