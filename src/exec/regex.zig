//! A small backtracking regular-expression matcher, enough for the pattern
//! vocabulary SQL queries actually use: anchors, `.`, character classes,
//! capturing and non-capturing groups, alternation, and quantifiers (`*`, `+`,
//! `?`, counted `{n}`/`{n,}`/`{n,m}`), each greedy or lazy. Deliberately a
//! subset, with no lookaround and no backreferences inside the pattern, so it
//! stays a few hundred lines instead of pulling in a regex dependency. What is
//! out of scope fails with `BadPattern` rather than matching literally: `(?=`
//! and friends, a backreference or other unknown letter escape (`\1`, `\b`), a
//! malformed `{...}` count (`\{` is the literal brace), a quantifier with nothing
//! to repeat, a third quantifier character. Quietly misreading a pattern would
//! return wrong rows.
//!
//! Compilation takes an allocator so callers can hand it a
//! `FixedBufferAllocator` over stack memory: matching a column costs no heap
//! traffic and nothing to free. Matching is continuation-passing (`Cont` is
//! what remains once the current atom succeeds), which is what lets a group
//! backtrack into the sequence that follows it. The subject is matched by
//! character: `.`, a class or a literal takes one UTF-8 code point, and a byte
//! that starts no valid sequence is a character of its own.
//!
//! Backtracking is worst-case exponential, so every `find` runs under a
//! `Budget` and gives up with `PatternTooComplex`, an error and never a null,
//! so a pathological pattern cannot read as a non-matching one. Two counters
//! for two failure modes. `steps` bounds wide blowup: `(a|aa)+b` against a run
//! of `a`s doubles its splits per character (28 took 2.5 seconds, 40 would take
//! hours); the allowance is far above what the depth cap allows, plus a
//! per-character term so a long subject with a cheap pattern never trips it.
//! `depth` bounds deep blowup: the matcher recurses once per repetition of a
//! group, and past the stack is a segfault no caller can catch (a repeated
//! literal, `.` or class loops instead, so it repeats without limit). The cap
//! claims 4 MiB of stack at a measured, rounded-up cost per frame, up to five
//! frames per repetition, which makes it a ceiling on how often a group repeats
//! in one match (`max_group_reps`: about 540 in Debug, 2,180 in release).
//! Carrying the continuation stack on the heap would remove it.

const std = @import("std");

pub const Error = error{ BadPattern, OutOfMemory, PatternTooComplex };

pub const max_groups = 10;
pub const Captures = [max_groups]?[2]usize;

/// One unit of the subject: a UTF-8 code point, or a byte that starts no valid
/// sequence, numbered past the code points (`invalid_base + byte`).
const Unit = struct { cp: u21, len: u3 };

const invalid_base: u21 = 0x110000;

fn unitAt(s: []const u8, pos: usize) Unit {
    const c = s[pos];
    if (c < 0x80) return .{ .cp = c, .len = 1 };
    const lone: Unit = .{ .cp = invalid_base + c, .len = 1 };
    const n = std.unicode.utf8ByteSequenceLength(c) catch return lone;
    if (pos + n > s.len) return lone;
    const cp = std.unicode.utf8Decode(s[pos..][0..n]) catch return lone;
    return .{ .cp = cp, .len = n };
}

/// The start of the unit that ends at `p`, no earlier than `lo` (both unit
/// boundaries): a valid sequence ending there is the unit, else the last byte.
fn unitBefore(s: []const u8, lo: usize, p: usize) usize {
    var len: usize = @min(4, p - lo);
    while (len > 1) : (len -= 1) {
        const q = p - len;
        if (s[q] >= 0xC0 and unitAt(s, q).len == len) return q;
    }
    return p - 1;
}

const Class = struct {
    neg: bool = false,
    bits: [32]u8 = .{0} ** 32,
    ranges: []const [2]u21 = &.{},

    fn has(self: Class, cp: u21) bool {
        const in = if (cp < 256)
            (self.bits[cp >> 3] >> @intCast(cp & 7)) & 1 != 0
        else for (self.ranges) |r| {
            if (cp >= r[0] and cp <= r[1]) break true;
        } else false;
        return in != self.neg;
    }

    fn set(self: *Class, c: u8) void {
        self.bits[c >> 3] |= @as(u8, 1) << @intCast(c & 7);
    }
};

const Atom = union(enum) {
    lit: u21,
    any,
    class: Class,
    bol,
    eol,
    group: Group,
};

const Group = struct { alt: []const []const Item, cap: ?u8 };

const Item = struct { atom: Atom, min: u32 = 1, max: u32 = 1, lazy: bool = false };

const Cont = union(enum) {
    done,
    seq: struct { items: []const Item, i: usize, rep: u32, next: *const Cont },
    close: struct { cap: u8, start: usize, next: *const Cont },
};

const Budget = struct {
    steps: u64,
    depth: u32 = 0,

    const stack_allowance = 4 << 20;

    const stack_per_level: usize = switch (@import("builtin").mode) {
        .Debug => 1536,
        else => 384,
    };

    const max_depth = stack_allowance / stack_per_level;

    pub const max_group_reps = max_depth / 5;

    fn init(subject_len: usize) Budget {
        return .{ .steps = 10_000_000 + 4 * @as(u64, subject_len) };
    }

    fn enter(self: *Budget) Error!void {
        if (self.steps == 0 or self.depth >= max_depth) return Error.PatternTooComplex;
        self.steps -= 1;
        self.depth += 1;
    }

    fn leave(self: *Budget) void {
        self.depth -= 1;
    }
};

/// `icase` matches a letter in either case, as a literal and inside a class.
pub const Options = struct { icase: bool = false };

/// `regexp_replace`'s flags: `g` replaces every match, `i` ignores case. Null for
/// any other letter.
pub const Flags = struct { global: bool = false, icase: bool = false };

pub fn parseFlags(s: []const u8) ?Flags {
    var f = Flags{};
    for (s) |c| switch (c) {
        'g' => f.global = true,
        'i' => f.icase = true,
        else => return null,
    };
    return f;
}

pub const Regex = struct {
    alt: []const []const Item,
    ngroups: u8,

    pub fn compile(gpa: std.mem.Allocator, pattern: []const u8) Error!Regex {
        return compileOpts(gpa, pattern, .{});
    }

    pub fn compileOpts(gpa: std.mem.Allocator, pattern: []const u8, opts: Options) Error!Regex {
        var p = Parser{ .src = pattern, .gpa = gpa, .icase = opts.icase };
        const alt = try p.parseAlt();
        if (p.i != pattern.len) return Error.BadPattern;
        return .{ .alt = alt, .ngroups = p.ngroup };
    }

    /// Leftmost match span at or after `from`, with `caps` filled (index 0 is
    /// the whole match). `PatternTooComplex` means gave up, not no match.
    pub fn find(self: Regex, s: []const u8, from: usize, caps: *Captures) Error!?[2]usize {
        var budget = Budget.init(s.len);
        var start = from;
        while (start <= s.len) : (start += if (start < s.len) unitAt(s, start).len else 1) {
            caps.* = .{null} ** max_groups;
            const done: Cont = .done;
            for (self.alt) |branch| {
                if (try step(branch, 0, 0, s, start, caps, &done, &budget)) |end| {
                    caps[0] = .{ start, end };
                    return .{ start, end };
                }
            }
        }
        return null;
    }
};

fn resume_(k: *const Cont, s: []const u8, pos: usize, caps: *Captures, b: *Budget) Error!?usize {
    try b.enter();
    defer b.leave();
    switch (k.*) {
        .done => return pos,
        .seq => |x| return step(x.items, x.i, x.rep, s, pos, caps, x.next, b),
        .close => |x| {
            const saved = caps[x.cap];
            caps[x.cap] = .{ x.start, pos };
            if (try resume_(x.next, s, pos, caps, b)) |e| return e;
            caps[x.cap] = saved;
            return null;
        },
    }
}

/// Match `items[i..]` where the item at `i` has already matched `rep` times.
/// Greedy tries one more repetition before settling for fewer; lazy inverts
/// only that order, so the set of reachable matches is the same.
fn step(items: []const Item, i: usize, rep: u32, s: []const u8, pos: usize, caps: *Captures, cont: *const Cont, b: *Budget) Error!?usize {
    try b.enter();
    defer b.leave();
    if (i == items.len) return resume_(cont, s, pos, caps, b);
    const it = items[i];
    if (it.atom != .group and it.atom != .bol and it.atom != .eol) return stepUnits(items, i, rep, s, pos, caps, cont, b);
    if (it.lazy) {
        if (rep >= it.min) {
            if (try step(items, i + 1, 0, s, pos, caps, cont, b)) |e| return e;
        }
        if (rep < it.max) {
            const k = Cont{ .seq = .{ .items = items, .i = i, .rep = rep + 1, .next = cont } };
            return matchAtom(it.atom, s, pos, caps, &k, b);
        }
        return null;
    }
    if (rep < it.max) {
        const k = Cont{ .seq = .{ .items = items, .i = i, .rep = rep + 1, .next = cont } };
        if (try matchAtom(it.atom, s, pos, caps, &k, b)) |e| return e;
    }
    if (rep >= it.min) return step(items, i + 1, 0, s, pos, caps, cont, b);
    return null;
}

/// A repeated single-unit atom (literal, `.`, class) walked in a loop rather than
/// a frame per repetition: greedy takes every unit it can, then gives them back
/// one at a time; lazy takes one more only when the rest fails.
fn stepUnits(items: []const Item, i: usize, rep: u32, s: []const u8, pos: usize, caps: *Captures, cont: *const Cont, b: *Budget) Error!?usize {
    const it = items[i];
    var n = rep;
    var p = pos;
    if (it.lazy) {
        while (true) {
            if (n >= it.min) {
                if (try step(items, i + 1, 0, s, p, caps, cont, b)) |e| return e;
            }
            if (n >= it.max) return null;
            p = unitMatch(it.atom, s, p) orelse return null;
            n += 1;
        }
    }
    while (n < it.max) {
        p = unitMatch(it.atom, s, p) orelse break;
        n += 1;
    }
    while (n >= it.min) {
        if (try step(items, i + 1, 0, s, p, caps, cont, b)) |e| return e;
        if (n == rep) break;
        p = unitBefore(s, pos, p);
        n -= 1;
    }
    return null;
}

/// Where a single-unit atom ends when it matches at `pos`.
fn unitMatch(a: Atom, s: []const u8, pos: usize) ?usize {
    if (pos >= s.len) return null;
    const u = unitAt(s, pos);
    const ok = switch (a) {
        .lit => |c| u.cp == c,
        .any => true,
        .class => |cl| cl.has(u.cp),
        else => unreachable,
    };
    return if (ok) pos + u.len else null;
}

fn matchAtom(a: Atom, s: []const u8, pos: usize, caps: *Captures, k: *const Cont, b: *Budget) Error!?usize {
    switch (a) {
        .bol => return if (pos == 0) resume_(k, s, pos, caps, b) else null,
        .eol => return if (pos == s.len) resume_(k, s, pos, caps, b) else null,
        .lit, .any, .class => return if (unitMatch(a, s, pos)) |e| resume_(k, s, e, caps, b) else null,
        .group => |g| {
            for (g.alt) |branch| {
                if (g.cap) |ci| {
                    const closer = Cont{ .close = .{ .cap = ci, .start = pos, .next = k } };
                    if (try step(branch, 0, 0, s, pos, caps, &closer, b)) |e| return e;
                } else {
                    if (try step(branch, 0, 0, s, pos, caps, k, b)) |e| return e;
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
    icase: bool = false,

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
            var quantified = false;
            if (self.i < self.src.len) switch (self.src[self.i]) {
                '*' => {
                    it.min = 0;
                    it.max = std.math.maxInt(u32);
                    self.i += 1;
                    quantified = true;
                },
                '+' => {
                    it.min = 1;
                    it.max = std.math.maxInt(u32);
                    self.i += 1;
                    quantified = true;
                },
                '?' => {
                    it.min = 0;
                    it.max = 1;
                    self.i += 1;
                    quantified = true;
                },
                '{' => {
                    try self.parseCount(&it);
                    quantified = true;
                },
                else => {},
            };
            if (quantified and self.i < self.src.len and self.src[self.i] == '?') {
                it.lazy = true;
                self.i += 1;
                if (self.i < self.src.len and self.src[self.i] == '?') return Error.BadPattern;
            }
            try items.append(it);
        }
        return items.toOwnedSlice();
    }

    fn parseCount(self: *Parser, it: *Item) Error!void {
        self.i += 1;
        const min = (try self.parseCountNum()) orelse return Error.BadPattern;
        var max = min;
        if (self.i < self.src.len and self.src[self.i] == ',') {
            self.i += 1;
            max = (try self.parseCountNum()) orelse std.math.maxInt(u32);
        }
        if (self.i >= self.src.len or self.src[self.i] != '}') return Error.BadPattern;
        self.i += 1;
        if (min > max) return Error.BadPattern;
        it.min = min;
        it.max = max;
    }

    fn parseCountNum(self: *Parser) Error!?u32 {
        const start = self.i;
        var n: u64 = 0;
        while (self.i < self.src.len and self.src[self.i] >= '0' and self.src[self.i] <= '9') : (self.i += 1) {
            const d: u64 = self.src[self.i] - '0';
            n = n * 10 + d;
            if (n > std.math.maxInt(u32)) return Error.BadPattern;
        }
        if (self.i == start) return null;
        const v: u32 = @intCast(n);
        return v;
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
                } else if (self.i < self.src.len and self.src[self.i] == '?') {
                    return Error.BadPattern;
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
            '{', '*', '+', '?' => return Error.BadPattern,
            '\\' => {
                if (self.i >= self.src.len) return Error.BadPattern;
                const e = self.src[self.i];
                if (e >= 0x80) return .{ .lit = self.unit() };
                self.i += 1;
                return try escapeAtom(e);
            },
            else => {
                if (c >= 0x80) {
                    self.i -= 1;
                    return .{ .lit = self.unit() };
                }
                if (!self.icase or !std.ascii.isAlphabetic(c)) return .{ .lit = c };
                var cl = Class{};
                cl.set(std.ascii.toLower(c));
                cl.set(std.ascii.toUpper(c));
                return .{ .class = cl };
            },
        }
    }

    /// The pattern's unit at `i`, consumed.
    fn unit(self: *Parser) u21 {
        const u = unitAt(self.src, self.i);
        self.i += u.len;
        return u.cp;
    }

    fn parseClass(self: *Parser) Error!Class {
        var cl = Class{};
        var ranges = std.array_list.Managed([2]u21).init(self.gpa);
        if (self.i < self.src.len and self.src[self.i] == '^') {
            cl.neg = true;
            self.i += 1;
        }
        var first = true;
        while (self.i < self.src.len and (self.src[self.i] != ']' or first)) {
            first = false;
            var lo: u21 = undefined;
            if (self.src[self.i] == '\\' and self.i + 1 < self.src.len and self.src[self.i + 1] < 0x80) {
                const e = self.src[self.i + 1];
                self.i += 2;
                switch (try escapeAtom(e)) {
                    .class => |sub| {
                        for (0..256) |b| {
                            if (sub.has(@intCast(b))) cl.set(@intCast(b));
                        }
                        if (sub.neg) try ranges.append(.{ 256, std.math.maxInt(u21) });
                        continue;
                    },
                    .lit => |l| lo = l,
                    else => return Error.BadPattern,
                }
            } else {
                if (self.src[self.i] == '\\') {
                    self.i += 1;
                    if (self.i >= self.src.len) return Error.BadPattern;
                }
                lo = self.unit();
            }
            var hi = lo;
            if (self.i + 1 < self.src.len and self.src[self.i] == '-' and self.src[self.i + 1] != ']') {
                self.i += 1;
                hi = self.unit();
            }
            var b: u21 = lo;
            while (b <= hi and b < 256) : (b += 1) cl.set(@intCast(b));
            if (hi >= 256 and hi >= lo) try ranges.append(.{ @max(lo, 256), hi });
        }
        if (self.i >= self.src.len) return Error.BadPattern;
        self.i += 1;
        cl.ranges = try ranges.toOwnedSlice();
        if (self.icase) for ('a'..'z' + 1) |lc| {
            const l: u8 = @intCast(lc);
            const u = std.ascii.toUpper(l);
            const in = (cl.bits[l >> 3] >> @intCast(l & 7)) & 1 != 0 or (cl.bits[u >> 3] >> @intCast(u & 7)) & 1 != 0;
            if (in) {
                cl.set(l);
                cl.set(u);
            }
        };
        return cl;
    }
};

/// A letter or digit escape outside the supported set is refused rather than read as
/// the character: `\1` would otherwise match a literal `1` and `\b` a `b`.
fn escapeAtom(e: u8) Error!Atom {
    var cl = Class{};
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
        else => {
            if (std.ascii.isAlphanumeric(e)) return Error.BadPattern;
            return .{ .lit = e };
        },
    }
    return .{ .class = cl };
}

/// `regexp_replace` without the global flag: `\1`..`\9` in `repl` expand to
/// captures, `\0` to the whole match. Compiles on every call; for a whole
/// column, compile once and use `replaceFirstRe`.
pub fn replaceFirst(
    out: std.mem.Allocator,
    scratch: std.mem.Allocator,
    s: []const u8,
    pattern: []const u8,
    repl: []const u8,
) Error![]const u8 {
    return replaceFirstRe(out, try Regex.compile(scratch, pattern), s, repl);
}

pub fn replaceFirstRe(
    out: std.mem.Allocator,
    re: Regex,
    s: []const u8,
    repl: []const u8,
) Error![]const u8 {
    return replaceRe(out, re, s, repl, false);
}

/// Every non-overlapping match replaced, left to right; after an empty match the
/// next character is copied, so the scan always advances.
pub fn replaceAllRe(
    out: std.mem.Allocator,
    re: Regex,
    s: []const u8,
    repl: []const u8,
) Error![]const u8 {
    return replaceRe(out, re, s, repl, true);
}

fn replaceRe(out: std.mem.Allocator, re: Regex, s: []const u8, repl: []const u8, all: bool) Error![]const u8 {
    var caps: Captures = undefined;
    var buf = std.array_list.Managed(u8).init(out);
    var at: usize = 0;
    var replaced = false;
    while (at <= s.len) {
        const span = (try re.find(s, at, &caps)) orelse break;
        replaced = true;
        try buf.appendSlice(s[at..span[0]]);
        try expandReplacement(&buf, s, repl, &caps);
        at = span[1];
        if (!all) break;
        if (span[0] == span[1]) {
            const n: usize = if (at < s.len) unitAt(s, at).len else 1;
            if (at < s.len) try buf.appendSlice(s[at..][0..n]);
            at += n;
        }
    }
    if (!replaced) return s;
    if (at < s.len) try buf.appendSlice(s[at..]);
    return buf.toOwnedSlice();
}

fn expandReplacement(buf: *std.array_list.Managed(u8), s: []const u8, repl: []const u8, caps: *const Captures) Error!void {
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
}

test "regex: literals, classes, anchors, groups" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    var re = try Regex.compile(a, "^ab+c$");
    try std.testing.expect((try re.find("abbbc", 0, &caps)) != null);
    try std.testing.expect((try re.find("ac", 0, &caps)) == null);

    re = try Regex.compile(a, "[a-z]+[0-9]");
    try std.testing.expect((try re.find("xyz7", 0, &caps)) != null);
    try std.testing.expect((try re.find("XYZ7", 0, &caps)) == null);

    re = try Regex.compile(a, "a|bc");
    try std.testing.expect((try re.find("zzbc", 0, &caps)) != null);

    re = try Regex.compile(a, "x(b+)(c?)d");
    try std.testing.expectEqual(@as(?[2]usize, .{ 1, 6 }), try re.find("zxbbbd", 0, &caps));
    try std.testing.expectEqual(@as(?[2]usize, .{ 1, 6 }), caps[0]);
    try std.testing.expectEqual(@as(?[2]usize, .{ 2, 5 }), caps[1]);
    try std.testing.expectEqual(@as(?[2]usize, .{ 5, 5 }), caps[2]);
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

test "regex: counted quantifiers" {
    var buf: [32 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    var re = try Regex.compile(a, "^a{3}$");
    try std.testing.expect((try re.find("aaa", 0, &caps)) != null);
    try std.testing.expect((try re.find("aa", 0, &caps)) == null);

    re = try Regex.compile(a, "a{2,}");
    try std.testing.expectEqual([2]usize{ 0, 4 }, (try re.find("aaaa", 0, &caps)).?);

    re = try Regex.compile(a, "a{2,3}");
    try std.testing.expectEqual([2]usize{ 0, 3 }, (try re.find("aaaa", 0, &caps)).?);

    re = try Regex.compile(a, "(ab){2}");
    try std.testing.expectEqual([2]usize{ 0, 4 }, (try re.find("abab", 0, &caps)).?);
    try std.testing.expect((try re.find("ab", 0, &caps)) == null);
}

test "regex: lazy quantifiers" {
    var buf: [32 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    var re = try Regex.compile(a, "a+?");
    try std.testing.expectEqual([2]usize{ 0, 1 }, (try re.find("aaa", 0, &caps)).?);

    re = try Regex.compile(a, "a*?b");
    try std.testing.expectEqual([2]usize{ 0, 3 }, (try re.find("aab", 0, &caps)).?);

    re = try Regex.compile(a, "ab??");
    try std.testing.expectEqual([2]usize{ 0, 1 }, (try re.find("ab", 0, &caps)).?);

    re = try Regex.compile(a, "a{2,4}?");
    try std.testing.expectEqual([2]usize{ 0, 2 }, (try re.find("aaaa", 0, &caps)).?);

    re = try Regex.compile(a, "<(.*?)>");
    try std.testing.expectEqual([2]usize{ 0, 3 }, (try re.find("<x><y>", 0, &caps)).?);
    try std.testing.expectEqual([2]usize{ 1, 2 }, caps[1].?);
}

test "regex: malformed quantifiers are rejected" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    const bad = [_][]const u8{ "a{", "a{}", "a{2", "a{2,1}", "a{x}", "a{,3}", "a*??", "a**", "a*+", "{2}" };
    for (bad) |pat| {
        try std.testing.expectError(Error.BadPattern, Regex.compile(a, pat));
    }
}

test "regex: a backreference or an unknown letter escape is refused, a punctuation escape is literal" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    for ([_][]const u8{ "(a)\\1", "\\bx", "x\\B", "\\Aa", "a\\z", "[\\b]" }) |pat| {
        try std.testing.expectError(Error.BadPattern, Regex.compile(a, pat));
    }
    var caps: Captures = undefined;
    const re = try Regex.compile(a, "\\.\\$\\\\\\-");
    try std.testing.expectEqual([2]usize{ 0, 4 }, (try re.find(".$\\-", 0, &caps)).?);
}

test "regex: characters are UTF-8 code points, a stray byte one of its own" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    var re = try Regex.compile(a, "^a.ão$");
    try std.testing.expect((try re.find("ação", 0, &caps)) != null);
    re = try Regex.compile(a, "[ç]");
    try std.testing.expectEqual([2]usize{ 1, 3 }, (try re.find("ação", 0, &caps)).?);
    re = try Regex.compile(a, "^[à-ú]+$");
    try std.testing.expect((try re.find("çãé", 0, &caps)) != null);
    try std.testing.expect((try re.find("çaé", 0, &caps)) == null);
    re = try Regex.compile(a, "^ç{2}€?$");
    try std.testing.expect((try re.find("çç€", 0, &caps)) != null);
    re = try Regex.compile(a, "^[^a]$");
    try std.testing.expect((try re.find("€", 0, &caps)) != null);
    try std.testing.expect((try re.find("\xff", 0, &caps)) != null);
    re = try Regex.compile(a, "^.{3}$");
    try std.testing.expect((try re.find("a\xffç", 0, &caps)) != null);
    re = try Regex.compile(a, "\\W+");
    try std.testing.expectEqual([2]usize{ 1, 6 }, (try re.find("aç€b", 0, &caps)).?);
    re = try Regex.compile(a, "^.*ç");
    try std.testing.expectEqual([2]usize{ 0, 7 }, (try re.find("éçxç", 0, &caps)).?);
    re = try Regex.compile(a, "^(.+)€\xffx$");
    try std.testing.expectEqual([2]usize{ 0, 9 }, (try re.find("€a€\xffx", 0, &caps)).?);
    try std.testing.expectEqual([2]usize{ 0, 4 }, caps[1].?);
    try std.testing.expectEqualStrings("-é-ç-", try replaceAllRe(a, try Regex.compile(a, "x*"), "éç", "-"));
}

test "regex: no match leaves the subject alone" {
    var buf: [8 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const got = try replaceFirst(std.testing.allocator, fba.allocator(), "plain", "^x(y)z$", "\\1");
    try std.testing.expectEqualStrings("plain", got);
}

test "regex: a pathological pattern gives up instead of hanging" {
    var buf: [32 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    const re = try Regex.compile(a, "(a|aa)+b");
    try std.testing.expectError(Error.PatternTooComplex, re.find("a" ** 40 ++ "c", 0, &caps));

    const deep = try Regex.compile(a, "a{1,1000000}b");
    try std.testing.expectError(Error.PatternTooComplex, deep.find("a" ** 100_000, 0, &caps));

    const n = 100_000;
    const run = "a" ** n;
    const plain = try Regex.compile(a, "[a-z]+9");
    const hit = run ++ "9";
    try std.testing.expectEqual([2]usize{ 0, n + 1 }, (try plain.find(hit, 0, &caps)).?);
    try std.testing.expect((try plain.find(hit[0..2000], 0, &caps)) == null);
    for ([_][]const u8{ "^a+$", "^a+?$", "^.*$", "^[^b]{2,}$" }) |pat| {
        const re2 = try Regex.compile(a, pat);
        try std.testing.expectEqual([2]usize{ 0, n }, (try re2.find(run, 0, &caps)).?);
    }
    const grp = try Regex.compile(a, "^(a)+$");
    const reps = Budget.max_group_reps;
    try std.testing.expectEqual([2]usize{ 0, reps / 2 }, (try grp.find(run[0 .. reps / 2], 0, &caps)).?);
    try std.testing.expectError(Error.PatternTooComplex, grp.find(run[0 .. reps * 2], 0, &caps));

    const long = "b" ** 200_000 ++ "zq";
    const lit = try Regex.compile(a, "zq");
    try std.testing.expectEqual([2]usize{ 200_000, 200_002 }, (try lit.find(long, 0, &caps)).?);
}

test "regex: icase folds literals and classes, a negated class included; replaceAllRe advances past empty matches" {
    var buf: [32 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    var re = try Regex.compileOpts(a, "hello", .{ .icase = true });
    try std.testing.expect((try re.find("say HeLLo", 0, &caps)) != null);
    re = try Regex.compileOpts(a, "[a-c]+", .{ .icase = true });
    try std.testing.expectEqual([2]usize{ 0, 3 }, (try re.find("AbC", 0, &caps)).?);
    re = try Regex.compileOpts(a, "[^a-z]", .{ .icase = true });
    try std.testing.expect((try re.find("ABC", 0, &caps)) == null);
    re = try Regex.compile(a, "hello");
    try std.testing.expect((try re.find("HELLO", 0, &caps)) == null);

    try std.testing.expectEqualStrings("a+b+c", try replaceAllRe(a, try Regex.compile(a, "-"), "a-b-c", "+"));
    try std.testing.expectEqualStrings("-a-b-c-", try replaceAllRe(a, try Regex.compile(a, "x*"), "abc", "-"));
    try std.testing.expectEqualStrings("a[1][2]", try replaceAllRe(a, try Regex.compile(a, "(\\d)"), "a12", "[\\1]"));
    try std.testing.expectEqualStrings("a+b-c", try replaceFirstRe(a, try Regex.compile(a, "-"), "a-b-c", "+"));
    try std.testing.expectEqualStrings("abc", try replaceAllRe(a, try Regex.compile(a, "z"), "abc", "+"));

    try std.testing.expectEqual(Flags{ .global = true, .icase = true }, parseFlags("gi").?);
    try std.testing.expect(parseFlags("gx") == null);
}
