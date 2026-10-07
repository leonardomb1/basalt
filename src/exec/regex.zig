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
//! backtrack into the sequence that follows it.
//!
//! Backtracking is worst-case exponential, so every `find` runs under a
//! `Budget` and gives up with `PatternTooComplex`, an error and never a null,
//! so a pathological pattern cannot read as a non-matching one. Two counters
//! for two failure modes. `steps` bounds wide blowup: `(a|aa)+b` against a run
//! of `a`s doubles its splits per character (28 took 2.5 seconds, 40 would take
//! hours); the allowance is far above what the depth cap allows, plus a
//! per-character term so a long subject with a cheap pattern never trips it.
//! `depth` bounds deep blowup: the matcher recurses once per repetition, and
//! past the stack is a segfault no caller can catch. The cap claims 4 MiB of
//! stack at a measured, rounded-up cost per frame, three frames per subject
//! character, which makes it a ceiling on subject length (about 900 characters
//! in Debug, 3600 in release). Carrying the continuation stack on the heap
//! would remove it; until a query needs longer text, this is the cheap half.

const std = @import("std");

pub const Error = error{ BadPattern, OutOfMemory, PatternTooComplex };

pub const max_groups = 10;
pub const Captures = [max_groups]?[2]usize;

const Class = struct {
    neg: bool,
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

    pub const max_subject_len = max_depth / 3;

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
        while (start <= s.len) : (start += 1) {
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

fn matchAtom(a: Atom, s: []const u8, pos: usize, caps: *Captures, k: *const Cont, b: *Budget) Error!?usize {
    switch (a) {
        .bol => return if (pos == 0) resume_(k, s, pos, caps, b) else null,
        .eol => return if (pos == s.len) resume_(k, s, pos, caps, b) else null,
        .lit => |c| return if (pos < s.len and s[pos] == c) resume_(k, s, pos + 1, caps, b) else null,
        .any => return if (pos < s.len) resume_(k, s, pos + 1, caps, b) else null,
        .class => |cl| return if (pos < s.len and cl.has(s[pos])) resume_(k, s, pos + 1, caps, b) else null,
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
                self.i += 1;
                return try escapeAtom(e);
            },
            else => {
                if (!self.icase or !std.ascii.isAlphabetic(c)) return .{ .lit = c };
                var cl = Class{ .neg = false, .bits = .{0} ** 32 };
                cl.set(std.ascii.toLower(c));
                cl.set(std.ascii.toUpper(c));
                return .{ .class = cl };
            },
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
                switch (try escapeAtom(e)) {
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
            if (at < s.len) try buf.append(s[at]);
            at += 1;
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

    const n = Budget.max_subject_len / 2;
    const plain = try Regex.compile(a, "[a-z]+9");
    const hit = "a" ** (Budget.max_subject_len / 2) ++ "9";
    try std.testing.expectEqual([2]usize{ 0, n + 1 }, (try plain.find(hit, 0, &caps)).?);
    try std.testing.expect((try plain.find(hit[0..n], 0, &caps)) == null);

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
