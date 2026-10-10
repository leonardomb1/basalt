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
//! `FixedBufferAllocator` over stack memory. Matching is continuation-passing
//! (`Cont` is what remains once the current atom succeeds), which is what lets a
//! group backtrack into the sequence that follows it, run as a loop over an
//! explicit stack of choice points (`Machine`): a short match stays in a stack
//! buffer, and a group repeated once per character of a long subject grows the
//! stacks on the heap rather than recursing. The subject is matched by
//! character: `.`, a class or a literal takes one UTF-8 code point, and a byte
//! that starts no valid sequence is a character of its own.
//!
//! Backtracking is worst-case exponential, so every `find` runs under a step
//! budget and gives up with `PatternTooComplex`, an error and never a null, so a
//! pathological pattern cannot read as a non-matching one: `(a|aa)+b` against a
//! run of `a`s doubles its splits per character (28 took 2.5 seconds, 40 would
//! take hours). The allowance has a per-character term so a long subject with a
//! cheap pattern never trips it, and either stack past `Machine.max_frames` gives
//! up the same way. A repetition of a group beyond its minimum that matches
//! nothing ends the loop, so `(a*)*` cannot spin.

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
        var sfa = std.heap.stackFallback(8 * 1024, std.heap.page_allocator);
        var m = Machine{ .s = s, .caps = caps, .gpa = sfa.get(), .steps = 10_000_000 + 4 * @as(u64, s.len) };
        defer m.deinit();
        var start = from;
        while (start <= s.len) : (start += if (start < s.len) unitAt(s, start).len else 1) {
            caps.* = .{null} ** max_groups;
            for (self.alt) |branch| {
                if (try m.run(branch, start)) |end| {
                    caps[0] = .{ start, end };
                    return .{ start, end };
                }
            }
        }
        return null;
    }
};

/// What remains once the current atom succeeds, as an index into `Machine.conts`
/// (0 is `done`). `seq` resumes `items[i]` after its `rep`-th repetition, which
/// began at `start`.
const Cont = union(enum) {
    done,
    seq: struct { items: []const Item, i: usize, rep: u32, start: usize, next: u32 },
    close: struct { cap: u8, start: usize, next: u32 },
};

const StepAt = struct { items: []const Item, i: usize, rep: u32, pos: usize, cont: u32 };

/// A choice point to come back to on failure, or a capture to put back on the way.
/// `more` is a lazy group's second choice, one more repetition; `branch` a group's
/// next alternative; `units` a repeated single-unit item giving back (greedy) or
/// taking (lazy) one more unit, `n` units in and ending at `p`.
const Choice = union(enum) {
    step: StepAt,
    more: StepAt,
    branch: struct { items: []const Item, i: usize, b: usize, pos: usize, cont: u32 },
    units: struct { items: []const Item, i: usize, rep: u32, start: usize, p: usize, n: u32, cont: u32 },
    restore: struct { cap: u8, saved: ?[2]usize },
};

const Frame = struct { choice: Choice, nconts: u32 };

const State = union(enum) { step: StepAt, resume_: struct { cont: u32, pos: usize }, fail };

/// One `find`'s backtracking state, kept on the heap so a group may repeat once per
/// character of a long subject. Continuations made after a choice point are dropped
/// when it is taken, so both stacks hold only the current path.
const Machine = struct {
    s: []const u8,
    caps: *Captures,
    gpa: std.mem.Allocator,
    conts: std.ArrayList(Cont) = .empty,
    stack: std.ArrayList(Frame) = .empty,
    steps: u64,

    /// Ceiling on either stack: about 70 MiB at worst, and a group repeated in one
    /// match about 500,000 times (two entries per repetition on each).
    const max_frames = 1 << 20;

    fn deinit(m: *Machine) void {
        m.conts.deinit(m.gpa);
        m.stack.deinit(m.gpa);
    }

    fn cont(m: *Machine, c: Cont) Error!u32 {
        if (m.conts.items.len >= max_frames) return Error.PatternTooComplex;
        try m.conts.append(m.gpa, c);
        return @intCast(m.conts.items.len - 1);
    }

    fn push(m: *Machine, c: Choice) Error!void {
        if (m.stack.items.len >= max_frames) return Error.PatternTooComplex;
        try m.stack.append(m.gpa, .{ .choice = c, .nconts = @intCast(m.conts.items.len) });
    }

    /// Whether `items` match at `pos`, and where the match ends.
    fn run(m: *Machine, items: []const Item, pos: usize) Error!?usize {
        m.conts.clearRetainingCapacity();
        m.stack.clearRetainingCapacity();
        _ = try m.cont(.done);
        var st: State = .{ .step = .{ .items = items, .i = 0, .rep = 0, .pos = pos, .cont = 0 } };
        while (true) {
            if (m.steps == 0) return Error.PatternTooComplex;
            m.steps -= 1;
            st = switch (st) {
                .step => |x| try m.step(x),
                .resume_ => |x| switch (m.conts.items[x.cont]) {
                    .done => return x.pos,
                    .seq => |q| if (x.pos == q.start and q.rep > q.items[q.i].min and !isUnit(q.items[q.i].atom))
                        .fail
                    else
                        .{ .step = .{ .items = q.items, .i = q.i, .rep = q.rep, .pos = x.pos, .cont = q.next } },
                    .close => |c| blk: {
                        try m.push(.{ .restore = .{ .cap = c.cap, .saved = m.caps[c.cap] } });
                        m.caps[c.cap] = .{ c.start, x.pos };
                        break :blk .{ .resume_ = .{ .cont = c.next, .pos = x.pos } };
                    },
                },
                .fail => try m.backtrack() orelse return null,
            };
        }
    }

    fn backtrack(m: *Machine) Error!?State {
        while (m.stack.pop()) |f| {
            if (f.choice == .restore) {
                m.caps[f.choice.restore.cap] = f.choice.restore.saved;
                continue;
            }
            m.conts.shrinkRetainingCapacity(f.nconts);
            return switch (f.choice) {
                .step => |x| .{ .step = x },
                .more => |x| try m.repeat(x),
                .branch => |x| try m.branch(x.items, x.i, x.b, x.pos, x.cont),
                .units => |x| try m.unitsAgain(x.items, x.i, x.rep, x.start, x.p, x.n, x.cont),
                .restore => unreachable,
            };
        }
        return null;
    }

    /// Match `items[i..]` where the item at `i` has already matched `rep` times.
    /// Greedy tries one more repetition before settling for fewer; lazy inverts
    /// only that order, so the set of reachable matches is the same.
    fn step(m: *Machine, x: StepAt) Error!State {
        if (x.i == x.items.len) return .{ .resume_ = .{ .cont = x.cont, .pos = x.pos } };
        const it = x.items[x.i];
        if (isUnit(it.atom)) return m.units(x);
        const next: StepAt = .{ .items = x.items, .i = x.i + 1, .rep = 0, .pos = x.pos, .cont = x.cont };
        if (it.lazy and x.rep >= it.min) {
            if (x.rep < it.max) try m.push(.{ .more = x });
            return .{ .step = next };
        }
        if (x.rep < it.max) {
            if (x.rep >= it.min) try m.push(.{ .step = next });
            return m.repeat(x);
        }
        return if (x.rep >= it.min) .{ .step = next } else .fail;
    }

    /// One more repetition of the group or anchor at `items[i]`.
    fn repeat(m: *Machine, x: StepAt) Error!State {
        const k = try m.cont(.{ .seq = .{ .items = x.items, .i = x.i, .rep = x.rep + 1, .start = x.pos, .next = x.cont } });
        return switch (x.items[x.i].atom) {
            .bol => if (x.pos == 0) .{ .resume_ = .{ .cont = k, .pos = x.pos } } else .fail,
            .eol => if (x.pos == m.s.len) .{ .resume_ = .{ .cont = k, .pos = x.pos } } else .fail,
            .group => m.branch(x.items, x.i, 0, x.pos, k),
            else => unreachable,
        };
    }

    fn branch(m: *Machine, items: []const Item, i: usize, b: usize, pos: usize, k: u32) Error!State {
        const g = items[i].atom.group;
        if (b + 1 < g.alt.len) try m.push(.{ .branch = .{ .items = items, .i = i, .b = b + 1, .pos = pos, .cont = k } });
        const next = if (g.cap) |ci| try m.cont(.{ .close = .{ .cap = ci, .start = pos, .next = k } }) else k;
        return .{ .step = .{ .items = g.alt[b], .i = 0, .rep = 0, .pos = pos, .cont = next } };
    }

    /// A repeated single-unit atom (literal, `.`, class) walked in a loop, one choice
    /// point for all its repetitions: greedy takes every unit it can, then gives them
    /// back one at a time; lazy takes one more only when the rest fails.
    fn units(m: *Machine, x: StepAt) Error!State {
        const it = x.items[x.i];
        var n = x.rep;
        var p = x.pos;
        const want = if (it.lazy) it.min else it.max;
        while (n < want) {
            p = unitMatch(it.atom, m.s, p) orelse break;
            n += 1;
        }
        if (n < it.min) return .fail;
        const again = if (it.lazy) n < it.max else n > x.rep and n > it.min;
        if (again) try m.push(.{ .units = .{ .items = x.items, .i = x.i, .rep = x.rep, .start = x.pos, .p = p, .n = n, .cont = x.cont } });
        return .{ .step = .{ .items = x.items, .i = x.i + 1, .rep = 0, .pos = p, .cont = x.cont } };
    }

    fn unitsAgain(m: *Machine, items: []const Item, i: usize, rep: u32, start: usize, p0: usize, n0: u32, k: u32) Error!State {
        const it = items[i];
        const p = if (it.lazy) unitMatch(it.atom, m.s, p0) orelse return .fail else unitBefore(m.s, start, p0);
        const n = if (it.lazy) n0 + 1 else n0 - 1;
        const again = if (it.lazy) n < it.max else n > rep and n > it.min;
        if (again) try m.push(.{ .units = .{ .items = items, .i = i, .rep = rep, .start = start, .p = p, .n = n, .cont = k } });
        return .{ .step = .{ .items = items, .i = i + 1, .rep = 0, .pos = p, .cont = k } };
    }
};

fn isUnit(a: Atom) bool {
    return switch (a) {
        .lit, .any, .class => true,
        else => false,
    };
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
    try std.testing.expectEqual([2]usize{ 0, n }, (try grp.find(run, 0, &caps)).?);
    try std.testing.expectEqual([2]usize{ n - 1, n }, caps[1].?);
    const alt = try Regex.compile(a, "^(?:a|b)*?(a)$");
    try std.testing.expectEqual([2]usize{ 0, n }, (try alt.find(run, 0, &caps)).?);
    const huge = try std.testing.allocator.alloc(u8, Machine.max_frames);
    defer std.testing.allocator.free(huge);
    @memset(huge, 'a');
    try std.testing.expectError(Error.PatternTooComplex, grp.find(huge, 0, &caps));

    const long = "b" ** 200_000 ++ "zq";
    const lit = try Regex.compile(a, "zq");
    try std.testing.expectEqual([2]usize{ 200_000, 200_002 }, (try lit.find(long, 0, &caps)).?);
}

test "regex: a repetition of a group that matches nothing ends the loop" {
    var buf: [16 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    var caps: Captures = undefined;

    try std.testing.expectEqual([2]usize{ 0, 3 }, (try (try Regex.compile(a, "^(a*)*b$")).find("aab", 0, &caps)).?);
    try std.testing.expectEqual([2]usize{ 0, 3 }, (try (try Regex.compile(a, "(a|)+b")).find("aab", 0, &caps)).?);
    try std.testing.expectEqual([2]usize{ 0, 0 }, (try (try Regex.compile(a, "^(a?){2}$")).find("", 0, &caps)).?);
    try std.testing.expect((try (try Regex.compile(a, "^(a*)+$")).find("aab", 0, &caps)) == null);
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
