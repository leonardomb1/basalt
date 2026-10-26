//! Source-scan tests that keep centralized logic from being duplicated again.
//!
//! A `Rule` names a home module, a pattern that marks a private copy of that
//! home's logic, and an allowlist of the files under src/ that hold such a copy
//! today. `scan` walks every .zig file under src/, blanks out comments and (by
//! default) test blocks so prose and fixtures never count, and matches the
//! pattern over what is left. A hit outside the allowlist is a stray copy.
//! This file is never scanned, since it spells every pattern out.
//!
//! Two modes: `.frozen` fails only on stray hits, so call sites may migrate to
//! the home (leaving allowlist entries stale) without touching the rule;
//! `.exact` also fails on stale entries, which is how a list is shrunk to the
//! home alone once its migration is done.
//!
//! Patterns are exec/regex.zig syntax. By default each line of the masked
//! file is matched on its own, so `.*` stops at the line end and `^ $` anchor
//! to the line; `span = .file` matches the whole masked file at once for
//! patterns that run over several lines (there `.` also matches a newline, so
//! bound the gap with a class, as `guard_pattern` does with `[^{}]`).

const std = @import("std");
const regex = @import("../exec/regex.zig");

pub const Rule = struct {
    home: []const u8,
    pattern: []const u8,
    under: []const u8 = "",
    allow: []const []const u8 = &.{},
    tests: bool = false,
    span: Span = .line,
    exact: bool = false,
};

pub const Span = enum { line, file };

pub const Mode = enum { frozen, exact };

pub const Hit = struct {
    path: []const u8,
    line: usize,
    text: []const u8,
};

pub const Report = struct {
    arena: std.heap.ArenaAllocator,
    hits: []const Hit,
    stray: []const Hit,
    stale: []const []const u8,

    pub fn deinit(self: *Report) void {
        self.arena.deinit();
    }

    /// True when the rule holds under `mode`.
    pub fn ok(self: Report, mode: Mode) bool {
        return self.stray.len == 0 and (mode == .frozen or self.stale.len == 0);
    }
};

const max_file = 16 << 20;
const self_path = "testing/dupscan.zig";

/// Every src-relative .zig path under `src`, sorted, with '/' separators.
pub fn sources(arena: std.mem.Allocator, src: std.fs.Dir) ![]const []const u8 {
    var list: std.ArrayList([]const u8) = .empty;
    var walker = try src.walk(arena);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".zig")) continue;
        const p = try arena.dupe(u8, entry.path);
        std.mem.replaceScalar(u8, p, std.fs.path.sep, '/');
        try list.append(arena, p);
    }
    std.mem.sort([]const u8, list.items, {}, lessPath);
    return list.items;
}

fn lessPath(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

/// Matches every rule over every source under `src`, reading and masking each
/// file once, and returns one report per rule in order; free each with deinit
/// and the slice with `gpa`.
pub fn scanAll(gpa: std.mem.Allocator, src: std.fs.Dir, rules: []const Rule) ![]Report {
    const Acc = struct {
        pat: Pattern,
        hits: std.ArrayList(Hit) = .empty,
        stray: std.ArrayList(Hit) = .empty,
        seen: []bool,
    };
    const reports = try gpa.alloc(Report, rules.len);
    var made: usize = 0;
    errdefer {
        for (reports[0..made]) |*r| r.deinit();
        gpa.free(reports);
    }
    const accs = try gpa.alloc(Acc, rules.len);
    defer gpa.free(accs);
    for (rules, reports, accs) |rule, *r, *acc| {
        r.* = .{ .arena = .init(gpa), .hits = &.{}, .stray = &.{}, .stale = &.{} };
        made += 1;
        const arena = r.arena.allocator();
        acc.* = .{ .pat = try Pattern.compile(arena, rule.pattern), .seen = try arena.alloc(bool, rule.allow.len) };
        @memset(acc.seen, false);
    }

    var scratch: std.heap.ArenaAllocator = .init(gpa);
    defer scratch.deinit();
    for (try sources(scratch.allocator(), src)) |path| {
        if (std.mem.eql(u8, path, self_path)) continue;
        const source = try src.readFileAllocOptions(gpa, path, max_file, null, .of(u8), 0);
        defer gpa.free(source);
        var masked: [2]?[]u8 = .{ null, null };
        defer for (masked) |m| if (m) |x| gpa.free(x);
        for (rules, reports, accs) |rule, *r, *acc| {
            if (!std.mem.startsWith(u8, path, rule.under) or !acc.pat.mayMatch(source)) continue;
            const slot = &masked[@intFromBool(rule.tests)];
            if (slot.* == null) slot.* = try mask(gpa, source, rule.tests);
            const starts = try matchStarts(gpa, acc.pat, slot.*.?, rule.span);
            defer gpa.free(starts);
            if (starts.len == 0) continue;
            const arena = r.arena.allocator();
            const allowed = indexOfPath(rule.allow, path);
            if (allowed) |i| acc.seen[i] = true;
            const owned = try arena.dupe(u8, path);
            for (starts) |at| {
                const hit: Hit = .{ .path = owned, .line = lineOf(source, at), .text = try arena.dupe(u8, lineText(source, at)) };
                try acc.hits.append(arena, hit);
                if (allowed == null) try acc.stray.append(arena, hit);
            }
        }
    }

    for (rules, reports, accs) |rule, *r, *acc| {
        const arena = r.arena.allocator();
        var stale: std.ArrayList([]const u8) = .empty;
        for (rule.allow, acc.seen) |p, seen| if (!seen) try stale.append(arena, p);
        r.hits = acc.hits.items;
        r.stray = acc.stray.items;
        r.stale = stale.items;
    }
    return reports;
}

/// `scanAll` for one rule.
pub fn scan(gpa: std.mem.Allocator, src: std.fs.Dir, rule: Rule) !Report {
    const reports = try scanAll(gpa, src, &.{rule});
    defer gpa.free(reports);
    return reports[0];
}

/// A compiled rule pattern plus, per top-level alternative, a literal that any
/// match of that alternative contains; a text holding none cannot match.
pub const Pattern = struct {
    re: regex.Regex,
    needles: ?[]const []const u8,

    pub fn compile(arena: std.mem.Allocator, pattern: []const u8) !Pattern {
        return .{ .re = try regex.Regex.compile(arena, pattern), .needles = try requiredLiterals(arena, pattern) };
    }

    /// False only when no match can occur in `text`.
    pub fn mayMatch(self: Pattern, text: []const u8) bool {
        const needles = self.needles orelse return true;
        for (needles) |n| if (std.mem.indexOf(u8, text, n) != null) return true;
        return false;
    }
};

/// The longest literal run each top-level alternative of `pattern` requires
/// (the first, on a tie), or null when some alternative requires none.
pub fn requiredLiterals(arena: std.mem.Allocator, pattern: []const u8) !?[]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var cur: std.ArrayList(u8) = .empty;
    var best: []const u8 = "";
    var depth: usize = 0;
    var i: usize = 0;
    while (i <= pattern.len) {
        const c: u8 = if (i < pattern.len) pattern[i] else '|';
        const top = depth == 0;
        i += 1;
        var keep = false;
        switch (c) {
            '\\' => {
                if (i >= pattern.len) return null;
                const e = pattern[i];
                i += 1;
                if (top and !std.ascii.isAlphanumeric(e)) {
                    try cur.append(arena, e);
                    keep = true;
                }
            },
            '(' => depth += 1,
            ')' => depth -|= 1,
            '[' => {
                if (i < pattern.len and pattern[i] == '^') i += 1;
                if (i < pattern.len and pattern[i] == ']') i += 1;
                while (i < pattern.len and pattern[i] != ']') i += if (pattern[i] == '\\') 2 else 1;
                i += 1;
            },
            '?', '*', '+', '{' => {
                if (top) {
                    while (cur.items.len > 0 and cur.items[cur.items.len - 1] & 0xC0 == 0x80) cur.items.len -= 1;
                    if (cur.items.len > 0) cur.items.len -= 1;
                }
                if (c == '{') i = (std.mem.indexOfScalarPos(u8, pattern, i, '}') orelse return null) + 1;
            },
            '.', '^', '$', '|' => {},
            else => if (top) {
                try cur.append(arena, c);
                keep = true;
            },
        }
        if (keep) continue;
        if (cur.items.len > best.len) best = try arena.dupe(u8, cur.items);
        cur.clearRetainingCapacity();
        if (c == '|' and top) {
            if (best.len == 0) return null;
            try out.append(arena, best);
            best = "";
        }
    }
    return out.items;
}

/// Start offsets of the leftmost non-overlapping matches in `text`, matched
/// per line or over the whole text; an empty match moves on by one byte.
pub fn matchStarts(gpa: std.mem.Allocator, pat: Pattern, text: []const u8, span: Span) ![]usize {
    var out: std.ArrayList(usize) = .empty;
    errdefer out.deinit(gpa);
    var caps: regex.Captures = undefined;
    var base: usize = 0;
    while (base <= text.len) {
        const end = if (span == .file) text.len else std.mem.indexOfScalarPos(u8, text, base, '\n') orelse text.len;
        const piece = text[base..end];
        var from: usize = 0;
        while (from <= piece.len and pat.mayMatch(piece[from..])) {
            const m = try pat.re.find(piece, from, &caps) orelse break;
            try out.append(gpa, base + m[0]);
            from = if (m[1] > m[0]) m[1] else m[0] + 1;
        }
        base = end + 1;
    }
    return out.toOwnedSlice(gpa);
}

fn indexOfPath(list: []const []const u8, path: []const u8) ?usize {
    for (list, 0..) |p, i| if (std.mem.eql(u8, p, path)) return i;
    return null;
}

fn lineOf(source: []const u8, at: usize) usize {
    return std.mem.count(u8, source[0..at], "\n") + 1;
}

fn lineText(source: []const u8, at: usize) []const u8 {
    const start = if (std.mem.lastIndexOfScalar(u8, source[0..at], '\n')) |n| n + 1 else 0;
    const end = std.mem.indexOfScalarPos(u8, source, at, '\n') orelse source.len;
    return std.mem.trim(u8, source[start..end], " \t\r");
}

/// Prints why `report` fails `rule` under `mode`.
pub fn explain(rule: Rule, report: Report, mode: Mode) void {
    for (report.stray) |h| std.debug.print("dupscan {s}: {s}:{d} copies what the home holds: {s}\n", .{ rule.home, h.path, h.line, h.text });
    if (mode == .exact) for (report.stale) |p| std.debug.print("dupscan {s}: {s} no longer matches; drop it from the allowlist\n", .{ rule.home, p });
}

/// Fails the calling test when any rule finds a stray copy, or under `.exact`
/// a stale allowlist entry; every failure is printed before returning.
pub fn expectConfined(rules: []const Rule, mode: Mode) !void {
    const gpa = std.testing.allocator;
    var src = try openSrc();
    defer src.close();
    const reports = try scanAll(gpa, src, rules);
    defer {
        for (reports) |*r| r.deinit();
        gpa.free(reports);
    }
    var failed = false;
    for (rules, reports) |rule, r| {
        const m: Mode = if (rule.exact) .exact else mode;
        if (r.ok(m)) continue;
        explain(rule, r, m);
        failed = true;
    }
    if (failed) return error.TestUnexpectedResult;
}

/// Opens the repository's src/ from the working directory or the nearest
/// ancestor holding build.zig and src/root.zig.
pub fn openSrc() !std.fs.Dir {
    var dir = try std.fs.cwd().openDir(".", .{});
    var depth: usize = 0;
    while (depth < 16) : (depth += 1) {
        if (isRepo(dir)) {
            defer dir.close();
            return dir.openDir("src", .{ .iterate = true });
        }
        const up = dir.openDir("..", .{}) catch break;
        dir.close();
        dir = up;
    }
    dir.close();
    return error.FileNotFound;
}

fn isRepo(dir: std.fs.Dir) bool {
    dir.access("build.zig", .{}) catch return false;
    dir.access("src/root.zig", .{}) catch return false;
    return true;
}

/// A copy of `source` where comments, doc comments and (unless `tests`) test
/// blocks are spaces; newlines stay so byte offsets and line numbers agree.
pub fn mask(gpa: std.mem.Allocator, source: [:0]const u8, tests: bool) ![]u8 {
    const out = try gpa.alloc(u8, source.len);
    for (out, source) |*o, c| o.* = if (c == '\n') '\n' else ' ';
    var tok: std.zig.Tokenizer = .init(source);
    var depth: usize = 0;
    var skip_at: ?usize = null;
    var skip_open = false;
    while (true) {
        const t = tok.next();
        if (t.tag == .eof) break;
        switch (t.tag) {
            .l_brace => {
                depth += 1;
                if (skip_at != null) skip_open = true;
            },
            .r_brace => depth -|= 1,
            .keyword_test => if (!tests and skip_at == null) {
                skip_at = depth;
                skip_open = false;
            },
            else => {},
        }
        const in_test = skip_at != null;
        if (in_test and skip_open and t.tag == .r_brace and depth == skip_at.?) {
            skip_at = null;
            continue;
        }
        if (in_test) continue;
        if (t.tag == .doc_comment or t.tag == .container_doc_comment) continue;
        @memcpy(out[t.loc.start..t.loc.end], source[t.loc.start..t.loc.end]);
    }
    return out;
}

/// The enforcement patterns from the centralization plan, each frozen at the
/// files that hold a copy today; the leading entries are the home and the new
/// modules that may legitimately hold one, some not yet written. Wave 2 shrinks
/// each list to its homes and switches to `.exact`.
pub const plan = [_]Rule{
    .{ .home = "civil", .pattern = civil_pattern, .exact = true, .allow = &.{"exec/civil.zig"} },
    .{ .home = "civil.Part", .pattern = part_pattern, .exact = true, .allow = &.{"exec/civil.zig"} },
    .{ .home = "textconv", .pattern = textconv_pattern, .exact = true, .allow = &textconv_allow },
    .{ .home = "floatord", .pattern = floatord_pattern, .exact = true, .allow = &.{"exec/floatord.zig"} },
    .{ .home = "decimal", .pattern = decimal_pattern, .exact = true, .allow = &.{"exec/decimal.zig"} },
    .{ .home = "jsontext", .pattern = jsontext_pattern, .allow = &.{
        "exec/jsontext.zig",
        "format/lake/delta_types.zig",
        "cli/args.zig",
        "cli/cmd_complete.zig",
        "cli/kernel.zig",
        "exec/eval/fn_row.zig",
        "exec/eval/fn_scalar.zig",
        "exec/eval/row.zig",
        "exec/json.zig",
        "exec/trace.zig",
        "format/lake/delta_write.zig",
        "format/parquet/nested.zig",
        "runtime/obs.zig",
    } },
    .{ .home = "hex", .pattern = hex_pattern, .allow = &.{
        "format/hex.zig",
        "format/uuid.zig",
        "store/scheme.zig",
        "exec/eval/fn_records.zig",
        "format/xml/token.zig",
    } },
    .{ .home = "utf16", .pattern = utf16_pattern, .allow = &.{
        "format/utf16.zig",
        "db/sqlserver/tds.zig",
        "net/auth/ntlm.zig",
        "store/smb.zig",
    } },
    .{ .home = "scheme", .pattern = scheme_pattern, .exact = true },
    .{ .home = "dial", .pattern = dial_pattern, .exact = true, .allow = &.{
        "connect/driver.zig",
    } },
    .{ .home = "options", .pattern = options_pattern, .exact = true, .allow = &options_allow },
    .{ .home = "retry", .pattern = retry_pattern, .exact = true, .allow = &.{
        "connect/retry.zig",
    } },
    .{ .home = "transient", .pattern = transient_pattern, .exact = true, .allow = &.{
        "connect/driver.zig",
    } },
    .{ .home = "queue", .pattern = queue_pattern, .exact = true, .allow = &.{
        "runtime/parallel.zig",
    } },
    .{ .home = "sql", .pattern = sql_pattern, .under = "db/", .exact = true, .allow = &.{
        "db/sql.zig",
    } },
    .{ .home = "guard", .pattern = guard_pattern, .span = .file, .exact = true, .allow = &.{
        "connect/guard.zig",
    } },
    .{ .home = "delta_types", .pattern = delta_types_pattern, .exact = true, .allow = &.{
        "format/lake/delta_types.zig",
    } },
    .{ .home = "unsigned", .pattern = unsigned_pattern, .exact = true, .allow = &.{
        "lang/types.zig",
    } },
    .{ .home = "widen", .pattern = widen_pattern, .exact = true, .allow = &.{
        "lang/types.zig",
    } },
};

pub const civil_pattern = "719_?468|146_?097|719_?16[23]|25_?56[789]|24_?107|2_?440_?588|86_400_000_000|std\\.time\\.epoch\\.";
pub const part_pattern = "\"(year|month|week|day|hour|minute|second)s?\", \\.(year|month|week|day|hour|minute|second)|eqlIgnoreCase\\([^\"]*\"(year|month|week|day|hour|minute|second)s?\"|stringToEnum\\(Part,";
pub const textconv_pattern = "(eqlIgnoreCase\\(|eql\\(u8, )[^\"]*\"(true|false|yes|no)\"\\)|\\[0\\] == '[tTyY]'|0x1p63";
pub const floatord_pattern = "0x7[fF][fF]8|>> 63 != 0\\) ~|\\| \\(1 << 63\\)|std\\.math\\.order\\([^)]*\\.float";
pub const decimal_pattern = "fn pow10|std\\.math\\.powi?\\(i(64|128|256), 10,|@(divTrunc|rem|mod|divFloor|divExact)\\(\\w+\\.unscaled, ";
pub const jsontext_pattern = "encodeJsonString(Chars)?";
pub const hex_pattern = "parseInt\\(.*16\\)";
pub const utf16_pattern = "utf16LeToUtf8Alloc|utf8ToUtf16LeAlloc";
pub const scheme_pattern = "startsWith\\(u8, .*\"(sftp|s3|az|smb|ftp|https?)://";
pub const dial_pattern = "tcpConnectToHost";
pub const options_pattern = "@intCast\\(try evalCfgInt|\\.str_lit => \\|\\w+\\| .*(parseInt\\(i64, \\w+, 10\\)|IgnoreCase\\(\\w+, \"true\"\\))";
pub const retry_pattern = "(^|[^a-zA-Z_])sleep\\([a-z_@]";
pub const transient_pattern = "== (408|429|50[0234])([^0-9]|$)|!= (408|429|50[0234])([^0-9]|$)|>= 500([^0-9]|$)|408, 429 ?(,|=>)";

/// testing_util.zig keeps the option readers as they were before options_schema,
/// the oracle the home's tests compare it against; reading through the home would
/// make those tests compare the home with itself.
/// testing_util.zig's option oracle reads `true`/`false` as it did before the
/// homes, for the same reason it keeps its own option readers (`options_allow`).
const textconv_allow = [_][]const u8{
    "exec/textconv.zig",
    "runtime/connect/testing_util.zig",
};

const options_allow = [_][]const u8{
    "runtime/connect/options_schema.zig",
    "runtime/connect/testing_util.zig",
};
pub const queue_pattern = "next\\.fetchAdd\\(";
pub const sql_pattern = "\"''\"|== '\\\\''|'\\\\'' =>";
pub const guard_pattern = "errdefer \\w+\\.abort\\(\\);([^{}]|\\{[^{}]*\\})*try \\w+\\.close\\(";
pub const delta_types_pattern = "\"timestamp_ntz\"|\"decimal\\(\"";
pub const unsigned_pattern = "decimal\\(20, ?0\\)";
pub const widen_pattern = "== \\.int and \\(\\S+ == \\.decimal or|== \\.decimal and \\S+ == \\.float";

fn startsOf(pattern: []const u8, text: []const u8, span: Span) ![]usize {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const pat = try Pattern.compile(arena.allocator(), pattern);
    return matchStarts(std.testing.allocator, pat, text, span);
}

fn expectStarts(pattern: []const u8, text: []const u8, span: Span, want: []const usize) !void {
    const got = try startsOf(pattern, text, span);
    defer std.testing.allocator.free(got);
    std.testing.expectEqualSlices(usize, want, got) catch |e| {
        std.debug.print("pattern {s} on {s}\n", .{ pattern, text });
        return e;
    };
}

fn expectNeedles(pattern: []const u8, want: ?[]const []const u8) !void {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const got = try requiredLiterals(arena.allocator(), pattern);
    errdefer std.debug.print("pattern {s}\n", .{pattern});
    if (want) |w| {
        const g = got orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(w.len, g.len);
        for (w, g) |x, y| try std.testing.expectEqualStrings(x, y);
    } else try std.testing.expectEqual(@as(?[]const []const u8, null), got);
}

test "dupscan requiredLiterals: the longest literal each alternative needs" {
    try expectNeedles("abc", &.{"abc"});
    try expectNeedles(civil_pattern, &.{ "719", "146", "719", "25", "107", "440", "86_400_000_000", "std.time.epoch." });
    try expectNeedles(part_pattern, &.{ "\", .", "eqlIgnoreCase(", "stringToEnum(Part," });
    try expectNeedles(textconv_pattern, &.{ "\")", "[0] == '", "0x1p63" });
    try expectNeedles(floatord_pattern, &.{ "0x7", ">> 63 != 0) ~", "| (1 << 63)", "std.math.order(" });
    try expectNeedles(decimal_pattern, &.{ "fn pow10", "std.math.pow", ".unscaled, " });
    try expectNeedles(jsontext_pattern, &.{"encodeJsonString"});
    try expectNeedles(scheme_pattern, &.{"startsWith(u8, "});
    try expectNeedles(sql_pattern, &.{ "\"''\"", "== '\\''", "'\\'' =>" });
    try expectNeedles(hex_pattern, &.{"parseInt("});
    try expectNeedles(guard_pattern, &.{"errdefer "});
    try expectNeedles(options_pattern, &.{ "@intCast(try evalCfgInt", ".str_lit => |" });
    try expectNeedles(retry_pattern, &.{"sleep("});
    try expectNeedles(transient_pattern, &.{ "== ", "!= ", ">= 500", "408, 429" });
    try expectNeedles("ab+c", &.{"a"});
    try expectNeedles("ab+cd", &.{"cd"});
    try expectNeedles("a{2,3}bc", &.{"bc"});
    try expectNeedles("x(ab)?yzw", &.{"yzw"});
    try expectNeedles("a[|]b|c", &.{ "a", "c" });
    try expectNeedles("a(b|c)d|e", &.{ "a", "e" });
    try expectNeedles("^pub", &.{"pub"});
    try expectNeedles("a|.b", &.{ "a", "b" });
    try expectNeedles("\\w+x", &.{"x"});
    try expectNeedles("a\\.b", &.{"a.b"});
    try expectNeedles("xyçã?z", &.{"xyç"});
    try expectNeedles("x*", null);
    try expectNeedles("(ab)", null);
    try expectNeedles("a|", null);
    try expectNeedles("", null);
}

test "dupscan matchStarts per line and per file" {
    try expectStarts("aa", "aaaaa aa", .line, &.{ 0, 2, 6 });
    try expectStarts("x*", "ab", .line, &.{ 0, 1, 2 });
    try expectStarts("[0-9]+", "12 345\n6", .line, &.{ 0, 3, 7 });
    try expectStarts("zz", "", .line, &.{});
    try expectStarts("^b", "ab\nbc\nb", .line, &.{ 3, 6 });
    try expectStarts("b$", "ab\nbc\nb", .line, &.{ 1, 6 });
    try expectStarts("^b", "ab\nbc\nb", .file, &.{});
    try expectStarts("parseInt\\(.*16\\)", "parseInt(u8, s,\n 16)", .line, &.{});
    try expectStarts("parseInt\\(.*16\\)", "parseInt(u8, s,\n 16)", .file, &.{0});
    try expectStarts("a\\sb", "a\nb", .file, &.{0});
    try expectStarts("a\\sb", "a\nb", .line, &.{});
}

test "dupscan plan patterns: what each one catches and what it lets through" {
    const cases = [_]struct { []const u8, []const u8, bool }{
        .{ civil_pattern, "const doe = z + 719468;", true },
        .{ civil_pattern, "const doe = z + 719_468;", true },
        .{ civil_pattern, "era * 146_097", true },
        .{ civil_pattern, "719_163", true },
        .{ civil_pattern, "719162", true },
        .{ civil_pattern, "719164", false },
        .{ civil_pattern, "25_569.0", true },
        .{ civil_pattern, "24107", true },
        .{ civil_pattern, "2_440_588", true },
        .{ civil_pattern, "86_400_000_000", true },
        .{ civil_pattern, "86400000000", false },
        .{ civil_pattern, "std.time.epoch.EpochSeconds", true },
        .{ civil_pattern, "std.time.timestamp()", false },
        .{ part_pattern, ".{ \"hours\", .hour },", true },
        .{ part_pattern, "if (std.ascii.eqlIgnoreCase(u, \"minutes\")) return .minute;", true },
        .{ part_pattern, "std.meta.stringToEnum(Part, low)", true },
        .{ part_pattern, "civil.Part.parse(word)", false },
        .{ part_pattern, ".{ .name = \"day\", .ty = types.Type.init(.date) },", false },
        .{ textconv_pattern, "return std.mem.eql(u8, a.value, \"true\") or std.mem.eql(u8, a.value, \"1\");", true },
        .{ textconv_pattern, "if (std.ascii.eqlIgnoreCase(v, \"true\"))", true },
        .{ textconv_pattern, "raw.len > 0 and (raw[0] == '1' or raw[0] == 't' or raw[0] == 'T')", true },
        .{ textconv_pattern, "if (!(r < 0x1p63)) return null;", true },
        .{ textconv_pattern, "std.mem.eql(u8, tls.?, \"off\")", false },
        .{ textconv_pattern, "textconv.boolText(.xsd_nil, a.value).?", false },
        .{ textconv_pattern, ".bool => |b| if (b) \"true\" else \"false\",", false },
        .{ floatord_pattern, "const b: u64 = if (std.math.isNan(y)) 0x7ff8000000000000 else @bitCast(y);", true },
        .{ floatord_pattern, "return if (b >> 63 != 0) ~b else b | (1 << 63);", true },
        .{ floatord_pattern, "return std.math.order(a.float, b.float);", true },
        .{ floatord_pattern, ".ints => |v| @as(u64, @bitCast(v[i])) ^ (1 << 63),", false },
        .{ floatord_pattern, "return std.math.order(a.int, b.int);", false },
        .{ floatord_pattern, "floatord.orderedBits(x)", false },
        .{ decimal_pattern, "fn pow10(n: u8) i64 {", true },
        .{ decimal_pattern, "const p = std.math.powi(i128, 10, scale) catch return off_max;", true },
        .{ decimal_pattern, "const top: i128 = std.math.pow(i128, 10, 38) - 1;", true },
        .{ decimal_pattern, "const q = @divTrunc(d.unscaled, p);", true },
        .{ decimal_pattern, "if (@rem(d.unscaled, p) != 0) return null;", true },
        .{ decimal_pattern, "if (!decimal.withinPrecision(r.unscaled, p)) return error.X;", false },
        .{ decimal_pattern, "std.math.mul(i128, x.unscaled, plan.fl)", false },
        .{ decimal_pattern, "const p = decimal.pow10Checked(s) orelse return off_max;", false },
        .{ decimal_pattern, "std.math.pow(f64, 10, @floatFromInt(scale))", false },
        .{ jsontext_pattern, "std.json.encodeJsonString(s, .{}, w)", true },
        .{ jsontext_pattern, "std.json.encodeJsonStringChars(s, .{}, w)", true },
        .{ jsontext_pattern, "jsontext.string(w, s)", false },
        .{ hex_pattern, "std.fmt.parseInt(u8, s[i..][0..2], 16)", true },
        .{ hex_pattern, "std.fmt.parseInt(u8, s, 10)", false },
        .{ utf16_pattern, "std.unicode.utf16LeToUtf8Alloc(a, w)", true },
        .{ utf16_pattern, "std.unicode.utf8ToUtf16LeAlloc(a, s)", true },
        .{ utf16_pattern, "std.unicode.utf8ToUtf16Le(buf, s)", false },
        .{ scheme_pattern, "if (std.mem.startsWith(u8, url, \"sftp://\")) {", true },
        .{ scheme_pattern, "std.mem.startsWith(u8, p, \"https://\")", true },
        .{ scheme_pattern, "std.mem.startsWith(u8, p, \"http://\")", true },
        .{ scheme_pattern, "std.mem.startsWith(u8, p, \"s3://\")", true },
        .{ scheme_pattern, "std.mem.startsWith(u8, p, \"abfss://\")", false },
        .{ dial_pattern, "std.net.tcpConnectToHost(gpa, host, port)", true },
        .{ options_pattern, "const p: u16 = @intCast(try evalCfgInt(cx, e));", true },
        .{ options_pattern, "std.math.cast(u16, try evalCfgInt(cx, e))", false },
        .{ scheme_pattern, "std.mem.startsWith(u8, p, \"az://\")", true },
        .{ scheme_pattern, "for ([_][]const u8{ \"sftp://\", \"smb://\" }) |pre| if (std.mem.startsWith(u8, root, pre))", false },
        .{ options_pattern, ".str_lit => |s| std.fmt.parseInt(i64, s, 10) catch null,", true },
        .{ options_pattern, ".str_lit => |s| if (std.fmt.parseInt(i64, s, 10)) |_| null else |_| int_cfg_msg,", true },
        .{ options_pattern, ".str_lit => |s| if (std.ascii.eqlIgnoreCase(s, \"true\"))", true },
        .{ options_pattern, ".str, .ident => |s| std.fmt.parseInt(i64, s, 10) catch null,", false },
        .{ options_pattern, "return if (std.ascii.eqlIgnoreCase(s, \"true\")) true else null;", false },
        .{ options_pattern, ".str_lit => |s| cfgBool(s),", false },
        .{ retry_pattern, "std.Thread.sleep(backoff_ns);", true },
        .{ retry_pattern, "std.Thread.sleep(delay * std.time.ns_per_ms);", true },
        .{ retry_pattern, "std.Thread.sleep(@min(wait, cap) * std.time.ns_per_ms);", true },
        .{ retry_pattern, "sleep(step * std.time.ns_per_ms);", true },
        .{ retry_pattern, "std.Thread.sleep(25 * std.time.ns_per_ms);", false },
        .{ retry_pattern, "while (!done and waited < 500) : (waited += 1) std.Thread.sleep(20 * std.time.ns_per_ms);", false },
        .{ retry_pattern, "if (!retry.pauseWith(std.Thread.sleep, 500)) return e;", false },
        .{ retry_pattern, "sleep: objstore.Sleep = std.Thread.sleep,", false },
        .{ retry_pattern, "std.posix.nanosleep(s, ns);", false },
        .{ transient_pattern, "if (code == 429) return true;", true },
        .{ transient_pattern, "if (status == 503 or status == 504)", true },
        .{ transient_pattern, "return r.code != 503", true },
        .{ transient_pattern, "else => code >= 500 and code < 600,", true },
        .{ transient_pattern, "408, 429 => true,", true },
        .{ transient_pattern, "408, 429, 500, 502, 503, 504 => true,", true },
        .{ transient_pattern, "if (code >= 400 and code < 500) return error.FtpServerBusy;", false },
        .{ transient_pattern, "if (code == 530) return error.FtpLoginFailed;", false },
        .{ transient_pattern, "if (code == 550) return error.FileNotFound;", false },
        .{ transient_pattern, "if (r.code != 125 and r.code != 150) return refusal(r.code, r.text);", false },
        .{ transient_pattern, "if (hello.code != 220) return refusal(hello.code, hello.text);", false },
        .{ transient_pattern, "501 => {", false },
        .{ transient_pattern, "while (waited < 500) : (waited += 1)", false },
        .{ transient_pattern, "if (n == 5000) break;", false },
        .{ transient_pattern, "base_ms: u64 = 500,", false },
        .{ queue_pattern, "const i = queue.next.fetchAdd(1, .monotonic);", true },
        .{ sql_pattern, "try w.writeAll(\"''\");", true },
        .{ sql_pattern, "try w.writeAll(\"'\");", false },
    };
    for (cases) |c| {
        const got = try startsOf(c[0], c[1], .line);
        defer std.testing.allocator.free(got);
        std.testing.expectEqual(c[2], got.len != 0) catch |e| {
            std.debug.print("pattern {s} on {s}\n", .{ c[0], c[1] });
            return e;
        };
    }
    try expectStarts(guard_pattern, "errdefer w.abort();\n    try body(w);\n    try w.close();", .file, &.{0});
    try expectStarts(guard_pattern, "  errdefer w.abort();\n}\nfn g() !void {\n    try w.close();", .file, &.{});
    try expectStarts(guard_pattern, "errdefer w.abort();\n    try w.finish();\n", .file, &.{});
    try expectStarts(guard_pattern, "x();\n    errdefer w.abort();\n    try w.writeBatch(a, .{ .n = n });\n    try w.close();", .file, &.{9});
    try expectStarts(guard_pattern, "errdefer w.abort();\n    if (a) { b(); }\n    try w.close();", .file, &.{0});
}

test "dupscan queue, sql, guard, delta_types, unsigned and widen patterns" {
    try expectNeedles(queue_pattern, &.{"next.fetchAdd("});
    try expectNeedles(delta_types_pattern, &.{ "\"timestamp_ntz\"", "\"decimal(\"" });
    try expectNeedles(unsigned_pattern, &.{"decimal(20,"});
    try expectNeedles(widen_pattern, &.{ " == .decimal or", "== .decimal and " });
    const cases = [_]struct { []const u8, []const u8, bool }{
        .{ queue_pattern, "const i = queue.next.fetchAdd(1, .monotonic);", true },
        .{ queue_pattern, "const i = q.next.fetchAdd(1, .seq_cst);", true },
        .{ queue_pattern, "const i = ctx.next.fetchAdd(1, .monotonic);", true },
        .{ queue_pattern, "const i = next.fetchAdd(1, order);", true },
        .{ queue_pattern, "_ = ctx.rows_out.fetchAdd(1, .monotonic);", false },
        .{ queue_pattern, "const i = parallel.claim(&ctx.next, n, .monotonic) orelse break;", false },
        .{ sql_pattern, "try w.writeAll(\"''\");", true },
        .{ sql_pattern, "const lit = try std.mem.replaceOwned(u8, a, s, \"'\", \"''\");", true },
        .{ sql_pattern, "if (c == '\\'' or (c == '\\\\' and self.mysqlWire())) try w.writeByte(c);", true },
        .{ sql_pattern, "'\\'' => try out.appendSlice(\"''\"),", true },
        .{ sql_pattern, "switch (c) { '\\'' => n += 1, else => {} }", true },
        .{ sql_pattern, "try w.writeByte('\\'');", false },
        .{ sql_pattern, "try w.writeAll(\"'\");", false },
        .{ delta_types_pattern, ".{ .name = \"timestamp_ntz\", .kind = .timestamp },", true },
        .{ delta_types_pattern, "if (oneOf(t, &.{ \"timestamp\", \"timestamp_ntz\" })) return T.init(.timestamp);", true },
        .{ delta_types_pattern, "if (std.mem.startsWith(u8, t, \"decimal(\") and std.mem.endsWith(u8, t, \")\")) {", true },
        .{ delta_types_pattern, "return std.fmt.allocPrint(arena, \"decimal({d},{d})\", .{ p, s });", false },
        .{ delta_types_pattern, "\"timestamp\"", false },
        .{ unsigned_pattern, "0x08 => if (unsigned) types.Type.decimal(20, 0) else types.Type.init(.int),", true },
        .{ unsigned_pattern, "Type.decimal(20,0)", true },
        .{ unsigned_pattern, "Type.decimal(38, 0)", false },
        .{ unsigned_pattern, "Type.decimal(20, 2)", false },
        .{ widen_pattern, "return (from == .int and (to == .decimal or to == .float)) or (from == .decimal and to == .float);", true },
        .{ widen_pattern, "if (a == .decimal and b == .float) return .float;", true },
        .{ widen_pattern, "if ((build == .int and p == .float) or (build == .float and p == .int)) return f;", false },
        .{ widen_pattern, "if (a.kind == .int and b.kind == .decimal) return unify(d, b);", false },
        .{ widen_pattern, "if (p.ty.kind == .int and (raw == .float or raw == .decimal))", false },
    };
    for (cases) |c| {
        const got = try startsOf(c[0], c[1], .line);
        defer std.testing.allocator.free(got);
        std.testing.expectEqual(c[2], got.len != 0) catch |e| {
            std.debug.print("pattern {s} on {s}\n", .{ c[0], c[1] });
            return e;
        };
    }
    try expectStarts(guard_pattern, "const w = try Writer.open(a, p, m, o);\n    errdefer w.abort();\n    for (bs) |b| try w.writeBatch(a, b);\n    try w.close();", .file, &.{43});
    try expectStarts(guard_pattern, "var g = Guard(*Writer).init(try Writer.open(a, p, m, o));\n    defer g.abort();\n    const w = g.get().?;\n    try g.close();", .file, &.{});
}

test "dupscan mask blanks comments and tests but keeps strings and lines" {
    const gpa = std.testing.allocator;
    const source =
        \\//! header 719468
        \\const a = 1; // tail 719468
        \\/// doc 719468
        \\const s = "719468 // not a comment";
        \\const m =
        \\    \\multi 719468
        \\;
        \\test "t 719468" {
        \\    const x = .{ .y = 719468 };
        \\    _ = x;
        \\}
        \\fn f() void {
        \\    if (true) {}
        \\}
        \\const after = 719468;
    ;
    const out = try mask(gpa, source, false);
    defer gpa.free(out);
    try std.testing.expectEqual(source.len, out.len);
    try std.testing.expectEqual(std.mem.count(u8, source, "\n"), std.mem.count(u8, out, "\n"));
    var arena: std.heap.ArenaAllocator = .init(gpa);
    defer arena.deinit();
    const re = try Pattern.compile(arena.allocator(), "719468");
    const starts = try matchStarts(gpa, re, out, .line);
    defer gpa.free(starts);
    var lines: [3]usize = undefined;
    try std.testing.expectEqual(@as(usize, 3), starts.len);
    for (starts, &lines) |s, *l| l.* = lineOf(source, s);
    try std.testing.expectEqualSlices(usize, &.{ 4, 6, 15 }, &lines);
    try std.testing.expect(std.mem.indexOf(u8, out, "fn f() void") != null);

    const with_tests = try mask(gpa, source, true);
    defer gpa.free(with_tests);
    const t = try matchStarts(gpa, re, with_tests, .line);
    defer gpa.free(t);
    try std.testing.expectEqual(@as(usize, 5), t.len);
}

test "dupscan mask handles bare test blocks and nested braces" {
    const gpa = std.testing.allocator;
    const source =
        \\test {
        \\    _ = struct { const k = 42; };
        \\}
        \\const keep = 42;
        \\test "x" { { { const z = 42; _ = z; } } }
        \\const also = 42;
    ;
    const out = try mask(gpa, source, false);
    defer gpa.free(out);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, out, "42"));
    try std.testing.expect(std.mem.indexOf(u8, out, "const keep = 42;") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "const also = 42;") != null);
}

test "dupscan scan sorts hits into stray and stale" {
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    try tmp.dir.makePath("exec/eval");
    try tmp.dir.makePath("db");
    try tmp.dir.writeFile(.{ .sub_path = "exec/civil.zig", .data = "pub const k = 719_468;\n" });
    try tmp.dir.writeFile(.{ .sub_path = "exec/eval/time.zig", .data = "const a = 1;\nconst b = 719468; const c = 719468;\n" });
    try tmp.dir.writeFile(.{ .sub_path = "db/wire.zig", .data = "// 719468\nconst d = 2;\n" });
    try tmp.dir.writeFile(.{ .sub_path = "db/sql.zig", .data = "\n\n  const e = 146097;\n" });
    try tmp.dir.writeFile(.{ .sub_path = "db/notes.txt", .data = "719468\n" });

    const rule: Rule = .{
        .home = "civil",
        .pattern = "719_?468|146_?097",
        .allow = &.{ "exec/civil.zig", "exec/eval/time.zig", "format/gone.zig" },
    };
    var r = try scan(gpa, tmp.dir, rule);
    defer r.deinit();
    try std.testing.expectEqual(@as(usize, 4), r.hits.len);
    try std.testing.expectEqual(@as(usize, 1), r.stray.len);
    try std.testing.expectEqualStrings("db/sql.zig", r.stray[0].path);
    try std.testing.expectEqual(@as(usize, 3), r.stray[0].line);
    try std.testing.expectEqualStrings("const e = 146097;", r.stray[0].text);
    try std.testing.expectEqual(@as(usize, 1), r.stale.len);
    try std.testing.expectEqualStrings("format/gone.zig", r.stale[0]);
    try std.testing.expectEqualStrings("db/sql.zig", r.hits[0].path);
    try std.testing.expectEqualStrings("exec/civil.zig", r.hits[1].path);
    try std.testing.expectEqual(@as(usize, 2), r.hits[2].line);
    try std.testing.expect(!r.ok(.frozen));

    var scoped = try scan(gpa, tmp.dir, .{ .home = "civil", .pattern = "719_?468|146_?097", .under = "exec/", .allow = rule.allow });
    defer scoped.deinit();
    try std.testing.expect(scoped.ok(.frozen));
    try std.testing.expect(!scoped.ok(.exact));

    var tight = try scan(gpa, tmp.dir, .{ .home = "civil", .pattern = "719_?468", .under = "exec/", .allow = &.{ "exec/civil.zig", "exec/eval/time.zig" } });
    defer tight.deinit();
    try std.testing.expect(tight.ok(.exact));
}

test "dupscan finds the repository src from the working directory" {
    var src = try openSrc();
    defer src.close();
    try src.access("root.zig", .{});
    try src.access("testing/dupscan.zig", .{});
    const rule: Rule = .{ .home = "dupscan", .pattern = "^pub const dupscan = @import\\(\"testing/dupscan\\.zig\"\\);$", .allow = &.{"root.zig"} };
    var r = try scan(std.testing.allocator, src, rule);
    defer r.deinit();
    if (!r.ok(.exact)) explain(rule, r, .exact);
    try std.testing.expect(r.ok(.exact));
    try std.testing.expectEqual(@as(usize, 1), r.hits.len);
    var own = try scan(std.testing.allocator, src, .{ .home = "dupscan", .pattern = "^pub fn matchStarts\\(" });
    defer own.deinit();
    try std.testing.expectEqual(@as(usize, 0), own.hits.len);
}

test "dupscan plan rules hold frozen" {
    try expectConfined(&plan, .frozen);
}
