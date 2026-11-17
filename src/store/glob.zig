//! A local path pattern: `*` any run of a name's characters, `?` one, `[abc]`,
//! `[a-z]` and `[!a]` one from a set, matched a path segment at a time, so no
//! wildcard crosses a `/`. As a shell does, a wildcard does not match a name
//! starting with `.` unless the pattern's segment starts with one. The matches are
//! files, not folders, sorted by path.

const std = @import("std");

/// Whether `path` is a local pattern: no URL scheme, and a `*`, `?` or `[` in it.
pub fn hasGlob(path: []const u8) bool {
    if (std.mem.indexOf(u8, path, "://") != null) return false;
    return std.mem.indexOfAny(u8, path, "*?[") != null;
}

/// The files `pattern` matches, sorted; none is an empty list.
pub fn expand(arena: std.mem.Allocator, pattern: []const u8) ![]const []const u8 {
    var cur = std.array_list.Managed([]const u8).init(arena);
    try cur.append(if (pattern.len > 0 and pattern[0] == '/') "/" else "");
    var it = std.mem.tokenizeScalar(u8, pattern, '/');
    while (it.next()) |seg| {
        var next = std.array_list.Managed([]const u8).init(arena);
        for (cur.items) |base| {
            if (std.mem.indexOfAny(u8, seg, "*?[") == null) {
                try next.append(try join(arena, base, seg));
                continue;
            }
            var dir = std.fs.cwd().openDir(if (base.len == 0) "." else base, .{ .iterate = true }) catch continue;
            defer dir.close();
            var di = dir.iterate();
            while (di.next() catch null) |ent| {
                if (ent.name.len > 0 and ent.name[0] == '.' and seg[0] != '.') continue;
                if (match(seg, ent.name)) try next.append(try join(arena, base, ent.name));
            }
        }
        cur = next;
    }
    var files = std.array_list.Managed([]const u8).init(arena);
    for (cur.items) |p| {
        const st = std.fs.cwd().statFile(p) catch continue;
        if (st.kind != .directory) try files.append(p);
    }
    std.sort.pdq([]const u8, files.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return files.items;
}

fn join(arena: std.mem.Allocator, base: []const u8, seg: []const u8) ![]const u8 {
    if (base.len == 0) return arena.dupe(u8, seg);
    if (std.mem.eql(u8, base, "/")) return std.mem.concat(arena, u8, &.{ "/", seg });
    return std.mem.concat(arena, u8, &.{ base, "/", seg });
}

/// Whether `name` matches the one-segment pattern `pat`.
pub fn match(pat: []const u8, name: []const u8) bool {
    var p: usize = 0;
    var n: usize = 0;
    var star_p: ?usize = null;
    var star_n: usize = 0;
    while (n < name.len) {
        if (p < pat.len) switch (pat[p]) {
            '*' => {
                star_p = p;
                star_n = n;
                p += 1;
                continue;
            },
            '?' => {
                p += 1;
                n += 1;
                continue;
            },
            '[' => if (setAt(pat, p)) |set| {
                if (inSet(set.body, name[n]) != set.negated) {
                    p = set.end;
                    n += 1;
                    continue;
                }
            } else if (name[n] == '[') {
                p += 1;
                n += 1;
                continue;
            },
            else => |c| if (c == name[n]) {
                p += 1;
                n += 1;
                continue;
            },
        };
        const sp = star_p orelse return false;
        p = sp + 1;
        star_n += 1;
        n = star_n;
    }
    while (p < pat.len and pat[p] == '*') p += 1;
    return p == pat.len;
}

const Set = struct { body: []const u8, negated: bool, end: usize };

/// The `[...]` opening at `pat[at]`, or null when it never closes (a plain `[`). A
/// `]` first in the set is a member.
fn setAt(pat: []const u8, at: usize) ?Set {
    var i = at + 1;
    const negated = i < pat.len and (pat[i] == '!' or pat[i] == '^');
    if (negated) i += 1;
    const start = i;
    if (i < pat.len and pat[i] == ']') i += 1;
    while (i < pat.len and pat[i] != ']') i += 1;
    if (i >= pat.len) return null;
    return .{ .body = pat[start..i], .negated = negated, .end = i + 1 };
}

fn inSet(body: []const u8, c: u8) bool {
    var i: usize = 0;
    while (i < body.len) {
        if (i + 2 < body.len and body[i + 1] == '-') {
            if (c >= body[i] and c <= body[i + 2]) return true;
            i += 3;
        } else {
            if (c == body[i]) return true;
            i += 1;
        }
    }
    return false;
}

test "glob: wildcards, sets and ranges match within one name" {
    try std.testing.expect(match("*", "stat"));
    try std.testing.expect(match("*.csv", "a.csv"));
    try std.testing.expect(!match("*.csv", "a.csv.gz"));
    try std.testing.expect(match("a*b*c", "aXXbYYc"));
    try std.testing.expect(!match("a*b*c", "aXXbYY"));
    try std.testing.expect(match("?", "7"));
    try std.testing.expect(!match("?", "77"));
    try std.testing.expect(match("[0-9]*", "1234"));
    try std.testing.expect(!match("[0-9]*", "self"));
    try std.testing.expect(match("[!0-9]*", "self"));
    try std.testing.expect(match("[]a]", "]"));
    try std.testing.expect(match("a[", "a["));
    try std.testing.expect(match("", ""));
    try std.testing.expect(!match("", "a"));
    try std.testing.expect(match("**", ""));
}

test "glob: a pattern expands to the files it matches, sorted, folders and hidden names left out" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "p/10/stat", "p/2/stat", "p/self/stat", "p/.hid/stat", "p/2/other" }) |f| {
        try tmp.dir.makePath(std.fs.path.dirname(f).?);
        try tmp.dir.writeFile(.{ .sub_path = f, .data = "x\n" });
    }
    try tmp.dir.makePath("p/9/stat");
    const base = try tmp.dir.realpathAlloc(a, ".");
    const got = try expand(a, try std.mem.concat(a, u8, &.{ base, "/p/*/stat" }));
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expect(std.mem.endsWith(u8, got[0], "/p/10/stat"));
    try std.testing.expect(std.mem.endsWith(u8, got[1], "/p/2/stat"));
    try std.testing.expect(std.mem.endsWith(u8, got[2], "/p/self/stat"));
    const digits = try expand(a, try std.mem.concat(a, u8, &.{ base, "/p/[0-9]*/stat" }));
    try std.testing.expectEqual(@as(usize, 2), digits.len);
    try std.testing.expectEqual(@as(usize, 0), (try expand(a, try std.mem.concat(a, u8, &.{ base, "/p/*/none" }))).len);
    try std.testing.expect(hasGlob("/proc/*/stat"));
    try std.testing.expect(!hasGlob("/proc/1/stat"));
    try std.testing.expect(!hasGlob("https://h/a*b"));
}
