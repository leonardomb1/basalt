//! Remote `@include`s, Go-style: `github.com/owner/repo/path/lib.sql@v1.2.0`,
//! `gitlab.com/group/repo/path.sql@ref`, or `host/group/sub/repo.git/path.sql@ref`
//! for a self-hosted GitLab (`.git` marks where the project ends). The ref is a tag,
//! branch or commit, and is required. Files are fetched over HTTPS as raw files, no
//! git needed; `GITHUB_TOKEN` or `GITLAB_TOKEN` reads a private repository, the
//! latter sent only to gitlab.com or the host `GITLAB_HOST` names.
//!
//! Every remote file is pinned by its SHA-256 in `basalt.sum` beside the root
//! script, as Go's `go.sum` pins a module: a file not pinned is refused until
//! `basalt check --pin` fetches it and records its hash, and a file whose content no
//! longer matches its pin (a moved tag or branch, a tampered host) is refused. Pinned
//! files are kept in a content-addressed cache, so a pinned script runs offline.
//!
//! A remote file's relative `@include`s resolve inside the same repository and ref,
//! and may not climb out of it; it can never include a local path.

const std = @import("std");

pub const Spec = struct {
    host: []const u8,
    project: []const u8,
    path: []const u8,
    ref: []const u8,

    /// How the spec is written, and its key in `basalt.sum`.
    pub fn key(self: Spec, arena: std.mem.Allocator) ![]const u8 {
        return std.fmt.allocPrint(arena, "{s}/{s}{s}/{s}@{s}", .{ self.host, self.project, if (self.dotGit()) ".git" else "", self.path, self.ref });
    }

    fn dotGit(self: Spec) bool {
        return !std.mem.eql(u8, self.host, "github.com") and !(std.mem.eql(u8, self.host, "gitlab.com") and std.mem.count(u8, self.project, "/") == 1);
    }

    /// The raw-file URL: GitHub's raw host, else GitLab's `/-/raw/` route.
    pub fn url(self: Spec, arena: std.mem.Allocator) ![]const u8 {
        if (std.mem.eql(u8, self.host, "github.com"))
            return std.fmt.allocPrint(arena, "https://raw.githubusercontent.com/{s}/{s}/{s}", .{ self.project, self.ref, self.path });
        return std.fmt.allocPrint(arena, "https://{s}/{s}/-/raw/{s}/{s}", .{ self.host, self.project, self.ref, self.path });
    }

    /// The header that reads a private repository, from the environment: the GitLab
    /// token goes only to gitlab.com or the host `GITLAB_HOST` names, so an include
    /// naming another host never receives it.
    pub fn auth(self: Spec, arena: std.mem.Allocator) !?std.http.Header {
        if (std.mem.eql(u8, self.host, "github.com")) {
            const t = std.process.getEnvVarOwned(arena, "GITHUB_TOKEN") catch return null;
            return .{ .name = "Authorization", .value = try std.fmt.allocPrint(arena, "Bearer {s}", .{t}) };
        }
        if (!gitlabHost(arena, self.host)) return null;
        const t = std.process.getEnvVarOwned(arena, "GITLAB_TOKEN") catch return null;
        return .{ .name = "PRIVATE-TOKEN", .value = t };
    }

    fn gitlabHost(arena: std.mem.Allocator, host: []const u8) bool {
        if (std.ascii.eqlIgnoreCase(host, "gitlab.com")) return true;
        const named = std.process.getEnvVarOwned(arena, "GITLAB_HOST") catch return false;
        return std.ascii.eqlIgnoreCase(std.mem.trim(u8, named, " /"), host);
    }

    /// `rel`, an include written inside this file, in the same repository and ref.
    pub fn relative(self: Spec, arena: std.mem.Allocator, rel: []const u8) !Spec {
        if (std.fs.path.isAbsolute(rel)) return error.RemoteAbsoluteInclude;
        const dir = std.fs.path.dirnamePosix(self.path) orelse "";
        var parts = std.array_list.Managed([]const u8).init(arena);
        var it = std.mem.tokenizeScalar(u8, dir, '/');
        while (it.next()) |p| try parts.append(p);
        var rit = std.mem.tokenizeScalar(u8, rel, '/');
        while (rit.next()) |p| {
            if (std.mem.eql(u8, p, ".")) continue;
            if (std.mem.eql(u8, p, "..")) {
                if (parts.items.len == 0) return error.RemoteEscapesRepo;
                _ = parts.pop();
                continue;
            }
            try parts.append(p);
        }
        if (parts.items.len == 0) return error.RemoteEscapesRepo;
        var out = self;
        out.path = try std.mem.join(arena, "/", parts.items);
        return out;
    }
};

/// The spec `s` names when it is a remote include: a host (a first segment with a
/// dot) and an `@ref` on the last segment. A local path is null.
pub fn parse(s: []const u8) ?Spec {
    if (std.fs.path.isAbsolute(s) or std.mem.startsWith(u8, s, ".")) return null;
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse return null;
    const host = s[0..slash];
    if (std.mem.indexOfScalar(u8, host, '.') == null) return null;
    const at = std.mem.lastIndexOfScalar(u8, s, '@') orelse return null;
    if (at < slash or std.mem.indexOfScalarPos(u8, s, at, '/') != null) return null;
    const ref = s[at + 1 ..];
    const rest = s[slash + 1 .. at];
    if (ref.len == 0 or rest.len == 0) return null;
    if (std.mem.indexOf(u8, rest, ".git/")) |g| {
        if (g == 0 or g + 5 >= rest.len) return null;
        return .{ .host = host, .project = rest[0..g], .path = rest[g + 5 ..], .ref = ref };
    }
    if (!std.mem.eql(u8, host, "github.com") and !std.mem.eql(u8, host, "gitlab.com")) return null;
    const a = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    const b = std.mem.indexOfScalarPos(u8, rest, a + 1, '/') orelse return null;
    if (b + 1 >= rest.len) return null;
    return .{ .host = host, .project = rest[0..b], .path = rest[b + 1 ..], .ref = ref };
}

/// Whether `s` is written as a remote include, well-formed or not, so a malformed
/// one is reported as such rather than looked for on disk.
pub fn looksRemote(s: []const u8) bool {
    if (std.fs.path.isAbsolute(s) or std.mem.startsWith(u8, s, ".")) return false;
    const slash = std.mem.indexOfScalar(u8, s, '/') orelse return false;
    const host = s[0..slash];
    return std.mem.eql(u8, host, "github.com") or std.mem.eql(u8, host, "gitlab.com") or std.mem.indexOf(u8, s, ".git/") != null;
}

/// GETs a URL, an optional header attached, and returns its body; the CLI supplies
/// one over HTTPS, a test a fake.
pub const Fetch = struct {
    ctx: *anyopaque,
    get: *const fn (ctx: *anyopaque, arena: std.mem.Allocator, url: []const u8, header: ?std.http.Header) anyerror![]const u8,
};

/// Resolves remote includes against `basalt.sum` and the cache, fetching only what
/// the cache lacks. With `pin`, a file not yet pinned is fetched and recorded, and
/// `save` writes the file; otherwise it is refused.
pub const Resolver = struct {
    arena: std.mem.Allocator,
    fetch: ?Fetch,
    sum_path: []const u8,
    cache_dir: ?[]const u8,
    pin: bool = false,
    pins: std.StringHashMap([]const u8),
    added: std.array_list.Managed([]const u8),
    msg: []const u8 = "",

    pub fn init(arena: std.mem.Allocator, root_dir: []const u8, fetch: ?Fetch, pin: bool) !Resolver {
        var r = Resolver{
            .arena = arena,
            .fetch = fetch,
            .sum_path = try std.fs.path.join(arena, &.{ if (root_dir.len == 0) "." else root_dir, "basalt.sum" }),
            .cache_dir = cacheDir(arena),
            .pin = pin,
            .pins = std.StringHashMap([]const u8).init(arena),
            .added = std.array_list.Managed([]const u8).init(arena),
        };
        const text = std.fs.cwd().readFileAlloc(arena, r.sum_path, 4 << 20) catch |e| switch (e) {
            error.FileNotFound => return r,
            else => return e,
        };
        var lines = std.mem.tokenizeScalar(u8, text, '\n');
        while (lines.next()) |raw| {
            const line = std.mem.trim(u8, raw, " \t\r");
            if (line.len == 0 or line[0] == '#') continue;
            const sp = std.mem.lastIndexOfScalar(u8, line, ' ') orelse continue;
            const h = std.mem.trim(u8, line[sp + 1 ..], " \t");
            if (!std.mem.startsWith(u8, h, "sha256:")) continue;
            try r.pins.put(std.mem.trim(u8, line[0..sp], " \t"), h["sha256:".len..]);
        }
        return r;
    }

    /// The text of `spec`, verified against its pin; on failure `msg` says why.
    pub fn get(self: *Resolver, spec: Spec) ![]const u8 {
        const k = try spec.key(self.arena);
        const pinned = self.pins.get(k);
        if (pinned == null and !self.pin)
            return self.fail("remote include `{s}` is not pinned in `{s}`; run `basalt check --pin` on the script to fetch it and record its hash", .{ k, self.sum_path });
        if (pinned) |want| if (self.cached(want)) |text| return text;
        const f = self.fetch orelse
            return self.fail("remote include `{s}` is not in the cache, and nothing here can fetch it", .{k});
        const text = f.get(f.ctx, self.arena, try spec.url(self.arena), try spec.auth(self.arena)) catch |e|
            return self.fail("could not fetch remote include `{s}` from {s}: {s}", .{ k, try spec.url(self.arena), @errorName(e) });
        const got = try hexHash(self.arena, text);
        if (pinned) |want| {
            if (!std.mem.eql(u8, want, got))
                return self.fail("remote include `{s}` no longer matches its pin in `{s}` (pinned sha256:{s}, fetched sha256:{s}): the ref moved or the host serves other content; review it, then delete its line and run `basalt check --pin`", .{ k, self.sum_path, want, got });
        } else {
            try self.pins.put(k, got);
            try self.added.append(k);
        }
        self.store(got, text);
        return text;
    }

    /// Writes `basalt.sum` with every pin, sorted, when `get` added any.
    pub fn save(self: *Resolver) !void {
        if (self.added.items.len == 0) return;
        var keys = std.array_list.Managed([]const u8).init(self.arena);
        var it = self.pins.keyIterator();
        while (it.next()) |k| try keys.append(k.*);
        std.sort.pdq([]const u8, keys.items, {}, struct {
            fn lt(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.lessThan(u8, a, b);
            }
        }.lt);
        var aw = std.Io.Writer.Allocating.init(self.arena);
        for (keys.items) |k| try aw.writer.print("{s} sha256:{s}\n", .{ k, self.pins.get(k).? });
        try std.fs.cwd().writeFile(.{ .sub_path = self.sum_path, .data = aw.written() });
    }

    fn cached(self: *Resolver, hash: []const u8) ?[]const u8 {
        const dir = self.cache_dir orelse return null;
        const p = std.fs.path.join(self.arena, &.{ dir, hash }) catch return null;
        const text = std.fs.cwd().readFileAlloc(self.arena, p, 8 << 20) catch return null;
        const got = hexHash(self.arena, text) catch return null;
        return if (std.mem.eql(u8, got, hash)) text else null;
    }

    fn store(self: *Resolver, hash: []const u8, text: []const u8) void {
        const dir = self.cache_dir orelse return;
        std.fs.cwd().makePath(dir) catch return;
        const p = std.fs.path.join(self.arena, &.{ dir, hash }) catch return;
        std.fs.cwd().writeFile(.{ .sub_path = p, .data = text }) catch {};
    }

    fn fail(self: *Resolver, comptime fmt: []const u8, args: anytype) error{ RemoteInclude, OutOfMemory } {
        self.msg = try std.fmt.allocPrint(self.arena, fmt, args);
        return error.RemoteInclude;
    }
};

fn hexHash(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    var d: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(text, &d, .{});
    return std.fmt.allocPrint(arena, "{x}", .{&d});
}

/// `$BASALT_CACHE/include`, else `$XDG_CACHE_HOME/basalt/include`, else
/// `~/.cache/basalt/include`; null when none is set.
fn cacheDir(arena: std.mem.Allocator) ?[]const u8 {
    if (std.process.getEnvVarOwned(arena, "BASALT_CACHE")) |d| return std.fs.path.join(arena, &.{ d, "include" }) catch null else |_| {}
    if (std.process.getEnvVarOwned(arena, "XDG_CACHE_HOME")) |d| return std.fs.path.join(arena, &.{ d, "basalt", "include" }) catch null else |_| {}
    const home = std.process.getEnvVarOwned(arena, "HOME") catch return null;
    return std.fs.path.join(arena, &.{ home, ".cache", "basalt", "include" }) catch null;
}

test "remote specs: GitHub, GitLab, a .git project, refs and relative paths" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const gh = parse("github.com/acme/sqlkit/stats/lib.sql@v1.2.0").?;
    try std.testing.expectEqualStrings("acme/sqlkit", gh.project);
    try std.testing.expectEqualStrings("stats/lib.sql", gh.path);
    try std.testing.expectEqualStrings("https://raw.githubusercontent.com/acme/sqlkit/v1.2.0/stats/lib.sql", try gh.url(a));
    try std.testing.expectEqualStrings("github.com/acme/sqlkit/stats/lib.sql@v1.2.0", try gh.key(a));
    const gl = parse("git.corp.local/data/etl/kit.git/a/b.sql@main").?;
    try std.testing.expectEqualStrings("data/etl/kit", gl.project);
    try std.testing.expectEqualStrings("https://git.corp.local/data/etl/kit/-/raw/main/a/b.sql", try gl.url(a));
    try std.testing.expectEqualStrings("git.corp.local/data/etl/kit.git/a/b.sql@main", try gl.key(a));
    try std.testing.expectEqualStrings("https://gitlab.com/g/r/-/raw/3f2a/x.sql", try parse("gitlab.com/g/r/x.sql@3f2a").?.url(a));
    try std.testing.expect(parse("lib/stats.sql") == null);
    try std.testing.expect(parse("github.com/acme/sqlkit/x.sql") == null);
    try std.testing.expect(looksRemote("github.com/acme/sqlkit/x.sql"));
    try std.testing.expect(!looksRemote("lib/x@y.sql"));
    try std.testing.expectEqualStrings("stats/util/x.sql", (try gh.relative(a, "util/x.sql")).path);
    try std.testing.expectEqualStrings("common.sql", (try gh.relative(a, "../common.sql")).path);
    try std.testing.expectError(error.RemoteEscapesRepo, gh.relative(a, "../../etc/x.sql"));
    try std.testing.expectError(error.RemoteAbsoluteInclude, gh.relative(a, "/etc/passwd"));
}

test "the GitLab token is sent only to gitlab.com or the host GITLAB_HOST names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expect(Spec.gitlabHost(a, "GitLab.com"));
    if (std.posix.getenv("GITLAB_HOST") == null) {
        try std.testing.expect(!Spec.gitlabHost(a, "evil.example"));
        const s = parse("evil.example/x/y.git/a.sql@v1").?;
        try std.testing.expect((try s.auth(a)) == null);
    }
}
