//! Where a path lives, read once from its scheme. `Scheme.of` matches the prefix
//! table case-insensitively (RFC 3986 schemes are), and every question a caller
//! asks of a scheme (`has`, `layer`, `prefix`, `rest`) is answered from that one
//! table. `Userinfo.parse` splits a URL's `[domain;]user[:password]` and
//! percent-decodes each part; `shown` is a URL as logs and messages may print it.

const std = @import("std");
const percent = @import("../net/percent.zig");

pub const Cap = enum {
    http,
    object_store,
    file_server,
    remote,
    replaced_on_write,
    http_ranged,
    streamed,
    atomic_create,
};

const Caps = std.EnumSet(Cap);

const Row = struct {
    prefix: []const u8,
    layer: []const u8,
    caps: Caps,
};

fn row(prefix: []const u8, layer: []const u8, caps: []const Cap) Row {
    return .{ .prefix = prefix, .layer = layer, .caps = Caps.initMany(caps) };
}

pub const Scheme = enum {
    local,
    http,
    https,
    s3,
    az,
    sftp,
    smb,
    ftp,
    other,

    const table = std.EnumArray(Scheme, Row).init(.{
        .local = row("", "local file", &.{.atomic_create}),
        .http = row("http://", "http", &.{ .http, .remote, .http_ranged, .streamed }),
        .https = row("https://", "http", &.{ .http, .remote, .http_ranged, .streamed }),
        .s3 = row("s3://", "s3 object", &.{ .object_store, .remote, .replaced_on_write, .http_ranged, .streamed, .atomic_create }),
        .az = row("az://", "azure blob", &.{ .object_store, .remote, .replaced_on_write, .http_ranged, .streamed, .atomic_create }),
        .sftp = row("sftp://", "sftp", &.{ .file_server, .remote, .replaced_on_write, .streamed }),
        .smb = row("smb://", "smb", &.{ .file_server, .remote, .replaced_on_write, .streamed }),
        .ftp = row("ftp://", "ftp", &.{ .file_server, .remote }),
        .other = row("", "local file", &.{}),
    });

    /// The scheme `path` starts with, ignoring case; `.other` for an unknown one
    /// (any `://` in it, as today's callers test), else `.local`.
    pub fn of(path: []const u8) Scheme {
        inline for (comptime std.enums.values(Scheme)) |s| {
            const p = comptime table.get(s).prefix;
            if (p.len > 0 and std.ascii.startsWithIgnoreCase(path, p)) return s;
        }
        if (std.mem.indexOf(u8, path, "://") != null) return .other;
        return .local;
    }

    pub fn has(s: Scheme, cap: Cap) bool {
        return table.get(s).caps.contains(cap);
    }

    /// The canonical lowercase prefix, `""` for `.local` and `.other`.
    pub fn prefix(s: Scheme) []const u8 {
        return table.get(s).prefix;
    }

    /// What `pathFail` calls the layer a path failed in.
    pub fn layer(s: Scheme) []const u8 {
        return table.get(s).layer;
    }

    /// `path` after its prefix, however the prefix was cased; `path` itself for
    /// `.local` and `.other`.
    pub fn rest(path: []const u8) []const u8 {
        return path[Scheme.of(path).prefix().len..];
    }
};

pub fn of(path: []const u8) Scheme {
    return Scheme.of(path);
}

/// `url` as a URI whose scheme is spelled in lowercase when it is one the table
/// knows, since std.http matches a scheme case-sensitively.
pub fn parseUri(url: []const u8) std.Uri.ParseError!std.Uri {
    var u = try std.Uri.parse(url);
    const p = of(url).prefix();
    if (p.len > 0) u.scheme = p[0 .. p.len - "://".len];
    return u;
}

pub const Userinfo = struct {
    user: []const u8,
    password: ?[]const u8 = null,
    domain: ?[]const u8 = null,

    pub const Opts = struct { domain: bool = false };

    /// `info` (the part before a URL's last `@`) split at its first `:` into user
    /// and password, and with `opts.domain` the user at its first `;` into domain
    /// and user (SMB's `domain;user`). Each part is percent-decoded after the split,
    /// so `%3A` and `%3B` stay inside their part.
    pub fn parse(arena: std.mem.Allocator, info: []const u8, opts: Opts) !Userinfo {
        var u = Userinfo{ .user = info };
        if (std.mem.indexOfScalar(u8, info, ':')) |c| {
            u.user = info[0..c];
            u.password = try percent.decode(arena, info[c + 1 ..]);
        }
        if (opts.domain) if (std.mem.indexOfScalar(u8, u.user, ';')) |semi| {
            u.domain = try percent.decode(arena, u.user[0..semi]);
            u.user = u.user[semi + 1 ..];
        };
        u.user = try percent.decode(arena, u.user);
        return u;
    }
};

/// `url` as logs and messages show it: without a `user:pass@` and with each query
/// value replaced by `...`, as either may hold a credential. `(url)` when it does
/// not fit `buf`.
pub fn shown(buf: []u8, url: []const u8) []const u8 {
    var w = std.Io.Writer.fixed(buf);
    writeShown(&w, url) catch return "(url)";
    return w.buffered();
}

pub fn writeShown(w: *std.Io.Writer, url: []const u8) std.Io.Writer.Error!void {
    var tail = url;
    if (std.mem.indexOf(u8, tail, "://")) |at| {
        try w.writeAll(tail[0 .. at + 3]);
        tail = tail[at + 3 ..];
        const host_end = std.mem.indexOfAny(u8, tail, "/?#") orelse tail.len;
        if (std.mem.lastIndexOfScalar(u8, tail[0..host_end], '@')) |amp| tail = tail[amp + 1 ..];
    }
    const q = std.mem.indexOfScalar(u8, tail, '?') orelse return w.writeAll(tail);
    try w.writeAll(tail[0 .. q + 1]);
    var it = std.mem.splitScalar(u8, tail[q + 1 ..], '&');
    var first = true;
    while (it.next()) |kv| {
        if (!first) try w.writeByte('&');
        first = false;
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse kv.len;
        try w.writeAll(kv[0..eq]);
        if (eq < kv.len) try w.writeAll("=...");
    }
}

/// `url` through `shown` for a `{f}` placeholder, with no buffer to size.
pub fn fmtShown(url: []const u8) std.fmt.Alt([]const u8, formatShown) {
    return .{ .data = url };
}

fn formatShown(url: []const u8, w: *std.Io.Writer) std.Io.Writer.Error!void {
    return writeShown(w, url);
}

const testing = std.testing;

test "of reads every prefix in any case, an unknown scheme as other, else local" {
    const cases = [_]struct { []const u8, Scheme }{
        .{ "http://h/a", .http },     .{ "HTTP://h/a", .http },
        .{ "https://h/a", .https },   .{ "HtTpS://h/a", .https },
        .{ "s3://b/k", .s3 },         .{ "S3://b/k", .s3 },
        .{ "az://a/c/k", .az },       .{ "AZ://a/c/k", .az },
        .{ "sftp://h/x", .sftp },     .{ "SFTP://h/x", .sftp },
        .{ "smb://h/s/x", .smb },     .{ "Smb://h/s/x", .smb },
        .{ "ftp://h/x", .ftp },       .{ "FTP://h/x", .ftp },
        .{ "s3a://b/k", .other },     .{ "abfss://c@a.dfs.core.windows.net/p", .other },
        .{ "file:///tmp/x", .other }, .{ "gs://b/k", .other },
        .{ "dir/a://b", .other },     .{ "/data/a.parquet", .local },
        .{ "a.parquet", .local },     .{ "", .local },
        .{ "file:/tmp/x", .local },   .{ "C:\\data\\x.csv", .local },
        .{ "s3:/b/k", .local },       .{ "https:/h", .local },
        .{ "s3://", .s3 },            .{ "sftp", .local },
        .{ "xs3://b/k", .other },     .{ " s3://b/k", .other },
    };
    for (cases) |c| try testing.expectEqual(c[1], of(c[0]));
}

test "rest drops the prefix whatever its case; prefix is canonical lowercase" {
    try testing.expectEqualStrings("b/k", Scheme.rest("S3://b/k"));
    try testing.expectEqualStrings("h/x", Scheme.rest("sftp://h/x"));
    try testing.expectEqualStrings("h/p?q", Scheme.rest("HTTPS://h/p?q"));
    try testing.expectEqualStrings("", Scheme.rest("az://"));
    try testing.expectEqualStrings("/data/x", Scheme.rest("/data/x"));
    try testing.expectEqualStrings("gs://b/k", Scheme.rest("gs://b/k"));
    for (std.enums.values(Scheme)) |s| {
        const p = s.prefix();
        for (p) |ch| try testing.expect(!std.ascii.isUpper(ch));
        if (p.len > 0) try testing.expectEqual(s, of(p));
    }
}

test "capabilities: each set is exactly the schemes today's predicate names" {
    const want = [_]struct { Cap, []const Scheme }{
        .{ .http, &.{ .http, .https } },
        .{ .object_store, &.{ .s3, .az } },
        .{ .file_server, &.{ .sftp, .smb, .ftp } },
        .{ .remote, &.{ .http, .https, .s3, .az, .sftp, .smb, .ftp } },
        .{ .replaced_on_write, &.{ .s3, .az, .sftp, .smb } },
        .{ .http_ranged, &.{ .http, .https, .s3, .az } },
        .{ .streamed, &.{ .http, .https, .s3, .az, .sftp, .smb } },
        .{ .atomic_create, &.{ .local, .s3, .az } },
    };
    for (want) |w| for (std.enums.values(Scheme)) |s| {
        const in = std.mem.indexOfScalar(Scheme, w[1], s) != null;
        if (s.has(w[0]) != in) {
            std.debug.print("{t} has {t}: want {}\n", .{ s, w[0], in });
            return error.TestUnexpectedResult;
        }
    };
}

test "layer names match pathFail's" {
    const want = [_]struct { Scheme, []const u8 }{
        .{ .az, "azure blob" }, .{ .s3, "s3 object" },     .{ .sftp, "sftp" },
        .{ .smb, "smb" },       .{ .ftp, "ftp" },          .{ .http, "http" },
        .{ .https, "http" },    .{ .local, "local file" }, .{ .other, "local file" },
    };
    for (want) |w| try testing.expectEqualStrings(w[1], w[0].layer());
}

const objstore = @import("objstore.zig");
const sftp = @import("sftp.zig");
const smb = @import("smb.zig");
const ftp = @import("ftp.zig");
const s3 = @import("s3.zig");
const azure = @import("azure.zig");
const remotefs = @import("remotefs.zig");
const bytes = @import("../format/bytes.zig");
const stream = @import("../format/stream.zig");
const lake_table = @import("../format/lake/table.zig");
const delta_write = @import("../format/lake/delta_write.zig");
const http_client = @import("../net/http_client.zig");

const corpus = [_][]const u8{
    "http://h/a",    "HTTP://h/a", "https://h/a", "Https://h/a",
    "s3://b/k",      "S3://b/k",   "az://a/c/k",  "Az://a/c/k",
    "sftp://h/x",    "SFTP://h/x", "smb://h/s/x", "SMB://h/s/x",
    "ftp://h/x",     "FTP://h/x",  "s3a://b/k",   "abfss://c@a.dfs.core.windows.net/p",
    "file:///tmp/x", "gs://b/k",   "dir/a://b",   "/data/a.parquet",
    "a.parquet",     "",           "s3:/b/k",     "https:/h",
};

fn lowerPrefix(path: []const u8) bool {
    const p = of(path).prefix();
    return std.mem.startsWith(u8, path, p);
}

fn pin(today: bool, path: []const u8, now: bool, case_folds_today: bool) !void {
    if (lowerPrefix(path) or case_folds_today) {
        if (today != now) {
            std.debug.print("`{s}`: today {} scheme {}\n", .{ path, today, now });
            return error.TestUnexpectedResult;
        }
    } else if (today) {
        std.debug.print("`{s}`: today matched a cased prefix\n", .{path});
        return error.TestUnexpectedResult;
    }
}

test "every store predicate is the scheme table, in any case; lake_table.isHttp still pins today's" {
    for (corpus) |p| {
        const s = of(p);
        try testing.expectEqual(s == .s3, s3.isUrl(p));
        try testing.expectEqual(s == .az, azure.isUrl(p));
        try testing.expectEqual(s == .sftp, sftp.isUrl(p));
        try testing.expectEqual(s == .smb, smb.isUrl(p));
        try testing.expectEqual(s == .ftp, ftp.isUrl(p));
        try testing.expectEqual(s.has(.object_store), objstore.isUrl(p));
        try testing.expectEqual(s.has(.replaced_on_write), objstore.isRemote(p));
        try testing.expectEqual(s.has(.http_ranged), bytes.isRemote(p));
        try testing.expectEqual(s.has(.streamed), stream.isUrl(p));
        try pin(lake_table.isHttp(p), p, s.has(.http), false);
        const commit_ok = delta_write.storeProblem(p) == null;
        try testing.expectEqual(s.has(.atomic_create), commit_ok);
    }
}

test "a cased prefix is remote: SFTP://, S3://, Az:// and HTTPS:// no longer fall to local files" {
    try testing.expect(sftp.isUrl("SFTP://h/x"));
    try testing.expect(s3.isUrl("S3://b/k"));
    try testing.expect(objstore.isUrl("Az://a/c/k") and objstore.isRemote("Az://a/c/k"));
    try testing.expect(objstore.isRemote("SFTP://h/x") and objstore.isRemote("SMB://h/s/x"));
    try testing.expect(bytes.isRemote("HTTPS://h/a") and stream.isUrl("HTTPS://h/a"));
    try testing.expect(objstore.find("S3://b/k") == &s3.provider);
    try testing.expect(objstore.find("AZ://a/c/k") == &azure.provider);
    try testing.expect(smb.isUrl("SMB://h/s/x") and ftp.isUrl("FTP://h/x"));
    try testing.expect(delta_write.storeProblem("FTP://h/x") != null and !of("FTP://h/x").has(.atomic_create));
    try testing.expect(delta_write.storeProblem("S3://b/k") == null and of("S3://b/k").has(.atomic_create));

    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lower = try s3.parsePrefix("s3://bkt/p/");
    const upper = try s3.parsePrefix("S3://bkt/p/");
    try testing.expectEqualStrings(lower.bucket, upper.bucket);
    try testing.expectEqualStrings(lower.prefix, upper.prefix);
    const az = try azure.parsePrefix("AZ://acct/cont/p/");
    try testing.expectEqualStrings("acct", az.account);
    try testing.expectEqualStrings("p/", az.prefix);
    const f = try ftp.parseUrl(a, "FTP://u@unregistered-host.invalid/x");
    try testing.expectEqualStrings("unregistered-host.invalid", f.host);
}

test "parseUri spells a known scheme in lowercase and leaves an unknown one alone" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "HTTPS://h/a", "https" }, .{ "Http://h:8080/a?b", "http" },
        .{ "https://h/a", "https" }, .{ "S3://b/k", "s3" },
        .{ "gs://b/k", "gs" },       .{ "Gs://b/k", "Gs" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], (try parseUri(c[0])).scheme);
    const u = try parseUri("HTTPS://user@Host.io:8443/p/q?x=1#f");
    try testing.expectEqual(@as(?u16, 8443), u.port);
    try testing.expectEqualStrings("Host.io", u.host.?.percent_encoded);
    try testing.expectError(error.UnexpectedCharacter, parseUri("not a url"));
}

test "Userinfo splits user and password at the first colon and decodes each part" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_]struct { []const u8, []const u8, ?[]const u8 }{
        .{ "u", "u", null },
        .{ "u:pw", "u", "pw" },
        .{ "u:p:w", "u", "p:w" },
        .{ "u:", "u", "" },
        .{ ":pw", "", "pw" },
        .{ "", "", null },
        .{ "u%40corp:p%3Aw%40", "u@corp", "p:w@" },
        .{ "u%3Ax:pw", "u:x", "pw" },
        .{ "%zz:%4", "%zz", "%4" },
        .{ "d;u:pw", "d;u", "pw" },
        .{ "us%C3%A9r:%F0%9F%98%80", "usér", "😀" },
    };
    for (cases) |c| {
        const u = try Userinfo.parse(a, c[0], .{});
        try testing.expectEqualStrings(c[1], u.user);
        if (c[2]) |pw| try testing.expectEqualStrings(pw, u.password.?) else try testing.expect(u.password == null);
        try testing.expect(u.domain == null);
    }
}

test "Userinfo with domain splits domain;user before decoding" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_]struct { []const u8, ?[]const u8, []const u8, ?[]const u8 }{
        .{ "u", null, "u", null },
        .{ "corp;u", "corp", "u", null },
        .{ "corp;u:pw", "corp", "u", "pw" },
        .{ "corp;u:p;w", "corp", "u", "p;w" },
        .{ ";u", "", "u", null },
        .{ "a;b;c", "a", "b;c", null },
        .{ "c%3Bx;u%3Bv:pw", "c;x", "u;v", "pw" },
        .{ "u:pw;x", null, "u", "pw;x" },
    };
    for (cases) |c| {
        const u = try Userinfo.parse(a, c[0], .{ .domain = true });
        if (c[1]) |d| try testing.expectEqualStrings(d, u.domain.?) else try testing.expect(u.domain == null);
        try testing.expectEqualStrings(c[2], u.user);
        if (c[3]) |pw| try testing.expectEqualStrings(pw, u.password.?) else try testing.expect(u.password == null);
    }
}

test "today: ftp's userinfo is Userinfo.parse; sftp takes it whole, smb splits only the domain" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const infos = [_][]const u8{ "u", "u:pw", "u%40corp:p%3Aw", ":pw", "u:p:w", "d;u:pw", "u%3Ax:y" };
    for (infos) |info| {
        const url = try std.fmt.allocPrint(a, "ftp://{s}@unregistered-host.invalid/f", .{info});
        const got = try ftp.parseUrl(a, url);
        const u = try Userinfo.parse(a, info, .{});
        try testing.expectEqualStrings(u.user, got.user);
        if (u.password) |pw| try testing.expectEqualStrings(pw, got.pass);
    }
    const auth = remotefs.authority("u:pw@h:22").?;
    try testing.expectEqualStrings("u:pw", auth.userinfo.?);
    try testing.expectEqualStrings("h", auth.host);
    try testing.expectEqual(@as(?u16, 22), auth.port);
    const u = try Userinfo.parse(a, auth.userinfo.?, .{});
    try testing.expectEqualStrings("u", u.user);
    try testing.expectEqualStrings("pw", u.password.?);
    const d = try Userinfo.parse(a, "corp;u", .{ .domain = true });
    try testing.expectEqualStrings("corp", d.domain.?);
    try testing.expectEqualStrings("u", d.user);
}

test "shown drops userinfo and every query value" {
    var buf: [256]u8 = undefined;
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "https://user:secret@api.example.com/v1/x?api_key=abc&page=2", "https://api.example.com/v1/x?api_key=...&page=..." },
        .{ "http://h/p", "http://h/p" },
        .{ "http://h/p?flag", "http://h/p?flag" },
        .{ "http://h/p?", "http://h/p?" },
        .{ "http://h/p?a=&b", "http://h/p?a=...&b" },
        .{ "http://h/p?a=1&&b=2", "http://h/p?a=...&&b=..." },
        .{ "sftp://u:pw@h/x", "sftp://h/x" },
        .{ "SFTP://u:p@w@h:22/x", "SFTP://h:22/x" },
        .{ "smb://d;u:pw@h/s/x", "smb://h/s/x" },
        .{ "s3://b/k@v", "s3://b/k@v" },
        .{ "http://h?x=@y", "http://h?x=..." },
        .{ "http://u@h#f@g", "http://h#f@g" },
        .{ "http://u:pw@h", "http://h" },
        .{ "/local/a@b?c=d", "/local/a@b?c=..." },
        .{ "", "" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], shown(&buf, c[0]));
    var small: [8]u8 = undefined;
    try testing.expectEqualStrings("(url)", shown(&small, "https://h/a/long/path"));
    try testing.expectEqualStrings("http://h", shown(&small, "http://h"));
    var empty: [0]u8 = undefined;
    try testing.expectEqualStrings("", shown(&empty, ""));
    try testing.expectEqualStrings("(url)", shown(&empty, "x"));
}

/// http_client's `redactUrl` before it became `shown`, the oracle `shown` must match.
fn oldRedactUrl(buf: []u8, url: []const u8) []const u8 {
    var w = std.Io.Writer.fixed(buf);
    var rest = url;
    if (std.mem.indexOf(u8, rest, "://")) |at| {
        w.writeAll(rest[0 .. at + 3]) catch return "(url)";
        rest = rest[at + 3 ..];
        const host_end = std.mem.indexOfAny(u8, rest, "/?#") orelse rest.len;
        if (std.mem.lastIndexOfScalar(u8, rest[0..host_end], '@')) |amp| rest = rest[amp + 1 ..];
    }
    const q = std.mem.indexOfScalar(u8, rest, '?') orelse {
        w.writeAll(rest) catch return "(url)";
        return w.buffered();
    };
    w.writeAll(rest[0 .. q + 1]) catch return "(url)";
    var it = std.mem.splitScalar(u8, rest[q + 1 ..], '&');
    var first = true;
    while (it.next()) |kv| {
        if (!first) w.writeByte('&') catch return "(url)";
        first = false;
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse kv.len;
        w.writeAll(kv[0..eq]) catch return "(url)";
        if (eq < kv.len) w.writeAll("=...") catch return "(url)";
    }
    return w.buffered();
}

test "shown equals http_client's old redactUrl byte for byte, and fmtShown equals shown" {
    const urls = [_][]const u8{
        "https://user:secret@api.example.com/v1/x?api_key=abc&page=2",
        "http://h/p",
        "http://h/p?flag",
        "http://h/p?a=&b",
        "sftp://u:pw@h/x",
        "http://u@h#f@g",
        "http://h?x=@y",
        "/local/a@b?c=d",
        "",
        "https://h/a/long/path/that/does/not/fit/the/buffer/at/all",
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    for (urls) |u| for ([_]usize{ 0, 8, 16, 32, 256 }) |cap| {
        const b1 = try ar.allocator().alloc(u8, cap);
        const b2 = try ar.allocator().alloc(u8, cap);
        try testing.expectEqualStrings(oldRedactUrl(b1, u), shown(b2, u));
        try testing.expectEqualStrings(oldRedactUrl(b1, u), http_client.redactUrl(b2, u));
    };
    for (urls) |u| {
        var buf: [256]u8 = undefined;
        const s = try std.fmt.allocPrint(ar.allocator(), "{f}", .{fmtShown(u)});
        try testing.expectEqualStrings(shown(&buf, u), s);
    }
}
