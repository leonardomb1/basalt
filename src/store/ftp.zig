//! Plain FTP (RFC 959) as a read source: `ftp://[user[:pass]@]host[:port]/path`.
//! Public data still lives there (DATASUS, IBGE), and the protocol offers no cheap
//! random access, so a file is downloaded whole to a temp folder and read from
//! there: every format works, zip members, Parquet and Excel included, and a CSV
//! still splits across lanes. `localize` maps a URL to its local copy once per
//! run, keeping the file's name so its extension still picks the reader;
//! `cleanup` removes the copies. A trailing `/` is a folder, subfolders included,
//! listed by MLSD (or NLST and a `CWD` per name where MLSD is missing), and capped
//! at `max_folder_files`. FTP is read, never written.
//!
//! The host may name a `CREATE CONNECTION … TYPE ftp` (`register`), whose host,
//! port and login stand in for the URL's. The login is the URL's, else the
//! connection's, else `FTP_USER` / `FTP_PASSWORD`, else anonymous.
//! Transfers are binary (`TYPE I`) over a passive data connection: EPSV, or PASV
//! where it is refused. PASV's advertised address is ignored in favour of the
//! control connection's host, since a server behind NAT advertises a private one.
//! The data connection is opened before RETR and drained before the final reply
//! is read, which some servers need to send it. A 4xx reply is transient, a 5xx
//! permanent (550 is a missing file); both carry the server's text in `lastError`.

const std = @import("std");

pub const default_port = 21;
const timeout_s = 60;

pub fn isUrl(path: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(path, "ftp://");
}

pub const Url = struct {
    host: []const u8,
    port: u16 = default_port,
    user: []const u8,
    pass: []const u8,
    /// Absolute and percent-decoded, as RETR takes it.
    path: []const u8,
};

pub const Conn = struct {
    host: []const u8,
    port: u16 = default_port,
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
};

var registry_mtx: std.Thread.Mutex = .{};
var registry: std.StringHashMapUnmanaged(Conn) = .empty;

/// Make `ftp://name/…` reach the server `c` describes. Process-wide, as sftp's
/// are; a later registration of a name replaces the earlier one. Strings are copied.
pub fn register(name: []const u8, c: Conn) !void {
    const gpa = std.heap.page_allocator;
    registry_mtx.lock();
    defer registry_mtx.unlock();
    var owned = c;
    owned.host = try gpa.dupe(u8, c.host);
    if (c.user) |u| owned.user = try gpa.dupe(u8, u);
    if (c.password) |p| owned.password = try gpa.dupe(u8, p);
    const gop = try registry.getOrPut(gpa, name);
    if (!gop.found_existing) gop.key_ptr.* = try gpa.dupe(u8, name);
    gop.value_ptr.* = owned;
}

fn registered(name: []const u8) ?Conn {
    registry_mtx.lock();
    defer registry_mtx.unlock();
    return registry.get(name);
}

pub fn parseUrl(arena: std.mem.Allocator, url: []const u8) !Url {
    if (!isUrl(url)) return error.InvalidFtpUrl;
    const rest = url["ftp://".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
    var authority = rest[0..slash];
    const path = try percentDecode(arena, if (slash < rest.len) rest[slash..] else "/");

    var user: ?[]const u8 = null;
    var pass: ?[]const u8 = null;
    if (std.mem.lastIndexOfScalar(u8, authority, '@')) |at| {
        const info = authority[0..at];
        authority = authority[at + 1 ..];
        if (std.mem.indexOfScalar(u8, info, ':')) |c| {
            user = try percentDecode(arena, info[0..c]);
            pass = try percentDecode(arena, info[c + 1 ..]);
        } else user = try percentDecode(arena, info);
    }
    var host = authority;
    var port: ?u16 = null;
    if (std.mem.lastIndexOfScalar(u8, authority, ':')) |c| {
        host = authority[0..c];
        port = std.fmt.parseInt(u16, authority[c + 1 ..], 10) catch return error.InvalidFtpUrl;
    }
    if (host.len == 0) return error.InvalidFtpUrl;
    if (registered(host)) |c| return .{
        .host = c.host,
        .port = port orelse c.port,
        .user = user orelse c.user orelse "anonymous",
        .pass = pass orelse (if (user == null) c.password else null) orelse "anonymous@",
        .path = path,
    };
    const env_user = std.process.getEnvVarOwned(arena, "FTP_USER") catch null;
    const env_pass = std.process.getEnvVarOwned(arena, "FTP_PASSWORD") catch null;
    return .{
        .host = host,
        .port = port orelse default_port,
        .user = user orelse env_user orelse "anonymous",
        .pass = pass orelse (if (user == null) env_pass else null) orelse "anonymous@",
        .path = path,
    };
}

fn percentDecode(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, s, '%') == null) return s;
    var out = try std.array_list.Managed(u8).initCapacity(arena, s.len);
    var i: usize = 0;
    while (i < s.len) : (i += 1) {
        if (s[i] == '%' and i + 2 < s.len) {
            if (std.fmt.parseInt(u8, s[i + 1 .. i + 3], 16)) |b| {
                out.appendAssumeCapacity(b);
                i += 2;
                continue;
            } else |_| {}
        }
        out.appendAssumeCapacity(s[i]);
    }
    return out.items;
}

threadlocal var last_err_buf: [256]u8 = undefined;
threadlocal var last_err_len: usize = 0;

/// The last server reply that failed a request on this thread, as `550 text`.
pub fn lastError() []const u8 {
    return last_err_buf[0..last_err_len];
}

fn setLastError(code: u16, text: []const u8) void {
    const s = std.fmt.bufPrint(&last_err_buf, "{d} {s}", .{ code, text }) catch last_err_buf[0..];
    last_err_len = s.len;
}

/// A refusal by its code: 4xx is worth retrying, 530 is the login, 550 a missing
/// file or folder.
fn refusal(code: u16, text: []const u8) anyerror {
    setLastError(code, text);
    if (code >= 400 and code < 500) return error.FtpServerBusy;
    if (code == 530) return error.FtpLoginFailed;
    if (code == 550) return error.FileNotFound;
    return error.FtpRefused;
}

fn setTimeout(handle: std.posix.socket_t) void {
    const tv = std.posix.timeval{ .sec = timeout_s, .usec = 0 };
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
}

const Reply = struct { code: u16, text: []const u8 };

pub const Session = struct {
    gpa: std.mem.Allocator,
    host: []const u8,
    stream: std.net.Stream,
    rbuf: [4096]u8 = undefined,
    wbuf: [1024]u8 = undefined,
    sr: std.net.Stream.Reader = undefined,
    sw: std.net.Stream.Writer = undefined,
    text_buf: [256]u8 = undefined,

    pub fn open(gpa: std.mem.Allocator, u: Url) !*Session {
        const stream = try std.net.tcpConnectToHost(gpa, u.host, u.port);
        errdefer stream.close();
        setTimeout(stream.handle);
        const self = try gpa.create(Session);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .host = u.host, .stream = stream };
        self.sr = std.net.Stream.Reader.init(stream, &self.rbuf);
        self.sw = std.net.Stream.Writer.init(stream, &self.wbuf);

        const hello = try self.reply();
        if (hello.code != 220) return refusal(hello.code, hello.text);
        var r = try self.command("USER {s}", .{u.user});
        if (r.code == 331 or r.code == 332) r = try self.command("PASS {s}", .{u.pass});
        if (r.code != 230 and r.code != 202) return refusal(r.code, r.text);
        r = try self.command("TYPE I", .{});
        if (r.code != 200) return refusal(r.code, r.text);
        return self;
    }

    pub fn close(self: *Session) void {
        _ = self.command("QUIT", .{}) catch {};
        self.stream.close();
        self.gpa.destroy(self);
    }

    fn line(self: *Session) ![]const u8 {
        const l = (try self.sr.interface().takeDelimiter('\n')) orelse return error.ServerClosedConnection;
        return std.mem.trimRight(u8, l, "\r");
    }

    /// One reply, a multi-line one (`123-` … `123 `) read to its last line.
    fn reply(self: *Session) !Reply {
        var l = try self.line();
        if (l.len < 3) return error.FtpProtocol;
        const code = std.fmt.parseInt(u16, l[0..3], 10) catch return error.FtpProtocol;
        if (l.len > 3 and l[3] == '-') {
            var tag: [3]u8 = undefined;
            @memcpy(&tag, l[0..3]);
            while (true) {
                l = try self.line();
                if (l.len >= 4 and std.mem.eql(u8, l[0..3], &tag) and l[3] == ' ') break;
            }
        }
        const text = std.mem.trim(u8, if (l.len > 4) l[4..] else "", " ");
        const n = @min(text.len, self.text_buf.len);
        @memcpy(self.text_buf[0..n], text[0..n]);
        return .{ .code = code, .text = self.text_buf[0..n] };
    }

    fn command(self: *Session, comptime fmt: []const u8, args: anytype) !Reply {
        try self.sw.interface.print(fmt ++ "\r\n", args);
        try self.sw.interface.flush();
        return self.reply();
    }

    /// A passive data connection to the control host: EPSV, else PASV.
    fn dataConn(self: *Session) !std.net.Stream {
        const port = blk: {
            const e = try self.command("EPSV", .{});
            if (e.code == 229) if (epsvPort(e.text)) |p| break :blk p;
            const p = try self.command("PASV", .{});
            if (p.code != 227) return refusal(p.code, p.text);
            break :blk pasvPort(p.text) orelse return error.FtpProtocol;
        };
        const s = try std.net.tcpConnectToHost(self.gpa, self.host, port);
        setTimeout(s.handle);
        return s;
    }

    /// Opens the data connection, sends `verb path`, and returns it once the
    /// server has answered 125 or 150.
    fn transfer(self: *Session, verb: []const u8, path: []const u8) !std.net.Stream {
        const data = try self.dataConn();
        errdefer data.close();
        const r = try self.command("{s} {s}", .{ verb, path });
        if (r.code != 125 and r.code != 150) return refusal(r.code, r.text);
        return data;
    }

    fn finish(self: *Session) !void {
        const r = try self.reply();
        if (r.code != 226 and r.code != 250) return refusal(r.code, r.text);
    }

    /// Downloads `path` into `file`.
    pub fn retrieve(self: *Session, path: []const u8, file: std.fs.File) !void {
        const data = try self.transfer("RETR", path);
        {
            defer data.close();
            var buf: [64 * 1024]u8 = undefined;
            while (true) {
                const n = try std.posix.read(data.handle, &buf);
                if (n == 0) break;
                try file.writeAll(buf[0..n]);
            }
        }
        try self.finish();
    }

    fn drain(arena: std.mem.Allocator, data: std.net.Stream) ![]const u8 {
        defer data.close();
        var body = std.array_list.Managed(u8).init(arena);
        var buf: [16 * 1024]u8 = undefined;
        while (true) {
            const n = try std.posix.read(data.handle, &buf);
            if (n == 0) break;
            try body.appendSlice(buf[0..n]);
        }
        return body.items;
    }

    pub const Entry = struct { name: []const u8, dir: bool };

    /// What folder `path` holds, without `.` or `..`: by MLSD, whose lines say
    /// which entry is a folder, else by NLST with a `CWD` to each name to tell.
    pub fn entries(self: *Session, arena: std.mem.Allocator, path: []const u8) ![]const Entry {
        var out = std.array_list.Managed(Entry).init(arena);
        mlsd: {
            const data = self.transfer("MLSD", path) catch |e| switch (e) {
                error.FtpRefused => break :mlsd,
                else => return e,
            };
            const body = try drain(arena, data);
            try self.finish();
            var it = std.mem.tokenizeAny(u8, body, "\r\n");
            while (it.next()) |l| if (mlsdEntry(l)) |en| try out.append(en);
            return out.items;
        }
        last_err_len = 0;
        const data = try self.transfer("NLST", path);
        const body = try drain(arena, data);
        try self.finish();
        var it = std.mem.tokenizeAny(u8, body, "\r\n");
        while (it.next()) |raw| {
            const name = std.fs.path.basenamePosix(std.mem.trimRight(u8, raw, "/"));
            if (name.len == 0 or std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
            try out.append(.{ .name = name, .dir = false });
        }
        for (out.items) |*en| {
            const r = try self.command("CWD {s}{s}", .{ path, en.name });
            en.dir = r.code == 250;
        }
        return out.items;
    }
};

/// A file or folder in an MLSD listing (`type=file;size=12; name`); `cdir`,
/// `pdir` and links are left out.
pub fn mlsdEntry(line: []const u8) ?Session.Entry {
    const sp = std.mem.indexOfScalar(u8, line, ' ') orelse return null;
    const name = line[sp + 1 ..];
    if (name.len == 0) return null;
    var facts = std.mem.tokenizeScalar(u8, line[0..sp], ';');
    while (facts.next()) |f| {
        if (f.len < 5 or !std.ascii.eqlIgnoreCase(f[0..5], "type=")) continue;
        const t = f[5..];
        if (std.ascii.eqlIgnoreCase(t, "file")) return .{ .name = name, .dir = false };
        if (std.ascii.eqlIgnoreCase(t, "dir")) return .{ .name = name, .dir = true };
        return null;
    }
    return null;
}

/// The port in `229 Entering Extended Passive Mode (|||port|)`.
pub fn epsvPort(text: []const u8) ?u16 {
    const at = std.mem.indexOf(u8, text, "|||") orelse return null;
    const end = std.mem.indexOfScalarPos(u8, text, at + 3, '|') orelse return null;
    return std.fmt.parseInt(u16, text[at + 3 .. end], 10) catch null;
}

/// The port in `227 Entering Passive Mode (h1,h2,h3,h4,p1,p2)`, with or without the
/// parentheses; the advertised host is not used.
pub fn pasvPort(text: []const u8) ?u16 {
    var nums: [6]u32 = undefined;
    var n: usize = 0;
    var i: usize = 0;
    while (i < text.len and n < 6) {
        if (!std.ascii.isDigit(text[i])) {
            i += 1;
            continue;
        }
        var v: u32 = 0;
        while (i < text.len and std.ascii.isDigit(text[i])) : (i += 1) v = v *% 10 +% (text[i] - '0');
        nums[n] = v;
        n += 1;
    }
    if (n < 6 or nums[4] > 255 or nums[5] > 255) return null;
    return @intCast(nums[4] * 256 + nums[5]);
}

const Local = struct {
    mutex: std.Thread.Mutex = .{},
    dir: ?[]const u8 = null,
    seq: usize = 0,
    /// Runs in progress: a buffer flush or a notebook cell can run beside another
    /// run, and the copies stay until the last of them ends.
    active: usize = 0,
    cache: std.StringHashMapUnmanaged([]const u8) = .empty,
};
var local: Local = .{};

/// The local copy of an `ftp://` path, downloaded on first use in this run: a file,
/// a folder (trailing `/`, every file under it) or an archive member
/// (`ftp://h/a.zip :: m.csv`, the archive fetched and the member kept). A socket
/// that fails or times out mid-reply is `ConnectionIoFailed`, which the exit code
/// counts as transient.
pub fn localize(arena: std.mem.Allocator, url: []const u8) anyerror![]const u8 {
    if (std.mem.indexOf(u8, url, " :: ")) |at| {
        return std.mem.concat(arena, u8, &.{ try localize(arena, url[0..at]), url[at..] });
    }
    local.mutex.lock();
    defer local.mutex.unlock();
    if (local.cache.get(url)) |p| return arena.dupe(u8, p);

    return fetch(arena, url) catch |e| switch (@as(anyerror, e)) {
        error.ReadFailed, error.WriteFailed, error.EndOfStream, error.WouldBlock => error.ConnectionIoFailed,
        else => e,
    };
}

fn fetch(arena: std.mem.Allocator, url: []const u8) ![]const u8 {
    const gpa = std.heap.page_allocator;
    last_err_len = 0;
    const u = try parseUrl(arena, url);
    if (local.dir == null) {
        const base = std.process.getEnvVarOwned(arena, "TMPDIR") catch try arena.dupe(u8, "/tmp");
        const dir = try std.fmt.allocPrint(gpa, "{s}/basalt-ftp-{x}", .{ base, std.crypto.random.int(u64) });
        try std.fs.cwd().makePath(dir);
        local.dir = dir;
    }
    local.seq += 1;
    const sub = try std.fmt.allocPrint(arena, "{s}/{d}", .{ local.dir.?, local.seq });
    try std.fs.cwd().makePath(sub);

    const s = try Session.open(gpa, u);
    defer s.close();
    const result = if (url[url.len - 1] == '/') blk: {
        var files = std.array_list.Managed(Remote).init(arena);
        try walk(s, arena, u.path, "", 0, &files);
        for (files.items) |f| {
            const dest = try std.fs.path.join(arena, &.{ sub, f.rel });
            if (std.fs.path.dirname(dest)) |d| try std.fs.cwd().makePath(d);
            try download(s, f.remote, dest);
        }
        break :blk try std.fmt.allocPrint(arena, "{s}/", .{sub});
    } else blk: {
        const name = std.fs.path.basenamePosix(u.path);
        const dest = try std.fs.path.join(arena, &.{ sub, if (name.len > 0) name else "download" });
        try download(s, u.path, dest);
        break :blk dest;
    };
    try local.cache.put(gpa, try gpa.dupe(u8, url), try gpa.dupe(u8, result));
    return result;
}

/// A folder read fetches at most this many files, so a URL one level too high
/// fails fast rather than copying a whole public archive.
pub const max_folder_files = 10_000;
const max_depth = 32;

const Remote = struct { remote: []const u8, rel: []const u8 };

/// Every file under folder `dir` (ending in `/`), subfolders included, skipping
/// names that start with `_` or `.` as every folder read does.
fn walk(s: *Session, arena: std.mem.Allocator, dir: []const u8, rel: []const u8, depth: usize, out: *std.array_list.Managed(Remote)) anyerror!void {
    if (depth > max_depth) return note(error.FtpFolderTooDeep, "the folder nests more than 32 levels deep");
    for (try s.entries(arena, dir)) |en| {
        if (en.name[0] == '_' or en.name[0] == '.') continue;
        const remote = try std.mem.concat(arena, u8, &.{ dir, en.name });
        const r = try std.mem.concat(arena, u8, &.{ rel, en.name });
        if (en.dir) {
            try walk(s, arena, try std.mem.concat(arena, u8, &.{ remote, "/" }), try std.mem.concat(arena, u8, &.{ r, "/" }), depth + 1, out);
        } else {
            if (out.items.len == max_folder_files)
                return note(error.FtpFolderTooLarge, "the folder holds more than 10000 files; read a subfolder or a file");
            try out.append(.{ .remote = remote, .rel = r });
        }
    }
}

fn note(e: anyerror, text: []const u8) anyerror {
    const n = @min(text.len, last_err_buf.len);
    @memcpy(last_err_buf[0..n], text[0..n]);
    last_err_len = n;
    return e;
}

fn download(s: *Session, remote: []const u8, dest: []const u8) !void {
    const f = try std.fs.cwd().createFile(dest, .{});
    defer f.close();
    s.retrieve(remote, f) catch |e| {
        std.fs.cwd().deleteFile(dest) catch {};
        return e;
    };
}

/// `msg` with each local copy's path written back as the URL it came from, so an
/// error names what the script read; into `buf`, cut at its end.
pub fn unlocalize(buf: []u8, msg: []const u8) []const u8 {
    local.mutex.lock();
    defer local.mutex.unlock();
    var n: usize = 0;
    var i: usize = 0;
    outer: while (i < msg.len) {
        var it = local.cache.iterator();
        while (it.next()) |e| {
            const copy = e.value_ptr.*;
            if (!std.mem.startsWith(u8, msg[i..], copy)) continue;
            const url = e.key_ptr.*;
            const m = @min(url.len, buf.len - n);
            @memcpy(buf[n..][0..m], url[0..m]);
            n += m;
            i += copy.len;
            continue :outer;
        }
        if (n == buf.len) break;
        buf[n] = msg[i];
        n += 1;
        i += 1;
    }
    return buf[0..n];
}

/// A run starts: its downloads outlive any other run's `endRun`.
pub fn beginRun() void {
    local.mutex.lock();
    defer local.mutex.unlock();
    local.active += 1;
}

/// A run ends; the last one to end removes the downloads.
pub fn endRun() void {
    local.mutex.lock();
    defer local.mutex.unlock();
    local.active -|= 1;
    if (local.active == 0) clear();
}

/// Removes the downloads; the next read fetches afresh.
pub fn cleanup() void {
    local.mutex.lock();
    defer local.mutex.unlock();
    clear();
}

fn clear() void {
    const gpa = std.heap.page_allocator;
    if (local.dir) |d| {
        std.fs.cwd().deleteTree(d) catch {};
        gpa.free(d);
        local.dir = null;
    }
    var it = local.cache.iterator();
    while (it.next()) |e| {
        gpa.free(e.key_ptr.*);
        gpa.free(e.value_ptr.*);
    }
    local.cache.clearAndFree(gpa);
}

test "an ftp URL parses its login, port and percent-encoded path; anonymous without one" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const u = try parseUrl(a, "ftp://ana:s%40cret@ftp.example.org:2121/dados/arquivo%20um.csv");
    try std.testing.expectEqualStrings("ftp.example.org", u.host);
    try std.testing.expectEqual(@as(u16, 2121), u.port);
    try std.testing.expectEqualStrings("ana", u.user);
    try std.testing.expectEqualStrings("s@cret", u.pass);
    try std.testing.expectEqualStrings("/dados/arquivo um.csv", u.path);
    const anon = try parseUrl(a, "ftp://ftp.datasus.gov.br/dissemin/");
    try std.testing.expectEqual(@as(u16, 21), anon.port);
    try std.testing.expectEqualStrings("/dissemin/", anon.path);
    try std.testing.expectError(error.InvalidFtpUrl, parseUrl(a, "ftp:///x"));
}

test "an MLSD line is a file or a folder by its type fact; the current and parent folders are left out" {
    const f = mlsdEntry("type=file;size=12;modify=20260101000000; dados 2026.csv").?;
    try std.testing.expectEqualStrings("dados 2026.csv", f.name);
    try std.testing.expect(!f.dir);
    try std.testing.expect(mlsdEntry("modify=1;Type=DIR; sub").?.dir);
    try std.testing.expectEqual(@as(?Session.Entry, null), mlsdEntry("type=cdir; ."));
    try std.testing.expectEqual(@as(?Session.Entry, null), mlsdEntry("type=pdir; .."));
    try std.testing.expectEqual(@as(?Session.Entry, null), mlsdEntry("garbage"));
}

test "a registered name stands in for the host, port and login; the URL's login still wins" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try register("pub_t", .{ .host = "ftp.example.org", .port = 2121, .user = "carga", .password = "pw" });
    const u = try parseUrl(a, "ftp://pub_t/dados/x.csv");
    try std.testing.expectEqualStrings("ftp.example.org", u.host);
    try std.testing.expectEqual(@as(u16, 2121), u.port);
    try std.testing.expectEqualStrings("carga", u.user);
    try std.testing.expectEqualStrings("pw", u.pass);
    const other = try parseUrl(a, "ftp://ana@pub_t:21/x.csv");
    try std.testing.expectEqualStrings("ana", other.user);
    try std.testing.expectEqualStrings("anonymous@", other.pass);
    try std.testing.expectEqual(@as(u16, 21), other.port);
}

test "passive replies: EPSV's port, and PASV's with or without parentheses, its host ignored" {
    try std.testing.expectEqual(@as(?u16, 50123), epsvPort("Entering Extended Passive Mode (|||50123|)"));
    try std.testing.expectEqual(@as(?u16, 4 * 256 + 2), pasvPort("Entering Passive Mode (10,0,0,7,4,2)."));
    try std.testing.expectEqual(@as(?u16, 200 * 256 + 1), pasvPort("Entering Passive Mode 192,168,1,1,200,1"));
    try std.testing.expectEqual(@as(?u16, null), pasvPort("garbage"));
}

/// A scripted FTP server for one control connection at a time: a multi-line
/// greeting, anonymous login, EPSV (or a 500 for it when `pasv_only`), PASV that
/// advertises an address no client can reach, RETR of `files`, NLST and CWD, and
/// MLSD unless `pasv_only`.
const FakeServer = struct {
    listener: std.net.Server,
    pasv_only: bool = false,
    stop: std.atomic.Value(bool) = .init(false),

    const files = [_]struct { path: []const u8, body: []const u8 }{
        .{ .path = "/d/a.csv", .body = "x,y\n1,2\n" },
        .{ .path = "/d/b.csv", .body = "x,y\n3,4\n" },
        .{ .path = "/d/sub/c.csv", .body = "x,y\n5,6\n" },
        .{ .path = "/d/_SUCCESS", .body = "" },
        .{ .path = "/um arquivo.csv", .body = "x\n9\n" },
    };

    fn start(pasv_only: bool) !*FakeServer {
        const self = try std.testing.allocator.create(FakeServer);
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        self.* = .{ .listener = try addr.listen(.{ .reuse_address = true }), .pasv_only = pasv_only };
        return self;
    }
    fn url(self: *FakeServer, buf: []u8, path: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "ftp://127.0.0.1:{d}{s}", .{ self.listener.listen_address.getPort(), path }) catch unreachable;
    }
    fn finish(self: *FakeServer, th: std.Thread) void {
        self.stop.store(true, .seq_cst);
        if (std.net.tcpConnectToAddress(self.listener.listen_address)) |c| c.close() else |_| {}
        th.join();
        self.listener.deinit();
        std.testing.allocator.destroy(self);
    }
    fn run(self: *FakeServer) void {
        while (!self.stop.load(.seq_cst)) {
            const conn = self.listener.accept() catch return;
            defer conn.stream.close();
            if (self.stop.load(.seq_cst)) return;
            self.session(conn.stream) catch {};
        }
    }
    fn session(self: *FakeServer, s: std.net.Stream) !void {
        var rb: [1024]u8 = undefined;
        var wb: [1024]u8 = undefined;
        var sr = std.net.Stream.Reader.init(s, &rb);
        var sw = std.net.Stream.Writer.init(s, &wb);
        const w = &sw.interface;
        try w.writeAll("220-Welcome to the test server\r\n220-Second line\r\n220 Ready\r\n");
        try w.flush();
        var data: ?std.net.Server = null;
        defer if (data) |*d| d.deinit();
        while (true) {
            const raw = (try sr.interface().takeDelimiter('\n')) orelse return;
            const l = std.mem.trimRight(u8, raw, "\r");
            const sp = std.mem.indexOfScalar(u8, l, ' ') orelse l.len;
            const verb = l[0..sp];
            const arg = if (sp < l.len) l[sp + 1 ..] else "";
            if (std.mem.eql(u8, verb, "USER")) {
                try w.writeAll("331 Password please\r\n");
            } else if (std.mem.eql(u8, verb, "PASS")) {
                try w.writeAll("230 Logged in\r\n");
            } else if (std.mem.eql(u8, verb, "TYPE")) {
                try w.writeAll("200 Binary\r\n");
            } else if (std.mem.eql(u8, verb, "EPSV") or std.mem.eql(u8, verb, "PASV")) {
                if (self.pasv_only and std.mem.eql(u8, verb, "EPSV")) {
                    try w.writeAll("500 EPSV not understood\r\n");
                } else {
                    if (data) |*d| d.deinit();
                    data = try (try std.net.Address.parseIp("127.0.0.1", 0)).listen(.{ .reuse_address = true });
                    const p = data.?.listen_address.getPort();
                    if (verb[0] == 'E')
                        try w.print("229 Entering Extended Passive Mode (|||{d}|)\r\n", .{p})
                    else
                        try w.print("227 Entering Passive Mode (10,9,9,9,{d},{d}).\r\n", .{ p / 256, p % 256 });
                }
            } else if (std.mem.eql(u8, verb, "CWD")) {
                try w.writeAll(if (std.mem.eql(u8, arg, "/d/sub")) "250 Okay\r\n" else "550 Not a directory\r\n");
            } else if (std.mem.eql(u8, verb, "RETR") or std.mem.eql(u8, verb, "NLST") or
                (std.mem.eql(u8, verb, "MLSD") and !self.pasv_only))
            {
                var body: ?[]const u8 = null;
                if (verb[0] == 'R') {
                    for (files) |f| if (std.mem.eql(u8, f.path, arg)) {
                        body = f.body;
                    };
                } else if (verb[0] == 'N') {
                    if (std.mem.eql(u8, arg, "/d/")) body = "a.csv\r\nb.csv\r\nsub\r\n_SUCCESS\r\n";
                    if (std.mem.eql(u8, arg, "/d/sub/")) body = "/d/sub/c.csv\r\n";
                } else {
                    if (std.mem.eql(u8, arg, "/d/")) body = "type=cdir; .\r\ntype=file;size=8; a.csv\r\ntype=file;size=8; b.csv\r\nType=dir;modify=20260101000000; sub\r\ntype=file;size=0; _SUCCESS\r\n";
                    if (std.mem.eql(u8, arg, "/d/sub/")) body = "type=file;size=8; c.csv\r\n";
                }
                const b = body orelse {
                    try w.writeAll("550 No such file or directory\r\n");
                    try w.flush();
                    continue;
                };
                const conn = try data.?.accept();
                try w.writeAll("150 Opening BINARY mode data connection\r\n");
                try w.flush();
                try conn.stream.writeAll(b);
                conn.stream.close();
                try w.writeAll("226 Transfer complete\r\n");
            } else if (std.mem.eql(u8, verb, "QUIT")) {
                try w.writeAll("221 Bye\r\n");
                try w.flush();
                return;
            } else {
                try w.writeAll("502 Not implemented\r\n");
            }
            try w.flush();
        }
    }
};

fn readLocal(arena: std.mem.Allocator, path: []const u8) ![]const u8 {
    return std.fs.cwd().readFileAlloc(arena, path, 1 << 20);
}

/// `/d/` as the fake server holds it: both files, the subfolder's, no `_SUCCESS`.
fn expectFolder(a: std.mem.Allocator, dir: []const u8) !void {
    try std.testing.expect(std.mem.endsWith(u8, dir, "/"));
    try std.testing.expectEqualStrings("x,y\n3,4\n", try readLocal(a, try std.fs.path.join(a, &.{ dir, "b.csv" })));
    try std.testing.expectEqualStrings("x,y\n5,6\n", try readLocal(a, try std.fs.path.join(a, &.{ dir, "sub", "c.csv" })));
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().access(try std.fs.path.join(a, &.{ dir, "_SUCCESS" }), .{}));
}

test "a file downloads over EPSV, a folder and its subfolder over MLSD, and a second read uses the copy" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const srv = try FakeServer.start(false);
    const th = try std.Thread.spawn(.{}, FakeServer.run, .{srv});
    defer srv.finish(th);
    defer cleanup();

    var ub: [128]u8 = undefined;
    const one = try localize(a, srv.url(&ub, "/d/a.csv"));
    try std.testing.expectEqualStrings("a.csv", std.fs.path.basename(one));
    try std.testing.expectEqualStrings("x,y\n1,2\n", try readLocal(a, one));
    try std.testing.expectEqualStrings(one, try localize(a, srv.url(&ub, "/d/a.csv")));

    const dir = try localize(a, srv.url(&ub, "/d/"));
    try expectFolder(a, dir);

    var mb: [256]u8 = undefined;
    const msg = try std.fmt.allocPrint(a, "no file in folder `{s}` nor `{s}sub/c.csv`", .{ dir, dir });
    const want = try std.fmt.allocPrint(a, "no file in folder `{s}` nor `{s}sub/c.csv`", .{ srv.url(&ub, "/d/"), srv.url(&ub, "/d/") });
    try std.testing.expectEqualStrings(want, unlocalize(&mb, msg));

    const member = try localize(a, try std.mem.concat(a, u8, &.{ srv.url(&ub, "/d/a.csv"), " :: inner.csv" }));
    try std.testing.expectEqualStrings(try std.mem.concat(a, u8, &.{ one, " :: inner.csv" }), member);
}

test "PASV's advertised host is ignored, a folder is walked by NLST and CWD without MLSD, and 550 is a missing file" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const srv = try FakeServer.start(true);
    const th = try std.Thread.spawn(.{}, FakeServer.run, .{srv});
    defer srv.finish(th);
    defer cleanup();

    var ub: [128]u8 = undefined;
    const p = try localize(a, srv.url(&ub, "/um%20arquivo.csv"));
    try std.testing.expectEqualStrings("x\n9\n", try readLocal(a, p));
    try expectFolder(a, try localize(a, srv.url(&ub, "/d/")));
    try std.testing.expectError(error.FileNotFound, localize(a, srv.url(&ub, "/nope.csv")));
    try std.testing.expectEqualStrings("550 No such file or directory", lastError());
}

test "the downloads stay until the last of two overlapping runs ends" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const srv = try FakeServer.start(false);
    const th = try std.Thread.spawn(.{}, FakeServer.run, .{srv});
    defer srv.finish(th);

    var ub: [128]u8 = undefined;
    beginRun();
    beginRun();
    const p = try localize(a, srv.url(&ub, "/d/a.csv"));
    endRun();
    try std.fs.cwd().access(p, .{});
    endRun();
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().access(p, .{}));
}

test "cleanup removes the downloads" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const srv = try FakeServer.start(false);
    const th = try std.Thread.spawn(.{}, FakeServer.run, .{srv});
    defer srv.finish(th);

    var ub: [128]u8 = undefined;
    const p = try localize(a, srv.url(&ub, "/d/b.csv"));
    cleanup();
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().access(p, .{}));
}
