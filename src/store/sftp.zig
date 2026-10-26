//! SFTP (version 3, draft-ietf-secsh-filexfer-02) over `ssh.zig` — reading and
//! writing files on an SFTP server: `sftp://[user@]host[:port]/path`.
//!
//! A file is read three ways. `File.read` answers a byte range, which is what
//! Parquet's footer-then-chunks reads and a zip's directory-then-member reads
//! need, so an `.xlsx` or a `.parquet` on a server is read without fetching the
//! rest of it. `Stream` reads front to back with requests kept in flight ahead
//! of the reader, for CSV; a `window` stream borrows its file, an opened one owns
//! it. And `Upload` writes to `name.part`, renamed over `name` by `finish` and
//! removed by `abort` or any failure, so a reader polling the folder never sees
//! half a file.
//!
//! Each request costs a round trip, so reads and writes are pipelined: up to
//! `inflight` requests out at once, matched to their replies by id. A server
//! may answer a read short of what was asked before the end of the file; the
//! rest is asked for again, and on failure what is still in flight is drained.
//!
//! Logging in is the expensive part — a key exchange and an authentication —
//! and an Excel workbook alone opens its file seven times, a parallel Parquet
//! read once per lane. Sessions are pooled per server and user for the life of
//! the process, each opened file its own handle on a session the opener has to
//! itself until it closes the file. `CREATE CONNECTION … TYPE sftp` registers a
//! `Conn` so a path's host part can name it. `last_error` (thread-local) keeps
//! why the last attempt to reach a server failed, in the session's own words (an
//! unknown host key's fingerprint) when it got that far.

const std = @import("std");
const msgfmt = @import("../format/msgfmt.zig");
const ThreadNote = @import("../connect/driver.zig").ThreadNote;
const remotefs = @import("remotefs.zig");
const scheme = @import("scheme.zig");
const ssh = @import("../net/ssh.zig");

pub const Error = error{
    SftpProtocol,
    SftpNoSuchFile,
    SftpPermissionDenied,
    SftpFailure,
    SftpUnsupported,
    NotSftpUrl,
};

const fxp = struct {
    const init = 1;
    const version = 2;
    const open = 3;
    const close = 4;
    const read = 5;
    const write = 6;
    const fstat = 8;
    const opendir = 11;
    const readdir = 12;
    const remove = 13;
    const stat = 17;
    const rename = 18;
    const status = 101;
    const handle = 102;
    const data = 103;
    const name = 104;
    const attrs = 105;
    const extended = 200;
    const extended_reply = 201;
};

const flags = struct {
    const read = 0x01;
    const write = 0x02;
    const creat = 0x08;
    const trunc = 0x10;
};

const status = struct {
    const ok = 0;
    const eof = 1;
    const no_such_file = 2;
    const permission_denied = 3;
    const op_unsupported = 8;
};

const inflight = 32;

pub fn isUrl(path: []const u8) bool {
    return scheme.of(path) == .sftp;
}

pub const Conn = struct {
    host: []const u8,
    port: u16 = 22,
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
    key_file: ?[]const u8 = null,
    key_passphrase: ?[]const u8 = null,
    known_hosts: ?[]const u8 = null,
    host_key: ?[]const u8 = null,
};

const Names = remotefs.Registry(Conn, false);

/// Make `sftp://name/…` reach the server `c` describes.
pub const register = Names.put;

pub const Client = struct {
    gpa: std.mem.Allocator,
    ssh: *ssh.Session,
    next_id: u32 = 1,
    inbuf: std.array_list.Managed(u8),
    consumed: usize = 0,
    out: ssh.Buf,
    max_read: u32 = 32 * 1024,
    max_write: u32 = 32 * 1024,
    posix_rename: bool = false,
    why: std.array_list.Managed(u8),

    fn open(gpa: std.mem.Allocator, cfg: ssh.Config) !*Client {
        const session = try ssh.Session.connect(gpa, cfg);
        errdefer session.close();
        try session.openSubsystem("sftp");
        const self = try gpa.create(Client);
        self.* = .{ .gpa = gpa, .ssh = session, .inbuf = .init(gpa), .out = .init(gpa), .why = .init(gpa) };
        errdefer self.destroy();
        var b = ssh.Buf.init(gpa);
        defer b.deinit();
        try b.u32be(5);
        try b.byte(fxp.init);
        try b.u32be(3);
        try session.send(b.list.items);
        const v = try self.reply();
        if (v[0] != fxp.version) return error.SftpProtocol;
        var c = ssh.Cursor{ .p = v[1..] };
        _ = try c.int(u32);
        var limits = false;
        while (c.left() > 0) {
            const n = try c.prefixed(u32);
            _ = try c.prefixed(u32);
            if (std.mem.eql(u8, n, "posix-rename@openssh.com")) self.posix_rename = true;
            if (std.mem.eql(u8, n, "limits@openssh.com")) limits = true;
        }
        if (limits) self.readLimits() catch {};
        return self;
    }

    /// `limits@openssh.com`: the largest read and write the server takes.
    fn readLimits(self: *Client) !void {
        const id = try self.begin(fxp.extended);
        try self.out.str("limits@openssh.com");
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] != fxp.extended_reply) return;
        var c = ssh.Cursor{ .p = r[5..] };
        _ = try c.int(u64);
        const mr = try c.int(u64);
        const mw = try c.int(u64);
        if (mr > 0) self.max_read = @intCast(@min(mr, 255 * 1024));
        if (mw > 0) self.max_write = @intCast(@min(mw, 255 * 1024));
    }

    fn destroy(self: *Client) void {
        self.ssh.close();
        self.inbuf.deinit();
        self.out.deinit();
        self.why.deinit();
        self.gpa.destroy(self);
    }

    pub fn lastError(self: *const Client) []const u8 {
        return if (self.why.items.len > 0) self.why.items else self.ssh.lastError();
    }

    /// Start a request of `kind`, with no reason kept from an earlier one: its length
    /// is patched in by `finish`.
    fn begin(self: *Client, kind: u8) !u32 {
        self.why.clearRetainingCapacity();
        self.out.list.clearRetainingCapacity();
        try self.out.u32be(0);
        try self.out.byte(kind);
        const id = self.next_id;
        self.next_id +%= 1;
        try self.out.u32be(id);
        return id;
    }

    fn finish(self: *Client) !void {
        const b = self.out.list.items;
        std.mem.writeInt(u32, b[0..4], @intCast(b.len - 4), .big);
        try self.ssh.send(b);
    }

    /// The next whole SFTP packet (type byte first), valid until the next call. Unread
    /// bytes move to the front only once the read part outweighs them: moving them on
    /// every reply copied megabytes per reply and ran Parquet reads at a seventh of the link.
    fn reply(self: *Client) ![]const u8 {
        const pending = self.inbuf.items.len - self.consumed;
        if (self.consumed > 0 and self.consumed >= pending) {
            std.mem.copyForwards(u8, self.inbuf.items[0..pending], self.inbuf.items[self.consumed..]);
            self.inbuf.shrinkRetainingCapacity(pending);
            self.consumed = 0;
        }
        const at = self.consumed;
        while (self.inbuf.items.len - at < 4) try self.ssh.recv(&self.inbuf);
        const n = std.mem.readInt(u32, self.inbuf.items[at..][0..4], .big);
        if (n == 0 or n > 512 * 1024) return error.SftpProtocol;
        while (self.inbuf.items.len - at < 4 + n) try self.ssh.recv(&self.inbuf);
        self.consumed = at + 4 + n;
        return self.inbuf.items[at + 4 .. at + 4 + n];
    }

    fn replyFor(self: *Client, id: u32) ![]const u8 {
        const r = try self.reply();
        if (r.len < 5 or std.mem.readInt(u32, r[1..5], .big) != id) return error.SftpProtocol;
        return r;
    }

    fn statusOf(self: *Client, r: []const u8) !void {
        if (r[0] != fxp.status) return error.SftpProtocol;
        var c = ssh.Cursor{ .p = r[5..] };
        const code = try c.int(u32);
        const text = c.prefixed(u32) catch "";
        self.why.clearRetainingCapacity();
        try self.why.appendSlice(text);
        return switch (code) {
            status.ok => {},
            status.eof => error.EndOfStream,
            status.no_such_file => error.SftpNoSuchFile,
            status.permission_denied => error.SftpPermissionDenied,
            status.op_unsupported => error.SftpUnsupported,
            else => error.SftpFailure,
        };
    }

    fn openHandle(self: *Client, path: []const u8, pflags: u32) ![]const u8 {
        const id = try self.begin(fxp.open);
        try self.out.str(path);
        try self.out.u32be(pflags);
        try self.out.u32be(0);
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] == fxp.handle) {
            var c = ssh.Cursor{ .p = r[5..] };
            return self.gpa.dupe(u8, try c.prefixed(u32));
        }
        try self.statusOf(r);
        return error.SftpProtocol;
    }

    fn closeHandle(self: *Client, handle: []const u8) !void {
        const id = try self.begin(fxp.close);
        try self.out.str(handle);
        try self.finish();
        try self.statusOf(try self.replyFor(id));
    }

    fn sizeOf(self: *Client, handle: ?[]const u8, path: []const u8) !u64 {
        const id = try self.begin(if (handle != null) fxp.fstat else fxp.stat);
        try self.out.str(handle orelse path);
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] != fxp.attrs) {
            try self.statusOf(r);
            return error.SftpProtocol;
        }
        var c = ssh.Cursor{ .p = r[5..] };
        const a = try readAttrs(&c);
        return a.size orelse error.SftpUnsupported;
    }

    fn readRange(self: *Client, handle: []const u8, off: u64, out: []u8) !usize {
        const Req = struct { id: u32, at: u64, len: u32 };
        var reqs: [inflight]Req = undefined;
        var n_out: usize = 0;
        var next_at: u64 = off;
        const end = off + out.len;
        var got: usize = 0;
        var eof = false;
        while (true) {
            while (!eof and n_out < inflight and next_at < end) {
                const len: u32 = @intCast(@min(@as(u64, self.max_read), end - next_at));
                const id = try self.begin(fxp.read);
                try self.out.str(handle);
                try self.out.u64be(next_at);
                try self.out.u32be(len);
                try self.finish();
                reqs[n_out] = .{ .id = id, .at = next_at, .len = len };
                n_out += 1;
                next_at += len;
            }
            if (n_out == 0) break;
            const r = try self.reply();
            if (r.len < 5) return error.SftpProtocol;
            const id = std.mem.readInt(u32, r[1..5], .big);
            const k = for (reqs[0..n_out], 0..) |q, i| {
                if (q.id == id) break i;
            } else return error.SftpProtocol;
            const q = reqs[k];
            reqs[k] = reqs[n_out - 1];
            n_out -= 1;
            if (r[0] == fxp.data) {
                var c = ssh.Cursor{ .p = r[5..] };
                const d = try c.prefixed(u32);
                if (d.len > q.len) return error.SftpProtocol;
                const dst: usize = @intCast(q.at - off);
                @memcpy(out[dst..][0..d.len], d);
                got += d.len;
                if (d.len < q.len and d.len > 0) {
                    const id2 = try self.begin(fxp.read);
                    try self.out.str(handle);
                    try self.out.u64be(q.at + d.len);
                    try self.out.u32be(q.len - @as(u32, @intCast(d.len)));
                    try self.finish();
                    reqs[n_out] = .{ .id = id2, .at = q.at + d.len, .len = q.len - @as(u32, @intCast(d.len)) };
                    n_out += 1;
                } else if (d.len == 0) eof = true;
            } else {
                self.statusOf(r) catch |e| switch (e) {
                    error.EndOfStream => {
                        eof = true;
                        continue;
                    },
                    else => {
                        while (n_out > 0) : (n_out -= 1) _ = self.reply() catch break;
                        return e;
                    },
                };
            }
        }
        return @min(got, out.len);
    }

    fn writeRange(self: *Client, handle: []const u8, off: u64, data: []const u8) !void {
        var ids: [inflight]u32 = undefined;
        var n_out: usize = 0;
        var at: usize = 0;
        while (at < data.len or n_out > 0) {
            while (at < data.len and n_out < inflight) {
                const len = @min(self.max_write, data.len - at);
                const id = try self.begin(fxp.write);
                try self.out.str(handle);
                try self.out.u64be(off + at);
                try self.out.str(data[at..][0..len]);
                try self.finish();
                ids[n_out] = id;
                n_out += 1;
                at += len;
            }
            const r = try self.reply();
            if (r.len < 5) return error.SftpProtocol;
            const id = std.mem.readInt(u32, r[1..5], .big);
            const k = for (ids[0..n_out], 0..) |q, i| {
                if (q == id) break i;
            } else return error.SftpProtocol;
            ids[k] = ids[n_out - 1];
            n_out -= 1;
            self.statusOf(r) catch |e| {
                while (n_out > 0) : (n_out -= 1) _ = self.reply() catch break;
                return e;
            };
        }
    }

    /// Uses posix-rename@openssh.com when offered; plain RENAME refuses an existing
    /// target, so that is removed first.
    fn rename(self: *Client, from: []const u8, to: []const u8) !void {
        if (self.posix_rename) {
            const id = try self.begin(fxp.extended);
            try self.out.str("posix-rename@openssh.com");
            try self.out.str(from);
            try self.out.str(to);
            try self.finish();
            return self.statusOf(try self.replyFor(id));
        }
        self.remove(to) catch {};
        const id = try self.begin(fxp.rename);
        try self.out.str(from);
        try self.out.str(to);
        try self.finish();
        return self.statusOf(try self.replyFor(id));
    }

    fn remove(self: *Client, path: []const u8) !void {
        const id = try self.begin(fxp.remove);
        try self.out.str(path);
        try self.finish();
        return self.statusOf(try self.replyFor(id));
    }

    fn listDir(self: *Client, arena: std.mem.Allocator, path: []const u8) ![]remotefs.Entry {
        const id = try self.begin(fxp.opendir);
        try self.out.str(path);
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] != fxp.handle) {
            try self.statusOf(r);
            return error.SftpProtocol;
        }
        var hc = ssh.Cursor{ .p = r[5..] };
        const handle = try arena.dupe(u8, try hc.prefixed(u32));
        var out = std.array_list.Managed(remotefs.Entry).init(arena);
        while (true) {
            const rid = try self.begin(fxp.readdir);
            try self.out.str(handle);
            try self.finish();
            const rr = try self.replyFor(rid);
            if (rr[0] != fxp.name) {
                self.statusOf(rr) catch |e| switch (e) {
                    error.EndOfStream => break,
                    else => return e,
                };
                break;
            }
            var c = ssh.Cursor{ .p = rr[5..] };
            const count = try c.int(u32);
            for (0..count) |_| {
                const name = try c.prefixed(u32);
                _ = try c.prefixed(u32);
                const a = try readAttrs(&c);
                try out.append(.{
                    .name = try arena.dupe(u8, name),
                    .file = if (a.perms) |p| (p & 0o170000) == 0o100000 else true,
                    .dir = if (a.perms) |p| (p & 0o170000) == 0o040000 else false,
                });
            }
        }
        self.closeHandle(handle) catch {};
        return out.items;
    }
};

const Attrs = struct { size: ?u64 = null, perms: ?u32 = null };

fn readAttrs(c: *ssh.Cursor) !Attrs {
    var a = Attrs{};
    const f = try c.int(u32);
    if (f & 0x1 != 0) a.size = try c.int(u64);
    if (f & 0x2 != 0) {
        _ = try c.int(u32);
        _ = try c.int(u32);
    }
    if (f & 0x4 != 0) a.perms = try c.int(u32);
    if (f & 0x8 != 0) {
        _ = try c.int(u32);
        _ = try c.int(u32);
    }
    if (f & 0x80000000 != 0) {
        const n = try c.int(u32);
        for (0..n) |_| {
            _ = try c.prefixed(u32);
            _ = try c.prefixed(u32);
        }
    }
    return a;
}

threadlocal var last_error: ThreadNote(512) = .{};

pub fn lastError() []const u8 {
    return last_error.get();
}

const Fs = remotefs.RemoteFs(Proto);
pub const File = Fs.File;
/// Fetches 1 MiB at a time (32 reads in flight): fetching only the reader's 64 KiB
/// kept two reads in flight and streamed CSV at a quarter of a Parquet read's speed.
pub const Stream = Fs.Stream;
pub const Upload = Fs.Upload;
pub const listPrefix = Fs.listPrefix;

const Proto = struct {
    pub const Link = Client;
    pub const Login = ssh.Config;
    pub const Handle = struct { id: []const u8, size: u64 = 0, open: bool = true };
    pub const Target = struct { cfg: ssh.Config, path: []const u8 };
    pub const stream_chunk = 1 << 20;
    pub const upload_buffer = 1 << 20;

    /// The host may name a registered connection; otherwise the user is the URL's or
    /// `$USER`, the password the URL's or `SFTP_PASSWORD`, the key `~/.ssh/id_ed25519`
    /// if present. A URL's `user[:password]` is percent-decoded.
    /// `/~/x` is `x` under the login's home, as curl reads it; other paths are absolute.
    pub fn resolve(arena: std.mem.Allocator, url: []const u8) !Target {
        if (!isUrl(url)) return error.NotSftpUrl;
        const rest = scheme.Scheme.rest(url);
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.NotSftpUrl;
        const a = remotefs.authority(rest[0..slash]) orelse return error.NotSftpUrl;
        var path = rest[slash..];
        if (std.mem.startsWith(u8, path, "/~/")) path = path[3..];
        const info: ?scheme.Userinfo = if (a.userinfo) |s| try scheme.Userinfo.parse(arena, s, .{}) else null;
        const url_user = if (info) |i| i.user else null;
        const url_pass = if (info) |i| i.password else null;
        const user = url_user orelse std.posix.getenv("USER") orelse "root";
        if (Names.get(a.host)) |c| return .{ .path = path, .cfg = .{
            .host = c.host,
            .port = a.port orelse c.port,
            .user = url_user orelse c.user orelse user,
            .password = url_pass orelse c.password,
            .key_file = c.key_file,
            .key_passphrase = c.key_passphrase,
            .known_hosts = c.known_hosts,
            .host_key = c.host_key,
        } };
        var key_file: ?[]const u8 = null;
        if (std.posix.getenv("HOME")) |home| {
            const kf = try msgfmt.allocPrint(arena, "{s}/.ssh/id_ed25519", .{home});
            if (std.fs.cwd().access(kf, .{})) |_| key_file = kf else |_| {}
        }
        return .{ .path = path, .cfg = .{
            .host = a.host,
            .port = a.port orelse 22,
            .user = user,
            .password = url_pass orelse std.posix.getenv("SFTP_PASSWORD"),
            .key_file = key_file,
        } };
    }

    /// A session is reused only for the same login and the same trust: a connection
    /// with another key or pin must not inherit a session the first one's checks let through.
    pub fn poolKey(buf: []u8, cfg: ssh.Config) []const u8 {
        var h = std.hash.Wyhash.init(0);
        inline for (.{ "password", "key_file", "key_passphrase", "known_hosts", "host_key" }) |f| {
            const v = @field(cfg, f) orelse "";
            h.update(std.mem.asBytes(&v.len));
            h.update(v);
            h.update(&[_]u8{@intFromBool(@field(cfg, f) != null)});
        }
        return std.fmt.bufPrint(buf, "{s}@{s}:{d}#{x}", .{ cfg.user, cfg.host, cfg.port, h.final() }) catch cfg.host;
    }

    pub fn clearNotes() void {
        ssh.clearFailure();
        last_error.clear();
    }

    pub fn connect(cfg: ssh.Config) !*Client {
        return Client.open(std.heap.page_allocator, cfg) catch |e| {
            last_error.set(sshWhy(e, cfg));
            return e;
        };
    }

    /// One stat of the home directory.
    pub fn ping(c: *Client) bool {
        const id = c.begin(fxp.stat) catch return false;
        c.out.str(".") catch return false;
        c.finish() catch return false;
        const r = c.replyFor(id) catch return false;
        return r[0] == fxp.attrs or r[0] == fxp.status;
    }

    pub fn destroy(c: *Client) void {
        c.destroy();
    }

    /// A file or directory error keeps the session usable; a transport error does not.
    pub fn protocolOk(e: anyerror) bool {
        return switch (e) {
            error.SftpNoSuchFile, error.SftpPermissionDenied, error.SftpFailure, error.SftpUnsupported, error.EndOfStream => true,
            else => false,
        };
    }

    pub fn why(c: *Client) []const u8 {
        return c.lastError();
    }

    /// `e`, its reason kept on the thread before anything else is asked of the session.
    fn noted(c: *Client, e: anyerror) anyerror {
        last_error.set(c.lastError());
        return e;
    }

    pub fn openRead(c: *Client, t: Target) !Handle {
        const id = c.openHandle(t.path, flags.read) catch |e| return noted(c, e);
        const size = c.sizeOf(id, t.path) catch |e| {
            const err = noted(c, e);
            c.closeHandle(id) catch {};
            c.gpa.free(id);
            return err;
        };
        return .{ .id = id, .size = size };
    }

    pub fn openWrite(c: *Client, _: Target, part: []const u8) !Handle {
        return .{ .id = c.openHandle(part, flags.write | flags.creat | flags.trunc) catch |e| return noted(c, e) };
    }

    pub fn readAt(c: *Client, h: Handle, off: u64, buf: []u8) !usize {
        return c.readRange(h.id, off, buf);
    }

    pub fn writeAt(c: *Client, h: Handle, off: u64, data: []const u8) !void {
        return c.writeRange(h.id, off, data);
    }

    pub fn closeHandle(c: *Client, h: Handle) !void {
        return c.closeHandle(h.id);
    }

    pub fn freeHandle(c: *Client, h: Handle) void {
        c.gpa.free(h.id);
    }

    pub fn commit(c: *Client, h: *Handle, part: []const u8, path: []const u8) !void {
        try c.closeHandle(h.id);
        h.open = false;
        try c.rename(part, path);
    }

    pub fn discard(c: *Client, h: *Handle, part: []const u8) void {
        if (h.open) c.closeHandle(h.id) catch {};
        c.remove(part) catch {};
    }

    pub fn listDir(c: *Client, arena: std.mem.Allocator, t: Target, rel: []const u8) ![]const remotefs.Entry {
        const top = if (t.path.len > 1 and t.path[t.path.len - 1] == '/') t.path[0 .. t.path.len - 1] else t.path;
        const dir = if (rel.len > 0) try msgfmt.allocPrint(arena, "{s}/{s}", .{ top, rel }) else if (top.len == 0) "." else top;
        return c.listDir(arena, dir) catch |e| return noted(c, e);
    }
};

threadlocal var why_buf: [600]u8 = undefined;

/// An SSH failure in words. A failed connect leaves no session to ask, so the error
/// and the server name it.
fn sshWhy(e: anyerror, cfg: ssh.Config) []const u8 {
    const said = ssh.lastFailure();
    if (said.len > 0) return said;
    return std.fmt.bufPrint(&why_buf, "{s} connecting to {s}:{d} as {s}", .{ @errorName(e), cfg.host, cfg.port, cfg.user }) catch @errorName(e);
}

test "sftp URLs: user, port, home-relative, registered names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = try Proto.resolve(a, "sftp://ana@files.example.com:2222/in/x.csv");
    try std.testing.expectEqualStrings("files.example.com", t.cfg.host);
    try std.testing.expectEqual(@as(u16, 2222), t.cfg.port);
    try std.testing.expectEqualStrings("ana", t.cfg.user);
    try std.testing.expectEqualStrings("/in/x.csv", t.path);
    try std.testing.expectEqualStrings("x.csv", (try Proto.resolve(a, "sftp://h/~/x.csv")).path);
    try std.testing.expectError(error.NotSftpUrl, Proto.resolve(a, "sftp://nohostpath"));
    try register("bank_t", .{ .host = "sftp.bank.example", .port = 2200, .user = "u1", .password = "p" });
    const b = try Proto.resolve(a, "sftp://bank_t/ret/a.ret");
    try std.testing.expectEqualStrings("sftp.bank.example", b.cfg.host);
    try std.testing.expectEqual(@as(u16, 2200), b.cfg.port);
    try std.testing.expectEqualStrings("u1", b.cfg.user);
}

test "sftp URLs: user and password split at the colon and decoded, in any scheme case" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = try Proto.resolve(a, "SFTP://u:pw@unregistered-host.invalid/data/x");
    try std.testing.expectEqualStrings("unregistered-host.invalid", t.cfg.host);
    try std.testing.expectEqualStrings("u", t.cfg.user);
    try std.testing.expectEqualStrings("pw", t.cfg.password.?);
    try std.testing.expectEqualStrings("/data/x", t.path);
    const enc = try Proto.resolve(a, "sftp://a%40corp:p%3Aw%40@unregistered-host.invalid/x");
    try std.testing.expectEqualStrings("a@corp", enc.cfg.user);
    try std.testing.expectEqualStrings("p:w@", enc.cfg.password.?);
    try register("bank_pw", .{ .host = "sftp.bank.example", .user = "u1", .password = "conf" });
    const r = try Proto.resolve(a, "sftp://u2:url@bank_pw/x");
    try std.testing.expectEqualStrings("u2", r.cfg.user);
    try std.testing.expectEqualStrings("url", r.cfg.password.?);
    try std.testing.expectEqualStrings("conf", (try Proto.resolve(a, "sftp://u2@bank_pw/x")).cfg.password.?);
}

test "live: upload, list, read back whole, by range and streaming; an aborted upload leaves nothing (BASALT_SSH_TEST_PORT)" {
    const port = std.posix.getenv("BASALT_SSH_TEST_PORT") orelse return error.SkipZigTest;
    const dir = std.posix.getenv("BASALT_SSH_TEST_DIR") orelse return error.SkipZigTest;
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try register("probe", .{
        .host = "127.0.0.1",
        .port = try std.fmt.parseInt(u16, port, 10),
        .user = "basalt",
        .password = "pw",
        .known_hosts = try msgfmt.allocPrint(a, "{s}/known_hosts", .{dir}),
    });
    const body = try a.alloc(u8, 3 * 1024 * 1024 + 123);
    for (body, 0..) |*b, i| b.* = @truncate(i *% 31 +% (i >> 9));

    const up = try Upload.open(a, "sftp://probe/~/t/big.bin");
    try up.interface.writeAll(body);
    try up.finish();

    const names = try listPrefix(a, "sftp://probe/~/t/");
    try std.testing.expect(names.len >= 1);
    var found = false;
    for (names) |n| found = found or std.mem.eql(u8, n, "big.bin");
    try std.testing.expect(found);

    const f = try File.open(a, "sftp://probe/~/t/big.bin");
    defer f.close();
    try std.testing.expectEqual(@as(u64, body.len), f.size);
    try std.testing.expectEqualSlices(u8, body, try f.read(a, 0, body.len));
    try std.testing.expectEqualSlices(u8, body[1_000_001..][0..77_777], try f.read(a, 1_000_001, 77_777));

    const st = try Stream.open(a, "sftp://probe/~/t/big.bin");
    defer st.close();
    const all = try st.interface.allocRemaining(a, .unlimited);
    try std.testing.expectEqualSlices(u8, body, all);

    const ab = try Upload.open(a, "sftp://probe/~/t/aborted.bin");
    try ab.interface.writeAll("partial");
    ab.abort();
    const after = try listPrefix(a, "sftp://probe/~/t/");
    for (after) |n| try std.testing.expect(std.mem.indexOf(u8, n, "aborted") == null);

    try std.testing.expectError(error.SftpNoSuchFile, File.open(a, "sftp://probe/~/t/missing.bin"));
}
