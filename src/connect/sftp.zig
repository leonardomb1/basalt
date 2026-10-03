//! SFTP (version 3, draft-ietf-secsh-filexfer-02) over `ssh.zig` — reading and
//! writing files on an SFTP server: `sftp://[user@]host[:port]/path`.
//!
//! A file is read three ways. `File.read` answers a byte range, which is what
//! Parquet's footer-then-chunks reads and a zip's directory-then-member reads
//! need, so an `.xlsx` or a `.parquet` on a server is read without fetching the
//! rest of it. `Stream` reads front to back with requests kept in flight ahead
//! of the reader, for CSV. And `Upload` writes to `name.part`, renamed over
//! `name` when complete, so a reader polling the folder never sees half a file.
//!
//! Each request costs a round trip, so reads and writes are pipelined: up to
//! `inflight` requests out at once, matched to their replies by id. A server
//! may answer a read short of what was asked before the end of the file; the
//! rest is asked for again.
//!
//! Logging in is the expensive part — a key exchange and an authentication —
//! and an Excel workbook alone opens its file seven times, a parallel Parquet
//! read once per lane. Sessions are pooled per server and user for the life of
//! the process, each opened file its own handle on a session the opener has to
//! itself until it closes the file.

const std = @import("std");
const ssh = @import("ssh.zig");

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

/// Requests kept in flight on a read or write.
const inflight = 32;

pub fn isUrl(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "sftp://");
}

/// What a `CREATE CONNECTION … TYPE sftp` resolved to, registered by the runtime
/// so a path's host part can name it.
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

var registry_mtx: std.Thread.Mutex = .{};
var registry: std.StringHashMapUnmanaged(Conn) = .empty;

/// Make `sftp://name/…` reach the server `c` describes. Process-wide, as the
/// pool is; a later registration of a name replaces the earlier one. Strings
/// are copied.
pub fn register(name: []const u8, c: Conn) !void {
    const gpa = std.heap.page_allocator;
    registry_mtx.lock();
    defer registry_mtx.unlock();
    var owned = c;
    inline for (.{ "host", "user", "password", "key_file", "key_passphrase", "known_hosts", "host_key" }) |f| {
        const v = @field(c, f);
        if (@TypeOf(v) == []const u8) {
            @field(owned, f) = try gpa.dupe(u8, v);
        } else if (v) |s| @field(owned, f) = try gpa.dupe(u8, s);
    }
    const key = try gpa.dupe(u8, name);
    try registry.put(gpa, key, owned);
}

/// A parsed `sftp://` path: the session it needs and the file on it.
pub const Target = struct {
    cfg: ssh.Config,
    path: []const u8,
};

/// `sftp://[user@]host[:port]/path`. The host may name a registered connection;
/// otherwise the user is the URL's or `$USER`, the password `SFTP_PASSWORD`, the
/// key `~/.ssh/id_ed25519` when there is one. `/~/x` is `x` under the login's
/// home directory, as curl reads it; any other path is absolute.
pub fn resolve(arena: std.mem.Allocator, url: []const u8) !Target {
    if (!isUrl(url)) return error.NotSftpUrl;
    const rest = url["sftp://".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.NotSftpUrl;
    var auth = rest[0..slash];
    var path = rest[slash..];
    if (std.mem.startsWith(u8, path, "/~/")) path = path[3..];
    var user: ?[]const u8 = null;
    if (std.mem.lastIndexOfScalar(u8, auth, '@')) |at| {
        user = auth[0..at];
        auth = auth[at + 1 ..];
    }
    var host = auth;
    var port: ?u16 = null;
    if (std.mem.lastIndexOfScalar(u8, auth, ':')) |c| {
        host = auth[0..c];
        port = std.fmt.parseInt(u16, auth[c + 1 ..], 10) catch return error.NotSftpUrl;
    }
    if (host.len == 0) return error.NotSftpUrl;

    registry_mtx.lock();
    const named = registry.get(host);
    registry_mtx.unlock();
    if (named) |c| return .{ .path = path, .cfg = .{
        .host = c.host,
        .port = port orelse c.port,
        .user = user orelse c.user orelse std.posix.getenv("USER") orelse "root",
        .password = c.password,
        .key_file = c.key_file,
        .key_passphrase = c.key_passphrase,
        .known_hosts = c.known_hosts,
        .host_key = c.host_key,
    } };

    var key_file: ?[]const u8 = null;
    if (std.posix.getenv("HOME")) |home| {
        const kf = try std.fmt.allocPrint(arena, "{s}/.ssh/id_ed25519", .{home});
        if (std.fs.cwd().access(kf, .{})) |_| key_file = kf else |_| {}
    }
    return .{ .path = path, .cfg = .{
        .host = host,
        .port = port orelse 22,
        .user = user orelse std.posix.getenv("USER") orelse "root",
        .password = std.posix.getenv("SFTP_PASSWORD"),
        .key_file = key_file,
    } };
}

// --- the protocol client ----------------------------------------------------

pub const Client = struct {
    gpa: std.mem.Allocator,
    ssh: *ssh.Session,
    key: []const u8,
    next_id: u32 = 1,
    inbuf: std.array_list.Managed(u8),
    /// Bytes of `inbuf` already handed out by `reply`.
    consumed: usize = 0,
    out: ssh.Buf,
    max_read: u32 = 32 * 1024,
    max_write: u32 = 32 * 1024,
    posix_rename: bool = false,
    /// Server-side reason for the last failed status.
    why: std.array_list.Managed(u8),

    fn open(gpa: std.mem.Allocator, cfg: ssh.Config, key: []const u8) !*Client {
        const session = try ssh.Session.connect(gpa, cfg);
        errdefer session.close();
        try session.openSubsystem("sftp");
        const self = try gpa.create(Client);
        self.* = .{ .gpa = gpa, .ssh = session, .key = try gpa.dupe(u8, key), .inbuf = .init(gpa), .out = .init(gpa), .why = .init(gpa) };
        errdefer self.destroy();
        var b = ssh.Buf.init(gpa);
        defer b.deinit();
        try b.u32be(5);
        try b.byte(fxp.init);
        try b.u32be(3);
        try session.send(b.list.items);
        const v = try self.reply();
        if (v[0] != fxp.version) return error.SftpProtocol;
        var c = ssh.Cursor{ .s = v[1..] };
        _ = try c.u32be();
        var limits = false;
        while (c.i < c.s.len) {
            const n = try c.str();
            _ = try c.str();
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
        var c = ssh.Cursor{ .s = r[5..] };
        _ = try c.u64be();
        const mr = try c.u64be();
        const mw = try c.u64be();
        if (mr > 0) self.max_read = @intCast(@min(mr, 255 * 1024));
        if (mw > 0) self.max_write = @intCast(@min(mw, 255 * 1024));
    }

    fn destroy(self: *Client) void {
        self.ssh.close();
        self.inbuf.deinit();
        self.out.deinit();
        self.why.deinit();
        self.gpa.free(self.key);
        self.gpa.destroy(self);
    }

    pub fn lastError(self: *const Client) []const u8 {
        return if (self.why.items.len > 0) self.why.items else self.ssh.lastError();
    }

    /// Start a request of `kind`: its length is patched in by `finish`.
    fn begin(self: *Client, kind: u8) !u32 {
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

    /// The next whole SFTP packet (type byte first), reassembled across channel
    /// data; valid until the next call.
    ///
    /// `consumed` marks where unread bytes start. They are moved to the front only
    /// once the read part outweighs them: moving whatever was pending on every
    /// reply copied megabytes per reply with reads pipelined, and a Parquet read
    /// over SFTP ran at a seventh of what the link carries.
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

    /// A STATUS reply as an error (OK is no error).
    fn statusOf(self: *Client, r: []const u8) !void {
        if (r[0] != fxp.status) return error.SftpProtocol;
        var c = ssh.Cursor{ .s = r[5..] };
        const code = try c.u32be();
        const text = c.str() catch "";
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
        try self.out.u32be(0); // no attributes
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] == fxp.handle) {
            var c = ssh.Cursor{ .s = r[5..] };
            return self.gpa.dupe(u8, try c.str());
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

    /// The size of an open file, or of a path.
    fn sizeOf(self: *Client, handle: ?[]const u8, path: []const u8) !u64 {
        const id = try self.begin(if (handle != null) fxp.fstat else fxp.stat);
        try self.out.str(handle orelse path);
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] != fxp.attrs) {
            try self.statusOf(r);
            return error.SftpProtocol;
        }
        var c = ssh.Cursor{ .s = r[5..] };
        const a = try readAttrs(&c);
        return a.size orelse error.SftpUnsupported;
    }

    /// `len` bytes at `off` into `out`, requests pipelined; short at the end of
    /// the file.
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
                var c = ssh.Cursor{ .s = r[5..] };
                const d = try c.str();
                if (d.len > q.len) return error.SftpProtocol;
                const dst: usize = @intCast(q.at - off);
                @memcpy(out[dst..][0..d.len], d);
                got += d.len;
                // a short answer before the end: ask for the rest of it again
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
                        // drain what is still in flight before failing
                        while (n_out > 0) : (n_out -= 1) _ = self.reply() catch break;
                        return e;
                    },
                };
            }
        }
        // got bytes are contiguous from `off` unless the file ended mid-range
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

    fn rename(self: *Client, from: []const u8, to: []const u8) !void {
        if (self.posix_rename) {
            const id = try self.begin(fxp.extended);
            try self.out.str("posix-rename@openssh.com");
            try self.out.str(from);
            try self.out.str(to);
            try self.finish();
            return self.statusOf(try self.replyFor(id));
        }
        // plain RENAME refuses an existing target: remove it first
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

    pub const Entry = struct { name: []const u8, size: u64, regular: bool };

    /// The entries of a directory, names copied into `arena`.
    fn listDir(self: *Client, arena: std.mem.Allocator, path: []const u8) ![]Entry {
        const id = try self.begin(fxp.opendir);
        try self.out.str(path);
        try self.finish();
        const r = try self.replyFor(id);
        if (r[0] != fxp.handle) {
            try self.statusOf(r);
            return error.SftpProtocol;
        }
        var hc = ssh.Cursor{ .s = r[5..] };
        const handle = try arena.dupe(u8, try hc.str());
        var out = std.array_list.Managed(Entry).init(arena);
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
            var c = ssh.Cursor{ .s = rr[5..] };
            const count = try c.u32be();
            for (0..count) |_| {
                const name = try c.str();
                _ = try c.str(); // longname
                const a = try readAttrs(&c);
                if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
                try out.append(.{
                    .name = try arena.dupe(u8, name),
                    .size = a.size orelse 0,
                    .regular = if (a.perms) |p| (p & 0o170000) == 0o100000 else true,
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
    const f = try c.u32be();
    if (f & 0x1 != 0) a.size = try c.u64be();
    if (f & 0x2 != 0) {
        _ = try c.u32be();
        _ = try c.u32be();
    }
    if (f & 0x4 != 0) a.perms = try c.u32be();
    if (f & 0x8 != 0) {
        _ = try c.u32be();
        _ = try c.u32be();
    }
    if (f & 0x80000000 != 0) {
        const n = try c.u32be();
        for (0..n) |_| {
            _ = try c.str();
            _ = try c.str();
        }
    }
    return a;
}

// --- the pool -----------------------------------------------------------------

var pool_mtx: std.Thread.Mutex = .{};
var pool: std.ArrayListUnmanaged(*Client) = .empty;

fn poolKey(buf: []u8, cfg: ssh.Config) []const u8 {
    return std.fmt.bufPrint(buf, "{s}@{s}:{d}", .{ cfg.user, cfg.host, cfg.port }) catch cfg.host;
}

/// A session to the server `cfg` names, the caller's alone until `checkin`.
pub fn checkout(cfg: ssh.Config) !*Client {
    ssh.clearFailure();
    last_error_len = 0;
    var kb: [512]u8 = undefined;
    const key = poolKey(&kb, cfg);
    {
        pool_mtx.lock();
        defer pool_mtx.unlock();
        var i = pool.items.len;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, pool.items[i].key, key)) return pool.swapRemove(i);
        }
    }
    return Client.open(std.heap.page_allocator, cfg, key);
}

/// Hand a session back for the next opener; a broken one is closed instead.
pub fn checkin(c: *Client, healthy: bool) void {
    if (!healthy) {
        c.destroy();
        return;
    }
    c.why.clearRetainingCapacity();
    pool_mtx.lock();
    defer pool_mtx.unlock();
    pool.append(std.heap.page_allocator, c) catch c.destroy();
}

/// Why the last attempt to reach `cfg` failed, for an error message — the
/// session's own words (an unknown host key's fingerprint) when it got that far.
pub threadlocal var last_error_buf: [512]u8 = undefined;
pub threadlocal var last_error_len: usize = 0;

fn noteError(text: []const u8) void {
    const n = @min(text.len, last_error_buf.len);
    @memcpy(last_error_buf[0..n], text[0..n]);
    last_error_len = n;
}

pub fn lastError() []const u8 {
    return last_error_buf[0..last_error_len];
}

// --- files --------------------------------------------------------------------

/// An open remote file, its session checked out for as long as it is open.
pub const File = struct {
    client: *Client,
    handle: []const u8,
    size: u64,
    healthy: bool = true,

    pub fn open(arena: std.mem.Allocator, url: []const u8) !*File {
        const t = try resolve(arena, url);
        const c = checkout(t.cfg) catch |e| {
            noteError(sshWhy(e, t.cfg));
            return e;
        };
        const handle = c.openHandle(t.path, flags.read) catch |e| {
            noteError(c.lastError());
            checkin(c, isProtocolOk(e));
            return e;
        };
        const size = c.sizeOf(handle, t.path) catch |e| {
            noteError(c.lastError());
            c.closeHandle(handle) catch {};
            c.gpa.free(handle);
            checkin(c, isProtocolOk(e));
            return e;
        };
        const self = try arena.create(File);
        self.* = .{ .client = c, .handle = handle, .size = size };
        return self;
    }

    /// `len` bytes at `off`, owned by `arena`.
    pub fn read(self: *File, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        const buf = try arena.alloc(u8, len);
        const n = self.client.readRange(self.handle, off, buf) catch |e| {
            self.healthy = isProtocolOk(e);
            return e;
        };
        if (n != len) return error.EndOfStream;
        return buf;
    }

    pub fn close(self: *File) void {
        if (self.healthy) self.client.closeHandle(self.handle) catch {
            self.healthy = false;
        };
        self.client.gpa.free(self.handle);
        checkin(self.client, self.healthy);
    }
};

/// A file read front to back as a `std.Io.Reader`, the next chunks requested
/// while the current one is consumed.
pub const Stream = struct {
    file: *File,
    at: u64 = 0,
    /// Where the stream stops: the file's end, or a window's.
    end: u64,
    /// Opened by `open`, closed with the stream; a window borrows its file.
    owns_file: bool,
    chunk: []u8,
    interface: std.Io.Reader,

    const chunk_size = 1 << 20;

    pub fn open(arena: std.mem.Allocator, url: []const u8) !*Stream {
        const f = try File.open(arena, url);
        errdefer f.close();
        return make(arena, f, 0, f.size, true);
    }

    /// `len` bytes of an open file from `off` — a zip member inside a remote
    /// archive.
    pub fn window(arena: std.mem.Allocator, f: *File, off: u64, len: u64) !*Stream {
        return make(arena, f, off, off + len, false);
    }

    fn make(arena: std.mem.Allocator, f: *File, start: u64, end: u64, owns: bool) !*Stream {
        const self = try arena.create(Stream);
        self.* = .{
            .file = f,
            .at = start,
            .end = end,
            .owns_file = owns,
            .chunk = try arena.alloc(u8, chunk_size),
            .interface = .{
                .buffer = try arena.alloc(u8, 64 * 1024),
                .vtable = &.{ .stream = streamFn },
                .seek = 0,
                .end = 0,
            },
        };
        return self;
    }

    pub fn close(self: *Stream) void {
        if (self.owns_file) self.file.close();
    }

    fn streamFn(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Stream = @fieldParentPtr("interface", r);
        if (self.at >= self.end) return error.EndOfStream;
        const want: usize = @intCast(@min(@as(u64, self.chunk.len), self.end - self.at));
        const take = limit.minInt(want);
        const n = self.file.client.readRange(self.file.handle, self.at, self.chunk[0..take]) catch {
            self.file.healthy = false;
            return error.ReadFailed;
        };
        if (n == 0) return error.EndOfStream;
        self.at += n;
        w.writeAll(self.chunk[0..n]) catch return error.WriteFailed;
        return n;
    }
};

/// A file written front to back to `path.part`, renamed over `path` by `finish`
/// and removed by `abort`.
pub const Upload = struct {
    client: *Client,
    handle: []const u8,
    path: []const u8,
    part: []const u8,
    at: u64 = 0,
    interface: std.Io.Writer,
    last_status: ?anyerror = null,
    last_error: []const u8 = "",
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, url: []const u8) !*Upload {
        const t = try resolve(arena, url);
        const c = checkout(t.cfg) catch |e| {
            noteError(sshWhy(e, t.cfg));
            return e;
        };
        const part = try std.fmt.allocPrint(arena, "{s}.part", .{t.path});
        const handle = c.openHandle(part, flags.write | flags.creat | flags.trunc) catch |e| {
            noteError(c.lastError());
            checkin(c, isProtocolOk(e));
            return e;
        };
        const self = try arena.create(Upload);
        self.* = .{
            .client = c,
            .handle = handle,
            .path = t.path,
            .part = part,
            .interface = .{ .buffer = try arena.alloc(u8, 1 << 20), .vtable = &.{ .drain = drainFn } },
        };
        return self;
    }

    fn put(self: *Upload, bytes: []const u8) std.Io.Writer.Error!usize {
        if (bytes.len == 0) return 0;
        self.client.writeRange(self.handle, self.at, bytes) catch |e| {
            self.last_status = e;
            self.last_error = self.client.lastError();
            return error.WriteFailed;
        };
        self.at += bytes.len;
        return bytes.len;
    }

    fn drainFn(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Upload = @fieldParentPtr("interface", w);
        var total: usize = 0;
        total += try self.put(w.buffered());
        for (data[0 .. data.len - 1]) |d| total += try self.put(d);
        const last = data[data.len - 1];
        for (0..splat) |_| total += try self.put(last);
        return w.consume(total);
    }

    /// Write what is buffered, close, and rename `path.part` to `path`.
    pub fn finish(self: *Upload) !void {
        defer self.release();
        self.interface.flush() catch |e| return self.last_status orelse e;
        try self.client.closeHandle(self.handle);
        self.client.rename(self.part, self.path) catch |e| {
            self.last_error = self.client.lastError();
            return e;
        };
        self.done = true;
    }

    /// Drop the upload: the `.part` is removed, `path` untouched.
    pub fn abort(self: *Upload) void {
        if (self.done) return;
        self.client.closeHandle(self.handle) catch {};
        self.client.remove(self.part) catch {};
        self.release();
    }

    fn release(self: *Upload) void {
        if (self.done) return;
        self.done = true;
        self.client.gpa.free(self.handle);
        checkin(self.client, self.last_status == null);
    }
};

/// The objects under `url` (ending in `/`) as `sftp://` URLs of regular files,
/// sorted by name — a server lists in whatever order it keeps.
pub fn listPrefix(arena: std.mem.Allocator, url: []const u8) ![]const []const u8 {
    const t = try resolve(arena, url);
    const c = checkout(t.cfg) catch |e| {
        noteError(sshWhy(e, t.cfg));
        return e;
    };
    var healthy = true;
    defer checkin(c, healthy);
    const dir = if (t.path.len > 1 and t.path[t.path.len - 1] == '/') t.path[0 .. t.path.len - 1] else t.path;
    const entries = c.listDir(arena, if (dir.len == 0) "." else dir) catch |e| {
        noteError(c.lastError());
        healthy = isProtocolOk(e);
        return e;
    };
    var names = std.array_list.Managed([]const u8).init(arena);
    for (entries) |en| if (en.regular) try names.append(en.name);
    std.mem.sort([]const u8, names.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    const base = url[0 .. url.len - (if (std.mem.endsWith(u8, url, "/")) @as(usize, 1) else 0)];
    const out = try arena.alloc([]const u8, names.items.len);
    for (names.items, out) |n, *o| o.* = try std.fmt.allocPrint(arena, "{s}/{s}", .{ base, n });
    return out;
}

/// A file or directory error keeps the session usable; a transport error
/// does not.
fn isProtocolOk(e: anyerror) bool {
    return switch (e) {
        error.SftpNoSuchFile, error.SftpPermissionDenied, error.SftpFailure, error.SftpUnsupported, error.EndOfStream => true,
        else => false,
    };
}

threadlocal var why_buf: [600]u8 = undefined;

/// An SSH failure in words. A failed connect leaves no session to ask, so the
/// session's reason is unavailable; the error and the server name it.
fn sshWhy(e: anyerror, cfg: ssh.Config) []const u8 {
    const said = ssh.lastFailure();
    if (said.len > 0) return said;
    return std.fmt.bufPrint(&why_buf, "{s} connecting to {s}:{d} as {s}", .{ @errorName(e), cfg.host, cfg.port, cfg.user }) catch @errorName(e);
}

test "sftp URLs: user, port, home-relative, registered names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = try resolve(a, "sftp://ana@files.example.com:2222/in/x.csv");
    try std.testing.expectEqualStrings("files.example.com", t.cfg.host);
    try std.testing.expectEqual(@as(u16, 2222), t.cfg.port);
    try std.testing.expectEqualStrings("ana", t.cfg.user);
    try std.testing.expectEqualStrings("/in/x.csv", t.path);
    try std.testing.expectEqualStrings("x.csv", (try resolve(a, "sftp://h/~/x.csv")).path);
    try std.testing.expectError(error.NotSftpUrl, resolve(a, "sftp://nohostpath"));
    try register("bank_t", .{ .host = "sftp.bank.example", .port = 2200, .user = "u1", .password = "p" });
    const b = try resolve(a, "sftp://bank_t/ret/a.ret");
    try std.testing.expectEqualStrings("sftp.bank.example", b.cfg.host);
    try std.testing.expectEqual(@as(u16, 2200), b.cfg.port);
    try std.testing.expectEqualStrings("u1", b.cfg.user);
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
        .known_hosts = try std.fmt.allocPrint(a, "{s}/known_hosts", .{dir}),
    });
    // 3 MB: past the read and write chunk sizes, and the stream's chunk
    const body = try a.alloc(u8, 3 * 1024 * 1024 + 123);
    for (body, 0..) |*b, i| b.* = @truncate(i *% 31 +% (i >> 9));

    const up = try Upload.open(a, "sftp://probe/~/t/big.bin");
    try up.interface.writeAll(body);
    try up.finish();

    const names = try listPrefix(a, "sftp://probe/~/t/");
    try std.testing.expect(names.len >= 1);
    var found = false;
    for (names) |n| found = found or std.mem.endsWith(u8, n, "/big.bin");
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
