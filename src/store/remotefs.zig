//! What the file-server protocols share. `Registry` holds the `CREATE CONNECTION`
//! names a URL's host part may stand for; `authority` splits `[user@]host[:port]`;
//! `walk` lists a folder, the one place a server-chosen name is checked before it
//! becomes a path. sftp, smb and ftp use those three.
//!
//! `RemoteFs(P)` is the rest for sftp and smb, which read and write in place: a
//! process-wide session pool keyed by the full login (a session idle 15 s or more
//! is probed before reuse, since a server or NAT may have dropped it; a transport
//! error retires it, a file error does not), `File` for ranged reads, `Stream`, an
//! `Upload` that writes `name.part` and only on `finish` puts it over `name`, and
//! `listPrefix`. `P` supplies the protocol: its types (`Link` the session, `Login`,
//! `Target` with a `cfg`, `Handle` with a `size`), the buffer sizes, and the
//! primitives `resolve`, `poolKey`, `clearNotes`, `connect`, `ping`, `destroy`,
//! `protocolOk`, `why`, `openRead`, `openWrite`, `readAt`, `writeAt`,
//! `closeHandle`, `freeHandle`, `commit`, `discard` and `listDir`.

const std = @import("std");
const objstore = @import("objstore.zig");

/// Connection names, process-wide as the pools are. A later registration of a name
/// replaces the earlier one; every string field is copied.
pub fn Registry(comptime Conn: type, comptime fold_case: bool) type {
    return struct {
        var mtx: std.Thread.Mutex = .{};
        var map: std.StringHashMapUnmanaged(Conn) = .empty;

        pub fn put(name: []const u8, c: Conn) !void {
            const gpa = std.heap.page_allocator;
            var owned = c;
            inline for (std.meta.fields(Conn)) |f| {
                if (f.type == []const u8) @field(owned, f.name) = try gpa.dupe(u8, @field(c, f.name));
                if (f.type == ?[]const u8) {
                    if (@field(c, f.name)) |v| @field(owned, f.name) = try gpa.dupe(u8, v);
                }
            }
            const key = if (fold_case) try std.ascii.allocLowerString(gpa, name) else try gpa.dupe(u8, name);
            mtx.lock();
            defer mtx.unlock();
            const gop = try map.getOrPut(gpa, key);
            if (gop.found_existing) gpa.free(key);
            gop.value_ptr.* = owned;
        }

        pub fn get(name: []const u8) ?Conn {
            var buf: [256]u8 = undefined;
            const key = if (fold_case and name.len <= buf.len) std.ascii.lowerString(&buf, name) else name;
            mtx.lock();
            defer mtx.unlock();
            return map.get(key);
        }
    };
}

pub const Authority = struct { userinfo: ?[]const u8 = null, host: []const u8, port: ?u16 = null };

/// `[userinfo@]host[:port]`, split at the last `@` and the last `:`; null for an
/// empty host or a port that is not one.
pub fn authority(s: []const u8) ?Authority {
    var a = Authority{ .host = s };
    if (std.mem.lastIndexOfScalar(u8, s, '@')) |at| {
        a.userinfo = s[0..at];
        a.host = s[at + 1 ..];
    }
    if (std.mem.lastIndexOfScalar(u8, a.host, ':')) |c| {
        a.port = std.fmt.parseInt(u16, a.host[c + 1 ..], 10) catch return null;
        a.host = a.host[0..c];
    }
    if (a.host.len == 0) return null;
    return a;
}

pub const Entry = struct { name: []const u8, dir: bool, file: bool };

pub const Walk = struct {
    max_files: usize = 1_000_000,
    skip_hidden_files: bool = false,
};

/// How deep a folder read descends; a server whose listing loops (a folder that
/// lists itself) is cut off here rather than walked forever.
pub const max_walk_depth = 32;

/// The files under a folder as `/`-separated paths relative to it, from
/// `lister.list(rel)`, each folder's entries in server order. Subfolders starting
/// `_` or `.` are not entered, nor such files listed with `skip_hidden_files`; a
/// name that is not one path part (empty, `.`, `..`, a `/`, `\` or NUL) is dropped,
/// so a server cannot point a path outside the folder.
pub fn walk(arena: std.mem.Allocator, lister: anytype, opts: Walk) ![]const []const u8 {
    const Todo = struct { rel: []const u8, depth: usize };
    var names = std.array_list.Managed([]const u8).init(arena);
    var todo = std.array_list.Managed(Todo).init(arena);
    try todo.append(.{ .rel = "", .depth = 0 });
    while (todo.pop()) |d| {
        if (d.depth > max_walk_depth) return error.FolderTooDeep;
        for (try lister.list(d.rel)) |en| {
            if (en.name.len == 0 or std.mem.indexOfAny(u8, en.name, "/\\\x00") != null) continue;
            if (std.mem.eql(u8, en.name, ".") or std.mem.eql(u8, en.name, "..")) continue;
            const hidden = en.name[0] == '_' or en.name[0] == '.';
            const sub = if (d.rel.len == 0) en.name else try std.fmt.allocPrint(arena, "{s}/{s}", .{ d.rel, en.name });
            if (en.dir) {
                if (!hidden) try todo.append(.{ .rel = sub, .depth = d.depth + 1 });
            } else if (en.file and !(hidden and opts.skip_hidden_files)) {
                if (names.items.len == opts.max_files) return error.FolderTooLarge;
                try names.append(sub);
            }
        }
    }
    return names.items;
}

pub fn RemoteFs(comptime P: type) type {
    return struct {
        const Link = P.Link;
        const Handle = P.Handle;

        const Idle = struct { s: *Link, key: []const u8, since: i64 };
        var pool_mtx: std.Thread.Mutex = .{};
        var pool: std.ArrayListUnmanaged(Idle) = .empty;

        pub const Lease = struct { s: *Link, key: []const u8 };

        /// A pooled session for `cfg`'s login, else a new one.
        pub fn checkout(arena: std.mem.Allocator, cfg: P.Login) !Lease {
            P.clearNotes();
            var kb: [512]u8 = undefined;
            const key = try arena.dupe(u8, P.poolKey(&kb, cfg));
            while (takeIdle(key)) |p| {
                std.heap.page_allocator.free(p.key);
                if (std.time.milliTimestamp() - p.since < 15_000 or P.ping(p.s)) return .{ .s = p.s, .key = key };
                P.destroy(p.s);
            }
            return .{ .s = try P.connect(cfg), .key = key };
        }

        fn takeIdle(key: []const u8) ?Idle {
            pool_mtx.lock();
            defer pool_mtx.unlock();
            var i = pool.items.len;
            while (i > 0) {
                i -= 1;
                if (std.mem.eql(u8, pool.items[i].key, key)) return pool.swapRemove(i);
            }
            return null;
        }

        /// Hands a session back for the next opener; a broken one is closed instead.
        pub fn checkin(l: Lease, healthy: bool) void {
            if (!healthy) return P.destroy(l.s);
            const gpa = std.heap.page_allocator;
            const key = gpa.dupe(u8, l.key) catch return P.destroy(l.s);
            pool_mtx.lock();
            defer pool_mtx.unlock();
            pool.append(gpa, .{ .s = l.s, .key = key, .since = std.time.milliTimestamp() }) catch {
                gpa.free(key);
                P.destroy(l.s);
            };
        }

        /// Opened for reading on a session it has to itself until `close`. Any failed
        /// read retires the session, as replies may still be in flight.
        pub const File = struct {
            lease: Lease,
            handle: Handle,
            size: u64,
            healthy: bool = true,

            pub fn open(arena: std.mem.Allocator, url: []const u8) !*File {
                const t = try P.resolve(arena, url);
                const self = try arena.create(File);
                const l = try checkout(arena, t.cfg);
                const h = P.openRead(l.s, t) catch |e| {
                    checkin(l, P.protocolOk(e));
                    return e;
                };
                self.* = .{ .lease = l, .handle = h, .size = h.size };
                return self;
            }

            pub fn read(self: *File, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
                const buf = try arena.alloc(u8, len);
                if (try self.readChunk(off, buf) != len) return error.EndOfStream;
                return buf;
            }

            pub fn readChunk(self: *File, off: u64, buf: []u8) !usize {
                return P.readAt(self.lease.s, self.handle, off, buf) catch |e| {
                    self.healthy = false;
                    return e;
                };
            }

            pub fn close(self: *File) void {
                if (self.healthy) P.closeHandle(self.lease.s, self.handle) catch {
                    self.healthy = false;
                };
                P.freeHandle(self.lease.s, self.handle);
                checkin(self.lease, self.healthy);
            }
        };

        pub const Stream = objstore.ChunkStream(File, P.stream_chunk);

        /// Writes `path.part`; `finish` puts it over `path`, and `abort` or any failure
        /// removes it, so a reader polling the folder never sees half a file.
        pub const Upload = struct {
            const upload_vtable = std.Io.Writer.VTable{ .drain = objstore.drain(Upload) };

            lease: Lease,
            handle: Handle,
            path: []const u8,
            part: []const u8,
            at: u64 = 0,
            interface: std.Io.Writer,
            last_status: ?anyerror = null,
            last_error: []const u8 = "",
            err_buf: [256]u8 = undefined,
            done: bool = false,

            pub fn open(arena: std.mem.Allocator, url: []const u8) !*Upload {
                const t = try P.resolve(arena, url);
                const part = try std.fmt.allocPrint(arena, "{s}.part", .{t.path});
                const self = try arena.create(Upload);
                const buffer = try arena.alloc(u8, P.upload_buffer);
                const l = try checkout(arena, t.cfg);
                const h = P.openWrite(l.s, t, part) catch |e| {
                    checkin(l, P.protocolOk(e));
                    return e;
                };
                self.* = .{ .lease = l, .handle = h, .path = t.path, .part = part, .interface = .{ .buffer = buffer, .vtable = &upload_vtable } };
                return self;
            }

            pub fn put(self: *Upload, bytes: []const u8) std.Io.Writer.Error!usize {
                if (bytes.len == 0) return 0;
                P.writeAt(self.lease.s, self.handle, self.at, bytes) catch |e| {
                    self.fail(e);
                    return error.WriteFailed;
                };
                self.at += bytes.len;
                return bytes.len;
            }

            pub fn finish(self: *Upload) !void {
                if (self.done) return;
                self.interface.flush() catch |e| {
                    const err = self.last_status orelse e;
                    self.abort();
                    return err;
                };
                P.commit(self.lease.s, &self.handle, self.part, self.path) catch |e| {
                    self.fail(e);
                    self.abort();
                    return e;
                };
                self.release();
            }

            /// The session's reason, copied: its buffer is reused once the session goes back.
            fn fail(self: *Upload, e: anyerror) void {
                self.last_status = e;
                const why = P.why(self.lease.s);
                const n = @min(why.len, self.err_buf.len);
                @memcpy(self.err_buf[0..n], why[0..n]);
                self.last_error = self.err_buf[0..n];
            }

            pub fn abort(self: *Upload) void {
                if (self.done) return;
                P.discard(self.lease.s, &self.handle, self.part);
                self.release();
            }

            fn release(self: *Upload) void {
                self.done = true;
                P.freeHandle(self.lease.s, self.handle);
                checkin(self.lease, if (self.last_status) |e| P.protocolOk(e) else true);
            }
        };

        /// The files under `url` (ending in `/`), as `walk` gives them.
        pub fn listPrefix(arena: std.mem.Allocator, url: []const u8) ![]const []const u8 {
            const t = try P.resolve(arena, url);
            var lister = Lister{ .lease = try checkout(arena, t.cfg), .t = t, .arena = arena };
            defer checkin(lister.lease, lister.healthy);
            return walk(arena, &lister, .{});
        }

        const Lister = struct {
            lease: Lease,
            t: P.Target,
            arena: std.mem.Allocator,
            healthy: bool = true,

            pub fn list(self: *Lister, rel: []const u8) ![]const Entry {
                return P.listDir(self.lease.s, self.arena, self.t, rel) catch |e| {
                    self.healthy = P.protocolOk(e);
                    return e;
                };
            }
        };
    };
}

test "walk drops names that are not one path part, enters no hidden folder, and caps depth and count" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Fake = struct {
        loop: bool = false,
        pub fn list(self: *@This(), rel: []const u8) ![]const Entry {
            if (self.loop) return &.{.{ .name = "again", .dir = true, .file = false }};
            if (rel.len == 0) return &.{
                .{ .name = "a.csv", .dir = false, .file = true },
                .{ .name = "_SUCCESS", .dir = false, .file = true },
                .{ .name = "sub", .dir = true, .file = false },
                .{ .name = "_tmp", .dir = true, .file = false },
                .{ .name = "", .dir = true, .file = false },
                .{ .name = "..", .dir = true, .file = false },
                .{ .name = "x/../../y", .dir = false, .file = true },
                .{ .name = "n\x00ul", .dir = false, .file = true },
                .{ .name = "link", .dir = false, .file = false },
            };
            if (std.mem.eql(u8, rel, "sub")) return &.{.{ .name = "b.csv", .dir = false, .file = true }};
            return error.Unexpected;
        }
    };
    var f = Fake{};
    const all = try walk(a, &f, .{});
    try std.testing.expectEqual(@as(usize, 3), all.len);
    try std.testing.expectEqualStrings("a.csv", all[0]);
    try std.testing.expectEqualStrings("_SUCCESS", all[1]);
    try std.testing.expectEqualStrings("sub/b.csv", all[2]);
    const shown = try walk(a, &f, .{ .skip_hidden_files = true });
    try std.testing.expectEqual(@as(usize, 2), shown.len);
    try std.testing.expectError(error.FolderTooLarge, walk(a, &f, .{ .max_files = 2 }));
    var loop = Fake{ .loop = true };
    try std.testing.expectError(error.FolderTooDeep, walk(a, &loop, .{}));
}

test "an authority splits login, host and port; an empty host or a bad port is none" {
    const x = authority("ana@files.example.com:2222").?;
    try std.testing.expectEqualStrings("ana", x.userinfo.?);
    try std.testing.expectEqualStrings("files.example.com", x.host);
    try std.testing.expectEqual(@as(?u16, 2222), x.port);
    try std.testing.expectEqual(@as(?u16, null), authority("h").?.port);
    try std.testing.expect(authority("u@") == null);
    try std.testing.expect(authority("h:99999") == null);
}
