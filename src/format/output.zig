//! Opens any path a writer targets as one stream of bytes, the write side of
//! `stream.zig`: a local file written whole through a `.part` (`store/part.zig`) or
//! appended to in place, an object, SFTP or SMB upload committed on `finish`, a
//! file someone else owns (stdout), or memory. A `.gz` name gzips what is written.
//! A format writer renders into `writer()` and never learns where its bytes go.
//!
//! On failure a local file is left as it was: an append is cut back to where the
//! file ended, a whole write's `.part` deleted, and an append whose file another run
//! replaced meanwhile fails rather than report rows that went to the old one; an upload is never committed, so its
//! staged blocks or multipart parts stay invisible. A local file this opened is
//! synced to disk before `finish` closes it, and before its `.part` is renamed over
//! the target, so a crash after the rename cannot leave the target torn.

const std = @import("std");
const pagecache = @import("../exec/pagecache.zig");
const deflate = @import("compress/deflate.zig");
const driver = @import("../exec/driver.zig");
const objstore = @import("../store/objstore.zig");
const stream = @import("stream.zig");
const Part = @import("../store/part.zig").Part;

/// The object version the next upload of `path` must replace (`expectVersion`).
threadlocal var expected: ?struct { path: []const u8, etag: []const u8 } = null;

/// Makes the next `Output.open` of `path` on this thread an upload sent with
/// `If-Match: etag`, for a writer opened by a format that knows nothing of it.
pub fn expectVersion(path: []const u8, etag: []const u8) void {
    expected = .{ .path = path, .etag = etag };
}

/// Drops a version `expectVersion` named that no open took.
pub fn forgetVersion() void {
    expected = null;
}

pub const Output = struct {
    backend: Backend,
    buf: [stream.LINE_BUF]u8 = undefined,
    fw: std.fs.File.Writer = undefined,
    owns_file: bool = true,
    gz: ?*deflate.Deflate = null,
    part: Part = .{},
    end: u64 = 0,
    appending: bool = false,
    path: []const u8 = "",

    const Backend = union(enum) {
        file: std.fs.File,
        object: objstore.Writer,
        memory: *std.Io.Writer.Allocating,
    };

    /// `.append` opens a local file at its end (`end` is where it was), and is
    /// refused for a remote one, which is written whole. An upload of the path
    /// `expectVersion` named last lands only over that version.
    pub fn open(arena: std.mem.Allocator, path: []const u8, content_type: []const u8, mode: driver.FileMode) !*Output {
        const self = try arena.create(Output);
        if (mode == .append and objstore.isRemote(path)) return error.AppendNotSupported;
        if (try objstore.openRemote(arena, path, content_type)) |o| {
            self.* = .{ .backend = .{ .object = o } };
            if (expected) |x| if (std.mem.eql(u8, x.path, path)) {
                _ = o.ifMatch(x.etag);
                expected = null;
            };
        } else {
            self.* = .{ .backend = undefined };
            self.backend = .{ .file = if (mode == .append) try std.fs.cwd().createFile(path, .{ .truncate = false, .read = true }) else try self.part.create(path) };
            self.fw = self.backend.file.writer(&self.buf);
            if (mode == .append) {
                errdefer self.backend.file.close();
                self.end = try self.backend.file.getEndPos();
                self.appending = true;
                self.path = path;
                try self.fw.seekTo(self.end);
            }
        }
        return self;
    }

    /// Writes to a file someone else owns: left open on `finish`, and written from
    /// where it stands, so a second writer carries on after the first.
    pub fn openBorrowed(arena: std.mem.Allocator, f: std.fs.File) !*Output {
        const self = try arena.create(Output);
        self.* = .{ .backend = .{ .file = f }, .owns_file = false };
        self.fw = self.backend.file.writerStreaming(&self.buf);
        return self;
    }

    /// Writes into memory, read back with `written`; nothing to commit or discard.
    pub fn openMemory(arena: std.mem.Allocator) !*Output {
        const m = try arena.create(std.Io.Writer.Allocating);
        m.* = std.Io.Writer.Allocating.init(arena);
        const self = try arena.create(Output);
        self.* = .{ .backend = .{ .memory = m }, .owns_file = false };
        return self;
    }

    /// What a memory output holds.
    pub fn written(self: *Output) []u8 {
        return self.backend.memory.written();
    }

    /// Gzips everything written from here on, for a `.gz` name; zstd is refused.
    pub fn compress(self: *Output, path: []const u8) !void {
        switch (stream.splitCodec(path).codec) {
            .none => {},
            .gzip => self.gz = try deflate.Deflate.init(pagecache.process, self.raw(), .{}),
            .zstd => return error.ZstdWriteUnsupported,
        }
    }

    /// An upload made to land only over the object version `etag` names
    /// (`objstore.Writer.ifMatch`); a local file or memory is untouched.
    pub fn ifMatch(self: *Output, etag: []const u8) void {
        switch (self.backend) {
            .object => |o| _ = o.ifMatch(etag),
            .file, .memory => {},
        }
    }

    pub fn writer(self: *Output) *std.Io.Writer {
        if (self.gz) |g| return &g.interface;
        return self.raw();
    }

    pub fn raw(self: *Output) *std.Io.Writer {
        return switch (self.backend) {
            .file => &self.fw.interface,
            .object => |o| o.io,
            .memory => |m| &m.writer,
        };
    }

    /// The local file being written, null for an upload or memory.
    pub fn localFile(self: *Output) ?std.fs.File {
        return switch (self.backend) {
            .file => |f| f,
            .object, .memory => null,
        };
    }

    /// The last byte of the local file an append extends, decoded when its name says
    /// `.gz`, null when it holds none: a `.gz` streams through once, in constant memory.
    pub fn lastByte(self: *Output, arena: std.mem.Allocator, path: []const u8) !?u8 {
        const f = self.localFile() orelse return null;
        if (self.end == 0) return null;
        if (stream.splitCodec(path).codec != .gzip) {
            var one: [1]u8 = undefined;
            _ = try f.preadAll(&one, self.end - 1);
            return one[0];
        }
        var fr = f.reader(try arena.alloc(u8, stream.LINE_BUF));
        var gz: @import("compress/inflate.zig").Gunzip = undefined;
        gz.init(&fr.interface, try arena.alloc(u8, stream.codecBuffer(.gzip)), try arena.alloc(u8, stream.LINE_BUF));
        var last: ?u8 = null;
        while (true) {
            const got = gz.interface.peekGreedy(1) catch |e| switch (e) {
                error.EndOfStream => return last,
                else => |x| return x,
            };
            last = got[got.len - 1];
            gz.interface.toss(got.len);
        }
    }

    /// Recovers the error an upload actually hit (see `objstore.Writer.specific`);
    /// a file's error is already the real one.
    pub fn specific(self: *Output, e: anyerror) anyerror {
        return switch (self.backend) {
            .file, .memory => e,
            .object => |o| o.specific(e),
        };
    }

    pub fn finish(self: *Output) !void {
        if (self.gz) |g| {
            defer {
                g.deinit(pagecache.process);
                self.gz = null;
            }
            g.finish() catch |e| {
                if (self.backend == .file) self.dropFile();
                return self.specific(e);
            };
        }
        switch (self.backend) {
            .file => |f| {
                self.fw.interface.flush() catch |e| {
                    self.dropFile();
                    return e;
                };
                if (self.owns_file) {
                    f.sync() catch |e| {
                        self.dropFile();
                        return e;
                    };
                    if (self.appending and !stillAt(f, self.path)) {
                        f.close();
                        return stream.explainf(error.FileReplaced, "`{s}` was written again by another run while rows were added to it, so they went to the file it replaced; run it again", .{self.path});
                    }
                    f.close();
                }
                try self.part.commit();
            },
            .object => |o| o.finish() catch |e| return self.specific(e),
            .memory => {},
        }
    }

    pub fn abort(self: *Output) void {
        if (self.gz) |g| {
            g.deinit(pagecache.process);
            self.gz = null;
        }
        switch (self.backend) {
            .file => self.dropFile(),
            .object => |o| o.abort(),
            .memory => {},
        }
    }

    /// Whether `path` still names the file `f` is open on: a rename over it in the
    /// meantime (another run writing it again whole) leaves `f` on a file no name
    /// reaches.
    fn stillAt(f: std.fs.File, path: []const u8) bool {
        const open_st = std.posix.fstat(f.handle) catch return false;
        const now = std.posix.fstatat(std.fs.cwd().fd, path, 0) catch return false;
        return open_st.ino == now.ino and open_st.dev == now.dev;
    }

    fn dropFile(self: *Output) void {
        if (self.owns_file) {
            if (self.appending) self.backend.file.setEndPos(self.end) catch {};
            self.backend.file.close();
        }
        self.part.discard();
    }
};

test "an append that fails is cut back to where the file ended; one that finishes keeps its rows; one whose file another run replaced fails" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id\n1\n" });
    const path = try tmp.dir.realpathAlloc(a, "t.csv");
    const failed = try Output.open(a, path, "text/csv", .append);
    try failed.writer().writeAll("2\n3");
    try failed.writer().flush();
    failed.abort();
    try std.testing.expectEqualStrings("id\n1\n", try tmp.dir.readFileAlloc(a, "t.csv", 1 << 10));
    const kept = try Output.open(a, path, "text/csv", .append);
    try kept.writer().writeAll("2\n");
    try kept.finish();
    try std.testing.expectEqualStrings("id\n1\n2\n", try tmp.dir.readFileAlloc(a, "t.csv", 1 << 10));

    const lost = try Output.open(a, path, "text/csv", .append);
    try lost.writer().writeAll("3\n");
    try tmp.dir.writeFile(.{ .sub_path = "new.csv", .data = "id\n9\n" });
    try tmp.dir.rename("new.csv", "t.csv");
    try std.testing.expectError(error.FileReplaced, lost.finish());
    try std.testing.expectEqualStrings("id\n9\n", try tmp.dir.readFileAlloc(a, "t.csv", 1 << 10));
}
