//! Opens any path a writer targets as one stream of bytes, the write side of
//! `stream.zig`: a local file written whole through a `.part` (`store/part.zig`) or
//! appended to in place, an object, SFTP or SMB upload committed on `finish`, or a
//! file someone else owns (stdout). A `.gz` name gzips what is written. A format
//! writer renders into `writer()` and never learns where its bytes go.
//!
//! On failure a local file keeps what was already flushed when appending, and is
//! otherwise left as it was; an upload is never committed, so its staged blocks or
//! multipart parts stay invisible.

const std = @import("std");
const deflate = @import("compress/deflate.zig");
const driver = @import("../connect/driver.zig");
const objstore = @import("../store/objstore.zig");
const stream = @import("stream.zig");
const Part = @import("../store/part.zig").Part;

pub const Output = struct {
    backend: Backend,
    buf: [stream.LINE_BUF]u8 = undefined,
    fw: std.fs.File.Writer = undefined,
    owns_file: bool = true,
    gz: ?*deflate.Deflate = null,
    part: Part = .{},
    end: u64 = 0,

    const Backend = union(enum) {
        file: std.fs.File,
        object: objstore.Writer,
    };

    /// `.append` opens a local file at its end (`end` is where it was), and is
    /// refused for a remote one, which is written whole.
    pub fn open(arena: std.mem.Allocator, path: []const u8, content_type: []const u8, mode: driver.FileMode) !*Output {
        const self = try arena.create(Output);
        if (mode == .append and objstore.isRemote(path)) return error.AppendNotSupported;
        if (try objstore.openRemote(arena, path, content_type)) |o| {
            self.* = .{ .backend = .{ .object = o } };
        } else {
            self.* = .{ .backend = undefined };
            self.backend = .{ .file = if (mode == .append) try std.fs.cwd().createFile(path, .{ .truncate = false, .read = true }) else try self.part.create(path) };
            self.fw = self.backend.file.writer(&self.buf);
            if (mode == .append) {
                errdefer self.backend.file.close();
                self.end = try self.backend.file.getEndPos();
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

    /// Gzips everything written from here on, for a `.gz` name; zstd is refused.
    pub fn compress(self: *Output, path: []const u8) !void {
        switch (stream.splitCodec(path).codec) {
            .none => {},
            .gzip => self.gz = try deflate.Deflate.init(std.heap.page_allocator, self.raw(), .{}),
            .zstd => return error.ZstdWriteUnsupported,
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
        };
    }

    /// The local file being written, null for an upload.
    pub fn localFile(self: *Output) ?std.fs.File {
        return switch (self.backend) {
            .file => |f| f,
            .object => null,
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
        var gz: stream.Gunzip = undefined;
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
            .file => e,
            .object => |o| o.specific(e),
        };
    }

    pub fn finish(self: *Output) !void {
        if (self.gz) |g| {
            defer {
                g.deinit(std.heap.page_allocator);
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
                if (self.owns_file) f.close();
                try self.part.commit();
            },
            .object => |o| o.finish() catch |e| return self.specific(e),
        }
    }

    pub fn abort(self: *Output) void {
        if (self.gz) |g| {
            g.deinit(std.heap.page_allocator);
            self.gz = null;
        }
        switch (self.backend) {
            .file => self.dropFile(),
            .object => |o| o.abort(),
        }
    }

    fn dropFile(self: *Output) void {
        if (self.owns_file) self.backend.file.close();
        self.part.discard();
    }
};
