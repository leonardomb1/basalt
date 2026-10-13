//! A local file written whole, as `sftp.zig` and `smb.zig` write a remote one: the
//! bytes go to `name.part`, renamed over `name` once the writer closes cleanly and
//! deleted if it fails or the run is interrupted, so a reader never finds a torn
//! file and a failed `LOAD` leaves the previous one in place. The `.part` takes an
//! existing target's mode and, where permitted, its owner and group. A target that
//! is not a regular file (a device, a FIFO, a symlink), has other hard links, or
//! sits in a directory where the `.part` cannot be created is written in place.

const std = @import("std");

pub const Part = struct {
    buf: [std.fs.max_path_bytes]u8 = undefined,
    len: usize = 0,

    const suffix = ".part";

    pub fn create(self: *Part, path: []const u8) !std.fs.File {
        self.len = 0;
        const cwd = std.fs.cwd();
        const st: ?std.posix.Stat = std.posix.fstatat(cwd.fd, path, std.posix.AT.SYMLINK_NOFOLLOW) catch |e| switch (e) {
            error.FileNotFound => null,
            else => return inPlace(path),
        };
        if (st) |s| if (!std.posix.S.ISREG(s.mode) or s.nlink > 1) return inPlace(path);
        if (path.len + suffix.len > self.buf.len) return inPlace(path);
        @memcpy(self.buf[0..path.len], path);
        @memcpy(self.buf[path.len..][0..suffix.len], suffix);
        const file = cwd.createFile(self.buf[0 .. path.len + suffix.len], .{
            .truncate = true,
            .mode = if (st) |s| s.mode & 0o777 else std.fs.File.default_mode,
        }) catch |e| switch (e) {
            error.AccessDenied, error.PermissionDenied => return inPlace(path),
            else => return e,
        };
        self.len = path.len + suffix.len;
        if (st) |s| {
            file.chown(s.uid, s.gid) catch {};
            file.chmod(s.mode & 0o7777) catch |e| {
                file.close();
                self.discard();
                return e;
            };
        }
        return file;
    }

    /// Call once the file is flushed and closed.
    pub fn commit(self: *Part) !void {
        if (self.len == 0) return;
        defer self.len = 0;
        try std.fs.cwd().rename(self.buf[0..self.len], self.buf[0 .. self.len - suffix.len]);
    }

    pub fn discard(self: *Part) void {
        if (self.len == 0) return;
        std.fs.cwd().deleteFile(self.buf[0..self.len]) catch {};
        self.len = 0;
    }
};

fn inPlace(path: []const u8) !std.fs.File {
    return std.fs.cwd().createFile(path, .{ .truncate = true });
}

test "Part: the target appears only on commit; a discard leaves the old file and no .part" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const path = try std.fs.path.join(std.testing.allocator, &.{ base, "out.csv" });
    defer std.testing.allocator.free(path);
    try tmp.dir.writeFile(.{ .sub_path = "out.csv", .data = "old\n" });

    var p = Part{};
    var f = try p.create(path);
    try f.writeAll("torn");
    f.close();
    p.discard();
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out.csv.part", .{}));
    var rb: [16]u8 = undefined;
    try std.testing.expectEqualStrings("old\n", try tmp.dir.readFile("out.csv", &rb));

    f = try p.create(path);
    try f.writeAll("new\n");
    try std.testing.expectEqualStrings("old\n", try tmp.dir.readFile("out.csv", &rb));
    f.close();
    try p.commit();
    try std.testing.expectEqualStrings("new\n", try tmp.dir.readFile("out.csv", &rb));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("out.csv.part", .{}));

    var dev = Part{};
    const null_dev = try dev.create("/dev/null");
    null_dev.close();
    try std.testing.expectEqual(@as(usize, 0), dev.len);
}

test "Part: the replacement keeps the target's mode; a hard-linked target is written in place" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    const path = try std.fs.path.join(std.testing.allocator, &.{ base, "private.csv" });
    defer std.testing.allocator.free(path);
    try tmp.dir.writeFile(.{ .sub_path = "private.csv", .data = "old\n" });
    {
        const f = try tmp.dir.openFile("private.csv", .{});
        defer f.close();
        try f.chmod(0o600);
    }

    var p = Part{};
    var f = try p.create(path);
    try std.testing.expect(p.len > 0);
    try f.writeAll("new\n");
    f.close();
    try p.commit();
    try std.testing.expectEqual(@as(std.posix.mode_t, 0o600), (try tmp.dir.statFile("private.csv")).mode & 0o7777);

    try std.posix.linkat(tmp.dir.fd, "private.csv", tmp.dir.fd, "alias.csv", 0);
    f = try p.create(path);
    try std.testing.expectEqual(@as(usize, 0), p.len);
    try f.writeAll("newer\n");
    f.close();
    try p.commit();
    var rb: [16]u8 = undefined;
    try std.testing.expectEqualStrings("newer\n", try tmp.dir.readFile("alias.csv", &rb));
    try std.testing.expectError(error.FileNotFound, tmp.dir.access("private.csv.part", .{}));
}

test "Part: a writable target in a read-only directory is written in place" {
    if (std.posix.geteuid() == 0) return error.SkipZigTest;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makeDir("ro");
    try tmp.dir.writeFile(.{ .sub_path = "ro/out.csv", .data = "old\n" });
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, "ro");
    defer std.testing.allocator.free(base);
    const path = try std.fs.path.join(std.testing.allocator, &.{ base, "out.csv" });
    defer std.testing.allocator.free(path);
    try std.posix.fchmodat(tmp.dir.fd, "ro", 0o555, 0);
    defer std.posix.fchmodat(tmp.dir.fd, "ro", 0o755, 0) catch {};

    var p = Part{};
    const f = try p.create(path);
    try std.testing.expectEqual(@as(usize, 0), p.len);
    try f.writeAll("new\n");
    f.close();
    try p.commit();
    var rb: [16]u8 = undefined;
    try std.testing.expectEqualStrings("new\n", try tmp.dir.readFile("ro/out.csv", &rb));
}
