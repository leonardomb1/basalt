//! A local file written whole, as `sftp.zig` and `smb.zig` write a remote one: the
//! bytes go to `name.<8 hex>.part`, a name of this write's own, renamed over `name`
//! once the writer closes cleanly and deleted if it fails or the run is interrupted,
//! so a reader never finds a torn file, a failed `LOAD` leaves the previous one in
//! place, and two runs writing one file at once each write their own copy. The `.part` takes an
//! existing target's mode and, where permitted, its owner and group. A target that
//! is not a regular file (a device, a FIFO, a symlink), has other hard links, or
//! sits in a directory where the `.part` cannot be created is written in place.

const std = @import("std");

pub const Part = struct {
    buf: [std.fs.max_path_bytes]u8 = undefined,
    len: usize = 0,
    target: usize = 0,

    const suffix = ".part";
    const tag_len = 9;

    pub fn create(self: *Part, path: []const u8) !std.fs.File {
        self.len = 0;
        const cwd = std.fs.cwd();
        const st: ?std.posix.Stat = std.posix.fstatat(cwd.fd, path, std.posix.AT.SYMLINK_NOFOLLOW) catch |e| switch (e) {
            error.FileNotFound => null,
            else => return inPlace(path),
        };
        if (st) |s| if (!std.posix.S.ISREG(s.mode) or s.nlink > 1) return inPlace(path);
        const n = path.len + tag_len + suffix.len;
        if (n > self.buf.len) return inPlace(path);
        @memcpy(self.buf[0..path.len], path);
        @memcpy(self.buf[path.len + tag_len ..][0..suffix.len], suffix);
        const file = while (true) {
            _ = std.fmt.bufPrint(self.buf[path.len..][0..tag_len], ".{x:0>8}", .{std.crypto.random.int(u32)}) catch unreachable;
            break cwd.createFile(self.buf[0..n], .{
                .exclusive = true,
                .mode = if (st) |s| s.mode & 0o777 else std.fs.File.default_mode,
            }) catch |e| switch (e) {
                error.PathAlreadyExists => continue,
                error.AccessDenied, error.PermissionDenied => return inPlace(path),
                else => return e,
            };
        };
        self.len = n;
        self.target = path.len;
        if (st) |s| {
            file.chown(s.uid, s.gid) catch file.chown(null, s.gid) catch {};
            file.chmod(s.mode & 0o7777) catch |e| {
                file.close();
                self.discard();
                return e;
            };
        }
        return file;
    }

    /// Call once the file is synced and closed; the directory is synced after the
    /// rename, so the rename itself survives a crash.
    pub fn commit(self: *Part) !void {
        if (self.len == 0) return;
        defer self.len = 0;
        const target = self.buf[0..self.target];
        try std.fs.cwd().rename(self.buf[0..self.len], target);
        var dir = try std.fs.cwd().openDir(std.fs.path.dirname(target) orelse ".", .{ .iterate = true });
        defer dir.close();
        try std.posix.fsync(dir.fd);
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

/// Whether `dir` holds a `.part` file.
fn anyPart(dir: std.fs.Dir) !bool {
    var d = try dir.openDir(".", .{ .iterate = true });
    defer d.close();
    var it = d.iterate();
    while (try it.next()) |e| if (std.mem.endsWith(u8, e.name, Part.suffix)) return true;
    return false;
}

test "Part: the target appears only on commit; a discard leaves the old file and no .part; two writes at once each write their own" {
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
    try std.testing.expect(!try anyPart(tmp.dir));
    var rb: [16]u8 = undefined;
    try std.testing.expectEqualStrings("old\n", try tmp.dir.readFile("out.csv", &rb));

    f = try p.create(path);
    try f.writeAll("new\n");
    try std.testing.expectEqualStrings("old\n", try tmp.dir.readFile("out.csv", &rb));
    f.close();
    try p.commit();
    try std.testing.expectEqualStrings("new\n", try tmp.dir.readFile("out.csv", &rb));
    try std.testing.expect(!try anyPart(tmp.dir));

    var one = Part{};
    var two = Part{};
    const f1 = try one.create(path);
    const f2 = try two.create(path);
    try f1.writeAll("first\n");
    try f2.writeAll("second, longer\n");
    f1.close();
    f2.close();
    try one.commit();
    try std.testing.expectEqualStrings("first\n", try tmp.dir.readFile("out.csv", &rb));
    try two.commit();
    try std.testing.expectEqualStrings("second, longer\n", try tmp.dir.readFile("out.csv", &rb));
    try std.testing.expect(!try anyPart(tmp.dir));

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
    try std.testing.expect(!try anyPart(tmp.dir));
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
