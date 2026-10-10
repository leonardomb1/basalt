//! A local file written whole, as `sftp.zig` and `smb.zig` write a remote one: the
//! bytes go to `name.part`, renamed over `name` once the writer closes cleanly and
//! deleted if it fails or the run is interrupted, so a reader never finds a torn
//! file and a failed `LOAD` leaves the previous one in place. A target that exists
//! but is not a regular file (a device, a FIFO, a symlink) is written in place.

const std = @import("std");

pub const Part = struct {
    buf: [std.fs.max_path_bytes]u8 = undefined,
    len: usize = 0,

    const suffix = ".part";

    pub fn create(self: *Part, path: []const u8) !std.fs.File {
        self.len = 0;
        if (regularOrAbsent(path) and path.len + suffix.len <= self.buf.len) {
            @memcpy(self.buf[0..path.len], path);
            @memcpy(self.buf[path.len..][0..suffix.len], suffix);
            self.len = path.len + suffix.len;
        }
        return std.fs.cwd().createFile(if (self.len > 0) self.buf[0..self.len] else path, .{ .truncate = true });
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

fn regularOrAbsent(path: []const u8) bool {
    const st = std.posix.fstatat(std.fs.cwd().fd, path, std.posix.AT.SYMLINK_NOFOLLOW) catch |e| return e == error.FileNotFound;
    return std.posix.S.ISREG(st.mode);
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
