//! Where operators put what does not fit in memory: a run's scratch space, handed
//! down by the runtime. `create` makes a new empty file in it, open for reading and
//! writing, with its path; `charge` books bytes against the run's disk cap and
//! fails with `error.SpillCapExceeded` past it, and `release` gives back what a
//! deleted file held, so the cap bounds the disk in use, not all ever written.
//! Files live until the run ends.
//! `DirSpace` is a space over an existing directory, for tests and for the runtime
//! to build on.

const std = @import("std");

pub const Space = struct {
    ctx: *anyopaque,
    createFn: *const fn (ctx: *anyopaque, arena: std.mem.Allocator, tag: []const u8) anyerror!File,
    chargeFn: *const fn (ctx: *anyopaque, bytes: u64) anyerror!void,
    releaseFn: ?*const fn (ctx: *anyopaque, bytes: u64) void = null,

    pub const File = struct { file: std.fs.File, path: []const u8 };

    pub fn create(self: Space, arena: std.mem.Allocator, tag: []const u8) !File {
        return self.createFn(self.ctx, arena, tag);
    }

    pub fn charge(self: Space, bytes: u64) !void {
        return self.chargeFn(self.ctx, bytes);
    }

    pub fn release(self: Space, bytes: u64) void {
        if (self.releaseFn) |f| f(self.ctx, bytes);
    }
};

pub const DirSpace = struct {
    dir: []const u8,
    cap: u64,
    used: std.atomic.Value(u64) = .init(0),
    peak: std.atomic.Value(u64) = .init(0),
    seq: std.atomic.Value(u64) = .init(0),

    pub fn space(self: *DirSpace) Space {
        return .{ .ctx = self, .createFn = createImpl, .chargeFn = chargeImpl, .releaseFn = releaseImpl };
    }

    fn createImpl(ctx: *anyopaque, arena: std.mem.Allocator, tag: []const u8) anyerror!Space.File {
        const self: *DirSpace = @ptrCast(@alignCast(ctx));
        const n = self.seq.fetchAdd(1, .monotonic);
        const path = try std.fmt.allocPrint(arena, "{s}/{s}-{d}.spill", .{ self.dir, tag, n });
        const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });
        return .{ .file = file, .path = path };
    }

    fn chargeImpl(ctx: *anyopaque, bytes: u64) anyerror!void {
        const self: *DirSpace = @ptrCast(@alignCast(ctx));
        const now = self.used.fetchAdd(bytes, .monotonic) + bytes;
        if (now > self.cap) {
            _ = self.used.fetchSub(bytes, .monotonic);
            return error.SpillCapExceeded;
        }
        _ = self.peak.fetchMax(now, .monotonic);
    }

    fn releaseImpl(ctx: *anyopaque, bytes: u64) void {
        const self: *DirSpace = @ptrCast(@alignCast(ctx));
        var cur = self.used.load(.monotonic);
        while (self.used.cmpxchgWeak(cur, cur -| bytes, .monotonic, .monotonic)) |seen| cur = seen;
    }
};

test "DirSpace: a refused charge holds nothing, and released bytes make room again" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    var ds = DirSpace{ .dir = try tmp.dir.realpath(".", &buf), .cap = 100 };
    const s = ds.space();
    try s.charge(80);
    try std.testing.expectError(error.SpillCapExceeded, s.charge(30));
    try std.testing.expectEqual(@as(u64, 80), ds.used.load(.monotonic));
    s.release(80);
    try s.charge(90);
    s.release(1000);
    try std.testing.expectEqual(@as(u64, 0), ds.used.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 90), ds.peak.load(.monotonic));
}
