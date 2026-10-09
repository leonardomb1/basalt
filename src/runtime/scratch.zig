//! A run's scratch space: the `Space` its blocking operators spill into. Each run
//! owns one, so a run ending never removes another's files (the serve flusher and
//! the notebook kernel can run while another run is going). Most runs never
//! spill, so the directory, `<spill_dir>/basalt-spill-<random hex>` (`spill_dir`
//! from `--spill-dir`, else `$TMPDIR`, else `/tmp`), is made on the first
//! `create`; `deinit` removes it with everything in it. Files and the disk cap
//! are a `DirSpace` over that directory; the lock covers only making it, as
//! `create` and `charge` may come from parallel lanes.

const DirSpace = @import("../exec/space.zig").DirSpace;
const Space = @import("../exec/space.zig").Space;
const std = @import("std");

pub const default_cap: u64 = 8 << 30;

pub const Scratch = struct {
    gpa: std.mem.Allocator,
    base: ?[]const u8,
    inner: DirSpace,
    mutex: std.Thread.Mutex = .{},
    made: bool = false,

    pub fn init(gpa: std.mem.Allocator, spill_dir: ?[]const u8, cap: u64) Scratch {
        return .{ .gpa = gpa, .base = spill_dir, .inner = .{ .dir = "", .cap = cap } };
    }

    pub fn space(self: *Scratch) Space {
        return .{ .ctx = self, .createFn = createImpl, .chargeFn = chargeImpl, .releaseFn = releaseImpl };
    }

    /// The directory, once a file has been made in it.
    pub fn dir(self: *Scratch) ?[]const u8 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return if (self.made) self.inner.dir else null;
    }

    pub fn deinit(self: *Scratch) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (!self.made) return;
        std.fs.cwd().deleteTree(self.inner.dir) catch {};
        self.gpa.free(self.inner.dir);
        self.inner.dir = "";
        self.made = false;
    }

    fn ensureDir(self: *Scratch) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.made) return;
        const env_tmp: ?[]u8 = if (self.base == null) std.process.getEnvVarOwned(self.gpa, "TMPDIR") catch null else null;
        defer if (env_tmp) |t| self.gpa.free(t);
        const base = self.base orelse if (env_tmp != null and env_tmp.?.len > 0) env_tmp.? else "/tmp";
        try std.fs.cwd().makePath(base);
        const path = try std.fmt.allocPrint(self.gpa, "{s}/basalt-spill-{x}", .{ base, std.crypto.random.int(u64) });
        errdefer self.gpa.free(path);
        try std.fs.cwd().makeDir(path);
        self.inner.dir = path;
        self.made = true;
    }

    fn createImpl(ctx: *anyopaque, arena: std.mem.Allocator, tag: []const u8) anyerror!Space.File {
        const self: *Scratch = @ptrCast(@alignCast(ctx));
        try self.ensureDir();
        return self.inner.space().create(arena, tag);
    }

    fn chargeImpl(ctx: *anyopaque, bytes: u64) anyerror!void {
        const self: *Scratch = @ptrCast(@alignCast(ctx));
        return self.inner.space().charge(bytes);
    }

    fn releaseImpl(ctx: *anyopaque, bytes: u64) void {
        const self: *Scratch = @ptrCast(@alignCast(ctx));
        self.inner.space().release(bytes);
    }
};

fn testBase(tmp: *std.testing.TmpDir, buf: []u8) ![]const u8 {
    return tmp.dir.realpath(".", buf);
}

test "Scratch makes no directory until the first create, then puts each file in it" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try testBase(&tmp, &buf);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();

    var s = Scratch.init(std.testing.allocator, base, default_cap);
    defer s.deinit();
    try s.space().charge(10);
    try std.testing.expect(s.dir() == null);
    var it = tmp.dir.iterate();
    try std.testing.expect((try it.next()) == null);

    const a = try s.space().create(ar.allocator(), "sort");
    defer a.file.close();
    const b = try s.space().create(ar.allocator(), "sort");
    defer b.file.close();
    const d = s.dir().?;
    try std.testing.expect(std.mem.startsWith(u8, d, base));
    try std.testing.expect(std.mem.startsWith(u8, std.fs.path.basename(d), "basalt-spill-"));
    try std.testing.expect(!std.mem.eql(u8, a.path, b.path));
    try std.testing.expectEqualStrings(d, std.fs.path.dirname(a.path).?);
    try std.testing.expectEqualStrings(d, std.fs.path.dirname(b.path).?);

    try a.file.writeAll("abc");
    try a.file.seekTo(0);
    var got: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try a.file.readAll(&got));
    try std.testing.expectEqualStrings("abc", &got);
}

test "Scratch fails past its cap with SpillCapExceeded" {
    var s = Scratch.init(std.testing.allocator, null, 100);
    defer s.deinit();
    try s.space().charge(60);
    try s.space().charge(40);
    try std.testing.expectError(error.SpillCapExceeded, s.space().charge(1));
}

test "Scratch deinit removes the directory and every file in it" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try testBase(&tmp, &buf);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();

    var s = Scratch.init(std.testing.allocator, base, default_cap);
    const f = try s.space().create(ar.allocator(), "join");
    try f.file.writeAll("spilled");
    f.file.close();
    const d = try ar.allocator().dupe(u8, s.dir().?);
    s.deinit();
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().access(d, .{}));
    try std.testing.expect(s.dir() == null);
}

test "two Scratch instances keep apart: own directories, own caps, one's cleanup leaves the other" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try testBase(&tmp, &buf);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();

    var one = Scratch.init(std.testing.allocator, base, 10);
    var two = Scratch.init(std.testing.allocator, base, 10);
    defer two.deinit();
    const f1 = try one.space().create(ar.allocator(), "agg");
    f1.file.close();
    const f2 = try two.space().create(ar.allocator(), "agg");
    f2.file.close();
    try std.testing.expect(!std.mem.eql(u8, one.dir().?, two.dir().?));

    try one.space().charge(10);
    try two.space().charge(10);
    try std.testing.expectError(error.SpillCapExceeded, one.space().charge(1));

    one.deinit();
    try std.fs.cwd().access(f2.path, .{});
    const f3 = try two.space().create(ar.allocator(), "agg");
    f3.file.close();
    try std.fs.cwd().access(f3.path, .{});
}

test "Scratch makes a missing spill directory and creates from parallel threads" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const root = try testBase(&tmp, &buf);
    const base = try std.fmt.allocPrint(std.testing.allocator, "{s}/nested/spill", .{root});
    defer std.testing.allocator.free(base);

    var s = Scratch.init(std.testing.allocator, base, default_cap);
    defer s.deinit();
    const Worker = struct {
        fn go(sc: *Scratch, ok: *bool) void {
            var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer ar.deinit();
            for (0..8) |_| {
                const f = sc.space().create(ar.allocator(), "lane") catch return;
                f.file.close();
                sc.space().charge(1) catch return;
            }
            ok.* = true;
        }
    };
    var oks = [_]bool{false} ** 4;
    var threads: [4]std.Thread = undefined;
    for (&threads, &oks) |*t, *ok| t.* = try std.Thread.spawn(.{}, Worker.go, .{ &s, ok });
    for (threads) |t| t.join();
    for (oks) |ok| try std.testing.expect(ok);

    var d = try std.fs.cwd().openDir(s.dir().?, .{ .iterate = true });
    defer d.close();
    var n: usize = 0;
    var it = d.iterate();
    while (try it.next()) |_| n += 1;
    try std.testing.expectEqual(@as(usize, 32), n);
}
