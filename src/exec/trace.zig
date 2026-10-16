//! Chrome trace events for `basalt run --trace FILE`: a span for each piece of work
//! the engine splits across threads (a lane's fold, a row group, a merge partition,
//! an emit), on the thread that ran it, written when the run ends as the JSON that
//! Perfetto (ui.perfetto.dev) and chrome://tracing open. Off, a span costs a load
//! and a branch; on, its end takes a lock, so spans mark units of work, not rows.

const std = @import("std");

const Event = struct { name: []const u8, tid: u64, ts: u64, dur: u64, arg: u64 };

var on = std.atomic.Value(bool).init(false);
var mtx: std.Thread.Mutex = .{};
var events: std.ArrayListUnmanaged(Event) = .empty;
var t0: std.time.Instant = undefined;

pub fn enable() void {
    t0 = std.time.Instant.now() catch return;
    on.store(true, .release);
}

pub fn enabled() bool {
    return on.load(.monotonic);
}

pub const Span = struct {
    name: []const u8,
    arg: u64,
    start: ?std.time.Instant,

    pub fn end(self: Span) void {
        const s = self.start orelse return;
        const now = std.time.Instant.now() catch return;
        mtx.lock();
        defer mtx.unlock();
        events.append(std.heap.page_allocator, .{
            .name = self.name,
            .tid = std.Thread.getCurrentId(),
            .ts = s.since(t0),
            .dur = now.since(s),
            .arg = self.arg,
        }) catch {};
    }
};

/// A span named `name`, a string that outlives the run, with `arg` (a lane, item or
/// partition number) shown beside it; `end` records it.
pub fn begin(name: []const u8, arg: u64) Span {
    if (!on.load(.monotonic)) return .{ .name = name, .arg = arg, .start = null };
    return .{ .name = name, .arg = arg, .start = std.time.Instant.now() catch null };
}

/// The last part of a type's name, for a span named after the context it works on.
pub fn short(comptime T: type) []const u8 {
    const full = @typeName(T);
    return comptime full[if (std.mem.lastIndexOfScalar(u8, full, '.')) |i| i + 1 else 0..];
}

/// Writes every span recorded so far to `path` and forgets them.
pub fn write(path: []const u8) !void {
    mtx.lock();
    defer mtx.unlock();
    defer events.clearAndFree(std.heap.page_allocator);
    var file = try std.fs.cwd().createFile(path, .{});
    defer file.close();
    var buf: [1 << 16]u8 = undefined;
    var fw = file.writer(&buf);
    const w = &fw.interface;
    try w.writeAll("{\"traceEvents\":[\n");
    for (events.items, 0..) |e, i| {
        try w.print("{s}{{\"name\":", .{if (i == 0) "" else ",\n"});
        try std.json.Stringify.encodeJsonString(e.name, .{}, w);
        try w.print(",\"ph\":\"X\",\"pid\":1,\"tid\":{d},\"ts\":{d}.{d:0>3},\"dur\":{d}.{d:0>3},\"args\":{{\"n\":{d}}}}}", .{
            e.tid, e.ts / 1000, e.ts % 1000, e.dur / 1000, e.dur % 1000, e.arg,
        });
    }
    try w.writeAll("\n]}\n");
    try w.flush();
}

test "spans are recorded only once enabled and written as trace events" {
    begin("before", 0).end();
    enable();
    defer on.store(false, .release);
    const s = begin("lane", 3);
    s.end();
    try std.testing.expectEqual(@as(usize, 1), events.items.len);
    try std.testing.expectEqualStrings("lane", events.items[0].name);
    try std.testing.expectEqualStrings("Span", short(Span));
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(dir);
    const path = try std.fs.path.join(std.testing.allocator, &.{ dir, "t.json" });
    defer std.testing.allocator.free(path);
    try write(path);
    const got = try tmp.dir.readFileAlloc(std.testing.allocator, "t.json", 1 << 16);
    defer std.testing.allocator.free(got);
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, got, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.object.get("traceEvents").?.array.items.len);
}
