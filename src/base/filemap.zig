//! A local file mapped whole, read-only and private: the CSV, NDJSON and records
//! lanes, the Arrow IPC reader and spill runs read their bytes in place through it.
//! An empty file maps to an empty slice, as `mmap` refuses a zero length, so a
//! caller judges the size on what it gets back and releases it either way.
//! `dropPages` lets a reader that walks a mapping once give back what it passed.

const std = @import("std");

pub const Map = []align(std.heap.page_size_min) const u8;

pub fn whole(file: std.fs.File) !Map {
    const size = (try file.stat()).size;
    if (size == 0) return &.{};
    return std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
}

pub fn release(m: Map) void {
    if (m.len > 0) std.posix.munmap(m);
}

/// Drops the pages wholly inside `bytes`, which must lie in a file mapping, from the
/// process, so what was read once stops counting as resident; touched again, they
/// fault back in from the file (anonymous memory would come back zeroed).
pub fn dropPages(bytes: []const u8) void {
    const page = std.heap.page_size_min;
    const lo = std.mem.alignForward(usize, @intFromPtr(bytes.ptr), page);
    const hi = std.mem.alignBackward(usize, @intFromPtr(bytes.ptr) + bytes.len, page);
    if (hi <= lo) return;
    const at: [*]align(page) u8 = @ptrFromInt(lo);
    std.posix.madvise(at, hi - lo, std.posix.MADV.DONTNEED) catch {};
}

test "an empty file maps to an empty slice and a full one to its bytes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    {
        const f = try tmp.dir.createFile("empty", .{ .read = true });
        defer f.close();
        const m = try whole(f);
        defer release(m);
        try std.testing.expectEqual(@as(usize, 0), m.len);
    }
    const f = try tmp.dir.createFile("full", .{ .read = true });
    defer f.close();
    try f.writeAll("a,b\n1,2\n");
    const m = try whole(f);
    defer release(m);
    try std.testing.expectEqualStrings("a,b\n1,2\n", m);
}
