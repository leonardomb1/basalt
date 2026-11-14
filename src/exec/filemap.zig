//! A local file mapped whole, read-only and private: the CSV, NDJSON and records
//! lanes, the Arrow IPC reader and spill runs read their bytes in place through it.
//! An empty file maps to an empty slice, as `mmap` refuses a zero length, so a
//! caller judges the size on what it gets back and releases it either way.

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
