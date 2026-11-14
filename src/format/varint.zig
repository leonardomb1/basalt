//! LEB128 varints and zigzag, as Parquet's Thrift compact protocol, its
//! DELTA_BINARY_PACKED headers and Snappy's length preamble use them.

const std = @import("std");

/// The varint at `pos.*`, advancing past it. Running off the end of `src` or past
/// ten bytes is corrupt.
pub fn read(src: []const u8, pos: *usize) error{CorruptVarint}!u64 {
    var v: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        if (pos.* >= src.len) return error.CorruptVarint;
        const b = src[pos.*];
        pos.* += 1;
        v |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) return v;
        shift = std.math.add(u6, shift, 7) catch return error.CorruptVarint;
    }
}

/// The varint at `in`'s position; null when `in` ends before its first byte. Ending
/// inside one is EndOfStream, one past ten bytes CorruptVarint.
pub fn take(in: *std.Io.Reader) (std.Io.Reader.Error || error{CorruptVarint})!?u64 {
    var v: u64 = 0;
    var shift: u7 = 0;
    while (true) {
        const b = in.takeByte() catch |e| {
            if (shift == 0 and e == error.EndOfStream) return null;
            return e;
        };
        v |= @as(u64, b & 0x7F) << @intCast(shift);
        if (b & 0x80 == 0) return v;
        shift += 7;
        if (shift > 63) return error.CorruptVarint;
    }
}

/// The varint opening `b`, unrolled for a buffer with ten bytes to spare, and how
/// many bytes it took; null for one past ten bytes.
pub inline fn readTen(b: *const [10]u8) ?struct { v: u64, len: usize } {
    var v: u64 = 0;
    inline for (0..10) |i| {
        v |= @as(u64, b[i] & 0x7F) << @as(u6, 7 * i);
        if (b[i] < 0x80) return .{ .v = v, .len = i + 1 };
    }
    return null;
}

pub fn write(out: *std.array_list.Managed(u8), v_in: u64) !void {
    var v = v_in;
    while (true) {
        const b: u8 = @intCast(v & 0x7F);
        v >>= 7;
        try out.append(if (v != 0) b | 0x80 else b);
        if (v == 0) return;
    }
}

pub fn unzigzag(u: u64) i64 {
    return @as(i64, @bitCast(u >> 1)) ^ -@as(i64, @intCast(u & 1));
}

pub fn zigzag(v: i64) u64 {
    return @bitCast((v >> 63) ^ (v << 1));
}

test "varint and zigzag round-trip, and an overlong varint is corrupt" {
    var out = std.array_list.Managed(u8).init(std.testing.allocator);
    defer out.deinit();
    const vals = [_]i64{ 0, -1, 1, 63, -64, 1 << 40, std.math.minInt(i64), std.math.maxInt(i64) };
    for (vals) |v| try write(&out, zigzag(v));
    var pos: usize = 0;
    for (vals) |v| try std.testing.expectEqual(v, unzigzag(try read(out.items, &pos)));
    try std.testing.expectEqual(out.items.len, pos);
    pos = 0;
    try std.testing.expectError(error.CorruptVarint, read(&([_]u8{0x80} ** 10 ++ [_]u8{0x01}), &pos));
    pos = 0;
    try std.testing.expectError(error.CorruptVarint, read(&[_]u8{0x80}, &pos));
}

test "take reads a varint off a stream, null at a clean end, and readTen agrees with read" {
    var r = std.Io.Reader.fixed(&.{ 0x96, 0x01, 0x05 });
    try std.testing.expectEqual(@as(?u64, 150), try take(&r));
    try std.testing.expectEqual(@as(?u64, 5), try take(&r));
    try std.testing.expectEqual(@as(?u64, null), try take(&r));
    var cut = std.Io.Reader.fixed(&.{0x96});
    try std.testing.expectError(error.EndOfStream, take(&cut));
    var long = std.Io.Reader.fixed(&(.{0xFF} ** 11));
    try std.testing.expectError(error.CorruptVarint, take(&long));
    const ten = [10]u8{ 0x96, 0x01, 0, 0, 0, 0, 0, 0, 0, 0 };
    const t = readTen(&ten).?;
    try std.testing.expectEqual(@as(u64, 150), t.v);
    try std.testing.expectEqual(@as(usize, 2), t.len);
    try std.testing.expect(readTen(&(.{0xFF} ** 10)) == null);
}
