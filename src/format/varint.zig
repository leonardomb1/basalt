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
