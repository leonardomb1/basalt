//! The bounds-checked cursor a parser walks a received body or file region with,
//! moved from `db/wire.zig` `Reader`: every read is checked against what is left
//! and fails with the caller's own `short` error rather than reading past it.

const std = @import("std");

pub fn Reader(comptime endian: std.builtin.Endian, comptime short: anytype) type {
    return struct {
        const Self = @This();
        pub const Error = @TypeOf(short);

        p: []const u8,
        i: usize = 0,

        pub fn left(self: *const Self) usize {
            return self.p.len - self.i;
        }

        pub fn bytes(self: *Self, n: usize) Error![]const u8 {
            if (n > self.left()) return short;
            defer self.i += n;
            return self.p[self.i..][0..n];
        }

        pub fn skip(self: *Self, n: usize) Error!void {
            _ = try self.bytes(n);
        }

        pub fn int(self: *Self, comptime T: type) Error!T {
            return std.mem.readInt(T, (try self.bytes(@sizeOf(T)))[0..@sizeOf(T)], endian);
        }

        /// Bytes preceded by their count as a `L` (SSH's `string` is `u32`).
        pub fn prefixed(self: *Self, comptime L: type) Error![]const u8 {
            const start = self.i;
            errdefer self.i = start;
            const n = try self.int(L);
            return self.bytes(n);
        }

        /// A NUL-terminated string, the NUL consumed.
        pub fn cstr(self: *Self) Error![]const u8 {
            const end = std.mem.indexOfScalarPos(u8, self.p, self.i, 0) orelse return short;
            defer self.i = end + 1;
            return self.p[self.i..end];
        }

        pub fn rest(self: *Self) []const u8 {
            defer self.i = self.p.len;
            return self.p[self.i..];
        }
    };
}

const testing = std.testing;

test "cursor: a reader never reads past its body" {
    var r: Reader(.big, error.Short) = .{ .p = "\x00\x02ab\x00c" };
    try testing.expectEqual(@as(u16, 2), try r.int(u16));
    try testing.expectEqualStrings("ab", try r.cstr());
    try testing.expectError(error.Short, r.int(u16));
    try testing.expectError(error.Short, r.cstr());
    try testing.expectError(error.Short, r.skip(std.math.maxInt(usize)));
    try testing.expectEqualStrings("c", r.rest());
    try testing.expectEqual(@as(usize, 0), r.left());
}

test "cursor: both byte orders, a failed read leaves the position alone" {
    var b: Reader(.big, error.Short) = .{ .p = "\x01\x02\x03\x04\x05" };
    var l: Reader(.little, error.Short) = .{ .p = "\x01\x02\x03\x04\x05" };
    try testing.expectEqual(@as(u32, 0x01020304), try b.int(u32));
    try testing.expectEqual(@as(u32, 0x04030201), try l.int(u32));
    try testing.expectError(error.Short, b.int(u16));
    try testing.expectEqual(@as(usize, 4), b.i);
    try testing.expectEqual(@as(u8, 5), try b.int(u8));
    try testing.expectEqualStrings("", try l.bytes(0));
    try testing.expectEqualStrings("\x05", l.rest());
    try testing.expectEqualStrings("", l.rest());
    try testing.expectError(error.Short, l.skip(1));
    var e: Reader(.little, error.Short) = .{ .p = "" };
    try testing.expectError(error.Short, e.cstr());
    try testing.expectEqualStrings("", try e.bytes(0));
    try testing.expectEqual(@as(u0, 0), try e.int(u0));
}

test "cursor: prefixed reads SSH's u32-length string, and a short one leaves the position alone" {
    var c: Reader(.big, error.Short) = .{ .p = "\x00\x00\x00\x02ab\x00\x00\x00\x05xy" };
    try testing.expectEqualStrings("ab", try c.prefixed(u32));
    try testing.expectError(error.Short, c.prefixed(u32));
    try testing.expectEqual(@as(usize, 6), c.i);
    var e: Reader(.big, error.Short) = .{ .p = "\x00\x00" };
    try testing.expectError(error.Short, e.prefixed(u32));
    try testing.expectEqual(@as(usize, 0), e.i);
    var z: Reader(.big, error.Short) = .{ .p = "\x00\x00\x00\x00" };
    try testing.expectEqualStrings("", try z.prefixed(u32));
    try testing.expectEqual(@as(usize, 4), z.i);
}

test "cursor: the same type for the same arguments, so wire can re-export it" {
    try testing.expect(Reader(.little, error.TdsProtocol) == Reader(.little, error.TdsProtocol));
    try testing.expect(Reader(.little, error.TdsProtocol) != Reader(.big, error.TdsProtocol));
}

test "cursor: random reads stay in bounds and fail only with `short`" {
    var prng = std.Random.DefaultPrng.init(0xc0de);
    const r = prng.random();
    var buf: [64]u8 = undefined;
    for (0..5000) |_| {
        const n = r.uintAtMost(usize, buf.len);
        r.bytes(buf[0..n]);
        var c: Reader(.little, error.Short) = .{ .p = buf[0..n] };
        for (0..16) |_| {
            const before = c.i;
            const left = c.left();
            const want = r.uintAtMost(usize, 20);
            switch (r.uintLessThan(u8, 5)) {
                0 => if (c.bytes(want)) |s| {
                    try testing.expectEqualSlices(u8, buf[before..][0..want], s);
                    try testing.expectEqual(before + want, c.i);
                } else |_| try testing.expect(want > left and c.i == before),
                1 => if (c.int(u64)) |v| {
                    try testing.expectEqual(std.mem.readInt(u64, buf[before..][0..8], .little), v);
                } else |_| try testing.expect(left < 8 and c.i == before),
                2 => if (c.cstr()) |s| {
                    try testing.expectEqualSlices(u8, buf[before..][0..s.len], s);
                    try testing.expectEqual(@as(u8, 0), buf[before + s.len]);
                    try testing.expect(std.mem.indexOfScalar(u8, s, 0) == null);
                    try testing.expectEqual(before + s.len + 1, c.i);
                } else |_| try testing.expect(std.mem.indexOfScalar(u8, buf[before..n], 0) == null and c.i == before),
                3 => if (c.skip(want)) |_| {
                    try testing.expectEqual(before + want, c.i);
                } else |_| try testing.expect(want > left and c.i == before),
                else => try testing.expectEqualSlices(u8, buf[before..n], c.rest()),
            }
            try testing.expect(c.i <= c.p.len);
            try testing.expectEqual(c.p.len - c.i, c.left());
        }
    }
}
