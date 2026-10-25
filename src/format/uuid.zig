//! A UUID's 16 bytes as its 36-character text, lower case. `canonical` reads the
//! bytes in order (RFC 9562, Delta, a big-endian u128); `mixed` is SQL Server's
//! UNIQUEIDENTIFIER, whose first three groups are stored little-endian.

const std = @import("std");
const hex = @import("hex.zig");

pub const Order = enum { canonical, mixed };

/// The byte each text position's hex pair reads, per order.
fn layout(comptime order: Order) [16]u8 {
    return switch (order) {
        .canonical => .{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
        .mixed => .{ 3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15 },
    };
}

pub fn text(b: *const [16]u8, comptime order: Order) [36]u8 {
    const at = comptime layout(order);
    var out: [36]u8 = undefined;
    var o: usize = 0;
    inline for (at, 0..) |src, i| {
        if (i == 4 or i == 6 or i == 8 or i == 10) {
            out[o] = '-';
            o += 1;
        }
        out[o] = hex.lower_digits[b[src] >> 4];
        out[o + 1] = hex.lower_digits[b[src] & 15];
        o += 2;
    }
    return out;
}

/// Whether `s` is 8-4-4-4-12 hex digits of either case.
pub fn isText(s: []const u8) bool {
    if (s.len != 36) return false;
    for (s, 0..) |c, i| {
        const dash = i == 8 or i == 13 or i == 18 or i == 23;
        if (if (dash) c != '-' else !std.ascii.isHex(c)) return false;
    }
    return true;
}

/// Random bytes with the version 4 and RFC variant bits set.
pub fn v4() [16]u8 {
    var b: [16]u8 = undefined;
    std.crypto.random.bytes(&b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    return b;
}

const testing = std.testing;

const sample = [16]u8{ 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF };

fn printed(a: std.mem.Allocator, b: [16]u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    });
}

fn printedMixed(a: std.mem.Allocator, b: [16]u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}-{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
        b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15],
    });
}

fn delimited(b: [16]u8) [36]u8 {
    const h = std.fmt.bytesToHex(b, .lower);
    var out: [36]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{s}-{s}-{s}-{s}-{s}", .{ h[0..8], h[8..12], h[12..16], h[16..20], h[20..32] }) catch unreachable;
    return out;
}

test "uuid: canonical and mixed text of a known vector" {
    try testing.expectEqualStrings("00112233-4455-6677-8899-aabbccddeeff", &text(&sample, .canonical));
    try testing.expectEqualStrings("33221100-5544-7766-8899-aabbccddeeff", &text(&sample, .mixed));
}

test "uuid: canonical matches aad uuidHex, split uuidAt and delta_dv; mixed matches TDS formatGuid" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x0001d);
    const r = prng.random();
    for (0..500) |_| {
        var b: [16]u8 = undefined;
        r.bytes(&b);
        try testing.expectEqualStrings(try printed(a, b), &text(&b, .canonical));
        try testing.expectEqualStrings(&delimited(b), &text(&b, .canonical));
        try testing.expectEqualStrings(try printedMixed(a, b), &text(&b, .mixed));
        try testing.expect(isText(&text(&b, .canonical)));
        const v = r.int(u128);
        var be: [16]u8 = undefined;
        std.mem.writeInt(u128, &be, v, .big);
        try testing.expectEqualStrings(try printed(a, be), &text(&be, .canonical));
    }
}

test "uuid: split's u128 boundaries read in order" {
    var be: [16]u8 = undefined;
    std.mem.writeInt(u128, &be, @intCast((@as(u256, 1) << 128) / 2), .big);
    try testing.expectEqualStrings("80000000-0000-0000-0000-000000000000", &text(&be, .canonical));
    std.mem.writeInt(u128, &be, std.math.maxInt(u128), .big);
    try testing.expectEqualStrings("ffffffff-ffff-ffff-ffff-ffffffffffff", &text(&be, .canonical));
}

test "uuid: isText pins delta isUuid: either case, dashes at 8, 13, 18, 23 only" {
    try testing.expect(isText("00112233-4455-6677-8899-aabbccddeeff"));
    try testing.expect(isText("00112233-4455-6677-8899-AABBCCDDEEFF"));
    for ([_][]const u8{
        "",
        "00112233-4455-6677-8899-aabbccddeef",
        "00112233-4455-6677-8899-aabbccddeeff0",
        "0011223304455-6677-8899-aabbccddeeff",
        "00112233-4455-6677-8899-aabbccddeefg",
        "00112233-4455-6677-8899+aabbccddeeff",
        "001122334455667788990aabbccddeeff---",
        "{0112233-4455-6677-8899-aabbccddeef}",
    }) |s| {
        try testing.expect(!isText(s));
    }
}

test "uuid: v4 sets the version and variant bits, as delta_log uuid does" {
    var seen = false;
    for (0..64) |_| {
        const b = v4();
        try testing.expectEqual(@as(u8, 0x40), b[6] & 0xf0);
        try testing.expectEqual(@as(u8, 0x80), b[8] & 0xc0);
        const t = text(&b, .canonical);
        try testing.expect(isText(&t));
        try testing.expectEqual(@as(u8, '4'), t[14]);
        try testing.expect(std.mem.indexOfScalar(u8, "89ab", t[19]) != null);
        for (t) |c| try testing.expect(!std.ascii.isUpper(c));
        if (!std.mem.eql(u8, &b, &v4())) seen = true;
    }
    try testing.expect(seen);
}
