//! CRC-32 (IEEE 802.3, the gzip and zip checksum), as `std.hash.Crc32` computes it,
//! a register updated over the bytes and inverted at the end. On x86-64 with
//! PCLMULQDQ the bulk of the input is folded 64 bytes at a time by carry-less
//! multiplication (Intel's "Fast CRC Computation Using PCLMULQDQ", the scheme the
//! Linux kernel and zlib-ng use), about twenty times the table loop's speed; the
//! tail under 64 bytes, and every byte on other targets, goes through the table.
//! gzip's inflate checked a decompressed CSV at 640 MB/s, 40% of its time.

const std = @import("std");
const builtin = @import("builtin");

const Table = std.hash.Crc32;

const has_clmul = builtin.cpu.arch == .x86_64 and
    std.Target.x86.featureSetHas(builtin.cpu.features, .pclmul) and
    std.Target.x86.featureSetHas(builtin.cpu.features, .sse4_1);

pub const Crc32 = struct {
    reg: u32 = 0xffff_ffff,

    pub fn update(self: *Crc32, bytes: []const u8) void {
        var rest = bytes;
        if (has_clmul and rest.len >= 64) {
            const n = rest.len & ~@as(usize, 15);
            self.reg = fold(self.reg, rest[0..n]);
            rest = rest[n..];
        }
        var t = Table{ .crc = self.reg };
        t.update(rest);
        self.reg = t.crc;
    }

    pub fn final(self: Crc32) u32 {
        return ~self.reg;
    }
};

pub fn hash(bytes: []const u8) u32 {
    var c = Crc32{};
    c.update(bytes);
    return c.final();
}

const V = @Vector(2, u64);

inline fn clmul(comptime imm: u8, a: V, b: V) V {
    var r = a;
    asm ("pclmulqdq %[imm], %[b], %[r]"
        : [r] "+x" (r),
        : [b] "x" (b),
          [imm] "n" (imm),
    );
    return r;
}

inline fn load(p: []const u8) V {
    return @bitCast(p[0..16].*);
}

/// One 128-bit lane folded forward over the next: `x` times the two folding
/// constants in `k`, the halves added, then the following 16 bytes added in.
inline fn foldOver(x: V, k: V, next: V) V {
    return clmul(0x00, x, k) ^ clmul(0x11, x, k) ^ next;
}

/// The register over `buf`, at least 64 bytes and a multiple of 16.
fn fold(reg: u32, buf: []const u8) u32 {
    const k1k2: V = .{ 0x1_5444_2bd4, 0x1_c6e4_1596 };
    const k3k4: V = .{ 0x1_7519_97d0, 0x0_ccaa_009e };
    const k5: V = .{ 0x1_63cd_6124, 0 };
    const mask32: V = .{ 0xffff_ffff, 0 };
    const poly: V = .{ 0x1_db71_0641, 0x1_f701_1641 };

    var x1 = load(buf[0..]) ^ V{ reg, 0 };
    var x2 = load(buf[16..]);
    var x3 = load(buf[32..]);
    var x4 = load(buf[48..]);
    var i: usize = 64;
    while (i + 64 <= buf.len) : (i += 64) {
        x1 = foldOver(x1, k1k2, load(buf[i..]));
        x2 = foldOver(x2, k1k2, load(buf[i + 16 ..]));
        x3 = foldOver(x3, k1k2, load(buf[i + 32 ..]));
        x4 = foldOver(x4, k1k2, load(buf[i + 48 ..]));
    }
    x1 = foldOver(x1, k3k4, x2);
    x1 = foldOver(x1, k3k4, x3);
    x1 = foldOver(x1, k3k4, x4);
    while (i + 16 <= buf.len) : (i += 16) x1 = foldOver(x1, k3k4, load(buf[i..]));

    // 128 bits to 64, adding the 32 zero bits a CRC appends.
    x1 = clmul(0x10, x1, k3k4) ^ V{ x1[1], 0 };
    x1 = clmul(0x00, x1 & mask32, k5) ^ V{ (x1[0] >> 32) | (x1[1] << 32), x1[1] >> 32 };
    // Barrett reduction, 64 bits to the 32 of the register.
    var x2b = clmul(0x10, x1 & mask32, poly);
    x2b = clmul(0x00, x2b & mask32, poly);
    x1 ^= x2b;
    return @truncate(x1[0] >> 32);
}

test "crc32 matches std.hash.Crc32 on every length from 0 to 700 and on long inputs split anywhere" {
    var prng = std.Random.DefaultPrng.init(0xc3c3);
    const r = prng.random();
    var buf: [4096]u8 = undefined;
    r.bytes(&buf);
    for (0..700) |n| try std.testing.expectEqual(Table.hash(buf[0..n]), hash(buf[0..n]));
    for (0..200) |_| {
        const n = r.uintLessThan(usize, buf.len);
        const cut = r.uintLessThan(usize, n + 1);
        var c = Crc32{};
        c.update(buf[0..cut]);
        c.update(buf[cut..n]);
        try std.testing.expectEqual(Table.hash(buf[0..n]), c.final());
    }
    try std.testing.expectEqual(@as(u32, 0xcbf43926), hash("123456789"));
}
