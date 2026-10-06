//! Deterministic mutation testing for the wire decoders.
//!
//! The real coverage-guided fuzzer (`zig build test --fuzz`) crashes in Zig
//! 0.15.2's own build runner (`std/Build/Fuzz.zig` addEntryPoint panics), so
//! until a toolchain carries the fix, this is the working half: a fixed-seed
//! mutator that pounds each decoder with corpus mutations and random buffers
//! on every ordinary `zig build test`. Shallow next to real fuzzing, but the
//! decoders' bugs are shallow too — the first one fell to the empty input.
//!
//! Deterministic on purpose: a failure reproduces by running the same test
//! again, no crash file needed. The default budget of 500 iterations keeps all
//! the harnesses together to a few seconds of a Debug test run, so the suite
//! stays fast enough to keep being run; `FUZZ_ITERS=200000` makes a real hunt.

const std = @import("std");

const default_iters = 500;

/// Half the runs mutate a corpus entry (truncated, bit-flipped, or stamped with an
/// extreme length field, where bounds bugs live); the other half are random noise.
pub fn pound(
    comptime testOne: fn (ctx: void, input: []const u8) anyerror!void,
    corpus: []const []const u8,
) !void {
    const iters: usize = blk: {
        const s = std.posix.getenv("FUZZ_ITERS") orelse break :blk default_iters;
        break :blk std.fmt.parseInt(usize, s, 10) catch default_iters;
    };
    var prng = std.Random.DefaultPrng.init(0xba5a17_f011);
    const r = prng.random();
    var buf: [4096]u8 = undefined;

    var i: usize = 0;
    while (i < iters) : (i += 1) {
        var len: usize = 0;
        if (corpus.len > 0 and r.boolean()) {
            const base = corpus[r.uintLessThan(usize, corpus.len)];
            len = @min(buf.len, base.len);
            @memcpy(buf[0..len], base[0..len]);
            if (r.boolean()) len = r.uintAtMost(usize, len);
            var flips = r.uintAtMost(usize, 8);
            while (flips > 0 and len > 0) : (flips -= 1) {
                buf[r.uintLessThan(usize, len)] ^= @as(u8, 1) << r.int(u3);
            }
            if (len >= 4 and r.boolean()) {
                const at = r.uintLessThan(usize, len - 3);
                std.mem.writeInt(u32, buf[at..][0..4], if (r.boolean()) 0xFFFF_FFFF else 0x7FFF_FFFF, .little);
            }
        } else {
            len = r.uintAtMost(usize, 256);
            r.bytes(buf[0..len]);
        }
        try testOne({}, buf[0..len]);
    }
}
