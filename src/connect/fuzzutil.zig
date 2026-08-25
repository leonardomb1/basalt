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
//! again, no crash file needed.

const std = @import("std");

/// ponytail: fixed iteration budget, sized so all six harnesses together add
/// a few seconds to a Debug `zig build test` — the suite staying fast is what
/// keeps it being run. For a real hunt, set `FUZZ_ITERS` — e.g.
/// `FUZZ_ITERS=200000 zig build test`.
const default_iters = 500;

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
        // Half the runs mutate a corpus entry (structurally close to valid,
        // which is where length-field and bounds bugs live); half are noise.
        var len: usize = 0;
        if (corpus.len > 0 and r.boolean()) {
            const base = corpus[r.uintLessThan(usize, corpus.len)];
            len = @min(buf.len, base.len);
            @memcpy(buf[0..len], base[0..len]);
            // Truncate sometimes: short packets are the classic trap.
            if (r.boolean()) len = r.uintAtMost(usize, len);
            var flips = r.uintAtMost(usize, 8);
            while (flips > 0 and len > 0) : (flips -= 1) {
                buf[r.uintLessThan(usize, len)] ^= @as(u8, 1) << r.int(u3);
            }
            // Occasionally stamp an extreme length-field-shaped value.
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
