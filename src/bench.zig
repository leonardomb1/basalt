//! SIMD microbenchmarks (`zig build bench`, forced ReleaseFast).
//!
//! These GATE the explicit kernels in `exec/simd.zig`: a kernel is only worth its
//! complexity if it beats an equivalent scalar loop on the same build. The bench
//! that drove the design conclusion is `f64 sum`: LLVM cannot auto-vectorize a
//! non-associative float reduction, so the explicit `@reduce` kernel wins ~4x.
//! The `i64 sum` line is the counter-example we keep as a reminder: integer sums
//! ARE associative, LLVM vectorizes them, and an explicit kernel would only lose.
//!
//! Working set is one ~4096-row batch kept resident in cache, repeated — matching
//! the engine's real (compute-bound) regime, not a multi-MB bandwidth sweep.

const std = @import("std");
const simd = @import("exec/simd.zig");

const N = 4096;
const REPS = 200_000;

fn scalarSumF(a: []const f64) f64 {
    var acc: f64 = 0;
    for (a) |x| acc += x;
    return acc;
}

fn scalarSumI(a: []const i64) i64 {
    var acc: i64 = 0;
    for (a) |x| acc +%= x;
    return acc;
}

fn simdSumI(a: []const i64) i64 {
    const L = simd.lanes(i64);
    const V = @Vector(L, i64);
    var acc: V = @splat(0);
    var i: usize = 0;
    while (i + L <= a.len) : (i += L) acc +%= @as(V, a[i..][0..L].*);
    var total: i64 = @reduce(.Add, acc);
    while (i < a.len) : (i += 1) total +%= a[i];
    return total;
}

fn report(name: []const u8, scalar_ns: u64, simd_ns: u64, w: *std.Io.Writer) !void {
    const sca: f64 = @floatFromInt(scalar_ns);
    const sim: f64 = @floatFromInt(simd_ns);
    const denom: f64 = @floatFromInt(N * REPS);
    try w.print(
        "{s:<18} scalar {d:>6.3} ns/elem   simd {d:>6.3} ns/elem   speedup {d:.2}x\n",
        .{ name, sca / denom, sim / denom, sca / sim },
    );
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const a = gpa.allocator();

    const xi = try a.alloc(i64, N);
    defer a.free(xi);
    const xf = try a.alloc(f64, N);
    defer a.free(xf);
    for (0..N) |i| {
        xi[i] = @intCast((i *% 2654435761) & 0xffff);
        xf[i] = @floatFromInt(xi[i]);
    }

    var timer = try std.time.Timer.start();
    var sink: u64 = 0;

    var out_buf: [4096]u8 = undefined;
    var out_file = std.fs.File.stdout().writer(&out_buf);
    const w = &out_file.interface;
    try w.print("SIMD microbench  (N={d} elems x {d} reps; native lanes i64={d}, f64={d})\n", .{
        N, REPS, simd.lanes(i64), simd.lanes(f64),
    });

    // The win: f64 reduction (LLVM cannot auto-vectorize; explicit @reduce can).
    {
        timer.reset();
        var acc: f64 = 0;
        for (0..REPS) |_| acc = scalarSumF(xf);
        const s = timer.read();
        sink +%= @bitCast(acc);
        timer.reset();
        var vacc: f64 = 0;
        for (0..REPS) |_| vacc = simd.sumF(xf);
        const v = timer.read();
        sink +%= @bitCast(vacc);
        try report("f64 sum", s, v, w);
    }
    // The counter-example: i64 reduction — LLVM already wins, kept as a reminder.
    {
        timer.reset();
        var acc: i64 = 0;
        for (0..REPS) |_| acc = scalarSumI(xi);
        const s = timer.read();
        sink +%= @bitCast(acc);
        timer.reset();
        var vacc: i64 = 0;
        for (0..REPS) |_| vacc = simdSumI(xi);
        const v = timer.read();
        sink +%= @bitCast(vacc);
        try report("i64 sum", s, v, w);
    }

    std.mem.doNotOptimizeAway(sink);
    try w.flush();
}
