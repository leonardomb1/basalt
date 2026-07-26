const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const strip = b.option(bool, "strip", "Omit debug info from the binary") orelse false;

    const opts = b.addOptions();
    opts.addOption([]const u8, "version", @import("build.zig.zon").version);

    const root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .strip = strip,
    });
    root_module.addOptions("build_options", opts);

    const exe = b.addExecutable(.{
        .name = "basalt",
        .root_module = root_module,
    });
    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);
    const run_step = b.step("run", "Run the basalt CLI");
    run_step.dependOn(&run_cmd.step);

    const unit_tests = b.addTest(.{
        .root_module = root_module,
    });
    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    const bench = b.addExecutable(.{
        .name = "bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const run_bench = b.addRunArtifact(bench);
    const bench_step = b.step("bench", "Run SIMD microbenchmarks (ReleaseFast)");
    bench_step.dependOn(&run_bench.step);
}
