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

    // End-to-end tests see the engine only as the `basalt` module, its public API.
    const basalt_module = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    basalt_module.addOptions("build_options", opts);
    const e2e_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/run_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "basalt", .module = basalt_module }},
        }),
    });
    const run_e2e_tests = b.addRunArtifact(e2e_tests);

    // The book's SQL examples, checked as `basalt check` would; it reads docs/, so
    // an edit there alone must re-run it.
    const docs_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/docs/examples_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "basalt", .module = basalt_module }},
        }),
    });
    const run_docs_tests = b.addRunArtifact(docs_tests);
    run_docs_tests.has_side_effects = true;
    run_docs_tests.setCwd(b.path("."));

    const test_step = b.step("test", "Run unit, end-to-end and documentation tests");
    test_step.dependOn(&run_unit_tests.step);
    test_step.dependOn(&run_e2e_tests.step);
    test_step.dependOn(&run_docs_tests.step);
    const docs_step = b.step("test-docs", "Check the SQL examples in docs/");
    docs_step.dependOn(&run_docs_tests.step);
    const unit_step = b.step("test-unit", "Run the unit tests in src/");
    unit_step.dependOn(&run_unit_tests.step);
    const e2e_step = b.step("test-e2e", "Run the end-to-end tests in tests/e2e/");
    e2e_step.dependOn(&run_e2e_tests.step);

    // Line coverage of src/ by both suites, through kcov. The tests are built with
    // LLVM here: the self-hosted backend's debug info is not one kcov can read.
    const coverage_step = b.step("coverage", "Measure the tests' line coverage of src/ (needs kcov)");
    if (b.findProgram(&.{"kcov"}, &.{})) |kcov| {
        const out_dir = b.getInstallPath(.prefix, "coverage");
        const include = b.fmt("--include-path={s}", .{b.pathFromRoot("src")});
        // kcov makes its output folder but not the parents, and a fresh checkout
        // has no zig-out/ yet.
        const mkdir = b.addSystemCommand(&.{ "mkdir", "-p", b.getInstallPath(.prefix, "") });
        mkdir.has_side_effects = true;
        const merge = b.addSystemCommand(&.{ kcov, "--merge", out_dir });
        const cov_unit = b.addTest(.{ .name = "cov-unit", .root_module = root_module, .use_llvm = true });
        const cov_e2e = b.addTest(.{ .name = "cov-e2e", .use_llvm = true, .root_module = b.createModule(.{
            .root_source_file = b.path("tests/e2e/run_test.zig"),
            .target = target,
            .optimize = .Debug,
            .imports = &.{.{ .name = "basalt", .module = basalt_module }},
        }) });
        for ([_]*std.Build.Step.Compile{ cov_unit, cov_e2e }, [_][]const u8{ "unit", "e2e" }) |t, name| {
            const dir = b.fmt("{s}-{s}", .{ out_dir, name });
            const run = b.addSystemCommand(&.{ kcov, include, dir });
            run.addArtifactArg(t);
            run.has_side_effects = true;
            run.step.dependOn(&mkdir.step);
            merge.addArg(dir);
            merge.step.dependOn(&run.step);
        }
        merge.has_side_effects = true;
        const summary = b.addRunArtifact(b.addExecutable(.{
            .name = "coverage-summary",
            .root_module = b.createModule(.{ .root_source_file = b.path("tools/coverage_summary.zig"), .target = b.graph.host }),
        }));
        summary.addArg(b.fmt("{s}/kcov-merged/coverage.json", .{out_dir}));
        summary.addArg(b.pathFromRoot("src"));
        summary.step.dependOn(&merge.step);
        summary.has_side_effects = true;
        coverage_step.dependOn(&summary.step);
    } else |_| {
        coverage_step.dependOn(&b.addFail("coverage needs kcov on the PATH (dnf install kcov, apt install kcov)").step);
    }

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

    // End-to-end harness: times the installed CLI on committed scripts, so it needs
    // the binary built first. Build with -Doptimize=ReleaseFast or the numbers are
    // meaningless.
    const bench_e2e = b.addExecutable(.{
        .name = "bench-e2e",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bench_e2e.zig"),
            .target = target,
            .optimize = .ReleaseFast,
        }),
    });
    const run_bench_e2e = b.addRunArtifact(bench_e2e);
    run_bench_e2e.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_bench_e2e.addArgs(args);
    const bench_e2e_step = b.step("bench-e2e", "Run end-to-end query + movement benchmarks");
    bench_e2e_step.dependOn(&run_bench_e2e.step);
}
