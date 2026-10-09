//! The end-to-end harness: fixtures in a tmp dir, a script through `run`, and
//! the output read back for assertions.
//!
//! Helpers substitute paths into the script: `$IN` is the input CSV (or parquet
//! fixture), `$LOOKUP` a join's build side, and `$B` the tmp dir in `checkAndRun`,
//! which also asserts that `check` accepts what `run` executes. The default harness
//! runs with `threads = 1`; the `*Threaded` helpers pick a thread count to reach
//! the parallel paths, whose output keeps file order like a serial run's.
//! `runScriptOpts` takes the run's options whole, for limits such as `op_memory`.
//!
//! The engine is reached only through the `basalt` module (src/root.zig), and the
//! fixture files through `basalt.fixtures`. Tests of a single function live beside
//! it in src/; these run whole scripts.

const std = @import("std");
const basalt = @import("basalt");
const analyze = basalt.analyze;
const parser = basalt.sql_parser;
const Diag = basalt.env.Diag;
const ParamArg = basalt.env.ParamArg;
const run = basalt.runtime.run;

pub fn runToString(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8) ![]u8 {
    return runToStringP(alloc, tmp, input, query, &[_]ParamArg{});
}

pub fn runToStringP(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8, cli_params: []const ParamArg) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);

    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .params = cli_params }, &rdiag) catch |e| {
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

pub fn runCsvThreaded(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, input: []const u8, query: []const u8, threads: usize) ![]u8 {
    try tmp.dir.writeFile(.{ .sub_path = "in.csv", .data = input });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const in_path = try std.fs.path.join(alloc, &.{ base, "in.csv" });
    defer alloc.free(in_path);
    const out_path = try std.fs.path.join(alloc, &.{ base, "out.csv" });
    defer alloc.free(out_path);
    const q = try std.mem.replaceOwned(u8, alloc, query, "$IN", in_path);
    defer alloc.free(q);
    const script = try std.fmt.allocPrint(alloc, "LOAD INTO '{s}' AS {s};", .{ out_path, q });
    defer alloc.free(script);
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads }, &rdiag) catch |e| {
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

pub fn runScript(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, script: []const u8, cli_params: []const ParamArg) ![]u8 {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);

    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .params = cli_params }, &rdiag) catch |e| {
        return e;
    };
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 20);
}

/// `runScript` with the run's options given, e.g. a tiny `op_memory` to force spilling.
pub fn runScriptOpts(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, script: []const u8, opts: basalt.env.RunOptions) ![]u8 {
    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var rdiag: Diag = .{};
    _ = try run(alloc, prog, opts, &rdiag);
    return tmp.dir.readFileAlloc(alloc, "out.csv", 1 << 24);
}

pub fn checkAndRun(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, tmpl: []const u8, threads: usize, cli_params: []const ParamArg) !void {
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "id,grp,amt\n1,a,10\n2,a,20\n3,b,5\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "id,grp,amt\n7,x,1\n8,x,2\n" });
    try tmp.dir.writeFile(.{ .sub_path = "outer.csv", .data = "name\na\nb\n" });
    try tmp.dir.writeFile(.{ .sub_path = "inner.csv", .data = "suffix\none\ntwo\n" });
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc, tmpl, "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSource(parena.allocator(), script, &pdiag) catch |e| {
        std.debug.print("parse error: {s}\n", .{pdiag.msg});
        return e;
    };
    const overrides = try parena.allocator().alloc(analyze.ParamOverride, cli_params.len);
    for (cli_params, overrides) |kv, *o| o.* = .{ .name = kv.key, .value = kv.val };
    var adiag = analyze.Diag{};
    _ = analyze.analyzeWith(parena.allocator(), prog, overrides, &adiag) catch |e| {
        std.debug.print("check error: {s}\n", .{adiag.msg});
        return e;
    };
    var rdiag: Diag = .{};
    _ = run(alloc, prog, .{ .threads = threads, .params = cli_params, .log = .{ .quiet = true } }, &rdiag) catch |e| {
        return e;
    };
}

/// `check` and `run` both refuse the script (`$B` the tmp dir), each naming `want`.
pub fn expectRefusedAlike(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir, tmpl: []const u8, want: []const u8) !void {
    const base = try tmp.dir.realpathAlloc(alloc, ".");
    defer alloc.free(base);
    const script = try std.mem.replaceOwned(u8, alloc, tmpl, "$B", base);
    defer alloc.free(script);

    var parena = std.heap.ArenaAllocator.init(alloc);
    defer parena.deinit();
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(parena.allocator(), script, &pdiag);
    var adiag = analyze.Diag{};
    if (analyze.analyze(parena.allocator(), prog, &adiag)) |_| {
        std.debug.print("check accepted: {s}\n", .{script});
        return error.TestUnexpectedResult;
    } else |_| {}
    var rdiag: Diag = .{};
    if (run(alloc, prog, .{ .log = .{ .quiet = true } }, &rdiag)) |_| {
        std.debug.print("run accepted: {s}\n", .{script});
        return error.TestUnexpectedResult;
    } else |_| {}
    for ([_][]const u8{ adiag.msg, rdiag.msg }) |msg| if (std.mem.indexOf(u8, msg, want) == null) {
        std.debug.print("want `{s}` in `{s}`\n", .{ want, msg });
        return error.TestUnexpectedResult;
    };
}

pub fn expectFile(tmp: *std.testing.TmpDir, name: []const u8, want: []const u8) !void {
    const alloc = std.testing.allocator;
    const got = try tmp.dir.readFileAlloc(alloc, name, 1 << 16);
    defer alloc.free(got);
    try std.testing.expectEqualStrings(want, got);
}
