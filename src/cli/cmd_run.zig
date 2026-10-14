//! `basalt run`: run a script once.

const ErrOut = @import("args.zig").ErrOut;
const Flags = @import("args.zig").Flags;
const analyze = @import("../runtime/analyze.zig");
const ast = @import("../lang/ast.zig");
const loadSource = @import("args.zig").loadSource;
const loadExit = @import("args.zig").loadExit;
const nextVal = @import("args.zig").nextVal;
const paramProblem = @import("args.zig").paramProblem;
const parseSrcTo = @import("args.zig").parseSrcTo;
const repeatedKey = @import("args.zig").repeatedKey;
const runtime = @import("../runtime/run.zig");
const sizeFlag = @import("args.zig").sizeFlag;
const std = @import("std");
const sharedFlag = @import("args.zig").sharedFlag;
const unknownOption = @import("args.zig").unknownOption;
const wantsJsonLog = @import("args.zig").wantsJsonLog;

pub fn cmdRun(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var stderr_buf: [4096]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writerStreaming(&stderr_buf);
    const stderr = &stderr_file.interface;
    defer stderr.flush() catch {};

    for (args[2..]) |a| {
        const name = a[0 .. std.mem.indexOfScalar(u8, a, '=') orelse a.len];
        if (std.mem.eql(u8, name, "--port") or std.mem.eql(u8, name, "--host")) {
            try stderr.print("error: `run {s}`: " ++ ast.http_mode_discontinued ++ "; drop it to run the script once\n", .{name});
            return 2;
        }
    }

    const src = loadSource(arena.allocator(), "run", args, stderr) catch |e| switch (e) {
        error.Usage, error.Unreadable => |le| return loadExit(le),
        else => return e,
    };
    var eo = ErrOut{ .w = stderr, .json = wantsJsonLog(args), .label = src.label };
    const prog = (try parseSrcTo(arena.allocator(), src, eo)) orelse return 1;
    eo.files = prog.files;

    var params = std.array_list.Managed(runtime.ParamArg).init(alloc);
    defer params.deinit();
    var fl = Flags{};
    var explain = false;
    var no_progress = false;
    var spill_dir: ?[]const u8 = null;
    var spill_cap: u64 = (runtime.RunOptions{}).spill_cap;
    var op_memory: usize = (runtime.RunOptions{}).op_memory;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-c") or std.mem.eql(u8, a, "--command")) {
            i += 1;
            continue;
        }
        if (std.mem.eql(u8, a, "--spill-dir")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            if (v.len == 0) {
                try stderr.print("error: --spill-dir needs a directory\n", .{});
                return 2;
            }
            spill_dir = v;
        } else if (std.mem.eql(u8, a, "--spill-cap")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            spill_cap = (try sizeFlag(v, a, stderr)) orelse return 2;
        } else if (std.mem.eql(u8, a, "--op-memory")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            const n = (try sizeFlag(v, a, stderr)) orelse return 2;
            op_memory = std.math.cast(usize, n) orelse {
                try stderr.print("error: --op-memory `{s}` is more than this machine can address\n", .{v});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--explain")) {
            explain = true;
        } else if (std.mem.eql(u8, a, "--no-progress")) {
            no_progress = true;
        } else if (std.mem.eql(u8, a, "-p") or std.mem.eql(u8, a, "--param")) {
            const kv = (try nextVal(args, &i, a, stderr)) orelse return 2;
            const eqp = std.mem.indexOfScalar(u8, kv, '=') orelse {
                try stderr.print("error: param must be key=value, got `{s}`\n", .{kv});
                return 2;
            };
            try params.append(.{ .key = kv[0..eqp], .val = kv[eqp + 1 ..] });
        } else if ((try sharedFlag(args, &i, &fl, .{ .output = true }, stderr)) orelse return 2) {} else if (try unknownOption(a, "run", stderr)) return 2;
    }
    var log = fl.log;
    const stdout_format = fl.format orelse .table;
    const keys = try arena.allocator().alloc([]const u8, params.items.len);
    for (params.items, keys) |p, *k| k.* = p.key;
    if (repeatedKey(keys)) |k| {
        try stderr.print("error: `-p {s}` given more than once\n", .{k});
        return 2;
    }
    for (keys) |k| if (try paramProblem(arena.allocator(), prog, k)) |why| {
        try eo.report(.{ .msg = why });
        return 1;
    };

    if (prog.explain == .plan) {
        var ebuf: [4096]u8 = undefined;
        var efile = std.fs.File.stdout().writerStreaming(&ebuf);
        const eout = &efile.interface;
        defer eout.flush() catch {};
        var adiag2 = analyze.Diag{};
        const plan = analyze.analyze(arena.allocator(), prog, &adiag2) catch |e| switch (e) {
            error.OutOfMemory => return e,
            error.AnalyzeFailed => {
                try eo.report(.{ .msg = adiag2.msg, .pos = adiag2.pos, .end = adiag2.end });
                return 1;
            },
        };
        if (stdout_format == .arrow) {
            var aw = std.Io.Writer.Allocating.init(arena.allocator());
            try analyze.render(plan, &aw.writer);
            _ = try runtime.printPlanArrow(alloc, aw.written(), .{ .kind = "explain", .line = 1, .col = 1, .t0_ms = std.time.milliTimestamp() });
            return 0;
        }
        try analyze.render(plan, eout);
        return 0;
    }

    log.summary = if (stdout_format == .json) .json_stdout else .stderr;

    var diag: runtime.Diag = .{};
    var sink = runtime.OutcomeSink.init(alloc);
    defer sink.deinit();
    const progress = !no_progress and !log.quiet and (log.format == .json or std.posix.isatty(std.fs.File.stderr().handle));
    _ = runtime.run(alloc, prog, .{ .params = params.items, .threads = fl.threads, .outcomes = &sink, .log = log, .explain = explain or prog.explain == .analyze, .stdout_format = stdout_format, .progress = progress, .items = true, .max_rows = fl.max_rows, .spill_dir = spill_dir, .spill_cap = spill_cap, .op_memory = op_memory }, &diag) catch |e| switch (e) {
        error.Aborted => {
            if (eo.json)
                try eo.report(.{ .msg = "aborted", .event = "aborted" })
            else
                try stderr.print("{s}: aborted\n", .{src.label});
            return 130;
        },
        error.PlanFailed => {
            try eo.report(.{ .msg = diag.msg, .pos = diag.pos, .end = diag.end, .transient = diag.retryable });
            return if (diag.retryable) 75 else 1;
        },
        error.OutOfMemory => return e,
        error.WriteFailed => if (stdoutClosed()) return 0 else {
            try stderr.print("{s}: runtime error: {s}\n", .{ src.label, runtime.failLabel(e) });
            return 1;
        },
        else => {
            const transient = diag.retryable or runtime.isTransient(e);
            if (diag.msg.len > 0)
                try eo.report(.{ .msg = diag.msg, .pos = diag.pos, .end = diag.end, .transient = transient })
            else if (eo.json)
                try eo.report(.{ .msg = runtime.failLabel(e), .transient = transient })
            else
                try stderr.print("{s}: runtime error{s}: {s}\n", .{ src.label, if (transient) " (transient)" else "", runtime.failLabel(e) });
            return if (transient) 75 else 1;
        },
    };
    return reportFailedItems(eo, &sink);
}

/// Whether stdout's reader has gone (`basalt run ... | head`): a write that failed
/// for that is the reader's choice, ended quietly as `rg` and `grep` do, not an error.
fn stdoutClosed() bool {
    var fds = [_]std.posix.pollfd{.{ .fd = std.posix.STDOUT_FILENO, .events = std.posix.POLL.OUT, .revents = 0 }};
    _ = std.posix.poll(&fds, 0) catch return false;
    return fds[0].revents & std.posix.POLL.ERR != 0;
}

/// The exit code for a run whose `FOR EACH` items failed under `ON ERROR CONTINUE`,
/// after naming each failure not already shown and the count, in the log's format.
fn reportFailedItems(eo: ErrOut, sink: *runtime.OutcomeSink) !u8 {
    const nfail = sink.failures();
    if (nfail == 0) return 0;
    var all_retryable = true;
    var buf: [1024]u8 = undefined;
    for (sink.list.items) |o| {
        if (o.ok) continue;
        if (!o.retryable) all_retryable = false;
        if (o.shown) continue;
        const msg = std.fmt.bufPrint(&buf, "item `{s}` failed: {s}", .{ o.item, o.err }) catch o.err;
        try eo.report(.{ .msg = msg, .transient = o.retryable });
    }
    const msg = try std.fmt.bufPrint(&buf, "{d}/{d} item(s) failed", .{ nfail, sink.list.items.len });
    try eo.report(.{ .msg = msg, .transient = all_retryable });
    return if (all_retryable) 75 else 1;
}

test "reportFailedItems: under --log-format json every line is an NDJSON event" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var sink = runtime.OutcomeSink.init(ar.allocator());
    defer sink.deinit();
    sink.record("ana", false, "unknown field `nope`", false);
    sink.recordShown("bob", false, "shown", false, true);
    sink.record("cid", true, "", false);
    var out: [1024]u8 = undefined;
    var w = std.Io.Writer.fixed(&out);
    try std.testing.expectEqual(@as(u8, 1), try reportFailedItems(.{ .w = &w, .json = true, .label = "s.sql" }, &sink));
    var lines = std.mem.splitScalar(u8, std.mem.trimRight(u8, w.buffered(), "\n"), '\n');
    var n: usize = 0;
    while (lines.next()) |line| : (n += 1) {
        const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, line, .{});
        parsed.deinit();
    }
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expect(std.mem.indexOf(u8, w.buffered(), "\"2/3 item(s) failed\"") != null);
}

test "run refuses --port and --host, and an endpoint script fails to parse" {
    var port = [_][:0]u8{ @constCast("basalt"), @constCast("run"), @constCast("--port"), @constCast("8080"), @constCast("-c"), @constCast("SELECT 1 AS one;") };
    try std.testing.expectEqual(@as(u8, 2), try cmdRun(std.testing.allocator, &port));
    var host = [_][:0]u8{ @constCast("basalt"), @constCast("run"), @constCast("-c"), @constCast("SELECT 1 AS one;"), @constCast("--host=127.0.0.1") };
    try std.testing.expectEqual(@as(u8, 2), try cmdRun(std.testing.allocator, &host));
    var endpoint = [_][:0]u8{ @constCast("basalt"), @constCast("run"), @constCast("-c"), @constCast("CREATE ENDPOINT '/x'; SELECT 1 AS one;") };
    try std.testing.expectEqual(@as(u8, 1), try cmdRun(std.testing.allocator, &endpoint));
}
