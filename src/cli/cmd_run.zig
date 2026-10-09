//! `basalt run`: run a script once, or serve it when it declares an endpoint.

const ErrOut = @import("args.zig").ErrOut;
const analyze = @import("../runtime/analyze.zig");
const http_server = @import("../server/http_server.zig");
const loadSource = @import("args.zig").loadSource;
const loadExit = @import("args.zig").loadExit;
const nextVal = @import("args.zig").nextVal;
const obs = @import("../runtime/obs.zig");
const parseLogFormat = @import("args.zig").parseLogFormat;
const parseSrcTo = @import("args.zig").parseSrcTo;
const runtime = @import("../runtime/run.zig");
const sizeFlag = @import("args.zig").sizeFlag;
const std = @import("std");
const threadFlagValue = @import("args.zig").threadFlagValue;
const unknownOption = @import("args.zig").unknownOption;
const wantsJsonLog = @import("args.zig").wantsJsonLog;

pub fn cmdRun(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    var stderr_buf: [4096]u8 = undefined;
    var stderr_file = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_file.interface;
    defer stderr.flush() catch {};

    const src = loadSource(arena.allocator(), "run", args, stderr) catch |e| switch (e) {
        error.Usage, error.Unreadable => |le| return loadExit(le),
        else => return e,
    };
    const eo = ErrOut{ .w = stderr, .json = wantsJsonLog(args), .label = src.label };
    const prog = (try parseSrcTo(arena.allocator(), src, eo)) orelse return 1;

    var params = std.array_list.Managed(runtime.ParamArg).init(alloc);
    defer params.deinit();
    var port: u16 = 8080;
    var host: []const u8 = http_server.default_host;
    var threads: usize = std.Thread.getCpuCount() catch 1;
    var log = runtime.LogConfig{};
    var level_set = false;
    var stdout_format: runtime.StdoutFormat = .table;
    var explain = false;
    var no_progress = false;
    var max_rows: ?u64 = null;
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
        if (threadFlagValue(a, args, &i)) |tv| {
            threads = std.fmt.parseInt(usize, tv, 10) catch {
                try stderr.print("error: invalid --threads `{s}`\n", .{tv});
                return 2;
            };
            if (threads == 0) threads = 1;
        } else if (std.mem.eql(u8, a, "--format")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            stdout_format = std.meta.stringToEnum(runtime.StdoutFormat, v) orelse {
                try stderr.print("error: --format must be table|json|csv|tsv|arrow\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--max-rows")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            max_rows = std.fmt.parseInt(u64, v, 10) catch {
                try stderr.print("error: invalid --max-rows `{s}`\n", .{v});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--spill-dir")) {
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
        } else if (std.mem.eql(u8, a, "--quiet") or std.mem.eql(u8, a, "-q")) {
            log.quiet = true;
        } else if (std.mem.eql(u8, a, "--log-format")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            log.format = parseLogFormat(v) orelse {
                try stderr.print("error: --log-format must be auto|text|json\n", .{});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--log-level")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            log.level = obs.Level.parse(v) orelse {
                try stderr.print("error: --log-level must be error|warn|info|debug\n", .{});
                return 2;
            };
            level_set = true;
        } else if (std.mem.eql(u8, a, "-p") or std.mem.eql(u8, a, "--param")) {
            const kv = (try nextVal(args, &i, a, stderr)) orelse return 2;
            const eqp = std.mem.indexOfScalar(u8, kv, '=') orelse {
                try stderr.print("error: param must be key=value, got `{s}`\n", .{kv});
                return 2;
            };
            try params.append(.{ .key = kv[0..eqp], .val = kv[eqp + 1 ..] });
        } else if (std.mem.eql(u8, a, "--host")) {
            host = (try nextVal(args, &i, a, stderr)) orelse return 2;
            _ = std.net.Address.parseIp(host, 0) catch {
                try stderr.print("error: invalid --host `{s}` (an IP address, such as 127.0.0.1)\n", .{host});
                return 2;
            };
        } else if (std.mem.eql(u8, a, "--port")) {
            const v = (try nextVal(args, &i, a, stderr)) orelse return 2;
            port = std.fmt.parseInt(u16, v, 10) catch {
                try stderr.print("error: invalid --port `{s}`\n", .{v});
                return 2;
            };
        } else if (try unknownOption(a, "run", stderr)) return 2;
    }

    if (prog.stmts.len > 0 and prog.stmts[0] == .kind and prog.stmts[0].kind.kind == .http) {
        http_server.serve(alloc, prog, host, port, .{ .format = log.format, .level = if (level_set) log.level else .info, .quiet = log.quiet, .summary = .stderr }) catch |e| {
            try stderr.print("{s}: serve error: {s}\n", .{ src.label, @errorName(e) });
            return 1;
        };
        return 0;
    }

    if (prog.explain == .plan) {
        var ebuf: [4096]u8 = undefined;
        var efile = std.fs.File.stdout().writer(&ebuf);
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
    _ = runtime.run(alloc, prog, .{ .params = params.items, .threads = threads, .outcomes = &sink, .log = log, .explain = explain or prog.explain == .analyze, .stdout_format = stdout_format, .progress = progress, .items = true, .max_rows = max_rows, .spill_dir = spill_dir, .spill_cap = spill_cap, .op_memory = op_memory }, &diag) catch |e| switch (e) {
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
    const nfail = sink.failures();
    if (nfail > 0) {
        var all_retryable = true;
        for (sink.list.items) |o| {
            if (o.ok) continue;
            if (!o.retryable) all_retryable = false;
            if (o.shown) continue;
            const tag = if (o.retryable) " (transient)" else "";
            try stderr.print("{s}: item `{s}` failed{s}: {s}\n", .{ src.label, o.item, tag, o.err });
        }
        try stderr.print("{s}: {d}/{d} item(s) failed\n", .{ src.label, nfail, sink.list.items.len });
        return if (all_retryable) 75 else 1;
    }
    return 0;
}
