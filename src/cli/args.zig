//! What every command shares: loading the script (a path, `-` or `-c`), finding it
//! among the flags, `-j`, and printing a diagnostic as text or JSON.

const ast = @import("../lang/ast.zig");
const include = @import("../lang/include.zig");
const obs = @import("../runtime/obs.zig");
const std = @import("std");

const Source = struct { label: []const u8, text: []const u8, dir: []const u8 = "." };

/// `Usage` is a command line without a script (exit 2); `Unreadable` a script that
/// could not be read (exit 1). Either is reported on `stderr` here.
pub const LoadError = error{ Usage, Unreadable };

/// The exit code for a script that could not be loaded.
pub fn loadExit(e: LoadError) u8 {
    return switch (e) {
        error.Usage => 2,
        error.Unreadable => 1,
    };
}

pub fn loadSource(arena: std.mem.Allocator, verb: []const u8, args: [][:0]u8, stderr: *std.Io.Writer) !Source {
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-c") or std.mem.eql(u8, args[i], "--command")) {
            if (i + 1 >= args.len) {
                try stderr.print("error: missing script after `{s}`\n", .{args[i]});
                return error.Usage;
            }
            return Source{ .label = "<command>", .text = args[i + 1] };
        }
    }
    const at = scriptArg(args) orelse {
        try stderr.print("error: `{s}` requires a <script> path, `-` for stdin, or `-c <script>`\n", .{verb});
        return error.Usage;
    };
    if (std.mem.eql(u8, args[at], "-")) {
        const text = std.fs.File.stdin().readToEndAlloc(arena, 8 << 20) catch |e| {
            try stderr.print("error: cannot read script from stdin: {s}\n", .{@errorName(e)});
            return error.Unreadable;
        };
        return Source{ .label = "<stdin>", .text = text };
    }
    const path = args[at];
    const text = std.fs.cwd().readFileAlloc(arena, path, 8 << 20) catch |e| {
        try stderr.print("error: cannot read `{s}`: {s}\n", .{ path, @errorName(e) });
        return error.Unreadable;
    };
    return Source{ .label = path, .text = text, .dir = std.fs.path.dirname(path) orelse "." };
}

const valued_flags = [_][]const u8{ "-p", "--param", "-j", "--threads", "--format", "--log-format", "--log-level", "--port", "--host", "--max-rows", "--pos", "--known" };

/// The first argument that is neither a flag nor a flag's value, `-` included, so the
/// script may come before or after its flags.
pub fn scriptArg(args: [][:0]u8) ?usize {
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "-")) return i;
        if (a.len > 0 and a[0] == '-') {
            for (valued_flags) |f| if (std.mem.eql(u8, a, f)) {
                i += 1;
                break;
            };
            continue;
        }
        return i;
    }
    return null;
}

/// Reports a dash-prefixed argument no branch claimed (`-` alone is stdin), so the
/// caller exits 2: a typo like `--treads` once ran single-threaded without a word.
pub fn unknownOption(arg: []const u8, verb: []const u8, stderr: *std.Io.Writer) !bool {
    if (arg.len < 2 or arg[0] != '-') return false;
    try stderr.print("error: unknown option `{s}` for `{s}` — see `basalt help`\n", .{ arg, verb });
    return true;
}

pub fn parseLogFormat(v: []const u8) ?obs.Format {
    if (std.mem.eql(u8, v, "text")) return .text;
    if (std.mem.eql(u8, v, "json")) return .json;
    if (std.mem.eql(u8, v, "auto")) return .auto;
    return null;
}

fn printDiag(stderr: *std.Io.Writer, label: []const u8, tag: []const u8, pos: ?ast.Pos, msg: []const u8) !void {
    if (pos) |p|
        try stderr.print("{s}:{d}:{d}: error{s}: {s}\n", .{ label, p.line, p.col, tag, msg })
    else
        try stderr.print("{s}: error{s}: {s}\n", .{ label, tag, msg });
}

pub const ErrOut = struct {
    w: *std.Io.Writer,
    json: bool,
    label: []const u8,
    bare: bool = false,

    const Located = struct {
        msg: []const u8,
        pos: ?ast.Pos = null,
        end: ?ast.Pos = null,
        file: ?[]const u8 = null,
        transient: bool = false,
        event: []const u8 = "script_error",
    };

    pub fn report(self: ErrOut, e: Located) !void {
        const file = e.file orelse self.label;
        if (!self.json) {
            return printDiag(self.w, file, if (e.transient) " (transient)" else "", e.pos, e.msg);
        }
        if (self.bare)
            try self.w.writeAll("{\"level\":\"error\",\"msg\":")
        else
            try self.w.print("{{\"ts\":{d},\"level\":\"error\",\"event\":\"{s}\",\"msg\":", .{ std.time.milliTimestamp(), e.event });
        try std.json.Stringify.encodeJsonString(e.msg, .{}, self.w);
        try self.w.writeAll(",\"file\":");
        try std.json.Stringify.encodeJsonString(file, .{}, self.w);
        if (e.pos) |p| {
            try self.w.print(",\"line\":{d},\"col\":{d}", .{ p.line, p.col });
            if (e.end) |x| try self.w.print(",\"end_line\":{d},\"end_col\":{d}", .{ x.line, x.col });
        }
        try self.w.print(",\"class\":\"{s}\"}}", .{if (e.transient) "transient" else "permanent"});
        if (!self.bare) try self.w.writeByte('\n');
    }
};

pub fn wantsJsonLog(args: [][:0]u8) bool {
    var i: usize = 2;
    while (i + 1 < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--log-format")) return std.mem.eql(u8, args[i + 1], "json");
    }
    return false;
}

/// The AST slices into `src.text` and the included files' texts, so all must outlive
/// it. An error in an included file names that file's path and lines.
fn parseSrc(arena: std.mem.Allocator, src: Source, stderr: *std.Io.Writer) !?ast.Program {
    return parseSrcTo(arena, src, .{ .w = stderr, .json = false, .label = src.label });
}

pub fn parseSrcTo(arena: std.mem.Allocator, src: Source, eo: ErrOut) !?ast.Program {
    var diag: include.Diag = .{};
    return include.loadProgram(arena, src.text, src.label, src.dir, &diag) catch |e| switch (e) {
        error.ParseFailed => {
            const p = diag.parse;
            try eo.report(.{
                .msg = p.msg,
                .pos = .{ .line = p.line, .col = p.col },
                .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null,
                .file = if (diag.label.len > 0) diag.label else null,
                .event = "parse_error",
            });
            return null;
        },
        error.OutOfMemory => return e,
    };
}

pub fn nextVal(args: [][:0]u8, i: *usize, flag: []const u8, stderr: *std.Io.Writer) !?[]const u8 {
    i.* += 1;
    if (i.* >= args.len) {
        try stderr.print("error: missing value after `{s}`\n", .{flag});
        return null;
    }
    return args[i.*];
}

pub fn threadFlagValue(a: []const u8, args: [][:0]u8, i: *usize) ?[]const u8 {
    if (std.mem.eql(u8, a, "-j") or std.mem.eql(u8, a, "--threads")) {
        if (i.* + 1 < args.len) {
            i.* += 1;
            return args[i.*];
        }
        return "";
    }
    if (std.mem.startsWith(u8, a, "-j")) return a[2..];
    if (std.mem.startsWith(u8, a, "--threads=")) return a["--threads=".len..];
    return null;
}

pub fn testArgv(arena: std.mem.Allocator, strs: []const []const u8) ![][:0]u8 {
    const out = try arena.alloc([:0]u8, strs.len);
    for (strs, 0..) |s, i| out[i] = try arena.dupeZ(u8, s);
    return out;
}

test "scriptArg: the script is found whatever order flags and it come in" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Case = struct { argv: []const []const u8, want: ?usize };
    const cases = [_]Case{
        .{ .argv = &.{ "basalt", "run", "x.sql", "--format", "json" }, .want = 2 },
        .{ .argv = &.{ "basalt", "run", "--format", "json", "x.sql" }, .want = 4 },
        .{ .argv = &.{ "basalt", "run", "--format", "json", "-" }, .want = 4 },
        .{ .argv = &.{ "basalt", "run", "-p", "k=v", "-j", "4", "--quiet", "x.sql" }, .want = 7 },
        .{ .argv = &.{ "basalt", "run", "-j8", "x.sql" }, .want = 3 },
        .{ .argv = &.{ "basalt", "run", "--format", "json" }, .want = null },
        .{ .argv = &.{ "basalt", "check", "-p", "out.sql" }, .want = null },
        .{ .argv = &.{ "basalt", "check", "--known", "t1,t2", "x.sql" }, .want = 4 },
    };
    for (cases) |c| try std.testing.expectEqual(c.want, scriptArg(try testArgv(a, c.argv)));
}

test "threadFlagValue recognizes all four -j/--threads spellings" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var args = try testArgv(a, &.{ "-j", "4" });
    var i: usize = 0;
    try std.testing.expectEqualStrings("4", threadFlagValue(args[0], args, &i).?);
    try std.testing.expectEqual(@as(usize, 1), i);
    args = try testArgv(a, &.{ "--threads", "8" });
    i = 0;
    try std.testing.expectEqualStrings("8", threadFlagValue(args[0], args, &i).?);
    try std.testing.expectEqual(@as(usize, 1), i);

    args = try testArgv(a, &.{"-j16"});
    i = 0;
    try std.testing.expectEqualStrings("16", threadFlagValue(args[0], args, &i).?);
    try std.testing.expectEqual(@as(usize, 0), i);
    args = try testArgv(a, &.{"--threads=2"});
    i = 0;
    try std.testing.expectEqualStrings("2", threadFlagValue(args[0], args, &i).?);

    args = try testArgv(a, &.{"-j"});
    i = 0;
    try std.testing.expectEqualStrings("", threadFlagValue(args[0], args, &i).?);

    args = try testArgv(a, &.{ "-p", "k=v" });
    i = 0;
    try std.testing.expect(threadFlagValue(args[0], args, &i) == null);
    try std.testing.expectEqual(@as(usize, 0), i);
}
