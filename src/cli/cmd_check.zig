//! `basalt check`: parse and validate a script without running it, every problem in
//! script order, as text or JSON for an editor.

const ErrOut = @import("args.zig").ErrOut;
const analyze = @import("../runtime/analyze.zig");
const ast = @import("../lang/ast.zig");
const include = @import("../lang/include.zig");
const loadSource = @import("args.zig").loadSource;
const nextVal = @import("args.zig").nextVal;
const parser = @import("../lang/sql_parser.zig");
const std = @import("std");
const unknownOption = @import("args.zig").unknownOption;
const types = @import("../lang/types.zig");

pub const CheckIssue = struct { msg: []const u8, pos: ?ast.Pos = null, end: ?ast.Pos = null, file: ?[]const u8 = null };

pub const CheckOpts = struct {
    overrides: []const analyze.ParamOverride = &.{},
    known: []const analyze.KnownTable = &.{},
    declarations_only: bool = false,
};

/// Every problem in `text`, in script order: each statement that does not parse
/// (parsing resumes at the next), then each that does not check. Nothing runs or connects.
pub fn checkText(a: std.mem.Allocator, text: []const u8, label: []const u8, dir: []const u8, opts: CheckOpts) ![]CheckIssue {
    var issues = std.array_list.Managed(CheckIssue).init(a);
    var names = std.array_list.Managed([]const u8).init(a);
    for (opts.known) |k| try names.append(k.name);
    var perrs = std.array_list.Managed(parser.Diagnostic).init(a);
    var idiag: include.Diag = .{};
    const prog = include.loadProgramOpts(a, text, label, dir, &idiag, .{ .known_tables = names.items, .errors = &perrs }) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.ParseFailed => {
            const p = idiag.parse;
            try issues.append(.{ .msg = p.msg, .pos = .{ .line = p.line, .col = p.col }, .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null, .file = if (idiag.label.len > 0 and !std.mem.eql(u8, idiag.label, label)) idiag.label else null });
            return issues.toOwnedSlice();
        },
    };
    for (perrs.items) |p| try issues.append(.{ .msg = p.msg, .pos = .{ .line = p.line, .col = p.col }, .end = if (p.end_line > 0) ast.Pos{ .line = p.end_line, .col = p.end_col } else null });

    var found = std.array_list.Managed(analyze.Issue).init(a);
    var adiag = analyze.Diag{};
    _ = try analyze.analyzeOpts(a, prog, .{
        .overrides = opts.overrides,
        .known_tables = opts.known,
        .issues = &found,
        .declarations_only = opts.declarations_only or perrs.items.len > 0,
    }, &adiag);
    for (found.items) |f| try issues.append(.{ .msg = f.msg, .pos = f.pos, .end = f.end });

    std.mem.sort(CheckIssue, issues.items, {}, struct {
        fn lt(_: void, x: CheckIssue, y: CheckIssue) bool {
            const px = x.pos orelse ast.Pos{ .line = 0, .col = 0 };
            const py = y.pos orelse ast.Pos{ .line = 0, .col = 0 };
            return px.line < py.line or (px.line == py.line and px.col < py.col);
        }
    }.lt);
    return issues.toOwnedSlice();
}

pub fn cmdCheck(alloc: std.mem.Allocator, args: [][:0]u8) !u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var out_buf: [8192]u8 = undefined;
    var out_file = std.fs.File.stdout().writer(&out_buf);
    const stdout = &out_file.interface;
    defer stdout.flush() catch {};
    var err_buf: [4096]u8 = undefined;
    var err_file = std.fs.File.stderr().writer(&err_buf);
    const stderr = &err_file.interface;
    defer stderr.flush() catch {};

    var overrides = std.array_list.Managed(analyze.ParamOverride).init(a);
    var known = std.array_list.Managed(analyze.KnownTable).init(a);
    var json = false;
    var i: usize = 2;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "-c") or std.mem.eql(u8, args[i], "--command")) {
            i += 1;
            continue;
        }
        if (std.mem.eql(u8, args[i], "--format")) {
            const v = (try nextVal(args, &i, "--format", stderr)) orelse return 2;
            if (std.mem.eql(u8, v, "json")) json = true else if (!std.mem.eql(u8, v, "text")) {
                try stderr.print("error: `check --format` must be text|json\n", .{});
                return 2;
            }
            continue;
        }
        if (std.mem.eql(u8, args[i], "--known")) {
            const v = (try nextVal(args, &i, "--known", stderr)) orelse return 2;
            var it = std.mem.splitScalar(u8, v, ',');
            while (it.next()) |n| {
                const name = std.mem.trim(u8, n, " ");
                if (name.len > 0) try known.append(.{ .name = name });
            }
            continue;
        }
        if (!std.mem.eql(u8, args[i], "-p") and !std.mem.eql(u8, args[i], "--param")) {
            if (try unknownOption(args[i], "check", stderr)) return 2;
            continue;
        }
        i += 1;
        if (i >= args.len) {
            try stderr.print("error: missing key=value after `-p`\n", .{});
            return 2;
        }
        const eq = std.mem.indexOfScalar(u8, args[i], '=') orelse {
            try stderr.print("error: param must be key=value, got `{s}`\n", .{args[i]});
            return 2;
        };
        try overrides.append(.{ .name = args[i][0..eq], .value = args[i][eq + 1 ..] });
    }

    const src = (try loadSource(a, "check", args, stderr)) orelse return 1;
    const eo = ErrOut{ .w = if (json) stdout else stderr, .json = json, .bare = true, .label = src.label };
    const issues = try checkText(a, src.text, src.label, src.dir, .{ .overrides = overrides.items, .known = known.items });
    if (json) try stdout.writeAll("[");
    for (issues, 0..) |is, k| {
        if (json and k > 0) try stdout.writeByte(',');
        try eo.report(.{ .msg = is.msg, .pos = is.pos, .end = is.end, .file = is.file });
    }
    if (json) try stdout.writeAll("]\n");
    if (issues.len > 0) return 1;
    if (!json) try stdout.print("ok: {s} checks out\n", .{src.label});
    return 0;
}

test "checkText: every problem in script order, known tables typed or not, nothing stops at the first" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const text =
        \\SELECT nope FROM RANGE(3);
        \\SELECT id, amt * 2 AS a2 FROM enrich WHERE day >= '2024-01-01';
        \\SELECT 1 +;
        \\SELECT missing FROM enrich;
        \\SELECT whatever FROM loose;
    ;
    const fields = [_]types.Schema.Field{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "amt", .ty = types.Type.decimal(10, 2) },
        .{ .name = "day", .ty = types.Type.init(.date) },
    };
    const known = [_]analyze.KnownTable{ .{ .name = "enrich", .schema = .{ .fields = &fields } }, .{ .name = "loose" } };
    const issues = try checkText(a, text, "t.sql", ".", .{ .known = &known });
    try std.testing.expectEqual(@as(usize, 3), issues.len);
    try std.testing.expectEqual(@as(u32, 1), issues[0].pos.?.line);
    try std.testing.expect(std.mem.indexOf(u8, issues[0].msg, "`nope`") != null);
    try std.testing.expectEqual(@as(u32, 3), issues[1].pos.?.line);
    try std.testing.expectEqual(@as(u32, 4), issues[2].pos.?.line);
    try std.testing.expect(std.mem.indexOf(u8, issues[2].msg, "`missing`") != null);

    const unknown = try checkText(a, "SELECT * FROM enrich;\nSELECT nope FROM RANGE(1);", "t.sql", ".", .{});
    try std.testing.expectEqual(@as(usize, 2), unknown.len);
    try std.testing.expect(std.mem.indexOf(u8, unknown[0].msg, "unknown source `enrich`") != null);

    try std.testing.expectEqual(@as(usize, 0), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{ .declarations_only = true })).len);
    try std.testing.expectEqual(@as(usize, 1), (try checkText(a, "PARAM n INT DEFAULT 1;", "t.sql", ".", .{})).len);
}
