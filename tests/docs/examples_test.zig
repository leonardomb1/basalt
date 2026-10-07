//! Every ```sql example in docs/ is parsed and checked as `basalt check` would,
//! after tests/docs/prelude.sql, so the book cannot describe syntax the engine
//! refuses. A fence tagged `sql ignore` is a fragment or synopsis and is skipped;
//! one tagged `sql error` must fail, and one tagged `sql cont` is checked after the
//! block before it on the page, whose declarations it uses. The run step has side effects so an edit to
//! the docs alone re-runs it; it reads docs/ from the build root.

const std = @import("std");
const basalt = @import("basalt");
const analyze = basalt.analyze;
const parser = basalt.sql_parser;

const prelude = @embedFile("prelude.sql");

/// Tables the examples read that no example declares.
const known_names = [_][]const u8{ "hits", "vendas", "pagamentos" };
const known = [_]analyze.KnownTable{ .{ .name = known_names[0] }, .{ .name = known_names[1] }, .{ .name = known_names[2] } };

const Block = struct { file: []const u8, line: usize, text: []const u8, expect_error: bool };

test "every SQL example in the book checks out" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var blocks = std.array_list.Managed(Block).init(a);
    var dir = try std.fs.cwd().openDir("docs", .{ .iterate = true });
    defer dir.close();
    var walker = try dir.walk(a);
    defer walker.deinit();
    while (try walker.next()) |entry| {
        if (entry.kind != .file or !std.mem.endsWith(u8, entry.path, ".md")) continue;
        const text = try dir.readFileAlloc(a, entry.path, 1 << 20);
        try collect(a, try std.fmt.allocPrint(a, "docs/{s}", .{entry.path}), text, &blocks);
    }
    try std.testing.expect(blocks.items.len > 0);

    var failures: usize = 0;
    for (blocks.items) |blk| {
        const why = try checkBlock(a, blk.text);
        if (blk.expect_error and why == null) {
            std.debug.print("{s}:{d}: expected an error, but the example checks out\n", .{ blk.file, blk.line });
            failures += 1;
        } else if (!blk.expect_error and why != null) {
            std.debug.print("{s}:{d}: {s}\n", .{ blk.file, blk.line, why.? });
            failures += 1;
        }
    }
    if (failures > 0) {
        std.debug.print("{d} of {d} examples failed\n", .{ failures, blocks.items.len });
        return error.TestUnexpectedResult;
    }
}

/// The ```sql blocks of one page, an indented fence (in a list) de-indented.
fn collect(a: std.mem.Allocator, file: []const u8, text: []const u8, out: *std.array_list.Managed(Block)) !void {
    var lines = std.mem.splitScalar(u8, text, '\n');
    var n: usize = 0;
    while (lines.next()) |raw| {
        n += 1;
        const indent = raw.len - std.mem.trimLeft(u8, raw, " ").len;
        const fence = raw[indent..];
        if (!std.mem.startsWith(u8, fence, "```sql")) continue;
        const info = std.mem.trim(u8, fence["```sql".len..], " ");
        const start = n + 1;
        var body = std.array_list.Managed(u8).init(a);
        while (lines.next()) |l| {
            n += 1;
            if (std.mem.startsWith(u8, std.mem.trimLeft(u8, l, " "), "```")) break;
            try body.appendSlice(if (l.len >= indent) l[indent..] else std.mem.trimLeft(u8, l, " "));
            try body.append('\n');
        }
        if (std.mem.eql(u8, info, "ignore")) continue;
        var src = body.items;
        if (std.mem.eql(u8, info, "cont")) {
            const prev = if (out.items.len > 0 and std.mem.eql(u8, out.items[out.items.len - 1].file, file)) out.items[out.items.len - 1].text else "";
            src = try std.mem.concat(a, u8, &.{ prev, src });
        }
        try out.append(.{ .file = file, .line = start, .text = src, .expect_error = std.mem.eql(u8, info, "error") });
    }
}

/// Null when the example (after the prelude) checks out, else the first problem.
fn checkBlock(a: std.mem.Allocator, example: []const u8) !?[]const u8 {
    var src = std.array_list.Managed(u8).init(a);
    var it = std.mem.splitScalar(u8, prelude, '\n');
    while (it.next()) |line| {
        if (std.mem.startsWith(u8, line, "PARAM ")) {
            const name_end = std.mem.indexOfScalarPos(u8, line, 6, ' ') orelse line.len;
            const decl = try std.fmt.allocPrint(a, "PARAM {s} ", .{line[6..name_end]});
            if (std.mem.indexOf(u8, example, decl) != null) continue;
            const let = try std.fmt.allocPrint(a, "LET {s} ", .{line[6..name_end]});
            if (std.mem.indexOf(u8, example, let) != null) continue;
        }
        try src.appendSlice(line);
        try src.append('\n');
    }
    try src.appendSlice(example);

    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSourceOpts(a, src.items, &pdiag, .{ .known_tables = &known_names }) catch
        return try std.fmt.allocPrint(a, "parse error: {s}", .{pdiag.msg});
    var diag = analyze.Diag{};
    _ = analyze.analyzeOpts(a, prog, .{ .known_tables = &known, .declarations_only = true }, &diag) catch |e| switch (e) {
        error.OutOfMemory => return e,
        error.AnalyzeFailed => return try a.dupe(u8, diag.msg),
    };
    return null;
}
