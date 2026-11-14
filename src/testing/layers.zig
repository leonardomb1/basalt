//! The layer order of src/, held by a test: base → lang → exec → net, store, db and
//! format (peers of one another) → runtime → cli. A file imports only from its own
//! layer or one below it; tests are exempt, as a test may check a lower layer against
//! a higher one. testing/ holds test support (fakes, AST builders) any layer may name,
//! as only tests reach it and so it never joins a binary. root.zig, main.zig and
//! bench/ sit at the top and import freely.

const std = @import("std");
const dupscan = @import("dupscan.zig");

const Layer = enum(u8) { base, lang, exec, io, runtime, cli, testing, top };

fn layerOf(path: []const u8) Layer {
    const head = if (std.mem.indexOfScalar(u8, path, '/')) |i| path[0..i] else return .top;
    const names = .{
        .{ "base", Layer.base },       .{ "lang", Layer.lang },       .{ "exec", Layer.exec },
        .{ "net", Layer.io },          .{ "store", Layer.io },        .{ "db", Layer.io },
        .{ "format", Layer.io },       .{ "runtime", Layer.runtime }, .{ "cli", Layer.cli },
        .{ "testing", Layer.testing },
    };
    inline for (names) |n| if (std.mem.eql(u8, head, n[0])) return n[1];
    return .top;
}

pub const Breach = struct { from: []const u8, line: usize, to: []const u8 };

/// Every import in `text` (the file at src-relative `from`) that reaches a higher
/// layer, outside a `test` block.
pub fn breaches(arena: std.mem.Allocator, from: []const u8, text: []const u8, out: *std.ArrayList(Breach)) !void {
    const own = layerOf(from);
    if (own == .top or own == .testing) return;
    var in_test = false;
    var lines = std.mem.splitScalar(u8, text, '\n');
    var n: usize = 0;
    while (lines.next()) |line| {
        n += 1;
        if (std.mem.startsWith(u8, line, "test ") and std.mem.endsWith(u8, line, "{")) in_test = true;
        if (in_test) {
            if (std.mem.eql(u8, line, "}")) in_test = false;
            continue;
        }
        var rest = line;
        while (std.mem.indexOf(u8, rest, "@import(\"")) |at| {
            rest = rest[at + 9 ..];
            const end = std.mem.indexOfScalar(u8, rest, '"') orelse break;
            const rel = rest[0..end];
            rest = rest[end..];
            if (!std.mem.endsWith(u8, rel, ".zig")) continue;
            const to = try resolve(arena, from, rel);
            const target = layerOf(to);
            if (target != .testing and @intFromEnum(target) > @intFromEnum(own)) try out.append(arena, .{ .from = from, .line = n, .to = to });
        }
    }
}

fn resolve(arena: std.mem.Allocator, from: []const u8, rel: []const u8) ![]const u8 {
    const dir = std.fs.path.dirnamePosix(from) orelse "";
    return std.fs.path.resolvePosix(arena, &.{ dir, rel });
}

test "layers: no file imports from a layer above its own" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var src = try dupscan.openSrc();
    defer src.close();
    var found: std.ArrayList(Breach) = .empty;
    for (try dupscan.sources(a, src)) |path| {
        const text = try src.readFileAlloc(a, path, 16 << 20);
        try breaches(a, path, text, &found);
    }
    for (found.items) |b| std.debug.print("layers: {s}:{d} imports {s}\n", .{ b.from, b.line, b.to });
    try std.testing.expectEqual(@as(usize, 0), found.items.len);
}

test "layers: a test block may reach up, a declaration may not, and peers meet" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var found: std.ArrayList(Breach) = .empty;
    const text =
        \\const types = @import("types.zig");
        \\const eval = @import("../exec/eval.zig");
        \\test "x" {
        \\    const pq = @import("../format/parquet/read.zig");
        \\}
        \\const msgfmt = @import("../base/msgfmt.zig");
    ;
    try breaches(a, "lang/conform.zig", text, &found);
    try std.testing.expectEqual(@as(usize, 1), found.items.len);
    try std.testing.expectEqualStrings("exec/eval.zig", found.items[0].to);
    try std.testing.expectEqual(@as(usize, 2), found.items[0].line);
    found.clearRetainingCapacity();
    try breaches(a, "store/s3.zig", "const x = @import(\"../format/stream.zig\");\nconst y = @import(\"../net/http.zig\");\nconst f = @import(\"../testing/http_fake.zig\");", &found);
    try std.testing.expectEqual(@as(usize, 0), found.items.len);
}
