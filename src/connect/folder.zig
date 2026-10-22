//! A folder read: a path ending in `/` — local, `sftp://`, `smb://`, `s3://` or `az://` —
//! is every file under it, subfolders included, as one table. Names starting
//! with `_` or `.` are skipped at any depth: `_SUCCESS`, `_temporary/`, `.crc`
//! are what Spark and Hadoop leave beside the data. The listing is sorted by
//! path, so the first file (whose schema the rest must match) and the order
//! rows arrive in do not depend on how a server lists. With no `format`, a
//! folder must hold one kind of file (`formats.zig` says which a folder reads); two kinds are
//! refused, as one table cannot be read from both, and `Verdict.mixed` names one
//! file of each.

const std = @import("std");
const formats = @import("../format/formats.zig");
const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");

pub fn isFolder(path: []const u8) bool {
    return path.len > 0 and path[path.len - 1] == '/';
}

pub fn list(arena: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    for (try listRaw(arena, path)) |f| if (!hidden(below(f, path))) try out.append(f);
    std.sort.pdq([]const u8, out.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return out.items;
}

/// Every file under `path`, hidden ones included, in the order the listing gave:
/// what a table's log (`_delta_log/`) is read from. An SFTP or SMB listing still
/// leaves out subfolders starting `_` or `.`.
pub fn listRaw(arena: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    if (sftp.isUrl(path) or smb.isUrl(path)) {
        const names = if (sftp.isUrl(path)) try sftp.listPrefix(arena, path) else try smb.listPrefix(arena, path);
        for (names) |n| try out.append(try std.mem.concat(arena, u8, &.{ path, n }));
    } else if (objstore.isUrl(path)) {
        var client = http_client.initClient(arena);
        defer client.deinit();
        try out.appendSlice(try objstore.listPrefix(arena, &client, path));
    } else {
        var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
        defer dir.close();
        var w = try dir.walk(arena);
        defer w.deinit();
        while (try w.next()) |en| {
            if (en.kind != .file) continue;
            try out.append(try std.fmt.allocPrint(arena, "{s}{s}", .{ path, en.path }));
        }
    }
    return out.items;
}

pub fn below(url: []const u8, folder: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, url, folder)) url[folder.len..] else url;
}

fn hidden(rel: []const u8) bool {
    var it = std.mem.tokenizeAny(u8, rel, "/\\");
    while (it.next()) |part| if (part[0] == '_' or part[0] == '.') return true;
    return false;
}

pub const Kind = formats.FileFormat;

fn isKind(name: []const u8, kind: Kind) bool {
    return formats.inFolder(formats.of(kind), name);
}

pub const Named = struct { kind: Kind, file: []const u8 };

pub const Verdict = union(enum) {
    kind: Kind,
    empty,
    neither,
    mixed: struct { a: Named, b: Named },
};

pub fn kindOf(files: []const []const u8) Verdict {
    var first: [std.meta.fields(Kind).len]?[]const u8 = @splat(null);
    for (files) |f| inline for (std.meta.fields(Kind), 0..) |k, i| {
        if (first[i] == null and isKind(f, @enumFromInt(k.value))) first[i] = f;
    };
    var found: ?Named = null;
    for (first, 0..) |f, i| {
        const name = f orelse continue;
        const here = Named{ .kind = @enumFromInt(i), .file = name };
        if (found) |a| return .{ .mixed = .{ .a = a, .b = here } };
        found = here;
    }
    if (found) |a| return .{ .kind = a.kind };
    return if (files.len == 0) .empty else .neither;
}

pub fn only(arena: std.mem.Allocator, files: []const []const u8, kind: Kind) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    for (files) |f| {
        if (isKind(f, kind)) try out.append(f);
    }
    return out.items;
}

pub fn label(kind: Kind) []const u8 {
    return formats.of(kind).label;
}

test "folder: hidden parts, kinds, a local walk sorted" {
    try std.testing.expect(hidden("_SUCCESS"));
    try std.testing.expect(hidden("year=2026/_temporary/x.parquet"));
    try std.testing.expect(hidden("part-0.parquet/.crc"));
    try std.testing.expect(!hidden("year=2026/month=10/part-0.parquet"));

    const pq = [_][]const u8{ "d/a.parquet", "d/notes.pdf" };
    try std.testing.expectEqual(Kind.parquet, kindOf(&pq).kind);
    const mixed = [_][]const u8{ "d/a.parquet", "d/b.CSV" };
    try std.testing.expectEqualStrings("d/b.CSV", kindOf(&mixed).mixed.a.file);
    const js = [_][]const u8{ "d/a.ndjson.gz", "d/b.jsonl" };
    try std.testing.expectEqual(Kind.json, kindOf(&js).kind);
    const pics = [_][]const u8{"d/x.png"};
    try std.testing.expect(kindOf(&pics) == .neither);
    try std.testing.expect(kindOf(&.{}) == .empty);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makePath("year=2026/month=10");
    try tmp.dir.makePath("_temporary");
    for ([_][]const u8{ "year=2026/month=10/b.parquet", "year=2026/a.parquet", "_temporary/x.parquet", "_SUCCESS" }) |f|
        try tmp.dir.writeFile(.{ .sub_path = f, .data = "" });
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const root = try tmp.dir.realpathAlloc(ar.allocator(), ".");
    const base = try std.fmt.allocPrint(ar.allocator(), "{s}/", .{root});
    const got = try list(ar.allocator(), base);
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expect(std.mem.endsWith(u8, got[0], "year=2026/a.parquet"));
    try std.testing.expect(std.mem.endsWith(u8, got[1], "year=2026/month=10/b.parquet"));
}
