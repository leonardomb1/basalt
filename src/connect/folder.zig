//! A folder read: a path ending in `/` — local, `sftp://`, `smb://`, `s3://` or `az://` —
//! is every file under it, subfolders included, as one table. Names starting
//! with `_` or `.` are skipped at any depth: `_SUCCESS`, `_temporary/`, `.crc`
//! are what Spark and Hadoop leave beside the data. The listing is sorted by
//! path, so the first file (whose schema the rest must match) and the order
//! rows arrive in do not depend on how a server lists.

const std = @import("std");
const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");

pub fn isFolder(path: []const u8) bool {
    return path.len > 0 and path[path.len - 1] == '/';
}

/// Every data file under `path`, sorted.
pub fn list(arena: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    if (sftp.isUrl(path)) {
        for (try sftp.listPrefix(arena, path)) |u| if (!hidden(below(u, path))) try out.append(u);
    } else if (smb.isUrl(path)) {
        for (try smb.listPrefix(arena, path)) |u| if (!hidden(below(u, path))) try out.append(u);
    } else if (objstore.isUrl(path)) {
        var client = http_client.initClient(arena);
        defer client.deinit();
        // an empty prefix fails here, in the provider's words
        const urls = try objstore.listPrefix(arena, &client, path);
        for (urls) |u| if (!hidden(below(u, path))) try out.append(u);
    } else {
        var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
        defer dir.close();
        var w = try dir.walk(arena);
        defer w.deinit();
        while (try w.next()) |en| {
            if (en.kind != .file or hidden(en.path)) continue;
            try out.append(try std.fmt.allocPrint(arena, "{s}{s}", .{ path, en.path }));
        }
    }
    std.mem.sort([]const u8, out.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    return out.items;
}

pub fn below(url: []const u8, folder: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, url, folder)) url[folder.len..] else url;
}

/// A path below the folder with a part starting `_` or `.`.
fn hidden(rel: []const u8) bool {
    var it = std.mem.tokenizeAny(u8, rel, "/\\");
    while (it.next()) |part| if (part[0] == '_' or part[0] == '.') return true;
    return false;
}

pub const Kind = enum { parquet, csv };

const csv_exts = [_][]const u8{ ".csv", ".tsv", ".txt", ".csv.gz", ".tsv.gz", ".txt.gz", ".csv.zst", ".tsv.zst", ".txt.zst" };

pub fn isCsvName(name: []const u8) bool {
    for (csv_exts) |x| if (std.ascii.endsWithIgnoreCase(name, x)) return true;
    return false;
}

pub fn isParquetName(name: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(name, ".parquet");
}

/// What a folder holds, when no `format` says: Parquet files, or CSVs. Both is
/// refused, as one table cannot be read from the two; `mixed` names one of each.
pub const Verdict = union(enum) {
    kind: Kind,
    empty,
    neither,
    mixed: struct { parquet: []const u8, csv: []const u8 },
};

pub fn kindOf(files: []const []const u8) Verdict {
    var pq: ?[]const u8 = null;
    var cs: ?[]const u8 = null;
    for (files) |f| {
        if (pq == null and isParquetName(f)) pq = f;
        if (cs == null and isCsvName(f)) cs = f;
    }
    if (pq != null and cs != null) return .{ .mixed = .{ .parquet = pq.?, .csv = cs.? } };
    if (pq != null) return .{ .kind = .parquet };
    if (cs != null) return .{ .kind = .csv };
    return if (files.len == 0) .empty else .neither;
}

/// The files of `files` a read of `kind` takes, in order.
pub fn only(arena: std.mem.Allocator, files: []const []const u8, kind: Kind) ![]const []const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    for (files) |f| {
        const keep = switch (kind) {
            .parquet => isParquetName(f),
            .csv => isCsvName(f),
        };
        if (keep) try out.append(f);
    }
    return out.items;
}

test "folder: hidden parts, kinds, a local walk sorted" {
    try std.testing.expect(hidden("_SUCCESS"));
    try std.testing.expect(hidden("year=2026/_temporary/x.parquet"));
    try std.testing.expect(hidden("part-0.parquet/.crc"));
    try std.testing.expect(!hidden("year=2026/month=10/part-0.parquet"));

    const pq = [_][]const u8{ "d/a.parquet", "d/notes.pdf" };
    try std.testing.expectEqual(Kind.parquet, kindOf(&pq).kind);
    const mixed = [_][]const u8{ "d/a.parquet", "d/b.CSV" };
    try std.testing.expectEqualStrings("d/b.CSV", kindOf(&mixed).mixed.csv);
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
