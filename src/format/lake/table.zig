//! A lake table read as a source: a folder that holds a table format's log rather
//! than loose files. Read as a plain Parquet folder it returns rows the log has
//! removed, so a folder read probes for the log first (`detect`) and, finding one,
//! resolves it once (`resolve`) into the table's live files and its schema as a
//! Parquet `Layout`. Everything a Parquet folder does then applies: lanes,
//! projection, row-group statistics and ranged reads.
//!
//! The format reads its own log (`delta.zig`); what a table read needs beside it is
//! here: how a path in a log maps to one basalt reads, how a partition value's text
//! becomes a typed value, and reading a log file whole wherever it lives. A folder
//! holding an Iceberg table's metadata is refused: basalt does not read Iceberg yet,
//! and read plain the folder would return the rows of files the table deleted.

const std = @import("std");
const Bytes = @import("../bytes.zig").Bytes;
const Layout = @import("../parquet/fit.zig").Layout;
const Value = @import("../../exec/value.zig").Value;
const azure = @import("../../store/azure.zig");
const cast = @import("../../exec/eval/cast.zig");
const delta = @import("delta.zig");
const eval = @import("../../exec/eval.zig");
const folder = @import("../../connect/folder.zig");
const formats = @import("../formats.zig");
const s3 = @import("../../store/s3.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const Kind = enum {
    delta,

    pub fn label(self: Kind) []const u8 {
        return switch (self) {
            .delta => "Delta",
        };
    }

    /// The kind a `format` names, null for none or one that is no table format.
    pub fn of(f: ?formats.FileFormat) ?Kind {
        return std.meta.stringToEnum(Kind, @tagName(f orelse return null));
    }
};

/// What a read is pinned to: a Delta commit `version`; none reads the newest.
pub const At = struct {
    version: ?u64 = null,

    pub fn eql(a: At, b: At) bool {
        return a.version == b.version;
    }

    pub fn pinned(self: At) bool {
        return self.version != null;
    }
};

pub const Resolved = struct {
    kind: Kind,
    root: []const u8,
    /// What EXPLAIN and `check` add after the root: "version 7, 12 files".
    label: []const u8,
    layout: *const Layout,
};

pub const Error = error{LakeTableRefused};

/// How deep a table's schema may nest structs; `collectLeaves` reads no deeper.
pub const max_depth = 64;

/// A table found in a folder; `listed` is its log's listing when finding it took
/// one, which resolving it then reads rather than listing again.
pub const Found = struct { kind: Kind, listed: ?[]const []const u8 = null };

/// The table format `root` (a folder path) holds, or null for a plain folder. A
/// listing that fails for any reason but the log's absence fails the read: reading
/// the folder plain instead would return the removed files' rows. Remotely a plain
/// folder costs a listing of each absent prefix, one request apiece. A folder
/// holding both a Delta log and Iceberg metadata (Delta UniForm) is read as Delta;
/// one holding Iceberg metadata alone is refused.
pub fn detect(arena: std.mem.Allocator, root: []const u8) !?Found {
    if (try holdsDelta(arena, root)) |listed| return .{ .kind = .delta, .listed = listed.items };
    if (try holdsIceberg(arena, root))
        return refuse("`{s}` holds an Iceberg table (metadata/*.metadata.json), which basalt does not read yet; read as a plain folder it would return the rows of files the table deleted, so it is refused (`WITH (format = 'parquet')` reads its files plain)", .{root});
    return null;
}

/// Whether `root` holds an Iceberg table's metadata files.
fn holdsIceberg(arena: std.mem.Allocator, root: []const u8) !bool {
    if (isHttp(root)) return false;
    const meta = try std.mem.concat(arena, u8, &.{ root, "metadata/" });
    if (std.mem.indexOf(u8, root, "://") != null) {
        const got = folder.listRaw(arena, meta) catch |e| return if (absent(e)) false else e;
        for (got) |p| if (isIcebergMetadata(p)) return true;
        return false;
    }
    var dir = std.fs.cwd().openDir(meta, .{ .iterate = true }) catch |e| return if (absent(e)) false else e;
    defer dir.close();
    var it = dir.iterate();
    while (try it.next()) |e| if (isIcebergMetadata(e.name)) return true;
    return false;
}

fn isIcebergMetadata(name: []const u8) bool {
    return std.mem.endsWith(u8, name, ".metadata.json") or std.mem.endsWith(u8, name, ".metadata.json.gz");
}

/// A listing, or none when a local folder was only opened.
pub const Listed = struct { items: ?[]const []const u8 = null };

fn holdsDelta(arena: std.mem.Allocator, root: []const u8) !?Listed {
    if (isHttp(root)) return null;
    const log = try std.mem.concat(arena, u8, &.{ root, delta.log_dir });
    if (std.mem.indexOf(u8, root, "://") != null) {
        const got = folder.listRaw(arena, log) catch |e| return if (absent(e)) null else e;
        return if (got.len > 0) .{ .items = got } else null;
    }
    var dir = std.fs.cwd().openDir(log, .{}) catch |e| return if (absent(e)) null else e;
    dir.close();
    return .{};
}

/// `root` as an absolute path when it is a relative local one, so paths a log
/// gives relative and absolute compare alike.
pub fn absoluteRoot(arena: std.mem.Allocator, root: []const u8) ![]const u8 {
    if (std.mem.indexOf(u8, root, "://") != null or std.fs.path.isAbsolute(root)) return root;
    const cwd = try std.process.getCwdAlloc(arena);
    return std.mem.concat(arena, u8, &.{ try std.fs.path.resolve(arena, &.{ cwd, root }), "/" });
}

/// Whether a listing or a read failed because nothing is there.
pub fn absent(e: anyerror) bool {
    return e == error.FileNotFound or e == error.NotDir or e == s3.Error.S3EmptyPrefix or
        e == azure.Error.AzureEmptyPrefix or e == error.SftpNoSuchFile or e == error.SmbNoSuchFile or
        e == error.HttpNotFound or e == s3.Error.S3KeyNotFound or e == azure.Error.AzureBlobNotFound;
}

/// Whether `path` may be a lake table: a folder.
pub fn isTablePath(path: []const u8) bool {
    return folder.isFolder(path);
}

/// `root` read as a `kind` table; `listed` what `detect` listed, if it did.
pub fn resolve(arena: std.mem.Allocator, root: []const u8, kind: Kind, at: At, listed: ?[]const []const u8) !*const Resolved {
    return switch (kind) {
        .delta => delta.resolve(arena, root, at, listed),
    };
}

pub fn isHttp(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://");
}

/// A refusal carrying its account, which the read reports as is.
pub fn refuse(comptime fmt: []const u8, args: anytype) anyerror {
    var buf: [480]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, fmt, args) catch blk: {
        @memcpy(buf[buf.len - 3 ..], "...");
        break :blk buf[0..];
    };
    return eval.explain(Error.LakeTableRefused, msg);
}

/// A whole file of a table's log, wherever it lives.
pub fn readWhole(arena: std.mem.Allocator, path: []const u8) ![]const u8 {
    const src = try Bytes.open(arena, path);
    defer src.close();
    const n = std.math.cast(usize, src.size()) orelse return error.FileTooBig;
    if (n == 0) return "";
    return src.range(arena, 0, n);
}

/// `%XX` escapes decoded, as a log's URI paths need; a malformed escape is kept.
pub const percentDecode = @import("../../net/percent.zig").decode;

/// A path a log names as one basalt reads: relative ones under `root`, `file:` URIs
/// as local paths, Hadoop's `s3a://` and `s3n://` as `s3://`, and an ADLS
/// `abfs[s]://container@account.dfs.core.windows.net/p` as `az://account/container/p`.
pub fn mapPath(arena: std.mem.Allocator, root: []const u8, p: []const u8) ![]const u8 {
    if (std.mem.startsWith(u8, p, "file:")) {
        var rest = p["file:".len..];
        if (std.mem.startsWith(u8, rest, "//")) {
            rest = rest[2..];
            const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return badScheme(p);
            const host = rest[0..slash];
            if (host.len > 0 and !std.mem.eql(u8, host, "localhost")) return badScheme(p);
            rest = rest[slash..];
        }
        return rest;
    }
    for ([_][]const u8{ "s3a://", "s3n://" }) |pre| if (std.mem.startsWith(u8, p, pre))
        return std.mem.concat(arena, u8, &.{ "s3://", p[pre.len..] });
    for ([_][]const u8{ "abfss://", "abfs://" }) |pre| if (std.mem.startsWith(u8, p, pre)) {
        const rest = p[pre.len..];
        const at = std.mem.indexOfScalar(u8, rest, '@') orelse return badScheme(p);
        const slash = std.mem.indexOfScalarPos(u8, rest, at, '/') orelse rest.len;
        const host = rest[at + 1 .. slash];
        const dot = std.mem.indexOfScalar(u8, host, '.') orelse return badScheme(p);
        return std.mem.concat(arena, u8, &.{ "az://", host[0..dot], "/", rest[0..at], rest[slash..] });
    };
    for ([_][]const u8{ "s3://", "az://", "sftp://", "smb://", "http://", "https://" }) |pre|
        if (std.mem.startsWith(u8, p, pre)) return p;
    if (std.mem.indexOf(u8, p, "://") != null) return badScheme(p);
    if (std.mem.startsWith(u8, p, "/")) return p;
    return std.mem.concat(arena, u8, &.{ root, p });
}

/// `version` on a folder that is no table: refused, as reading it plain would
/// ignore the version asked for.
pub fn notPinnable(root: []const u8) anyerror {
    return refuse("`version` pins a Delta table to a commit; folder `{s}` holds no _delta_log/", .{root});
}

fn badScheme(p: []const u8) anyerror {
    return refuse("path `{s}` has a scheme basalt does not read (file, s3, s3a, az, abfss, http, sftp, smb)", .{p});
}

/// A partition value's text as a value of its column's type: null, or empty for
/// any type, is NULL; dates, timestamps (with a space or ISO `T`, and an optional
/// `Z`), numbers and booleans as text; binary as its characters' codes. Null when
/// the text is not of that type.
pub fn partitionValue(arena: std.mem.Allocator, text: ?[]const u8, ty: types.Type) !?Value {
    const t = text orelse return .null;
    if (t.len == 0) return .null;
    switch (ty.kind) {
        .string => return .{ .string = t },
        .bytes => {
            var out = std.array_list.Managed(u8).init(arena);
            var it = (std.unicode.Utf8View.init(t) catch return .{ .bytes = t }).iterator();
            while (it.nextCodepoint()) |c| {
                if (c > 0xFF) return .{ .bytes = t };
                try out.append(@intCast(c));
            }
            return .{ .bytes = out.items };
        },
        else => return cast.castValueTyped(arena, .{ .string = t }, ty) catch null,
    }
}

test "mapPath: relative, file:, s3a and abfss paths, and a scheme basalt does not read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("/t/x=1/a.parquet", try mapPath(a, "/t/", "x=1/a.parquet"));
    try testing.expectEqualStrings("/data/t/a.parquet", try mapPath(a, "/t/", "file:///data/t/a.parquet"));
    try testing.expectEqualStrings("/data/t/a.parquet", try mapPath(a, "/t/", "file:/data/t/a.parquet"));
    try testing.expectEqualStrings("s3://b/t/a.parquet", try mapPath(a, "/t/", "s3a://b/t/a.parquet"));
    try testing.expectEqualStrings("az://acct/ctr/t/a.parquet", try mapPath(a, "/t/", "abfss://ctr@acct.dfs.core.windows.net/t/a.parquet"));
    try testing.expectError(Error.LakeTableRefused, mapPath(a, "/t/", "hdfs://nn/t/a.parquet"));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.LakeTableRefused).?, "`hdfs://nn/t/a.parquet`") != null);
    try testing.expectEqualStrings("ts=2026-01-01 00:00:00/a b.parquet", try percentDecode(a, "ts=2026-01-01%2000%3A00%3A00/a%20b.parquet"));
    try testing.expectEqualStrings("100%", try percentDecode(a, "100%"));
    try testing.expectEqualStrings("%zz", try percentDecode(a, "%zz"));
}

test "partitionValue: every type, empty and null text as NULL, both timestamp forms" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const T = types.Type;
    try testing.expect((try partitionValue(a, null, T.init(.int))).? == .null);
    try testing.expect((try partitionValue(a, "", T.init(.string))).? == .null);
    try testing.expectEqual(@as(i64, -7), (try partitionValue(a, "-7", T.init(.int))).?.int);
    try testing.expectEqual(@as(f64, 1.5), (try partitionValue(a, "1.5", T.init(.float))).?.float);
    try testing.expectEqual(true, (try partitionValue(a, "true", T.init(.bool))).?.bool);
    try testing.expectEqual(@as(i32, 20454), (try partitionValue(a, "2026-01-01", T.init(.date))).?.date);
    const us: i64 = 20454 * 86_400_000_000 + 3_600_000_000 + 500_000;
    try testing.expectEqual(us, (try partitionValue(a, "2026-01-01 01:00:00.5", T.init(.timestamp))).?.timestamp);
    try testing.expectEqual(us, (try partitionValue(a, "2026-01-01T01:00:00.500000Z", T.init(.timestamp))).?.timestamp);
    try testing.expectEqual(@as(i128, 1050), (try partitionValue(a, "10.5", T.decimal(10, 2))).?.decimal.unscaled);
    try testing.expectEqualStrings("\x01\x02", (try partitionValue(a, "\u{1}\u{2}", T.init(.bytes))).?.bytes);
    try testing.expectEqualStrings("eu", (try partitionValue(a, "eu", T.init(.string))).?.string);
    try testing.expect(try partitionValue(a, "x", T.init(.int)) == null);
}

test {
    _ = @import("delta.zig");
    _ = @import("delta_dv.zig");
    _ = @import("delta_log.zig");
    _ = @import("delta_merge.zig");
    _ = @import("delta_types.zig");
    _ = @import("delta_write.zig");
    _ = @import("roaring.zig");
}
