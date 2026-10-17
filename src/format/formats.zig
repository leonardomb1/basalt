//! The file formats basalt reads and writes, one entry each: the names `format = '…'`
//! takes, the extensions that pick it, what it can be read through (compression, an
//! archive, a remote location, stdin, a folder), how a read fans out, the options a
//! read and a write take, and how it is written and appended to. Every check and
//! every message that lists formats is derived from this table, the lists at compile
//! time, so a new format is one entry and no message goes stale.

const std = @import("std");
const stream = @import("stream.zig");

pub const FileFormat = enum { csv, parquet, arrow, xlsx, json, xml };

/// How a read splits across lanes: by row group anywhere, by byte range of a local
/// uncompressed file, or not at all.
pub const Lanes = enum { row_groups, local_ranges, serial };

/// Writing a format: why a `.gz` name cannot gzip it and why `APPEND` cannot extend
/// it, each null when it can.
pub const Write = struct {
    gzip_refused: ?[]const u8 = null,
    append_refused: ?[]const u8 = null,
};

pub const Format = struct {
    tag: FileFormat,
    label: []const u8,
    read_what: []const u8,
    names: []const []const u8,
    exts: []const []const u8,
    folder_exts: []const []const u8 = &.{},
    compressed_read_refused: ?[]const u8 = null,
    archive_refused: ?[]const u8 = null,
    local_only: ?[]const u8 = null,
    lanes: Lanes,
    stdin: bool = false,
    read_keys: []const []const u8,
    write: Write,
    write_keys: []const []const u8,
};

pub const table = [_]Format{
    .{
        .tag = .csv,
        .label = "CSV",
        .read_what = "a CSV read",
        .names = &.{"csv"},
        .exts = &.{".csv"},
        .folder_exts = &.{ ".csv", ".tsv", ".txt" },
        .lanes = .local_ranges,
        .stdin = true,
        .read_keys = &.{ "format", "delimiter", "delim", "encoding" },
        .write = .{},
        .write_keys = &.{ "format", "delimiter", "delim" },
    },
    .{
        .tag = .parquet,
        .label = "Parquet",
        .read_what = "a Parquet read",
        .names = &.{"parquet"},
        .exts = &.{".parquet"},
        .folder_exts = &.{".parquet"},
        .compressed_read_refused = "parquet needs random access, so it cannot be read through compression; decompress it first",
        .archive_refused = "parquet needs random access, so it cannot be read out of an archive; extract it first",
        .lanes = .row_groups,
        .read_keys = &.{"format"},
        .write = .{ .gzip_refused = "Parquet compresses its own pages, so it is written without a `.gz`/`.zst` suffix", .append_refused = "a parquet file's footer indexes every row group and is written last, so appending means rewriting the file" },
        .write_keys = &.{ "format", "compression" },
    },
    .{
        .tag = .arrow,
        .label = "Arrow IPC",
        .read_what = "an Arrow IPC read",
        .names = &.{ "arrow", "ipc", "feather" },
        .exts = &.{ ".arrow", ".arrows", ".feather", ".ipc" },
        .compressed_read_refused = "an Arrow IPC file is read memory-mapped, so it cannot be read through compression; decompress it first (IPC compresses its own buffers)",
        .archive_refused = "an Arrow IPC file is read memory-mapped, so it cannot be read out of an archive; extract it first",
        .local_only = "Arrow IPC is read from a local file; fetch it first",
        .lanes = .serial,
        .read_keys = &.{"format"},
        .write = .{ .gzip_refused = "Arrow IPC compresses its own buffers, so it is written without a `.gz`/`.zst` suffix", .append_refused = "an Arrow IPC file's footer indexes every batch and is written last, so appending means rewriting the file" },
        .write_keys = &.{"format"},
    },
    .{
        .tag = .xlsx,
        .label = "Excel",
        .read_what = "an Excel read",
        .names = &.{ "xlsx", "excel" },
        .exts = &.{ ".xlsx", ".xlsm" },
        .compressed_read_refused = "an Excel workbook is a zip archive already, so it is read uncompressed; decompress it first",
        .archive_refused = "an Excel workbook is itself a zip archive, so it cannot be read out of another; extract it first",
        .lanes = .serial,
        .read_keys = &.{ "format", "sheet", "range", "header" },
        .write = .{ .gzip_refused = "an Excel workbook is a zip archive already, so it is written without a `.gz`/`.zst` suffix", .append_refused = "an Excel workbook is a zip archive whose directory is written last, so appending means rewriting the file" },
        .write_keys = &.{ "format", "sheet" },
    },
    .{
        .tag = .json,
        .label = "JSON",
        .read_what = "a JSON read",
        .names = &.{ "json", "ndjson", "jsonl" },
        .exts = &.{ ".json", ".ndjson", ".jsonl" },
        .folder_exts = &.{ ".json", ".ndjson", ".jsonl" },
        .lanes = .serial,
        .stdin = true,
        .read_keys = &.{ "format", "items" },
        .write = .{},
        .write_keys = &.{ "format", "array" },
    },
    .{
        .tag = .xml,
        .label = "XML",
        .read_what = "an XML read",
        .names = &.{"xml"},
        .exts = &.{".xml"},
        .folder_exts = &.{".xml"},
        .lanes = .serial,
        .stdin = true,
        .read_keys = &.{ "format", "row", "encoding" },
        .write = .{ .append_refused = "an XML file's root element closes on its last line, so appending means rewriting the file" },
        .write_keys = &.{ "format", "root", "row" },
    },
};

pub fn of(tag: FileFormat) *const Format {
    return &table[@intFromEnum(tag)];
}

/// The format `format = 'name'` names, case aside; null for none.
pub fn byName(name: []const u8) ?*const Format {
    for (&table) |*f| for (f.names) |n| if (std.ascii.eqlIgnoreCase(n, name)) return f;
    return null;
}

/// The format a path's data name picks by extension, through a codec suffix or an
/// archive member (`orders.csv.gz`, `a.zip :: x.json`), case aside; null for none.
pub fn ofPath(path: []const u8) ?FileFormat {
    const bare = stream.dataName(path);
    for (&table) |*f| for (f.exts) |e| if (std.ascii.endsWithIgnoreCase(bare, e)) return f.tag;
    return null;
}

/// Whether a file in a folder is one of `f`'s, compressed or not.
pub fn inFolder(f: *const Format, name: []const u8) bool {
    const bare = stream.splitCodec(name).rest;
    for (f.folder_exts) |e| if (std.ascii.endsWithIgnoreCase(bare, e)) return true;
    return false;
}

/// Every `format` name, for the message refusing an unknown one.
pub const names_list: []const u8 = blk: {
    var s: []const u8 = "";
    for (table) |f| for (f.names) |n| {
        s = s ++ (if (s.len == 0) "" else ", ") ++ n;
    };
    break :blk s;
};

/// What basalt reads, for the message refusing a file it does not.
pub const handled_list: []const u8 = blk: {
    var s: []const u8 = "";
    for (table, 0..) |f, i| {
        var exts: []const u8 = "";
        for (f.exts) |e| exts = exts ++ (if (exts.len == 0) "" else ", ") ++ "`" ++ e ++ "`";
        const sep = if (i == 0) "" else if (i + 1 == table.len) " and " else ", ";
        s = s ++ sep ++ f.label ++ " (" ++ exts ++ ")";
    }
    var stream_labels: []const u8 = "";
    var n_stream = 0;
    for (table) |f| if (f.compressed_read_refused == null) {
        n_stream += 1;
    };
    var k = 0;
    for (table) |f| if (f.compressed_read_refused == null) {
        k += 1;
        stream_labels = stream_labels ++ (if (k == 1) "" else if (k == n_stream) " or " else ", ") ++ f.label;
    };
    break :blk s ++ ", a " ++ stream_labels ++ " file optionally `.gz`/`.zst` compressed or inside a `.zip`";
};

/// The labels of the formats one predicate admits, joined as prose ("CSV, JSON or XML").
fn labels(comptime pred: fn (Format) bool) []const u8 {
    comptime {
        var n = 0;
        for (table) |f| if (pred(f)) {
            n += 1;
        };
        var s: []const u8 = "";
        var k = 0;
        for (table) |f| if (pred(f)) {
            k += 1;
            s = s ++ (if (k == 1) "" else if (k == n) " or " else ", ") ++ f.label;
        };
        return s;
    }
}

fn readsStdin(f: Format) bool {
    return f.stdin;
}

fn readsFolder(f: Format) bool {
    return f.folder_exts.len > 0;
}

fn gzips(f: Format) bool {
    return f.write.gzip_refused == null;
}

pub const stdin_labels = labels(readsStdin);
pub const folder_labels = labels(readsFolder);
pub const gzip_labels = labels(gzips);

/// Each format's folder extensions as prose, by tag, for the message that finds none.
pub const folder_exts_of: [table.len][]const u8 = blk: {
    var out: [table.len][]const u8 = undefined;
    for (table, &out) |f, *o| {
        var t: []const u8 = "";
        for (f.folder_exts, 0..) |e, i| t = t ++ (if (i == 0) "" else if (i + 1 == f.folder_exts.len) " or " else ", ") ++ e;
        o.* = t;
    }
    break :blk out;
};

/// Every extension a folder is scanned for, for the message that finds none.
pub const folder_exts_list: []const u8 = blk: {
    var all: []const []const u8 = &.{};
    for (table) |f| all = all ++ f.folder_exts;
    var s: []const u8 = "";
    for (all, 0..) |e, i| s = s ++ (if (i == 0) "" else if (i + 1 == all.len) " or " else ", ") ++ e;
    break :blk s;
};

/// Every key some format's read takes, and every key some file write takes.
pub const all_read_keys = union_of(.read);
pub const all_write_keys = union_of(.write);

fn union_of(comptime side: enum { read, write }) []const []const u8 {
    comptime {
        var out: []const []const u8 = &.{};
        for (table) |f| for (if (side == .read) f.read_keys else f.write_keys) |k| {
            const seen = for (out) |o| {
                if (std.mem.eql(u8, o, k)) break true;
            } else false;
            if (!seen) out = out ++ .{k};
        };
        return out;
    }
}

/// The keys of every format stdin reads, for a STDIN read.
pub const stdin_read_keys: []const []const u8 = blk: {
    var out: []const []const u8 = &.{};
    for (table) |f| if (f.stdin) for (f.read_keys) |k| {
        const seen = for (out) |o| {
            if (std.mem.eql(u8, o, k)) break true;
        } else false;
        if (!seen) out = out ++ .{k};
    };
    break :blk out;
};

/// Keys as a message lists them, an alias (`delim`) left out.
pub fn takes(comptime keys: []const []const u8) []const u8 {
    comptime {
        var s: []const u8 = "";
        for (keys) |k| {
            if (std.mem.eql(u8, k, "delim")) continue;
            s = s ++ (if (s.len == 0) "" else ", ") ++ k;
        }
        return s;
    }
}

test "formats: tags index the table, names and extensions resolve, lists are generated" {
    for (table, 0..) |f, i| try std.testing.expectEqual(i, @intFromEnum(f.tag));
    try std.testing.expectEqual(FileFormat.arrow, byName("FEATHER").?.tag);
    try std.testing.expect(byName("yaml") == null);
    try std.testing.expectEqual(FileFormat.parquet, ofPath("a/B.PARQUET").?);
    try std.testing.expectEqual(FileFormat.json, ofPath("x.ndjson.gz").?);
    try std.testing.expectEqual(FileFormat.csv, ofPath("a.zip :: in.csv").?);
    try std.testing.expect(ofPath("x.yaml") == null);
    try std.testing.expect(inFolder(of(.csv), "d/a.tsv.zst"));
    try std.testing.expect(!inFolder(of(.csv), "d/a.json"));
    try std.testing.expectEqualStrings("csv, parquet, arrow, ipc, feather, xlsx, excel, json, ndjson, jsonl, xml", names_list);
    try std.testing.expectEqualStrings("CSV, JSON or XML", stdin_labels);
    try std.testing.expectEqualStrings("CSV, Parquet, JSON or XML", folder_labels);
}
