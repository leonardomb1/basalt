//! What a read or write reaches, decided without connecting: its file format and
//! dialect hints, the problems an unreadable or unwritable target has, archive members,
//! how a read splits across lanes, and a file's schema when it can be read offline.

const Diag = @import("../analyze.zig").Diag;
const Error = @import("../analyze.zig").Error;
const arrowread = @import("../../format/arrowread.zig");
const ast = @import("../../lang/ast.zig");
const csv = @import("../../format/csv.zig");
const fail = @import("../analyze.zig").fail;
const folder = @import("../../connect/folder.zig");
const pqdecode = @import("../../format/parquet/read.zig");
const pqwrite = @import("../../format/parquet/write.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const xlsx = @import("../../format/xlsx.zig");
const zipsrc = @import("../../format/zipsrc.zig");

pub const FileFormat = enum {
    csv,
    parquet,
    arrow,
    xlsx,
};

/// The named format, else the extension's, else CSV. The one place the CSV fast paths
/// ask, so a binary format is never memory-mapped and parsed as text.
pub fn readFormat(path: []const u8, explicit: ?FileFormat) FileFormat {
    return explicit orelse formatOfPath(path) orelse .csv;
}

pub fn hintText(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, key)) continue;
        return switch (h.value) {
            .str => |s| s,
            .ident => |s| s,
            else => null,
        };
    }
    return null;
}

/// Validated here, not at the reader, so `check` rejects a typo before anything opens.
/// The delimiter is one byte; a tab may be spelled out, since SQL's `'\t'` is no escape.
pub fn dialectFromHints(hints: []const ast.Hint, diag: *Diag) Error!csv.Dialect {
    var d = csv.Dialect{};
    if (hintText(hints, "delimiter") orelse hintText(hints, "delim")) |s| {
        const one: ?u8 = if (s.len == 1)
            s[0]
        else if (std.mem.eql(u8, s, "\\t") or std.mem.eql(u8, s, "tab"))
            '\t'
        else
            null;
        d.delim = one orelse return fail(diag, "delimiter must be a single character (or `tab`), got `{s}`", .{s});
        if (d.delim == '"' or d.delim == '\n' or d.delim == '\r')
            return fail(diag, "delimiter cannot be a quote or a newline", .{});
    }
    if (hintText(hints, "encoding")) |s| {
        d.encoding = csv.Encoding.parse(s) orelse
            return fail(diag, "unknown encoding `{s}` (utf8, latin1 / iso-8859-1, cp1252 / windows-1252)", .{s});
    }
    return d;
}

pub fn formatFromHints(hints: []const ast.Hint, diag: *Diag) Error!?FileFormat {
    const s = hintText(hints, "format") orelse return null;
    if (std.ascii.eqlIgnoreCase(s, "csv")) return .csv;
    if (std.ascii.eqlIgnoreCase(s, "parquet")) return .parquet;
    inline for (.{ "arrow", "ipc", "feather" }) |n| if (std.ascii.eqlIgnoreCase(s, n)) return .arrow;
    inline for (.{ "xlsx", "excel" }) |n| if (std.ascii.eqlIgnoreCase(s, n)) return .xlsx;
    return fail(diag, "unknown format `{s}` (csv, parquet, arrow, xlsx)", .{s});
}

/// Validated here so `check` turns away a malformed range before a run.
pub fn xlsxOptions(hints: []const ast.Hint, diag: *Diag) Error!xlsx.Options {
    var o = xlsx.Options{};
    o.sheet = hintText(hints, "sheet");
    if (hintText(hints, "range")) |r| o.range = xlsx.parseRange(r) orelse
        return fail(diag, "`range = '{s}'` is not a cell range like `A1:F100`, `B3` or `B3:F`", .{r});
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "header")) continue;
        o.header = switch (h.value) {
            .flag => true,
            .int => |n| n != 0,
            .str, .ident => |s| if (std.ascii.eqlIgnoreCase(s, "true")) true else if (std.ascii.eqlIgnoreCase(s, "false")) false else return fail(diag, "`header` is true or false, not `{s}`", .{s}),
        };
    }
    return o;
}

/// `csv.dataName` walks the chain first, so `orders.csv.gz` and `inf.zip :: x.csv`
/// both answer `.csv`.
pub fn formatOfPath(path: []const u8) ?FileFormat {
    const bare = csv.dataName(path);
    if (pqwrite.Writer.isPath(bare)) return .parquet;
    if (arrowread.isPath(bare)) return .arrow;
    if (xlsx.isPath(bare)) return .xlsx;
    if (std.ascii.endsWithIgnoreCase(bare, ".csv")) return .csv;
    return null;
}

/// The format actually resolved, not the connector: every bare path is `csv`, which
/// once labelled parquet scans as csv. A malformed hint falls back to the extension.
pub fn formatLabel(path: []const u8, hints: []const ast.Hint) []const u8 {
    var d = Diag{};
    const explicit = formatFromHints(hints, &d) catch null;
    return @tagName(explicit orelse formatOfPath(path) orelse .csv);
}

/// Why `path` cannot be written beyond `unreadableTarget`; a CSV is gzipped for a
/// `.gz` name, nothing else is.
pub fn unwritableTarget(path: []const u8, explicit: ?FileFormat) ?[]const u8 {
    const codec = csv.splitCodec(path).codec;
    if (codec == .none) return null;
    const fmt = explicit orelse formatOfPath(path) orelse .csv;
    if (fmt != .csv) return "Parquet and Arrow compress their own pages, so they are written without a `.gz`/`.zst` suffix";
    if (codec == .zstd) return "basalt compresses CSV output as gzip; name it `.csv.gz`, as zstd output is not supported";
    return null;
}

/// Why `path` cannot be read or written as a table, or null. A trailing `/` is a folder
/// and an archive is `archiveProblem`'s to judge.
pub fn unreadableTarget(path: []const u8, explicit: ?FileFormat) ?[]const u8 {
    if (std.mem.endsWith(u8, path, "/")) return null;

    if (csv.splitArchive(path) != null) return null;

    const fmt = explicit orelse formatOfPath(path);
    if (csv.splitCodec(path).codec != .none and fmt == .parquet)
        return "parquet needs random access, so it cannot be read through compression; decompress it first";
    if (fmt == .xlsx and csv.splitCodec(path).codec != .none)
        return "an Excel workbook is a zip archive already, so it is read uncompressed; decompress it first";
    if (fmt == .arrow) {
        if (csv.splitCodec(path).codec != .none)
            return "an Arrow IPC file is read memory-mapped, so it cannot be read through compression; decompress it first (IPC compresses its own buffers)";
        if (std.mem.indexOf(u8, path, "://") != null)
            return "Arrow IPC is read from a local file; fetch it first";
    }

    if (explicit != null) return null;
    if (fmt != null) return null;
    return "basalt handles `.csv`, `.parquet`, Arrow IPC (`.arrow`, `.feather`, `.ipc`, `.arrows`) and Excel (`.xlsx`, read only), a CSV optionally `.gz`/`.zst` compressed or inside a `.zip`; name the format with `WITH (format = 'csv')` if the extension differs";
}

/// Opens the archive and stays quiet when it cannot (data may not be fetched yet), so
/// a `.json` member is refused like a `.json` file. A remote one is opened only when
/// `online`.
pub fn archiveProblem(arena: std.mem.Allocator, path: []const u8, explicit: ?FileFormat, online: bool) ?[]const u8 {
    const ar = csv.splitArchive(path) orelse return null;
    if (!online and csv.CsvReader.isUrl(ar.archive)) {
        const m = ar.member orelse return null;
        return memberProblem(arena, m, explicit);
    }

    const members = zipsrc.names(arena, ar.archive) catch return null;
    if (members.len == 0) return "the archive holds no files";

    const chosen = if (ar.member) |want| blk: {
        for (members) |m| if (std.mem.eql(u8, m, want)) break :blk m;
        return std.fmt.allocPrint(arena, "no file `{s}` in the archive ({s})", .{ want, joinNames(arena, members) }) catch null;
    } else if (members.len > 1)
        return std.fmt.allocPrint(arena, "the archive holds {d} files; name one with `:: <name>` ({s})", .{ members.len, joinNames(arena, members) }) catch null
    else
        members[0];

    return memberProblem(arena, chosen, explicit);
}

fn memberProblem(arena: std.mem.Allocator, chosen: []const u8, explicit: ?FileFormat) ?[]const u8 {
    if (explicit == null and formatOfPath(chosen) == null)
        return std.fmt.allocPrint(arena, "`{s}` inside it is not a `.csv` or `.parquet`; name the format with `WITH (format = 'csv')`", .{chosen}) catch null;
    if ((explicit orelse formatOfPath(chosen)) == .parquet)
        return "parquet needs random access, so it cannot be read out of an archive; extract it first";
    if ((explicit orelse formatOfPath(chosen)) == .arrow)
        return "an Arrow IPC file is read memory-mapped, so it cannot be read out of an archive; extract it first";
    if ((explicit orelse formatOfPath(chosen)) == .xlsx)
        return "an Excel workbook is itself a zip archive, so it cannot be read out of another; extract it first";
    return null;
}

fn joinNames(arena: std.mem.Allocator, items: []const []const u8) []const u8 {
    var out: []const u8 = "";
    for (items, 0..) |m, i| {
        if (i == 3) return std.fmt.allocPrint(arena, "{s}, …", .{out}) catch out;
        out = std.fmt.allocPrint(arena, "{s}{s}{s}", .{ out, if (i == 0) "" else ", ", m }) catch return out;
    }
    return out;
}

/// Whether a file read's hints still let it fan out: a CSV dialect, or a `format`
/// agreeing with the extension. `lanes.laneEligible` and EXPLAIN both ask this.
pub fn laneHints(st: ast.Stage) bool {
    for (st.hints) |h| {
        if (std.mem.eql(u8, h.key, "delimiter") or std.mem.eql(u8, h.key, "delim") or std.mem.eql(u8, h.key, "encoding")) continue;
        if (std.mem.eql(u8, h.key, "format")) {
            if (st.node != .read or st.node.read.form != .path) return false;
            var d = Diag{};
            const f = (formatFromHints(st.hints, &d) catch return false) orelse return false;
            if (f != readFormat(st.node.read.form.path, null)) return false;
            continue;
        }
        return false;
    }
    return true;
}

/// A parquet is cut into row groups anywhere; a CSV into byte ranges only locally. A
/// compressed stream or archive member has no offset-to-row mapping, matching
/// `MappedCsv.open`'s `NotMappable`; Arrow and workbooks read serially.
pub fn morselParallelRead(connector: []const u8, node: ast.Stage.Node) bool {
    if (!std.mem.eql(u8, connector, "csv")) return false;
    const path = switch (node) {
        .read => |rd| switch (rd.form) {
            .path => |p| p,
            else => return false,
        },
        else => return false,
    };
    if (csv.splitCodec(path).codec != .none or csv.splitArchive(path) != null) return false;
    if (pqwrite.Writer.isPath(path)) return true;
    if (arrowread.isPath(path)) return false;
    if (xlsx.isPath(path)) return false;
    return std.mem.indexOf(u8, path, "://") == null;
}

/// A local CSV header, parquet footer, Parquet folder's first file or workbook's
/// first pass, as the run reads them; everything else stays unresolved.
pub fn offlineSchema(arena: std.mem.Allocator, rd: ast.Read, hints: []const ast.Hint) ?types.Schema {
    if (std.mem.eql(u8, rd.connector, "unit")) return .{ .fields = &.{} };
    if (std.mem.eql(u8, rd.connector, "range")) {
        const fields = arena.alloc(types.Schema.Field, 1) catch return null;
        fields[0] = .{ .name = "range", .ty = .{ .kind = .int } };
        return .{ .fields = fields };
    }
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (csv.CsvReader.isUrl(rd.form.path)) return null;
        if (folder.isFolder(rd.form.path)) {
            var fdiag = Diag{};
            const explicit = formatFromHints(hints, &fdiag) catch return null;
            if (explicit != null and explicit.? != .parquet) return null;
            const all = folder.list(arena, rd.form.path) catch return null;
            if (explicit == null and !std.meta.eql(folder.kindOf(all), folder.Verdict{ .kind = .parquet })) return null;
            const files = folder.only(arena, all, .parquet) catch return null;
            if (files.len == 0) return null;
            const pf = pqdecode.Folder.open(arena, rd.form.path, files, null) catch return null;
            defer pf.close();
            return pf.schema;
        }
        if (csv.splitCodec(rd.form.path).codec == .none and csv.splitArchive(rd.form.path) == null and
            pqdecode.Reader.isPath(rd.form.path))
        {
            const pr = pqdecode.Reader.open(arena, rd.form.path) catch return null;
            return pr.schema;
        }
        var fdiag = Diag{};
        const explicit = formatFromHints(hints, &fdiag) catch return null;
        if (readFormat(rd.form.path, explicit) == .arrow) {
            const ar = arrowread.Reader.open(arena, rd.form.path) catch return null;
            defer ar.close();
            return ar.schema;
        }
        if (readFormat(rd.form.path, explicit) == .xlsx) {
            var odiag = Diag{};
            const opts = xlsxOptions(hints, &odiag) catch return null;
            const xr = xlsx.Reader.open(arena, arena, rd.form.path, opts) catch return null;
            defer xr.close();
            return xr.schema;
        }
        var hdiag = Diag{};
        const d = dialectFromHints(hints, &hdiag) catch return null;
        const reader = csv.CsvReader.open(arena, rd.form.path, d) catch return null;
        const schema = reader.schema;
        reader.close();
        return schema;
    }
    return null;
}

test "unreadableTarget: an extension basalt does not read is refused" {
    try std.testing.expect(unreadableTarget("/data/x.csv", null) == null);
    try std.testing.expect(unreadableTarget("/data/X.CSV", null) == null);
    try std.testing.expect(unreadableTarget("/data/x.parquet", null) == null);
    try std.testing.expect(unreadableTarget("https://h/d.csv?token=abc", null) == null);
    try std.testing.expect(unreadableTarget("s3://bkt/bronze/", null) == null);

    try std.testing.expect(unreadableTarget("/data/x.csv.gz", null) == null);
    try std.testing.expect(unreadableTarget("/data/x.csv.zst", null) == null);
    try std.testing.expect(unreadableTarget("/data/inf.zip", null) == null);
    try std.testing.expect(unreadableTarget("/data/inf.zip :: a.csv", null) == null);
    try std.testing.expect(unreadableTarget("/data/x.parquet.gz", null) != null);

    try std.testing.expect(unreadableTarget("/data/rows.json", null) != null);
    try std.testing.expect(unreadableTarget("/data/book.xlsx", null) == null);
    try std.testing.expect(unreadableTarget("/data/book.xlsx.gz", null) != null);
    try std.testing.expect(unreadableTarget("/data/book.xls", null) != null);
    try std.testing.expect(unreadableTarget("/data/noext", null) != null);
    try std.testing.expect(unreadableTarget("/data/weird.dat", .csv) == null);
}

test "dialectFromHints: parses, and rejects what cannot work" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const mkh = struct {
        fn hint(al: std.mem.Allocator, k: []const u8, v: []const u8) ![]const ast.Hint {
            const h = try al.alloc(ast.Hint, 1);
            h[0] = .{ .key = k, .value = .{ .str = v }, .pos = .{ .line = 1, .col = 1 } };
            return h;
        }
    };

    var d = Diag{};
    try std.testing.expectEqual(@as(u8, ','), (try dialectFromHints(&.{}, &d)).delim);
    try std.testing.expectEqual(@as(u8, ';'), (try dialectFromHints(try mkh.hint(a, "delimiter", ";"), &d)).delim);
    try std.testing.expectEqual(@as(u8, '\t'), (try dialectFromHints(try mkh.hint(a, "delimiter", "tab"), &d)).delim);
    try std.testing.expectEqual(@as(u8, '|'), (try dialectFromHints(try mkh.hint(a, "delim", "|"), &d)).delim);
    try std.testing.expectEqual(csv.Encoding.latin1, (try dialectFromHints(try mkh.hint(a, "encoding", "iso-8859-1"), &d)).encoding);

    try std.testing.expectError(error.AnalyzeFailed, dialectFromHints(try mkh.hint(a, "delimiter", ";;"), &d));
    try std.testing.expectError(error.AnalyzeFailed, dialectFromHints(try mkh.hint(a, "delimiter", "\""), &d));
    try std.testing.expectError(error.AnalyzeFailed, dialectFromHints(try mkh.hint(a, "encoding", "latin9"), &d));
}

test "laneHints: a CSV dialect and an agreeing format fan out; anything else stays serial" {
    const rd = ast.Stage.Node{ .read = .{ .connector = "csv", .form = .{ .path = "x.csv" } } };
    const pos = ast.Pos{ .line = 1, .col = 1 };
    const Case = struct { hints: []const ast.Hint, ok: bool };
    const cases = [_]Case{
        .{ .hints = &.{}, .ok = true },
        .{ .hints = &.{ .{ .key = "delimiter", .value = .{ .str = ";" }, .pos = pos }, .{ .key = "encoding", .value = .{ .str = "latin1" }, .pos = pos } }, .ok = true },
        .{ .hints = &.{.{ .key = "format", .value = .{ .str = "csv" }, .pos = pos }}, .ok = true },
        .{ .hints = &.{.{ .key = "format", .value = .{ .str = "parquet" }, .pos = pos }}, .ok = false },
        .{ .hints = &.{.{ .key = "split", .value = .{ .str = "id" }, .pos = pos }}, .ok = false },
    };
    for (cases) |c| try std.testing.expectEqual(c.ok, laneHints(.{ .node = rd, .hints = c.hints, .pos = pos }));
}

test "formatLabel names the reader, not the connector" {
    const no_hints: []const ast.Hint = &.{};
    try std.testing.expectEqualStrings("parquet", formatLabel("t.parquet", no_hints));
    try std.testing.expectEqualStrings("csv", formatLabel("t.csv", no_hints));
    try std.testing.expectEqualStrings("csv", formatLabel("t.csv.gz", no_hints));
    try std.testing.expectEqualStrings("csv", formatLabel("a.zip :: t.csv", no_hints));
    const as_parquet: []const ast.Hint = &.{.{ .key = "format", .value = .{ .str = "parquet" }, .pos = .{ .line = 1, .col = 1 } }};
    try std.testing.expectEqualStrings("parquet", formatLabel("t.dat", as_parquet));
    try std.testing.expectEqualStrings("csv", formatLabel("t.dat", no_hints));
}
