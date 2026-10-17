//! What a read or write reaches, decided without connecting: its file format and
//! dialect hints, the problems an unreadable or unwritable target has, archive members,
//! how a read splits across lanes, and a file's schema when it can be read offline.

const Diag = @import("../analyze.zig").Diag;
const Error = @import("../analyze.zig").Error;
const arrowread = @import("../../format/arrow/read.zig");
const ast = @import("../../lang/ast.zig");
const pqcodec = @import("../../format/compress/codecs.zig");
const csv = @import("../../format/csv.zig");
const json = @import("../../format/json/read.zig");
const jsonwrite = @import("../../format/json/write.zig");
const xmlread = @import("../../format/xml/read.zig");
const xmlwrite = @import("../../format/xml/write.zig");
const fail = @import("../analyze.zig").fail;
const folder = @import("../../connect/folder.zig");
const ftp = @import("../../store/ftp.zig");
const pqdecode = @import("../../format/parquet/read.zig");
const std = @import("std");
const table_builtins = @import("../../connect/table_builtins.zig");
const replace = @import("../../db/replace.zig");
const types = @import("../../lang/types.zig");
const xlsx = @import("../../format/xlsx/read.zig");
const xlsxwrite = @import("../../format/xlsx/write.zig");
const zipsrc = @import("../../format/compress/zip.zig");
const formats = @import("../../format/formats.zig");

pub const FileFormat = formats.FileFormat;

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
    if (formats.byName(s)) |f| return f.tag;
    return fail(diag, "unknown format `{s}` (" ++ formats.names_list ++ ")", .{s});
}

/// How a JSON read cuts its input into rows: `format = 'ndjson'` (or `'jsonl'`)
/// reads one value per line however it starts, `'json'` and the extension let the
/// first byte decide, and `items` names the array of one document.
pub fn jsonOptions(hints: []const ast.Hint) json.Options {
    var o = json.Options{ .items = hintText(hints, "items") };
    if (hintText(hints, "format")) |f| {
        if (std.ascii.eqlIgnoreCase(f, "ndjson") or std.ascii.eqlIgnoreCase(f, "jsonl")) o.shape = .ndjson;
    }
    return o;
}

/// A Parquet sink's page codec, `compression = 'snappy' | 'gzip' | 'none'`, snappy
/// when unset. Any other target refuses the key: a CSV file takes its codec from
/// a `.gz` suffix.
pub fn parquetCodecFromHints(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!pqcodec.Codec {
    const s = hintText(hints, "compression") orelse return .snappy;
    const file = std.mem.eql(u8, w.connector, "csv") and w.target.len > 0;
    const fmt = if (file) (try formatFromHints(hints, diag)) orelse formatOfPath(w.target) else null;
    if (!file)
        return fail(diag, "`compression` applies to a Parquet file target only; a starrocks or doris load is compressed by the connection's `compression` option", .{});
    if (fmt != .parquet)
        return fail(diag, "`compression` applies to a Parquet file target only; a CSV file is gzipped by a `.gz` suffix", .{});
    if (std.ascii.eqlIgnoreCase(s, "snappy")) return .snappy;
    if (std.ascii.eqlIgnoreCase(s, "gzip")) return .gzip;
    if (std.ascii.eqlIgnoreCase(s, "none")) return .uncompressed;
    return fail(diag, "unknown Parquet compression `{s}` (snappy, gzip, none)", .{s});
}

/// How a REPLACE into a database table clears the target at its cutover,
/// `cutover = 'truncate' | 'delete'`, truncate when unset (see `db/replace.zig`).
/// The key's target kind is judged by `checkSinkForm`; its mode and value here.
pub fn cutoverFromHints(w: ast.Write, hints: []const ast.Hint) Error!?replace.Cutover {
    const s = hintText(hints, "cutover") orelse return null;
    if (w.mode != .overwrite) return error.AnalyzeFailed;
    if (std.ascii.eqlIgnoreCase(s, "truncate")) return .truncate;
    if (std.ascii.eqlIgnoreCase(s, "delete")) return .delete;
    return error.AnalyzeFailed;
}

pub fn cutoverProblem(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!replace.Cutover {
    const s = hintText(hints, "cutover") orelse return .truncate;
    if (w.mode != .overwrite)
        return fail(diag, "`cutover` applies to a REPLACE only: it says how the target is cleared once the new rows are staged", .{});
    return (cutoverFromHints(w, hints) catch null) orelse
        fail(diag, "unknown `cutover = '{s}'` (truncate, delete)", .{s});
}

/// Validated here so `check` turns away a malformed range before a run.
pub fn xlsxOptions(hints: []const ast.Hint, diag: *Diag) Error!xlsx.Options {
    var o = xlsx.Options{};
    o.sheet = hintText(hints, "sheet");
    if (hintText(hints, "range")) |r| o.range = xlsx.parseRange(r) orelse
        return fail(diag, "`range = '{s}'` is not a cell range like `A1:F100`, `B3` or `B3:F`", .{r});
    if (try hintBool(hints, "header", diag)) |b| o.header = b;
    return o;
}

/// A yes-or-no option: bare (`WITH (header)`), a number, or `true`/`false`.
pub fn hintBool(hints: []const ast.Hint, key: []const u8, diag: *Diag) Error!?bool {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, key)) continue;
        return switch (h.value) {
            .flag => true,
            .int => |n| n != 0,
            .str, .ident => |s| if (std.ascii.eqlIgnoreCase(s, "true")) true else if (std.ascii.eqlIgnoreCase(s, "false")) false else fail(diag, "`{s}` is true or false, not `{s}`", .{ key, s }),
        };
    }
    return null;
}

/// A JSON sink's shape, `array = true` for one top-level array, else NDJSON. Any
/// other target refuses the key.
pub fn jsonShapeFromHints(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!jsonwrite.Shape {
    const array = (try hintBool(hints, "array", diag)) orelse return .ndjson;
    const file = std.mem.eql(u8, w.connector, "csv") and w.target.len > 0;
    const fmt = if (file) (try formatFromHints(hints, diag)) orelse formatOfPath(w.target) else null;
    if (fmt != .json)
        return fail(diag, "`array` applies to a JSON file target only: it writes one top-level array instead of a row a line", .{});
    if (array and w.mode == .append)
        return fail(diag, json_array_append_fmt, .{w.target});
    return if (array) .array else .ndjson;
}

/// A file write's options, parsed once for its format; a target that is not a file
/// is `.csv`, its keys still checked by each format's parser so it refuses theirs.
pub const WriteOptions = union(FileFormat) {
    csv: void,
    parquet: pqcodec.Codec,
    arrow: void,
    xlsx: []const u8,
    json: jsonwrite.Shape,
    xml: xmlwrite.Names,
};

pub fn writeOptions(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!WriteOptions {
    const codec = try parquetCodecFromHints(w, hints, diag);
    const shape = try jsonShapeFromHints(w, hints, diag);
    const names = try xmlNamesFromHints(w, hints, diag);
    const sheet = try xlsxSheetFromHints(hints, diag);
    const file = std.mem.eql(u8, w.connector, "csv") and w.target.len > 0;
    const fmt: FileFormat = if (file) (try formatFromHints(hints, diag)) orelse formatOfPath(w.target) orelse .csv else .csv;
    return switch (fmt) {
        .csv => .csv,
        .parquet => .{ .parquet = codec },
        .arrow => .arrow,
        .xlsx => .{ .xlsx = sheet },
        .json => .{ .json = shape },
        .xml => .{ .xml = names },
    };
}

/// An XML sink's root and row element names, `rows` and `row` when unset. Any other
/// target refuses the keys.
pub fn xmlNamesFromHints(w: ast.Write, hints: []const ast.Hint, diag: *Diag) Error!xmlwrite.Names {
    var n = xmlwrite.Names{};
    const root = hintText(hints, "root");
    const row = hintText(hints, "row");
    if (root == null and row == null) return n;
    const file = std.mem.eql(u8, w.connector, "csv") and w.target.len > 0;
    const fmt = if (file) (try formatFromHints(hints, diag)) orelse formatOfPath(w.target) else null;
    if (fmt != .xml)
        return fail(diag, "`root` and `row` apply to an XML file target only: they name its root and row elements", .{});
    if (root) |r| n.root = if (r.len > 0) r else return fail(diag, "`root` names an element, so it cannot be empty", .{});
    if (row) |r| n.row = if (r.len > 0) r else return fail(diag, "`row` names an element, so it cannot be empty", .{});
    return n;
}

pub const json_array_append_fmt = "`APPEND` into `{s}` is not supported: a JSON array closes on its last byte, so appending means rewriting the file. Write NDJSON (no `array`) to append, or use `REPLACE`";

/// An Excel sink's worksheet name, `Sheet1` when unset; validated here so `check`
/// refuses a name Excel would.
pub fn xlsxSheetFromHints(hints: []const ast.Hint, diag: *Diag) Error![]const u8 {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "sheet")) continue;
        const name = switch (h.value) {
            .str, .ident => |s| s,
            else => return fail(diag, "`sheet` is a quoted worksheet name, like `sheet = 'Vendas'`", .{}),
        };
        if (xlsxwrite.sheetNameProblem(name)) |why| return fail(diag, "`sheet = '{s}'`: {s}", .{ name, why });
        return name;
    }
    return "Sheet1";
}

/// An XML read's options: `row` names the row element, `encoding` overrides the
/// declared one; validated here so `check` refuses a bad one before a run.
pub fn xmlOptions(hints: []const ast.Hint, text: []const []const u8, diag: *Diag) Error!xmlread.Options {
    var o = xmlread.Options{ .row = hintText(hints, "row"), .text = text };
    if (hintText(hints, "encoding")) |s| {
        o.encoding = csv.Encoding.parse(s) orelse
            return fail(diag, "unknown encoding `{s}` (utf8, latin1 / iso-8859-1, cp1252 / windows-1252)", .{s});
    }
    return o;
}

/// `csv.dataName` walks the chain first, so `orders.csv.gz` and `inf.zip :: x.csv`
/// both answer `.csv`.
pub fn formatOfPath(path: []const u8) ?FileFormat {
    return formats.ofPath(path);
}

/// The format actually resolved, not the connector: every bare path is `csv`, which
/// once labelled parquet scans as csv. A malformed hint falls back to the extension.
pub fn formatLabel(path: []const u8, hints: []const ast.Hint) []const u8 {
    var d = Diag{};
    const explicit = formatFromHints(hints, &d) catch null;
    return @tagName(explicit orelse formatOfPath(path) orelse .csv);
}

/// Why STDIN cannot be read as `fmt`, or null: stdin is a stream, so it takes only
/// the formats read front to back.
pub fn stdinProblem(fmt: FileFormat) ?[]const u8 {
    if (formats.of(fmt).stdin) return null;
    return "STDIN is read as a stream, so it takes " ++ formats.stdin_labels ++ "; a file needing random access is read from a path";
}

/// Why `path` cannot be written beyond `unreadableTarget`; a CSV is gzipped for a
/// `.gz` name, nothing else is.
pub fn unwritableTarget(path: []const u8, explicit: ?FileFormat) ?[]const u8 {
    if (std.ascii.endsWithIgnoreCase(csv.dataName(path), ".xlsm"))
        return "basalt writes `.xlsx`; a macro-enabled `.xlsm` holds macros it cannot write";
    if (csv.splitArchive(path) != null)
        return "basalt reads `.zip` archives but does not write them; write a `.csv` or `.csv.gz`";
    const codec = csv.splitCodec(path).codec;
    if (codec == .none) return null;
    const fmt = formats.of(explicit orelse formatOfPath(path) orelse .csv);
    if (fmt.write.gzip_refused) |why| return why;
    if (codec == .zstd) return "basalt compresses " ++ formats.gzip_labels ++ " output as gzip; name it `.gz`, as zstd output is not supported";
    return null;
}

/// Why `path` cannot be read or written as a table, or null. A trailing `/` is a folder
/// and an archive is `archiveProblem`'s to judge.
pub fn unreadableTarget(path: []const u8, explicit: ?FileFormat) ?[]const u8 {
    if (std.mem.endsWith(u8, path, "/")) return null;

    if (csv.splitArchive(path) != null) return null;

    const tag = explicit orelse formatOfPath(path) orelse
        return "basalt handles " ++ formats.handled_list ++ "; name the format with `WITH (format = 'csv')` if the extension differs";
    const fmt = formats.of(tag);
    if (csv.splitCodec(path).codec != .none) if (fmt.compressed_read_refused) |why| return why;
    if (std.mem.indexOf(u8, path, "://") != null) if (fmt.local_only) |why| return why;
    return null;
}

/// Opens the archive and stays quiet when it cannot (data may not be fetched yet), so
/// a `.json` member is refused like a `.json` file. A remote one is opened only when
/// `online`.
pub fn archiveProblem(arena: std.mem.Allocator, path: []const u8, explicit: ?FileFormat, online: bool) ?[]const u8 {
    const ar = csv.splitArchive(path) orelse return null;
    if (!online and (csv.CsvReader.isUrl(ar.archive) or ftp.isUrl(ar.archive))) {
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
    const tag = explicit orelse formatOfPath(chosen) orelse
        return std.fmt.allocPrint(arena, "`{s}` inside it has no extension basalt reads; name the format with `WITH (format = 'csv')`", .{chosen}) catch null;
    return formats.of(tag).archive_refused;
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
    if (node == .read and node.read.form == .builtin) return table_builtins.of(node.read.form.builtin).lanes;
    if (!std.mem.eql(u8, connector, "csv")) return false;
    const path = switch (node) {
        .read => |rd| switch (rd.form) {
            .path => |p| p,
            else => return false,
        },
        else => return false,
    };
    if (csv.splitCodec(path).codec != .none or csv.splitArchive(path) != null) return false;
    return switch (formats.of(readFormat(path, null)).lanes) {
        .row_groups => true,
        .local_ranges => std.mem.indexOf(u8, path, "://") == null,
        .serial => false,
    };
}

/// A local CSV header, parquet footer, Parquet folder's first file, workbook's first
/// pass or JSON file's (or JSON folder's first file's) sampled rows, as the run reads
/// them; everything else stays unresolved.
pub fn offlineSchema(arena: std.mem.Allocator, rd: ast.Read, hints: []const ast.Hint) ?types.Schema {
    if (std.mem.eql(u8, rd.connector, "unit")) return .{ .fields = &.{} };
    if (rd.form == .builtin) return .{ .fields = arena.dupe(types.Schema.Field, table_builtins.of(rd.form.builtin).schema.fields) catch return null };
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (csv.CsvReader.isUrl(rd.form.path) or ftp.isUrl(rd.form.path)) return null;
        if (folder.isFolder(rd.form.path)) {
            var fdiag = Diag{};
            const explicit = formatFromHints(hints, &fdiag) catch return null;
            const all = folder.list(arena, rd.form.path) catch return null;
            if (explicit == .json or (explicit == null and std.meta.eql(folder.kindOf(all), folder.Verdict{ .kind = .json }))) {
                const files = folder.only(arena, all, .json) catch return null;
                if (files.len == 0) return null;
                return jsonSchema(arena, files, hints);
            }
            if (explicit == .xml or (explicit == null and std.meta.eql(folder.kindOf(all), folder.Verdict{ .kind = .xml }))) {
                const files = folder.only(arena, all, .xml) catch return null;
                if (files.len == 0) return null;
                return xmlSchema(arena, files, hints, rd.text_cols);
            }
            if (explicit != null and explicit.? != .parquet) return null;
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
            defer pr.close();
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
        if (readFormat(rd.form.path, explicit) == .json) return jsonSchema(arena, &.{rd.form.path}, hints);
        if (readFormat(rd.form.path, explicit) == .xml) return xmlSchema(arena, &.{rd.form.path}, hints, rd.text_cols);
        var hdiag = Diag{};
        var d = dialectFromHints(hints, &hdiag) catch return null;
        d.text = rd.text_cols;
        const reader = csv.CsvReader.open(arena, rd.form.path, d) catch return null;
        const schema = reader.schema;
        reader.close();
        return schema;
    }
    return null;
}

fn xmlSchema(arena: std.mem.Allocator, files: []const []const u8, hints: []const ast.Hint, text: []const []const u8) ?types.Schema {
    var d = Diag{};
    const f = xmlread.Files.open(arena, arena, files, xmlOptions(hints, text, &d) catch return null) catch return null;
    defer f.close();
    return f.reader.schema;
}

fn jsonSchema(arena: std.mem.Allocator, files: []const []const u8, hints: []const ast.Hint) ?types.Schema {
    const f = json.Files.open(arena, arena, files, jsonOptions(hints)) catch return null;
    defer f.close();
    return f.reader.schema;
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

    try std.testing.expect(unreadableTarget("/data/rows.yaml", null) != null);
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
