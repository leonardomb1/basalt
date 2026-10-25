//! Opening a source: local and remote files by format, folders, Arrow, Excel, SQL
//! reads, generated rows and constant sources, with the columns a query reads.

const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const Env = @import("../env.zig").Env;
const SqlDriver = @import("../connect.zig").SqlDriver;
const analyze = @import("../analyze.zig");
const options = @import("../analyze/options.zig");
const arrowread = @import("../../format/arrow/read.zig");
const avroread = @import("../../format/avro/read.zig");
const ast = @import("../../lang/ast.zig");
const azure = @import("../../store/azure.zig");
const connectWhy = @import("dbconfig.zig").connectWhy;
const csv = @import("../../format/csv.zig");
const driver = @import("../../connect/driver.zig");
const env_mod = @import("../env.zig");
const eval = @import("../../exec/eval.zig");
const RunCfg = @import("register.zig").RunCfg;
const folder = @import("../../connect/folder.zig");
const ftp = @import("../../store/ftp.zig");
const gen = @import("../../connect/gen.zig");
const guardFileFormat = @import("sink.zig").guardFileFormat;
const hintedWhere = @import("sql_read.zig").hintedWhere;
const httpAttrUsed = @import("../connect.zig").httpAttrUsed;
const http_client = @import("../../net/http_client.zig");
const pathFail = @import("../env.zig").pathFail;
const aErr = @import("../env.zig").aErr;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const pqdecode = @import("../../format/parquet/read.zig");
const queryParams = @import("../connect.zig").queryParams;
const readReport = @import("../connect.zig").readReport;
const readSql = @import("sql_read.zig").readSql;
const resolveDbConfig = @import("dbconfig.zig").resolveDbConfig;
const s3 = @import("../../store/s3.zig");
const sql = @import("../../db/sql.zig");
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const sqlDescFor = @import("sql_read.zig").sqlDescFor;
const std = @import("std");
const types = @import("../../lang/types.zig");
const table_builtins = @import("../../connect/table_builtins.zig");
const xlsx = @import("../../format/xlsx/read.zig");
const jsonread = @import("../../format/json/read.zig");
const xmlread = @import("../../format/xml/read.zig");
const formats = @import("../../format/formats.zig");
const lake = @import("../../format/lake/table.zig");
const stream = @import("../../format/stream.zig");

pub const ConstSource = struct {
    batch: Batch,
    out: *const types.Schema,
    yielded: bool = false,

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *ConstSource = @ptrCast(@alignCast(ptr));
        return self.out.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        _ = arena;
        const self: *ConstSource = @ptrCast(@alignCast(ptr));
        if (self.yielded) return null;
        self.yielded = true;
        return self.batch;
    }
    fn closeFn(ptr: *anyopaque) void {
        _ = ptr;
    }
    pub const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

/// A read of an `ftp://` path as a read of its local copy, downloaded once per run;
/// any other read unchanged.
pub fn localRead(env: *Env, rd: ast.Read) !ast.Read {
    if (rd.form != .path or !ftp.isUrl(rd.form.path)) return rd;
    var out = rd;
    out.form = .{ .path = ftp.localize(env.arena, rd.form.path) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not download `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) })) };
    return out;
}

pub fn openSource(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
    return openSourceProjected(env, rd, hints, null, &.{});
}

/// Only the parquet and Arrow readers act on `project`. Parquet readers are counted
/// so a top-N bound is pushed only into a pipeline with exactly one of them.
pub fn openSourceProjected(
    env: *Env,
    rd_in: ast.Read,
    hints: []const ast.Hint,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
) !driver.Source {
    var src = try openSourceOf(env, rd_in, hints, project, bounds);
    src.stop = env.stop;
    return src;
}

fn openSourceOf(
    env: *Env,
    rd_in: ast.Read,
    hints: []const ast.Hint,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
) !driver.Source {
    const rd = try localRead(env, rd_in);
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (try resolveFolder(env, rd.form.path, hints)) |fr| if (fr.kind == .parquet) {
            env.forgetFolder(rd.form.path);
            const pf = try openParquetFolder(env, rd.form.path, fr, project);
            pf.bounds = bounds;
            env.pq_readers += 1;
            env.pq_folder = pf;
            return pf.source();
        };
    }
    const is_pq = std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and
        pqdecode.Reader.isPath(rd.form.path);
    if (is_pq) {
        const pr = openParquetFile(env, rd.form.path, project) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        pr.bounds = bounds;
        noteParquet(env, pr);
        env.pq_readers += 1;
        env.pq_reader = pr;
        return pr.source();
    }
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and project != null) {
        var fdiag = analyze.Diag{};
        const want = analyze.formatFromHints(hints, &fdiag) catch null;
        if (analyze.readFormat(rd.form.path, want) == .arrow and analyze.unreadableTarget(rd.form.path, want) == null)
            return openArrow(env, rd.form.path, project);
    }
    return openSourceCols(env, rd, hints, project);
}

/// A Parquet file's reader, over the bytes planning kept open for it when it
/// opened this file and chose not to split it (`Env.pq_held`).
fn openParquetFile(env: *Env, path: []const u8, project: ?[]const []const u8) !*pqdecode.Reader {
    if (env.pq_held) |h| if (std.mem.eql(u8, h.path, path)) {
        env.pq_held = null;
        return pqdecode.Reader.openProjectedOn(env.arena, h.src, path, project);
    };
    return pqdecode.Reader.openProjected(env.arena, path, project);
}

/// A folder's format and files, or null when `path` is not a folder. Without
/// `format` the listing decides; a mix of Parquet and CSV is refused. Remembered
/// until the source opens, since the read is resolved twice and listing is a round trip.
pub fn resolveFolder(env: *Env, path: []const u8, hints: []const ast.Hint) !?FolderRead {
    if (!lake.isTablePath(path)) return null;
    var fdiag = analyze.Diag{};
    const want = analyze.formatFromHints(hints, &fdiag) catch |e| return aErr(env, &fdiag, e);
    const at = analyze.lakeAt(hints, &fdiag) catch |e| return aErr(env, &fdiag, e);
    return resolveFolderFmt(env, path, want, at);
}

/// A folder holding a lake table's log is that table, its live files read through
/// the table's schema, whether `format` names it or not: read as a plain folder it
/// would return the rows its log removed. `format = 'parquet'` reads the files plain.
pub fn resolveFolderFmt(env: *Env, path: []const u8, want: ?analyze.FileFormat, at: lake.At) !?FolderRead {
    if (!lake.isTablePath(path)) return null;
    for (env.folder_memo.items) |m| if (std.mem.eql(u8, m.path, path) and m.want == want and m.at.eql(at)) return m.read;
    const read = try resolveTable(env, path, want, at) orelse try resolvePlain(env, path, want, at);
    try env.folder_memo.append(env.arena, .{ .path = try env.arena.dupe(u8, path), .want = want, .at = at, .read = read });
    return read;
}

fn resolveTable(env: *Env, path: []const u8, want: ?analyze.FileFormat, at: lake.At) !?FolderRead {
    if (want != null and lake.Kind.of(want) == null) return null;
    const found: lake.Found = if (lake.Kind.of(want)) |k| .{ .kind = k } else (lake.detect(env.arena, path) catch |e|
        return listFailed(env, path, e)) orelse return null;
    const kind = found.kind;
    const t = lake.resolve(env.arena, path, kind, at, found.listed) catch |e| {
        if (eval.takeFailure(e)) |why| return planErrT(env.diag, e, try env.arena.dupe(u8, why));
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read {s} table `{s}` ({s})", .{ kind.label(), path, try pathFail(env.arena, path, e) }));
    };
    return .{ .kind = .parquet, .files = try t.layout.paths(env.arena), .table = t };
}

fn listFailed(env: *Env, path: []const u8, e: anyerror) anyerror {
    if (e == azure.Error.AzureEmptyPrefix)
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{path}));
    if (e == s3.Error.S3EmptyPrefix)
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{path}));
    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not list folder `{s}` ({s})", .{ path, try pathFail(env.arena, path, e) }));
}

fn resolvePlain(env: *Env, path: []const u8, want: ?analyze.FileFormat, at: lake.At) !FolderRead {
    if (at.pinned()) {
        const e = lake.notPinnable(path);
        return planErrT(env.diag, e, try env.arena.dupe(u8, eval.takeFailure(e).?));
    }
    const all = folder.list(env.arena, path) catch |e| return listFailed(env, path, e);
    if (all.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no files in folder `{s}`", .{path}));
    const kind: folder.Kind = if (want) |w| (if (formats.of(w).folder_exts.len > 0) w else return planErr(env.diag, try std.fmt.allocPrint(env.arena, "folder `{s}`: a folder reads " ++ formats.folder_labels ++ " files", .{path}))) else switch (folder.kindOf(all)) {
        .kind => |k| k,
        .mixed => |m| return planErr(env.diag, try std.fmt.allocPrint(env.arena, "folder `{s}` holds both {s} (`{s}`) and {s} (`{s}`) files; one table reads one format — name it with WITH (format = '{s}') or '{s}'", .{ path, folder.label(m.a.kind), folder.below(m.a.file, path), folder.label(m.b.kind), folder.below(m.b.file, path), @tagName(m.a.kind), @tagName(m.b.kind) })),
        .empty => unreachable,
        .neither => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no " ++ formats.folder_exts_list ++ " file in folder `{s}`", .{path})),
    };
    const files = try folder.only(env.arena, all, kind);
    if (files.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no {s} file in folder `{s}`", .{ formats.folder_exts_of[@intFromEnum(kind)], path }));
    return .{ .kind = kind, .files = files };
}

pub const FolderRead = env_mod.FolderRead;

fn openParquetFolder(env: *Env, path: []const u8, fr: FolderRead, project: ?[][]const u8) !*pqdecode.Folder {
    if (fr.table) |t| {
        const pf = try pqdecode.Folder.openLayout(env.arena, t.layout, project);
        pf.tally = env.scan;
        return pf;
    }
    const files = fr.files;
    const pf = pqdecode.Folder.open(env.arena, path, files, project) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` in folder `{s}` ({s})", .{ files[0], path, try pathFail(env.arena, files[0], e) }));
    if (pf.firstReader()) |first| {
        noteParquet(env, first);
        pf.tally = first.tally;
    }
    return pf;
}

pub fn noteParquet(env: *Env, pr: *pqdecode.Reader) void {
    const t = env.scan orelse return;
    pr.tally = t;
    driver.ScanTally.add(&t.columns_read, pr.leaves.len);
    driver.ScanTally.add(&t.columns_total, pr.md.leafCount());
}

fn openArrow(env: *Env, path: []const u8, project: ?[][]const u8) !driver.Source {
    const r = arrowread.Reader.openProjected(env.arena, path, project) catch |e| {
        const why = switch (e) {
            error.UnsupportedArrow => if (eval.takeFailure(e)) |w|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read {s}", .{w}))
            else
                "an Arrow type or feature basalt does not read",
            error.NotArrow => "not an Arrow IPC file or stream",
            error.CorruptArrow => "the file is truncated or corrupt",
            else => try pathFail(env.arena, path, e),
        };
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read Arrow IPC `{s}` ({s})", .{ path, why }));
    };
    if (env.scan) |t| {
        driver.ScanTally.add(&t.columns_read, r.keep.len);
        driver.ScanTally.add(&t.columns_total, r.fields.len);
    }
    return r.source();
}

fn openXlsx(env: *Env, path: []const u8, hints: []const ast.Hint) !driver.Source {
    var odiag = analyze.Diag{};
    const opts = analyze.xlsxOptions(hints, &odiag) catch |e| return aErr(env, &odiag, e);
    const r = xlsx.Reader.open(env.arena, env.gpa, path, opts) catch |e| {
        const why = switch (e) {
            error.XlsxSheetNotFound => blk: {
                const names = xlsx.sheetNames(env.arena, env.gpa, path) catch &.{};
                const list = try std.mem.join(env.arena, ", ", names);
                break :blk if (opts.sheet) |w|
                    try std.fmt.allocPrint(env.arena, "no sheet `{s}` (sheets: {s})", .{ w, list })
                else
                    "the workbook has no worksheet";
            },
            error.NotXlsx, error.ZipNoEndRecord, error.ZipMemberNotFound => "not an Excel workbook (.xlsx)",
            error.BadXml, error.XmlTokenTooLong => "the workbook's XML is malformed",
            error.XlsxTooManyStrings => "its shared strings pass 512 MB",
            else => try pathFail(env.arena, path, e),
        };
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read Excel workbook `{s}` ({s})", .{ path, why }));
    };
    return r.source();
}

/// JSON or NDJSON files as one table, `path` naming them in messages.
fn openJson(env: *Env, path: []const u8, files: []const []const u8, hints: []const ast.Hint) !driver.Source {
    const f = jsonread.Files.open(env.arena, env.gpa, files, analyze.jsonOptions(hints)) catch |e| {
        if (e == error.InvalidJson)
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "`{s}` is not valid JSON or NDJSON in its first rows", .{path}));
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read JSON `{s}` ({s})", .{ path, try pathFail(env.arena, path, e) }));
    };
    return f.source();
}

/// Avro files as one table, `path` naming them in messages.
fn openAvro(env: *Env, path: []const u8, files: []const []const u8) !driver.Source {
    const f = avroread.Files.open(env.arena, env.gpa, files, .{}) catch |e| return avroOpenFailed(env, path, e);
    return f.source();
}

fn avroOpenFailed(env: *Env, path: []const u8, e: anyerror) anyerror {
    const why = switch (e) {
        error.NotAvro => "not an Avro object container file",
        error.CorruptAvro, error.UnsupportedAvro, error.BadAvroSchema => eval.takeFailure(e) orelse @errorName(e),
        else => try pathFail(env.arena, path, e),
    };
    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read Avro `{s}` ({s})", .{ path, why }));
}

/// XML files as one table, `path` naming them in messages.
fn openXml(env: *Env, path: []const u8, files: []const []const u8, hints: []const ast.Hint, text: []const []const u8) !driver.Source {
    var odiag = analyze.Diag{};
    const opts = analyze.xmlOptions(hints, text, &odiag) catch |e| return aErr(env, &odiag, e);
    const f = xmlread.Files.open(env.arena, env.gpa, files, opts) catch |e| return xmlOpenFailed(env, path, e);
    return f.source();
}

fn xmlOpenFailed(env: *Env, path: []const u8, e: anyerror) anyerror {
    const told = eval.takeFailure(e);
    if (told) |t| if (std.mem.startsWith(u8, t, path) and std.mem.startsWith(u8, t[path.len..], ": "))
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read XML `{s}`: {s}", .{ path, t[path.len + 2 ..] }));
    const why = switch (e) {
        error.BadXml, error.XmlTokenTooLong => told orelse "it is not well-formed XML in its first rows",
        error.XmlUnsupportedEncoding, error.TextTypeMismatch => told orelse @errorName(e),
        else => try pathFail(env.arena, path, e),
    };
    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read XML `{s}` ({s})", .{ path, why }));
}

/// The process's stdin, as CSV unless `format` names JSON or NDJSON. Refused when
/// stdin is already spoken for: the script came from it, or a REPL or kernel reads it.
fn openStdin(env: *Env, hints: []const ast.Hint, project: ?[][]const u8) !driver.Source {
    if (!env.stdin_free)
        return planErr(env.diag, "FROM STDIN needs stdin free for data, but here it carries the script (`basalt run -`), a REPL's input or a kernel's requests; pass the script as a file or with -c");
    var fdiag = analyze.Diag{};
    const want = analyze.formatFromHints(hints, &fdiag) catch |e| return aErr(env, &fdiag, e);
    if (want) |w| if (analyze.stdinProblem(w)) |why| return planErr(env.diag, why);
    const s = try stream.Stream.openStdin(env.arena);
    if (want == .json) {
        const f = jsonread.Files.on(env.arena, env.gpa, s, &.{}, analyze.jsonOptions(hints)) catch |e| {
            if (e == error.InvalidJson) return planErrT(env.diag, e, "stdin is not valid JSON or NDJSON in its first rows");
            return e;
        };
        return f.source();
    }
    if (want == .xml) {
        var odiag = analyze.Diag{};
        const opts = analyze.xmlOptions(hints, &.{}, &odiag) catch |e| return aErr(env, &odiag, e);
        const f = xmlread.Files.on(env.arena, env.gpa, s, &.{}, opts) catch |e| return xmlOpenFailed(env, "stdin", e);
        return f.source();
    }
    if (want == .avro) {
        const f = avroread.Files.on(env.arena, env.gpa, s, &.{}, .{}) catch |e| return avroOpenFailed(env, "stdin", e);
        return f.source();
    }
    var ddiag = analyze.Diag{};
    const d = analyze.dialectFromHints(hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
    const r = csv.CsvReader.openStream(env.arena, s, d) catch |e| {
        if (e == error.EmptyCsv) return planErrT(env.diag, e, "stdin is empty: a CSV read needs at least a header line");
        return e;
    };
    if (project) |p| try r.project(p);
    return r.source();
}

/// A source opened for `rd`, a CSV converting only the columns `project` names. A union's
/// branches already carry its `where` hint, so it is not folded in twice.
fn openSourceCols(env: *Env, rd: ast.Read, hints: []const ast.Hint, project: ?[][]const u8) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "read http needs a quoted URL");
        var hopts = http_client.optsFromHints(hints);
        hopts.logger = env.log;
        const s = http_client.HttpSource.open(env.arena, env.gpa, rd.form.path, hopts) catch |e|
            return httpOpenFailed(env, e, rd.form.path);
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "unit")) {
        const s = gen.UnitSource.open(env.gpa) catch return error.OutOfMemory;
        return s.source();
    }
    if (std.mem.eql(u8, rd.connector, "stdin")) return openStdin(env, hints, project);
    if (rd.form == .builtin) return openTableBuiltin(env, rd.form.builtin);
    if (std.mem.eql(u8, rd.connector, "csv")) {
        if (rd.form != .path) return planErr(env.diag, "read csv needs a quoted path");
        var fdiag = analyze.Diag{};
        const want = analyze.formatFromHints(hints, &fdiag) catch |e| return aErr(env, &fdiag, e);
        try guardFileFormat(env, rd.form.path, want, "read");
        const fr = try resolveFolder(env, rd.form.path, hints);
        env.forgetFolder(rd.form.path);
        if (fr) |f| switch (f.kind) {
            .parquet => return (try openParquetFolder(env, rd.form.path, f, null)).source(),
            .json => return openJson(env, rd.form.path, f.files, hints),
            .xml => return openXml(env, rd.form.path, f.files, hints, rd.text_cols),
            .avro => return openAvro(env, rd.form.path, f.files),
            .csv => {},
            .arrow, .xlsx, .records, .delta => unreachable,
        };
        switch (analyze.readFormat(rd.form.path, want)) {
            .parquet => {
                const pr = openParquetFile(env, rd.form.path, null) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
                noteParquet(env, pr);
                return pr.source();
            },
            .arrow => return openArrow(env, rd.form.path, null),
            .xlsx => return openXlsx(env, rd.form.path, hints),
            .json => return openJson(env, rd.form.path, &.{rd.form.path}, hints),
            .xml => return openXml(env, rd.form.path, &.{rd.form.path}, hints, rd.text_cols),
            .avro => return openAvro(env, rd.form.path, &.{rd.form.path}),
            .records, .delta => unreachable,
            .csv => {},
        }
        var ddiag = analyze.Diag{};
        var d = analyze.dialectFromHints(hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
        d.text = rd.text_cols;
        if (analyze.archiveProblem(env.arena, rd.form.path, want, true)) |why|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot read `{s}`: {s}", .{ rd.form.path, why }));
        const opened = if (fr) |f| csv.CsvReader.openList(env.arena, f.files, d) else csv.CsvReader.open(env.arena, rd.form.path, d);
        const reader = opened catch |e| {
            if (e == azure.Error.AzureEmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{rd.form.path}));
            if (e == error.NoCsvInFolder)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no .csv, .tsv or .txt file in folder `{s}`", .{rd.form.path}));
            if (e == error.EmptyFolder)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no files in folder `{s}`", .{rd.form.path}));
            if (e == s3.Error.S3EmptyPrefix)
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{rd.form.path}));
            const what: []const u8 = if (csv.splitArchive(rd.form.path) != null) "archive" else "input CSV";
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open {s} `{s}` ({s})", .{ what, rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        };
        if (project) |p| try reader.project(p);
        return reader.source();
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try options.unknownConnectionMsg(env.arena, rd.connector, "read"));
    var rd_eff = rd;
    rd_eff.where = try hintedWhere(env.arena, rd, hints);
    if (rd_eff.where.len > 0 and !std.mem.eql(u8, conn.connector, "http"))
        if (env.scan) |t| driver.ScanTally.add(&t.sql_filters, 1);
    if (std.mem.eql(u8, conn.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "reading an http connection needs a path: conn.GET('/path') or a CREATE RESOURCE");
        const cfg = RunCfg{ .env = env, .connector = conn.connector };
        var auth: []const u8 = "";
        for (conn.config) |attr| {
            if (std.mem.eql(u8, attr.key, "auth")) auth = try cfg.text(attr.value);
        }
        var kvs = std.array_list.Managed(http_client.KV).init(env.arena);
        for (conn.config) |attr| {
            if (!httpAttrUsed(conn, auth, attr.key)) continue;
            try kvs.append(.{ .key = attr.key, .value = try cfg.text(attr.value) });
        }
        var errmsg: []const u8 = "";
        const cc = http_client.connFromKvs(env.arena, kvs.items, &errmsg) catch
            return planErr(env.diag, try options.httpConnMsg(env.arena, rd.connector, errmsg));
        const all = try std.mem.concat(env.arena, ast.Hint, &.{ conn.hints, hints });
        var hopts = http_client.optsFromHints(all);
        hopts.logger = env.log;
        const path = try http_client.withQuery(env.arena, rd.form.path, try queryParams(env.arena, all));
        if (env.profile) |p| try p.noteSent("{s}: {s}", .{ rd.connector, path });
        const s = http_client.HttpSource.openConn(env.arena, env.gpa, cc, path, hopts) catch |e|
            return httpOpenFailed(env, e, path);
        return s.source();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        const query = try readSql(env, rd_eff);
        env.log.log(.debug, "sql read ({s}): {s}", .{ rd.connector, query });
        if (env.profile) |p| try p.noteSent("{s}: {s}", .{ rd.connector, query });
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, conn.connector, e) }));
                const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
                    defer c.close();
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} read failed ({s}): {s}", .{ conn.connector, @errorName(e), c.wire.last_error }));
                };
                s.report = try readReport(env, conn.connector);
                env.sql_desc = try sqlDescFor(env, info.kind, info.dialect, cfg, query, rd_eff);
                return s.source();
            },
        }
    }
    return planErr(env.diag, try options.unsupportedSourceMsg(env.arena, conn.connector));
}

/// A failed first fetch as a plan error naming the URL and status; a cancel stays
/// `Aborted` so the run reports it as cancelled, not failed.
fn httpOpenFailed(env: *Env, e: anyerror, url: []const u8) anyerror {
    if (e == error.Aborted) return e;
    const msg = if (eval.takeFailure(e)) |why|
        try std.fmt.allocPrint(env.arena, "http read failed: {s} ({s})", .{ why, @errorName(e) })
    else
        try std.fmt.allocPrint(env.arena, "http read failed for `{s}` ({s})", .{ url, @errorName(e) });
    return planErrT(env.diag, e, msg);
}

/// A table builtin opened on its arguments, params substituted and each folded to
/// its value; a path it cannot open is named with why.
/// A builtin read's arguments, params substituted and folded to their kinds.
pub fn builtinArgs(env: *Env, call: ast.BuiltinRead) ![]Value {
    const b = table_builtins.of(call);
    const vals = try env.arena.alloc(Value, call.args.len);
    for (call.args, b.args[0..call.args.len], vals) |e, kind, *v| {
        const r = try analyze.substExpr(env.arena, e, env.params_expr);
        v.* = try table_builtins.argValue(env.arena, kind, r) orelse return planErr(env.diag, b.arg_msg);
    }
    return vals;
}

fn openTableBuiltin(env: *Env, call: ast.BuiltinRead) !driver.Source {
    const b = table_builtins.of(call);
    const vals = try builtinArgs(env, call);
    return b.open(env.arena, env.gpa, vals) catch |e| {
        if (e == error.OutOfMemory) return e;
        const path = table_builtins.pathArg(b, vals) orelse return e;
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s}: could not open `{s}` ({s})", .{ b.label, path, try pathFail(env.arena, path, e) }));
    };
}
