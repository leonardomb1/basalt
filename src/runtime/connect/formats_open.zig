//! How each file format is opened, the runtime half of `format/formats.zig`: a read
//! of a path or a folder's files, a read of stdin, a write, and a write's options
//! from its hints, each with the format's own messages. One namespace per format
//! builds its entry at compile time, a missing one fails the build, and the entries
//! are checked against the metadata table, so `source.zig`, `sink.zig` and
//! `targets.zig` look a format up instead of switching on it.

const std = @import("std");
const Env = @import("../env.zig").Env;
const FolderRead = @import("../env.zig").FolderRead;
const aErr = @import("../env.zig").aErr;
const pathFail = @import("../env.zig").pathFail;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const analyze = @import("../analyze.zig");
const targets = @import("../analyze/targets.zig");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const driver = @import("../../exec/driver.zig");
const eval = @import("../../exec/eval.zig");
const obs = @import("../../exec/obs.zig");
const azure = @import("../../store/azure.zig");
const s3 = @import("../../store/s3.zig");
const formats = @import("../../format/formats.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const stream = @import("../../format/stream.zig");
const csvfmt = @import("../../format/csv.zig");
const pqdecode = @import("../../format/parquet/read.zig");
const pqwrite = @import("../../format/parquet/write.zig");
const arrowread = @import("../../format/arrow/read.zig");
const arrowwrite = @import("../../format/arrow/write.zig");
const xlsxread = @import("../../format/xlsx/read.zig");
const xlsxwrite = @import("../../format/xlsx/write.zig");
const jsonread = @import("../../format/json/read.zig");
const jsonwrite = @import("../../format/json/write.zig");
const xmlread = @import("../../format/xml/read.zig");
const xmlwrite = @import("../../format/xml/write.zig");
const avroread = @import("../../format/avro/read.zig");
const avrowrite = @import("../../format/avro/write.zig");
const recordswrite = @import("../../format/records/write.zig");
const delta_write = @import("../../format/lake/delta_write.zig");

pub const FileFormat = formats.FileFormat;

/// A read of `path`: `folder` when a folder's listing chose this format, its files
/// read as one table; `want` the format `format` names.
pub const Read = struct {
    path: []const u8,
    folder: ?FolderRead,
    want: ?FileFormat,
    hints: []const ast.Hint,
    text: []const []const u8,
    project: ?[][]const u8,
};

pub const ReadFn = *const fn (env: *Env, r: Read) anyerror!driver.Source;
pub const StdinFn = *const fn (env: *Env, s: *stream.Stream, hints: []const ast.Hint, project: ?[][]const u8) anyerror!driver.Source;
pub const WriteFn = *const fn (env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink;
pub const OptionsFn = *const fn (in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions;

/// One format's openers; `read` is null for a format no path is read as, `stdin`
/// for one stdin cannot carry. A write takes its options from `env.write_opts`.
pub const Open = struct {
    read: ?ReadFn,
    stdin: ?StdinFn,
    write: WriteFn,
    options: OptionsFn,
};

pub const table: [formats.table.len]Open = blk: {
    var t: [formats.table.len]Open = undefined;
    for (formats.table) |f| {
        const o = entry(@field(impls, @tagName(f.tag)));
        if ((o.read != null) != (formats.readable(f) and f.exts.len > 0))
            @compileError("formats_open: " ++ @tagName(f.tag) ++ " has a `read` exactly when a path is read as it");
        if ((o.stdin != null) != (formats.readable(f) and f.stdin))
            @compileError("formats_open: " ++ @tagName(f.tag) ++ " has a `stdin` exactly when formats.zig says stdin carries it");
        t[@intFromEnum(f.tag)] = o;
    }
    break :blk t;
};

pub fn of(tag: FileFormat) *const Open {
    return &table[@intFromEnum(tag)];
}

fn entry(comptime N: type) Open {
    return .{
        .read = if (@hasDecl(N, "read")) &N.read else null,
        .stdin = if (@hasDecl(N, "stdin")) &N.stdin else null,
        .write = &N.write,
        .options = &N.options,
    };
}

/// A read's files: the folder's, else its one path.
fn filesOf(r: *const Read) []const []const u8 {
    return if (r.folder) |f| f.files else (&r.path)[0..1];
}

const impls = struct {
    pub const csv = struct {
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            var ddiag = analyze.Diag{};
            var d = analyze.dialectFromHints(r.hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
            d.text = r.text;
            if (analyze.archiveProblem(env.arena, r.path, r.want, true)) |why|
                return planErr(env.diag, try msgfmt.allocPrint(env.arena, "cannot read `{s}`: {s}", .{ r.path, why }));
            const opened = if (r.folder) |f| csvfmt.CsvReader.openList(env.arena, f.files, d) else csvfmt.CsvReader.open(env.arena, r.path, d);
            const reader = opened catch |e| {
                if (e == azure.Error.AzureEmptyPrefix)
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{r.path}));
                if (e == error.NoCsvInFolder)
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "no .csv, .tsv or .txt file in folder `{s}`", .{r.path}));
                if (e == error.EmptyFolder)
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "no files in folder `{s}`", .{r.path}));
                if (e == s3.Error.S3EmptyPrefix)
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{r.path}));
                const what: []const u8 = if (csvfmt.splitArchive(r.path) != null) "archive" else "input CSV";
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open {s} `{s}` ({s})", .{ what, r.path, try pathFail(env.arena, r.path, e) }));
            };
            if (r.project) |p| try reader.project(p);
            return reader.source();
        }

        pub fn stdin(env: *Env, s: *stream.Stream, hints: []const ast.Hint, project: ?[][]const u8) anyerror!driver.Source {
            var ddiag = analyze.Diag{};
            const d = analyze.dialectFromHints(hints, &ddiag) catch |e| return aErr(env, &ddiag, e);
            const r = csvfmt.CsvReader.openStream(env.arena, s, d) catch |e| {
                if (e == error.EmptyCsv) return planErrT(env.diag, e, "stdin is empty: a CSV read needs at least a header line");
                return e;
            };
            if (project) |p| try r.project(p);
            return r.source();
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            const writer = csvfmt.CsvWriter.open(env.arena, w.target, schema, mode, env.csv_out) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output CSV `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return writer.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            _ = in;
            return .csv;
        }
    };

    pub const parquet = struct {
        /// Unprojected: a projected Parquet read, with its bounds and reader count,
        /// opens in `source.openSourceOf`.
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            if (r.folder) |f| return (try openParquetFolder(env, r.path, f, null)).source();
            const pr = openParquetFile(env, r.path, null) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ r.path, try pathFail(env.arena, r.path, e) }));
            noteParquet(env, pr);
            return pr.source();
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            const pw = pqwrite.Writer.open(env.arena, w.target, schema, env.write_opts.parquet, mode) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output parquet `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return pw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .parquet = in.codec };
        }
    };

    pub const arrow = struct {
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            return openArrow(env, r.path, r.project);
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            _ = mode;
            const aw = arrowwrite.FileSink.open(env.gpa, env.arena, w.target, schema) catch |e| switch (e) {
                error.ArrowUnsupportedType => return planErr(env.diag, "Arrow IPC cannot carry a list, map or struct column"),
                else => return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output Arrow IPC `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) })),
            };
            return aw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            _ = in;
            return .arrow;
        }
    };

    pub const xlsx = struct {
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            var odiag = analyze.Diag{};
            const opts = analyze.xlsxOptions(r.hints, &odiag) catch |e| return aErr(env, &odiag, e);
            const x = xlsxread.Reader.open(env.arena, env.gpa, r.path, opts) catch |e| {
                const why = switch (e) {
                    error.XlsxSheetNotFound => blk: {
                        const names = xlsxread.sheetNames(env.arena, env.gpa, r.path) catch &.{};
                        const list = try std.mem.join(env.arena, ", ", names);
                        break :blk if (opts.sheet) |s|
                            try msgfmt.allocPrint(env.arena, "no sheet `{s}` (sheets: {s})", .{ s, list })
                        else
                            "the workbook has no worksheet";
                    },
                    error.NotXlsx, error.ZipNoEndRecord, error.ZipMemberNotFound => "not an Excel workbook (.xlsx)",
                    error.BadXml, error.XmlTokenTooLong => "the workbook's XML is malformed",
                    error.XlsxTooManyStrings => "its shared strings pass 512 MB",
                    else => try pathFail(env.arena, r.path, e),
                };
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read Excel workbook `{s}` ({s})", .{ r.path, why }));
            };
            return x.source();
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            _ = mode;
            const xw = xlsxwrite.Writer.open(env.arena, w.target, schema, env.write_opts.xlsx) catch |e| switch (e) {
                error.XlsxTooManyColumns, error.XlsxNestedColumn, error.XlsxBadSheetName => return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "cannot write `{s}`: {s}", .{ w.target, eval.takeFailure(e) orelse @errorName(e) })),
                else => return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output Excel workbook `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) })),
            };
            return xw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .xlsx = in.sheet };
        }
    };

    pub const json = struct {
        /// JSON or NDJSON files as one table, `path` naming them in messages.
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            const f = jsonread.Files.open(env.arena, env.gpa, filesOf(&r), analyze.jsonOptions(r.hints)) catch |e| {
                if (e == error.InvalidJson)
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "`{s}` is not valid JSON or NDJSON in its first rows", .{r.path}));
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read JSON `{s}` ({s})", .{ r.path, try pathFail(env.arena, r.path, e) }));
            };
            return f.source();
        }

        pub fn stdin(env: *Env, s: *stream.Stream, hints: []const ast.Hint, project: ?[][]const u8) anyerror!driver.Source {
            _ = project;
            const f = jsonread.Files.on(env.arena, env.gpa, s, &.{}, analyze.jsonOptions(hints)) catch |e| {
                if (e == error.InvalidJson) return planErrT(env.diag, e, "stdin is not valid JSON or NDJSON in its first rows");
                return e;
            };
            return f.source();
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            const jw = jsonwrite.Writer.open(env.arena, w.target, schema, mode, env.write_opts.json) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output JSON `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return jw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .json = in.shape };
        }
    };

    pub const xml = struct {
        /// XML files as one table, `path` naming them in messages.
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            var odiag = analyze.Diag{};
            const opts = analyze.xmlOptions(r.hints, r.text, &odiag) catch |e| return aErr(env, &odiag, e);
            const f = xmlread.Files.open(env.arena, env.gpa, filesOf(&r), opts) catch |e| return xmlOpenFailed(env, r.path, e);
            return f.source();
        }

        pub fn stdin(env: *Env, s: *stream.Stream, hints: []const ast.Hint, project: ?[][]const u8) anyerror!driver.Source {
            _ = project;
            var odiag = analyze.Diag{};
            const opts = analyze.xmlOptions(hints, &.{}, &odiag) catch |e| return aErr(env, &odiag, e);
            const f = xmlread.Files.on(env.arena, env.gpa, s, &.{}, opts) catch |e| return xmlOpenFailed(env, "stdin", e);
            return f.source();
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            _ = mode;
            const xw = xmlwrite.Writer.open(env.arena, w.target, schema, env.write_opts.xml) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output XML `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return xw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .xml = in.names };
        }
    };

    pub const records = struct {
        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            _ = schema;
            const rw = recordswrite.Writer.open(env.arena, w.target, mode, env.write_opts.records) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output records file `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return rw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .records = try targets.recordsOptions(in.hints, in.diag) };
        }
    };

    pub const delta = struct {
        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            _ = mode;
            const o = env.write_opts.delta;
            if (w.mode == .upsert and w.mode.upsert.keys.len == 0)
                return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`UPSERT` into Delta table `{s}` has no key; name it with `UPSERT ON (col)`", .{w.target}));
            const p = delta_write.planEvolving(env.arena, w.target, delta_write.modeOf(w.mode), env.fmt_out == .delta, o.partition_by, o.codec, schema, o.evolve) catch |e|
                return planErrT(env.diag, e, try pathFail(env.arena, w.target, e));
            const ds = delta_write.Sink.open(env.gpa, env.arena, p, .{ .space = env.space, .op_memory = env.op_memory, .threads = env.pipe.threads, .err = env.errctx, .warn = .{ .ctx = env.log, .f = warnLog } }) catch |e|
                return planErrT(env.diag, e, try pathFail(env.arena, w.target, e));
            return ds.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .delta = .{ .codec = in.codec, .partition_by = try targets.partitionBy(in.arena, in.hints, in.diag), .evolve = try targets.evolveOf(in.hints, in.diag) } };
        }
    };

    pub const avro = struct {
        /// Avro files as one table, `path` naming them in messages.
        pub fn read(env: *Env, r: Read) anyerror!driver.Source {
            const f = avroread.Files.open(env.arena, env.gpa, filesOf(&r), .{}) catch |e| return avroOpenFailed(env, r.path, e);
            return f.source();
        }

        pub fn stdin(env: *Env, s: *stream.Stream, hints: []const ast.Hint, project: ?[][]const u8) anyerror!driver.Source {
            _ = hints;
            _ = project;
            const f = avroread.Files.on(env.arena, env.gpa, s, &.{}, .{}) catch |e| return avroOpenFailed(env, "stdin", e);
            return f.source();
        }

        pub fn write(env: *Env, w: ast.Write, schema: types.Schema, mode: driver.FileMode) anyerror!driver.Sink {
            _ = mode;
            const aw = avrowrite.Writer.open(env.arena, w.target, schema, env.write_opts.avro) catch |e| switch (e) {
                error.AvroUnsupportedType, error.AvroBadName, error.AvroCodecUnsupported => return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "cannot write `{s}`: {s}", .{ w.target, eval.takeFailure(e) orelse @errorName(e) })),
                else => return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not open output Avro `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) })),
            };
            return aw.sink();
        }

        pub fn options(in: targets.WriteOptionsIn) analyze.Error!analyze.WriteOptions {
            return .{ .avro = try targets.avroCodecFromHints(in.hints, in.diag) };
        }
    };
};

/// A Parquet file's reader, over the bytes planning kept open for it when it
/// opened this file and chose not to split it (`Env.pq_held`).
pub fn openParquetFile(env: *Env, path: []const u8, project: ?[]const []const u8) !*pqdecode.Reader {
    if (env.pipe.pq_held) |h| if (std.mem.eql(u8, h.path, path)) {
        env.pipe.pq_held = null;
        return pqdecode.Reader.openProjectedOn(env.arena, h.src, path, project);
    };
    return pqdecode.Reader.openProjected(env.arena, path, project);
}

pub fn openParquetFolder(env: *Env, path: []const u8, fr: FolderRead, project: ?[][]const u8) !*pqdecode.Folder {
    if (fr.table) |t| {
        const pf = try pqdecode.Folder.openLayout(env.arena, t.layout, project);
        pf.tally = env.scan;
        return pf;
    }
    const files = fr.files;
    const pf = pqdecode.Folder.open(env.arena, path, files, project) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read parquet `{s}` in folder `{s}` ({s})", .{ files[0], path, try pathFail(env.arena, files[0], e) }));
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

pub fn openArrow(env: *Env, path: []const u8, project: ?[][]const u8) !driver.Source {
    const r = arrowread.Reader.openProjected(env.arena, path, project) catch |e| {
        const why = switch (e) {
            error.UnsupportedArrow => if (eval.takeFailure(e)) |w|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read {s}", .{w}))
            else
                "an Arrow type or feature basalt does not read",
            error.NotArrow => "not an Arrow IPC file or stream",
            error.CorruptArrow => "the file is truncated or corrupt",
            else => try pathFail(env.arena, path, e),
        };
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read Arrow IPC `{s}` ({s})", .{ path, why }));
    };
    if (env.scan) |t| {
        driver.ScanTally.add(&t.columns_read, r.keep.len);
        driver.ScanTally.add(&t.columns_total, r.fields.len);
    }
    return r.source();
}

fn avroOpenFailed(env: *Env, path: []const u8, e: anyerror) anyerror {
    const why = switch (e) {
        error.NotAvro => "not an Avro object container file",
        error.CorruptAvro, error.UnsupportedAvro, error.BadAvroSchema => eval.takeFailure(e) orelse @errorName(e),
        else => try pathFail(env.arena, path, e),
    };
    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read Avro `{s}` ({s})", .{ path, why }));
}

fn xmlOpenFailed(env: *Env, path: []const u8, e: anyerror) anyerror {
    const told = eval.takeFailure(e);
    if (told) |t| if (std.mem.startsWith(u8, t, path) and std.mem.startsWith(u8, t[path.len..], ": "))
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read XML `{s}`: {s}", .{ path, t[path.len + 2 ..] }));
    const why = switch (e) {
        error.BadXml, error.XmlTokenTooLong => told orelse "it is not well-formed XML in its first rows",
        error.XmlUnsupportedEncoding, error.TextTypeMismatch => told orelse @errorName(e),
        else => try pathFail(env.arena, path, e),
    };
    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read XML `{s}` ({s})", .{ path, why }));
}

/// A Delta write's warning (`delta_merge.Warn`) logged at `warn`.
fn warnLog(ctx: *anyopaque, msg: []const u8) void {
    const log: *obs.Logger = @ptrCast(@alignCast(ctx));
    log.log(.warn, "{s}", .{msg});
}
