//! Opening a source: local and remote files by format (each through its
//! `formats_open.zig` entry), folders, SQL reads, generated rows and constant
//! sources, with the columns a query reads.

const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const Env = @import("../env.zig").Env;
const SqlDriver = @import("../connect.zig").SqlDriver;
const analyze = @import("../analyze.zig");
const options = @import("../analyze/options.zig");
const ast = @import("../../lang/ast.zig");
const azure = @import("../../store/azure.zig");
const connectWhy = @import("dbconfig.zig").connectWhy;
const driver = @import("../../exec/driver.zig");
const env_mod = @import("../env.zig");
const eval = @import("../../exec/eval.zig");
const RunCfg = @import("register.zig").RunCfg;
const folder = @import("../../store/folder.zig");
const ftp = @import("../../store/ftp.zig");
const gen = @import("../../exec/gen.zig");
const guardFileFormat = @import("sink.zig").guardFileFormat;
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
const msgfmt = @import("../../base/msgfmt.zig");
const types = @import("../../lang/types.zig");
const table_builtins = @import("table_builtins.zig");
const formats = @import("../../format/formats.zig");
const formats_open = @import("formats_open.zig");
const lake = @import("../../format/lake/table.zig");
const stream = @import("../../format/stream.zig");
const observe = @import("observe.zig");
const op = @import("../../exec/op.zig");
const readLabel = @import("../connect.zig").readLabel;
const changes = @import("changes.zig");
const trace = @import("../../base/iotrace.zig");
const scheme = @import("../../store/scheme.zig");
const urls = @import("../../base/url.zig");

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
    const span = trace.begin(.download, rd.form.path);
    defer span.finish();
    out.form = .{ .path = ftp.localize(env.arena, rd.form.path) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not download `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) })) };
    return out;
}

pub fn openSource(env: *Env, rd: ast.Read, hints: []const ast.Hint) !driver.Source {
    return openSourceProjected(env, rd, hints, null, &.{});
}

/// Only the Parquet, Arrow and CSV readers act on `project`. A file read's `cols`
/// (a joined side narrowed to what follows it) stands in for a `project` not given.
/// Parquet readers are counted so a top-N bound is pushed only into a pipeline with
/// exactly one of them.
pub fn openSourceProjected(
    env: *Env,
    rd_in: ast.Read,
    hints: []const ast.Hint,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
) !driver.Source {
    const own: ?[][]const u8 = if (project == null and rd_in.form == .path and rd_in.cols.len > 0) @constCast(rd_in.cols) else null;
    var src = try openSourceOf(env, rd_in, hints, project orelse own, bounds);
    src.stop = env.stop;
    return src;
}

fn openSourceOf(
    env: *Env,
    rd_in: ast.Read,
    hints_in: []const ast.Hint,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
) !driver.Source {
    env.last_request = null;
    const hints = try paramHints(env, hints_in);
    const rd = try localRead(env, rd_in);
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) {
        if (try resolveFolder(env, rd.form.path, hints)) |fr| if (fr.kind == .parquet) {
            env.forgetFolder(rd.form.path);
            const pf = try formats_open.openParquetFolder(env, rd.form.path, fr, project);
            pf.bounds = bounds;
            env.pipe.pq_readers += 1;
            env.pipe.pq_folder = pf;
            return pf.source();
        };
    }
    const is_pq = std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and
        pqdecode.Reader.isPath(rd.form.path);
    if (is_pq) {
        const pr = formats_open.openParquetFile(env, rd.form.path, project) catch |e|
            return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read parquet `{s}` ({s})", .{ rd.form.path, try pathFail(env.arena, rd.form.path, e) }));
        pr.bounds = bounds;
        formats_open.noteParquet(env, pr);
        env.pipe.pq_readers += 1;
        env.pipe.pq_reader = pr;
        return pr.source();
    }
    if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path and project != null) {
        var fdiag = analyze.Diag{};
        const want = analyze.formatFromHints(hints, &fdiag) catch null;
        if (analyze.readFormat(rd.form.path, want) == .arrow and analyze.unreadableTarget(rd.form.path, want) == null)
            return formats_open.openArrow(env, rd.form.path, project);
    }
    return openSourceCols(env, rd, hints, project);
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
    if (env.tables) |m| if (m.find(path, lake.Kind.of(want), at)) |t|
        return .{ .kind = .parquet, .files = try t.layout.paths(env.arena), .table = t };
    const found: lake.Found = if (lake.Kind.of(want)) |k| .{ .kind = k } else (lake.detect(env.arena, path) catch |e|
        return listFailed(env, path, e)) orelse return null;
    const kind = found.kind;
    const got = if (env.tables) |m| m.resolve(path, kind, at, found.listed) else lake.resolve(env.arena, path, kind, at, found.listed);
    const t = got catch |e| {
        if (eval.takeFailure(e)) |why| return planErrT(env.diag, e, try env.arena.dupe(u8, why));
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read {s} table `{s}` ({s})", .{ kind.label(), path, try pathFail(env.arena, path, e) }));
    };
    return .{ .kind = .parquet, .files = try t.layout.paths(env.arena), .table = t };
}

fn listFailed(env: *Env, path: []const u8, e: anyerror) anyerror {
    if (e == azure.Error.AzureEmptyPrefix)
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "no blobs under prefix `{s}`", .{path}));
    if (e == s3.Error.S3EmptyPrefix)
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "no objects under prefix `{s}`", .{path}));
    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not list folder `{s}` ({s})", .{ path, try pathFail(env.arena, path, e) }));
}

fn resolvePlain(env: *Env, path: []const u8, want: ?analyze.FileFormat, at: lake.At) !FolderRead {
    if (at.pinned()) {
        const e = lake.notPinnable(path);
        return planErrT(env.diag, e, try env.arena.dupe(u8, eval.takeFailure(e).?));
    }
    const all = folder.list(env.arena, path) catch |e| return listFailed(env, path, e);
    if (all.len == 0) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "no files in folder `{s}`", .{path}));
    const kind: folder.Kind = if (want) |w| (if (formats.of(w).folder_exts.len > 0) w else return planErr(env.diag, try msgfmt.allocPrint(env.arena, "folder `{s}`: a folder reads " ++ formats.folder_labels ++ " files", .{path}))) else switch (folder.kindOf(all)) {
        .kind => |k| k,
        .mixed => |m| return planErr(env.diag, try msgfmt.allocPrint(env.arena, "folder `{s}` holds both {s} (`{s}`) and {s} (`{s}`) files; one table reads one format — name it with WITH (format = '{s}') or '{s}'", .{ path, folder.label(m.a.kind), folder.below(m.a.file, path), folder.label(m.b.kind), folder.below(m.b.file, path), @tagName(m.a.kind), @tagName(m.b.kind) })),
        .empty => unreachable,
        .neither => return planErr(env.diag, try msgfmt.allocPrint(env.arena, "no " ++ formats.folder_exts_list ++ " file in folder `{s}`", .{path})),
    };
    const files = try folder.only(env.arena, all, kind);
    if (files.len == 0) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "no {s} file in folder `{s}`", .{ formats.folder_exts_of[@intFromEnum(kind)], path }));
    return .{ .kind = kind, .files = files };
}

pub const FolderRead = env_mod.FolderRead;

/// The process's stdin, as CSV unless `format` names JSON or NDJSON. Refused when
/// stdin is already spoken for: the script came from it, or a REPL or kernel reads it.
fn openStdin(env: *Env, hints: []const ast.Hint, project: ?[][]const u8) !driver.Source {
    if (!env.stdin_free)
        return planErr(env.diag, "FROM STDIN needs stdin free for data, but here it carries the script (`basalt run -`), a REPL's input or a kernel's requests; pass the script as a file or with -c");
    var fdiag = analyze.Diag{};
    const want = analyze.formatFromHints(hints, &fdiag) catch |e| return aErr(env, &fdiag, e);
    if (want) |w| if (analyze.stdinProblem(w)) |why| return planErr(env.diag, why);
    const s = try stream.Stream.openStdin(env.arena);
    return formats_open.of(want orelse .csv).stdin.?(env, s, hints, project);
}

/// A source opened for `rd`, a CSV converting only the columns `project` names. A union's
/// branches already carry its `where` hint, so it is not folded in twice.
fn openSourceCols(env: *Env, rd: ast.Read, hints: []const ast.Hint, project: ?[][]const u8) !driver.Source {
    if (std.mem.eql(u8, rd.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "read http needs a quoted URL");
        if (try http_client.optionProblem(env.arena, hints, false, true)) |p| return planErr(env.diag, p.msg);
        var hopts = try http_client.optsFromHints(env.arena, hints);
        hopts.logger = env.log;
        const req = if (env.profile != null) try noteRequest(env, rd, hints, try msgfmt.allocPrint(env.arena, "{f}", .{urls.fmtShown(rd.form.path)})) else null;
        if (req) |r| hopts.page_count = &r.pages;
        const clock = observe.Clock.start();
        const s = http_client.HttpSource.open(env.arena, env.gpa, rd.form.path, hopts) catch |e|
            return httpOpenFailed(env, e, rd.form.path);
        return observe.source(env.arena, s.source(), req, clock.ns());
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
        const kind = if (fr) |f| (if (f.kind == .csv) analyze.readFormat(rd.form.path, want) else f.kind) else analyze.readFormat(rd.form.path, want);
        const open = formats_open.of(kind).read orelse unreachable;
        return open(env, .{
            .path = rd.form.path,
            .folder = if (fr) |f| (if (f.kind == kind) f else null) else null,
            .want = want,
            .hints = hints,
            .text = rd.text_cols,
            .project = project,
        });
    }
    const conn = env.connections.get(rd.connector) orelse
        return planErr(env.diag, try options.unknownConnectionMsg(env.arena, rd.connector, "read"));
    var rd_eff = rd;
    if (rd_eff.where.len > 0 and !std.mem.eql(u8, conn.connector, "http"))
        if (env.scan) |t| driver.ScanTally.add(&t.sql_filters, 1);
    if (std.mem.eql(u8, conn.connector, "http")) {
        if (rd.form != .path) return planErr(env.diag, "reading an http connection needs a path: conn.GET('/path')");
        const hc = try httpConn(env, rd.connector, conn, hints);
        const cc = hc.cc;
        const all = hc.hints;
        var hopts = try http_client.optsFromHints(env.arena, all);
        hopts.logger = env.log;
        const path = try http_client.withQuery(env.arena, rd.form.path, try queryParams(env.arena, all));
        const req = if (env.profile != null) try noteRequest(env, rd_eff, hints, try msgfmt.allocPrint(env.arena, "{f}", .{urls.fmtShown(path)})) else null;
        if (req) |r| hopts.page_count = &r.pages;
        const clock = observe.Clock.start();
        const s = http_client.HttpSource.openConn(env.arena, env.gpa, cc, path, hopts) catch |e|
            return httpOpenFailed(env, e, path);
        return observe.source(env.arena, s.source(), req, clock.ns());
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        const shown = rd_eff;
        if (changes.asked(hints)) rd_eff = try changes.read(env, rd_eff, hints, conn, info);
        const query = try readSql(env, rd_eff);
        env.log.log(.debug, "sql read ({s}): {s}", .{ rd.connector, query });
        const req = try noteRequest(env, shown, hints, query);
        const clock = observe.Clock.start();
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, e) }));
                const s = sql.Source.open(env.gpa, c.sqlConn(), query) catch |e| {
                    defer c.close();
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s} read failed ({s}): {s}", .{ conn.connector, @errorName(e), c.wire.last_error }));
                };
                s.report = try readReport(env, conn.connector);
                env.pipe.sql_desc = try sqlDescFor(env, info.kind, info.dialect, cfg, query, rd_eff);
                return observe.source(env.arena, s.source(), req, clock.ns());
            },
        }
    }
    return planErr(env.diag, try options.unsupportedSourceMsg(env.arena, conn.connector));
}

/// An http connection's settings, and a read's options (`hints`) after its own,
/// checked: what a read of one of its paths, or a lookup's request, is opened with.
pub fn httpConn(env: *Env, name: []const u8, conn: ast.Connection, hints: []const ast.Hint) !struct { cc: http_client.ConnConfig, hints: []const ast.Hint } {
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
        return planErr(env.diag, try options.httpConnMsg(env.arena, name, errmsg));
    const all = try std.mem.concat(env.arena, ast.Hint, &.{ conn.hints, hints });
    if (try http_client.optionProblem(env.arena, all, false, true)) |p| return planErr(env.diag, p.msg);
    return .{ .cc = cc, .hints = all };
}

/// Read options `hints` with each `$name` value the run's PARAM or LET of that name,
/// as `changes_from = $since` reads: known only once the statement runs.
fn paramHints(env: *Env, hints: []const ast.Hint) ![]const ast.Hint {
    var out: ?[]ast.Hint = null;
    for (hints, 0..) |h, i| {
        if (h.value != .ident or !std.mem.startsWith(u8, h.value.ident, "$")) continue;
        const name = h.value.ident[1..];
        const v = env.params.get(name) orelse
            return planErr(env.diag, try msgfmt.allocPrint(env.arena, "read option `{s}` names `${s}`, which no PARAM or LET declares", .{ h.key, name }));
        const o = out orelse try env.arena.dupe(ast.Hint, hints);
        o[i].value = switch (v) {
            .null => .{ .str = "" },
            .int => |n| .{ .int = n },
            else => .{ .str = try @import("../../exec/eval/format.zig").valueToString(env.arena, v) },
        };
        out = o;
    }
    return out orelse hints;
}

/// Records what `rd` is about to send, for the scan built over it to claim.
fn noteRequest(env: *Env, rd: ast.Read, hints: []const ast.Hint, text: []const u8) !?*op.Request {
    const p = env.profile orelse return null;
    const purpose: op.Request.Purpose = if (env.request_purpose != .read)
        env.request_purpose
    else if (std.mem.eql(u8, rd.where, "1 = 0")) .probe else .read;
    const r = try p.request(purpose, try readLabel(env, rd, hints), text);
    env.last_request = r;
    return r;
}

/// A failed first fetch as a plan error naming the URL and status; a cancel stays
/// `Aborted` so the run reports it as cancelled, not failed.
fn httpOpenFailed(env: *Env, e: anyerror, url: []const u8) anyerror {
    if (e == error.Aborted) return e;
    const msg = if (eval.takeFailure(e)) |why|
        try msgfmt.allocPrint(env.arena, "http read failed: {s} ({s})", .{ why, @errorName(e) })
    else
        try msgfmt.allocPrint(env.arena, "http read failed for `{s}` ({s})", .{ url, @errorName(e) });
    return planErrT(env.diag, e, msg);
}

/// A table builtin opened on its arguments, params substituted and each folded to
/// its value; a path it cannot open is named with why.
/// A builtin read's arguments, params substituted and folded to their kinds.
pub fn builtinArgs(env: *Env, call: ast.BuiltinRead) ![]Value {
    const b = table_builtins.of(call);
    const vals = try env.arena.alloc(Value, call.args.len);
    for (call.args, b.sig.args[0..call.args.len], vals) |e, kind, *v| {
        const r = try analyze.substExpr(env.arena, e, env.params_expr);
        v.* = try table_builtins.argValue(env.arena, kind, r) orelse return planErr(env.diag, b.sig.arg_msg);
    }
    return vals;
}

fn openTableBuiltin(env: *Env, call: ast.BuiltinRead) !driver.Source {
    const b = table_builtins.of(call);
    const vals = try builtinArgs(env, call);
    return b.open(env.arena, env.gpa, vals) catch |e| {
        if (e == error.OutOfMemory) return e;
        const path = table_builtins.pathArg(b, vals) orelse return e;
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s}: could not open `{s}` ({s})", .{ b.label, path, try pathFail(env.arena, path, e) }));
    };
}
