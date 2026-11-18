//! Opening a sink: files by format (each through its `formats_open.zig` entry) and
//! write mode, stdout, a SQL table, Stream Load.

const ArrowWriter = @import("../../format/arrow/write.zig").ArrowWriter;
const Batch = @import("../../exec/batch.zig").Batch;
const DiscardSink = @import("../connect.zig").DiscardSink;
const Env = @import("../env.zig").Env;
const jsonwrite = @import("../../format/json/write.zig");
const SplitCtx = @import("../connect.zig").SplitCtx;
const SqlDriver = @import("../connect.zig").SqlDriver;
const TableWriter = @import("../../format/table.zig").TableWriter;
const analyze = @import("../analyze.zig");
const options = @import("../analyze/options.zig");
const arrow = @import("../../format/arrow/write.zig");
const ast = @import("../../lang/ast.zig");
const connectWhy = @import("dbconfig.zig").connectWhy;
const csv = @import("../../format/csv.zig");
const driver = @import("../../exec/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const env_mod = @import("../env.zig");
const RunCfg = @import("register.zig").RunCfg;
const opts_schema = @import("options_schema.zig");
const mem_connector = @import("../connect.zig").mem_connector;
const obs = @import("../../exec/obs.zig");
const op = @import("../../exec/op.zig");
const openBulkOrInsert = @import("lane_sink.zig").openBulkOrInsert;
const ReplacePlan = @import("lane_sink.zig").ReplacePlan;
const ReplaceSink = @import("lane_sink.zig").ReplaceSink;
const sql = @import("../../db/sql.zig");
const aErr = @import("../env.zig").aErr;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const proberOpen = @import("sql_read.zig").proberOpen;
const readReport = @import("../connect.zig").readReport;
const redialFor = @import("lane_sink.zig").redialFor;
const resolveDbConfig = @import("dbconfig.zig").resolveDbConfig;
const split = @import("split.zig");
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const srOpenErr = @import("../env.zig").srOpenErr;
const std = @import("std");
const msgfmt = @import("../../base/msgfmt.zig");
const streamload = @import("../../db/streamload.zig");
const types = @import("../../lang/types.zig");
const formats = @import("../../format/formats.zig");
const formats_open = @import("formats_open.zig");

/// Bare `upsert` takes its keys from the source table's primary key; a non-table
/// source gets an error pointing at `upsert on <col>`.
pub fn resolveUpsertKeys(env: *Env, w: ast.Write) !ast.Write {
    if (w.mode != .upsert or w.mode.upsert.keys.len > 0) return w;
    const desc = env.pipe.sql_desc orelse return planErr(env.diag, options.bare_upsert_source_msg);
    const table = desc.table orelse return planErr(env.diag, options.bare_upsert_query_msg);
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql, .report = try readReport(env, @tagName(desc.kind)), .prof = env.profile, .pool = env.conn_pool };
    const prober = split.Prober{ .ctx = &pctx, .openFn = proberOpen };
    const keys = split.introspectPkCols(env.arena, prober, desc.dialect, desc.read.form.table.parts) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read primary key of `{s}`: {s}", .{ table, @errorName(e) }));
    if (keys.len == 0) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "no primary key found on `{s}`; name the key with `UPSERT ON (col)`", .{table}));
    env.log.log(.debug, "upsert: inferred key on {s}: {s}", .{ table, try std.mem.join(env.arena, ", ", keys) });
    var out = w;
    out.mode = .{ .upsert = .{ .keys = keys, .partial = w.mode.upsert.partial } };
    return out;
}

fn fileWriteMode(env: *Env, w: ast.Write) !driver.FileMode {
    if (w.mode != .append) return .truncate;
    if (env.write_opts == .delta) return .append;
    if (env.write_opts == .json and env.write_opts.json == .array) return planErr(env.diag, try msgfmt.allocPrint(env.arena, analyze.json_array_append_fmt, .{w.target}));
    const why = analyze.appendUnsupported(w.target, env.fmt_out) orelse return .append;
    return planErr(env.diag, try msgfmt.allocPrint(env.arena, analyze.append_unsupported_fmt, .{ w.target, why }));
}

/// `run` does not analyze first; without this a `.zip` was parsed as CSV and
/// `COUNT(*)` answered with its deflate stream's newline count.
pub fn guardFileFormat(env: *Env, path: []const u8, explicit: ?analyze.FileFormat, comptime verb: []const u8) !void {
    if (comptime std.mem.eql(u8, verb, "read")) if (explicit) |f| if (formats.readRefused(f)) |why|
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "cannot read `{s}`: {s}", .{ path, why }));
    const problem = if (comptime std.mem.eql(u8, verb, "write"))
        analyze.unwritableTarget(path, explicit) orelse analyze.targetProblem(path, explicit)
    else
        analyze.unreadableTarget(path, explicit);
    if (problem) |why|
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "cannot " ++ verb ++ " `{s}`: {s}", .{ path, why }));
    if (comptime std.mem.eql(u8, verb, "read")) if (analyze.archiveProblem(env.arena, path, explicit, true)) |why|
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "cannot read `{s}`: {s}", .{ path, why }));
}

/// Held from a stdout result's open to its close or abort, hook call included:
/// stdout is one stream per process, so results from PARALLEL loop rows or
/// concurrent kernel requests come out whole, one after another.
var stdout_mu: std.Thread.Mutex = .{};

const StdoutSink = struct {
    inner: driver.Sink,
    gpa: std.mem.Allocator,
    info: arrow.ResultInfo,
    hook: ?env_mod.ResultHook,
    cap: ?*driver.RowCap,
    log: *obs.Logger,
    rows: std.atomic.Value(u64) = .init(0),
    mu: std.Thread.Mutex = .{},

    fn wrap(env: *Env, inner: driver.Sink, info: arrow.ResultInfo, cap: ?*driver.RowCap) !driver.Sink {
        const self = try env.gpa.create(StdoutSink);
        self.* = .{ .inner = inner, .gpa = env.gpa, .info = info, .hook = env.on_result, .cap = cap, .log = env.log };
        return .{ .ptr = self, .vtable = if (cap == null and inner.canRender()) &vt_render else &vt_plain };
    }

    pub fn writeBatch(self: *StdoutSink, arena: std.mem.Allocator, b: Batch) !void {
        const c = self.cap orelse {
            try self.inner.writeBatch(arena, b);
            _ = self.rows.fetchAdd(b.len, .monotonic);
            return;
        };
        self.mu.lock();
        defer self.mu.unlock();
        const room = c.max - c.kept;
        if (b.len > room) {
            c.truncated = true;
            if (c.stop) |f| f.store(true, .release);
        }
        const take: usize = @intCast(@min(room, b.len));
        if (take == 0) return;
        const part = if (take == b.len) b else try op.sliceBatch(arena, b, 0, take);
        try self.inner.writeBatch(arena, part);
        c.kept += take;
        _ = self.rows.fetchAdd(take, .monotonic);
    }

    pub fn close(self: *StdoutSink) !void {
        defer stdout_mu.unlock();
        defer self.gpa.destroy(self);
        try self.inner.close();
        const truncated = if (self.cap) |c| c.truncated else false;
        if (truncated) self.log.log(.warn, "result cut at --max-rows {d}", .{self.cap.?.max});
        if (self.hook) |h| h.call(.{ .info = self.info, .rows = self.rows.load(.monotonic), .truncated = truncated });
    }

    pub fn abort(self: *StdoutSink) void {
        defer stdout_mu.unlock();
        self.inner.abort();
        self.gpa.destroy(self);
    }

    const vt_plain = driver.sinkVTable(StdoutSink);
    const vt_render = blk: {
        var v = driver.sinkVTable(StdoutSink);
        v.renderBatch = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror![]const u8 {
                const self: *StdoutSink = @ptrCast(@alignCast(p));
                const out = try self.inner.renderBatch(arena, b).?;
                _ = self.rows.fetchAdd(b.len, .monotonic);
                return out;
            }
        }.f;
        v.writeRendered = struct {
            fn f(p: *anyopaque, bytes: []const u8) anyerror!void {
                const self: *StdoutSink = @ptrCast(@alignCast(p));
                return self.inner.writeRendered(bytes);
            }
        }.f;
        break :blk v;
    };
};

pub fn openSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, mem_connector)) {
        const ms = env.mem_sink.?;
        ms.schema = try dupeSchema(env.arena, schema);
        if (ms.keep_rels) for (@constCast(ms.schema.fields), schema.fields) |*o, f| {
            o.rel = f.rel;
            o.base = f.base;
            o.clash = f.clash;
        };
        return env.mem_sink.?.sink();
    }
    if (env.union_sink) |s| return s;
    if (env.explain) return if (env.profile) |p| DiscardSink.counting(&p.sink_bytes) else DiscardSink.sink();
    if (std.mem.eql(u8, w.connector, "stdout")) {
        var info = env.takeResult();
        var cap: ?*driver.RowCap = null;
        if (env.max_rows) |n| {
            cap = try env.arena.create(driver.RowCap);
            cap.?.* = .{ .max = n, .stop = env.stop };
            info.cap = cap;
        }
        stdout_mu.lock();
        errdefer stdout_mu.unlock();
        const inner = try openStdoutSink(env, schema, info);
        errdefer inner.abort();
        return StdoutSink.wrap(env, inner, info, cap);
    }
    const cols = try insertSchema(env, w, schema);
    try uniqueTargetColumns(env, w, cols);
    return openTargetSink(env, w, cols);
}

/// Why an INSERT's staging could not be made: the server's words, a column the
/// target lacks or a target that is not there.
pub fn insertSetupFailed(env: *Env, connector: []const u8, target: []const u8, e: anyerror) ![]const u8 {
    const ec = env.errctx;
    const why = if (ec.msg.len > 0) try env.arena.dupe(u8, ec.msg) else @errorName(e);
    ec.msg = "";
    return msgfmt.allocPrint(env.arena, "INSERT INTO {s}: {s} ({s} needs an existing table with the query's columns)", .{ target, why, connector });
}

/// The rows' columns as an `INSERT` names them: its column list, in order, over the
/// query's columns; the query's own names when it gives none.
pub fn insertSchema(env: *Env, w: ast.Write, schema: types.Schema) !types.Schema {
    return (try analyze.insertColumns(env.arena, w, schema)) orelse planErr(env.diag, try analyze.insertCountMsg(env.arena, w, schema));
}

/// `analyze.uniqueColumns` for a LOAD target; a shown result or an in-memory one may
/// repeat a name.
pub fn uniqueTargetColumns(env: *Env, w: ast.Write, schema: types.Schema) !void {
    if (std.mem.eql(u8, w.connector, "stdout") or std.mem.eql(u8, w.connector, mem_connector)) return;
    var ad = analyze.Diag{};
    analyze.uniqueColumns(schema, &ad) catch |e| return aErr(env, &ad, e);
    if (env.write_opts == .records) analyze.recordsColumn(schema, &ad) catch |e| return aErr(env, &ad, e);
}

fn openStdoutSink(env: *Env, schema: types.Schema, info: arrow.ResultInfo) !driver.Sink {
    switch (env.stdout_format) {
        .json => {
            const writer = jsonwrite.Writer.openStdout(env.arena, schema) catch
                return planErr(env.diag, "could not open stdout json writer");
            return writer.sink();
        },
        .arrow => {
            const writer = ArrowWriter.open(env.gpa, schema, info) catch |e| switch (e) {
                error.ArrowUnsupportedType => return planErr(env.diag, "arrow output cannot carry a list, map or struct column"),
                else => return planErr(env.diag, "could not open stdout arrow writer"),
            };
            return writer.sink();
        },
        .csv, .tsv => {
            const delim: u8 = if (env.stdout_format == .tsv) '\t' else ',';
            const writer = csv.CsvWriter.openStdout(env.arena, schema, .{ .delim = delim }) catch
                return planErr(env.diag, "could not open stdout csv writer");
            return writer.sink();
        },
        .table => {
            const writer = TableWriter.open(env.gpa, schema) catch
                return planErr(env.diag, "could not open stdout table");
            return writer.sink();
        },
    }
}

/// Why an UPSERT into table `table`, whose unique keys are `keys`, cannot merge by
/// the keys it names, or null. Each of these matches rows by the table's own keys,
/// not by the keys an upsert names: PostgreSQL's ON CONFLICT needs a whole key on
/// exactly them; a StarRocks or Doris load merges only into a PRIMARY KEY or UNIQUE
/// KEY table, by its key, and appends to any other; MySQL's ON DUPLICATE KEY fires
/// on every unique key, so it needs one on exactly them and no other whose columns
/// the load writes (one on a column it does not write, as an auto-increment id,
/// never collides). Without that MySQL, StarRocks and Doris added each row again,
/// or MySQL updated another row in its place.
pub fn upsertKeyProblem(arena: std.mem.Allocator, dialect: sql.Dialect, table: []const u8, keys: []const split.UniqueKey, named: []const []const u8, written: types.Schema) !?[]const u8 {
    const ci = dialect != .postgres;
    const list = try std.mem.join(arena, ", ", named);
    const exact = for (keys) |k| {
        if (k.whole and sameNames(k.cols, named, ci)) break true;
    } else false;
    if (!exact) return switch (dialect) {
        .starrocks, .doris => try msgfmt.allocPrint(arena, "UPSERT ON ({s}) into `{s}` needs a {s} KEY table keyed on exactly ({s}), which `{s}` is not: {s} merges a load's rows only into one, by its key, and appends them to any other, so every row would be added again; recreate it keyed on ({s}) or load with REPLACE", .{
            list, table, if (dialect == .starrocks) "PRIMARY" else "UNIQUE", list, table, if (dialect == .starrocks) "StarRocks" else "Doris", list,
        }),
        else => try msgfmt.allocPrint(arena, "UPSERT ON ({s}) into `{s}` needs a primary key or unique index on exactly ({s}), which the table lacks: {s}; add one (ALTER TABLE {s} ADD PRIMARY KEY ({s})) or load with REPLACE", .{
            list, table, list, if (dialect == .mysql) "MySQL matches an upsert's rows only by the table's unique keys, so every row would be added again" else "PostgreSQL's ON CONFLICT matches rows by one", table, list,
        }),
    };
    if (dialect != .mysql) return null;
    for (keys) |k| {
        if (k.whole and sameNames(k.cols, named, ci)) continue;
        const writes = for (k.cols) |c| {
            if (c.len > 0 and !nameOf(written, c, ci)) break false;
        } else true;
        if (writes) return try msgfmt.allocPrint(arena, "UPSERT ON ({s}) into `{s}`: MySQL also matches an upsert's rows by the table's unique key ({s}), which this load writes, so a row new by ({s}) could overwrite another row; leave those columns out of the load, or name that key in UPSERT ON", .{
            list, table, try std.mem.join(arena, ", ", k.cols), list,
        });
    }
    return null;
}

/// Refuses an UPSERT whose table, if it exists, would not merge rows by the keys it
/// names (`upsertKeyProblem`), before a row is written; the catalog is read on a
/// connection of its own.
fn checkUpsertTarget(env: *Env, conn: ast.Connection, w: ast.Write, schema: types.Schema) !void {
    if (w.mode != .upsert) return;
    const info = sqlConnInfo(conn) orelse return;
    if (info.dialect == .sqlserver) return;
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = info.kind, .cfg = try resolveDbConfig(env, conn, info.port), .base_sql = "", .report = try readReport(env, conn.connector), .prof = env.profile, .pool = env.conn_pool };
    const keys = split.uniqueKeys(env.arena, .{ .ctx = &pctx, .openFn = proberOpen }, info.dialect, try sql.nameParts(env.arena, w.target)) catch |e|
        return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "could not read the unique keys of `{s}`: {s}", .{ w.target, @errorName(e) }));
    const ks = keys orelse return;
    if (try upsertKeyProblem(env.arena, info.dialect, w.target, ks, w.mode.upsert.keys, schema)) |msg| return planErr(env.diag, msg);
}

fn sameNames(a: []const []const u8, b: []const []const u8, ci: bool) bool {
    if (a.len != b.len) return false;
    for (a) |x| {
        const found = for (b) |y| {
            if (sameName(x, y, ci)) break true;
        } else false;
        if (!found) return false;
    }
    return true;
}

fn nameOf(schema: types.Schema, name: []const u8, ci: bool) bool {
    for (schema.fields) |f| if (sameName(f.name, name, ci)) return true;
    return false;
}

fn sameName(a: []const u8, b: []const u8, ci: bool) bool {
    return if (ci) std.ascii.eqlIgnoreCase(a, b) else std.mem.eql(u8, a, b);
}

fn openTargetSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, "csv")) {
        const fmode = try fileWriteMode(env, w);
        return formats_open.of(env.write_opts).write(env, w, schema, fmode);
    }
    const conn = env.connections.get(w.connector) orelse
        return planErr(env.diag, try options.unknownConnectionMsg(env.arena, w.connector, "write"));
    try checkUpsertTarget(env, conn, w, schema);
    if (streamload.Flavor.of(conn.connector)) |flavor| {
        var cfg = try resolveStreamLoadConfig(env, conn, flavor);
        if (streamload.unquotableName(flavor, cfg.database, w.target, schema)) |name|
            return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} cannot name a database, table or column `{s}`: it drops a backtick from a quoted name, so the rows would land under another name", .{ flavor.name(), name }));
        const plan: ?*ReplacePlan = if (w.insert != null) try @import("lane_sink.zig").streamInsertPlan(env, conn, w, schema) else null;
        if (plan != null) cfg.auto_create = false;
        const s = streamload.StreamLoadSink.open(env.gpa, cfg, if (plan) |p| p.stagingTarget() else w.target, schema, if (plan != null) .append else w.mode) catch |e| {
            if (plan) |p| p.abort();
            return srOpenErr(env, e, try msgfmt.allocPrint(env.arena, "{s} sink open failed", .{flavor.name()}));
        };
        s.logger = env.log;
        s.errctx = env.errctx;
        const p = plan orelse return s.sink();
        const rs = try env.arena.create(ReplaceSink);
        rs.* = .{ .inner = s.sink(), .plan = p };
        return rs.sink();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, e) }));
                var target = w.target;
                var mode = w.mode;
                var plan: ?*ReplacePlan = null;
                if (w.mode == .overwrite) {
                    const qt = try sql.quoteIdent(env.arena, info.dialect, w.target);
                    c.sqlConn().exec(try sql.createTableSql(env.arena, info.dialect, qt, schema, .append)) catch |e| {
                        defer c.close();
                        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} sink failed ({s}): {s}", .{ conn.connector, @errorName(e), c.wire.last_error }));
                    };
                    const p = try env.arena.create(ReplacePlan);
                    p.* = .{ .gpa = env.gpa, .kind = info.kind, .cfg = cfg, .dialect = info.dialect, .target = w.target, .schema = schema, .how = env.cutover, .errctx = env.errctx };
                    p.prepare(env.arena) catch |e| {
                        c.close();
                        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} REPLACE staging failed ({s})", .{ conn.connector, @errorName(e) }));
                    };
                    target = p.stagingTarget();
                    mode = .append;
                    plan = p;
                } else if (w.insert != null) {
                    const p = try env.arena.create(ReplacePlan);
                    p.* = .{ .gpa = env.gpa, .kind = info.kind, .cfg = cfg, .dialect = info.dialect, .target = w.target, .schema = schema, .how = env.cutover, .errctx = env.errctx, .insert = true };
                    p.prepare(env.arena) catch |e| {
                        c.close();
                        return planErr(env.diag, try insertSetupFailed(env, conn.connector, w.target, e));
                    };
                    target = p.stagingTarget();
                    plan = p;
                }
                const inner = openBulkOrInsert(env.gpa, c, SqlDriver(k).Bulk, info.dialect, target, schema, mode, try redialFor(env.arena, info.kind, cfg), env.errctx, plan != null or env.table_lock) catch |e| {
                    defer c.close();
                    if (plan) |p| p.abort();
                    return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s} sink failed ({s}): {s}", .{ conn.connector, @errorName(e), c.wire.last_error }));
                };
                const p = plan orelse return inner;
                const rs = try env.arena.create(ReplaceSink);
                rs.* = .{ .inner = inner, .plan = p };
                return rs.sink();
            },
        }
    }
    return planErr(env.diag, try options.unsupportedSinkMsg(env.arena, conn.connector));
}

/// A StarRocks or Doris load's options; `host`/`port`/`be_url` are spellings of
/// `fe_host`/`fe_port`/`load_url`, and an integer is refused under the key written.
const StreamCfg = struct {
    run: RunCfg,

    pub fn value(self: StreamCfg, comptime F: type, opt: opts_schema.Opt, e: *const ast.Expr) !?F {
        if (F == u16 or F == u32) return try self.run.int(F, e, opt.key);
        if (F == bool) return try self.run.flag(e);
        const v = try self.run.text(e);
        const env = self.run.env;
        if (F == []const u8) return v;
        if (F == sql.TlsMode) return std.meta.stringToEnum(sql.TlsMode, v) orelse planErr(env.diag, env_mod.tls_values_msg);
        if (F == u64) {
            const n = env_mod.parseByteSize(v) orelse 0;
            return if (n == 0) planErr(env.diag, try options.loadSizeMsg(env.arena, v)) else n;
        }
        if (F == streamload.Compression) return std.meta.stringToEnum(streamload.Compression, v) orelse
            planErr(env.diag, try options.loadCompressionMsg(env.arena, v));
        @compileError("no reader for " ++ @typeName(F));
    }
};

pub fn resolveStreamLoadConfig(env: *Env, conn: ast.Connection, flavor: streamload.Flavor) !streamload.Config {
    var cfg = streamload.Config{ .flavor = flavor, .database = "", .errctx = env.errctx, .space = env.space };
    try opts_schema.bind(streamload.Config, &opts_schema.stream, .ignore, &opts_schema.stream_aliases, &cfg, conn.config, StreamCfg{ .run = .{ .env = env, .connector = conn.connector } });
    if (cfg.database.len == 0) return planErr(env.diag, try options.needsDatabaseMsg(env.arena, flavor.name()));
    if (env.sink_label_prefix) |lp| cfg.label_prefix = lp;
    return cfg;
}

const CfgCase = struct { key: []const u8, value: ast.Expr, msg: []const u8 };

test "a connection's int options outside their type and a non-bool auto_create are refused, not truncated" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Value = @import("../../exec/value.zig").Value;
    var params = std.StringHashMap(Value).init(a);
    var bindings = std.StringHashMap(ast.Pipeline).init(a);
    var connections = std.StringHashMap(ast.Connection).init(a);
    var sources = std.array_list.Managed(driver.Source).init(a);
    var diag = env_mod.Diag{};
    var log = obs.Logger.init(0, .text, .err);
    var params_expr = std.StringHashMap(*const ast.Expr).init(a);
    var errctx = op.ErrCtx{};
    var rows = obs.RowCounter.init(0);
    var json = std.StringHashMap(std.json.Value).init(a);
    const fns = std.StringHashMap(ast.FnDecl).init(a);
    var env = Env{ .arena = a, .gpa = a, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns };

    const pos = ast.Pos{ .line = 0, .col = 0 };
    var db = ast.Expr{ .str_lit = "d" };
    var host = ast.Expr{ .str_lit = "h" };
    const refused = [_]CfgCase{
        .{ .key = "fe_port", .value = .{ .int_lit = 70000 }, .msg = "starrocks connection `fe_port` is out of range" },
        .{ .key = "port", .value = .{ .str_lit = "-1" }, .msg = "starrocks connection `port` is out of range" },
        .{ .key = "buckets", .value = .{ .int_lit = -1 }, .msg = "starrocks connection `buckets` is out of range" },
        .{ .key = "replication_num", .value = .{ .int_lit = 1 << 40 }, .msg = "starrocks connection `replication_num` is out of range" },
        .{ .key = "auto_create", .value = .{ .str_lit = "yes" }, .msg = options.bool_cfg_msg },
    };
    for (refused) |c| {
        var v = c.value;
        const conn = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
            .{ .key = "database", .value = &db, .pos = pos },
            .{ .key = c.key, .value = &v, .pos = pos },
        } };
        diag = .{};
        try std.testing.expectError(error.PlanFailed, resolveStreamLoadConfig(&env, conn, .starrocks));
        try std.testing.expectEqualStrings(c.msg, diag.msg);
    }

    for ([_]struct { []const u8, bool }{ .{ "TRUE", true }, .{ "False", false }, .{ "true", true } }) |c| {
        var v = ast.Expr{ .str_lit = c[0] };
        const conn = ast.Connection{ .name = "sr", .connector = "starrocks", .pos = pos, .config = &.{
            .{ .key = "database", .value = &db, .pos = pos },
            .{ .key = "auto_create", .value = &v, .pos = pos },
        } };
        try std.testing.expectEqual(c[1], (try resolveStreamLoadConfig(&env, conn, .starrocks)).auto_create);
    }

    for ([_]ast.Expr{ .{ .int_lit = 70000 }, .{ .int_lit = -1 }, .{ .str_lit = "65536" } }) |p| {
        var v = p;
        const conn = ast.Connection{ .name = "pg", .connector = "postgres", .pos = pos, .config = &.{
            .{ .key = "host", .value = &host, .pos = pos },
            .{ .key = "port", .value = &v, .pos = pos },
        } };
        diag = .{};
        try std.testing.expectError(error.PlanFailed, resolveDbConfig(&env, conn, 5432));
        try std.testing.expectEqualStrings("postgres connection `port` is out of range", diag.msg);
    }
}

test "upsertKeyProblem: an upsert merges only by a whole unique key on exactly its keys, and on MySQL by no other key the load writes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const written = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
        .{ .name = "k", .ty = types.Type.init(.int) },
        .{ .name = "v", .ty = types.Type.init(.string) },
    } };
    const k: []const []const u8 = &.{"k"};
    const pk_k = split.UniqueKey{ .cols = &.{"k"}, .whole = true };
    const pk_id = split.UniqueKey{ .cols = &.{"id"}, .whole = true };
    const uq_auto = split.UniqueKey{ .cols = &.{"auto_id"}, .whole = true };
    const prefix_k = split.UniqueKey{ .cols = &.{"k"}, .whole = false };

    const Case = struct { d: sql.Dialect, keys: []const split.UniqueKey, named: []const []const u8 = &.{"k"}, says: ?[]const u8 };
    const cases = [_]Case{
        .{ .d = .mysql, .keys = &.{}, .says = "every row would be added again" },
        .{ .d = .mysql, .keys = &.{pk_k}, .says = null },
        .{ .d = .mysql, .keys = &.{ pk_k, uq_auto }, .says = null },
        .{ .d = .mysql, .keys = &.{ pk_id, pk_k }, .says = "unique key (id), which this load writes" },
        .{ .d = .mysql, .keys = &.{prefix_k}, .says = "needs a primary key or unique index on exactly (k)" },
        .{ .d = .mysql, .keys = &.{.{ .cols = &.{"K"}, .whole = true }}, .says = null },
        .{ .d = .mysql, .keys = &.{.{ .cols = &.{ "v", "k" }, .whole = true }}, .named = &.{ "k", "v" }, .says = null },
        .{ .d = .postgres, .keys = &.{}, .says = "ON CONFLICT matches rows by one" },
        .{ .d = .postgres, .keys = &.{ pk_id, pk_k }, .says = null },
        .{ .d = .postgres, .keys = &.{.{ .cols = &.{"K"}, .whole = true }}, .says = "needs a primary key" },
        .{ .d = .postgres, .keys = &.{pk_id}, .named = k, .says = "ALTER TABLE t ADD PRIMARY KEY (k)" },
        .{ .d = .starrocks, .keys = &.{}, .says = "needs a PRIMARY KEY table keyed on exactly (k)" },
        .{ .d = .starrocks, .keys = &.{.{ .cols = &.{"K"}, .whole = true }}, .says = null },
        .{ .d = .doris, .keys = &.{.{ .cols = &.{ "id", "k" }, .whole = true }}, .says = "needs a UNIQUE KEY table" },
        .{ .d = .doris, .keys = &.{pk_k}, .says = null },
    };
    for (cases) |c| {
        const got = try upsertKeyProblem(a, c.d, "t", c.keys, c.named, written);
        const ok = if (c.says) |s| got != null and std.mem.indexOf(u8, got.?, s) != null else got == null;
        std.testing.expect(ok) catch |e| {
            std.debug.print("{s} {any}: want {?s}, got {?s}\n", .{ @tagName(c.d), c.keys.len, c.says, got });
            return e;
        };
    }
}
