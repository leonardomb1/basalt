//! Opening a sink: files by format and write mode, stdout, a SQL table, Stream Load.

const ArrowFileSink = @import("../../format/arrow.zig").FileSink;
const ArrowWriter = @import("../../format/arrow.zig").ArrowWriter;
const Batch = @import("../../exec/batch.zig").Batch;
const DiscardSink = @import("../connect.zig").DiscardSink;
const Env = @import("../env.zig").Env;
const JsonWriter = @import("../../connect/table.zig").JsonWriter;
const SplitCtx = @import("../connect.zig").SplitCtx;
const SqlDriver = @import("../connect.zig").SqlDriver;
const TableWriter = @import("../../connect/table.zig").TableWriter;
const analyze = @import("../analyze.zig");
const arrow = @import("../../format/arrow.zig");
const arrowread = @import("../../format/arrowread.zig");
const ast = @import("../../lang/ast.zig");
const connectWhy = @import("dbconfig.zig").connectWhy;
const csv = @import("../../format/csv.zig");
const driver = @import("../../connect/driver.zig");
const dupeSchema = @import("../connect.zig").dupeSchema;
const env_mod = @import("../env.zig");
const eqlAny = @import("../env.zig").eqlAny;
const evalCfgBool = @import("register.zig").evalCfgBool;
const evalCfgInt = @import("register.zig").evalCfgInt;
const evalCfgStr = @import("register.zig").evalCfgStr;
const mem_connector = @import("../connect.zig").mem_connector;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openBulkOrInsert = @import("lane_sink.zig").openBulkOrInsert;
const pathFail = @import("../env.zig").pathFail;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const pqwrite = @import("../../format/parquet/write.zig");
const proberOpen = @import("sql_read.zig").proberOpen;
const readReport = @import("../connect.zig").readReport;
const redialFor = @import("lane_sink.zig").redialFor;
const resolveDbConfig = @import("dbconfig.zig").resolveDbConfig;
const split = @import("../../connect/split.zig");
const sqlConnInfo = @import("../connect.zig").sqlConnInfo;
const srOpenErr = @import("../env.zig").srOpenErr;
const std = @import("std");
const streamload = @import("../../db/streamload.zig");
const types = @import("../../lang/types.zig");
const xlsx = @import("../../format/xlsx.zig");

/// Bare `upsert` takes its keys from the source table's primary key; a non-table
/// source gets an error pointing at `upsert on <col>`.
pub fn resolveUpsertKeys(env: *Env, w: ast.Write) !ast.Write {
    if (w.mode != .upsert or w.mode.upsert.keys.len > 0) return w;
    const desc = env.sql_desc orelse return planErr(env.diag, "`upsert` without `on <key>` infers the primary key from the source, which needs a SQL `table` read — this pipeline's source can't be introspected; name the key with `upsert on <col>`");
    const table = desc.table orelse return planErr(env.diag, "`upsert` key inference needs `read <conn> table <name>` (a `query` source has no single table to introspect); name the key with `upsert on <col>`");
    var pctx = SplitCtx{ .gpa = env.gpa, .kind = desc.kind, .cfg = desc.cfg, .base_sql = desc.base_sql, .report = try readReport(env, @tagName(desc.kind)) };
    const prober = split.Prober{ .ctx = &pctx, .openFn = proberOpen };
    const keys = split.introspectPkCols(env.arena, prober, desc.dialect, table) catch |e|
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not read primary key of `{s}`: {s}", .{ table, @errorName(e) }));
    if (keys.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "no primary key found on `{s}`; name the key with `upsert on <col>`", .{table}));
    env.log.log(.debug, "upsert: inferred key on {s}: {s}", .{ table, try std.mem.join(env.arena, ", ", keys) });
    var out = w;
    out.mode = .{ .upsert = .{ .keys = keys, .partial = w.mode.upsert.partial } };
    return out;
}

fn fileWriteMode(env: *Env, w: ast.Write) !driver.FileMode {
    if (w.mode != .append) return .truncate;
    const why = analyze.appendUnsupported(w.target) orelse return .append;
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`APPEND` into `{s}` is not supported: {s}. Use `REPLACE`, write each run to its own path, or accumulate with `INTO BUFFER` and load the buffer once", .{ w.target, why }));
}

/// `run` does not analyze first; without this a `.zip` was parsed as CSV and
/// `COUNT(*)` answered with its deflate stream's newline count.
pub fn guardFileFormat(env: *Env, path: []const u8, explicit: ?analyze.FileFormat, comptime verb: []const u8) !void {
    const unwritable = if (comptime std.mem.eql(u8, verb, "write")) analyze.unwritableTarget(path, explicit) else null;
    if (unwritable orelse analyze.unreadableTarget(path, explicit)) |why|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot " ++ verb ++ " `{s}`: {s}", .{ path, why }));
}

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
            driver.requestStop();
        }
        const take: usize = @intCast(@min(room, b.len));
        if (take == 0) return;
        const part = if (take == b.len) b else try op.sliceBatch(arena, b, 0, take);
        try self.inner.writeBatch(arena, part);
        c.kept += take;
        _ = self.rows.fetchAdd(take, .monotonic);
    }

    pub fn close(self: *StdoutSink) !void {
        defer self.gpa.destroy(self);
        defer driver.resetStop();
        try self.inner.close();
        const truncated = if (self.cap) |c| c.truncated else false;
        if (truncated) self.log.log(.warn, "result cut at --max-rows {d}", .{self.cap.?.max});
        if (self.hook) |h| h.call(.{ .info = self.info, .rows = self.rows.load(.monotonic), .truncated = truncated });
    }

    pub fn abort(self: *StdoutSink) void {
        driver.resetStop();
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
        env.mem_sink.?.schema = try dupeSchema(env.arena, schema);
        return env.mem_sink.?.sink();
    }
    if (env.explain) return DiscardSink.sink();
    if (std.mem.eql(u8, w.connector, "stdout")) {
        var info = env.takeResult();
        var cap: ?*driver.RowCap = null;
        if (env.max_rows) |n| {
            cap = try env.arena.create(driver.RowCap);
            cap.?.* = .{ .max = n };
            info.cap = cap;
        }
        driver.resetStop();
        const inner = try openStdoutSink(env, schema, info);
        if (env.on_result == null and cap == null) return inner;
        errdefer inner.abort();
        return StdoutSink.wrap(env, inner, info, cap);
    }
    return openTargetSink(env, w, schema);
}

fn openStdoutSink(env: *Env, schema: types.Schema, info: arrow.ResultInfo) !driver.Sink {
    switch (env.stdout_format) {
        .json => {
            const writer = JsonWriter.open(env.gpa, schema) catch
                return planErr(env.diag, "could not open stdout json writer");
            return writer.sink();
        },
        .arrow => {
            const writer = ArrowWriter.open(env.gpa, schema, info) catch |e| switch (e) {
                error.ArrowUnsupportedType => return planErr(env.diag, "arrow output cannot carry an array or struct column"),
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

fn openTargetSink(env: *Env, w: ast.Write, schema: types.Schema) !driver.Sink {
    if (std.mem.eql(u8, w.connector, "csv")) {
        const fmode = try fileWriteMode(env, w);
        if ((env.fmt_out == null and xlsx.isPath(w.target)) or env.fmt_out == .xlsx)
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot write `{s}`: basalt reads Excel workbooks but does not write them; write a `.csv` or `.parquet`", .{w.target}));
        const wfmt = env.fmt_out orelse (if (pqwrite.Writer.isPath(w.target)) analyze.FileFormat.parquet else if (arrowread.isPath(w.target)) analyze.FileFormat.arrow else analyze.FileFormat.csv);
        if (wfmt == .arrow) {
            const aw = ArrowFileSink.open(env.gpa, w.target, schema) catch |e| switch (e) {
                error.ArrowRemoteSink => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "cannot write `{s}`: Arrow IPC is written to a local file", .{w.target})),
                error.ArrowUnsupportedType => return planErr(env.diag, "Arrow IPC cannot carry an array or struct column"),
                else => return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output Arrow IPC `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) })),
            };
            return aw.sink();
        }
        if (wfmt == .parquet) {
            const pw = pqwrite.Writer.open(env.arena, w.target, schema, .snappy, fmode) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output parquet `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
            return pw.sink();
        }
        const writer = csv.CsvWriter.open(env.arena, w.target, schema, fmode, env.csv_out) catch |e|
            return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "could not open output CSV `{s}` ({s})", .{ w.target, try pathFail(env.arena, w.target, e) }));
        return writer.sink();
    }
    const conn = env.connections.get(w.connector) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown connection `{s}`", .{w.connector}));
    if (streamload.Flavor.of(conn.connector)) |flavor| {
        const cfg = try resolveStreamLoadConfig(env, conn, flavor);
        const s = streamload.StreamLoadSink.open(env.gpa, cfg, w.target, schema, w.mode) catch |e|
            return srOpenErr(env, e, try std.fmt.allocPrint(env.arena, "{s} sink open failed", .{flavor.name()}));
        s.logger = env.log;
        s.errctx = env.errctx;
        return s.sink();
    }
    if (sqlConnInfo(conn)) |info| {
        const cfg = try resolveDbConfig(env, conn, info.port);
        switch (info.kind) {
            inline else => |k| {
                const c = SqlDriver(k).connect(env.gpa, cfg) catch |e|
                    return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "{s} connect failed: {s}", .{ conn.connector, try connectWhy(env.arena, conn.connector, e) }));
                return openBulkOrInsert(env.gpa, c, SqlDriver(k).Bulk, info.dialect, w.target, schema, w.mode, try redialFor(env.arena, info.kind, cfg)) catch |e| {
                    defer c.close();
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} sink failed ({s}): {s}", .{ conn.connector, @errorName(e), c.last_error }));
                };
            },
        }
    }
    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unsupported sink connector `{s}`", .{conn.connector}));
}

pub fn resolveStreamLoadConfig(env: *Env, conn: ast.Connection, flavor: streamload.Flavor) !streamload.Config {
    var cfg = streamload.Config{ .flavor = flavor, .database = "", .errctx = env.errctx };
    for (conn.config) |attr| {
        const k = attr.key;
        if (eqlAny(k, &.{ "host", "fe_host" })) {
            cfg.fe_host = try evalCfgStr(env, attr.value);
        } else if (eqlAny(k, &.{ "port", "fe_port" })) {
            cfg.fe_port = @intCast(try evalCfgInt(env, attr.value));
        } else if (eqlAny(k, &.{ "be_url", "load_url" })) {
            cfg.load_url = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "database")) {
            cfg.database = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "user")) {
            cfg.user = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "password")) {
            cfg.password = try evalCfgStr(env, attr.value);
        } else if (std.mem.eql(u8, k, "buckets")) {
            cfg.buckets = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "replication_num")) {
            cfg.replication_num = @intCast(try evalCfgInt(env, attr.value));
        } else if (std.mem.eql(u8, k, "auto_create")) {
            cfg.auto_create = try evalCfgBool(env, attr.value);
        } else if (std.mem.eql(u8, k, "label_prefix")) {
            cfg.label_prefix = try evalCfgStr(env, attr.value);
        }
    }
    if (cfg.database.len == 0) return planErr(env.diag, try std.fmt.allocPrint(env.arena, "{s} connection needs a `database`", .{flavor.name()}));
    if (env.load_label_prefix) |lp| cfg.label_prefix = lp;
    if (env.load_run_id) |rid| cfg.run_id = rid;
    return cfg;
}
