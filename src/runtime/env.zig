//! Shared runtime state: the per-run `Env`, the public option and result types,
//! and the diagnostic helpers every runtime module reports through. A leaf: it
//! imports nothing else under runtime/, and `BodyFn` is how run.zig hands loop
//! bodies to script.zig without script.zig importing the runtime.
//!
//! `Diag.msg` points into its own buffer, so a message outlives the plan arena;
//! `pos` is cleared by `setMsg` and stamped as the error unwinds, innermost stage
//! first, so a position always belongs to the message beside it.
//!
//! Loop scope (`LoopRow`): a row's own variables, then each enclosing loop's,
//! then the script's PARAMs and LETs. Script scope is consulted only by `${...}`
//! string interpolation, never when folding a bare name in an expression, where
//! a bare name is a column and a same-named param must not displace it.
//!
//! The serve flusher pins `load_label_prefix` and `load_run_id` so a replayed
//! buffer segment produces the same Stream Load labels, and the sink's dedup
//! makes redelivery effectively-once.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/pqdecode.zig");
const driver = @import("../connect/driver.zig");
const Batch = @import("../exec/batch.zig").Batch;
const sql = @import("../db/sql.zig");
const registry = @import("../connect/registry.zig");
const azure = @import("../store/azure.zig");
const s3 = @import("../store/s3.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const analyze = @import("analyze.zig");
const obs = @import("obs.zig");
const pushdown = @import("pushdown.zig");
const Value = @import("../exec/value.zig").Value;
const arrow = @import("../format/arrow.zig");

pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    retryable: bool = false,
    pos: ?ast.Pos = null,
    end: ?ast.Pos = null,

    pub fn stamp(self: *Diag, pos: ast.Pos) void {
        if (self.pos == null) self.pos = pos;
    }
};

pub const BodyFn = *const fn (env: *Env, body: []const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void;

pub const requestAbort = driver.requestAbort;
pub const aborting = driver.aborting;
pub const errLabel = op.errLabel;
pub const resetAbort = driver.resetAbort;

var g_reload = std.atomic.Value(bool).init(false);
pub fn requestReload() void {
    g_reload.store(true, .seq_cst);
}
pub fn takeReload() bool {
    return g_reload.swap(false, .seq_cst);
}

/// Connection and network failures, worth a retry. An unknown host is not:
/// it is usually a typo.
pub fn isTransient(e: anyerror) bool {
    return switch (e) {
        error.ConnectionRefused,
        error.ConnectionTimedOut,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.HostUnreachable,
        error.BrokenPipe,
        error.TemporaryNameServerFailure,
        error.NameServerFailure,
        error.HostLacksNetworkAddresses,
        error.HttpServerBusy,
        error.HttpTransportFailed,
        error.ServerClosedConnection,
        error.ConnectionIoFailed,
        => true,
        else => false,
    };
}
pub const Stats = struct {
    rows_out: usize = 0,
    rows_read: usize = 0,
    run_id: u64 = 0,
    elapsed_ms: u64 = 0,
    source: []const u8 = "",
    sink: []const u8 = "",
};
pub const ParamArg = struct { key: []const u8, val: []const u8 };

pub const SummaryMode = enum { none, stderr, json_stdout };

pub const LogConfig = struct {
    format: obs.Format = .auto,
    level: obs.Level = .warn,
    quiet: bool = false,
    summary: SummaryMode = .none,
};

pub const ItemOutcome = struct {
    item: []const u8,
    ok: bool,
    err: []const u8 = "",
    retryable: bool = false,
    shown: bool = false,
};

pub const OutcomeSink = struct {
    alloc: std.mem.Allocator,
    list: std.array_list.Managed(ItemOutcome),
    mutex: std.Thread.Mutex = .{},

    pub fn init(alloc: std.mem.Allocator) OutcomeSink {
        return .{ .alloc = alloc, .list = std.array_list.Managed(ItemOutcome).init(alloc) };
    }
    pub fn deinit(self: *OutcomeSink) void {
        self.list.deinit();
    }
    pub fn record(self: *OutcomeSink, item: []const u8, ok: bool, err: []const u8, retryable: bool) void {
        self.recordShown(item, ok, err, retryable, false);
    }
    pub fn recordShown(self: *OutcomeSink, item: []const u8, ok: bool, err: []const u8, retryable: bool, shown: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self.list.append(.{
            .item = self.alloc.dupe(u8, item) catch item,
            .ok = ok,
            .err = self.alloc.dupe(u8, err) catch err,
            .retryable = retryable,
            .shown = shown,
        }) catch {};
    }
    pub fn failures(self: *OutcomeSink) usize {
        var n: usize = 0;
        for (self.list.items) |o| {
            if (!o.ok) n += 1;
        }
        return n;
    }
};

pub const RunOptions = struct {
    params: []const ParamArg = &.{},
    request_body: ?[]const u8 = null,
    threads: usize = 1,
    log: LogConfig = .{},
    outcomes: ?*OutcomeSink = null,
    buffer_segment: ?u64 = null,
    explain: bool = false,
    stdout_format: StdoutFormat = .table,
    load_label_prefix: ?[]const u8 = null,
    load_run_id: ?u64 = null,
    items: bool = false,
    progress: bool = false,
    line_base: u32 = 0,
    on_result: ?ResultHook = null,
    max_rows: ?u64 = null,
    progress_hook: ?obs.Progress.EventHook = null,
    on_let: ?LetHook = null,
    declarations_only: bool = false,
    on_load: ?LoadHook = null,
    summary_out: ?*obs.Summary = null,
};

pub const FactsCache = struct {
    mu: std.Thread.Mutex = .{},
    arena: std.heap.ArenaAllocator,
    map: std.StringHashMap(*const pushdown.Facts),

    pub fn init(gpa: std.mem.Allocator) FactsCache {
        return .{ .arena = std.heap.ArenaAllocator.init(gpa), .map = std.StringHashMap(*const pushdown.Facts).init(gpa) };
    }
    pub fn deinit(self: *FactsCache) void {
        self.map.deinit();
        self.arena.deinit();
    }
};

pub const LoadDone = struct {
    ordinal: u64,
    target: []const u8,
    line: u32 = 0,
    col: u32 = 0,
    rows_read: u64 = 0,
    rows_written: u64 = 0,
    elapsed_ms: u64 = 0,
    lanes: usize = 1,
    ok: bool = true,
    reason: []const u8 = "",
    transient: bool = false,
    loop_row: usize = 0,
    loop_rows: usize = 0,
    loop_done: usize = 0,
    loop_total: usize = 0,
};

pub const LoadHook = struct {
    ctx: *anyopaque,
    f: *const fn (ctx: *anyopaque, done: LoadDone) void,
};

pub const LetHook = struct {
    ctx: *anyopaque,
    f: *const fn (ctx: *anyopaque, name: []const u8, value: Value) void,
};

pub const ResultDone = struct { info: arrow.ResultInfo, rows: u64, truncated: bool = false };

pub const ResultHook = struct {
    ctx: *anyopaque,
    f: *const fn (ctx: *anyopaque, done: ResultDone) void,

    pub fn call(self: ResultHook, done: ResultDone) void {
        self.f(self.ctx, done);
    }
};

pub const StdoutFormat = enum { table, json, csv, tsv, arrow };

pub const SqlKind = registry.SqlKind;

pub const SqlDesc = struct {
    kind: SqlKind,
    dialect: sql.Dialect,
    cfg: DbConfig,
    base_sql: []const u8,
    table: ?[]const u8,
    read: ast.Read = .{ .connector = "", .form = .unit },
};

pub const FolderRead = struct { kind: @import("../connect/folder.zig").Kind, files: []const []const u8 };

pub const Env = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    params: *std.StringHashMap(Value),
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    sources: *std.array_list.Managed(driver.Source),
    request_body: ?[]const u8,
    diag: *Diag,
    log: *obs.Logger,
    progress: ?*obs.Progress = null,
    loads: ?*obs.LoadTally = null,
    items: bool = false,
    item_reported: bool = false,
    on_load: ?LoadHook = null,
    loop_row: usize = 0,
    loop_rows: usize = 0,

    params_expr: *std.StringHashMap(*const ast.Expr),
    errctx: *op.ErrCtx,
    facts_cache: ?*FactsCache = null,
    rows_read: *obs.RowCounter,
    json_params: *std.StringHashMap(std.json.Value),
    fns: *const std.StringHashMap(ast.FnDecl),
    materialized: std.StringHashMapUnmanaged(Materialized) = .{},
    mem_sink: ?*MemSink = null,
    script_scope: LoopRow = no_loop_vars,
    call_depth: usize = 0,
    buffer_decl: ?ast.BufferDecl = null,
    buffer_segment: ?u64 = null,
    load_label_prefix: ?[]const u8 = null,
    load_run_id: ?u64 = null,
    sql_desc: ?SqlDesc = null,
    csv_in: csv.Dialect = .{},
    csv_out: csv.Dialect = .{},
    fmt_in: ?analyze.FileFormat = null,
    fmt_out: ?analyze.FileFormat = null,
    src_name: []const u8 = "",
    pq_readers: usize = 0,
    pq_reader: ?*pqdecode.Reader = null,
    pq_folder: ?*pqdecode.Folder = null,
    sort_threads: usize = 1,
    folder_memo: ?struct { path: []const u8, read: FolderRead } = null,
    sink_name: []const u8 = "",
    last_target: []const u8 = "",
    wrote_sink: bool = false,
    stdout_format: StdoutFormat = .table,
    explain: bool = false,
    kind_name: []const u8 = "batch",
    result: arrow.ResultInfo = .{},
    results_printed: u32 = 0,
    line_base: u32 = 0,
    scan: ?*driver.ScanTally = null,
    on_result: ?ResultHook = null,
    max_rows: ?u64 = null,
    on_let: ?LetHook = null,

    pub fn takeResult(self: *Env) arrow.ResultInfo {
        var info = self.result;
        info.statement = self.results_printed;
        self.results_printed += 1;
        return info;
    }

    pub fn noteResult(self: *Env, kind: []const u8, pos: ?ast.Pos) void {
        const p = pos orelse ast.Pos{ .line = 0, .col = 0 };
        self.result = .{
            .kind = kind,
            .line = if (p.line > self.line_base) p.line - self.line_base else 0,
            .col = if (p.line > self.line_base) p.col else 0,
            .t0_ms = std.time.milliTimestamp(),
        };
    }
};

pub const PipeRes = struct { op: op.Op, schema: types.Schema };

pub const Materialized = struct { schema: types.Schema, batches: []const Batch };

pub const MemSink = struct {
    arena: std.mem.Allocator,
    batches: std.array_list.Managed(Batch),
    schema: types.Schema = .{ .fields = &.{} },

    pub fn sink(self: *MemSink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.Sink.VTable{
        .writeBatch = struct {
            fn f(p: *anyopaque, _: std.mem.Allocator, b: Batch) anyerror!void {
                const self: *MemSink = @ptrCast(@alignCast(p));
                try self.batches.append(try b.deepCopy(self.arena));
            }
        }.f,
        .close = struct {
            fn f(_: *anyopaque) anyerror!void {}
        }.f,
        .abort = struct {
            fn f(_: *anyopaque) void {}
        }.f,
    };
};

pub const MemSource = struct {
    m: Materialized,
    next_i: usize = 0,

    pub fn source(self: *MemSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.Source.VTable{
        .schema = struct {
            fn f(p: *anyopaque) types.Schema {
                return @as(*MemSource, @ptrCast(@alignCast(p))).m.schema;
            }
        }.f,
        .next = struct {
            fn f(p: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
                const self: *MemSource = @ptrCast(@alignCast(p));
                if (self.next_i >= self.m.batches.len) return null;
                defer self.next_i += 1;
                return self.m.batches[self.next_i];
            }
        }.f,
        .close = struct {
            fn f(_: *anyopaque) void {}
        }.f,
    };
};

pub const Row = []const []const u8;

pub const LoopRow = struct {
    names: []const []const u8,
    types: []const ?types.Type = &.{},
    cells: Row,
    outer: ?*const LoopRow = null,
    script: bool = false,

    pub fn typeAt(self: LoopRow, i: usize) ?types.Type {
        return if (i < self.types.len) self.types[i] else null;
    }

    pub fn appendScope(
        self: LoopRow,
        arena: std.mem.Allocator,
        names: *std.array_list.Managed([]const u8),
        vals: *std.array_list.Managed(Value),
    ) !void {
        for (self.names, self.cells, 0..) |nm, cell, i| {
            try names.append(nm);
            try vals.append(loopValue(arena, cell, self.typeAt(i)));
        }
        if (self.outer) |o| try o.appendScope(arena, names, vals);
    }

    pub fn appendLoopVars(
        self: LoopRow,
        arena: std.mem.Allocator,
        names: *std.array_list.Managed([]const u8),
        vals: *std.array_list.Managed(Value),
    ) !void {
        if (self.script) return;
        for (self.names, self.cells, 0..) |nm, cell, i| {
            try names.append(nm);
            try vals.append(loopValue(arena, cell, self.typeAt(i)));
        }
        if (self.outer) |o| try o.appendLoopVars(arena, names, vals);
    }

    pub fn loopVar(self: LoopRow, arena: std.mem.Allocator, name: []const u8) ?Value {
        if (self.script) return null;
        for (self.names, self.cells, 0..) |nm, cell, i| {
            if (std.mem.eql(u8, nm, name)) return loopValue(arena, cell, self.typeAt(i));
        }
        return if (self.outer) |o| o.loopVar(arena, name) else null;
    }

    pub fn lookup(self: LoopRow, name: []const u8) ?[]const u8 {
        for (self.names, self.cells) |nm, val| {
            if (std.mem.eql(u8, nm, name)) return val;
        }
        return if (self.outer) |o| o.lookup(name) else null;
    }
};

pub const body_stmt_rule = "a `for` or statement-function body may contain only pipelines, `WITH`, `FOR EACH`, `CASE`, `CALL`, `PRINT`, `EXPLAIN` and `THROW` statements";

pub const no_loop_vars = LoopRow{ .names = &[_][]const u8{}, .cells = &[_][]const u8{} };

pub const DbAuth = enum { sql, aad, ntlm, kerberos };

pub const DbConfig = struct {
    host: []const u8 = "",
    port: u16,
    port_explicit: bool = false,
    user: []const u8 = "",
    password: []const u8 = "",
    database: []const u8 = "",
    tls: sql.TlsMode = .off,
    auth: DbAuth = .sql,
    domain: []const u8 = "",
    realm: []const u8 = "",
    kdc: []const u8 = "",
    spn: []const u8 = "",
    client_id: []const u8 = "",
    resource: []const u8 = "",
    token: []const u8 = "",
};

pub fn eqlAny(k: []const u8, opts: []const []const u8) bool {
    for (opts) |o| {
        if (std.mem.eql(u8, k, o)) return true;
    }
    return false;
}

pub fn setMsg(diag: *Diag, msg: []const u8) void {
    const n = @min(msg.len, diag.buf.len);
    @memcpy(diag.buf[0..n], msg[0..n]);
    diag.msg = diag.buf[0..n];
    diag.pos = null;
    diag.end = null;
}

pub fn srOpenErr(env: *Env, e: anyerror, what: []const u8) error{PlanFailed} {
    const msg = if (env.diag.msg.len == 0)
        std.fmt.allocPrint(env.arena, "{s} ({s})", .{ what, @errorName(e) }) catch what
    else
        std.fmt.allocPrint(env.arena, "{s} ({s}) — {s}", .{ what, @errorName(e), env.diag.msg }) catch what;
    return planErr(env.diag, msg);
}

pub fn planErr(diag: *Diag, msg: []const u8) error{PlanFailed} {
    setMsg(diag, msg);
    return error.PlanFailed;
}

/// `planErr` that keeps a transient cause's retry intent through `PlanFailed`.
pub fn planErrT(diag: *Diag, e: anyerror, msg: []const u8) error{PlanFailed} {
    if (isTransient(e)) diag.retryable = true;
    setMsg(diag, msg);
    return error.PlanFailed;
}

fn pathLayer(path: []const u8) []const u8 {
    if (azure.isUrl(path)) return "azure blob";
    if (s3.isUrl(path)) return "s3 object";
    if (sftp.isUrl(path)) return "sftp";
    if (smb.isUrl(path)) return "smb";
    if (std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://")) return "http";
    return "local file";
}

pub fn pathFail(arena: std.mem.Allocator, path: []const u8, e: anyerror) ![]const u8 {
    if (sftp.isUrl(path)) if (sftp.lastError().len > 0)
        return std.fmt.allocPrint(arena, "{s}: {s}: {s}", .{ pathLayer(path), @errorName(e), sftp.lastError() });
    if (smb.isUrl(path)) if (smb.lastError().len > 0)
        return std.fmt.allocPrint(arena, "{s}: {s}: {s}", .{ pathLayer(path), @errorName(e), smb.lastError() });
    return std.fmt.allocPrint(arena, "{s}: {s}", .{ pathLayer(path), @errorName(e) });
}

pub fn schemaPtr(arena: std.mem.Allocator, schema: types.Schema) !*types.Schema {
    const p = try arena.create(types.Schema);
    p.* = schema;
    return p;
}

/// A loop cell as its declared type. A blank cell, or one a numeric or bool type
/// cannot parse, binds to null, so a typed guard does not match a missing value.
pub fn loopValue(arena: std.mem.Allocator, cell: []const u8, ty: ?types.Type) Value {
    const t = ty orelse return .{ .string = cell };
    if (cell.len == 0) return .null;
    return switch (t.kind) {
        .int, .float, .bool => eval.castValue(arena, .{ .string = cell }, t.kind) catch .null,
        else => .{ .string = cell },
    };
}

pub fn mk(arena: std.mem.Allocator, e: ast.Expr) anyerror!*ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

/// The DSL has no date, time or decimal literal, so those become their text
/// cast to their own type: a DATE param stays a DATE and a decimal keeps its digits.
pub fn mkLit(arena: std.mem.Allocator, v: Value) anyerror!*ast.Expr {
    return switch (v) {
        .null => mk(arena, .null_lit),
        .bool => |b| mk(arena, .{ .bool_lit = b }),
        .int => |i| mk(arena, .{ .int_lit = i }),
        .float => |f| mk(arena, .{ .float_lit = f }),
        .string => |s| mk(arena, .{ .str_lit = s }),
        .date => typedLit(arena, v, types.Type.init(.date)),
        .time => typedLit(arena, v, types.Type.init(.time)),
        .timestamp => typedLit(arena, v, types.Type.init(.timestamp)),
        .decimal => |d| typedLit(arena, v, types.Type.decimal(38, d.scale)),
        else => mk(arena, .null_lit),
    };
}

fn typedLit(arena: std.mem.Allocator, v: Value, ty: types.Type) anyerror!*ast.Expr {
    const text = try mk(arena, .{ .str_lit = try eval.valueToString(arena, v) });
    return mk(arena, .{ .cast = .{ .e = text, .ty = ty } });
}

pub fn forHintName(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key)) return switch (h.value) {
            .ident => |s| s,
            .str => |s| s,
            else => null,
        };
    }
    return null;
}

pub fn hasFlagHint(hints: []const ast.Hint, key: []const u8) bool {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key)) return true;
    }
    return false;
}

pub fn forHintIdent(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key) and h.value == .ident) return h.value.ident;
    }
    return null;
}
