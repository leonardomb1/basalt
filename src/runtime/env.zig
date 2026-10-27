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

const std = @import("std");
const msgfmt = @import("../format/msgfmt.zig");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const lake = @import("../format/lake/table.zig");
const driver = @import("../connect/driver.zig");
const Batch = @import("../exec/batch.zig").Batch;
const sql = @import("../db/sql.zig");
const registry = @import("../connect/registry.zig");
const azure = @import("../store/azure.zig");
const s3 = @import("../store/s3.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const ftp = @import("../store/ftp.zig");
const analyze = @import("analyze.zig");
const obs = @import("obs.zig");
const pushdown = @import("pushdown.zig");
const Value = @import("../exec/value.zig").Value;
const arrow = @import("../format/arrow/write.zig");
const Space = @import("../exec/space.zig").Space;
const Profile = @import("../exec/profile.zig").Profile;

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

/// SIGTERM/SIGINT, with atomic stores only (async-signal-safe): the run stops at
/// its next boundary. False once already aborting: the caller exits at once.
pub fn terminate() bool {
    if (driver.aborting()) return false;
    driver.requestAbort();
    return true;
}

/// Connection, network and throttling failures, worth a retry. An unknown host
/// counts too: DNS may not know a host that is still coming up.
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
        error.UnknownHostName,
        error.HttpServerBusy,
        error.FtpServerBusy,
        error.HttpTransportFailed,
        error.ServerClosedConnection,
        error.ConnectionIoFailed,
        error.SmbDisconnected,
        error.SshConnectionLost,
        error.S3Throttled,
        error.AzureThrottled,
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
    /// The `-j` lane count; null picks it per input (`lanes`, `lanes/sources.csvLanes`).
    threads: ?usize = 1,
    log: LogConfig = .{},
    outcomes: ?*OutcomeSink = null,
    explain: bool = false,
    stdout_format: StdoutFormat = .table,
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
    spill_dir: ?[]const u8 = null,
    spill_cap: u64 = 8 << 30,
    op_memory: usize = 2 << 30,
    stdin_free: bool = false,

    /// The lanes a query may fan out on: the `-j` given, or one per physical core.
    pub fn lanes(self: RunOptions) usize {
        return self.threads orelse @import("parallel.zig").defaultLanes();
    }
};

/// Catalog answers asked once per run and shared by parallel loop workers. Each is
/// asked outside `mu`, so one table's round trip never holds up another's.
pub const FactsCache = struct {
    mu: std.Thread.Mutex = .{},
    settled: std.Thread.Condition = .{},
    arena: std.heap.ArenaAllocator,
    facts: OnceMap(*const pushdown.Facts, adoptFacts) = .{},
    rows: OnceMap(?u64, null) = .{},

    pub fn init(gpa: std.mem.Allocator) FactsCache {
        return .{ .arena = std.heap.ArenaAllocator.init(gpa) };
    }
    pub fn deinit(self: *FactsCache) void {
        self.arena.deinit();
    }
};

/// Copies facts built in a private arena into the cache's.
fn adoptFacts(f: *const pushdown.Facts, ca: std.mem.Allocator) error{OutOfMemory}!*const pushdown.Facts {
    const out = try ca.create(pushdown.Facts);
    out.* = pushdown.Facts.init(ca);
    try out.ensureTotalCapacity(f.count());
    var it = f.iterator();
    while (it.next()) |e| out.putAssumeCapacity(try ca.dupe(u8, e.key_ptr.*), e.value_ptr.*);
    return out;
}

/// Answers computed at most once per key across threads: the first asker claims the
/// key and computes outside the lock, askers of the same key wait for it, and other
/// keys go ahead. A failed computation gives the key up, and the next waiter tries.
/// `adopt` copies an answer out of the computation's private arena, which is gone
/// once it is published; null keeps the answer as it is.
pub fn OnceMap(comptime V: type, comptime adopt: ?fn (V, std.mem.Allocator) error{OutOfMemory}!V) type {
    return struct {
        map: std.StringHashMapUnmanaged(?V) = .{},

        const Self = @This();

        /// The answer under `key`, from `compute(ctx, scratch)` when no thread has one yet.
        pub fn get(self: *Self, c: *FactsCache, key: []const u8, ctx: anytype, comptime compute: anytype) !V {
            c.mu.lock();
            defer c.mu.unlock();
            while (self.map.get(key)) |slot| {
                if (slot) |v| return v;
                c.settled.wait(&c.mu);
            }
            const ca = c.arena.allocator();
            try self.map.put(ca, try ca.dupe(u8, key), null);

            c.mu.unlock();
            var scratch = std.heap.ArenaAllocator.init(c.arena.child_allocator);
            defer scratch.deinit();
            const got = compute(ctx, scratch.allocator());
            c.mu.lock();

            defer c.settled.broadcast();
            const v = keep(got, ca) catch |e| {
                _ = self.map.remove(key);
                return e;
            };
            self.map.getPtr(key).?.* = v;
            return v;
        }

        fn keep(got: anytype, ca: std.mem.Allocator) !V {
            const v = try got;
            return if (adopt) |f| f(v, ca) else v;
        }
    };
}

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

/// A folder's format and files; for a lake table, the files its log keeps live and
/// the table they were resolved from.
pub const FolderRead = struct {
    kind: @import("../connect/folder.zig").Kind,
    files: []const []const u8,
    table: ?*const lake.Resolved = null,
};

/// A folder read resolved while planning, kept until its source opens: planning
/// resolves each read several times, and a lake table's resolution replays its log.
pub const FolderMemo = struct { path: []const u8, want: ?analyze.FileFormat, at: lake.At, read: FolderRead };

pub const Env = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    shared: ?std.mem.Allocator = null,
    params: *std.StringHashMap(Value),
    bindings: *std.StringHashMap(ast.Pipeline),
    connections: *std.StringHashMap(ast.Connection),
    sources: *std.array_list.Managed(driver.Source),
    diag: *Diag,
    log: *obs.Logger,
    progress: ?*obs.Progress = null,
    loads: ?*obs.LoadTally = null,
    items: bool = false,
    item_reported: bool = false,
    on_load: ?LoadHook = null,
    loop_row: usize = 0,
    loop_rows: usize = 0,
    now_us: ?i64 = null,

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
    sink_label_prefix: ?[]const u8 = null,
    sql_desc: ?SqlDesc = null,
    csv_in: csv.Dialect = .{},
    csv_out: csv.Dialect = .{},
    fmt_in: ?analyze.FileFormat = null,
    fmt_out: ?analyze.FileFormat = null,
    write_opts: analyze.WriteOptions = .csv,
    cutover: @import("../db/replace.zig").Cutover = .truncate,
    src_name: []const u8 = "",
    pq_readers: usize = 0,
    pq_reader: ?*pqdecode.Reader = null,
    /// A single remote Parquet file planning opened to decide its lanes and then read
    /// serially: its bytes, kept open for the serial read, so the size is not asked
    /// again and a body a server sent whole is not downloaded twice.
    pq_held: ?struct { path: []const u8, src: pqdecode.Bytes } = null,
    pq_folder: ?*pqdecode.Folder = null,
    sort_threads: usize = 1,
    folder_memo: std.ArrayListUnmanaged(FolderMemo) = .empty,
    /// The run's lake tables, each replayed once per statement; null in a PARALLEL
    /// loop's row, which resolves its own.
    tables: ?*lake.Memo = null,
    sink_name: []const u8 = "",
    last_target: []const u8 = "",
    wrote_sink: bool = false,
    stdout_format: StdoutFormat = .table,
    explain: bool = false,
    /// The actuals `EXPLAIN ANALYZE` collects for the pipeline it runs; null otherwise,
    /// so a plain run folds nothing.
    profile: ?*Profile = null,
    result: arrow.ResultInfo = .{},
    results_printed: u32 = 0,
    line_base: u32 = 0,
    scan: ?*driver.ScanTally = null,
    on_result: ?ResultHook = null,
    max_rows: ?u64 = null,
    /// The running statement's row-cap flag when it shows a capped result: every
    /// source it opens carries it, so the cap ends this statement's reads only.
    stop: ?*std.atomic.Value(bool) = null,
    on_let: ?LetHook = null,
    space: ?Space = null,
    op_memory: usize = 2 << 30,
    stdin_free: bool = false,

    /// Forgets `path`'s resolutions as its source opens, so a later read of it, after
    /// a statement may have written there, lists it again.
    pub fn forgetFolder(self: *Env, path: []const u8) void {
        var i: usize = 0;
        while (i < self.folder_memo.items.len) {
            if (std.mem.eql(u8, self.folder_memo.items[i].path, path)) {
                _ = self.folder_memo.swapRemove(i);
            } else i += 1;
        }
    }

    /// A statement ended: the lake tables it read are replayed anew by the next,
    /// and after it `wrote` every table is.
    pub fn endStatement(self: *Env, wrote: bool) void {
        if (self.tables) |m| m.endStatement(wrote);
    }

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
    nulls: []const bool = &.{},

    pub fn typeAt(self: LoopRow, i: usize) ?types.Type {
        return if (i < self.types.len) self.types[i] else null;
    }

    /// A cell's value; a NULL PARAM or LET renders as "" but stays NULL here.
    fn valueAt(self: LoopRow, arena: std.mem.Allocator, i: usize) Value {
        if (i < self.nulls.len and self.nulls[i]) return .null;
        return loopValue(arena, self.cells[i], self.typeAt(i));
    }

    pub fn appendScope(
        self: LoopRow,
        arena: std.mem.Allocator,
        names: *std.array_list.Managed([]const u8),
        vals: *std.array_list.Managed(Value),
    ) !void {
        for (self.names, 0..) |nm, i| {
            try names.append(nm);
            try vals.append(self.valueAt(arena, i));
        }
        if (self.outer) |o| try o.appendScope(arena, names, vals);
    }

    pub const Scope = struct { names: []const []const u8, values: []const Value };

    /// What a constant expression may read here: the loop variables, innermost
    /// first, then `params`.
    pub fn scope(self: LoopRow, arena: std.mem.Allocator, params: *const std.StringHashMap(Value)) !Scope {
        var names = std.array_list.Managed([]const u8).init(arena);
        var values = std.array_list.Managed(Value).init(arena);
        try self.appendLoopVars(arena, &names, &values);
        var it = params.iterator();
        while (it.next()) |kv| {
            try names.append(kv.key_ptr.*);
            try values.append(kv.value_ptr.*);
        }
        return .{ .names = names.items, .values = values.items };
    }

    pub fn appendLoopVars(
        self: LoopRow,
        arena: std.mem.Allocator,
        names: *std.array_list.Managed([]const u8),
        vals: *std.array_list.Managed(Value),
    ) !void {
        if (self.script) return;
        for (self.names, 0..) |nm, i| {
            try names.append(nm);
            try vals.append(self.valueAt(arena, i));
        }
        if (self.outer) |o| try o.appendLoopVars(arena, names, vals);
    }

    pub fn loopVar(self: LoopRow, arena: std.mem.Allocator, name: []const u8) ?Value {
        if (self.script) return null;
        for (self.names, 0..) |nm, i| {
            if (std.mem.eql(u8, nm, name)) return self.valueAt(arena, i);
        }
        return if (self.outer) |o| o.loopVar(arena, name) else null;
    }

    /// This row's first typed variable whose non-blank cell does not parse as its type.
    pub fn badCell(self: LoopRow, arena: std.mem.Allocator) ?struct { name: []const u8, cell: []const u8, ty: types.Type } {
        for (self.names, self.cells, 0..) |nm, cell, i| {
            const t = self.typeAt(i) orelse continue;
            if (cell.len == 0) continue;
            switch (t.kind) {
                .int, .float, .bool => _ = eval.castValue(arena, .{ .string = cell }, t.kind) catch
                    return .{ .name = nm, .cell = cell, .ty = t },
                else => {},
            }
        }
        return null;
    }

    pub fn lookup(self: LoopRow, name: []const u8) ?[]const u8 {
        for (self.names, self.cells) |nm, val| {
            if (std.mem.eql(u8, nm, name)) return val;
        }
        return if (self.outer) |o| o.lookup(name) else null;
    }
};

pub const tls_values_msg = "connection `tls` must be \"off\", \"require\" or \"insecure\"";
pub const auth_values_msg = "connection `auth` must be \"sql\", \"aad\", \"ntlm\" or \"kerberos\"";
pub const ntlm_needs_tls_msg = "connection `auth = 'ntlm'` requires an encrypted channel: set `tls = 'require'`, or `tls = 'insecure'` for a self-signed server certificate";

pub const body_stmt_rule = "a `FOR EACH` or statement-function body may contain only queries, `WITH`, `FOR EACH`, `CASE`, `CALL`, `PRINT`, `EXPLAIN` and `THROW` statements";

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
    connect_timeout: u32 = driver.default_connect_timeout_s,
    query_timeout: u32 = 0,

    pub fn timeouts(self: DbConfig) driver.Timeouts {
        return .{ .connect_s = self.connect_timeout, .query_s = self.query_timeout };
    }
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
        msgfmt.allocPrint(env.arena, "{s} ({s})", .{ what, @errorName(e) }) catch what
    else
        msgfmt.allocPrint(env.arena, "{s} ({s}) — {s}", .{ what, @errorName(e), env.diag.msg }) catch what;
    return planErr(env.diag, msg);
}

/// An analyze failure as a plan failure, keeping its position and span.
pub fn aErr(env: *Env, ad: *const analyze.Diag, e: analyze.Error) anyerror {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.AnalyzeFailed => blk: {
            const err = planErr(env.diag, ad.msg);
            env.diag.pos = ad.pos;
            env.diag.end = ad.end;
            break :blk err;
        },
    };
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
    if (ftp.isUrl(path)) return "ftp";
    if (lake.isHttp(path)) return "http";
    return "local file";
}

pub fn pathFail(arena: std.mem.Allocator, path: []const u8, e: anyerror) ![]const u8 {
    if (eval.takeFailure(e)) |why| return arena.dupe(u8, why);
    if (connectorNote(e, path)) |n| return msgfmt.allocPrint(arena, "{s}: {s}: {s}", .{ n.layer, @errorName(e), n.why });
    return msgfmt.allocPrint(arena, "{s}: {s}", .{ pathLayer(path), @errorName(e) });
}

/// A file connector's own words for its last failure on this thread and the layer it
/// is, both shown as `layer: Error: words`.
pub const ConnectorNote = struct { layer: []const u8, why: []const u8 };

/// The note for `e`: from the connector `path` names when there is a path, else from
/// the one the error's name belongs to (a lane reports without its path); null when
/// that connector kept no words.
pub fn connectorNote(e: anyerror, path: ?[]const u8) ?ConnectorNote {
    const Which = enum { sftp, smb, ftp, none };
    const which: Which = if (path) |p|
        (if (sftp.isUrl(p)) .sftp else if (smb.isUrl(p)) .smb else if (ftp.isUrl(p)) .ftp else .none)
    else blk: {
        const name = @errorName(e);
        if (std.mem.startsWith(u8, name, "Sftp") or std.mem.startsWith(u8, name, "Ssh")) break :blk .sftp;
        if (std.mem.startsWith(u8, name, "Smb")) break :blk .smb;
        break :blk .none;
    };
    const why = switch (which) {
        .sftp => sftp.lastError(),
        .smb => smb.lastError(),
        .ftp => ftp.lastError(),
        .none => return null,
    };
    if (why.len == 0) return null;
    return .{ .layer = @tagName(which), .why = why };
}

/// `512MB`, `8GB`, `2GiB`, `64k` or a bare byte count, case-insensitive; every
/// unit is binary (`1MB` is 1024 KiB). Null when it does not parse or overflows.
pub fn parseByteSize(txt: []const u8) ?u64 {
    const t = std.mem.trim(u8, txt, " \t");
    var n: usize = 0;
    while (n < t.len and std.ascii.isDigit(t[n])) n += 1;
    if (n == 0) return null;
    const v = std.fmt.parseInt(u64, t[0..n], 10) catch return null;
    const unit = std.mem.trim(u8, t[n..], " \t");
    const units = [_]struct { []const u8, u6 }{
        .{ "", 0 },     .{ "b", 0 },
        .{ "k", 10 },   .{ "kb", 10 },
        .{ "kib", 10 }, .{ "m", 20 },
        .{ "mb", 20 },  .{ "mib", 20 },
        .{ "g", 30 },   .{ "gb", 30 },
        .{ "gib", 30 }, .{ "t", 40 },
        .{ "tb", 40 },  .{ "tib", 40 },
    };
    for (units) |u| {
        if (std.ascii.eqlIgnoreCase(unit, u[0])) return std.math.shlExact(u64, v, u[1]) catch null;
    }
    return null;
}

test "parseByteSize: suffixes, case, spaces, and what it refuses" {
    const ok = [_]struct { []const u8, u64 }{
        .{ "0", 0 },             .{ "4096", 4096 },        .{ "12b", 12 },
        .{ "64k", 64 << 10 },    .{ "64KB", 64 << 10 },    .{ "64KiB", 64 << 10 },
        .{ "512MB", 512 << 20 }, .{ "512mib", 512 << 20 }, .{ "8GB", 8 << 30 },
        .{ "2GiB", 2 << 30 },    .{ " 16 gb ", 16 << 30 }, .{ "1TB", 1 << 40 },
    };
    for (ok) |c| try std.testing.expectEqual(@as(?u64, c[1]), parseByteSize(c[0]));
    const bad = [_][]const u8{ "", "GB", "-1GB", "1.5GB", "8 XB", "8GBs", "99999999999999999999", "99999999999TB" };
    for (bad) |b| try std.testing.expectEqual(@as(?u64, null), parseByteSize(b));
}

pub fn schemaPtr(arena: std.mem.Allocator, schema: types.Schema) !*types.Schema {
    const p = try arena.create(types.Schema);
    p.* = schema;
    return p;
}

/// A loop cell as its declared type. A blank cell binds to null, so a typed guard
/// does not match a missing value; one the type cannot parse is refused before the
/// body runs (`LoopRow.badCell`).
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
        .bytes => |b| blk: {
            const hex = try arena.alloc(u8, b.len * 2);
            for (b, 0..) |c, i| _ = std.fmt.bufPrint(hex[i * 2 ..][0..2], "{X:0>2}", .{c}) catch unreachable;
            const args = try arena.alloc(*ast.Expr, 1);
            args[0] = try mk(arena, .{ .str_lit = hex });
            break :blk mk(arena, .{ .call = .{ .name = "unhex", .args = args } });
        },
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

test "isTransient: object-store throttling is worth a retry" {
    try std.testing.expect(isTransient(error.S3Throttled));
    try std.testing.expect(isTransient(error.AzureThrottled));
    try std.testing.expect(!isTransient(error.OutOfMemory));
}

test "connectorNote: none for a local path, an error no file connector names, or a connector with no words; pathFail then names the layer" {
    try std.testing.expect(connectorNote(error.FileNotFound, "data/x.csv") == null);
    try std.testing.expect(connectorNote(error.OutOfMemory, null) == null);
    try std.testing.expect(connectorNote(error.ConnectionResetByPeer, null) == null);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectEqualStrings("local file: FileNotFound", try pathFail(ar.allocator(), "data/x.csv", error.FileNotFound));
    try std.testing.expectEqualStrings("s3 object: S3Throttled", try pathFail(ar.allocator(), "s3://b/k", error.S3Throttled));
}

test "terminate: the first signal aborts the run, a second exits" {
    defer driver.resetAbort();
    try std.testing.expect(terminate());
    try std.testing.expect(driver.aborting());
    try std.testing.expect(!terminate());
}

const OnceTest = struct {
    asked: std.atomic.Value(u32) = .init(0),
    b_asked: std.Thread.ResetEvent = .{},
    claimed: std.Thread.ResetEvent = .{},
    release: std.Thread.ResetEvent = .{},

    fn slow(t: *OnceTest, _: std.mem.Allocator) error{}!?u64 {
        _ = t.asked.fetchAdd(1, .monotonic);
        t.claimed.set();
        t.release.timedWait(5 * std.time.ns_per_s) catch {};
        return 42;
    }
    fn waitsForB(t: *OnceTest, _: std.mem.Allocator) error{Timeout}!?u64 {
        t.claimed.set();
        try t.b_asked.timedWait(5 * std.time.ns_per_s);
        return 1;
    }
    fn b(t: *OnceTest, _: std.mem.Allocator) error{}!?u64 {
        t.b_asked.set();
        return 2;
    }
    fn fails(t: *OnceTest, _: std.mem.Allocator) error{Unreachable}!?u64 {
        t.claimed.set();
        t.release.timedWait(5 * std.time.ns_per_s) catch {};
        return error.Unreachable;
    }
};

test "facts cache: one key is asked once while its other askers wait" {
    var cache = FactsCache.init(std.testing.allocator);
    defer cache.deinit();
    var t = OnceTest{};
    var got: [4]?u64 = @splat(null);
    const Run = struct {
        fn go(c: *FactsCache, tt: *OnceTest, out: *?u64) void {
            out.* = c.rows.get(c, "pg\x00t", tt, OnceTest.slow) catch null;
        }
    };
    var threads: [4]std.Thread = undefined;
    threads[0] = try std.Thread.spawn(.{}, Run.go, .{ &cache, &t, &got[0] });
    try t.claimed.timedWait(5 * std.time.ns_per_s);
    for (threads[1..], got[1..]) |*th, *g| th.* = try std.Thread.spawn(.{}, Run.go, .{ &cache, &t, g });
    std.Thread.sleep(20 * std.time.ns_per_ms);
    t.release.set();
    for (threads) |th| th.join();
    try std.testing.expectEqual(@as(u32, 1), t.asked.load(.monotonic));
    for (got) |g| try std.testing.expectEqual(@as(?u64, 42), g);
}

test "facts cache: another key is asked while the first is still out" {
    var cache = FactsCache.init(std.testing.allocator);
    defer cache.deinit();
    var t = OnceTest{};
    var a_err: ?anyerror = null;
    const Run = struct {
        fn go(c: *FactsCache, tt: *OnceTest, err: *?anyerror) void {
            _ = c.rows.get(c, "pg\x00a", tt, OnceTest.waitsForB) catch |e| {
                err.* = e;
            };
        }
    };
    const th = try std.Thread.spawn(.{}, Run.go, .{ &cache, &t, &a_err });
    try t.claimed.timedWait(5 * std.time.ns_per_s);
    try std.testing.expectEqual(@as(?u64, 2), try cache.rows.get(&cache, "pg\x00b", &t, OnceTest.b));
    th.join();
    try std.testing.expectEqual(@as(?anyerror, null), a_err);
}

test "facts cache: a failed ask gives its key to the next asker" {
    var cache = FactsCache.init(std.testing.allocator);
    defer cache.deinit();
    var t = OnceTest{};
    var first: ?anyerror = null;
    const Run = struct {
        fn go(c: *FactsCache, tt: *OnceTest, err: *?anyerror) void {
            _ = c.rows.get(c, "pg\x00t", tt, OnceTest.fails) catch |e| {
                err.* = e;
            };
        }
    };
    const th = try std.Thread.spawn(.{}, Run.go, .{ &cache, &t, &first });
    try t.claimed.timedWait(5 * std.time.ns_per_s);
    const Release = struct {
        fn go(tt: *OnceTest) void {
            std.Thread.sleep(20 * std.time.ns_per_ms);
            tt.release.set();
        }
    };
    const rel = try std.Thread.spawn(.{}, Release.go, .{&t});
    try std.testing.expectEqual(@as(?u64, 42), try cache.rows.get(&cache, "pg\x00t", &t, OnceTest.slow));
    th.join();
    rel.join();
    try std.testing.expectEqual(@as(?anyerror, error.Unreachable), first);
    try std.testing.expectEqual(@as(u32, 1), t.asked.load(.monotonic));
}
