//! Shared runtime state: the per-run `Env`, the public option/result types,
//! and the diagnostic helpers every runtime module reports through. A leaf —
//! it imports nothing else under runtime/.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const driver = @import("../connect/driver.zig");
const sql = @import("../connect/sql.zig");
const registry = @import("../connect/registry.zig");
const azure = @import("../connect/azure.zig");
const s3 = @import("../connect/s3.zig");
const analyze = @import("analyze.zig");
const obs = @import("obs.zig");
const Value = @import("../exec/value.zig").Value;

/// `msg` points into the inline `buf`, so it outlives the run's plan arena.
pub const Diag = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    /// Set when the failure looks transient (network/connection) so the control
    /// plane can retry; left false for permanent failures (bad SQL, schema, auth,
    /// rejected data) where a retry would just fail the same way.
    retryable: bool = false,
    /// Where the failing statement/stage sits in the script. Cleared by `setMsg`
    /// and stamped as the error unwinds, innermost stage first, so the position
    /// always belongs to the message beside it.
    pos: ?ast.Pos = null,

    pub fn stamp(self: *Diag, pos: ast.Pos) void {
        if (self.pos == null) self.pos = pos;
    }
};

/// Runs one rendered loop body. Supplied by run.zig to the script module, so the
/// code that renders a `for`/`CALL` body never imports the runtime that executes it.
pub const BodyFn = *const fn (env: *Env, body: []const ast.Stmt, lr: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void;

pub const requestAbort = driver.requestAbort;
pub const aborting = driver.aborting;
/// Friendly text for an operator error, so the CLI never prints a bare Zig
/// error name for a condition the engine has a sentence about.
pub const errLabel = op.errLabel;
pub const resetAbort = driver.resetAbort;

var g_reload = std.atomic.Value(bool).init(false);
pub fn requestReload() void {
    g_reload.store(true, .seq_cst);
}
pub fn takeReload() bool {
    return g_reload.swap(false, .seq_cst);
}

/// A failure worth retrying: connection/network-level, not a config or data error.
/// Host resolution (`UnknownHostName`) is treated as permanent — usually a typo.
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

/// Where the end-of-run summary goes. `.none` keeps `run()` silent when embedded
/// (tests, the HTTP server); the CLI opts into `.stderr` or `.json_stdout`.
pub const SummaryMode = enum { none, stderr, json_stdout };

/// Logging/output config (from CLI flags). `format = .auto` picks human text on a
/// TTY, NDJSON when piped. The default level is `warn`, so a healthy run is silent
/// unless something needs attention; plan shape lives at `debug` (and in EXPLAIN).
pub const LogConfig = struct {
    format: obs.Format = .auto,
    level: obs.Level = .warn,
    quiet: bool = false,
    summary: SummaryMode = .none,
};

/// Inputs to a run: params (from CLI flags or an HTTP request's query string) and
/// an optional request body that `read request` consumes.
/// The result of one for-each item (one table in a fan-out batch), so the control
/// plane can retry just the failures rather than the whole batch.
pub const ItemOutcome = struct {
    item: []const u8,
    ok: bool,
    err: []const u8 = "",
    retryable: bool = false,
    /// The failure was already shown as an ` x target` item line.
    shown: bool = false,
};

/// A thread-safe collector for per-item outcomes. Strings are duped into the
/// caller-provided allocator so they outlive the run's internal arena.
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
    /// Worker threads for map-only pipelines (scan → filter/project/explode). 1 =
    /// the serial driver (deterministic, used by the in-process test harness); the
    /// CLI defaults this to the detected core count.
    threads: usize = 1,
    log: LogConfig = .{},
    /// Optional collector for per-item outcomes of a `for`-each fan-out. When set,
    /// continue-mode partial failures are reported here instead of failing the run.
    outcomes: ?*OutcomeSink = null,
    /// Serve flusher: restrict every `FROM BUFFER` source to this one segment.
    buffer_segment: ?u64 = null,
    /// Print the executed operator tree with measured time and row counts.
    explain: bool = false,
    /// `--format`: what a terminal SELECT writes to stdout.
    stdout_format: StdoutFormat = .table,
    /// Serve flusher: pin the StarRocks label prefix (the segment label) and
    /// run_id (the segment seq) so a replayed segment produces the SAME labels
    /// — the sink's dedup then makes redelivery effectively-once.
    load_label_prefix: ?[]const u8 = null,
    load_run_id: ?u64 = null,
    /// Print a ` + target` line as each `LOAD` of a multi-load run finishes. For
    /// a person at `basalt run` or the REPL; `serve` leaves it off.
    items: bool = false,
    /// Draw a live progress line on stderr while a `LOAD` runs. The caller sets it
    /// only when stderr is a terminal; nothing else ever turns it on.
    progress: bool = false,
};

/// What a terminal `SELECT` writes to stdout: a text table for a person, or rows
/// for a program — NDJSON, CSV, TSV, or an Arrow IPC stream.
pub const StdoutFormat = enum { table, json, csv, tsv, arrow };

pub const SqlKind = registry.SqlKind;

/// Captured when a SQL source is opened, so the planner can re-open the same
/// source per split (each split = the base query wrapped with a key-range WHERE).
pub const SqlDesc = struct {
    kind: SqlKind,
    dialect: sql.Dialect,
    cfg: DbConfig,
    base_sql: []const u8,
    table: ?[]const u8,
    /// The read `base_sql` renders, kept so a split plan can re-render it with
    /// its key column added when a projection left the key out.
    read: ast.Read = .{ .connector = "", .form = .unit },
};

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
    /// Finished/failed `LOAD` counts for the run summary; null in a bare test env.
    loads: ?*obs.LoadTally = null,
    /// Report each finished `LOAD` as an item line (`RunOptions.items`, and only
    /// when the script has more than one to report).
    items: bool = false,
    /// A failed `LOAD` already said so in an item line; the `FOR EACH` that catches
    /// the error reads and clears this rather than logging the same failure twice.
    item_reported: bool = false,
    /// Param name → literal expr, for substitution in stage expressions.
    params_expr: *std.StringHashMap(*const ast.Expr),
    /// Runtime expression-error context (which stage/column failed).
    errctx: *op.ErrCtx,
    /// Emitted-row counter shared by every source (via `obs.CountingSource`).
    rows_read: *std.atomic.Value(u64),
    /// Parsed JSON params (the request body), navigated by `for x in p.path`.
    /// Scalar `p.a.b` path access is substituted at plan time (expand.zig).
    json_params: *std.StringHashMap(std.json.Value),
    /// Statement-form `CREATE FUNCTION` declarations by name — the macro table
    /// `CALL` renders through the for-each machinery. Expression-form functions
    /// never reach here; expand.zig inlined them.
    fns: *const std.StringHashMap(ast.FnDecl),
    /// The PARAMs and LETs as `${...}` interpolation bindings — the outer scope of
    /// every loop row, and the whole scope for a statement outside any loop.
    /// Empty when the script declares neither.
    script_scope: LoopRow = no_loop_vars,
    /// Nesting depth of the `CALL` currently being rendered (recursion guard).
    call_depth: usize = 0,
    /// The endpoint's `INTO BUFFER` declaration, if any (dir + declared schema
    /// for `FROM BUFFER` reads that don't name them).
    buffer_decl: ?ast.BufferDecl = null,
    /// See RunOptions: flusher-mode segment restriction and label pinning.
    buffer_segment: ?u64 = null,
    load_label_prefix: ?[]const u8 = null,
    load_run_id: ?u64 = null,
    /// Set by `openSource` to the SQL source of the pipeline being built — the LAST
    /// one opened, so a join whose build side reads SQL leaves this describing the
    /// binding, not the probe read (`sqlDescForStage` re-derives one read's own).
    sql_desc: ?SqlDesc = null,
    /// The CSV dialect of the pipeline currently running, from its `WITH (...)`:
    /// `csv_in` for the leading read (delimiter + encoding), `csv_out` for the
    /// sink (delimiter only — a CSV sink always writes UTF-8). Set per pipeline by
    /// `runOutput`, the same lifecycle as `sink_name` below, so the parallel
    /// readers and `openSink` can see them without threading hints through every
    /// call site. A CTE's own read does not use these: `openSource` is handed the
    /// binding's hints directly.
    csv_in: csv.Dialect = .{},
    csv_out: csv.Dialect = .{},
    /// `WITH (format = ...)` for the same two ends, when the script names it.
    fmt_in: ?analyze.FileFormat = null,
    fmt_out: ?analyze.FileFormat = null,
    /// Connector types of the first source/sink, for the run summary.
    src_name: []const u8 = "",
    /// Parquet readers opened for the current pipeline, and the last one — used
    /// to push a top-N bound only when there is exactly one.
    pq_readers: usize = 0,
    pq_reader: ?*pqdecode.Reader = null,
    sink_name: []const u8 = "",
    /// The last `LOAD`'s target as the script spelled it, for the run summary.
    last_target: []const u8 = "",
    /// Set once any pipeline writes somewhere other than the stdout table, so a
    /// terminal `SELECT` — whose printed table is its own feedback — gets no summary.
    wrote_sink: bool = false,
    /// `--format json`: stdout sinks emit NDJSON rows instead of the table.
    stdout_format: StdoutFormat = .table,
    /// `EXPLAIN ANALYZE`: run the pipeline for its actuals, write nothing.
    explain: bool = false,
    /// The program's `@kind`, for the header an `EXPLAIN <query>;` statement prints.
    kind_name: []const u8 = "batch",
};

pub const PipeRes = struct { op: op.Op, schema: types.Schema };

/// One discovery row: the first `var_names.len` columns coerced to text.
pub const Row = []const []const u8;

/// A for-loop's per-row binding, threaded as one value through the interpolation and
/// render chain: the loop variable names, their declared types (parallel; empty = all
/// untyped), and this row's raw text cells. Bundled so a new per-variable attribute
/// doesn't mean re-touching every render signature.
pub const LoopRow = struct {
    names: []const []const u8,
    types: []const ?types.Type = &.{},
    cells: Row,
    /// Script scope — the PARAMs and LETs — searched after this row's own names,
    /// which is what makes the innermost binding win (§9's scope rule).
    ///
    /// It is deliberately consulted by the *string* interpolation only, never by
    /// `renderExpr`'s bare-name folding: `$p` in an expression is already a folded
    /// literal by the time a pipeline runs (expand.zig did it), and a bare name
    /// here is a column, so folding script scope into expressions would let a
    /// param quietly displace a same-named column. A `${...}` hole is the one
    /// place the value is still a needle in a string.
    outer: ?*const LoopRow = null,
    /// Set on the script-scope row that ends every chain, so a walk over the
    /// enclosing *loops* knows where to stop.
    script: bool = false,

    pub fn typeAt(self: LoopRow, i: usize) ?types.Type {
        return if (i < self.types.len) self.types[i] else null;
    }

    /// This row's bindings followed by every enclosing scope's, in lookup order.
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

    /// This row's loop variables followed by every enclosing loop's, as typed
    /// values — what an expression may fold. Stops short of script scope.
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

    /// The loop variable `name` as a typed value, innermost loop first.
    pub fn loopVar(self: LoopRow, arena: std.mem.Allocator, name: []const u8) ?Value {
        if (self.script) return null;
        for (self.names, self.cells, 0..) |nm, cell, i| {
            if (std.mem.eql(u8, nm, name)) return loopValue(arena, cell, self.typeAt(i));
        }
        return if (self.outer) |o| o.loopVar(arena, name) else null;
    }

    /// The text bound to a bare `${name}`, searching this row then outwards.
    pub fn lookup(self: LoopRow, name: []const u8) ?[]const u8 {
        for (self.names, self.cells) |nm, val| {
            if (std.mem.eql(u8, nm, name)) return val;
        }
        return if (self.outer) |o| o.lookup(name) else null;
    }
};

/// The one rule `check` and `run` both apply to a `for`/statement-function body.
pub const body_stmt_rule = "a `for` or statement-function body may contain only pipelines, `WITH`, `FOR EACH`, `CASE`, `CALL`, `PRINT`, `EXPLAIN` and `THROW` statements";

/// No loop variables in scope — the binding a top-level `CALL` starts from.
pub const no_loop_vars = LoopRow{ .names = &[_][]const u8{}, .cells = &[_][]const u8{} };

/// How a SQL Server connection authenticates: a SQL login (default), an Azure AD
/// token (`auth = 'aad'`), or Windows NTLMv2 (`auth = 'ntlm'`).
pub const DbAuth = enum { sql, aad, ntlm };

pub const DbConfig = struct {
    host: []const u8 = "",
    port: u16,
    /// Was `port` set explicitly in the connection config? A named instance
    /// (`host\INSTANCE`) resolves its port via the SQL Server Browser only when
    /// this is false — an explicit port skips the lookup (firewalled UDP 1434).
    port_explicit: bool = false,
    user: []const u8 = "",
    password: []const u8 = "",
    database: []const u8 = "",
    tls: sql.TlsMode = .off,
    auth: DbAuth = .sql,
    domain: []const u8 = "",
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
}

/// A StarRocks open failure. The reason is appended only when there is one: a
/// dangling em-dash with nothing after it reads as truncated output, which is what
/// this printed whenever the failure carried no diagnostic of its own.
pub fn srOpenErr(env: *Env, e: anyerror, comptime what: []const u8) error{PlanFailed} {
    const msg = if (env.diag.msg.len == 0)
        std.fmt.allocPrint(env.arena, what ++ " ({s})", .{@errorName(e)}) catch what
    else
        std.fmt.allocPrint(env.arena, what ++ " ({s}) — {s}", .{ @errorName(e), env.diag.msg }) catch what;
    return planErr(env.diag, msg);
}

pub fn planErr(diag: *Diag, msg: []const u8) error{PlanFailed} {
    setMsg(diag, msg);
    return error.PlanFailed;
}

/// `planErr` that also classifies the underlying error as transient/permanent,
/// so the wrapped (PlanFailed) result still carries retry intent to the CLI.
pub fn planErrT(diag: *Diag, e: anyerror, msg: []const u8) error{PlanFailed} {
    if (isTransient(e)) diag.retryable = true;
    setMsg(diag, msg);
    return error.PlanFailed;
}

/// Which layer a file-shaped path resolves to, for error messages. A bare
/// `FileNotFound` on an `az://` path reads as a storage-account problem and
/// sends the reader off to check credentials and containers; naming the layer
/// that actually failed ends that detour at the first error.
fn pathLayer(path: []const u8) []const u8 {
    if (azure.isUrl(path)) return "azure blob";
    if (s3.isUrl(path)) return "s3 object";
    if (std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://")) return "http";
    return "local file";
}

/// `<layer>: <ErrorName>`, the parenthetical every file-shaped open error carries.
pub fn pathFail(arena: std.mem.Allocator, path: []const u8, e: anyerror) ![]const u8 {
    return std.fmt.allocPrint(arena, "{s}: {s}", .{ pathLayer(path), @errorName(e) });
}

pub fn schemaPtr(arena: std.mem.Allocator, schema: types.Schema) !*types.Schema {
    const p = try arena.create(types.Schema);
    p.* = schema;
    return p;
}

/// Coerce a loop variable's raw text cell to its declared type for expression
/// evaluation. Untyped (`null`) stays a string; an empty/blank cell or an
/// unparseable value under a declared numeric/bool type binds to null (so a typed
/// guard simply doesn't match a missing value). String/bytes/temporal/decimal types
/// keep the raw text — the eval layer coerces those as needed.
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

pub fn mkLit(arena: std.mem.Allocator, v: Value) anyerror!*ast.Expr {
    return switch (v) {
        .null => mk(arena, .null_lit),
        .bool => |b| mk(arena, .{ .bool_lit = b }),
        .int => |i| mk(arena, .{ .int_lit = i }),
        .float => |f| mk(arena, .{ .float_lit = f }),
        .string => |s| mk(arena, .{ .str_lit = s }),
        // The DSL has no date/time literal; comparisons coerce a string against a
        // temporal column either way, so the ISO text is the faithful literal for
        // a folded `LET cutoff = date_add('day', -7, today())`.
        .date, .time, .timestamp => mk(arena, .{ .str_lit = try eval.valueToString(arena, v) }),
        else => mk(arena, .null_lit),
    };
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

/// A hint value as a name, accepting either a bare ident or a quoted string
/// (`@[table_field = physical]` or `@[tag_substr = "4,2"]`).
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
