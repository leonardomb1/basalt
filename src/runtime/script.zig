//! Script-level statements: FOR EACH discovery and rendering, `${...}`
//! interpolation, CALL, PRINT, THROW, and the script scope they resolve in.

const std = @import("std");
const parser = @import("../lang/sql_parser.zig");
const ast = @import("../lang/ast.zig");
const expand = @import("../lang/expand.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");

const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const parallel = @import("parallel.zig");
const pushdown = @import("pushdown.zig");
const Value = @import("../exec/value.zig").Value;

const aborting = @import("env.zig").aborting;
const BodyFn = @import("env.zig").BodyFn;
const Diag = @import("env.zig").Diag;
const Env = @import("env.zig").Env;
const forHintIdent = @import("env.zig").forHintIdent;
const isTransient = @import("env.zig").isTransient;
const LoopRow = @import("env.zig").LoopRow;
const mk = @import("env.zig").mk;
const mkLit = @import("env.zig").mkLit;
const no_loop_vars = @import("env.zig").no_loop_vars;
const OutcomeSink = @import("env.zig").OutcomeSink;
const planErr = @import("env.zig").planErr;
const Row = @import("env.zig").Row;
const RunOptions = @import("env.zig").RunOptions;
const Stats = @import("env.zig").Stats;

const discoverRows = @import("plan.zig").discoverRows;
const discoverRowsJson = @import("plan.zig").discoverRowsJson;
const discoverRowsPipeline = @import("plan.zig").discoverRowsPipeline;

const ForMode = enum { sequential, parallel };
const OnError = enum { stop, continue_ };

/// Identify a for-each row the way the script names it — `n=0`, or
/// `db=sales, tbl=orders` — so a failure points at a binding rather than an index.
/// Falls back to the first cell if the label cannot be built.
fn rowLabel(arena: std.mem.Allocator, names: []const []const u8, row: Row) []const u8 {
    var out: []const u8 = "";
    for (names, 0..) |n, i| {
        if (i >= row.len) break;
        const sep: []const u8 = if (out.len > 0) ", " else "";
        out = std.fmt.allocPrint(arena, "{s}{s}{s}={s}", .{ out, sep, n, row[i] }) catch return row[0];
    }
    return if (out.len > 0) out else row[0];
}

/// Render every `${ ... }` placeholder in `s`, chaining over the loop variables
/// `names[i]` → `values[i]`. Two body shapes:
///   * `${var}` — fast path: substitute the loop variable's value. An unknown
///     variable is left verbatim, so non-loop `${...}` text rides through untouched.
///   * `${ <expr> }` — anything else is parsed as an expression and evaluated with
///     the loop variables in scope (bound as strings, or as their declared type when
///     the `for` header annotated one, e.g. `port:int`), then formatted to text. This
///     is where conditionals/coalesce/case-folding live: `${lower(name)}`,
///     `${if(pk == "", concat(name, "id"), pk)}`, `${coalesce(pk, "default")}`.
/// Strings without `${` are returned as-is (no allocation).
pub fn interpAll(arena: std.mem.Allocator, s: []const u8, lr: LoopRow) ![]const u8 {
    if (std.mem.indexOf(u8, s, "${") == null) return s;
    var out = std.array_list.Managed(u8).init(arena);
    errdefer out.deinit();
    var i: usize = 0;
    while (i < s.len) {
        if (s[i] == '$' and i + 1 < s.len and s[i + 1] == '{') {
            const close = interpClose(s, i + 2) orelse {
                try out.appendSlice(s[i..]);
                break;
            };
            const inner = s[i + 2 .. close];
            if (bareInterp(inner)) {
                if (lr.lookup(inner)) |val| {
                    try out.appendSlice(val);
                } else {
                    try out.appendSlice(s[i .. close + 1]);
                }
            } else {
                try out.appendSlice(try evalInterpExpr(arena, inner, lr));
            }
            i = close + 1;
        } else {
            try out.append(s[i]);
            i += 1;
        }
    }
    return out.toOwnedSlice();
}

/// The index of the `}` that closes a `${` opened just before `start`. Scanning
/// is quote-aware — a `}` inside `'...'` (a string) or `"..."` (a quoted column
/// name) does not close — and brace-balanced. Both quotes escape by doubling.
fn interpClose(s: []const u8, start: usize) ?usize {
    var depth: usize = 1;
    var k = start;
    while (k < s.len) {
        switch (s[k]) {
            '\'', '"' => {
                const q = s[k];
                k += 1;
                while (k < s.len) {
                    if (s[k] == q) {
                        if (k + 1 < s.len and s[k + 1] == q) {
                            k += 2;
                            continue;
                        }
                        k += 1;
                        break;
                    }
                    k += 1;
                }
            },
            '{' => {
                depth += 1;
                k += 1;
            },
            '}' => {
                depth -= 1;
                if (depth == 0) return k;
                k += 1;
            },
            else => k += 1,
        }
    }
    return null;
}

/// A `${var}` body — a bare identifier (the fast variable-substitution path).
/// Anything richer (calls, operators, a path) is evaluated as an expression.
fn bareInterp(inner: []const u8) bool {
    if (inner.len == 0 or !(std.ascii.isAlphabetic(inner[0]) or inner[0] == '_')) return false;
    for (inner[1..]) |c| if (!(std.ascii.isAlphanumeric(c) or c == '_')) return false;
    return true;
}

/// Detect constructs that only work after plan-time expansion and so cannot appear in
/// a `${...}` hole (which is parsed/evaluated fresh, without expansion): `let … in`
/// and `?.` safe navigation. Returns a user-facing reason, or null if the expression
/// is fine for interpolation.
fn interpUnsupported(e: *const ast.Expr) ?[]const u8 {
    return switch (e.*) {
        .let_in => "`let … in` is not supported inside ${...}; bind it in a select/fn instead",
        .field => |q| if (q.safe.len > 0) "`?.` safe navigation is not supported inside ${...}" else null,
        .unary => |u| interpUnsupported(u.e),
        .binary => |b| interpUnsupported(b.l) orelse interpUnsupported(b.r),
        .cond => |c| interpUnsupported(c.cond) orelse interpUnsupported(c.then) orelse interpUnsupported(c.els),
        .cast => |c| interpUnsupported(c.e),
        .is_null => |n| interpUnsupported(n.e),
        .call => |c| {
            for (c.args) |a| if (interpUnsupported(a)) |why| return why;
            return null;
        },
        .match => |m| {
            if (m.subject) |s| if (interpUnsupported(s)) |why| return why;
            for (m.arms) |arm| {
                for (arm.pats) |p| if (interpUnsupported(p)) |why| return why;
                if (arm.guard) |g| if (interpUnsupported(g)) |why| return why;
                if (interpUnsupported(arm.value)) |why| return why;
            }
            return null;
        },
        else => null,
    };
}

/// Parse a `${ <expr> }` body and evaluate it with the loop variables in scope,
/// returning the result formatted as text. Variables bind as strings unless the
/// `for` header declared a type (`port:int`), in which case the coerced value is
/// used — so `${if(port > 1000, ...)}` compares numerically, matching the `match`
/// path. A parse/eval failure is a permanent error (reported on stderr).
fn evalInterpExpr(arena: std.mem.Allocator, text: []const u8, lr: LoopRow) ![]const u8 {
    var diag = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const e = parser.parseExprStr(arena, text, &diag) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        std.debug.print("[interp] ${{{s}}}: {s}\n", .{ text, diag.msg });
        return error.InterpFailed;
    };
    if (interpUnsupported(e)) |why| {
        std.debug.print("[interp] ${{{s}}}: {s}\n", .{ text, why });
        return error.InterpFailed;
    }
    var names = std.array_list.Managed([]const u8).init(arena);
    var vals = std.array_list.Managed(Value).init(arena);
    try lr.appendScope(arena, &names, &vals);
    const result = eval.constEval(arena, e, names.items, vals.items) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        std.debug.print("[interp] ${{{s}}}: {s}\n", .{ text, @errorName(err) });
        return error.InterpFailed;
    };
    return eval.valueToString(arena, result);
}

/// Render a name whose parts may be `${...}` templates — a computed table, or a
/// column named by `IDENTIFIER(<expr>)`. A literal name is returned as is.
fn renderQual(arena: std.mem.Allocator, q: ast.QualName, lr: LoopRow) !ast.QualName {
    for (q.parts) |p| {
        if (std.mem.indexOf(u8, p, "${") != null) break;
    } else return q;
    const parts = try arena.alloc([]const u8, q.parts.len);
    for (q.parts, parts) |s, *dst| dst.* = try interpAll(arena, s, lr);
    return .{ .parts = parts, .safe = q.safe };
}

fn renderRead(arena: std.mem.Allocator, rd: ast.Read, lr: LoopRow) !ast.Read {
    return .{ .connector = rd.connector, .form = switch (rd.form) {
        .table => |q| .{ .table = try renderQual(arena, q, lr) },
        .query => |s| .{ .query = try interpAll(arena, s, lr) },
        .path => |s| .{ .path = try interpAll(arena, s, lr) },
        .request => |d| .{ .request = d },
        .unit => .unit,
        .range => |r| .{ .range = .{
            .lo = try renderExpr(arena, r.lo, lr),
            .hi = try renderExpr(arena, r.hi, lr),
        } },
        .buffer => |b| .{ .buffer = .{
            .name = try interpAll(arena, b.name, lr),
            .dir = try interpAll(arena, b.dir, lr),
        } },
    }, .where = try interpAll(arena, rd.where, lr) };
}

/// A nested `FOR EACH`'s discovery source rendered with the enclosing row, so a
/// loop inside a loop (or inside a `CALL`ed statement function) can discover from
/// `IDENTIFIER($outer)`. The body is left as is: it renders per inner row.
pub fn renderForSource(arena: std.mem.Allocator, fe: ast.ForEach, lr: LoopRow) anyerror!ast.ForEach {
    var out = fe;
    out.source = switch (fe.source) {
        .read => |rd| .{ .read = try renderRead(arena, rd, lr) },
        .pipeline => |p| .{ .pipeline = try renderPipeline(arena, p, lr) },
        .json_path => |p| .{ .json_path = p },
    };
    return out;
}

/// Interpolate `${var}` into a union stage: the discovered form's discovery query
/// (raw text or in-engine sub-pipeline), or each explicit branch's read target +
/// tag. Lets a for-loop drive which tables a union reconciles (e.g. a per-table
/// discovery query keyed by the loop value).
fn renderUnion(arena: std.mem.Allocator, u: ast.Union, lr: LoopRow) anyerror!ast.Union {
    if (u.branches.len > 0) {
        const branches = try arena.alloc(ast.UnionBranch, u.branches.len);
        for (u.branches, branches) |b, *o| o.* = .{
            .read = try renderRead(arena, b.read, lr),
            .tag = if (b.tag) |t| try interpAll(arena, t, lr) else null,
            .pipeline = if (b.pipeline) |p| try renderPipeline(arena, p, lr) else null,
        };
        return .{ .branches = branches, .discover_conn = u.discover_conn, .discover_query = u.discover_query, .discover_json = u.discover_json, .discover_pipeline = u.discover_pipeline, .pos = u.pos };
    }
    return .{
        .branches = u.branches,
        .discover_conn = u.discover_conn,
        .discover_query = try interpAll(arena, u.discover_query, lr),
        .discover_json = try interpAll(arena, u.discover_json, lr),
        .discover_pipeline = if (u.discover_pipeline) |p| try renderPipeline(arena, p, lr) else null,
        .pos = u.pos,
    };
}

fn renderMode(arena: std.mem.Allocator, mode: ast.WriteMode, lr: LoopRow) !ast.WriteMode {
    switch (mode) {
        .upsert => |u| {
            const keys = try arena.alloc([]const u8, u.keys.len);
            for (u.keys, keys) |k, *dst| dst.* = try interpAll(arena, k, lr);
            var partial: ?[]const []const u8 = null;
            if (u.partial) |pc| {
                const out = try arena.alloc([]const u8, pc.len);
                for (pc, out) |c, *dst| dst.* = try interpAll(arena, c, lr);
                partial = out;
            }
            return .{ .upsert = .{ .keys = keys, .partial = partial } };
        },
        else => return mode,
    }
}

fn renderWrite(arena: std.mem.Allocator, w: ast.Write, lr: LoopRow) !ast.Write {
    return .{
        .connector = w.connector,
        .form = if (w.form) |f| try interpAll(arena, f, lr) else null,
        .target = try interpAll(arena, w.target, lr),
        .mode = try renderMode(arena, w.mode, lr),
    };
}

/// Instantiate the body template for one row: interpolate each `${var}` into the
/// read/write targets + upsert keys (v1 = targets only; expressions untouched).
fn renderHints(arena: std.mem.Allocator, hints: []const ast.Hint, lr: LoopRow) ![]const ast.Hint {
    if (hints.len == 0) return hints;
    const out = try arena.alloc(ast.Hint, hints.len);
    for (hints, out) |h, *o| {
        o.* = h;
        o.value = switch (h.value) {
            .str => |s| .{ .str = try interpAll(arena, s, lr) },
            .ident => |s| .{ .ident = try interpAll(arena, s, lr) },
            else => h.value,
        };
    }
    return out;
}

pub fn renderPipeline(arena: std.mem.Allocator, body: ast.Pipeline, lr: LoopRow) anyerror!ast.Pipeline {
    const stages = try arena.alloc(ast.Stage, body.stages.len);
    for (body.stages, stages) |src, *dst| {
        dst.* = src;
        dst.hints = try renderHints(arena, src.hints, lr);
        switch (src.node) {
            .read => |rd| dst.node = .{ .read = try renderRead(arena, rd, lr) },
            .union_ => |u| dst.node = .{ .union_ = try renderUnion(arena, u, lr) },
            .write => |w| dst.node = .{ .write = try renderWrite(arena, w, lr) },
            .filter => |e| dst.node = .{ .filter = try renderExpr(arena, e, lr) },
            .select => |items| dst.node = .{ .select = try renderSelect(arena, items, lr) },
            .aggregate => |ag| {
                const aggs = try arena.alloc(ast.AggItem, ag.aggs.len);
                for (ag.aggs, 0..) |a, i| aggs[i] = .{ .name = a.name, .func = a.func, .arg = if (a.arg) |e| try renderExpr(arena, e, lr) else null, .distinct = a.distinct };
                dst.node = .{ .aggregate = .{ .aggs = aggs, .by = try renderQuals(arena, ag.by, lr) } };
            },
            .distinct => |d| if (d.on) |on| {
                dst.node = .{ .distinct = .{ .on = try renderQuals(arena, on, lr) } };
            },
            .sort => |st| {
                const keys = try arena.alloc(ast.SortKey, st.keys.len);
                for (st.keys, keys) |k, *o| o.* = .{ .field = try renderQual(arena, k.field, lr), .desc = k.desc };
                dst.node = .{ .sort = .{ .keys = keys } };
            },
            else => {},
        }
    }
    return .{ .stages = stages, .pos = body.pos };
}

/// Deep-copy an expression, interpolating `${var}` needles into every string
/// literal and folding bare loop-variable references to this row's value.
/// Other leaves are reused as-is.
const RenderCtx = struct { arena: std.mem.Allocator, lr: LoopRow };

fn renderRecur(ctx: RenderCtx, e: *const ast.Expr) anyerror!*ast.Expr {
    return renderExpr(ctx.arena, e, ctx.lr);
}

fn renderExpr(arena: std.mem.Allocator, e: *const ast.Expr, lr: LoopRow) anyerror!*ast.Expr {
    if (e.* == .str_lit) return try mk(arena, .{ .str_lit = try interpAll(arena, e.str_lit, lr) });
    // `$name` parses to a plain single-part field, so a loop variable used as a
    // value is indistinguishable from a column here — bind it to the row's cell,
    // shadowing a same-named source column (the rule scalar params already use).
    // Multi-part paths (`$job.x`) belong to expand.zig and are left alone.
    if (e.* == .field) {
        if (e.field.single()) |nm| {
            if (lr.loopVar(arena, nm)) |v| return mkLit(arena, v);
        }
        const q = try renderQual(arena, e.field, lr);
        if (q.parts.ptr != e.field.parts.ptr) return mk(arena, .{ .field = q });
    }
    return ast.rebuildExpr(arena, e, RenderCtx{ .arena = arena, .lr = lr }, renderRecur);
}

fn renderQuals(arena: std.mem.Allocator, qs: []const ast.QualName, lr: LoopRow) ![]const ast.QualName {
    const out = try arena.alloc(ast.QualName, qs.len);
    for (qs, out) |q, *o| o.* = try renderQual(arena, q, lr);
    return out;
}

fn renderSelect(arena: std.mem.Allocator, items: []const ast.SelectItem, lr: LoopRow) ![]const ast.SelectItem {
    const out = try arena.alloc(ast.SelectItem, items.len);
    for (items, 0..) |it, i| out[i] = switch (it) {
        .field => |q| .{ .field = try renderQual(arena, q, lr) },
        .computed => |c| .{ .computed = .{
            .name = try interpAll(arena, c.name, lr),
            .expr = try renderExpr(arena, c.expr, lr),
        } },
        else => it,
    };
    return out;
}

/// Shared state for parallel-for workers. Per-table work uses a worker-private
/// arena/env; only the counters + the first-error buffer are shared (atomics +
/// a mutex), so no allocation ever races on the plan arena.
const ForCtx = struct {
    fe: ast.ForEach,
    needles: []const []const u8,
    rows: []const Row,
    base: *Env,
    body: BodyFn,
    worker_opts: RunOptions,
    on_error: OnError,
    outcomes: ?*OutcomeSink = null,
    /// The enclosing scope every row chains to: script scope, or the outer loop's row.
    outer: *const LoopRow,
    /// This loop is the one the progress line counts (`Progress.loopBegin`).
    counted: bool = false,
    next: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    rows_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    failures: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Workers run on their own Env, so the summary gate has to be carried back.
    wrote_sink: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    mu: std.Thread.Mutex = .{},
    /// Wide enough to hold a full `Diag` message (512) plus the row label.
    first_err_buf: [640]u8 = undefined,
    first_err_len: usize = 0,
    first_retryable: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

/// `item` is the retry identity recorded in the outcome sink (the first cell);
/// `label` is the human row identity and `msg` the already-resolved diagnostic.
fn forRecordFail(ctx: *ForCtx, item: []const u8, label: []const u8, msg: []const u8, retryable: bool, shown: bool) void {
    _ = ctx.failures.fetchAdd(1, .monotonic);
    if (ctx.outcomes) |sink| sink.recordShown(item, false, msg, retryable, shown);
    ctx.mu.lock();
    defer ctx.mu.unlock();
    if (ctx.first_err_len == 0) {
        const lab = label[0..@min(label.len, 96)];
        const why = msg[0..@min(msg.len, 512)];
        const s = std.fmt.bufPrint(&ctx.first_err_buf, "row {s}: {s}", .{ lab, why }) catch ctx.first_err_buf[0..0];
        ctx.first_err_len = s.len;
        ctx.first_retryable.store(retryable, .monotonic);
    }
    if (ctx.on_error == .stop) ctx.stop.store(true, .release);
}

fn forWorker(ctx: *ForCtx, _: usize) void {
    const gpa = ctx.base.gpa;
    while (true) {
        if (aborting()) break;
        if (ctx.on_error == .stop and ctx.stop.load(.acquire)) break;
        const i = ctx.next.fetchAdd(1, .monotonic);
        if (i >= ctx.rows.len) break;
        const row = ctx.rows[i];

        var w_arena = std.heap.ArenaAllocator.init(gpa);
        defer w_arena.deinit();
        var w_batch = std.heap.ArenaAllocator.init(gpa);
        defer w_batch.deinit();
        var w_sources = std.array_list.Managed(driver.Source).init(w_arena.allocator());
        var w_diag = Diag{};
        var w_errctx = op.ErrCtx{};
        // Copy the run's Env and override only what is per-worker. Listing the
        // fields instead silently defaulted every one that was not named — so a
        // PARALLEL for-each lost `explain` (EXPLAIN ANALYZE then really wrote
        // its sinks), `stdout_format`, and the buffer-segment / StarRocks label
        // pinning that makes a replayed flush dedup instead of duplicating.
        // A copy also cannot rot when a field is added to Env later.
        var w_env = ctx.base.*;
        w_env.arena = w_arena.allocator();
        w_env.gpa = gpa;
        w_env.sources = &w_sources;
        w_env.diag = &w_diag;
        w_env.errctx = &w_errctx;
        // The bindings map is a pointer in the copy; a body's per-row `WITH` would
        // otherwise be written by every worker at once, so each gets its own.
        var w_bindings = ctx.base.bindings.cloneWithAllocator(w_arena.allocator()) catch {
            forRecordFail(ctx, row[0], rowLabel(w_arena.allocator(), ctx.needles, row), "OutOfMemory", false, false);
            break;
        };
        w_env.bindings = &w_bindings;
        var st = Stats{ .run_id = 0 };
        var lanes: usize = 1;
        const lr = LoopRow{ .names = ctx.needles, .types = ctx.fe.var_types, .cells = row, .outer = ctx.outer };
        if (ctx.body(&w_env, ctx.fe.body, lr, ctx.worker_opts, &st, &lanes, &w_batch)) |_| {
            _ = ctx.rows_out.fetchAdd(st.rows_out, .monotonic);
            if (ctx.outcomes) |sink| sink.record(row[0], true, "", false);
        } else |e| {
            if (e == error.Aborted) {
                for (w_sources.items) |sc| sc.close();
                break;
            }
            const emsg = if (w_diag.msg.len > 0) w_diag.msg else @errorName(e);
            const label = rowLabel(w_arena.allocator(), ctx.needles, row);
            if (ctx.on_error == .continue_ and !w_env.item_reported) w_env.log.log(.err, "for-each row {s}: {s}", .{ label, emsg });
            forRecordFail(ctx, row[0], label, emsg, isTransient(e) or w_diag.retryable, w_env.item_reported);
        }
        if (w_env.wrote_sink) ctx.wrote_sink.store(true, .monotonic);
        for (w_sources.items) |sc| sc.close();
        if (ctx.base.progress) |p| p.loopTick(ctx.counted);
    }
}

/// A `CALL` renders at most this deep. Statement functions may call each other, but
/// a cycle would otherwise render forever (there is no value to terminate on, the
/// way an expression `fn` bottoms out at a literal).
const max_call_depth = 16;

/// The PARAMs and LETs as interpolation bindings, rendered to text the same way a
/// discovery cell is. `params` already holds both by the time this runs
/// (`resolveLets` folds LETs into it), and map order does not matter: names are
/// unique, so a lookup finds the same binding whatever the order.
pub fn buildScriptScope(arena: std.mem.Allocator, params: *std.StringHashMap(Value)) !LoopRow {
    const n = params.count();
    if (n == 0) return no_loop_vars;
    const names = try arena.alloc([]const u8, n);
    const cells = try arena.alloc([]const u8, n);
    var i: usize = 0;
    var it = params.iterator();
    while (it.next()) |kv| {
        names[i] = kv.key_ptr.*;
        cells[i] = try eval.valueToString(arena, kv.value_ptr.*);
        i += 1;
    }
    return .{ .names = names, .cells = cells, .script = true };
}

/// Interpolate a statement's `${...}` holes against script scope alone — what a
/// `LOAD INTO`/`SELECT` outside any `FOR EACH` needs, since `IDENTIFIER(...)` is
/// lowered to a template string by the parser and a plain script had nothing to
/// render it. Cheap to skip when the script declares no PARAM or LET, and it
/// leaves expressions alone (see `LoopRow.outer`).
pub fn renderScriptScope(env: *Env, p: ast.Pipeline) !ast.Pipeline {
    if (env.script_scope.names.len == 0) return p;
    return renderPipeline(env.arena, p, .{
        .names = &[_][]const u8{},
        .cells = &[_][]const u8{},
        .outer = &env.script_scope,
    });
}

/// `CALL f(a, b)` — a plan-time statement macro. The declared parameters become
/// loop variables and the argument values their cells, so the body is rendered and
/// executed by exactly the machinery one sequential `FOR EACH ROW OF` iteration
/// uses (`LoopRow` → `renderPipeline` → the supplied body runner); a call is one such iteration
/// over a hand-built row. `outer` is the binding in scope at the call site, so an
/// argument may be a literal, a `$param`, or an enclosing loop variable.
pub fn runCall(env: *Env, c: ast.CallStmt, outer: LoopRow, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, run_body: BodyFn) anyerror!void {
    const fd = env.fns.get(c.name) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "CALL: no function named `{s}`", .{c.name}));
    const body = switch (fd.body) {
        .stmts => |b| b,
        .expr => return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`{s}` is a scalar function — use it in an expression, not CALL", .{c.name})),
    };
    // expand.zig has already filled defaults and checked the count; a mismatch here
    // would mean an unexpanded program, so report it rather than index past the end.
    if (c.args.len != fd.params.len)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "`{s}` expects {d} argument(s), got {d}", .{ c.name, fd.params.len, c.args.len }));
    if (env.call_depth >= max_call_depth)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "CALL nesting too deep (recursive?) at `{s}`", .{c.name}));

    // Argument scope: the enclosing loop variables first (they shadow same-named
    // params, matching `runForMatch`), then the resolved params.
    var names = std.array_list.Managed([]const u8).init(env.arena);
    var values = std.array_list.Managed(Value).init(env.arena);
    try outer.appendLoopVars(env.arena, &names, &values);
    var it = env.params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }

    const pnames = try env.arena.alloc([]const u8, fd.params.len);
    const ptypes = try env.arena.alloc(?types.Type, fd.params.len);
    const cells = try env.arena.alloc([]const u8, fd.params.len);
    for (fd.params, c.args, 0..) |p, a, i| {
        pnames[i] = p.name;
        ptypes[i] = p.ty;
        const v = eval.constEval(env.arena, a, names.items, values.items) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "CALL {s}: argument `{s}`: {s}", .{ c.name, p.name, @errorName(e) }));
        cells[i] = try eval.valueToString(env.arena, v);
    }

    env.log.log(.info, "call {s}: {d} argument(s) [depth {d}]", .{ c.name, cells.len, env.call_depth + 1 });
    env.call_depth += 1;
    defer env.call_depth -= 1;
    try run_body(env, body, .{ .names = pnames, .types = ptypes, .cells = cells, .outer = &env.script_scope }, opts, stats, lanes_used, batch_arena);
}

/// `THROW 'msg' [WHEN cond];` — the script's own precondition. Both operands fold
/// against the loop variables in scope (they shadow same-named params, as in
/// `runCall`) and then the resolved params; only a `true` condition fires, and an
/// absent one always does. The message becomes the run's error text unchanged.
/// A fired guard is permanent — `retryable` is cleared so no scheduler retries it.
/// Render a `PRINT` argument to its output text. The loop variables in scope come
/// first so they shadow a same-named param, matching `runForMatch` and `runCall`;
/// the result is formatted by the same helper that turns a cell into text
/// everywhere else, so a non-string renders exactly as it would in a sink.
pub fn printText(arena: std.mem.Allocator, e: *const ast.Expr, lr: LoopRow, params: *std.StringHashMap(Value)) ![]const u8 {
    var names = std.array_list.Managed([]const u8).init(arena);
    var values = std.array_list.Managed(Value).init(arena);
    try lr.appendLoopVars(arena, &names, &values);
    var it = params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }
    return eval.valueToString(arena, try eval.constEval(arena, e, names.items, values.items));
}

/// `PRINT <expr>;` — a script's own progress line. It goes to stderr through the
/// run logger at `info`, so it inherits the level filter, `--log-format json` and
/// the mutex that keeps parallel lanes from interleaving; stdout stays reserved
/// for data (`--format json` NDJSON rows or the run summary object).
pub fn runPrint(env: *Env, p: ast.Print, lr: LoopRow) anyerror!void {
    const text = printText(env.arena, p.expr, lr, env.params) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "PRINT: {s}", .{@errorName(e)}));
    env.log.script(text);
}

pub fn runThrow(env: *Env, t: ast.Throw, outer: LoopRow) anyerror!void {
    var names = std.array_list.Managed([]const u8).init(env.arena);
    var values = std.array_list.Managed(Value).init(env.arena);
    try outer.appendLoopVars(env.arena, &names, &values);
    var it = env.params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }

    if (t.when) |w| {
        const c = eval.constEval(env.arena, w, names.items, values.items) catch |e|
            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "THROW condition: {s}", .{@errorName(e)}));
        if (!(c == .bool and c.bool)) return;
    }
    const m = eval.constEval(env.arena, t.message, names.items, values.items) catch |e|
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "THROW message: {s}", .{@errorName(e)}));
    env.diag.retryable = false;
    return planErr(env.diag, try eval.valueToString(env.arena, m));
}

/// Expand a `for <vars> in <source>` block, running its body once per discovered row.
/// `mode` (sequential|parallel) and `on_error` (stop|continue) come from `@[...]`.
pub fn runForEach(env: *Env, fe: ast.ForEach, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator, run_body: BodyFn, outer: *const LoopRow) !void {
    const mode: ForMode = if (forHintIdent(fe.hints, "mode")) |m|
        (if (std.mem.eql(u8, m, "parallel")) ForMode.parallel else ForMode.sequential)
    else
        .sequential;
    const on_error: OnError = if (forHintIdent(fe.hints, "on_error")) |m|
        (if (std.mem.eql(u8, m, "continue")) OnError.continue_ else OnError.stop)
    else
        .stop;

    const rows = switch (fe.source) {
        .read => |rd| try discoverRows(env, rd, fe.var_names.len),
        .json_path => |p| try discoverRowsJson(env, p, fe.var_names),
        .pipeline => |p| try discoverRowsPipeline(env, p, fe.var_names.len),
    };
    env.log.log(.debug, "for-each {s}: {d} row(s) [{s}, on_error={s}]", .{ fe.var_names[0], rows.len, @tagName(mode), if (on_error == .continue_) "continue" else "stop" });
    if (rows.len == 0) return;
    const needles = fe.var_names;
    const counted = if (env.progress) |p| p.loopBegin(rows.len) else false;
    defer if (env.progress) |p| p.loopEnd();

    switch (mode) {
        .sequential => {
            var failures: usize = 0;
            var first_err: ?[]const u8 = null;
            for (rows, 0..) |row, ri| {
                if (aborting()) return error.Aborted;
                if (ri > 0) if (env.progress) |p| p.loopTick(counted);
                const base = env.sources.items.len;
                // Cleared per row so a failure never reports the previous row's message.
                env.diag.retryable = false;
                env.diag.msg = "";
                env.diag.pos = null;
                const lr = LoopRow{ .names = needles, .types = fe.var_types, .cells = row, .outer = outer };
                if (run_body(env, fe.body, lr, opts, stats, lanes_used, batch_arena)) |_| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    if (opts.outcomes) |sink| sink.record(row[0], true, "", false);
                } else |e| {
                    for (env.sources.items[base..]) |sc| sc.close();
                    env.sources.shrinkRetainingCapacity(base);
                    if (e == error.Aborted) return error.Aborted;
                    failures += 1;
                    const emsg = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
                    const label = rowLabel(env.arena, needles, row);
                    if (opts.outcomes) |sink| sink.recordShown(row[0], false, emsg, isTransient(e) or env.diag.retryable, env.item_reported);
                    // In stop mode the same text comes back out as the run's error, so
                    // only a loop that carries on logs the row here.
                    if (on_error == .continue_ and !env.item_reported) env.log.log(.err, "for-each row {s}: {s}", .{ label, emsg });
                    env.item_reported = false;
                    if (first_err == null)
                        first_err = std.fmt.allocPrint(env.arena, "row {s}: {s}", .{ label, emsg }) catch null;
                    if (on_error == .stop) {
                        if (isTransient(e)) env.diag.retryable = true;
                        // `emsg` may alias `diag.buf`; rewriting the diag with it would
                        // be a self-copy, so on an alloc failure keep what is there.
                        if (first_err) |why|
                            return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each {s}", .{why}));
                        return error.PlanFailed;
                    }
                }
            }
            if (failures > 0 and opts.outcomes == null)
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ failures, rows.len, first_err orelse "?" }));
        },
        .parallel => {
            var wopts = opts;
            wopts.threads = 1;
            const nworkers = @min(@max(opts.threads, @as(usize, 1)), rows.len);
            var ctx = ForCtx{ .fe = fe, .needles = needles, .rows = rows, .base = env, .body = run_body, .worker_opts = wopts, .on_error = on_error, .outcomes = opts.outcomes, .outer = outer, .counted = counted };
            const lanes = try parallel.spawnJoin(env.arena, nworkers, forWorker, &ctx);
            if (aborting()) return error.Aborted;
            stats.rows_out += ctx.rows_out.load(.monotonic);
            lanes_used.* = @max(lanes_used.*, lanes);
            if (ctx.wrote_sink.load(.monotonic)) env.wrote_sink = true;
            const fails = ctx.failures.load(.monotonic);
            if (fails > 0 and (on_error == .stop or opts.outcomes == null)) {
                if (ctx.first_retryable.load(.monotonic)) env.diag.retryable = true;
                const first = ctx.first_err_buf[0..ctx.first_err_len];
                const why = if (on_error == .stop)
                    try std.fmt.allocPrint(env.arena, "for-each {s}", .{first})
                else
                    try std.fmt.allocPrint(env.arena, "for-each: {d}/{d} failed (first: {s})", .{ fails, rows.len, first });
                return planErr(env.diag, why);
            }
        },
    }
}
