//! `DELETE FROM` on a database table or a Delta table. The parser reads it as the rows
//! to delete: the table read, conditions on its columns, and each IN or EXISTS
//! subquery as a semi or anti join on a binding.
//!
//! On a Delta table the engine finds the rows: the conditions and joins are built
//! as any query's are over the table's files read with each row's file and position
//! (`lake/delta_delete.zig`), and the rows they keep are deleted in one commit.
//!
//! On a database none of that is read here. The conditions go to the
//! database as SQL, written as they were (`pushdown/translate`, `need = .source`: the
//! database decides its text comparisons, as for any statement it runs); each
//! subquery's rows, its key columns alone, are loaded first into a staging table of
//! the run's own on that database, on the lanes as any load; then one `DELETE ... WHERE
//! ... [NOT] EXISTS (SELECT 1 FROM staging ...)` deletes them in a single statement,
//! and the staging tables are dropped, whether it ran or failed. A `NOT IN` stays
//! `NOT IN`, so the database applies SQL's rule for a NULL in the subquery.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const sql = @import("../../db/sql.zig");
const replace = @import("../../db/replace.zig");
const connect = @import("../connect.zig");
const dbconfig = @import("../connect/dbconfig.zig");
const options = @import("../analyze/options.zig");
const env_mod = @import("../env.zig");
const stampedBinding = @import("../plan.zig").stampedBinding;
const analyze = @import("../analyze.zig");
const plan = @import("../plan.zig");
const op = @import("../../exec/op.zig");
const Folder = @import("../../format/parquet/folder.zig").Folder;
const lake_delete = @import("../../format/lake/delta_delete.zig");
const eval = @import("../../exec/eval.zig");
const print = @import("../../lang/print.zig");
const warnLog = @import("../connect/formats_open.zig").warnLog;

const Env = env_mod.Env;
const RunOptions = env_mod.RunOptions;
const Stats = env_mod.Stats;
const planErr = env_mod.planErr;
const planErrT = env_mod.planErrT;

const Member = struct { join: ast.Join, staging: []const u8 = "" };

/// Runs `out`, a DELETE's pipeline (`ast.Write.delete` set), on its database table.
pub fn runDelete(env: *Env, out: ast.Pipeline, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const stages = out.stages;
    const w = stages[stages.len - 1].node.write;
    const between = stages[1 .. stages.len - 1];
    if (try analyze.deleteProblem(env.arena, w, between, env.connections, env.params_expr, false)) |why| return planErr(env.diag, why);
    const conn = env.connections.get(w.connector) orelse return runDeleteDelta(env, w, between, opts, stats);
    const info = connect.sqlConnInfo(conn).?;
    const d = info.dialect;
    const table = analyze.deleteTable(w.target);
    var conds = std.array_list.Managed([]const u8).init(env.arena);
    switch (try analyze.deleteConds(env.arena, w, between, d, env.params_expr, false)) {
        .sql => |texts| try conds.appendSlice(texts),
        .unbound => |name| return planErr(env.diag, try msgfmt.allocPrint(env.arena, "DELETE FROM {s}: `${s}` has no value to send to the database", .{ w.target, name })),
        .unsendable => unreachable,
    }
    var members = std.array_list.Managed(Member).init(env.arena);
    for (between) |st| if (st.node == .join) try members.append(.{ .join = st.node.join });

    const dry = env.explain or opts.explain;
    var staged: usize = 0;
    defer for (members.items[0..staged]) |m| dropStaging(env, info, conn, m.staging);
    for (members.items) |*m| {
        m.staging = try replace.stagingName(env.arena, w.target, std.crypto.random.int(u32));
        if (!dry) {
            staged += 1;
            try stageMember(env, w, m.*, opts, lanes_used, batch_arena);
        }
        try conds.append(try memberSql(env.arena, d, table, m.*));
        if (m.join.null_aware and !dry) try warnNullKey(env, info, conn, w.target, m.*);
    }

    const qt = try sql.quoteIdent(env.arena, d, w.target);
    const stmt = if (conds.items.len == 0)
        try msgfmt.allocPrint(env.arena, "DELETE FROM {s}", .{qt})
    else
        try msgfmt.allocPrint(env.arena, "DELETE FROM {s} WHERE {s}", .{ qt, try std.mem.join(env.arena, " AND ", conds.items) });
    if (dry) {
        env.log.script(try msgfmt.allocPrint(env.arena, "DELETE FROM {s}: would run `{s}`", .{ w.target, stmt }));
        return;
    }
    const cfg = try dbconfig.resolveDbConfig(env, conn, info.port);
    const t0 = std.time.milliTimestamp();
    const deleted: ?u64 = switch (info.kind) {
        inline else => |k| blk: {
            const c = connect.SqlDriver(k).connect(env.gpa, cfg) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "DELETE FROM {s}: {s} connect failed: {s}", .{ w.target, conn.connector, try dbconfig.connectWhy(env.arena, e) }));
            defer c.close();
            const n = c.execBatch(stmt) catch |e| {
                if (e == error.OutOfMemory) return e;
                const why = if (c.wire.last_error.len > 0) c.wire.last_error else @errorName(e);
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "DELETE FROM {s} failed: {s}", .{ w.target, why }));
            };
            break :blk if (k == .sqlserver) c.last_done_count else n;
        },
    };
    stats.rows_out += deleted orelse 0;
    const ms = std.time.milliTimestamp() - t0;
    if (deleted) |n|
        env.log.script(try msgfmt.allocPrint(env.arena, "Deleted {d} row{s} from {s} in {d} ms", .{ n, if (n == 1) "" else "s", w.target, ms }))
    else
        env.log.script(try msgfmt.allocPrint(env.arena, "Deleted from {s} in {d} ms", .{ w.target, ms }));
}

/// `DELETE FROM` Delta table `w.target`: the rows of its files that the stages
/// `between` keep, found on the engine, deleted in one commit; under EXPLAIN only
/// counted.
fn runDeleteDelta(env: *Env, w: ast.Write, between_in: []const ast.Stage, opts: RunOptions, stats: *Stats) !void {
    const a = env.arena;
    const between = try analyze.substFilterParams(a, between_in, env.params_expr);
    var conds = std.array_list.Managed(*const ast.Expr).init(a);
    var texts = std.array_list.Managed([]const u8).init(a);
    var subqueries = false;
    for (between) |st| switch (st.node) {
        .filter => |f| {
            const own = try analyze.deleteOwn(a, w, f);
            try conds.append(own);
            var aw = std.Io.Writer.Allocating.init(a);
            print.expr(&aw.writer, own) catch return error.OutOfMemory;
            try texts.append(aw.written());
        },
        .join => |j| {
            subqueries = true;
            try texts.append(if (j.null_aware)
                try msgfmt.allocPrint(a, "{s} NOT IN (subquery)", .{j.left_keys[0].last()})
            else if (j.kind == .anti) "NOT EXISTS (subquery)" else "EXISTS (subquery)");
        },
        else => {},
    };
    const t0 = std.time.milliTimestamp();
    const t = lake_delete.open(a, w.target, conds.items, !subqueries) catch |e| return lakeFailed(env, e);
    const dry = env.explain or opts.explain;
    if (dry and !opts.explain) {
        const whole = if (t.whole.len == 0) "" else try msgfmt.allocPrint(a, ", and remove {d} whole by its partition values", .{t.whole.len});
        env.log.script(try msgfmt.allocPrint(a, "DELETE FROM {s}: would read {d} of its {d} file{s}{s}", .{ w.target, t.match.len, t.st.files.count(), if (t.st.files.count() == 1) "" else "s", whole }));
        return;
    }
    const hits = try a.alloc(std.DynamicBitSetUnmanaged, t.match.len);
    for (hits) |*h| h.* = .{};
    if (t.match.len > 0) try findRows(env, &t, between, hits);
    if (!dry) for (between) |st| if (st.node == .join and st.node.join.null_aware) try warnNullKeyEngine(env, w.target, st.node.join);
    if (dry) {
        const n = try lake_delete.rowsOf(a, &t, hits);
        env.log.script(try msgfmt.allocPrint(a, "DELETE FROM {s}: would delete {d} row{s}, reading {d} of its {d} file{s}", .{ w.target, n, if (n == 1) "" else "s", t.match.len, t.st.files.count(), if (t.st.files.count() == 1) "" else "s" }));
        return;
    }
    const got = lake_delete.apply(env.gpa, a, &t, hits, try std.mem.join(a, " AND ", texts.items), .{ .space = env.space, .op_memory = env.op_memory, .threads = env.pipe.threads, .err = env.errctx, .warn = .{ .ctx = env.log, .f = warnLog } }) catch |e| return lakeFailed(env, e);
    stats.rows_out += got.rows;
    const ms = std.time.milliTimestamp() - t0;
    env.log.script(try msgfmt.allocPrint(a, "Deleted {d} row{s} from {s} in {d} ms ({d} file{s} removed, {d} added)", .{ got.rows, if (got.rows == 1) "" else "s", w.target, ms, got.removed, if (got.removed == 1) "" else "s", got.added }));
}

/// Marks in `hits` (one per file of `t.match`) the position of each row the stages
/// `between` keep, built over `t.layout` as a query's stages are over its read.
fn findRows(env: *Env, t: *const lake_delete.Target, between: []const ast.Stage, hits: []std.DynamicBitSetUnmanaged) !void {
    const src = try Folder.openLayout(env.arena, t.layout, null);
    const s = src.source();
    defer s.close();
    const scan = try env.arena.create(op.Scan);
    scan.* = .{ .src = s };
    var current: op.Op = .{ .scan = scan };
    var schema = t.layout.schema;
    for (between) |st| {
        if (st.node == .select) continue;
        const r = try plan.buildStage(env, st, current, schema);
        current = r.op;
        schema = r.schema;
    }
    defer current.release();
    const ti = schema.indexOf(lake_delete.tag_name).?;
    const pi = schema.indexOf(lake_delete.pos_name).?;
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    while (try current.next(scratch.allocator())) |b| {
        for (0..b.len) |r| {
            const h = &hits[@intCast(b.columns[ti].getValue(r).int)];
            const p: usize = @intCast(b.columns[pi].getValue(r).int);
            if (p >= h.bit_length) try h.resize(env.arena, @max(p + 1, h.bit_length * 2), false);
            h.set(p);
        }
        _ = scratch.reset(.retain_capacity);
    }
}

/// SQL's rule for a NULL among a `NOT IN` subquery's rows, said as a warning when
/// the subquery, run once more on the engine, holds one.
fn warnNullKeyEngine(env: *Env, target: []const u8, j: ast.Join) !void {
    const b = (try stampedBinding(env, j.binding)) orelse return;
    const rk = j.right_keys[0];
    const key = try ast.mk(env.arena, .{ .field = .{ .parts = try env.arena.dupe([]const u8, &.{rk.last()}), .span = rk.span } });
    const test_null = try ast.mk(env.arena, .{ .is_null = .{ .e = key, .negated = false } });
    const stages = try std.mem.concat(env.arena, ast.Stage, &.{ b.stages, &.{.{ .node = .{ .filter = test_null }, .hints = &.{}, .pos = b.pos }} });
    const pipe = try plan.buildPipeline(env, stages);
    defer pipe.op.release();
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    var n: u64 = 0;
    while (try pipe.op.next(scratch.allocator())) |bt| {
        n += bt.len;
        _ = scratch.reset(.retain_capacity);
    }
    if (n > 0) env.log.log(.warn, "DELETE FROM {s}: the NOT IN subquery returned {d} NULL{s}, so by SQL's rule no row is deleted; `WHERE {s} IS NOT NULL` in the subquery leaves them out", .{ target, n, if (n == 1) "" else "s", rk.last() });
}

/// A Delta table's refusal or failure as the run's error, in its own words.
fn lakeFailed(env: *Env, e: anyerror) anyerror {
    if (e == error.OutOfMemory) return e;
    return planErrT(env.diag, e, eval.takeFailure(e) orelse @errorName(e));
}

/// Loads subquery `m`'s rows, its key columns alone, into its staging table on the
/// DELETE's database, as an APPEND that creates the table.
fn stageMember(env: *Env, w: ast.Write, m: Member, opts: RunOptions, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const b = (try stampedBinding(env, m.join.binding)) orelse return planErr(env.diag, try msgfmt.allocPrint(env.arena, "DELETE FROM {s}: unknown subquery `{s}`", .{ w.target, m.join.binding }));
    const keys = try env.arena.alloc(ast.SelectItem, m.join.right_keys.len);
    for (keys, m.join.right_keys) |*k, rk| k.* = .{ .field = .{ .parts = try env.arena.dupe([]const u8, &.{rk.last()}), .span = rk.span } };
    var st = std.array_list.Managed(ast.Stage).init(env.arena);
    try st.appendSlice(b.stages);
    try st.append(.{ .node = .{ .select = keys }, .hints = &.{}, .pos = b.pos });
    try st.append(.{ .node = .{ .write = .{ .connector = w.connector, .form = null, .target = m.staging, .mode = .append } }, .hints = &.{}, .pos = b.pos });
    var scratch: Stats = .{};
    const outer_target = env.last_target;
    const outer_loads = env.loads;
    const outer_hook = env.on_load;
    const outer_wrote = env.wrote_sink;
    env.loads = null;
    env.on_load = null;
    defer {
        env.last_target = outer_target;
        env.loads = outer_loads;
        env.on_load = outer_hook;
        env.wrote_sink = outer_wrote;
    }
    try @import("output.zig").runOutput(env, .{ .stages = try st.toOwnedSlice(), .pos = b.pos }, opts, &scratch, lanes_used, batch_arena);
}

/// Subquery `m` as a condition on the table `table` names: `[NOT] EXISTS` over its
/// staging table matched on the keys, or `NOT IN` for a `NOT IN`.
fn memberSql(a: std.mem.Allocator, d: sql.Dialect, table: []const u8, m: Member) ![]const u8 {
    const qs = try sql.quoteIdent(a, d, m.staging);
    const qtab = try sql.quoteIdent(a, d, table);
    if (m.join.null_aware)
        return msgfmt.allocPrint(a, "{s}.{s} NOT IN (SELECT {s} FROM {s})", .{ qtab, try sql.quoteIdent(a, d, m.join.left_keys[0].last()), try sql.quoteIdent(a, d, m.join.right_keys[0].last()), qs });
    var eqs = std.array_list.Managed([]const u8).init(a);
    for (m.join.left_keys, m.join.right_keys) |lk, rk|
        try eqs.append(try msgfmt.allocPrint(a, "s.{s} = {s}.{s}", .{ try sql.quoteIdent(a, d, rk.last()), qtab, try sql.quoteIdent(a, d, lk.last()) }));
    return msgfmt.allocPrint(a, "{s}EXISTS (SELECT 1 FROM {s} s WHERE {s})", .{ if (m.join.kind == .anti) "NOT " else "", qs, try std.mem.join(a, " AND ", eqs.items) });
}

/// SQL's rule for a NULL among a `NOT IN` subquery's rows, that no row is then
/// deleted, said as a warning when the staged rows hold one.
fn warnNullKey(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, target: []const u8, m: Member) !void {
    const d = info.dialect;
    const rk = try sql.quoteIdent(env.arena, d, m.join.right_keys[0].last());
    const q = try msgfmt.allocPrint(env.arena, "SELECT COUNT(*) FROM {s} WHERE {s} IS NULL", .{ try sql.quoteIdent(env.arena, d, m.staging), rk });
    const cfg = dbconfig.resolveDbConfig(env, conn, info.port) catch return;
    const c = connect.connectSql(env.gpa, info.kind, cfg) catch return;
    const cur = c.queryCursor(q) catch {
        c.close();
        return;
    };
    defer cur.close();
    const b = (cur.nextBatch(env.arena) catch return) orelse return;
    if (b.len == 0) return;
    const v = b.columns[0].getValue(0);
    const n: i64 = switch (v) {
        .int => |x| x,
        .decimal => |x| @intCast(x.unscaled),
        else => return,
    };
    if (n > 0) env.log.log(.warn, "DELETE FROM {s}: the NOT IN subquery returned {d} NULL{s}, so by SQL's rule no row is deleted; `WHERE {s} IS NOT NULL` in the subquery leaves them out", .{ target, n, if (n == 1) "" else "s", m.join.right_keys[0].last() });
}

fn dropStaging(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, staging: []const u8) void {
    const cfg = dbconfig.resolveDbConfig(env, conn, info.port) catch return;
    const c = connect.connectSql(env.gpa, info.kind, cfg) catch return;
    defer c.close();
    const drop = replace.dropSql(env.arena, info.dialect, staging) catch return;
    c.exec(drop) catch {};
}
