//! `DELETE FROM` on a database table. The parser reads it as the rows to delete: the
//! table read, conditions on its columns, and each IN or EXISTS subquery as a semi or
//! anti join on a binding. None of that is read here. The conditions go to the
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
    const conn = env.connections.get(w.connector) orelse
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`DELETE FROM` a Delta table `{s}` is not supported yet; it deletes from a database table", .{w.target}));
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
