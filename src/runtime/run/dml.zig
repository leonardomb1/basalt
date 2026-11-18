//! `DELETE FROM` on a database table or a Delta table. The parser reads it as the rows
//! to delete: the table read, conditions on its columns, and each IN or EXISTS
//! subquery as a semi or anti join on a binding.
//!
//! On a Delta table the engine finds the rows: the conditions and joins are built
//! as any query's are over the table's files read with each row's file and position
//! (`lake/delta_dml.zig`), and the rows they keep are deleted, or updated, in one
//! commit.
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
const update_rules = @import("../analyze/update.zig");
const plan = @import("../plan.zig");
const op = @import("../../exec/op.zig");
const Folder = @import("../../format/parquet/folder.zig").Folder;
const lake_dml = @import("../../format/lake/delta_dml.zig");
const eval = @import("../../exec/eval.zig");
const print = @import("../../lang/print.zig");
const types = @import("../../lang/types.zig");
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
    const table = analyze.dmlTable(w.target);
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
        if (m.join.null_aware and !dry) try warnNullKey(env, info, conn, try msgfmt.allocPrint(env.arena, "DELETE FROM {s}", .{w.target}), "deleted", m.*);
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
    const t0 = std.time.milliTimestamp();
    const deleted = try execStmt(env, info, conn, try msgfmt.allocPrint(env.arena, "DELETE FROM {s}", .{w.target}), stmt);
    stats.rows_out += deleted orelse 0;
    const ms = std.time.milliTimestamp() - t0;
    if (deleted) |n|
        env.log.script(try msgfmt.allocPrint(env.arena, "Deleted {d} row{s} from {s} in {d} ms", .{ n, if (n == 1) "" else "s", w.target, ms }))
    else
        env.log.script(try msgfmt.allocPrint(env.arena, "Deleted from {s} in {d} ms", .{ w.target, ms }));
}

/// Runs `out`, an UPDATE's pipeline (`ast.Write.update` set), on its table. On a
/// database the FROM source's columns the statement reads, and each subquery's
/// keys, are staged into tables of the run's own as a DELETE's are; a key of the
/// source on more than one row matching a row to update is refused; then one
/// statement, in the dialect's form of an update joined to a table, changes the rows.
pub fn runUpdate(env: *Env, out: ast.Pipeline, opts: RunOptions, stats: *Stats, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) anyerror!void {
    const a = env.arena;
    const stages = out.stages;
    const w = stages[stages.len - 1].node.write;
    const u = w.update.?;
    const between = stages[1 .. stages.len - 1];
    if (try analyze.updateProblem(a, w, between, env.connections, env.params_expr, null, false)) |why| return planErr(env.diag, why);
    const conn = env.connections.get(w.connector) orelse return runUpdateDelta(env, w, between, opts, stats);
    const info = connect.sqlConnInfo(conn).?;
    const d = info.dialect;
    var why: []const u8 = "";
    const sh = (try update_rules.shapeOf(a, u, between, &why)).?;
    const table = analyze.dmlTable(w.target);
    const what = try msgfmt.allocPrint(a, "UPDATE {s}", .{w.target});
    const built = switch (try update_rules.build(a, u, table, sh, d, env.params_expr, false)) {
        .sql => |x| x,
        .unbound => |name| return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: `${s}` has no value to send to the database", .{ what, name })),
        .unsendable => unreachable,
    };
    const dry = env.explain or opts.explain;
    var drops = std.array_list.Managed([]const u8).init(a);
    defer for (drops.items) |t| dropStaging(env, info, conn, t);

    var conds = std.array_list.Managed([]const u8).init(a);
    try conds.appendSlice(built.conds[built.keys.len..]);
    const src: ?[]const u8 = if (sh.from) |j| blk: {
        const staging = try replace.stagingName(a, w.target, std.crypto.random.int(u32));
        if (!dry) {
            try drops.append(staging);
            const items = try a.alloc(ast.SelectItem, built.src_cols.len);
            for (items, built.src_cols) |*it, c| it.* = .{ .computed = .{ .name = try update_rules.stagedName(a, c), .expr = try ast.mk(a, .{ .field = .{ .parts = try a.dupe([]const u8, &.{c}) } }) } };
            try stageRows(env, w, j.binding, items, staging, opts, lanes_used, batch_arena);
            try checkFromKeys(env, info, conn, what, w.target, j, staging, built.conds[built.keys.len .. built.keys.len + sh.pre.len]);
        }
        break :blk staging;
    } else null;
    for (sh.members) |j| {
        const m = Member{ .join = j, .staging = try replace.stagingName(a, w.target, std.crypto.random.int(u32)) };
        if (!dry) {
            try drops.append(m.staging);
            try stageMember(env, w, m, opts, lanes_used, batch_arena);
            if (j.null_aware) try warnNullKey(env, info, conn, what, "updated", m);
        }
        try conds.append(try memberSql(a, d, table, m));
    }

    const qt = try sql.quoteIdent(a, d, w.target);
    const sets = try std.mem.join(a, ", ", built.sets);
    const where = if (conds.items.len == 0) "" else try msgfmt.allocPrint(a, " WHERE {s}", .{try std.mem.join(a, " AND ", conds.items)});
    const on = try std.mem.join(a, " AND ", built.keys);
    const stmt = if (src) |staging| blk: {
        const qs = try sql.quoteIdent(a, d, staging);
        break :blk switch (d) {
            .mysql, .starrocks, .doris => try msgfmt.allocPrint(a, "UPDATE {s} JOIN {s} s ON {s} SET {s}{s}", .{ qt, qs, on, sets, where }),
            .sqlserver => try msgfmt.allocPrint(a, "UPDATE basalt_target SET {s} FROM {s} AS basalt_target JOIN {s} s ON {s}{s}", .{ sets, qt, qs, on, where }),
            else => try msgfmt.allocPrint(a, "UPDATE {s} SET {s} FROM {s} s WHERE {s}{s}", .{ qt, sets, qs, on, if (where.len > 0) try msgfmt.allocPrint(a, " AND {s}", .{where[" WHERE ".len..]}) else "" }),
        };
    } else try msgfmt.allocPrint(a, "UPDATE {s} SET {s}{s}", .{ qt, sets, where });
    if (dry) {
        env.log.script(try msgfmt.allocPrint(a, "{s}: would run `{s}`", .{ what, stmt }));
        return;
    }
    const t0 = std.time.milliTimestamp();
    const changed = try execStmt(env, info, conn, what, stmt);
    stats.rows_out += changed orelse 0;
    const ms = std.time.milliTimestamp() - t0;
    if (changed) |n|
        env.log.script(try msgfmt.allocPrint(a, "Updated {d} row{s} of {s} in {d} ms", .{ n, if (n == 1) "" else "s", w.target, ms }))
    else
        env.log.script(try msgfmt.allocPrint(a, "Updated {s} in {d} ms", .{ w.target, ms }));
}

/// Refuses an UPDATE whose FROM source, staged as `staging`, holds a key on more than
/// one row where a row of the table to update (by its conditions `pre`) has it, as
/// that row would take one of several values.
fn checkFromKeys(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, what: []const u8, target: []const u8, j: ast.Join, staging: []const u8, pre: []const []const u8) !void {
    const a = env.arena;
    const d = info.dialect;
    var keys = std.array_list.Managed([]const u8).init(a);
    var match = std.array_list.Managed([]const u8).init(a);
    for (j.left_keys, j.right_keys) |lk, rk| {
        const sk = try sql.quoteIdent(a, d, try update_rules.stagedName(a, rk.last()));
        try keys.append(sk);
        try match.append(try msgfmt.allocPrint(a, "{s} = d.{s}", .{ try sql.quoteIdent(a, d, lk.last()), sk }));
    }
    try match.appendSlice(pre);
    const ks = try std.mem.join(a, ", ", keys.items);
    const q = try msgfmt.allocPrint(a, "SELECT COUNT(*) FROM (SELECT {s} FROM {s} GROUP BY {s} HAVING COUNT(*) > 1) d WHERE EXISTS (SELECT 1 FROM {s} WHERE {s})", .{ ks, try sql.quoteIdent(a, d, staging), ks, try sql.quoteIdent(a, d, target), try std.mem.join(a, " AND ", match.items) });
    const n = countOf(env, info, conn, q) orelse return;
    if (n > 0) return planErr(env.diag, try msgfmt.allocPrint(a, "{s}: the FROM source holds {d} key{s} on more than one row, matching rows to update, so each such row would take one of several values; give each key once", .{ what, n, if (n == 1) "" else "s" }));
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
            const own = try analyze.dmlOwn(a, w, f);
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
    const t = lake_dml.open(a, w.target, .delete, conds.items, !subqueries) catch |e| return lakeFailed(env, e);
    const dry = env.explain or opts.explain;
    if (dry and !opts.explain) {
        const whole = if (t.whole.len == 0) "" else try msgfmt.allocPrint(a, ", and remove {d} whole by its partition values", .{t.whole.len});
        env.log.script(try msgfmt.allocPrint(a, "DELETE FROM {s}: would read {d} of its {d} file{s}{s}", .{ w.target, t.match.len, t.st.files.count(), if (t.st.files.count() == 1) "" else "s", whole }));
        return;
    }
    const hits = try a.alloc(std.DynamicBitSetUnmanaged, t.match.len);
    for (hits) |*h| h.* = .{};
    if (t.match.len > 0) try findRows(env, &t, between, hits);
    if (!dry) for (between) |st| if (st.node == .join and st.node.join.null_aware) try warnNullKeyEngine(env, try msgfmt.allocPrint(a, "DELETE FROM {s}", .{w.target}), "deleted", st.node.join);
    if (dry) {
        const n = try lake_dml.rowsOf(a, &t, hits);
        env.log.script(try msgfmt.allocPrint(a, "DELETE FROM {s}: would delete {d} row{s}, reading {d} of its {d} file{s}", .{ w.target, n, if (n == 1) "" else "s", t.match.len, t.st.files.count(), if (t.st.files.count() == 1) "" else "s" }));
        return;
    }
    const got = lake_dml.delete(env.gpa, a, &t, hits, try std.mem.join(a, " AND ", texts.items), .{ .space = env.space, .op_memory = env.op_memory, .threads = env.pipe.threads, .err = env.errctx, .warn = .{ .ctx = env.log, .f = warnLog } }) catch |e| return lakeFailed(env, e);
    stats.rows_out += got.rows;
    const ms = std.time.milliTimestamp() - t0;
    env.log.script(try msgfmt.allocPrint(a, "Deleted {d} row{s} from {s} in {d} ms ({d} file{s} removed, {d} added)", .{ got.rows, if (got.rows == 1) "" else "s", w.target, ms, got.removed, if (got.removed == 1) "" else "s", got.added }));
}

/// `UPDATE` of Delta table `w.target`: the rows of its files that the stages
/// `between` keep, found on the engine with their new values, which are written as
/// they come; then their files are rewritten and the commit made. Under EXPLAIN
/// they are only counted.
fn runUpdateDelta(env: *Env, w: ast.Write, between_in: []const ast.Stage, opts: RunOptions, stats: *Stats) !void {
    const a = env.arena;
    const u = w.update.?;
    const between = try analyze.substFilterParams(a, between_in, env.params_expr);
    var conds = std.array_list.Managed(*const ast.Expr).init(a);
    var texts = std.array_list.Managed([]const u8).init(a);
    var joined = false;
    for (between) |st| switch (st.node) {
        .filter => |f| {
            const own = try analyze.dmlOwn(a, w, f);
            if (!joined) try conds.append(own);
            try texts.append(try update_rules.exprText(a, own));
        },
        .join => |j| {
            joined = true;
            try texts.append(if (j.kind == .inner) "FROM (source)" else if (j.null_aware)
                try msgfmt.allocPrint(a, "{s} NOT IN (subquery)", .{j.left_keys[0].last()})
            else if (j.kind == .anti) "NOT EXISTS (subquery)" else "EXISTS (subquery)");
        },
        else => {},
    };
    const t0 = std.time.milliTimestamp();
    const t = lake_dml.open(a, w.target, .update, conds.items, false) catch |e| return lakeFailed(env, e);
    const cols: types.Schema = .{ .fields = t.layout.schema.fields[0..t.width] };
    if (try analyze.updateProblem(a, w, between_in, env.connections, env.params_expr, cols, false)) |why| return planErr(env.diag, why);
    const dry = env.explain or opts.explain;
    if (dry and !opts.explain) {
        env.log.script(try msgfmt.allocPrint(a, "UPDATE {s}: would read {d} of its {d} file{s}", .{ w.target, t.match.len, t.st.files.count(), if (t.st.files.count() == 1) "" else "s" }));
        return;
    }
    const sets = try a.alloc(ast.SelectItem, t.width + 2);
    for (sets[0..t.width], cols.fields) |*it, f| {
        it.* = .{ .field = .{ .parts = try a.dupe([]const u8, &.{f.name}) } };
        if (std.mem.eql(u8, f.name, @import("../../format/lake/delta.zig").row_version_name)) {
            it.* = .{ .computed = .{ .name = f.name, .expr = try ast.mk(a, .{ .cast = .{ .e = try ast.mk(a, .null_lit), .ty = f.ty } }) } };
            continue;
        }
        for (u.sets) |set| if (std.ascii.eqlIgnoreCase(set.column, f.name)) {
            const e = try analyze.dmlOwn(a, w, try analyze.substExpr(a, set.expr, env.params_expr));
            it.* = .{ .computed = .{ .name = f.name, .expr = try ast.mk(a, .{ .cast = .{ .e = @constCast(e), .ty = f.ty } }) } };
        };
    }
    sets[t.width] = .{ .field = .{ .parts = try a.dupe([]const u8, &.{lake_dml.tag_name}) } };
    sets[t.width + 1] = .{ .field = .{ .parts = try a.dupe([]const u8, &.{lake_dml.pos_name}) } };
    const project = ast.Stage{ .node = .{ .select = sets }, .hints = &.{}, .pos = if (between.len > 0) between[between.len - 1].pos else u.sets[0].pos };
    const stages = try std.mem.concat(a, ast.Stage, &.{ between, &.{project} });

    const hits = try a.alloc(std.DynamicBitSetUnmanaged, t.match.len);
    for (hits) |*h| h.* = .{};
    const writer: ?*lake_dml.Writer = if (dry) null else lake_dml.Writer.begin(env.gpa, a, &t, try std.mem.join(a, " AND ", texts.items), .{ .space = env.space, .op_memory = env.op_memory, .threads = env.pipe.threads, .err = env.errctx, .warn = .{ .ctx = env.log, .f = warnLog } }) catch |e| return lakeFailed(env, e);
    errdefer if (writer) |wr| wr.abort();
    if (t.match.len > 0) try findRowsInto(env, &t, stages, hits, writer, w.target);
    if (!dry) for (between) |st| if (st.node == .join and st.node.join.null_aware) try warnNullKeyEngine(env, try msgfmt.allocPrint(a, "UPDATE {s}", .{w.target}), "updated", st.node.join);
    if (writer) |wr| {
        const got = wr.finish(hits) catch |e| return lakeFailed(env, e);
        stats.rows_out += got.rows;
        const ms = std.time.milliTimestamp() - t0;
        env.log.script(try msgfmt.allocPrint(a, "Updated {d} row{s} of {s} in {d} ms ({d} file{s} removed, {d} added)", .{ got.rows, if (got.rows == 1) "" else "s", w.target, ms, got.removed, if (got.removed == 1) "" else "s", got.added }));
        return;
    }
    const n = try lake_dml.rowsOf(a, &t, hits);
    env.log.script(try msgfmt.allocPrint(a, "UPDATE {s}: would update {d} row{s}, reading {d} of its {d} file{s}", .{ w.target, n, if (n == 1) "" else "s", t.match.len, t.st.files.count(), if (t.st.files.count() == 1) "" else "s" }));
}

/// Marks in `hits` (one per file of `t.match`) the position of each row the stages
/// `between` keep, built over `t.layout` as a query's stages are over its read.
fn findRows(env: *Env, t: *const lake_dml.Target, between: []const ast.Stage, hits: []std.DynamicBitSetUnmanaged) !void {
    return findRowsInto(env, t, between, hits, null, "");
}

/// `findRows`, each batch also handed to `writer` (an UPDATE's new rows, ending in
/// the projection's file and position), refusing a row met twice, which an UPDATE's
/// FROM source matched on more than one row; `target` names the table for that.
fn findRowsInto(env: *Env, t: *const lake_dml.Target, stages: []const ast.Stage, hits: []std.DynamicBitSetUnmanaged, writer: ?*lake_dml.Writer, target: []const u8) !void {
    const src = try Folder.openLayout(env.arena, t.layout, null);
    const s = src.source();
    defer s.close();
    const scan = try env.arena.create(op.Scan);
    scan.* = .{ .src = s };
    var current: op.Op = .{ .scan = scan };
    var schema = t.layout.schema;
    for (stages) |st| {
        if (st.node == .select and st.node.select.len > 0 and st.node.select[0] == .star) continue;
        const r = try plan.buildStage(env, st, current, schema);
        current = r.op;
        schema = r.schema;
    }
    defer current.release();
    const ti = schema.indexOf(lake_dml.tag_name).?;
    const pi = schema.indexOf(lake_dml.pos_name).?;
    var scratch = std.heap.ArenaAllocator.init(env.gpa);
    defer scratch.deinit();
    while (try current.next(scratch.allocator())) |b| {
        for (0..b.len) |r| {
            const h = &hits[@intCast(b.columns[ti].getValue(r).int)];
            const p: usize = @intCast(b.columns[pi].getValue(r).int);
            if (p >= h.bit_length) try h.resize(env.arena, @max(p + 1, h.bit_length * 2), false);
            if (writer != null and h.isSet(p))
                return planErr(env.diag, try msgfmt.allocPrint(env.arena, "UPDATE {s}: a row matches more than one row of the FROM source, so it would take one of several values; give each key once", .{target}));
            h.set(p);
        }
        if (writer) |wr| wr.put(scratch.allocator(), b) catch |e| return lakeFailed(env, e);
        _ = scratch.reset(.retain_capacity);
    }
}

/// SQL's rule for a NULL among a `NOT IN` subquery's rows, said as a warning when
/// the subquery, run once more on the engine, holds one.
fn warnNullKeyEngine(env: *Env, what: []const u8, done: []const u8, j: ast.Join) !void {
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
    if (n > 0) env.log.log(.warn, "{s}: the NOT IN subquery returned {d} NULL{s}, so by SQL's rule no row is {s}; `WHERE {s} IS NOT NULL` in the subquery leaves them out", .{ what, n, if (n == 1) "" else "s", done, rk.last() });
}

/// A Delta table's refusal or failure as the run's error, in its own words.
fn lakeFailed(env: *Env, e: anyerror) anyerror {
    if (e == error.OutOfMemory) return e;
    return planErrT(env.diag, e, eval.takeFailure(e) orelse @errorName(e));
}

/// Loads subquery `m`'s rows, its key columns alone, into its staging table on the
/// DELETE's database, as an APPEND that creates the table.
fn stageMember(env: *Env, w: ast.Write, m: Member, opts: RunOptions, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const keys = try env.arena.alloc(ast.SelectItem, m.join.right_keys.len);
    for (keys, m.join.right_keys) |*k, rk| k.* = .{ .field = .{ .parts = try env.arena.dupe([]const u8, &.{rk.last()}), .span = rk.span } };
    try stageRows(env, w, m.join.binding, keys, m.staging, opts, lanes_used, batch_arena);
}

/// Loads the rows of binding `binding` as `items` name them into table `staging` on
/// `w`'s database, as an APPEND that creates the table, outside the run's own loads.
fn stageRows(env: *Env, w: ast.Write, binding: []const u8, items: []const ast.SelectItem, staging: []const u8, opts: RunOptions, lanes_used: *usize, batch_arena: *std.heap.ArenaAllocator) !void {
    const b = (try stampedBinding(env, binding)) orelse return planErr(env.diag, try msgfmt.allocPrint(env.arena, "{s}: unknown subquery `{s}`", .{ w.target, binding }));
    var st = std.array_list.Managed(ast.Stage).init(env.arena);
    try st.appendSlice(b.stages);
    try st.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = b.pos });
    try st.append(.{ .node = .{ .write = .{ .connector = w.connector, .form = null, .target = staging, .mode = .append } }, .hints = &.{}, .pos = b.pos });
    var scratch: Stats = .{};
    const outer_target = env.last_target;
    const outer_loads = env.loads;
    const outer_hook = env.on_load;
    const outer_wrote = env.wrote_sink;
    const outer_items = env.items;
    env.loads = null;
    env.on_load = null;
    env.items = false;
    defer {
        env.last_target = outer_target;
        env.loads = outer_loads;
        env.on_load = outer_hook;
        env.wrote_sink = outer_wrote;
        env.items = outer_items;
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
/// `done` (deleted, updated), said as a warning when the staged rows hold one;
/// `what` names the statement.
fn warnNullKey(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, what: []const u8, done: []const u8, m: Member) !void {
    const d = info.dialect;
    const rk = try sql.quoteIdent(env.arena, d, m.join.right_keys[0].last());
    const q = try msgfmt.allocPrint(env.arena, "SELECT COUNT(*) FROM {s} WHERE {s} IS NULL", .{ try sql.quoteIdent(env.arena, d, m.staging), rk });
    const n = countOf(env, info, conn, q) orelse return;
    if (n > 0) env.log.log(.warn, "{s}: the NOT IN subquery returned {d} NULL{s}, so by SQL's rule no row is {s}; `WHERE {s} IS NOT NULL` in the subquery leaves them out", .{ what, n, if (n == 1) "" else "s", done, m.join.right_keys[0].last() });
}

/// The count query `q` returns, on a connection of its own (the cursor owns it);
/// null when it cannot be had.
fn countOf(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, q: []const u8) ?i64 {
    const cfg = dbconfig.resolveDbConfig(env, conn, info.port) catch return null;
    const c = connect.connectSql(env.gpa, info.kind, cfg) catch return null;
    const cur = c.queryCursor(q) catch {
        c.close();
        return null;
    };
    defer cur.close();
    const b = (cur.nextBatch(env.arena) catch return null) orelse return null;
    if (b.len == 0) return null;
    return switch (b.columns[0].getValue(0)) {
        .int => |x| x,
        .decimal => |x| @intCast(x.unscaled),
        else => null,
    };
}

/// Runs `stmt`, the statement `what` names, on a connection of its own, returning
/// the rows the database reports it changed, when it reports them.
fn execStmt(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, what: []const u8, stmt: []const u8) !?u64 {
    const cfg = try dbconfig.resolveDbConfig(env, conn, info.port);
    return switch (info.kind) {
        inline else => |k| blk: {
            const c = connect.SqlDriver(k).connect(env.gpa, cfg) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s}: {s} connect failed: {s}", .{ what, conn.connector, try dbconfig.connectWhy(env.arena, e) }));
            defer c.close();
            const n = c.execBatch(stmt) catch |e| {
                if (e == error.OutOfMemory) return e;
                const why = if (c.wire.last_error.len > 0) c.wire.last_error else @errorName(e);
                return planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "{s} failed: {s}", .{ what, why }));
            };
            break :blk if (k == .sqlserver) c.last_done_count else n;
        },
    };
}

fn dropStaging(env: *Env, info: connect.SqlConnInfo, conn: ast.Connection, staging: []const u8) void {
    const cfg = dbconfig.resolveDbConfig(env, conn, info.port) catch return;
    const c = connect.connectSql(env.gpa, info.kind, cfg) catch return;
    defer c.close();
    const drop = replace.dropSql(env.arena, info.dialect, staging) catch return;
    c.exec(drop) catch {};
}
