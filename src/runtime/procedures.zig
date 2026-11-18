//! Built-in procedures: what a `CALL` runs when the script declares no FUNCTION
//! of its name. `delta_vacuum` deletes the files no version of a Delta table within
//! its retention needs, `delta_optimize` compacts its small files, and `execute_on`
//! sends SQL to a database connection as written, for the setup a load needs. Each
//! says what it did through the script channel `PRINT` writes to.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const analyze = @import("analyze.zig");
const maint = @import("../format/lake/delta_maint.zig");
const eval = @import("../exec/eval.zig");
const msgfmt = @import("../base/msgfmt.zig");
const env_mod = @import("env.zig");
const Value = @import("../exec/value.zig").Value;
const connect = @import("connect.zig");
const dbconfig = @import("connect/dbconfig.zig");
const options = @import("analyze/options.zig");
const registry = @import("connect/registry.zig");

const Env = env_mod.Env;
const planErr = env_mod.planErr;

pub const Proc = struct { name: []const u8, min: usize, max: usize, usage: []const u8 };

pub const procs = [_]Proc{
    .{ .name = "delta_vacuum", .min = 1, .max = 2, .usage = "CALL delta_vacuum('<table folder>/'[, <retention in hours>])" },
    .{ .name = "delta_optimize", .min = 1, .max = 2, .usage = "CALL delta_optimize('<table folder>/'[, '<predicate on partition columns>'])" },
    .{ .name = "execute_on", .min = 2, .max = 2, .usage = "CALL execute_on('<connection>', $$<SQL>$$)" },
};

pub fn lookup(name: []const u8) ?*const Proc {
    for (&procs) |*p| if (std.mem.eql(u8, p.name, name)) return p;
    return null;
}

/// Why an `execute_on` call cannot run as written, from the script's connections:
/// a connection named as a literal that the script does not declare, or that is not
/// a database. A name computed at run time is checked when it runs.
pub fn executeOnProblem(arena: std.mem.Allocator, stmts: []const ast.Stmt, c: ast.CallStmt) !?[]const u8 {
    if (c.args.len != 2 or c.args[0].* != .str_lit) return null;
    const name = c.args[0].str_lit;
    for (stmts) |s| {
        if (s != .connection or !std.mem.eql(u8, s.connection.name, name)) continue;
        return try notDatabase(arena, s.connection);
    }
    return try options.unknownConnectionMsg(arena, name, "execute_on");
}

fn notDatabase(arena: std.mem.Allocator, conn: ast.Connection) !?[]const u8 {
    if (registry.Connector.parse(conn.connector)) |k| if (k.sqlRead() != null) return null;
    return try msgfmt.allocPrint(arena, "`execute_on` sends SQL to a database (postgres, mysql, sqlserver, starrocks, doris); `{s}` is a {s} connection", .{ conn.name, conn.connector });
}

/// Why `c` calls procedure `p` with a wrong number of arguments, null when it does not.
pub fn arityProblem(arena: std.mem.Allocator, p: *const Proc, c: ast.CallStmt) !?[]const u8 {
    if (c.args.len >= p.min and c.args.len <= p.max) return null;
    if (p.min == p.max) return try msgfmt.allocPrint(arena, "`{s}` takes {d} arguments, not {d}: {s}", .{ p.name, p.min, c.args.len, p.usage });
    return try msgfmt.allocPrint(arena, "`{s}` takes {d} to {d} arguments, not {d}: {s}", .{ p.name, p.min, p.max, c.args.len, p.usage });
}

/// Runs procedure `p` for `c`, its arguments evaluated in scope `sc`; `dry` under
/// `EXPLAIN ANALYZE`, which moves no data: it works out and says what it would do,
/// deleting and committing nothing.
pub fn run(env: *Env, p: *const Proc, c: ast.CallStmt, sc: analyze.Scope, dry: bool) !void {
    errdefer env.diag.stamp(c.pos);
    if (try arityProblem(env.arena, p, c)) |why| return planErr(env.diag, why);
    const args = try env.arena.alloc(Value, c.args.len);
    for (c.args, args, 0..) |e, *v, i| v.* = switch (try analyze.fold(env.arena, e, sc)) {
        .value => |x| x,
        .failed => |why| return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s}`: argument {d}: {s}", .{ p.name, i + 1, why })),
    };
    if (std.mem.eql(u8, p.name, "execute_on")) return executeOn(env, args, dry);
    const root = try folderArg(env, p, args[0]);
    if (std.mem.eql(u8, p.name, "delta_vacuum")) {
        const hours: ?i64 = if (args.len > 1) switch (args[1]) {
            .null => null,
            .int => |n| if (n >= 0) n else return planErr(env.diag, "`delta_vacuum`'s retention is a number of hours, at least 0"),
            else => return planErr(env.diag, "`delta_vacuum`'s retention is a whole number of hours"),
        } else null;
        const got = maint.vacuum(env.arena, root, if (hours) |h| h * 3_600_000 else null, std.time.milliTimestamp(), dry) catch |e| return explain(env, e);
        env.log.script(try msgfmt.allocPrint(env.arena, "delta_vacuum {s}: {s} {d} file{s}, {s} {d} the table still reads", .{ root, if (dry) "would delete" else "deleted", got.deleted, if (got.deleted == 1) "" else "s", if (dry) "would keep" else "kept", got.kept }));
        return;
    }
    if (std.mem.eql(u8, p.name, "delta_optimize")) {
        const where: ?[]const u8 = if (args.len > 1) switch (args[1]) {
            .null => null,
            .string => |t| t,
            else => return planErr(env.diag, "`delta_optimize`'s predicate is SQL text, e.g. 'region = ''eu'''"),
        } else null;
        const got = maint.optimize(env.gpa, env.arena, root, where, .{ .space = env.space, .op_memory = env.op_memory, .threads = env.pipe.threads, .err = env.errctx }, dry) catch |e| return explain(env, e);
        if (dry) {
            env.log.script(try msgfmt.allocPrint(env.arena, "delta_optimize {s}: would compact {d} file{s} across {d} partition{s}", .{ root, got.removed, if (got.removed == 1) "" else "s", got.partitions, if (got.partitions == 1) "" else "s" }));
            return;
        }
        env.log.script(try msgfmt.allocPrint(env.arena, "delta_optimize {s}: compacted {d} file{s} into {d} across {d} partition{s}", .{ root, got.removed, if (got.removed == 1) "" else "s", got.added, got.partitions, if (got.partitions == 1) "" else "s" }));
        return;
    }
    unreachable;
}

/// `execute_on(conn, sql)`: `sql` sent as written to database connection `conn`, on a
/// connection of its own closed after, so a `SET` or temporary table it makes is gone
/// for what reads next. It runs once, never again after a failure, and under `EXPLAIN
/// ANALYZE` is only named.
fn executeOn(env: *Env, args: []const Value, dry: bool) !void {
    const name = switch (args[0]) {
        .string => |s| s,
        else => return planErr(env.diag, "`execute_on` takes the connection's name as text: CALL execute_on('pg', $$...$$)"),
    };
    const text = switch (args[1]) {
        .string => |s| s,
        else => return planErr(env.diag, "`execute_on` takes the SQL to run as text: CALL execute_on('pg', $$...$$)"),
    };
    const conn = env.connections.get(name) orelse return planErr(env.diag, try options.unknownConnectionMsg(env.arena, name, "execute_on"));
    if (try notDatabase(env.arena, conn)) |why| return planErr(env.diag, why);
    const info = connect.sqlConnInfo(conn).?;
    const lead = std.mem.trim(u8, text[0 .. std.mem.indexOfScalar(u8, text, '\n') orelse text.len], " \t\r");
    if (dry) {
        env.log.script(try msgfmt.allocPrint(env.arena, "execute_on {s}: would run `{s}`{s}", .{ name, lead, if (lead.len < std.mem.trim(u8, text, " \t\r\n").len) " ..." else "" }));
        return;
    }
    const cfg = try dbconfig.resolveDbConfig(env, conn, info.port);
    const t0 = std.time.milliTimestamp();
    const changed: ?u64 = switch (info.kind) {
        inline else => |k| blk: {
            const c = connect.SqlDriver(k).connect(env.gpa, cfg) catch |e|
                return env_mod.planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "execute_on {s}: {s} connect failed: {s}", .{ name, conn.connector, try dbconfig.connectWhy(env.arena, e) }));
            defer c.close();
            break :blk c.execBatch(text) catch |e| {
                if (e == error.OutOfMemory) return e;
                const why = if (c.wire.last_error.len > 0) c.wire.last_error else @errorName(e);
                return env_mod.planErrT(env.diag, e, try msgfmt.allocPrint(env.arena, "execute_on {s} failed: {s}", .{ name, why }));
            };
        },
    };
    const ms = std.time.milliTimestamp() - t0;
    if (changed) |n|
        env.log.script(try msgfmt.allocPrint(env.arena, "execute_on {s}: done in {d} ms, {d} row{s} changed", .{ name, ms, n, if (n == 1) "" else "s" }))
    else
        env.log.script(try msgfmt.allocPrint(env.arena, "execute_on {s}: done in {d} ms", .{ name, ms }));
}

/// A procedure's table argument: a folder path, ending in `/`.
fn folderArg(env: *Env, p: *const Proc, v: Value) ![]const u8 {
    const s = switch (v) {
        .string => |s| s,
        else => return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s}` takes the Delta table's folder as text: {s}", .{ p.name, p.usage })),
    };
    if (s.len == 0) return planErr(env.diag, try msgfmt.allocPrint(env.arena, "`{s}` needs the Delta table's folder: {s}", .{ p.name, p.usage }));
    return if (s[s.len - 1] == '/') s else try std.mem.concat(env.arena, u8, &.{ s, "/" });
}

fn explain(env: *Env, e: anyerror) anyerror {
    if (e == error.OutOfMemory) return e;
    const why = eval.takeFailure(e) orelse @errorName(e);
    return planErr(env.diag, why);
}
