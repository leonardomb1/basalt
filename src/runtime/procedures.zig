//! Built-in procedures: what a `CALL` runs when the script declares no FUNCTION
//! of its name. They maintain Delta tables: `delta_vacuum` deletes the files no
//! version within the retention needs, `delta_optimize` compacts small files. Each
//! says what it did through the script channel `PRINT` writes to.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const analyze = @import("analyze.zig");
const maint = @import("../format/lake/delta_maint.zig");
const eval = @import("../exec/eval.zig");
const msgfmt = @import("../base/msgfmt.zig");
const env_mod = @import("env.zig");
const Value = @import("../exec/value.zig").Value;

const Env = env_mod.Env;
const planErr = env_mod.planErr;

pub const Proc = struct { name: []const u8, min: usize, max: usize, usage: []const u8 };

pub const procs = [_]Proc{
    .{ .name = "delta_vacuum", .min = 1, .max = 2, .usage = "CALL delta_vacuum('<table folder>/'[, <retention in hours>])" },
    .{ .name = "delta_optimize", .min = 1, .max = 2, .usage = "CALL delta_optimize('<table folder>/'[, '<predicate on partition columns>'])" },
};

pub fn lookup(name: []const u8) ?*const Proc {
    for (&procs) |*p| if (std.mem.eql(u8, p.name, name)) return p;
    return null;
}

/// Why `c` calls procedure `p` with a wrong number of arguments, null when it does not.
pub fn arityProblem(arena: std.mem.Allocator, p: *const Proc, c: ast.CallStmt) !?[]const u8 {
    if (c.args.len >= p.min and c.args.len <= p.max) return null;
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
