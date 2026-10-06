//! PARAMs and LETs bound before a script runs: from the command line or request,
//! defaults, and LET values folded or queried once.

const Diag = @import("../env.zig").Diag;
const ParamArg = @import("../env.zig").ParamArg;
const Value = @import("../../exec/value.zig").Value;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const constEvalDefault = @import("../run.zig").constEvalDefault;
const errLabel = @import("../env.zig").errLabel;
const eval = @import("../../exec/eval.zig");
const mkLit = @import("../env.zig").mkLit;
const planErr = @import("../env.zig").planErr;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// A LET named on the command line is an error, not ignored. A default takes the
/// declared type: `PARAM d DATE DEFAULT '2026-01-01'` is a DATE.
pub fn resolveParams(arena: std.mem.Allocator, program: ast.Program, cli: []const ParamArg, params: *std.StringHashMap(Value), diag: *Diag) !void {
    for (cli) |kv| {
        for (program.stmts) |s| {
            if (s != .let_const) continue;
            if (std.mem.eql(u8, kv.key, s.let_const.name))
                return planErr(diag, try std.fmt.allocPrint(arena, "`{s}` is a LET, not a PARAM — it cannot be bound externally", .{kv.key}));
        }
    }
    for (program.stmts) |s| {
        if (s != .param) continue;
        const p = s.param;
        if (p.is_json) continue;
        var v: ?Value = null;
        for (cli) |kv| {
            if (std.mem.eql(u8, kv.key, p.name)) {
                if (analyze.paramTextProblem(arena, p.ty, kv.val)) |want|
                    return planErr(diag, try std.fmt.allocPrint(arena, "PARAM `{s}`: `{s}` is not {s}", .{ p.name, kv.val, want }));
                v = try parseParamValue(arena, p.ty, kv.val, diag);
                break;
            }
        }
        if (v == null) {
            if (p.default) |d| {
                const raw = try constEvalDefault(d, diag);
                if (raw == .string and p.ty.kind != .string and p.ty.kind != .bytes) {
                    if (analyze.paramTextProblem(arena, p.ty, raw.string)) |want|
                        return planErr(diag, try std.fmt.allocPrint(arena, "PARAM `{s}`: `{s}` is not {s}", .{ p.name, raw.string, want }));
                    try params.put(p.name, try parseParamValue(arena, p.ty, raw.string, diag));
                    continue;
                }
                v = switch (p.ty.kind) {
                    .date, .time, .timestamp, .decimal => if (raw == .null) raw else eval.castValueTyped(arena, raw, p.ty) catch
                        return planErr(diag, try std.fmt.allocPrint(arena, "PARAM `{s}`: `{s}` is not {s}", .{ p.name, if (raw == .string) raw.string else "its DEFAULT", analyze.paramTextProblem(arena, p.ty, if (raw == .string) raw.string else "") orelse try p.ty.name(arena) })),
                    else => raw,
                };
            } else {
                return planErr(diag, try std.fmt.allocPrint(arena, "missing required param `{s}`", .{p.name}));
            }
        }
        try params.put(p.name, v.?);
    }
}

/// Fold every statement-level expression `LET` into a constant, in declaration order,
/// with params and earlier LETs in scope, registered as a PARAM is. A LET is never
/// bound from outside, so this is the only place its value is decided.
pub fn resolveLets(
    arena: std.mem.Allocator,
    program: ast.Program,
    params: *std.StringHashMap(Value),
    params_expr: *std.StringHashMap(*const ast.Expr),
    diag: *Diag,
) !void {
    var names = std.array_list.Managed([]const u8).init(arena);
    var values = std.array_list.Managed(Value).init(arena);
    var it = params.iterator();
    while (it.next()) |kv| {
        try names.append(kv.key_ptr.*);
        try values.append(kv.value_ptr.*);
    }

    for (program.stmts) |s| {
        if (s != .let_const) continue;
        const l = s.let_const;
        for (program.stmts) |p| {
            if (p == .param and std.mem.eql(u8, p.param.name, l.name))
                return planErr(diag, try std.fmt.allocPrint(arena, "`{s}` is declared twice: LET and PARAM share one name space", .{l.name}));
        }
        if (params.contains(l.name))
            return planErr(diag, try std.fmt.allocPrint(arena, "duplicate LET `{s}`", .{l.name}));

        if (try analyze.letRefProblem(arena, program, l)) |why| {
            const e = planErr(diag, why);
            diag.pos = l.pos;
            return e;
        }
        const le = l.expr orelse continue;
        const v = eval.constEval(arena, le, names.items, values.items) catch |e|
            return planErr(diag, try std.fmt.allocPrint(arena, "LET `{s}`: {s}", .{ l.name, errLabel(e) }));
        try names.append(l.name);
        try values.append(v);
        try params.put(l.name, v);
        try params_expr.put(l.name, try mkLit(arena, v));
    }
}

/// Typed text is read as a CAST reads it: `-p d=2026-02-01` binds what
/// `CAST('2026-02-01' AS DATE)` would, and a decimal rounds as a cast does.
fn parseParamValue(arena: std.mem.Allocator, ty: types.Type, str: []const u8, diag: *Diag) !Value {
    return switch (ty.kind) {
        .int => .{ .int = std.fmt.parseInt(i64, str, 10) catch return planErr(diag, "invalid integer param value") },
        .float => .{ .float = std.fmt.parseFloat(f64, str) catch return planErr(diag, "invalid float param value") },
        .string => .{ .string = try arena.dupe(u8, str) },
        .bool => if (std.mem.eql(u8, str, "true")) Value{ .bool = true } else if (std.mem.eql(u8, str, "false")) Value{ .bool = false } else planErr(diag, "invalid bool param value"),
        .date, .time, .timestamp, .decimal => eval.castValueTyped(arena, .{ .string = str }, ty) catch
            planErr(diag, try std.fmt.allocPrint(arena, "invalid {s} param value `{s}` — expected {s}", .{ try ty.name(arena), str, switch (ty.kind) {
                .date => "YYYY-MM-DD",
                .time => "HH:MM[:SS[.ffffff]]",
                .timestamp => "YYYY-MM-DD[ HH:MM:SS[.ffffff]]",
                else => "a number",
            } })),
        else => planErr(diag, try std.fmt.allocPrint(arena, "a {s} param cannot be bound from text", .{try ty.name(arena)})),
    };
}
