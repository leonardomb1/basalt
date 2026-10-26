//! PARAMs and LETs bound before a script runs: from the command line or request,
//! defaults, and LET values folded or queried once.

const Diag = @import("../env.zig").Diag;
const ParamArg = @import("../env.zig").ParamArg;
const Value = @import("../../exec/value.zig").Value;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const errLabel = @import("../env.zig").errLabel;
const eval = @import("../../exec/eval.zig");
const mkLit = @import("../env.zig").mkLit;
const planErr = @import("../env.zig").planErr;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// A default takes the declared type: `PARAM d DATE DEFAULT '2026-01-01'` is a DATE.
/// `checkFirst` already refused a LET bound on the command line and a PARAM declared twice.
pub fn resolveParams(arena: std.mem.Allocator, program: ast.Program, cli: []const ParamArg, params: *std.StringHashMap(Value), diag: *Diag) !void {
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
                const raw = analyze.paramDefaultValue(d) orelse return planErr(diag, analyze.param_default_msg);
                if (raw == .string and p.ty.kind != .string and p.ty.kind != .bytes) {
                    if (analyze.paramTextProblem(arena, p.ty, raw.string)) |want|
                        return planErr(diag, try std.fmt.allocPrint(arena, "PARAM `{s}`: `{s}` is not {s}", .{ p.name, raw.string, want }));
                    try params.put(p.name, try parseParamValue(arena, p.ty, raw.string, diag));
                    continue;
                }
                if (p.ty.kind == .int and (raw == .float or raw == .decimal))
                    return planErr(diag, try std.fmt.allocPrint(arena, "PARAM `{s}`: its DEFAULT is not an integer", .{p.name}));
                v = switch (p.ty.kind) {
                    .float => if (raw == .int or raw == .decimal) Value{ .float = eval.toF64(raw) } else raw,
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
/// bound from outside, so this is the only place its value is decided. `checkFirst`
/// already refused a name declared twice and a LET reading what it may not.
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
        const le = l.expr orelse continue;
        const v = eval.constEval(arena, le, names.items, values.items) catch |e|
            return planErrAt(diag, try std.fmt.allocPrint(arena, "LET `{s}`: {s}", .{ l.name, eval.takeFailure(e) orelse errLabel(e) }), l.pos);
        try names.append(l.name);
        try values.append(v);
        try params.put(l.name, v);
        try params_expr.put(l.name, try mkLit(arena, v));
    }
}

fn planErrAt(diag: *Diag, msg: []const u8, pos: ast.Pos) error{PlanFailed} {
    const e = planErr(diag, msg);
    diag.pos = pos;
    return e;
}

/// Typed text is read as a CAST reads it: `-p d=2026-02-01` binds what
/// `CAST('2026-02-01' AS DATE)` would, and a decimal rounds as a cast does.
fn parseParamValue(arena: std.mem.Allocator, ty: types.Type, str: []const u8, diag: *Diag) !Value {
    return switch (ty.kind) {
        .int => analyze.paramFromText(arena, ty, str) orelse planErr(diag, "invalid integer param value"),
        .float => analyze.paramFromText(arena, ty, str) orelse planErr(diag, "invalid float param value"),
        .string => .{ .string = try arena.dupe(u8, str) },
        .bool => analyze.paramFromText(arena, ty, str) orelse planErr(diag, "invalid bool param value"),
        .date, .time, .timestamp, .decimal => analyze.paramFromText(arena, ty, str) orelse
            planErr(diag, try std.fmt.allocPrint(arena, "invalid {s} param value `{s}` — expected {s}", .{ try ty.name(arena), str, switch (ty.kind) {
                .date => "YYYY-MM-DD",
                .time => "HH:MM[:SS[.ffffff]]",
                .timestamp => "YYYY-MM-DD[ HH:MM:SS[.ffffff]]",
                else => "a number",
            } })),
        else => planErr(diag, try std.fmt.allocPrint(arena, "a {s} param cannot be bound from text", .{try ty.name(arena)})),
    };
}

fn bindOne(arena: std.mem.Allocator, decl: []const u8, text: []const u8) !?Value {
    const prog = try analyze.parse(arena, decl);
    var params = std.StringHashMap(Value).init(arena);
    var diag = Diag{};
    resolveParams(arena, prog, &.{.{ .key = "p", .val = text }}, &params, &diag) catch return null;
    return params.get("p").?;
}

test "PARAM text binds as CAST reads it: trimmed, no `_` separators, no hex floats; check and run agree" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const int_decl = "PARAM p INT; SELECT $p AS p;";
    const float_decl = "PARAM p FLOAT; SELECT $p AS p;";
    try std.testing.expectEqual(@as(i64, 5), (try bindOne(a, int_decl, " 5")).?.int);
    try std.testing.expectEqual(@as(i64, -7), (try bindOne(a, int_decl, "-7 ")).?.int);
    try std.testing.expectEqual(@as(i64, 5), (try bindOne(a, int_decl, "+5")).?.int);
    try std.testing.expect((try bindOne(a, int_decl, "1_000")) == null);
    try std.testing.expect((try bindOne(a, int_decl, "1.5")) == null);
    try std.testing.expect((try bindOne(a, int_decl, "0x10")) == null);
    try std.testing.expectEqual(@as(f64, 1000), (try bindOne(a, float_decl, " 1e3 ")).?.float);
    try std.testing.expectEqual(@as(f64, 0.5), (try bindOne(a, float_decl, "0.5")).?.float);
    try std.testing.expect((try bindOne(a, float_decl, "0x1p3")) == null);
    try std.testing.expect((try bindOne(a, float_decl, "1_0.5")) == null);
    const int_ty = types.Type.init(.int);
    const float_ty = types.Type.init(.float);
    for ([_][]const u8{ " 5", "+5", "-7 ", "1_000", "1.5", "0x10", "", " " }) |t|
        try std.testing.expectEqual((try bindOne(a, int_decl, t)) == null, analyze.paramTextProblem(a, int_ty, t) != null);
    for ([_][]const u8{ " 1e3 ", "0.5", "0x1p3", "1_0.5", "nan", "inf", "" }) |t|
        try std.testing.expectEqual((try bindOne(a, float_decl, t)) == null, analyze.paramTextProblem(a, float_ty, t) != null);
}

test "PARAM BOOL text reads as CAST reads it, and anything else is refused at check and run alike (D6)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const decl = "PARAM p BOOL; SELECT $p AS p;";
    for ([_][]const u8{ "true", "TRUE", "True", " true", "t", "1", "yes", "Y" }) |t| {
        try std.testing.expectEqual(true, (try bindOne(a, decl, t)).?.bool);
        try std.testing.expect(analyze.paramTextProblem(a, types.Type.init(.bool), t) == null);
    }
    for ([_][]const u8{ "false", "F", "0", "no", "n" }) |t|
        try std.testing.expectEqual(false, (try bindOne(a, decl, t)).?.bool);
    for ([_][]const u8{ "on", "off", "tr", "2", "" }) |t| {
        try std.testing.expect((try bindOne(a, decl, t)) == null);
        try std.testing.expect(analyze.paramTextProblem(a, types.Type.init(.bool), t) != null);
    }
}
