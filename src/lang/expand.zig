//! Plan-time expansion of user-defined scalar functions (`fn name(a,b) = expr`).
//!
//! `expandProgram` collects the `fn` declarations, then rewrites every expression
//! in the program so each call to a user fn is replaced by the fn's body with the
//! call's arguments substituted for the parameters (a hygienic inline expansion).
//! The `fn` declarations are dropped from the returned program, so the rest of the
//! engine (type-checker, evaluator, planner) never sees a user function. Recursion
//! and arity mismatches are reported as `ExpandFailed` with a message.

const std = @import("std");
const ast = @import("ast.zig");

pub const Error = error{ OutOfMemory, ExpandFailed };

const max_depth = 64; // a fn call chain deeper than this is treated as recursion

const Ctx = struct {
    arena: std.mem.Allocator,
    fns: *const std.StringHashMap(ast.FnDecl),
    json: *const std.StringHashMap(?std.json.Value),
    msg: *[]const u8,
};

const Subst = std.StringHashMap(*ast.Expr);

/// Expand user-fn calls and JSON-param path refs, returning a program with the
/// `fn` declarations removed. `body` is the request body (or null offline); it is
/// parsed once if any `json` params are declared, and `p.a.b` field refs are
/// replaced by literals of the resolved scalar (or `null` when unbound, e.g.
/// offline `check`).
pub fn expandProgram(arena: std.mem.Allocator, program: ast.Program, body: ?[]const u8, msg: *[]const u8) Error!ast.Program {
    var fns = std.StringHashMap(ast.FnDecl).init(arena);
    for (program.stmts) |s| if (s == .func) try fns.put(s.func.name, s.func);

    var json = std.StringHashMap(?std.json.Value).init(arena);
    var has_json = false;
    for (program.stmts) |s| if (s == .param and s.param.is_json) {
        has_json = true;
    };
    if (has_json) {
        var parsed: ?std.json.Value = null;
        if (body) |b| parsed = std.json.parseFromSliceLeaky(std.json.Value, arena, b, .{}) catch {
            msg.* = "invalid JSON in request body";
            return error.ExpandFailed;
        };
        for (program.stmts) |s| if (s == .param and s.param.is_json) try json.put(s.param.name, parsed);
    }

    var cx = Ctx{ .arena = arena, .fns = &fns, .json = &json, .msg = msg };
    var out = std.array_list.Managed(ast.Stmt).init(arena);
    for (program.stmts) |s| {
        if (s == .func) continue; // consumed
        try out.append(try expandStmt(&cx, s));
    }
    return .{ .stmts = try out.toOwnedSlice() };
}

fn expandStmt(cx: *Ctx, s: ast.Stmt) Error!ast.Stmt {
    return switch (s) {
        .kind => |k| .{ .kind = .{ .kind = k.kind, .config = try expandAttrs(cx, k.config), .pos = k.pos } },
        .param => |p| .{ .param = .{ .name = p.name, .ty = p.ty, .default = if (p.default) |d| try expandExpr(cx, d, null, 0) else null, .source = p.source, .pos = p.pos, .is_json = p.is_json } },
        .connection => |c| .{ .connection = .{ .name = c.name, .connector = c.connector, .config = try expandAttrs(cx, c.config), .pos = c.pos } },
        .binding => |b| .{ .binding = .{ .name = b.name, .pipeline = try expandPipeline(cx, b.pipeline), .pos = b.pos } },
        .output => |p| .{ .output = try expandPipeline(cx, p) },
        .for_each => |fe| .{ .for_each = .{ .var_names = fe.var_names, .source = fe.source, .hints = fe.hints, .body = try expandPipeline(cx, fe.body), .pos = fe.pos } },
        .match => |m| .{ .match = try expandStmtMatch(cx, m) },
        .func => unreachable, // dropped in expandProgram / skipped in arm bodies
    };
}

fn expandAttrs(cx: *Ctx, attrs: []const ast.Attr) Error![]const ast.Attr {
    const out = try cx.arena.alloc(ast.Attr, attrs.len);
    for (attrs, 0..) |a, i| out[i] = .{ .key = a.key, .value = try expandExpr(cx, a.value, null, 0), .pos = a.pos };
    return out;
}

fn expandPipeline(cx: *Ctx, p: ast.Pipeline) Error!ast.Pipeline {
    const stages = try cx.arena.alloc(ast.Stage, p.stages.len);
    for (p.stages, 0..) |st, i| stages[i] = .{ .node = try expandNode(cx, st.node), .hints = st.hints, .pos = st.pos };
    return .{ .stages = stages, .pos = p.pos };
}

fn expandNode(cx: *Ctx, n: ast.Stage.Node) Error!ast.Stage.Node {
    return switch (n) {
        .filter => |e| .{ .filter = try expandExpr(cx, e, null, 0) },
        .select => |items| .{ .select = try expandSelect(cx, items) },
        .aggregate => |ag| blk: {
            const aggs = try cx.arena.alloc(ast.AggItem, ag.aggs.len);
            for (ag.aggs, 0..) |a, i| aggs[i] = .{ .name = a.name, .func = a.func, .arg = if (a.arg) |e| try expandExpr(cx, e, null, 0) else null };
            break :blk .{ .aggregate = .{ .aggs = aggs, .by = ag.by } };
        },
        else => n, // read/union/limit/distinct/sort/join/write/explode/ref carry no free exprs
    };
}

fn expandSelect(cx: *Ctx, items: []const ast.SelectItem) Error![]const ast.SelectItem {
    const out = try cx.arena.alloc(ast.SelectItem, items.len);
    for (items, 0..) |it, i| out[i] = switch (it) {
        .computed => |c| .{ .computed = .{ .name = c.name, .expr = try expandExpr(cx, c.expr, null, 0) } },
        else => it,
    };
    return out;
}

fn expandStmtMatch(cx: *Ctx, m: ast.StmtMatch) Error!ast.StmtMatch {
    const subject = if (m.subject) |s| try expandExpr(cx, s, null, 0) else null;
    const arms = try cx.arena.alloc(ast.StmtArm, m.arms.len);
    for (m.arms, 0..) |arm, i| {
        const pats = try cx.arena.alloc(*ast.Expr, arm.pats.len);
        for (arm.pats, 0..) |p, j| pats[j] = try expandExpr(cx, p, null, 0);
        const guard = if (arm.guard) |g| try expandExpr(cx, g, null, 0) else null;
        var body = std.array_list.Managed(ast.Stmt).init(cx.arena);
        for (arm.body) |st| {
            if (st == .func) continue; // fns are top-level; ignore in arm bodies
            try body.append(try expandStmt(cx, st));
        }
        arms[i] = .{ .pats = pats, .guard = guard, .body = try body.toOwnedSlice(), .is_default = arm.is_default };
    }
    return .{ .subject = subject, .arms = arms, .pos = m.pos };
}

// --- expression expansion ---

fn mk(cx: *Ctx, e: ast.Expr) Error!*ast.Expr {
    const p = try cx.arena.create(ast.Expr);
    p.* = e;
    return p;
}

fn mkNull(cx: *Ctx) Error!*ast.Expr {
    return mk(cx, .null_lit);
}

/// Navigate a JSON value along `path`, returning a literal of the leaf scalar. An
/// unbound binding (offline `check`) yields `null`; a missing key or non-scalar
/// leaf at run time is an error.
fn jsonPathLit(cx: *Ctx, maybe_val: ?std.json.Value, path: []const []const u8) Error!*ast.Expr {
    var cur = maybe_val orelse return mkNull(cx);
    for (path) |key| {
        switch (cur) {
            .object => |o| cur = o.get(key) orelse {
                cx.msg.* = std.fmt.allocPrint(cx.arena, "json path: key `{s}` not found", .{key}) catch "json path: key not found";
                return error.ExpandFailed;
            },
            else => {
                cx.msg.* = std.fmt.allocPrint(cx.arena, "json path: `{s}` is not an object", .{key}) catch "json path: not an object";
                return error.ExpandFailed;
            },
        }
    }
    return jsonScalarLit(cx, cur);
}

fn jsonScalarLit(cx: *Ctx, v: std.json.Value) Error!*ast.Expr {
    return switch (v) {
        .null => mkNull(cx),
        .bool => |b| mk(cx, .{ .bool_lit = b }),
        .integer => |i| mk(cx, .{ .int_lit = i }),
        .float => |f| mk(cx, .{ .float_lit = f }),
        .number_string, .string => |s| mk(cx, .{ .str_lit = s }),
        .array, .object => {
            cx.msg.* = "json path resolves to an array/object where a scalar is expected";
            return error.ExpandFailed;
        },
    };
}

fn expandExpr(cx: *Ctx, e: *const ast.Expr, subst: ?*const Subst, depth: usize) Error!*ast.Expr {
    if (depth > max_depth) {
        cx.msg.* = "fn expansion too deep (recursive `fn`?)";
        return error.ExpandFailed;
    }
    return switch (e.*) {
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit => mk(cx, e.*),
        .field => |q| {
            if (subst) |s| if (q.single()) |nm| if (s.get(nm)) |arg| return arg;
            // JSON-param path access: `p.a.b` where `p` is a declared json param.
            if (q.parts.len >= 1) if (cx.json.get(q.parts[0])) |maybe_val| return jsonPathLit(cx, maybe_val, q.parts[1..]);
            return mk(cx, e.*);
        },
        .unary => |u| mk(cx, .{ .unary = .{ .op = u.op, .e = try expandExpr(cx, u.e, subst, depth) } }),
        .binary => |b| mk(cx, .{ .binary = .{ .op = b.op, .l = try expandExpr(cx, b.l, subst, depth), .r = try expandExpr(cx, b.r, subst, depth) } }),
        .cond => |c| mk(cx, .{ .cond = .{ .cond = try expandExpr(cx, c.cond, subst, depth), .then = try expandExpr(cx, c.then, subst, depth), .els = try expandExpr(cx, c.els, subst, depth) } }),
        .cast => |c| mk(cx, .{ .cast = .{ .e = try expandExpr(cx, c.e, subst, depth), .ty = c.ty } }),
        .is_null => |n| mk(cx, .{ .is_null = .{ .e = try expandExpr(cx, n.e, subst, depth), .negated = n.negated } }),
        .match => |m| expandMatchExpr(cx, m, subst, depth),
        .call => |c| expandCall(cx, c, subst, depth),
    };
}

fn expandCall(cx: *Ctx, c: ast.Expr.Call, subst: ?*const Subst, depth: usize) Error!*ast.Expr {
    // Expand the arguments under the current substitution first.
    const args = try cx.arena.alloc(*ast.Expr, c.args.len);
    for (c.args, 0..) |a, i| args[i] = try expandExpr(cx, a, subst, depth);

    if (cx.fns.get(c.name)) |fd| {
        if (fd.params.len != args.len) {
            cx.msg.* = std.fmt.allocPrint(cx.arena, "`{s}` expects {d} argument(s), got {d}", .{ c.name, fd.params.len, args.len }) catch "fn arity mismatch";
            return error.ExpandFailed;
        }
        var inner = Subst.init(cx.arena);
        for (fd.params, args) |pn, av| try inner.put(pn, av);
        return expandExpr(cx, fd.body, &inner, depth + 1);
    }
    return mk(cx, .{ .call = .{ .name = c.name, .args = args } });
}

fn expandMatchExpr(cx: *Ctx, m: ast.Match, subst: ?*const Subst, depth: usize) Error!*ast.Expr {
    const subject = if (m.subject) |s| try expandExpr(cx, s, subst, depth) else null;
    const arms = try cx.arena.alloc(ast.MatchArm, m.arms.len);
    for (m.arms, 0..) |arm, i| {
        const pats = try cx.arena.alloc(*ast.Expr, arm.pats.len);
        for (arm.pats, 0..) |p, j| pats[j] = try expandExpr(cx, p, subst, depth);
        arms[i] = .{
            .pats = pats,
            .guard = if (arm.guard) |g| try expandExpr(cx, g, subst, depth) else null,
            .value = try expandExpr(cx, arm.value, subst, depth),
            .is_default = arm.is_default,
        };
    }
    return mk(cx, .{ .match = .{ .subject = subject, .arms = arms } });
}

test "expandProgram inlines a user fn and drops its declaration" {
    const parser = @import("parser.zig");
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, "@batch\nfn empresa(t) = substr(t, 4, 2)\n" ++
        "read csv \"x\" | select e = empresa(id) | write stdout", &diag);
    var msg: []const u8 = "";
    const out = try expandProgram(a, prog, null, &msg);
    // the `fn` declaration is dropped → stmts are [kind, output]
    try std.testing.expectEqual(@as(usize, 2), out.stmts.len);
    try std.testing.expect(out.stmts[1] == .output);
    // `empresa(id)` is inlined to `substr(id, 4, 2)`
    const sel = out.stmts[1].output.stages[1].node.select;
    const e = sel[0].computed.expr;
    try std.testing.expect(e.* == .call);
    try std.testing.expectEqualStrings("substr", e.call.name);
    try std.testing.expectEqual(@as(usize, 3), e.call.args.len);
}

test "expandProgram rejects recursion and arity mismatch" {
    const parser = @import("parser.zig");
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var d1 = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const p1 = try parser.parseSource(a, "@batch\nfn loopy(x) = loopy(x)\nread csv \"x\" | select y = loopy(id) | write stdout", &d1);
    var m1: []const u8 = "";
    try std.testing.expectError(error.ExpandFailed, expandProgram(a, p1, null, &m1));

    var d2 = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const p2 = try parser.parseSource(a, "@batch\nfn one(b) = b\nread csv \"x\" | select y = one(id, status) | write stdout", &d2);
    var m2: []const u8 = "";
    try std.testing.expectError(error.ExpandFailed, expandProgram(a, p2, null, &m2));
}

fn outputSelect(prog: ast.Program) []const ast.SelectItem {
    for (prog.stmts) |s| if (s == .output) return s.output.stages[1].node.select;
    return &.{};
}

test "expandProgram substitutes JSON-param path access from the body" {
    const parser = @import("parser.zig");
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, "@batch\nparam job json from body\n" ++
        "read csv \"x\" | select h = job.source.host, n = job.n | write stdout", &diag);
    var msg: []const u8 = "";
    const out = try expandProgram(a, prog, "{\"source\":{\"host\":\"142.0.65.89\"},\"n\":7}", &msg);
    const sel = outputSelect(out);
    try std.testing.expect(sel[0].computed.expr.* == .str_lit);
    try std.testing.expectEqualStrings("142.0.65.89", sel[0].computed.expr.str_lit);
    try std.testing.expect(sel[1].computed.expr.* == .int_lit);
    try std.testing.expectEqual(@as(i64, 7), sel[1].computed.expr.int_lit);
}

test "expandProgram leaves JSON paths null when unbound (offline check)" {
    const parser = @import("parser.zig");
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag = parser.Diagnostic{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, "@batch\nparam job json from body\n" ++
        "read csv \"x\" | select h = job.source.host | write stdout", &diag);
    var msg: []const u8 = "";
    const out = try expandProgram(a, prog, null, &msg);
    try std.testing.expect(outputSelect(out)[0].computed.expr.* == .null_lit);
}
