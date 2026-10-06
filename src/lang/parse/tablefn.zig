//! Table functions and subqueries: a call inlined as a binding, a correlated call
//! decorrelated into a join, derived tables, and `IN (SELECT ...)` lifted into a semi
//! join.

const Parser = @import("../sql_parser.zig").Parser;
const AliasSet = @import("../sql_parser.zig").AliasSet;
const Correlated = Parser.Correlated;
const Derived = Parser.Derived;
const Error = @import("../sql_parser.zig").Error;
const LateralKey = Parser.LateralKey;
const Pos = @import("../sql_parser.zig").Pos;
const TableCall = Parser.TableCall;
const TableFrame = @import("../sql_parser.zig").TableFrame;
const Token = @import("../sql_parser.zig").Token;
const ast = @import("../ast.zig");
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const expand = @import("../expand.zig");
const isReservedAfterSource = @import("../sql_parser.zig").isReservedAfterSource;
const max_tvf_depth = @import("../sql_parser.zig").max_tvf_depth;
const std = @import("std");

pub fn findTableFn(self: *Parser, name: []const u8) ?ast.FnDecl {
    var i = self.table_fns.items.len;
    while (i > 0) {
        i -= 1;
        if (eqlNoCase(self.table_fns.items[i].name, name)) return self.table_fns.items[i];
    }
    return null;
}

/// A `WITH` name as bound: itself, or inside a table function's body a name unique
/// to this call.
pub fn cteName(self: *Parser, name: []const u8) Error![]const u8 {
    const f = self.tvf orelse return name;
    const bname = try std.fmt.allocPrint(self.arena, "__tvf{d}_{s}", .{ f.id, name });
    try f.ctes.append(.{ name, bname });
    return bname;
}

/// Parse a table function's body once at its declaration, where mistakes are reported,
/// binding nothing; returns its tokens for every call to re-parse.
pub fn checkTableBody(self: *Parser) Error![]const Token {
    const start = self.i;
    const let_base = self.let_names.items.len;
    defer self.let_names.shrinkRetainingCapacity(let_base);
    var frame = TableFrame{ .names = &.{}, .args = &.{}, .ctes = .init(self.arena), .id = 0 };
    const outer = self.tvf;
    self.tvf = &frame;
    defer self.tvf = outer;
    var discard = std.array_list.Managed(ast.Stmt).init(self.arena);
    var stages = std.array_list.Managed(ast.Stage).init(self.arena);
    try self.parseQuery(&discard, &stages);
    _ = try self.expect(.semi);
    const body = self.toks[start..self.i];
    const toks = try self.arena.alloc(Token, body.len + 1);
    @memcpy(toks[0..body.len], body);
    const last = body[body.len - 1];
    toks[body.len] = .{ .tag = .eof, .text = "", .line = last.line, .col = last.col };
    return toks;
}

/// `f(args)` in FROM or JOIN: bind the arguments, re-parse the body, and lower it to a
/// binding like a derived table, so a filter on the call still reaches the source.
pub fn callTableFn(self: *Parser, fd: ast.FnDecl, pos: Pos, place: enum { from, join, lateral }) Error!TableCall {
    const lateral = place == .lateral;
    _ = try self.expect(.lparen);
    var args = std.array_list.Managed(*ast.Expr).init(self.arena);
    if (!self.at(.rparen)) {
        try args.append(try self.parseExpr());
        while (self.eat(.comma)) try args.append(try self.parseExpr());
    }
    _ = try self.expect(.rparen);

    var required: usize = 0;
    for (fd.params) |p| {
        if (p.default == null) required += 1;
    }
    const n = args.items.len;
    if (n < required or n > fd.params.len) {
        if (required == fd.params.len)
            return self.fail(pos, "`{s}` expects {d} argument(s), got {d}", .{ fd.name, fd.params.len, n });
        return self.fail(pos, "`{s}` expects {d} to {d} argument(s), got {d}", .{ fd.name, required, fd.params.len, n });
    }
    for (fd.params[n..]) |p| try args.append(p.default.?);
    const names = try self.arena.alloc([]const u8, fd.params.len);
    var correlated = std.array_list.Managed(Correlated).init(self.arena);
    for (fd.params, args.items, names, 1..) |p, *a, *nm, i| {
        nm.* = p.name;
        if (!self.constItemExpr(a.*)) {
            const column = a.*.* == .field and !a.*.field.dollar;
            if (lateral and column) {
                const marker = try self.mk(.{ .field = .{ .parts = try self.arena.dupe([]const u8, &.{ "\x00lateral", p.name }) } });
                try correlated.append(.{ .param = p.name, .left = a.*.field, .marker = marker });
                a.* = marker;
                continue;
            }
            if (column and place == .join)
                return self.fail(pos, "`{s}`: argument {d} (`{s}`) names a column — pass a row's value with `JOIN LATERAL {s}(...)`", .{ fd.name, i, p.name, fd.name });
            return self.fail(pos, "`{s}`: argument {d} (`{s}`) must be a constant — a literal, a `$param`, or an expression over them", .{ fd.name, i, p.name });
        }
        const want = (p.ty orelse continue).kind;
        const got = expand.literalKind(a.*) orelse continue;
        if (!expand.acceptsLiteral(want, got))
            return self.fail(pos, "`{s}`: argument {d} (`{s}`) expects {s}, got {s}", .{ fd.name, i, p.name, expand.typeWord(want), expand.typeWord(got) });
    }
    if (self.tvf_depth >= max_tvf_depth)
        return self.fail(pos, "table function `{s}` nests more than {d} calls deep — does it reach itself?", .{ fd.name, max_tvf_depth });

    self.derived_n += 1;
    var frame = TableFrame{ .names = names, .args = args.items, .ctes = .init(self.arena), .id = self.derived_n };
    const saved_toks = self.toks;
    const saved_i = self.i;
    const outer = self.tvf;
    self.toks = fd.body.table;
    self.i = 0;
    self.tvf = &frame;
    self.tvf_depth += 1;
    defer {
        self.toks = saved_toks;
        self.i = saved_i;
        self.tvf = outer;
        self.tvf_depth -= 1;
    }
    var inner = std.array_list.Managed(ast.Stmt).init(self.arena);
    var stages = std.array_list.Managed(ast.Stage).init(self.arena);
    self.parseQuery(&inner, &stages) catch |e| return self.inTableFn(e, fd.name, pos);
    _ = self.expect(.semi) catch |e| return self.inTableFn(e, fd.name, pos);
    const keys = if (correlated.items.len == 0) &.{} else try self.decorrelate(fd.name, pos, &stages, inner.items, correlated.items);

    const bname = try std.fmt.allocPrint(self.arena, "__tvf{d}_{s}", .{ frame.id, fd.name });
    try self.let_names.append(bname);
    try self.pending_bindings.appendSlice(inner.items);
    try self.pending_bindings.append(.{ .binding = .{
        .name = bname,
        .pipeline = .{ .stages = try stages.toOwnedSlice(), .pos = pos },
        .pos = pos,
    } });
    return .{ .binding = bname, .keys = keys };
}

/// Turn each `x = $param` of a `JOIN LATERAL` body into an output column the join
/// matches: one read and a hash join, not a query per row. Refused when the body has a
/// LIMIT, DISTINCT, GROUP BY or window, or uses `$param` any other way.
pub fn decorrelate(self: *Parser, name: []const u8, pos: Pos, stages: *std.array_list.Managed(ast.Stage), inner: []const ast.Stmt, cor: []const Correlated) Error![]const LateralKey {
    const p0 = cor[0].param;
    for (stages.items) |st| switch (st.node) {
        .read, .ref, .join, .filter, .select => {},
        else => return self.fail(pos, "`{s}`: a row's column passed as `${s}` needs a body that is a plain SELECT ... FROM ... WHERE — this one has {s}, which the join could not apply per row", .{ name, p0, stageWord(st.node) }),
    };
    for (inner) |stmt| {
        if (stmt != .binding) continue;
        for (stmt.binding.pipeline.stages) |st| {
            if (stageMentions(st, cor)) |pn|
                return self.fail(pos, "`{s}`: `${s}` reaches a WITH or a table function inside the body — with a row's column, it may only appear as `column = ${s}` in the body's WHERE", .{ name, pn, pn });
        }
    }

    var eqs = std.array_list.Managed(struct { cor: usize, col: ast.QualName }).init(self.arena);
    var k: usize = 0;
    while (k < stages.items.len) {
        const st = &stages.items[k];
        if (st.node != .filter) {
            k += 1;
            continue;
        }
        var parts = std.array_list.Managed(*ast.Expr).init(self.arena);
        try splitConj(st.node.filter, &parts);
        var keep = std.array_list.Managed(*ast.Expr).init(self.arena);
        for (parts.items) |cj| {
            if (eqOfMarker(cj, cor)) |m| {
                try eqs.append(.{ .cor = m.cor, .col = m.col });
                continue;
            }
            for (cor) |c| if (containsExpr(cj, c.marker))
                return self.fail(pos, "`{s}`: with a row's column, `${s}` may only appear as `column = ${s}` in the body's WHERE", .{ name, c.param, c.param });
            try keep.append(cj);
        }
        if (keep.items.len == 0) {
            _ = stages.orderedRemove(k);
            continue;
        }
        var e = keep.items[0];
        for (keep.items[1..]) |r| e = try self.mk(.{ .binary = .{ .op = .@"and", .l = e, .r = r } });
        st.node = .{ .filter = e };
        k += 1;
    }
    for (cor, 0..) |c, ci| {
        for (eqs.items) |q| {
            if (q.cor == ci) break;
        } else return self.fail(pos, "`{s}`: `${s}` gets a row's column, but the body never says `column = ${s}` in its WHERE — that is what the join matches on", .{ name, c.param, c.param });
    }

    var sel_at: ?usize = null;
    for (stages.items, 0..) |st, i| if (st.node == .select) {
        sel_at = i;
    };
    var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
    if (sel_at) |si| try items.appendSlice(stages.items[si].node.select) else try items.append(.star);
    for (items.items) |*it| {
        if (it.* != .computed) continue;
        for (cor, 0..) |c, ci| {
            if (it.computed.expr == c.marker) {
                const q = for (eqs.items) |e| {
                    if (e.cor == ci) break e.col;
                } else unreachable;
                it.computed.expr = try self.mk(.{ .field = q });
            } else if (containsExpr(it.computed.expr, c.marker))
                return self.fail(pos, "`{s}`: with a row's column, `${s}` may only be a SELECT item on its own (it is then the column it equals)", .{ name, c.param });
        }
    }
    const keys = try self.arena.alloc(LateralKey, eqs.items.len);
    for (eqs.items, keys) |q, *key| {
        const out = outputNameOf(items.items, q.col) orelse blk: {
            const pn = cor[q.cor].param;
            for (items.items) |it| {
                const taken = switch (it) {
                    .field => |f| std.mem.eql(u8, f.last(), pn),
                    .computed => |cc| std.mem.eql(u8, cc.name, pn),
                    else => false,
                };
                if (taken) return self.fail(pos, "`{s}`: the body's output already has a column `{s}`; select `{s}` so the join can match on it", .{ name, pn, q.col.last() });
            }
            try items.append(.{ .computed = .{ .name = pn, .expr = try self.mk(.{ .field = q.col }) } });
            break :blk pn;
        };
        key.* = .{ .left = cor[q.cor].left, .right = out };
    }
    const sel = ast.Stage{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos };
    if (sel_at) |si| stages.items[si] = sel else try stages.append(sel);
    return keys;
}

/// Name the call that reached an error inside a table function's body, once, at
/// the call the script wrote.
pub fn inTableFn(self: *Parser, e: Error, name: []const u8, pos: Pos) Error {
    if (e == error.ParseFailed and self.tvf_depth == 1)
        self.diag.msg = std.fmt.allocPrint(self.arena, "in `{s}` called at {d}:{d}: {s}", .{ name, pos.line, pos.col, self.diag.msg }) catch self.diag.msg;
    return e;
}

/// `( <query> ) [AS] alias` in FROM or JOIN, lowered to a binding. Inner bindings
/// gather in a local list: sharing `parseQuery`'s own drain list once lost them.
pub fn parseDerivedTable(self: *Parser) Error!Derived {
    const dpos = self.curPos();
    _ = try self.expect(.lparen);
    var sub_stages = std.array_list.Managed(ast.Stage).init(self.arena);
    var inner_bindings = std.array_list.Managed(ast.Stmt).init(self.arena);
    try self.parseQuery(&inner_bindings, &sub_stages);
    _ = try self.expect(.rparen);

    _ = self.eatKw("as");
    var alias: ?[]const u8 = null;
    var alias_pos = dpos;
    if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
        alias = self.advance().text;
        alias_pos = self.prevPos();
    }
    self.derived_n += 1;
    const name = if (alias) |al|
        try std.fmt.allocPrint(self.arena, "__derived{d}_{s}", .{ self.derived_n, al })
    else
        try std.fmt.allocPrint(self.arena, "__derived{d}", .{self.derived_n});

    try self.let_names.append(name);
    try self.pending_bindings.appendSlice(inner_bindings.items);
    try self.pending_bindings.append(.{ .binding = .{
        .name = name,
        .pipeline = .{ .stages = try sub_stages.toOwnedSlice(), .pos = dpos },
        .pos = dpos,
    } });
    return .{ .binding = name, .alias = alias, .alias_pos = alias_pos };
}

/// A join's reading right side (path, `IDENTIFIER`, table or query), lowered as
/// `(SELECT * FROM it) alias` would be, with its read hints.
pub fn parseJoinRead(self: *Parser) Error!Derived {
    const rpos = self.curPos();
    var scratch = AliasSet{};
    var hints = std.array_list.Managed(ast.Hint).init(self.arena);
    const node = try self.parseFromSource(&scratch, &hints);
    if (self.isKw("with") and self.peekTag() == .lparen) {
        _ = self.advance();
        try self.parseWithHints(&hints);
    }
    const alias: ?[]const u8 = if (scratch.n > 0) scratch.names[0] else null;
    self.derived_n += 1;
    const name = if (alias) |al|
        try std.fmt.allocPrint(self.arena, "__derived{d}_{s}", .{ self.derived_n, al })
    else
        try std.fmt.allocPrint(self.arena, "__derived{d}", .{self.derived_n});
    try self.let_names.append(name);
    const stages = try self.arena.alloc(ast.Stage, 1);
    stages[0] = .{ .node = node, .hints = try hints.toOwnedSlice(), .pos = rpos };
    try self.pending_bindings.append(.{ .binding = .{
        .name = name,
        .pipeline = .{ .stages = stages, .pos = rpos },
        .pos = rpos,
    } });
    return .{ .binding = name, .alias = alias, .alias_pos = rpos };
}

/// A parenthesized query in expression or LET position, `(` consumed, its bindings
/// routed to `pending_bindings`.
pub fn parseSubqueryPipeline(self: *Parser) Error!ast.Pipeline {
    const qpos = self.curPos();
    var sub_stages = std.array_list.Managed(ast.Stage).init(self.arena);
    var inner_bindings = std.array_list.Managed(ast.Stmt).init(self.arena);
    try self.parseQuery(&inner_bindings, &sub_stages);
    _ = try self.expect(.rparen);
    try self.pending_bindings.appendSlice(inner_bindings.items);
    return .{ .stages = try sub_stages.toOwnedSlice(), .pos = qpos };
}

/// Output name of a pipeline's single column, or null, walked back from the last
/// stage as the plan will resolve it.
pub fn firstOutName(stages: []const ast.Stage) ?[]const u8 {
    var i = stages.len;
    while (i > 0) {
        i -= 1;
        switch (stages[i].node) {
            .select => |items| {
                if (items.len != 1) return null;
                return switch (items[0]) {
                    .field => |f| f.parts[f.parts.len - 1],
                    .computed => |c| c.name,
                    else => null,
                };
            },
            .aggregate => |ag| {
                if (ag.by.len + ag.aggs.len != 1) return null;
                if (ag.aggs.len == 1) return ag.aggs[0].name;
                return ag.by[0].parts[ag.by[0].parts.len - 1];
            },
            .filter, .sort, .limit, .distinct => {},
            else => return null,
        }
    }
    return null;
}

pub fn stageWord(n: ast.Stage.Node) []const u8 {
    return switch (n) {
        .aggregate => "a GROUP BY or an aggregate",
        .distinct => "DISTINCT",
        .limit => "a LIMIT",
        .sort => "an ORDER BY",
        .window => "a window function",
        .union_ => "a UNION, INTERSECT or EXCEPT",
        .explode => "an UNNEST",
        else => @tagName(n),
    };
}

/// The parameter a stage passes a `JOIN LATERAL` marker to, if any.
pub fn stageMentions(st: ast.Stage, cor: []const Correlated) ?[]const u8 {
    for (cor) |c| {
        const hit = switch (st.node) {
            .filter => |e| containsExpr(e, c.marker),
            .select => |items| for (items) |it| {
                if (it == .computed and containsExpr(it.computed.expr, c.marker)) break true;
            } else false,
            else => false,
        };
        if (hit) return c.param;
    }
    return null;
}

pub fn splitConj(e: *ast.Expr, out: *std.array_list.Managed(*ast.Expr)) Error!void {
    if (e.* == .binary and e.binary.op == .@"and") {
        try splitConj(e.binary.l, out);
        try splitConj(e.binary.r, out);
    } else try out.append(e);
}

/// `col = $p` or `$p = col` for a `JOIN LATERAL` marker: which one, and `col`.
pub fn eqOfMarker(e: *const ast.Expr, cor: []const Correlated) ?struct { cor: usize, col: ast.QualName } {
    if (e.* != .binary or e.binary.op != .eq) return null;
    const b = e.binary;
    for (cor, 0..) |c, i| {
        const other = if (b.l == c.marker) b.r else if (b.r == c.marker) b.l else continue;
        if (other.* != .field or other.field.dollar) return null;
        return .{ .cor = i, .col = other.field };
    }
    return null;
}

/// The name `col` leaves the SELECT list under, as itself, renamed or in a `*`; null
/// otherwise. A computed item that only shares the name is not it.
pub fn outputNameOf(items: []const ast.SelectItem, col: ast.QualName) ?[]const u8 {
    const nm = col.last();
    for (items) |it| switch (it) {
        .field => |f| if (std.mem.eql(u8, f.last(), nm)) return nm,
        .computed => |cc| if (cc.expr.* == .field and std.mem.eql(u8, cc.expr.field.last(), nm)) return cc.name,
        .star => return nm,
        .star_except => |ex| {
            for (ex) |x| {
                if (std.mem.eql(u8, x, nm)) break;
            } else return nm;
        },
        .star_rename => |rn| {
            for (rn) |r| {
                if (std.mem.eql(u8, r.from, nm)) return r.to;
            }
            return nm;
        },
    };
    return null;
}

/// Whether `e` contains `needle` by pointer, to reject an `IN (SELECT ...)` sentinel
/// off the AND spine.
pub fn containsExpr(e: *const ast.Expr, needle: *const ast.Expr) bool {
    if (e == needle) return true;
    return switch (e.*) {
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit, .field, .lambda_var => false,
        .lambda => |l| containsExpr(l.body, needle),
        .unary => |u| containsExpr(u.e, needle),
        .binary => |b| containsExpr(b.l, needle) or containsExpr(b.r, needle),
        .is_null => |n| containsExpr(n.e, needle),
        .cast => |c| containsExpr(c.e, needle),
        .cond => |c| containsExpr(c.cond, needle) or containsExpr(c.then, needle) or containsExpr(c.els, needle),
        .call => |c| blk: {
            for (c.args) |a| {
                if (containsExpr(a, needle)) break :blk true;
            }
            break :blk false;
        },
        .match => |m| blk: {
            if (m.subject) |s| {
                if (containsExpr(s, needle)) break :blk true;
            }
            for (m.arms) |arm| {
                for (arm.pats) |p| {
                    if (containsExpr(p, needle)) break :blk true;
                }
                if (arm.guard) |g| {
                    if (containsExpr(g, needle)) break :blk true;
                }
                if (containsExpr(arm.value, needle)) break :blk true;
            }
            break :blk false;
        },
        .let_in => |li| containsExpr(li.value, needle) or containsExpr(li.body, needle),
    };
}

pub fn isSentinel(self: *Parser, e: *const ast.Expr) bool {
    for (self.pending_semijoins.items) |sj| {
        if (sj.sentinel == e) return true;
    }
    return false;
}

/// Remove the semi-join sentinels from a WHERE's top-level AND spine, returning what
/// remains (null if nothing). A sentinel under OR, NOT or CASE is an error: dropping
/// it there would change the predicate.
pub fn liftSemiJoins(self: *Parser, e: *ast.Expr, pos: Pos) Error!?*ast.Expr {
    if (self.isSentinel(e)) return null;
    if (e.* == .binary and e.binary.op == .@"and") {
        const l = try self.liftSemiJoins(e.binary.l, pos);
        const r = try self.liftSemiJoins(e.binary.r, pos);
        if (l == null) return r;
        if (r == null) return l;
        e.binary.l = l.?;
        e.binary.r = r.?;
        return e;
    }
    for (self.pending_semijoins.items) |sj| {
        if (containsExpr(e, sj.sentinel))
            return self.fail(sj.pos, "IN (SELECT ...) is only supported as a top-level AND condition of WHERE — not under OR, NOT or CASE", .{});
    }
    return e;
}
