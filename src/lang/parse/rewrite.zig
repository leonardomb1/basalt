//! Rewrites of a parsed query: join keys and the conditions an ON carries beside
//! them, aggregates lifted out of expressions, HAVING, and alias stripping.

const Parser = @import("../sql_parser.zig").Parser;
const AliasSet = @import("../sql_parser.zig").AliasSet;
const Error = @import("../sql_parser.zig").Error;
const ExprAlias = Parser.ExprAlias;
const Pos = @import("../sql_parser.zig").Pos;
const aggregates = @import("../aggregates.zig");
const ast = @import("../ast.zig");
const quote = @import("../quote.zig");
const binOpText = @import("../sql_parser.zig").binOpText;
const outName = @import("../sql_parser.zig").outName;
const qualHasPrefix = @import("../sql_parser.zig").qualHasPrefix;
const std = @import("std");
const stripPrefix = @import("../sql_parser.zig").stripPrefix;
const stripQual = @import("../sql_parser.zig").stripQual;

/// The one shape a join key may take: the index is built on stored columns, so a
/// computed key has to be computed first.
pub fn joinKeyFail(self: *Parser) Error {
    return self.fail(self.curPos(), "join keys must be plain columns; compute them in the CTE / a select first", .{});
}

pub fn parseJoinKey(self: *Parser) Error!ast.QualName {
    if (!(self.atName() or self.at(.string))) return self.joinKeyFail();
    return self.parseQualNameTok();
}

pub fn parseQualNameTok(self: *Parser) Error!ast.QualName {
    const start = self.curPos();
    var parts = std.array_list.Managed([]const u8).init(self.arena);
    try parts.append(try self.expectColName());
    while (self.at(.dot) and (self.peekTag() == .ident or self.peekTag() == .qident or self.peekTag() == .string)) {
        _ = self.advance();
        try parts.append(try self.expectColName());
    }
    return .{ .parts = try parts.toOwnedSlice(), .span = self.spanFrom(start) };
}

/// Every column an expression reads, so a pre-aggregation projection keeps the
/// aggregates' inputs.
pub fn collectFields(_: *Parser, e: *const ast.Expr, out: *std.array_list.Managed(ast.QualName)) Error!void {
    const S = struct {
        fn add(o: *std.array_list.Managed(ast.QualName), n: *const ast.Expr) ?bool {
            if (n.* != .field) return null;
            o.append(n.field) catch return true;
            return false;
        }
    };
    if (ast.anyExpr(e, out, S.add)) return error.OutOfMemory;
}

/// Render an expression as the key GROUP BY, ORDER BY, HAVING and lifted aggregates
/// bind by. It is exact where SQL is: columns keep their case, string literals their
/// quotes and case, operators their grouping, so two different expressions never
/// share a key. Arguments render in full: rendering them as `?` once merged
/// `sum(v*2)` and `sum(v+100)` into one accumulator. Once a query's tables are
/// known (`key_aliases`) a column renders as it resolves, so `a.g` and `g` of the
/// FROM table `a` share a key.
pub fn exprKey(self: *Parser, e: *const ast.Expr, buf: *std.array_list.Managed(u8)) Error!void {
    switch (e.*) {
        .field => |q0| {
            const q = if (self.key_aliases) |al| stripQual(q0, al) else q0;
            if (q.dollar) try buf.append('$');
            for (q.parts, 0..) |part, i| {
                if (i != 0) try buf.append('.');
                if (i < q.safe.len and q.safe[i]) try buf.append('?');
                if (plainName(part)) try buf.appendSlice(part) else try quote.ident(buf, part);
            }
        },
        .int_lit => |v| try buf.print("{d}", .{v}),
        .float_lit => |v| try buf.print("{e}", .{v}),
        .dec_lit => |v| try buf.print("{f}", .{v}),
        .str_lit => |v| try quote.strLit(buf, v),
        .bool_lit => |v| try buf.appendSlice(if (v) "true" else "false"),
        .null_lit => try buf.appendSlice("null"),
        .call => |c| {
            for (c.name) |ch| try buf.append(std.ascii.toLower(ch));
            try buf.append('(');
            if (c.distinct) try buf.appendSlice("distinct ");
            if (c.args.len == 0) try buf.append('*');
            for (c.args, 0..) |a, i| {
                if (i != 0) try buf.append(',');
                try self.exprKey(a, buf);
            }
            try buf.append(')');
        },
        .unary => |u| {
            try buf.appendSlice(switch (u.op) {
                .neg => "(-",
                .not => "(not ",
                .bit_not => "(~",
            });
            try self.exprKey(u.e, buf);
            try buf.append(')');
        },
        .binary => |b| {
            try buf.append('(');
            try self.exprKey(b.l, buf);
            try buf.print(" {s} ", .{binOpText(b.op)});
            try self.exprKey(b.r, buf);
            try buf.append(')');
        },
        .cast => |c| {
            try buf.appendSlice("cast(");
            try self.exprKey(c.e, buf);
            try buf.print(" as {s}", .{try c.ty.name(self.arena)});
            if (c.safe) try buf.append('?');
            try buf.append(')');
        },
        .is_null => |n| {
            try buf.append('(');
            try self.exprKey(n.e, buf);
            try buf.appendSlice(if (n.negated) " is not " else " is ");
            try buf.appendSlice(@tagName(n.kind));
            try buf.append(')');
        },
        .cond => |c| {
            try buf.appendSlice("if(");
            try self.exprKey(c.cond, buf);
            try buf.append(',');
            try self.exprKey(c.then, buf);
            try buf.append(',');
            try self.exprKey(c.els, buf);
            try buf.append(')');
        },
        .match => |m| {
            try buf.appendSlice("match(");
            if (m.subject) |s| try self.exprKey(s, buf);
            for (m.arms) |arm| {
                try buf.appendSlice(if (arm.is_default) ";else" else ";when");
                for (arm.pats) |p| {
                    try buf.append(' ');
                    try self.exprKey(p, buf);
                }
                if (arm.guard) |g| {
                    try buf.appendSlice(" if ");
                    try self.exprKey(g, buf);
                }
                try buf.appendSlice(" then ");
                try self.exprKey(arm.value, buf);
            }
            try buf.append(')');
        },
        .let_in => |l| {
            try buf.print("let({s}=", .{l.name});
            try self.exprKey(l.value, buf);
            try buf.appendSlice(" in ");
            try self.exprKey(l.body, buf);
            try buf.append(')');
        },
        .lambda => |l| {
            try buf.append('(');
            for (l.params, 0..) |pp, k| {
                if (k > 0) try buf.append(',');
                try buf.appendSlice(pp);
            }
            try buf.appendSlice(")->");
            try self.exprKey(l.body, buf);
        },
        .lambda_var => |n| try buf.appendSlice(n),
    }
}

fn plainName(s: []const u8) bool {
    if (s.len == 0 or std.ascii.isDigit(s[0])) return false;
    for (s) |c| if (!(std.ascii.isLower(c) or std.ascii.isDigit(c) or c == '_')) return false;
    return true;
}

pub const Bound = struct { name: []const u8, item: bool };

/// The name `e` binds to: the SELECT item it repeats, else a hidden `__aN` that every
/// mention of the same expression shares. Hidden names stay plain, as a pushed-down
/// query writes them as aliases.
pub fn bindName(self: *Parser, e: *const ast.Expr, map: []const ExprAlias) Error!Bound {
    var buf = std.array_list.Managed(u8).init(self.arena);
    try self.exprKey(e, &buf);
    for (map) |m| if (std.mem.eql(u8, m.synth, buf.items)) return .{ .name = m.out, .item = true };
    for (self.hidden_keys.items) |hk| if (std.mem.eql(u8, hk[0], buf.items)) return .{ .name = hk[1], .item = false };
    const name = try self.freshName("__a{d}");
    try self.hidden_keys.append(self.arena, .{ try buf.toOwnedSlice(), name });
    return .{ .name = name, .item = false };
}

/// Whether an aggregate call is buried inside an expression, as in `round(avg(x), 2)`.
pub fn containsAgg(e: *const ast.Expr) bool {
    const S = struct {
        fn hit(_: void, n: *const ast.Expr) ?bool {
            return if (n.* == .call and aggregates.lookup(n.call.name) != null) true else null;
        }
    };
    return ast.anyExpr(e, {}, S.hit);
}

/// Point the post-aggregate entry for `name` at `expr`, for an item lifted out of the
/// pre-aggregate projection that has no column to read.
pub fn repointPostItem(
    _: *Parser,
    post: *std.array_list.Managed(ast.SelectItem),
    name: []const u8,
    expr: *ast.Expr,
) Error!void {
    for (post.items) |*p| {
        if (!std.mem.eql(u8, outName(p.*) orelse continue, name)) continue;
        p.* = .{ .computed = .{ .name = name, .expr = expr } };
        return;
    }
    try post.append(.{ .computed = .{ .name = name, .expr = expr } });
}

/// Whether a SELECT item is one value for the whole query (literals, `now()`-style
/// calls, any `$name`, a CASE over them), so it may sit beside aggregates. A bare
/// column never is.
pub fn constItemExpr(_: *Parser, e: *const ast.Expr) bool {
    const S = struct {
        fn varies(_: void, n: *const ast.Expr) ?bool {
            return switch (n.*) {
                .field => |q| !q.dollar,
                .call => |c| if (aggregates.lookup(c.name) != null) true else null,
                else => null,
            };
        }
    };
    return !ast.anyExpr(e, {}, S.varies);
}

/// Lift the aggregate calls out of a scalar expression into columns of the aggregate
/// stage, leaving a projection over them. Naming goes through `exprKey` and the alias
/// map, so repeated mentions share one column.
pub fn liftAggs(
    self: *Parser,
    e: *ast.Expr,
    aggs: *std.array_list.Managed(ast.AggItem),
    map: []const ExprAlias,
    pos: Pos,
) Error!*ast.Expr {
    const Ctx = struct {
        p: *Parser,
        a: *std.array_list.Managed(ast.AggItem),
        m: []const ExprAlias,
        pos: Pos,
    };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .call) {
                if (aggregates.lookup(node.call.name)) |f| {
                    for (node.call.args) |a| {
                        if (containsAgg(a))
                            return cx.p.fail(cx.pos, "aggregate functions cannot be nested", .{});
                    }
                    const key = (try cx.p.bindName(node, cx.m)).name;
                    const seen = for (cx.a.items) |it| {
                        if (std.mem.eql(u8, it.name, key)) break true;
                    } else false;
                    if (!seen) {
                        const arg: ?*ast.Expr = if (node.call.args.len > 0) node.call.args[0] else null;
                        try cx.a.append(.{ .name = key, .func = f, .arg = arg, .distinct = node.call.distinct });
                    }
                    return cx.p.mk(.{ .field = try cx.p.singleName(key) });
                }
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .a = aggs, .m = map, .pos = pos }, e);
}

pub fn singleName(self: *Parser, name: []const u8) Error!ast.QualName {
    return ast.QualName.one(self.arena, name);
}

/// Which sides `e` names: a column of the join's right side (`rname.col`), and any
/// other, a left column or one written without its table.
pub fn sidesOf(e: *const ast.Expr, rname: []const u8) struct { right: bool, other: bool } {
    const S = struct {
        fn right(r: []const u8, n: *const ast.Expr) ?bool {
            return if (n.* == .field) !n.field.dollar and qualHasPrefix(n.field, r) else null;
        }
        fn other(r: []const u8, n: *const ast.Expr) ?bool {
            return if (n.* == .field) !n.field.dollar and !qualHasPrefix(n.field, r) else null;
        }
    };
    return .{ .right = ast.anyExpr(e, rname, S.right), .other = ast.anyExpr(e, rname, S.other) };
}

/// Whether `e` names a column with its table (`qualified`), or without.
pub fn hasFieldOf(e: *const ast.Expr, qualified: bool) bool {
    const S = struct {
        fn hit(q: bool, n: *const ast.Expr) ?bool {
            return if (n.* == .field) !n.field.dollar and (n.field.parts.len > 1) == q else null;
        }
    };
    return ast.anyExpr(e, qualified, S.hit);
}

/// `e` with the join's right name taken off its columns: `sra.x` is `x` to the right
/// side's own rows.
pub fn stripRightExpr(self: *Parser, e: *ast.Expr, rname: []const u8) Error!*ast.Expr {
    const Ctx = struct { p: *Parser, rname: []const u8 };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field and !node.field.dollar and qualHasPrefix(node.field, cx.rname))
                return cx.p.mk(.{ .field = stripPrefix(node.field, cx.rname) });
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .rname = rname }, e);
}

/// Rewrite `alias.x` to `x` in an expression tree, a bare `x` stamped with the
/// FROM's join sides (`stripQual`).
pub fn stripExpr(self: *Parser, e: *ast.Expr, aliases: *const AliasSet) Error!*ast.Expr {
    const Ctx = struct { p: *Parser, aliases: *const AliasSet };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field) {
                const q = stripQual(node.field, cx.aliases);
                if (q.parts.ptr != node.field.parts.ptr or q.sides.len != node.field.sides.len)
                    return cx.p.mk(.{ .field = q });
                return @constCast(node);
            }
            if (node.* == .call and std.mem.eql(u8, node.call.name, "search_cols"))
                return stripSearchCols(cx.p, node.call, cx.aliases);
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .aliases = aliases }, e);
}

/// `search`'s column items with the FROM table's alias dropped, as its columns
/// carry none: `*@a` becomes `*@` (that table's own), `=a.x` becomes `=x`.
fn stripSearchCols(p: *Parser, c: ast.Expr.Call, aliases: *const AliasSet) Error!*ast.Expr {
    const args = try p.arena.alloc(*ast.Expr, c.args.len);
    for (c.args, args) |a, *o| {
        o.* = a;
        if (a.* != .str_lit) continue;
        const item = a.str_lit;
        if (std.mem.startsWith(u8, item, "*@") and item.len > 2 and aliases.strips(item[2..])) {
            o.* = try p.mk(.{ .str_lit = "*@" });
        } else if (std.mem.startsWith(u8, item, "=")) {
            if (std.mem.indexOfScalar(u8, item, '.')) |dot| if (aliases.strips(item[1..dot])) {
                o.* = try p.mk(.{ .str_lit = try std.fmt.allocPrint(p.arena, "={s}", .{item[dot + 1 ..]}) });
            };
        }
    }
    return p.mk(.{ .call = .{ .name = c.name, .args = args } });
}
