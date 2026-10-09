//! Rewrites of a parsed query: join keys and the conditions an ON carries beside
//! them, aggregates lifted out of expressions, HAVING, and alias stripping.

const Parser = @import("../sql_parser.zig").Parser;
const AliasSet = @import("../sql_parser.zig").AliasSet;
const Error = @import("../sql_parser.zig").Error;
const ExprAlias = Parser.ExprAlias;
const Pos = @import("../sql_parser.zig").Pos;
const aggFunc = @import("../sql_parser.zig").aggFunc;
const ast = @import("../ast.zig");
const qualHasPrefix = @import("../sql_parser.zig").qualHasPrefix;
const resolveExprAlias = Parser.resolveExprAlias;
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
pub fn collectFields(self: *Parser, e: *const ast.Expr, out: *std.array_list.Managed(ast.QualName)) Error!void {
    switch (e.*) {
        .field => |q| out.append(q) catch return error.OutOfMemory,
        .lambda => |l| try self.collectFields(l.body, out),
        .lambda_var => {},
        .unary => |u| try self.collectFields(u.e, out),
        .binary => |b| {
            try self.collectFields(b.l, out);
            try self.collectFields(b.r, out);
        },
        .call => |c| for (c.args) |a| try self.collectFields(a, out),
        .cond => |c| {
            try self.collectFields(c.cond, out);
            try self.collectFields(c.then, out);
            try self.collectFields(c.els, out);
        },
        .cast => |c| try self.collectFields(c.e, out),
        .is_null => |n| try self.collectFields(n.e, out),
        .let_in => |l| {
            try self.collectFields(l.value, out);
            try self.collectFields(l.body, out);
        },
        .match => |m| {
            if (m.subject) |subj| try self.collectFields(subj, out);
            for (m.arms) |arm| {
                for (arm.pats) |pat| try self.collectFields(pat, out);
                if (arm.guard) |g| try self.collectFields(g, out);
                try self.collectFields(arm.value, out);
            }
        },
        .null_lit, .bool_lit, .int_lit, .float_lit, .str_lit => {},
    }
}

/// Render an expression as `synthName` renders tokens, so HAVING matches the item that
/// computed it. Arguments render in full: rendering them as `?` once merged
/// `sum(v*2)` and `sum(v+100)` into one accumulator.
pub fn exprKey(self: *Parser, e: *const ast.Expr, buf: *std.array_list.Managed(u8)) Error!void {
    switch (e.*) {
        .field => |q| for (q.parts, 0..) |part, i| {
            if (i != 0) buf.append('.') catch return error.OutOfMemory;
            for (part) |c| buf.append(std.ascii.toLower(c)) catch return error.OutOfMemory;
        },
        .int_lit => |v| buf.writer().print("{d}", .{v}) catch return error.OutOfMemory,
        .str_lit => |v| for (v) |c| buf.append(std.ascii.toLower(c)) catch return error.OutOfMemory,
        .call => |c| {
            for (c.name) |ch| buf.append(std.ascii.toLower(ch)) catch return error.OutOfMemory;
            buf.append('(') catch return error.OutOfMemory;
            if (c.args.len == 0) buf.append('*') catch return error.OutOfMemory;
            for (c.args, 0..) |a, i| {
                if (i != 0) buf.append(',') catch return error.OutOfMemory;
                try self.exprKey(a, buf);
            }
            buf.append(')') catch return error.OutOfMemory;
        },
        .float_lit => |v| buf.writer().print("{d}", .{v}) catch return error.OutOfMemory,
        .bool_lit => |v| buf.appendSlice(if (v) "true" else "false") catch return error.OutOfMemory,
        .null_lit => buf.appendSlice("null") catch return error.OutOfMemory,
        .unary => |u| {
            buf.appendSlice(switch (u.op) {
                .neg => "-",
                .not => "not ",
                .bit_not => "~",
            }) catch return error.OutOfMemory;
            try self.exprKey(u.e, buf);
        },
        .binary => |b| {
            try self.exprKey(b.l, buf);
            buf.appendSlice(switch (b.op) {
                .add => "+",
                .sub => "-",
                .mul => "*",
                .div => "/",
                .mod => "%",
                .bit_and => "&",
                .bit_or => "|",
                .bit_xor => "^",
                .shl => "<<",
                .shr => ">>",
                .eq => "=",
                .ne => "<>",
                .lt => "<",
                .le => "<=",
                .gt => ">",
                .ge => ">=",
                .@"and" => " and ",
                .@"or" => " or ",
            }) catch return error.OutOfMemory;
            try self.exprKey(b.r, buf);
        },
        .cast => |c| {
            buf.appendSlice("cast(") catch return error.OutOfMemory;
            try self.exprKey(c.e, buf);
            buf.appendSlice(" as ") catch return error.OutOfMemory;
            buf.appendSlice(@tagName(c.ty.kind)) catch return error.OutOfMemory;
            if (c.safe) buf.append('?') catch return error.OutOfMemory;
            buf.append(')') catch return error.OutOfMemory;
        },
        .is_null => |n| {
            try self.exprKey(n.e, buf);
            buf.appendSlice(if (n.negated) " is not " else " is ") catch return error.OutOfMemory;
            buf.appendSlice(@tagName(n.kind)) catch return error.OutOfMemory;
        },
        .cond => |c| {
            buf.appendSlice("if(") catch return error.OutOfMemory;
            try self.exprKey(c.cond, buf);
            buf.append(',') catch return error.OutOfMemory;
            try self.exprKey(c.then, buf);
            buf.append(',') catch return error.OutOfMemory;
            try self.exprKey(c.els, buf);
            buf.append(')') catch return error.OutOfMemory;
        },
        .lambda => |l| {
            for (l.params, 0..) |pp, k| {
                if (k > 0) buf.append(',') catch return error.OutOfMemory;
                buf.appendSlice(pp) catch return error.OutOfMemory;
            }
            buf.appendSlice("->") catch return error.OutOfMemory;
            try self.exprKey(l.body, buf);
        },
        .lambda_var => |n| buf.appendSlice(n) catch return error.OutOfMemory,
        else => buf.append('?') catch return error.OutOfMemory,
    }
}

/// Whether an aggregate call is buried inside an expression, as in `round(avg(x), 2)`.
pub fn containsAgg(e: *const ast.Expr) bool {
    return switch (e.*) {
        .call => |c| aggFunc(c.name) != null or blk: {
            for (c.args) |a| if (containsAgg(a)) break :blk true;
            break :blk false;
        },
        .unary => |u| containsAgg(u.e),
        .binary => |b| containsAgg(b.l) or containsAgg(b.r),
        .cond => |c| containsAgg(c.cond) or containsAgg(c.then) or containsAgg(c.els),
        .cast => |c| containsAgg(c.e),
        .is_null => |n| containsAgg(n.e),
        .let_in => |l| containsAgg(l.value) or containsAgg(l.body),
        .match => |m| blk: {
            if (m.subject) |s| if (containsAgg(s)) break :blk true;
            for (m.arms) |arm| {
                for (arm.pats) |p| if (containsAgg(p)) break :blk true;
                if (arm.guard) |g| if (containsAgg(g)) break :blk true;
                if (containsAgg(arm.value)) break :blk true;
            }
            break :blk false;
        },
        else => false,
    };
}

/// Point the post-aggregate entry for `name` at `expr`, for an item lifted out of the
/// pre-aggregate projection that has no column to read.
pub fn repointPostItem(
    self: *Parser,
    post: *std.array_list.Managed(ast.SelectItem),
    name: []const u8,
    expr: *ast.Expr,
) Error!void {
    for (post.items) |*p| {
        const p_name = switch (p.*) {
            .field => |q| q.last(),
            .computed => |c| c.name,
            else => continue,
        };
        if (!std.mem.eql(u8, p_name, name)) continue;
        p.* = .{ .computed = .{ .name = name, .expr = expr } };
        return;
    }
    try post.append(.{ .computed = .{ .name = name, .expr = expr } });
    _ = self;
}

/// Whether a SELECT item is one value for the whole query (literals, `now()`-style
/// calls, any `$name`), so it may sit beside aggregates. A bare column never is.
pub fn constItemExpr(self: *Parser, e: *const ast.Expr) bool {
    return switch (e.*) {
        .int_lit, .float_lit, .str_lit, .bool_lit, .null_lit => true,
        .field => |q| q.dollar,
        .lambda => |l| self.constItemExpr(l.body),
        .lambda_var => true,
        .unary => |u| self.constItemExpr(u.e),
        .binary => |b| self.constItemExpr(b.l) and self.constItemExpr(b.r),
        .cond => |c| self.constItemExpr(c.cond) and self.constItemExpr(c.then) and self.constItemExpr(c.els),
        .cast => |c| self.constItemExpr(c.e),
        .is_null => |n| self.constItemExpr(n.e),
        .call => |c| {
            if (aggFunc(c.name) != null) return false;
            for (c.args) |a| if (!self.constItemExpr(a)) return false;
            return true;
        },
        else => false,
    };
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
                if (aggFunc(node.call.name)) |f| {
                    for (node.call.args) |a| {
                        if (containsAgg(a))
                            return cx.p.fail(cx.pos, "aggregate functions cannot be nested", .{});
                    }
                    var buf = std.array_list.Managed(u8).init(cx.p.arena);
                    try cx.p.exprKey(node, &buf);
                    const key = resolveExprAlias(cx.m, buf.toOwnedSlice() catch return error.OutOfMemory);

                    var seen = false;
                    for (cx.a.items) |it| {
                        if (std.mem.eql(u8, it.name, key)) seen = true;
                    }
                    if (!seen) {
                        const arg: ?*ast.Expr = if (node.call.args.len > 0) node.call.args[0] else null;
                        cx.a.append(.{
                            .name = key,
                            .func = f,
                            .arg = arg,
                            .distinct = node.call.distinct,
                        }) catch return error.OutOfMemory;
                    }
                    const parts = cx.p.arena.alloc([]const u8, 1) catch return error.OutOfMemory;
                    parts[0] = key;
                    return cx.p.mk(.{ .field = .{ .parts = parts } });
                }
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .a = aggs, .m = map, .pos = pos }, e);
}

/// Add aggregates that only HAVING names as output columns, dropped again by the
/// post-aggregate projection. Returns whether any was added.
pub fn addHavingAggs(
    self: *Parser,
    h: *const ast.Expr,
    aggs: *std.array_list.Managed(ast.AggItem),
    map: []const ExprAlias,
) Error!bool {
    switch (h.*) {
        .call => |c| {
            if (aggFunc(c.name)) |f| {
                var buf = std.array_list.Managed(u8).init(self.arena);
                try self.exprKey(h, &buf);
                const key = buf.toOwnedSlice() catch return error.OutOfMemory;
                const want = resolveExprAlias(map, key);
                for (aggs.items) |it| {
                    if (std.mem.eql(u8, it.name, want)) return false;
                }
                const arg: ?*ast.Expr = if (c.args.len > 0) c.args[0] else null;
                aggs.append(.{
                    .name = want,
                    .func = f,
                    .arg = arg,
                    .distinct = c.distinct,
                }) catch return error.OutOfMemory;
                return true;
            }
            var any = false;
            for (c.args) |a| {
                if (try self.addHavingAggs(a, aggs, map)) any = true;
            }
            return any;
        },
        .unary => |u| return self.addHavingAggs(u.e, aggs, map),
        .binary => |b| {
            const l = try self.addHavingAggs(b.l, aggs, map);
            const r = try self.addHavingAggs(b.r, aggs, map);
            return l or r;
        },
        .cond => |c| {
            const a = try self.addHavingAggs(c.cond, aggs, map);
            const b = try self.addHavingAggs(c.then, aggs, map);
            const d = try self.addHavingAggs(c.els, aggs, map);
            return a or b or d;
        },
        .cast => |c| return self.addHavingAggs(c.e, aggs, map),
        .is_null => |n| return self.addHavingAggs(n.e, aggs, map),
        else => return false,
    }
}

pub fn singleName(self: *Parser, name: []const u8) Error!ast.QualName {
    const parts = self.arena.alloc([]const u8, 1) catch return error.OutOfMemory;
    parts[0] = name;
    return .{ .parts = parts };
}

/// Swap each aggregate call in HAVING for the column the aggregate stage already
/// produced, making the clause an ordinary filter.
pub fn havingRewrite(self: *Parser, e: *ast.Expr, map: []const ExprAlias) Error!*ast.Expr {
    const Ctx = struct { p: *Parser, m: []const ExprAlias };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .call and aggFunc(node.call.name) != null) {
                var buf = std.array_list.Managed(u8).init(cx.p.arena);
                try cx.p.exprKey(node, &buf);
                const key = buf.toOwnedSlice() catch return error.OutOfMemory;
                const parts = cx.p.arena.alloc([]const u8, 1) catch return error.OutOfMemory;
                parts[0] = resolveExprAlias(cx.m, key);
                return cx.p.mk(.{ .field = .{ .parts = parts } });
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .m = map }, e);
}

/// Which sides `e` names: a column of the join's right side (`rname.col`), and any
/// other, a left column or one written without its table.
pub fn sidesOf(self: *Parser, e: *ast.Expr, rname: []const u8) Error!struct { right: bool, other: bool } {
    const Ctx = struct { p: *Parser, rname: []const u8, right: *bool, other: *bool };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field) {
                if (!node.field.dollar) {
                    if (qualHasPrefix(node.field, cx.rname)) cx.right.* = true else cx.other.* = true;
                }
                return @constCast(node);
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    var right = false;
    var other = false;
    _ = try S.recur(.{ .p = self, .rname = rname, .right = &right, .other = &other }, e);
    return .{ .right = right, .other = other };
}

/// Whether `e` names a column without its table.
pub fn hasBareField(self: *Parser, e: *ast.Expr) Error!bool {
    const Ctx = struct { p: *Parser, bare: *bool };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field) {
                if (!node.field.dollar and node.field.parts.len == 1) cx.bare.* = true;
                return @constCast(node);
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    var bare = false;
    _ = try S.recur(.{ .p = self, .bare = &bare }, e);
    return bare;
}

pub fn qualOne(self: *Parser, name: []const u8) Error!ast.QualName {
    const parts = try self.arena.alloc([]const u8, 1);
    parts[0] = name;
    return .{ .parts = parts };
}

pub fn namesOtherSide(self: *Parser, e: *ast.Expr, rname: []const u8) Error!bool {
    const Ctx = struct { p: *Parser, rname: []const u8, other: *bool };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field) {
                if (!node.field.dollar and !qualHasPrefix(node.field, cx.rname)) cx.other.* = true;
                return @constCast(node);
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    var other = false;
    _ = try S.recur(.{ .p = self, .rname = rname, .other = &other }, e);
    return other;
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

/// Rewrite `alias.x` to `x` in an expression tree.
pub fn stripExpr(self: *Parser, e: *ast.Expr, aliases: *const AliasSet) Error!*ast.Expr {
    const Ctx = struct { p: *Parser, aliases: *const AliasSet };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field) {
                const q = stripQual(node.field, cx.aliases);
                if (q.parts.ptr != node.field.parts.ptr)
                    return cx.p.mk(.{ .field = q });
                return @constCast(node);
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .aliases = aliases }, e);
}
