//! Expressions: precedence climbing with depth limits, unary and primary terms,
//! calls and lambdas, field references and the CASE expression.

const Parser = @import("../sql_parser.zig").Parser;
const BinInfo = Parser.BinInfo;
const Error = @import("../sql_parser.zig").Error;
const aggregates = @import("../aggregates.zig");
const ast = @import("../ast.zig");
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const firstOutName = Parser.firstOutName;
const max_expr_depth = Parser.max_expr_depth;
const max_paren_depth = Parser.max_paren_depth;
const std = @import("std");

/// One call argument: an expression, or a lambda `x -> body` / `(acc, x) -> body`,
/// which only the JSON array functions accept.
pub fn parseCallArg(self: *Parser) Error!*ast.Expr {
    const n = self.lambdaHead() orelse return self.parseExpr();
    const pos = self.curPos();
    const paren = self.at(.lparen);
    if (paren) _ = self.advance();
    const params = try self.arena.alloc([]const u8, n);
    for (params, 0..) |*pp, k| {
        if (k > 0) _ = self.advance();
        const t = self.advance();
        for (params[0..k]) |prev| {
            if (std.mem.eql(u8, prev, t.text)) return self.fail(.{ .line = t.line, .col = t.col }, "lambda parameter `{s}` is named twice", .{t.text});
        }
        pp.* = t.text;
    }
    if (paren) _ = self.advance();
    _ = self.advance();
    if (self.lambda_n + n > self.lambda_names.len)
        return self.fail(pos, "lambdas nest too deep ({d} parameters in scope at most)", .{self.lambda_names.len});
    for (params) |pp| {
        self.lambda_names[self.lambda_n] = pp;
        self.lambda_n += 1;
    }
    defer self.lambda_n -= n;
    const body = try self.parseExpr();
    return self.mk(.{ .lambda = .{ .params = params, .body = body } });
}

/// The parameter count when the tokens ahead open a lambda (`x ->` or `(a, b) ->`),
/// else null.
pub fn lambdaHead(self: *Parser) ?usize {
    if (self.at(.ident)) return if (self.peekTag() == .arrow) 1 else null;
    if (!self.at(.lparen)) return null;
    var j = self.i + 1;
    var n: usize = 0;
    while (j + 1 < self.toks.len and self.toks[j].tag == .ident) : (j += 2) {
        n += 1;
        switch (self.toks[j + 1].tag) {
            .comma => continue,
            .rparen => return if (j + 2 < self.toks.len and self.toks[j + 2].tag == .arrow) n else null,
            else => return null,
        }
    }
    return null;
}

/// The lambda parameter `name` refers to, innermost first, if any is in scope.
pub fn lambdaParam(self: *Parser, name: []const u8) ?[]const u8 {
    var i = self.lambda_n;
    while (i > 0) {
        i -= 1;
        if (std.mem.eql(u8, self.lambda_names[i], name)) return self.lambda_names[i];
    }
    return null;
}

pub fn parseExpr(self: *Parser) Error!*ast.Expr {
    const pos = self.curPos();
    self.expr_nest += 1;
    defer self.expr_nest -= 1;
    const e = try self.parseBin(0);
    if (self.expr_nest == 1 and deeperThan(e, max_expr_depth))
        return self.fail(pos, "expression nests more than {d} levels deep — a long chain of `+`, `||` or comparisons; split it, or build the value in steps", .{max_expr_depth});
    return e;
}

/// Whether `e` is more than `limit` levels deep, stopping at the limit so the check
/// cannot exhaust the stack itself.
pub fn deeperThan(e: *const ast.Expr, limit: usize) bool {
    if (limit == 0) return true;
    return switch (e.*) {
        .binary => |b| deeperThan(b.l, limit - 1) or deeperThan(b.r, limit - 1),
        .unary => |u| deeperThan(u.e, limit - 1),
        .is_null => |n| deeperThan(n.e, limit - 1),
        .cast => |c| deeperThan(c.e, limit - 1),
        .call => |c| for (c.args) |a| {
            if (deeperThan(a, limit - 1)) break true;
        } else false,
        .cond => |c| deeperThan(c.cond, limit - 1) or deeperThan(c.then, limit - 1) or deeperThan(c.els, limit - 1),
        else => false,
    };
}

/// A left-deep run of one AND or OR rebuilt balanced, operands in order: both are
/// associative under three-valued logic, and the depth drops from n to log n.
pub fn balanceChain(self: *Parser, e: *ast.Expr) Error!*ast.Expr {
    if (e.* != .binary) return e;
    const op = e.binary.op;
    if (op != .@"and" and op != .@"or") return e;
    var n: usize = 1;
    var node = e;
    while (node.* == .binary and node.binary.op == op) : (node = node.binary.l) n += 1;
    if (n <= 4) return e;
    const items = try self.arena.alloc(*ast.Expr, n);
    node = e;
    var i = n - 1;
    while (node.* == .binary and node.binary.op == op) : (node = node.binary.l) {
        items[i] = node.binary.r;
        i -= 1;
    }
    items[0] = node;
    return self.buildBalanced(op, items);
}

pub fn buildBalanced(self: *Parser, op: ast.BinOp, items: []const *ast.Expr) Error!*ast.Expr {
    if (items.len == 1) return items[0];
    const mid = items.len / 2;
    return self.mk(.{ .binary = .{ .op = op, .l = try self.buildBalanced(op, items[0..mid]), .r = try self.buildBalanced(op, items[mid..]) } });
}

/// Binding powers, low to high: `or` 10, `and` 20, unary `not` 25, `??` 30, comparisons
/// 40, `|` 42, `^` 44, `&` 46, shifts 48, `+ - ||` 50, `* / %` 60.
pub fn binInfo(self: *Parser) ?BinInfo {
    switch (self.curTag()) {
        .eq, .assign => return .{ .op = .eq, .lbp = 40 },
        .ne => return .{ .op = .ne, .lbp = 40 },
        .lt => return .{ .op = .lt, .lbp = 40 },
        .le => return .{ .op = .le, .lbp = 40 },
        .gt => return .{ .op = .gt, .lbp = 40 },
        .ge => return .{ .op = .ge, .lbp = 40 },
        .bar => return .{ .op = .bit_or, .lbp = 42 },
        .caret => return .{ .op = .bit_xor, .lbp = 44 },
        .amp => return .{ .op = .bit_and, .lbp = 46 },
        .shl => return .{ .op = .shl, .lbp = 48 },
        .shr => return .{ .op = .shr, .lbp = 48 },
        .plus => return .{ .op = .add, .lbp = 50 },
        .minus => return .{ .op = .sub, .lbp = 50 },
        .star => return .{ .op = .mul, .lbp = 60 },
        .slash => return .{ .op = .div, .lbp = 60 },
        .percent => return .{ .op = .mod, .lbp = 60 },
        .ident => {
            if (self.isKw("and")) return .{ .op = .@"and", .lbp = 20 };
            if (self.isKw("or")) return .{ .op = .@"or", .lbp = 10 };
            return null;
        },
        else => return null,
    }
}

/// BETWEEN's bounds parse above AND's binding power, since BETWEEN uses AND as its
/// separator.
pub fn parseBin(self: *Parser, min_bp: u8) Error!*ast.Expr {
    var lhs = try self.parseUnary();
    while (true) {
        if (self.isKw("is") and min_bp < 40) {
            _ = self.advance();
            const negated = self.eatKw("not");
            if (self.eatKw("null")) {
                lhs = try self.mk(.{ .is_null = .{ .e = lhs, .negated = negated, .kind = .is_null } });
            } else if (self.eatKw("empty")) {
                lhs = try self.mk(.{ .is_null = .{ .e = lhs, .negated = negated, .kind = .is_empty } });
            } else {
                return self.fail(self.curPos(), "expected NULL or EMPTY after IS", .{});
            }
            continue;
        }
        if (self.isKw("like") and min_bp < 40) {
            _ = self.advance();
            const pat = try self.parseBin(40);
            const args = try self.arena.alloc(*ast.Expr, 2);
            args[0] = lhs;
            args[1] = pat;
            lhs = try self.mk(.{ .call = .{ .name = "like", .args = args } });
            continue;
        }
        if (self.isKw("not") and self.peekKw("like") and min_bp < 40) {
            _ = self.advance();
            _ = self.advance();
            const pat = try self.parseBin(40);
            const args = try self.arena.alloc(*ast.Expr, 2);
            args[0] = lhs;
            args[1] = pat;
            const call = try self.mk(.{ .call = .{ .name = "like", .args = args } });
            lhs = try self.mk(.{ .unary = .{ .op = .not, .e = call } });
            continue;
        }
        if ((self.isKw("in") or (self.isKw("not") and self.peekKw("in"))) and min_bp < 40) {
            const negated = self.isKw("not");
            if (negated) _ = self.advance();
            const inpos = self.curPos();
            _ = self.advance();
            _ = try self.expect(.lparen);
            if (self.isKw("select") or self.isKw("with")) {
                if (!self.in_where)
                    return self.fail(inpos, "IN (SELECT ...) is only supported in a WHERE clause", .{});
                if (lhs.* != .field)
                    return self.fail(inpos, "the left side of IN (SELECT ...) must be a plain column", .{});
                const pipe = try self.parseSubqueryPipeline();
                const col = firstOutName(pipe.stages) orelse
                    return self.fail(inpos, "the subquery of IN must produce exactly one named column", .{});
                self.derived_n += 1;
                const bname = try std.fmt.allocPrint(self.arena, "__insq{d}", .{self.derived_n});
                try self.let_names.append(bname);
                try self.pending_bindings.append(.{ .binding = .{
                    .name = bname,
                    .pipeline = pipe,
                    .pos = inpos,
                } });
                const sentinel = try self.mk(.{ .bool_lit = true });
                try self.pending_semijoins.append(.{
                    .sentinel = sentinel,
                    .lhs = lhs,
                    .binding = bname,
                    .right_col = col,
                    .negated = negated,
                    .pos = inpos,
                });
                lhs = sentinel;
                continue;
            }
            var alt: ?*ast.Expr = null;
            while (true) {
                const v = try self.parseExpr();
                const cmp = try self.mk(.{ .binary = .{ .op = .eq, .l = lhs, .r = v } });
                alt = if (alt) |acc| try self.mk(.{ .binary = .{ .op = .@"or", .l = acc, .r = cmp } }) else cmp;
                if (!self.eat(.comma)) break;
            }
            _ = try self.expect(.rparen);
            alt = try self.balanceChain(alt.?);
            lhs = if (negated) try self.mk(.{ .unary = .{ .op = .not, .e = alt.? } }) else alt.?;
            continue;
        }
        if ((self.isKw("between") or (self.isKw("not") and self.peekKw("between"))) and min_bp < 40) {
            const negated = self.isKw("not");
            if (negated) _ = self.advance();
            _ = self.advance();
            const lo = try self.parseBin(40);
            if (!self.eatKw("and"))
                return self.fail(self.curPos(), "expected `AND` between the bounds of BETWEEN", .{});
            const hi = try self.parseBin(40);
            const ge = try self.mk(.{ .binary = .{ .op = .ge, .l = lhs, .r = lo } });
            const le = try self.mk(.{ .binary = .{ .op = .le, .l = lhs, .r = hi } });
            const both = try self.mk(.{ .binary = .{ .op = .@"and", .l = ge, .r = le } });
            lhs = if (negated) try self.mk(.{ .unary = .{ .op = .not, .e = both } }) else both;
            continue;
        }
        if (self.at(.qq) and min_bp < 30) {
            _ = self.advance();
            const rhs = try self.parseBin(30);
            const args = try self.arena.alloc(*ast.Expr, 2);
            args[0] = lhs;
            args[1] = rhs;
            lhs = try self.mk(.{ .call = .{ .name = "coalesce", .args = args } });
            continue;
        }
        if (self.at(.pipe) and min_bp < 50) {
            _ = self.advance();
            const rhs = try self.parseBin(50);
            const args = try self.arena.alloc(*ast.Expr, 2);
            args[0] = lhs;
            args[1] = rhs;
            lhs = try self.mk(.{ .call = .{ .name = "concat", .args = args } });
            continue;
        }
        const info = self.binInfo() orelse break;
        if (info.lbp <= min_bp) break;
        _ = self.advance();
        const rhs = try self.parseBin(info.lbp);
        lhs = try self.mk(.{ .binary = .{ .op = info.op, .l = lhs, .r = rhs } });
    }
    return self.balanceChain(lhs);
}

pub fn parseUnary(self: *Parser) Error!*ast.Expr {
    if (self.unary_nest >= max_paren_depth)
        return self.fail(self.curPos(), "expression nests more than {d} levels of parentheses, calls or unary operators", .{max_paren_depth});
    self.unary_nest += 1;
    defer self.unary_nest -= 1;
    if (self.eat(.minus)) {
        const e = try self.parseUnary();
        return self.mk(.{ .unary = .{ .op = .neg, .e = e } });
    }
    if (self.eat(.tilde)) {
        const e = try self.parseUnary();
        return self.mk(.{ .unary = .{ .op = .bit_not, .e = e } });
    }
    if (self.isKw("not") and !self.peekKw("like") and !self.peekKw("in")) {
        _ = self.advance();
        const e = try self.parseBin(25);
        return self.mk(.{ .unary = .{ .op = .not, .e = e } });
    }
    return self.parsePrimary();
}

/// An aggregate refuses arguments past its maximum: extras were once dropped, so
/// `SUM(x, id)` answered `SUM(x)`.
pub fn parsePrimary(self: *Parser) Error!*ast.Expr {
    const t = self.cur();
    switch (t.tag) {
        .int => {
            _ = self.advance();
            const v = std.fmt.parseInt(i64, t.text, 10) catch
                return self.fail(self.curPos(), "bad integer `{s}`", .{t.text});
            return self.mk(.{ .int_lit = v });
        },
        .float => {
            _ = self.advance();
            const v = std.fmt.parseFloat(f64, t.text) catch
                return self.fail(self.curPos(), "bad float `{s}`", .{t.text});
            return self.mk(.{ .float_lit = v });
        },
        .string => {
            _ = self.advance();
            return self.mk(.{ .str_lit = t.text });
        },
        .dollar_ident => {
            const q = try self.parseDollarPath();
            if (self.tvf) |f| if (q.parts.len == 1) if (f.arg(q.parts[0])) |a| return a;
            return self.mk(.{ .field = q });
        },
        .lparen => {
            if (self.peekKw("select") or self.peekKw("with")) {
                const spos = self.curPos();
                if (self.query_depth == 0)
                    return self.fail(spos, "a scalar subquery is only supported inside a query — bind it first with `LET x = (SELECT ...);`", .{});
                _ = self.advance();
                const pipe = try self.parseSubqueryPipeline();
                self.derived_n += 1;
                const name = try std.fmt.allocPrint(self.arena, "__scalar{d}", .{self.derived_n});
                try self.pending_bindings.append(.{ .let_const = .{ .name = name, .expr = null, .query = pipe, .pos = spos } });
                try self.const_names.append(name);
                const parts = try self.arena.alloc([]const u8, 1);
                parts[0] = name;
                return self.mk(.{ .field = .{ .parts = parts, .dollar = true } });
            }
            _ = self.advance();
            const e = try self.parseExpr();
            _ = try self.expect(.rparen);
            return e;
        },
        .qident => return self.mk(.{ .field = try self.parseQualNameField() }),
        .ident => {
            if (eqlNoCase(t.text, "null")) {
                _ = self.advance();
                return self.mk(.null_lit);
            }
            if (eqlNoCase(t.text, "true")) {
                _ = self.advance();
                return self.mk(.{ .bool_lit = true });
            }
            if (eqlNoCase(t.text, "false")) {
                _ = self.advance();
                return self.mk(.{ .bool_lit = false });
            }
            if (eqlNoCase(t.text, "case")) return self.parseCaseExpr();
            if (eqlNoCase(t.text, "search") and self.peekTag() == .lparen) return self.parseSearch();
            if (eqlNoCase(t.text, "extract") and self.peekTag() == .lparen) {
                _ = self.advance();
                _ = self.advance();
                const unit = try self.expectColName();
                if (!self.eatKw("from")) _ = try self.expect(.comma);
                const src = try self.parseExpr();
                _ = try self.expect(.rparen);
                const xargs = try self.arena.alloc(*ast.Expr, 2);
                xargs[0] = try self.mk(.{ .str_lit = unit });
                xargs[1] = src;
                return self.mk(.{ .call = .{ .name = "extract", .args = xargs } });
            }
            if ((eqlNoCase(t.text, "cast") or eqlNoCase(t.text, "try_cast")) and self.peekTag() == .lparen) {
                const safe = eqlNoCase(t.text, "try_cast");
                _ = self.advance();
                _ = self.advance();
                const e = try self.parseExpr();
                try self.expectKw("as");
                const ty = try self.parseTypeName();
                _ = try self.expect(.rparen);
                return self.mk(.{ .cast = .{ .e = e, .ty = ty, .safe = safe } });
            }
            if (eqlNoCase(t.text, "identifier") and self.peekTag() == .lparen)
                return self.mk(.{ .field = try self.parseColRef() });
            if (eqlNoCase(t.text, "if") and self.peekTag() == .lparen) {
                _ = self.advance();
                _ = self.advance();
                const c = try self.parseExpr();
                _ = try self.expect(.comma);
                const then = try self.parseExpr();
                _ = try self.expect(.comma);
                const els = try self.parseExpr();
                _ = try self.expect(.rparen);
                return self.mk(.{ .cond = .{ .cond = c, .then = then, .els = els } });
            }
            if (eqlNoCase(t.text, "let")) {
                _ = self.advance();
                const name = try self.expectIdent();
                _ = try self.expect(.assign);
                const value = try self.parseBin(40);
                try self.expectKw("in");
                const body = try self.parseExpr();
                return self.mk(.{ .let_in = .{ .name = name, .value = value, .body = body } });
            }
            if (self.peekTag() == .lparen) {
                _ = self.advance();
                _ = self.advance();
                var args = std.array_list.Managed(*ast.Expr).init(self.arena);
                var call_distinct = false;
                if (!self.at(.rparen)) {
                    call_distinct = self.eatKw("distinct");
                    if (self.at(.star) and self.peekTag() == .rparen) {
                        _ = self.advance();
                    } else {
                        try args.append(try self.parseCallArg());
                        while (self.eat(.comma)) try args.append(try self.parseCallArg());
                    }
                }
                _ = try self.expect(.rparen);
                const lower = try std.ascii.allocLowerString(self.arena, t.text);
                if (aggregates.lookup(lower)) |f| if (args.items.len > aggregates.spec(f).max_args)
                    return self.fail(.{ .line = t.line, .col = t.col }, "`{s}` takes one argument, not {d}", .{ lower, args.items.len });
                const name_span = ast.Span{ .start = .{ .line = t.line, .col = t.col }, .end = .{ .line = t.end_line, .col = t.end_col } };
                return self.mk(.{ .call = .{ .name = lower, .args = try args.toOwnedSlice(), .distinct = call_distinct, .span = name_span } });
            }
            if (self.peekTag() != .dot) if (self.lambdaParam(t.text)) |name| {
                _ = self.advance();
                return self.mk(.{ .lambda_var = name });
            };
            const q = try self.parseQualNameField();
            return self.mk(.{ .field = q });
        },
        else => return self.fail(self.curPos(), "expected an expression, found {s}", .{t.tag.describe()}),
    }
}

/// A column reference in an expression: `a`, `t.col`, `a.b.c` (with `?.`).
pub fn parseQualNameField(self: *Parser) Error!ast.QualName {
    const start = self.curPos();
    var parts = std.array_list.Managed([]const u8).init(self.arena);
    var safes = std.array_list.Managed(bool).init(self.arena);
    try parts.append(try self.expectIdent());
    while ((self.at(.dot) or self.at(.qdot)) and (self.peekTag() == .ident or self.peekTag() == .qident)) {
        const safe = self.at(.qdot);
        _ = self.advance();
        try parts.append(try self.expectIdent());
        try safes.append(safe);
    }
    var any_safe = false;
    for (safes.items) |s| any_safe = any_safe or s;
    return .{
        .parts = try parts.toOwnedSlice(),
        .safe = if (any_safe) try safes.toOwnedSlice() else &.{},
        .span = self.spanFrom(start),
    };
}

/// CASE expression -> ast.Match (subject + `,` alternation, or guard form).
pub fn parseCaseExpr(self: *Parser) Error!*ast.Expr {
    try self.expectKw("case");
    var subject: ?*ast.Expr = null;
    if (!self.isKw("when")) subject = try self.parseExpr();

    var arms = std.array_list.Managed(ast.MatchArm).init(self.arena);
    while (self.eatKw("when")) {
        var pats = std.array_list.Managed(*ast.Expr).init(self.arena);
        var guard: ?*ast.Expr = null;
        if (subject != null) {
            try pats.append(try self.parseExpr());
            while (self.eat(.comma)) try pats.append(try self.parseExpr());
        } else {
            guard = try self.parseExpr();
        }
        try self.expectKw("then");
        const value = try self.parseExpr();
        try arms.append(.{ .pats = try pats.toOwnedSlice(), .guard = guard, .value = value, .is_default = false });
    }
    if (self.eatKw("else")) {
        const value = try self.parseExpr();
        try arms.append(.{ .pats = &.{}, .guard = null, .value = value, .is_default = true });
    }
    try self.expectKw("end");
    if (arms.items.len == 0)
        return self.fail(self.curPos(), "CASE needs at least one WHEN arm", .{});
    return self.mk(.{ .match = .{ .subject = subject, .arms = try arms.toOwnedSlice() } });
}

/// `search(cols, query)`. The columns become string items of a `search_cols` call
/// (`*`; `*@t` for `t.*`; `-name` for each `EXCEPT` name; `=col` or `=t.col` for a
/// listed column) so that no pass reading column references mistakes them for
/// some: the evaluator resolves them against the rows it is given.
pub fn parseSearch(self: *Parser) Error!*ast.Expr {
    const start = self.advance();
    _ = self.advance();
    var items = std.array_list.Managed(*ast.Expr).init(self.arena);
    if (self.at(.lparen)) {
        _ = self.advance();
        while (true) {
            const q = try self.parseQualNameField();
            try items.append(try self.mk(.{ .str_lit = try std.fmt.allocPrint(self.arena, "={s}", .{try std.mem.join(self.arena, ".", q.parts)}) }));
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
    } else {
        var star = false;
        if (self.at(.star)) {
            _ = self.advance();
            try items.append(try self.mk(.{ .str_lit = "*" }));
            star = true;
        } else if (self.at(.ident) and self.peekTag() == .dot and self.i + 2 < self.toks.len and self.toks[self.i + 2].tag == .star) {
            const rel = self.advance().text;
            _ = self.advance();
            _ = self.advance();
            try items.append(try self.mk(.{ .str_lit = try std.fmt.allocPrint(self.arena, "*@{s}", .{rel}) }));
            star = true;
        } else {
            const q = try self.parseQualNameField();
            try items.append(try self.mk(.{ .str_lit = try std.fmt.allocPrint(self.arena, "={s}", .{try std.mem.join(self.arena, ".", q.parts)}) }));
        }
        if (star and self.eatKw("except")) {
            _ = try self.expect(.lparen);
            while (true) {
                const name = try self.expectColName();
                try items.append(try self.mk(.{ .str_lit = try std.fmt.allocPrint(self.arena, "-{s}", .{name}) }));
                if (!self.eat(.comma)) break;
            }
            _ = try self.expect(.rparen);
        }
    }
    if (!self.eat(.comma))
        return self.fail(self.curPos(), "search takes the columns, then the text: `search(*, 'sp 2026')`, `search(t.*, 'x')`, `search((a, b), 'x')`", .{});
    const query = try self.parseExpr();
    _ = try self.expect(.rparen);
    const args = try self.arena.alloc(*ast.Expr, 2);
    args[0] = try self.mk(.{ .call = .{ .name = "search_cols", .args = try items.toOwnedSlice() } });
    args[1] = query;
    const span = ast.Span{ .start = .{ .line = start.line, .col = start.col }, .end = .{ .line = start.end_line, .col = start.end_col } };
    return self.mk(.{ .call = .{ .name = "search", .args = args, .span = span } });
}
