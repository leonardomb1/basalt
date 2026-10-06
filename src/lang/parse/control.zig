//! Control flow: FOR EACH, the `$p.path` of a JSON parameter, and the CASE statement.

const Parser = @import("../sql_parser.zig").Parser;
const Error = @import("../sql_parser.zig").Error;
const ast = @import("../ast.zig");
const std = @import("std");
const types = @import("../types.zig");

/// `pre` receives the discovery source's bindings, which run ahead of the loop. The
/// loop variables are script constants in the body, so one may sit beside an aggregate.
pub fn parseForEach(self: *Parser, pre: *std.array_list.Managed(ast.Stmt)) Error!ast.ForEach {
    const pos = self.curPos();
    try self.expectKw("for");
    try self.expectKw("each");
    try self.expectKw("row");
    try self.expectKw("of");
    _ = try self.expect(.lparen);

    var source: ast.ForSource = undefined;
    if (self.at(.dollar_ident)) {
        source = .{ .json_path = try self.parseDollarPath() };
    } else if (self.at(.string)) {
        source = .{ .read = .{ .connector = "csv", .form = .{ .path = self.advance().text } } };
    } else if (self.isKw("select") or self.isKw("with")) {
        source = .{ .pipeline = try self.parseSubQuery(pos) };
        try pre.appendSlice(self.pending_bindings.items);
        self.pending_bindings.clearRetainingCapacity();
    } else if (self.at(.ident)) {
        const conn = try self.expectIdent();
        if (!self.isConn(conn))
            return self.fail(pos, "FOR EACH ROW OF: expected `SELECT ...`, `$param.path`, or `<conn>.QUERY($$...$$)`", .{});
        _ = try self.expect(.dot);
        try self.expectKw("query");
        _ = try self.expect(.lparen);
        const q = try self.expect(.string);
        _ = try self.expect(.rparen);
        source = .{ .read = .{ .connector = conn, .form = .{ .query = q.text } } };
    } else {
        return self.fail(self.curPos(), "FOR EACH ROW OF: expected `SELECT ...`, `$param.path`, or `<conn>.QUERY($$...$$)`", .{});
    }
    _ = try self.expect(.rparen);

    try self.expectKw("as");
    _ = try self.expect(.lparen);
    var names = std.array_list.Managed([]const u8).init(self.arena);
    var tys = std.array_list.Managed(?types.Type).init(self.arena);
    while (true) {
        try names.append(try self.expectIdent());
        if (self.eat(.colon)) {
            try tys.append(try self.parseTypeName());
        } else {
            try tys.append(null);
        }
        if (!self.eat(.comma)) break;
    }
    _ = try self.expect(.rparen);

    var hints = std.array_list.Managed(ast.Hint).init(self.arena);
    while (true) {
        if (self.eatKw("parallel")) {
            try hints.append(.{ .key = "mode", .value = .{ .ident = "parallel" }, .pos = pos });
        } else if (self.eatKw("sequential")) {
            try hints.append(.{ .key = "mode", .value = .{ .ident = "sequential" }, .pos = pos });
        } else if (self.isKw("on") and self.peekKw("error")) {
            _ = self.advance();
            _ = self.advance();
            if (self.eatKw("continue")) {
                try hints.append(.{ .key = "on_error", .value = .{ .ident = "continue" }, .pos = pos });
            } else if (self.eatKw("stop")) {
                try hints.append(.{ .key = "on_error", .value = .{ .ident = "stop" }, .pos = pos });
            } else {
                return self.fail(self.curPos(), "expected CONTINUE or STOP after ON ERROR", .{});
            }
        } else break;
    }

    const const_base = self.const_names.items.len;
    for (names.items) |n| try self.const_names.append(n);

    var body = std.array_list.Managed(ast.Stmt).init(self.arena);
    while (!self.at(.eof) and !self.isKw("end")) {
        try self.parseStatement(&body);
    }
    self.const_names.shrinkRetainingCapacity(const_base);
    try self.expectKw("end");
    try self.expectKw("for");
    _ = self.eat(.semi);

    return .{
        .var_names = try names.toOwnedSlice(),
        .var_types = try tys.toOwnedSlice(),
        .source = source,
        .hints = try hints.toOwnedSlice(),
        .body = try body.toOwnedSlice(),
        .pos = pos,
    };
}

pub fn parseDollarPath(self: *Parser) Error!ast.QualName {
    const start = self.curPos();
    const t = try self.expect(.dollar_ident);
    var parts = std.array_list.Managed([]const u8).init(self.arena);
    var safes = std.array_list.Managed(bool).init(self.arena);
    try parts.append(t.text);
    while (self.at(.dot) or self.at(.qdot)) {
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
        .dollar = true,
    };
}

pub fn parseCaseStmt(self: *Parser) Error!ast.StmtMatch {
    const pos = self.curPos();
    try self.expectKw("case");
    var subject: ?*ast.Expr = null;
    if (!self.isKw("when")) subject = try self.parseExpr();

    var arms = std.array_list.Managed(ast.StmtArm).init(self.arena);
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
        var body = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof) and !self.isKw("when") and !self.isKw("else") and !self.isKw("end")) {
            try self.parseStatement(&body);
        }
        try arms.append(.{
            .pats = try pats.toOwnedSlice(),
            .guard = guard,
            .body = try body.toOwnedSlice(),
            .is_default = false,
        });
    }
    if (self.eatKw("else")) {
        var body = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof) and !self.isKw("end")) {
            try self.parseStatement(&body);
        }
        try arms.append(.{ .pats = &.{}, .guard = null, .body = try body.toOwnedSlice(), .is_default = true });
    }
    try self.expectKw("end");
    try self.expectKw("case");
    _ = self.eat(.semi);
    if (arms.items.len == 0)
        return self.fail(pos, "CASE statement needs at least one WHEN arm", .{});
    return .{ .subject = subject, .arms = try arms.toOwnedSlice(), .pos = pos };
}
