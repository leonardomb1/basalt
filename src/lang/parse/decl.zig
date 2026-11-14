//! Declarations: connections and their HTTP clauses, functions (scalar,
//! table and statement), CALL, THROW, parameters and type names.

const Parser = @import("../sql_parser.zig").Parser;
const Error = @import("../sql_parser.zig").Error;
const Pos = @import("../sql_parser.zig").Pos;
const ast = @import("../ast.zig");
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const std = @import("std");
const msgfmt = @import("../../base/msgfmt.zig");
const table_builtins = @import("../table_builtins.zig");
const types = @import("../types.zig");

pub fn parseConnection(self: *Parser, pos: Pos) Error!ast.Connection {
    const name = try self.expectIdent();
    try self.expectKw("type");
    const connector_raw = try self.expectIdent();
    const connector = try std.ascii.allocLowerString(self.arena, connector_raw);
    var attrs = std.array_list.Managed(ast.Attr).init(self.arena);
    var has_user = false;
    var has_pass = false;
    if (self.eatKw("options")) {
        _ = try self.expect(.lparen);
        while (!self.at(.rparen)) {
            const apos = self.curPos();
            const key = try self.expectIdent();
            _ = try self.expect(.assign);
            const value = try self.parseExpr();
            if (eqlNoCase(key, "user")) has_user = true;
            if (eqlNoCase(key, "password")) has_pass = true;
            try attrs.append(.{ .key = key, .value = value, .pos = apos });
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
    }
    var hints = std.array_list.Managed(ast.Hint).init(self.arena);
    try self.parseHttpClauses(&hints);
    if (hints.items.len > 0 and !std.mem.eql(u8, connector, "http"))
        return self.fail(hints.items[0].pos, "PAGINATE / RETRY / WITH on a connection are for `http` connections only", .{});
    _ = try self.expect(.semi);
    if (!has_user) try attrs.append(.{ .key = "user", .value = try self.envCall(name, "_USER"), .pos = pos });
    if (!has_pass) try attrs.append(.{ .key = "password", .value = try self.envCall(name, "_PASS"), .pos = pos });
    return .{ .name = name, .connector = connector, .config = try attrs.toOwnedSlice(), .pos = pos, .hints = try hints.toOwnedSlice() };
}

/// `[PAGINATE ...] [RETRY ...] [WITH (...)]` in any order, for every read of a
/// connection.
pub fn parseHttpClauses(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
    while (true) {
        if (self.isKw("paginate")) {
            try self.parsePaginate(hints);
        } else if (self.isKw("retry")) {
            try self.parseRetry(hints);
        } else if (self.isKw("with") and self.peekTag() == .lparen) {
            _ = self.advance();
            try self.parseWithHints(hints);
        } else break;
    }
}

/// `GET(path, name = value ...)` / `POST(path, body = value, ...)` after `conn.`: every
/// `name = value` but `body` is a query parameter, URL-encoded when the request is built.
pub fn parseHttpCall(self: *Parser, conn: []const u8, hints: *std.array_list.Managed(ast.Hint)) Error!ast.Read {
    const pos = self.curPos();
    const verb = self.advance().text;
    const post = eqlNoCase(verb, "post");
    _ = try self.expect(.lparen);
    const path = try self.exprToTemplate(try self.parseExpr());
    if (post) try hints.append(.{ .key = "method", .value = .{ .ident = "post" }, .pos = pos });
    while (self.eat(.comma)) {
        const apos = self.curPos();
        const key = try self.expectIdent();
        _ = try self.expect(.assign);
        const expr = try self.parseExpr();
        const val = try self.exprToTemplate(expr);
        if (eqlNoCase(key, "body")) {
            if (!post) return self.fail(apos, "GET takes no body — use {s}.POST(path, body = ...)", .{conn});
            try hints.append(.{ .key = "body", .value = .{ .str = val }, .pos = apos });
        } else if (expr.* != .null_lit) {
            try hints.append(.{ .key = try msgfmt.allocPrint(self.arena, "query:{s}", .{key}), .value = .{ .str = val }, .pos = apos });
        }
    }
    _ = try self.expect(.rparen);
    return .{ .connector = conn, .form = .{ .path = path } };
}

pub fn isHttpVerb(self: *Parser) bool {
    return (self.isKw("get") or self.isKw("post")) and self.peekTag() == .lparen;
}

pub fn envCall(self: *Parser, conn_name: []const u8, suffix: []const u8) Error!*ast.Expr {
    const upper = try std.ascii.allocUpperString(self.arena, conn_name);
    const var_name = try msgfmt.allocPrint(self.arena, "{s}{s}", .{ upper, suffix });
    const arg = try self.mk(.{ .str_lit = var_name });
    const args = try self.arena.alloc(*ast.Expr, 1);
    args[0] = arg;
    return self.mk(.{ .call = .{ .name = "env", .args = args } });
}

/// `CREATE [OR REPLACE] FUNCTION name(params) AS <expr>;` or `AS <stmts> END;`. A
/// statement function's parameters are script constants inside it, like loop variables.
pub fn parseFunction(self: *Parser, pos: Pos, replace: bool) Error!ast.FnDecl {
    const name = try self.expectIdent();
    _ = try self.expect(.lparen);
    var params = std.array_list.Managed(ast.FnParam).init(self.arena);
    if (!self.at(.rparen)) {
        try params.append(try self.parseFnParam());
        while (self.eat(.comma)) try params.append(try self.parseFnParam());
    }
    _ = try self.expect(.rparen);
    var seen_default = false;
    for (params.items, 0..) |p, i| {
        for (params.items[0..i]) |q| if (std.mem.eql(u8, q.name, p.name))
            return self.fail(p.pos orelse pos, "`{s}`: parameter `{s}` is declared twice", .{ name, p.name });
        if (p.default != null) {
            seen_default = true;
        } else if (seen_default) {
            return self.fail(p.pos orelse pos, "`{s}`: parameter `{s}` without DEFAULT follows one with DEFAULT", .{ name, p.name });
        }
    }
    if (self.eatKw("returns")) {
        try self.expectKw("table");
        if (table_builtins.lookup(name) != null)
            return self.fail(pos, "`{s}` is a built-in table function and cannot be redefined", .{name});
        try self.expectKw("as");
        if (!self.isKw("select") and !self.isKw("with"))
            return self.fail(self.curPos(), "`{s}`: a table function's body is a query — SELECT ... or WITH ...", .{name});
        const body = try self.checkTableBody();
        const fd = ast.FnDecl{ .name = name, .params = try params.toOwnedSlice(), .body = .{ .table = body.toks }, .replace = replace, .pos = pos, .table_check = body.check };
        try self.table_fns.append(fd);
        return fd;
    }
    try self.expectKw("as");

    if (self.atStmtBody()) {
        var body = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof) and !self.isKw("end")) {
            try self.parseStatement(&body);
        }
        try self.expectKw("end");
        _ = try self.expect(.semi);
        if (body.items.len == 0)
            return self.fail(pos, "`{s}`: statement function body is empty", .{name});
        return .{ .name = name, .params = try params.toOwnedSlice(), .body = .{ .stmts = try body.toOwnedSlice() }, .replace = replace, .pos = pos };
    }

    const body = try self.parseExpr();
    _ = try self.expect(.semi);
    return .{ .name = name, .params = try params.toOwnedSlice(), .body = .{ .expr = body }, .replace = replace, .pos = pos };
}

/// Whether the body after `AS` opens a statement block. A `CASE` does only when
/// closed by `END CASE`; otherwise it is the scalar form.
pub fn atStmtBody(self: *Parser) bool {
    return self.isKw("load") or self.isKw("for") or self.isKw("call") or
        self.isKw("print") or self.isKw("throw") or self.isKw("select") or self.isKw("with") or
        (self.isKw("case") and self.atStmtCase());
}

/// Whether the `CASE` here is closed by `END CASE`, pairing each block opener ahead
/// (`case`, `for`) with its `end`.
pub fn atStmtCase(self: *Parser) bool {
    var depth: usize = 0;
    var j = self.i;
    while (j < self.toks.len) : (j += 1) {
        const t = self.toks[j];
        if (t.tag != .ident) continue;
        if (eqlNoCase(t.text, "case") or eqlNoCase(t.text, "for")) {
            depth += 1;
        } else if (eqlNoCase(t.text, "end")) {
            depth -= 1;
            const next = if (j + 1 < self.toks.len) self.toks[j + 1] else return false;
            const closes_named = next.tag == .ident and (eqlNoCase(next.text, "case") or eqlNoCase(next.text, "for"));
            if (depth == 0) return closes_named and eqlNoCase(next.text, "case");
            if (closes_named) j += 1;
        }
    }
    return false;
}

pub fn parseFnParam(self: *Parser) Error!ast.FnParam {
    const pos = self.curPos();
    const name = try self.expectIdent();
    var ty: ?types.Type = null;
    if (!self.at(.comma) and !self.at(.rparen) and !self.isKw("default"))
        ty = try self.parseTypeName();
    var default: ?*ast.Expr = null;
    if (self.eatKw("default")) default = try self.parseExpr();
    return .{ .name = name, .ty = ty, .default = default, .pos = pos };
}

pub fn parseCallStmt(self: *Parser) Error!ast.CallStmt {
    const pos = self.curPos();
    try self.expectKw("call");
    const name = try self.expectIdent();
    _ = try self.expect(.lparen);
    var args = std.array_list.Managed(*ast.Expr).init(self.arena);
    if (!self.at(.rparen)) {
        try args.append(try self.parseExpr());
        while (self.eat(.comma)) try args.append(try self.parseExpr());
    }
    _ = try self.expect(.rparen);
    _ = try self.expect(.semi);
    return .{ .name = name, .args = try args.toOwnedSlice(), .pos = pos };
}

/// `THROW <message> [WHEN <condition>];`, `WHEN` read greedily to end the message.
pub fn parseThrowStmt(self: *Parser) Error!ast.Throw {
    const pos = self.curPos();
    try self.expectKw("throw");
    const message = try self.parseExpr();
    const when: ?*ast.Expr = if (self.eatKw("when")) try self.parseExpr() else null;
    _ = try self.expect(.semi);
    return .{ .message = message, .when = when, .pos = pos };
}

pub fn parseParam(self: *Parser) Error!ast.Param {
    const pos = self.curPos();
    try self.expectKw("param");
    const name = try self.expectIdent();
    var is_json = false;
    var ty = types.Type.init(.string);
    if (self.isKw("json")) {
        _ = self.advance();
        is_json = true;
    } else {
        ty = try self.parseTypeName();
    }
    var default: ?*ast.Expr = null;
    if (self.eatKw("default")) default = try self.parseExpr();
    _ = try self.expect(.semi);
    return .{ .name = name, .ty = ty, .default = default, .pos = pos, .is_json = is_json };
}

pub fn parseTypeName(self: *Parser) Error!types.Type {
    const pos = self.curPos();
    const name = try self.expectIdent();
    const Map = struct { n: []const u8, k: types.TypeKind };
    const simple = [_]Map{
        .{ .n = "bool", .k = .bool },          .{ .n = "boolean", .k = .bool },
        .{ .n = "int", .k = .int },            .{ .n = "integer", .k = .int },
        .{ .n = "bigint", .k = .int },         .{ .n = "smallint", .k = .int },
        .{ .n = "tinyint", .k = .int },        .{ .n = "float", .k = .float },
        .{ .n = "real", .k = .float },         .{ .n = "double", .k = .float },
        .{ .n = "string", .k = .string },      .{ .n = "text", .k = .string },
        .{ .n = "bytes", .k = .bytes },        .{ .n = "binary", .k = .bytes },
        .{ .n = "varbinary", .k = .bytes },    .{ .n = "date", .k = .date },
        .{ .n = "time", .k = .time },          .{ .n = "timestamp", .k = .timestamp },
        .{ .n = "datetime", .k = .timestamp },
    };
    for (simple) |m| {
        if (eqlNoCase(name, m.n)) {
            if (eqlNoCase(name, "double")) _ = self.eatKw("precision");
            return types.Type.init(m.k);
        }
    }
    if (eqlNoCase(name, "varchar") or eqlNoCase(name, "char") or eqlNoCase(name, "nvarchar")) {
        if (self.eat(.lparen)) {
            _ = try self.expect(.int);
            _ = try self.expect(.rparen);
        }
        return types.Type.init(.string);
    }
    if (eqlNoCase(name, "decimal") or eqlNoCase(name, "numeric")) {
        var p: u8 = 38;
        var s: u8 = 0;
        if (self.eat(.lparen)) {
            p = try self.expectU8();
            if (self.eat(.comma)) s = try self.expectU8();
            _ = try self.expect(.rparen);
            if (p < 1 or p > 38) return self.fail(pos, "DECIMAL precision must be 1 to 38, got {d}", .{p});
            if (s > p) return self.fail(pos, "DECIMAL scale {d} is larger than its precision {d}", .{ s, p });
        }
        return types.Type.decimal(p, s);
    }
    if (eqlNoCase(name, "list")) {
        _ = try self.expect(.lparen);
        const elem = try self.arena.create(types.Type);
        elem.* = try parseInnerType(self);
        _ = try self.expect(.rparen);
        return .{ .kind = .array, .elem = elem };
    }
    if (eqlNoCase(name, "map")) {
        _ = try self.expect(.lparen);
        const kpos = self.curPos();
        const k = try self.parseTypeName();
        if (k.isNested()) return self.fail(kpos, "a MAP key must be a scalar type, got {s}", .{try k.name(self.arena)});
        _ = try self.expect(.comma);
        const v = try parseInnerType(self);
        _ = try self.expect(.rparen);
        const kv = try self.arena.dupe(types.Type.Field, &.{ .{ .name = "key", .ty = k }, .{ .name = "value", .ty = v } });
        return .{ .kind = .map, .fields = kv };
    }
    if (eqlNoCase(name, "struct")) {
        _ = try self.expect(.lparen);
        var fields = std.array_list.Managed(types.Type.Field).init(self.arena);
        while (true) {
            const fpos = self.curPos();
            const fname = try self.expectIdent();
            for (fields.items) |f| if (std.mem.eql(u8, f.name, fname)) return self.fail(fpos, "STRUCT field `{s}` is named twice", .{fname});
            try fields.append(.{ .name = fname, .ty = try parseInnerType(self) });
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
        return .{ .kind = .@"struct", .fields = fields.items };
    }
    return self.fail(pos, "unknown type `{s}`", .{name});
}

/// A nested type's element, value or field type: nullable unless `NOT NULL`
/// follows it, as SQL columns are.
fn parseInnerType(self: *Parser) Error!types.Type {
    const t = try self.parseTypeName();
    if (self.isKw("not") and self.peekKw("null")) {
        _ = self.advance();
        _ = self.advance();
        return t;
    }
    return t.asNullable();
}

pub fn expectU8(self: *Parser) Error!u8 {
    const t = try self.expect(.int);
    return std.fmt.parseInt(u8, t.text, 10) catch
        self.fail(.{ .line = t.line, .col = t.col }, "number out of range: {s}", .{t.text});
}
