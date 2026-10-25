//! The parser's statements: the program, DESCRIBE and SHOW, EXPLAIN, LET, PRINT,
//! CREATE and ACCEPT, and the `${...}` templates statements carry.

const Parser = @import("../sql_parser.zig").Parser;
const AliasSet = @import("../sql_parser.zig").AliasSet;
const Error = @import("../sql_parser.zig").Error;
const PendingSemiJoin = Parser.PendingSemiJoin;
const Pos = @import("../sql_parser.zig").Pos;
const Resource = @import("../sql_parser.zig").Resource;
const ast = @import("../ast.zig");
const binOpText = @import("../sql_parser.zig").binOpText;
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const quote = @import("../quote.zig");
const std = @import("std");

/// A script that opens with EXPLAIN explains the whole script offline, without
/// binding params; EXPLAIN anywhere else is `parseExplainStmt`.
pub fn parseProgram(self: *Parser) Error!ast.Program {
    self.conn_names = std.array_list.Managed([]const u8).init(self.arena);
    self.conn_types = std.array_list.Managed([]const u8).init(self.arena);
    for (self.known_conns) |c| {
        try self.conn_names.append(c.name);
        try self.conn_types.append(c.connector);
    }
    self.resources = std.array_list.Managed(Resource).init(self.arena);
    self.table_fns = std.array_list.Managed(ast.FnDecl).init(self.arena);
    try self.table_fns.appendSlice(self.known_fns);
    self.let_names = std.array_list.Managed([]const u8).init(self.arena);
    self.pending_bindings = std.array_list.Managed(ast.Stmt).init(self.arena);
    self.pending_semijoins = std.array_list.Managed(PendingSemiJoin).init(self.arena);

    var explain: ast.ExplainMode = .none;
    if (self.eatKw("explain")) explain = try explainMode(self);

    var stmts = std.array_list.Managed(ast.Stmt).init(self.arena);
    while (!self.at(.eof)) {
        const start = self.i;
        const n0 = stmts.items.len;
        self.parseStatement(&stmts) catch |e| {
            const errs = self.errors orelse return e;
            if (e == error.OutOfMemory) return e;
            try errs.append(self.diag.*);
            stmts.shrinkRetainingCapacity(n0);
            self.pending_bindings.clearRetainingCapacity();
            if (self.opensBlock(start)) break;
            while (!self.at(.eof) and !self.at(.semi)) _ = self.advance();
            _ = self.eat(.semi);
            continue;
        };
    }
    if (stmts.items.len == 0) {
        const recovered = if (self.errors) |errs| errs.items.len > 0 else false;
        if (!recovered) return self.fail(self.curPos(), "empty program: expected at least one statement", .{});
    }

    try stmts.insert(0, .{ .kind = .{ .pos = .{ .line = 1, .col = 1 } } });
    return .{ .stmts = try stmts.toOwnedSlice(), .explain = explain };
}

/// Whether the statement at token `i` has a body with `;`s of its own (a
/// connection or resource is one statement).
pub fn opensBlock(self: *Parser, i: usize) bool {
    if (i >= self.toks.len) return false;
    const t = self.toks[i];
    if (t.tag != .ident) return false;
    if (eqlNoCase(t.text, "for") or eqlNoCase(t.text, "case")) return true;
    if (!eqlNoCase(t.text, "create")) return false;
    for (self.toks[i + 1 .. @min(self.toks.len, i + 4)]) |n| {
        if (n.tag == .ident and eqlNoCase(n.text, "function")) return true;
    }
    return false;
}

/// One top-level or arm-body statement, appended to `out` after any bindings a
/// query LET's own subqueries create. Blocks (CASE, FOR EACH, a function body)
/// nesting past `max_stmt_depth` are refused, so a crafted script fails, not the stack.
pub fn parseStatement(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    if (self.stmt_depth >= Parser.max_stmt_depth)
        return self.fail(self.curPos(), "statements nest more than {d} levels deep", .{Parser.max_stmt_depth});
    self.stmt_depth += 1;
    defer self.stmt_depth -= 1;
    if (self.isKw("create")) return self.parseCreate(out);
    if (self.isKw("param")) return out.append(.{ .param = try self.parseParam() });
    if (self.isKw("let")) {
        const l = try self.parseLetStmt();
        if (self.pending_bindings.items.len > 0) {
            try out.appendSlice(self.pending_bindings.items);
            self.pending_bindings.clearRetainingCapacity();
        }
        return out.append(.{ .let_const = l });
    }
    if (self.isKw("print")) return out.append(.{ .print = try self.parsePrintStmt() });
    if (self.isKw("load")) return self.parseLoadInto(out);
    if (self.isKw("for")) {
        var pre = std.array_list.Managed(ast.Stmt).init(self.arena);
        const fe = try self.parseForEach(&pre);
        try out.appendSlice(pre.items);
        return out.append(.{ .for_each = fe });
    }
    if (self.isKw("case")) return out.append(.{ .match = try self.parseCaseStmt() });
    if (self.isKw("throw")) return out.append(.{ .throw = try self.parseThrowStmt() });
    if (self.isKw("call")) return out.append(.{ .call = try self.parseCallStmt() });
    if (self.isKw("explain")) return self.parseExplainStmt(out);
    if (self.isKw("describe") or self.isKw("desc")) return self.parseDescribe(out);
    if (self.isKw("show")) return self.parseShow(out);
    if (self.isKw("with") or self.isKw("select")) return self.parseTerminalQuery(out);
    return self.fail(self.curPos(), "expected a statement (CREATE / PARAM / LET / PRINT / THROW / EXPLAIN / DESCRIBE / SHOW / LOAD INTO / SELECT / FOR / CASE / CALL), found {s}", .{self.curTag().describe()});
}

/// `DESCRIBE <source|query>;`, lowered to a describe-mode EXPLAIN over `read | write
/// stdout`, so everything that walks an EXPLAIN carries it. A table read asks for no rows.
pub fn parseDescribe(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    const pos = self.curPos();
    _ = self.advance();
    var stages = std.array_list.Managed(ast.Stage).init(self.arena);
    if (self.isKw("select") or self.isKw("with")) {
        self.top_names = .show;
        try self.parseQuery(out, &stages);
    } else {
        var aliases = AliasSet{};
        var hints = std.array_list.Managed(ast.Hint).init(self.arena);
        var node = try self.parseFromSource(&aliases, &hints);
        if (self.isKw("with") and self.peekTag() == .lparen) {
            _ = self.advance();
            try self.parseWithHints(&hints);
        }
        if (node == .read and (node.read.form == .table or node.read.form == .query)) node.read.where = "1 = 0";
        try stages.append(.{ .node = node, .hints = try hints.toOwnedSlice(), .pos = pos });
    }
    try stages.append(.{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = pos });
    _ = try self.expect(.semi);
    try out.append(.{ .explain = .{ .mode = .describe, .pipeline = .{ .stages = try stages.toOwnedSlice(), .pos = pos }, .pos = pos } });
}

/// `SHOW TABLES FROM conn[.schema] [LIKE p]` as a `conn.QUERY(...)` on information_schema,
/// spelled in upper case: a binary-collation SQL Server database resolves only
/// `INFORMATION_SCHEMA.TABLES` and `TABLE_NAME`, and postgres and mysql accept it.
pub fn parseShow(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    const pos = self.curPos();
    try self.expectKw("show");
    try self.expectKw("tables");
    try self.expectKw("from");
    const conn = try self.expectIdent();
    const kind = self.connType(conn) orelse
        return self.fail(pos, "SHOW TABLES FROM: `{s}` is not a connection", .{conn});
    var schema: ?[]const u8 = null;
    if (self.eat(.dot)) schema = try self.expectIdent();
    var like: ?[]const u8 = null;
    if (self.eatKw("like")) like = (try self.expect(.string)).text;
    _ = try self.expect(.semi);
    if (std.mem.eql(u8, kind, "http")) {
        if (schema != null) return self.fail(pos, "SHOW TABLES FROM: `{s}` is an http connection, which has no schemas", .{conn});
        var p = try self.showResources(conn, like, pos);
        p.show = true;
        return out.append(.{ .output = p });
    }

    var q = std.array_list.Managed(u8).init(self.arena);
    try q.appendSlice("SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, TABLE_TYPE AS table_type FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')");
    if (schema) |sc| {
        try q.writer().print(" AND TABLE_SCHEMA = '{s}'", .{sc});
    } else {
        try q.appendSlice(" AND TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog', 'mysql', 'performance_schema', 'sys', '_statistics_')");
    }
    if (like) |pat| try q.writer().print(" AND TABLE_NAME LIKE '{s}'", .{pat});
    try q.appendSlice(" ORDER BY 1, 2");

    const stages = try self.arena.alloc(ast.Stage, 2);
    stages[0] = .{ .node = .{ .read = .{ .connector = conn, .form = .{ .query = try q.toOwnedSlice() } } }, .hints = &.{}, .pos = pos };
    stages[1] = .{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = pos };
    try out.append(.{ .output = .{ .stages = stages, .pos = pos, .show = true } });
}

pub fn connType(self: *Parser, name: []const u8) ?[]const u8 {
    for (self.conn_names.items, self.conn_types.items) |n, t| {
        if (std.ascii.eqlIgnoreCase(n, name)) return t;
    }
    return null;
}

/// What follows `EXPLAIN`: `ANALYZE`, or the plan alone.
fn explainMode(self: *Parser) Error!ast.ExplainMode {
    if (self.isKw("costs"))
        return self.fail(self.curPos(), "EXPLAIN COSTS is not supported: basalt has no cost model", .{});
    return if (self.eatKw("analyze")) .analyze else .plan;
}

/// `EXPLAIN [ANALYZE] <query>;` in statement position, explained against the
/// declarations above it. A `WITH`'s CTEs stay ordinary bindings.
pub fn parseExplainStmt(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    const pos = self.curPos();
    try self.expectKw("explain");
    const mode = try explainMode(self);

    const base = out.items.len;
    if (self.isKw("load")) {
        try self.parseLoadInto(out);
    } else if (self.isKw("with") or self.isKw("select")) {
        try self.parseTerminalQuery(out);
    } else {
        return self.fail(self.curPos(), "expected SELECT, WITH or LOAD INTO after EXPLAIN, found {s}", .{self.curTag().describe()});
    }

    var i = out.items.len;
    while (i > base) {
        i -= 1;
        if (out.items[i] != .output) continue;
        const pipe = out.items[i].output;
        out.items[i] = .{ .explain = .{ .mode = mode, .pipeline = pipe, .pos = pos } };
        return;
    }
    return self.fail(pos, "EXPLAIN needs a query to explain", .{});
}

/// `LET name = <expr>;`, or `LET x = (SELECT ...);` whose single cell is evaluated at
/// run time in statement order (see `runScalarLet`).
pub fn parseLetStmt(self: *Parser) Error!ast.LetConst {
    const pos = self.curPos();
    try self.expectKw("let");
    const name = try self.expectIdent();
    _ = try self.expect(.assign);
    if (self.at(.lparen) and (self.peekKw("select") or self.peekKw("with"))) {
        _ = self.advance();
        const pipe = try self.parseSubqueryPipeline();
        _ = try self.expect(.semi);
        return .{ .name = name, .expr = null, .query = pipe, .pos = pos };
    }
    const expr = try self.parseExpr();
    _ = try self.expect(.semi);
    return .{ .name = name, .expr = expr, .pos = pos };
}

pub fn parsePrintStmt(self: *Parser) Error!ast.Print {
    const pos = self.curPos();
    try self.expectKw("print");
    const expr = try self.parseExpr();
    _ = try self.expect(.semi);
    return .{ .expr = expr, .pos = pos };
}

pub fn parseCreate(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    const pos = self.curPos();
    try self.expectKw("create");
    const replace = if (self.eatKw("or")) blk: {
        try self.expectKw("replace");
        break :blk true;
    } else false;
    if (self.eatKw("connection")) {
        const conn = try self.parseConnection(pos);
        try self.conn_names.append(conn.name);
        try self.conn_types.append(conn.connector);
        return out.append(.{ .connection = conn });
    }
    if (self.eatKw("function")) {
        return out.append(.{ .func = try self.parseFunction(pos, replace) });
    }
    if (self.eatKw("resource")) return self.parseResource(pos);
    return self.fail(self.curPos(), "expected CONNECTION, RESOURCE, or FUNCTION after CREATE", .{});
}

/// One dotted name atom: an identifier, or `IDENTIFIER(<expr>)` lowered to a `${...}`
/// template computed per row.
pub fn parseNameSegment(self: *Parser) Error![]const u8 {
    if (self.isKw("identifier") and self.peekTag() == .lparen) {
        _ = self.advance();
        _ = try self.expect(.lparen);
        const e = try self.parseExpr();
        _ = try self.expect(.rparen);
        return self.exprToTemplate(e);
    }
    return self.expectIdent();
}

pub fn parseColRef(self: *Parser) Error!ast.QualName {
    if (self.isKw("identifier") and self.peekTag() == .lparen) {
        const start = self.curPos();
        var q = try self.singleName(try self.parseNameSegment());
        q.span = self.spanFrom(start);
        return q;
    }
    return self.parseQualNameTok();
}

/// A write-target atom: a name, a quoted string interpolated as-is, or `IDENTIFIER(<expr>)`.
pub fn parseTargetSegment(self: *Parser) Error![]const u8 {
    if (self.isKw("identifier") and self.peekTag() == .lparen) {
        _ = self.advance();
        _ = try self.expect(.lparen);
        const e = try self.parseExpr();
        _ = try self.expect(.rparen);
        return self.exprToTemplate(e);
    }
    return self.expectColName();
}

/// A union's trailing clauses in either order: `PUSHDOWN(<expr>)`, a `where` hint for
/// every branch (empty means none), and `ANCHOR SCHEMA <table>`.
pub fn parseUnionClauses(self: *Parser, hints: *std.array_list.Managed(ast.Hint), pos: Pos) Error!void {
    while (true) {
        if (self.eatKw("pushdown")) {
            _ = try self.expect(.lparen);
            const outer_quals = self.qual_sink;
            self.qual_sink = null;
            const e = try self.parseExpr();
            self.qual_sink = outer_quals;
            _ = try self.expect(.rparen);
            const frag = try self.exprToTemplate(e);
            if (frag.len > 0)
                try hints.append(.{ .key = "where", .value = .{ .str = frag }, .pos = pos });
        } else if (self.eatKw("anchor")) {
            try self.expectKw("schema");
            const q = try self.parseQualNameTok();
            try hints.append(.{ .key = "canon", .value = .{ .ident = q.last() }, .pos = pos });
        } else break;
    }
}

/// Lower a reflection expression to a `${...}` template: a `concat`/`||` chain splices
/// literal text with holes, a string literal stays literal, anything else is one hole.
pub fn exprToTemplate(self: *Parser, e: *const ast.Expr) Error![]const u8 {
    var buf = std.array_list.Managed(u8).init(self.arena);
    try self.templatePart(&buf, e);
    return buf.toOwnedSlice();
}

pub fn templatePart(self: *Parser, buf: *std.array_list.Managed(u8), e0: *const ast.Expr) Error!void {
    const e = typedText(e0);
    switch (e.*) {
        .str_lit => |s| return buf.appendSlice(s),
        .int_lit => |v| return buf.writer().print("{d}", .{v}),
        .bool_lit => |b| return buf.appendSlice(if (b) "true" else "false"),
        .call => |c| if (std.mem.eql(u8, c.name, "concat")) {
            for (c.args) |arg| try self.templatePart(buf, arg);
            return;
        },
        else => {},
    }
    try buf.appendSlice("${");
    try self.unparse(buf, e);
    try buf.append('}');
}

/// A string literal a typed parameter took as a CAST (a table function's DATE
/// argument), read back as the text it was written as.
fn typedText(e: *const ast.Expr) *const ast.Expr {
    return if (e.* == .cast and e.cast.e.* == .str_lit) e.cast.e else e;
}

/// Print an expression back as `${ }` hole text; `$name` already parsed to a bare
/// field, so it prints as `name`.
pub fn unparse(self: *Parser, buf: *std.array_list.Managed(u8), e: *const ast.Expr) Error!void {
    switch (typedText(e).*) {
        .null_lit => try buf.appendSlice("null"),
        .bool_lit => |b| try buf.appendSlice(if (b) "true" else "false"),
        .int_lit => |v| try buf.writer().print("{d}", .{v}),
        .float_lit => |v| try buf.writer().print("{e}", .{v}),
        .dec_lit => |v| try buf.writer().print("{f}", .{v}),
        .str_lit => |s| try quote.strLit(buf, s),
        .field => |q| for (q.parts, 0..) |p, i| {
            if (i > 0) try buf.append('.');
            try buf.appendSlice(p);
        },
        .call => |c| {
            try buf.appendSlice(c.name);
            try buf.append('(');
            for (c.args, 0..) |arg, i| {
                if (i > 0) try buf.appendSlice(", ");
                try self.unparse(buf, arg);
            }
            try buf.append(')');
        },
        .binary => |b| {
            try buf.append('(');
            try self.unparse(buf, b.l);
            try buf.append(' ');
            try buf.appendSlice(binOpText(b.op));
            try buf.append(' ');
            try self.unparse(buf, b.r);
            try buf.append(')');
        },
        .unary => |u| {
            try buf.appendSlice(switch (u.op) {
                .not => "not ",
                .neg => "-",
                .bit_not => "~",
            });
            try self.unparse(buf, u.e);
        },
        .cond => |c| {
            try buf.appendSlice("if(");
            try self.unparse(buf, c.cond);
            try buf.appendSlice(", ");
            try self.unparse(buf, c.then);
            try buf.appendSlice(", ");
            try self.unparse(buf, c.els);
            try buf.append(')');
        },
        else => return self.fail(self.curPos(), "expression too complex to use as a dynamic identifier or predicate", .{}),
    }
}
