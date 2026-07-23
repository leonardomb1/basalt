//! Parser for the Basalt SQL dialect (migration.md). Produces the SAME
//! `ast.Program` as the BSL parser — the planner, analyzer, pushdown, and
//! executor are shared ("one plan"). Only the surface differs.
//!
//! Mapping highlights (see migration.md and examples/golden/README.md):
//!   CREATE ENDPOINT '/x'            -> KindDecl http (absent -> batch)
//!   PARAM x INT DEFAULT 7           -> Param (referenced as $x)
//!   CREATE CONNECTION c TYPE t ...  -> Connection (+ credential convention:
//!                                      user/password default to env(C_USER/C_PASS))
//!   LOAD INTO tgt USING f ... AS q  -> output Pipeline ending in a write stage
//!   terminal SELECT                 -> output Pipeline ending in `write stdout`
//!   WITH name AS (...)              -> Let binding (+ ref stage when sourced)
//!   FROM conn.tbl PUSHDOWN($$..$$)  -> read stage with a `where` hint
//!   WHERE / GROUP BY / ORDER BY ... -> filter / aggregate / sort / limit stages
//!   UNION ALL BY NAME + ANCHOR      -> union_ stage (tag col = literal-as-alias)
//!   FOR EACH ROW OF (...) AS (...)  -> ForEach (PARALLEL / ON ERROR -> hints)
//!   CASE ... THEN <stmts> END CASE  -> StmtMatch (plan-time dispatch)

const std = @import("std");
const tok = @import("token.zig");
const lex = @import("sql_lexer.zig");
const ast = @import("ast.zig");
const types = @import("types.zig");

const Token = tok.Token;
const Tag = tok.Tag;
const Pos = ast.Pos;

pub const Diagnostic = struct { msg: []const u8, line: u32, col: u32 };
pub const Error = error{ ParseFailed, OutOfMemory };

/// Tokenize and parse a whole Basalt SQL program.
pub fn parseSource(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic) Error!ast.Program {
    const toks = lex.tokenize(arena, src) catch return error.OutOfMemory;
    var p = Parser{ .arena = arena, .toks = toks, .diag = diag };
    for (toks) |t| {
        if (t.tag == .invalid) return p.fail(.{ .line = t.line, .col = t.col }, "invalid token `{s}`", .{t.text});
    }
    return p.parseProgram();
}

/// Tokenize and parse a single standalone expression (used to evaluate the
/// body of a `${ <expr> }` interpolation hole). Fails on trailing input.
pub fn parseExprStr(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic) Error!*ast.Expr {
    const toks = lex.tokenize(arena, src) catch return error.OutOfMemory;
    var p = Parser{ .arena = arena, .toks = toks, .diag = diag };
    for (toks) |t| {
        if (t.tag == .invalid) return p.fail(.{ .line = t.line, .col = t.col }, "invalid token `{s}`", .{t.text});
    }
    const e = try p.parseExpr();
    if (!p.at(.eof)) return p.fail(p.curPos(), "unexpected trailing input in expression", .{});
    return e;
}

fn eqlNoCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

/// One registered FROM/JOIN alias: references `alias.x` are rewritten to `x`.
const Alias = struct { name: []const u8 };

const MAX_ALIASES = 8;

const AliasSet = struct {
    names: [MAX_ALIASES][]const u8 = undefined,
    n: usize = 0,

    fn add(self: *AliasSet, name: []const u8) void {
        if (self.n < MAX_ALIASES) {
            self.names[self.n] = name;
            self.n += 1;
        }
    }
    fn has(self: *const AliasSet, name: []const u8) bool {
        for (self.names[0..self.n]) |a| {
            if (std.mem.eql(u8, a, name)) return true;
        }
        return false;
    }
};

/// Words that terminate an alias-free position (so `FROM t WHERE ...` doesn't
/// read WHERE as an alias).
const reserved_after_source = [_][]const u8{
    "where",    "group",  "order",   "limit",  "union", "anchor", "join",
    "inner",    "left",   "right",   "full",   "cross", "semi",   "anti",
    "on",       "pushdown", "with",  "as",     "end",   "when",   "then",
    "else",     "case",   "select",  "from",   "load",  "for",    "using",
    "upsert",   "append", "replace", "split",  "jobs",  "offset", "paginate",
    "retry",    "create", "param",   "having", "and",   "or",     "not",
};

fn isReservedAfterSource(name: []const u8) bool {
    for (reserved_after_source) |k| {
        if (eqlNoCase(name, k)) return true;
    }
    return false;
}

const agg_names = [_]struct { n: []const u8, f: ast.AggFunc }{
    .{ .n = "count", .f = .count },
    .{ .n = "sum", .f = .sum },
    .{ .n = "avg", .f = .avg },
    .{ .n = "min", .f = .min },
    .{ .n = "max", .f = .max },
};

fn aggFunc(name: []const u8) ?ast.AggFunc {
    for (agg_names) |m| {
        if (eqlNoCase(name, m.n)) return m.f;
    }
    return null;
}

pub const Parser = struct {
    arena: std.mem.Allocator,
    toks: []const Token,
    i: usize = 0,
    diag: *Diagnostic,

    endpoint: ?ast.KindDecl = null,
    conn_names: std.array_list.Managed([]const u8) = undefined,
    let_names: std.array_list.Managed([]const u8) = undefined,

    // --- cursor helpers -----------------------------------------------------

    fn cur(self: *Parser) Token {
        return self.toks[self.i];
    }
    fn curTag(self: *Parser) Tag {
        return self.toks[self.i].tag;
    }
    fn curPos(self: *Parser) Pos {
        const t = self.toks[self.i];
        return .{ .line = t.line, .col = t.col };
    }
    fn peekTag(self: *Parser) Tag {
        const j = self.i + 1;
        return if (j < self.toks.len) self.toks[j].tag else .eof;
    }
    fn peekTok(self: *Parser) Token {
        const j = self.i + 1;
        return if (j < self.toks.len) self.toks[j] else self.toks[self.toks.len - 1];
    }
    fn advance(self: *Parser) Token {
        const t = self.toks[self.i];
        if (t.tag != .eof) self.i += 1;
        return t;
    }
    fn at(self: *Parser, tag: Tag) bool {
        return self.curTag() == tag;
    }
    fn eat(self: *Parser, tag: Tag) bool {
        if (self.at(tag)) {
            _ = self.advance();
            return true;
        }
        return false;
    }
    /// Case-insensitive keyword check (SQL style).
    fn isKw(self: *Parser, kw: []const u8) bool {
        const t = self.cur();
        return t.tag == .ident and eqlNoCase(t.text, kw);
    }
    fn peekKw(self: *Parser, kw: []const u8) bool {
        const t = self.peekTok();
        return t.tag == .ident and eqlNoCase(t.text, kw);
    }
    fn eatKw(self: *Parser, kw: []const u8) bool {
        if (self.isKw(kw)) {
            _ = self.advance();
            return true;
        }
        return false;
    }
    fn expect(self: *Parser, tag: Tag) Error!Token {
        if (self.at(tag)) return self.advance();
        return self.fail(self.curPos(), "expected {s}, found {s}", .{ tag.describe(), self.curTag().describe() });
    }
    fn expectIdent(self: *Parser) Error![]const u8 {
        if (self.at(.ident)) return self.advance().text;
        return self.fail(self.curPos(), "expected identifier, found {s}", .{self.curTag().describe()});
    }
    fn expectKw(self: *Parser, kw: []const u8) Error!void {
        if (self.eatKw(kw)) return;
        return self.fail(self.curPos(), "expected `{s}`, found {s}", .{ kw, self.curTag().describe() });
    }
    /// A column name: identifier or quoted string (so '${var}' can build it).
    fn expectColName(self: *Parser) Error![]const u8 {
        if (self.at(.ident) or self.at(.string)) return self.advance().text;
        return self.fail(self.curPos(), "expected a column name, found {s}", .{self.curTag().describe()});
    }

    fn fail(self: *Parser, pos: Pos, comptime fmt: []const u8, args: anytype) Error {
        self.diag.* = .{
            .msg = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory formatting diagnostic",
            .line = pos.line,
            .col = pos.col,
        };
        return error.ParseFailed;
    }

    fn mk(self: *Parser, e: ast.Expr) Error!*ast.Expr {
        const p = try self.arena.create(ast.Expr);
        p.* = e;
        return p;
    }

    fn isConn(self: *Parser, name: []const u8) bool {
        for (self.conn_names.items) |c| {
            if (std.mem.eql(u8, c, name)) return true;
        }
        return false;
    }
    fn isLet(self: *Parser, name: []const u8) bool {
        for (self.let_names.items) |c| {
            if (std.mem.eql(u8, c, name)) return true;
        }
        return false;
    }

    // --- program ------------------------------------------------------------

    pub fn parseProgram(self: *Parser) Error!ast.Program {
        self.conn_names = std.array_list.Managed([]const u8).init(self.arena);
        self.let_names = std.array_list.Managed([]const u8).init(self.arena);

        var stmts = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof)) {
            try self.parseStatement(&stmts);
        }
        if (stmts.items.len == 0)
            return self.fail(self.curPos(), "empty program: expected at least one statement", .{});

        // The runtime expects the @kind declaration first.
        const kind: ast.KindDecl = self.endpoint orelse
            .{ .kind = .batch, .config = &.{}, .pos = .{ .line = 1, .col = 1 } };
        try stmts.insert(0, .{ .kind = kind });
        return .{ .stmts = try stmts.toOwnedSlice() };
    }

    /// One top-level (or arm-body) statement, appended to `out`.
    fn parseStatement(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        if (self.isKw("create")) return self.parseCreate(out);
        if (self.isKw("param")) return out.append(.{ .param = try self.parseParam() });
        if (self.isKw("load")) return self.parseLoadInto(out);
        if (self.isKw("for")) return out.append(.{ .for_each = try self.parseForEach() });
        if (self.isKw("case")) return out.append(.{ .match = try self.parseCaseStmt() });
        if (self.isKw("with") or self.isKw("select")) return self.parseTerminalQuery(out);
        return self.fail(self.curPos(), "expected a statement (CREATE / PARAM / LOAD INTO / SELECT / FOR / CASE), found {s}", .{self.curTag().describe()});
    }

    // --- CREATE -------------------------------------------------------------

    fn parseCreate(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        try self.expectKw("create");
        if (self.eatKw("or")) try self.expectKw("replace");
        if (self.eatKw("endpoint")) {
            const path = try self.expect(.string);
            var attrs = std.array_list.Managed(ast.Attr).init(self.arena);
            try attrs.append(.{ .key = "path", .value = try self.mk(.{ .str_lit = path.text }), .pos = pos });
            if (self.eatKw("doc")) {
                const doc = try self.expect(.string);
                try attrs.append(.{ .key = "doc", .value = try self.mk(.{ .str_lit = doc.text }), .pos = pos });
            }
            _ = try self.expect(.semi);
            if (self.endpoint != null)
                return self.fail(pos, "duplicate CREATE ENDPOINT", .{});
            self.endpoint = .{ .kind = .http, .config = try attrs.toOwnedSlice(), .pos = pos };
            return;
        }
        if (self.eatKw("connection")) {
            const conn = try self.parseConnection(pos);
            try self.conn_names.append(conn.name);
            return out.append(.{ .connection = conn });
        }
        if (self.eatKw("function")) {
            return out.append(.{ .func = try self.parseFunction(pos) });
        }
        return self.fail(self.curPos(), "expected ENDPOINT, CONNECTION, or FUNCTION after CREATE", .{});
    }

    fn parseConnection(self: *Parser, pos: Pos) Error!ast.Connection {
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
        _ = try self.expect(.semi);
        // Credential convention: connection `erp` resolves ERP_USER / ERP_PASS
        // from the environment unless the script names them explicitly.
        if (!has_user) try attrs.append(.{ .key = "user", .value = try self.envCall(name, "_USER"), .pos = pos });
        if (!has_pass) try attrs.append(.{ .key = "password", .value = try self.envCall(name, "_PASS"), .pos = pos });
        return .{ .name = name, .connector = connector, .config = try attrs.toOwnedSlice(), .pos = pos };
    }

    fn envCall(self: *Parser, conn_name: []const u8, suffix: []const u8) Error!*ast.Expr {
        const upper = try std.ascii.allocUpperString(self.arena, conn_name);
        const var_name = try std.fmt.allocPrint(self.arena, "{s}{s}", .{ upper, suffix });
        const arg = try self.mk(.{ .str_lit = var_name });
        const args = try self.arena.alloc(*ast.Expr, 1);
        args[0] = arg;
        return self.mk(.{ .call = .{ .name = "env", .args = args } });
    }

    fn parseFunction(self: *Parser, pos: Pos) Error!ast.FnDecl {
        const name = try self.expectIdent();
        _ = try self.expect(.lparen);
        var params = std.array_list.Managed([]const u8).init(self.arena);
        if (!self.at(.rparen)) {
            try params.append(try self.expectIdent());
            while (self.eat(.comma)) try params.append(try self.expectIdent());
        }
        _ = try self.expect(.rparen);
        try self.expectKw("as");
        const body = try self.parseExpr();
        _ = try self.expect(.semi);
        return .{ .name = name, .params = try params.toOwnedSlice(), .body = body, .pos = pos };
    }

    // --- PARAM --------------------------------------------------------------

    fn parseParam(self: *Parser) Error!ast.Param {
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
        var source: ?ast.ParamSource = null;
        if (self.eatKw("from")) {
            if (self.eatKw("query")) {
                source = .query;
            } else if (self.eatKw("body")) {
                source = .body;
            } else if (self.eatKw("header")) {
                source = .header;
                if (self.eat(.lparen)) { // FROM HEADER('X-Name') — name currently unused
                    _ = try self.expect(.string);
                    _ = try self.expect(.rparen);
                }
            } else {
                return self.fail(self.curPos(), "expected QUERY, BODY, or HEADER after FROM", .{});
            }
        }
        if (is_json and source == null) source = .body;
        _ = try self.expect(.semi);
        return .{ .name = name, .ty = ty, .default = default, .source = source, .pos = pos, .is_json = is_json };
    }

    fn parseTypeName(self: *Parser) Error!types.Type {
        const pos = self.curPos();
        const name = try self.expectIdent();
        const Map = struct { n: []const u8, k: types.TypeKind };
        const simple = [_]Map{
            .{ .n = "bool", .k = .bool },       .{ .n = "boolean", .k = .bool },
            .{ .n = "int", .k = .int },         .{ .n = "integer", .k = .int },
            .{ .n = "bigint", .k = .int },      .{ .n = "smallint", .k = .int },
            .{ .n = "tinyint", .k = .int },     .{ .n = "float", .k = .float },
            .{ .n = "real", .k = .float },      .{ .n = "double", .k = .float },
            .{ .n = "string", .k = .string },   .{ .n = "text", .k = .string },
            .{ .n = "bytes", .k = .bytes },     .{ .n = "binary", .k = .bytes },
            .{ .n = "varbinary", .k = .bytes }, .{ .n = "date", .k = .date },
            .{ .n = "time", .k = .time },       .{ .n = "timestamp", .k = .timestamp },
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
                _ = try self.expect(.comma);
                s = try self.expectU8();
                _ = try self.expect(.rparen);
            }
            return types.Type.decimal(p, s);
        }
        return self.fail(pos, "unknown type `{s}`", .{name});
    }

    fn expectU8(self: *Parser) Error!u8 {
        const t = try self.expect(.int);
        return std.fmt.parseInt(u8, t.text, 10) catch
            self.fail(.{ .line = t.line, .col = t.col }, "number out of range: {s}", .{t.text});
    }

    // --- LOAD INTO ------------------------------------------------------------

    fn parseLoadInto(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        try self.expectKw("load");
        try self.expectKw("into");

        var write: ast.Write = undefined;
        if (self.at(.string)) {
            // file target by path: LOAD INTO '/out/x.csv'
            write = .{ .connector = "csv", .form = null, .target = self.advance().text, .mode = .default };
        } else {
            const conn = try self.expectIdent();
            if (!self.isConn(conn))
                return self.fail(pos, "unknown connection `{s}` in LOAD INTO (declare it with CREATE CONNECTION first)", .{conn});
            _ = try self.expect(.dot);
            var parts = std.array_list.Managed([]const u8).init(self.arena);
            try parts.append(try self.expectColName());
            while (self.eat(.dot)) try parts.append(try self.expectColName());
            const target = try std.mem.join(self.arena, ".", parts.items);
            write = .{ .connector = conn, .form = null, .target = target, .mode = .default };
        }

        if (self.eatKw("using")) write.form = try self.expectIdent();

        var whints = std.array_list.Managed(ast.Hint).init(self.arena);
        // dispositions + load clauses, any order
        while (true) {
            if (self.eatKw("append")) {
                write.mode = .append;
            } else if (self.eatKw("replace")) {
                write.mode = .overwrite;
            } else if (self.eatKw("upsert")) {
                var keys = std.array_list.Managed([]const u8).init(self.arena);
                if (self.eatKw("on")) {
                    _ = try self.expect(.lparen);
                    try keys.append(try self.expectColName());
                    while (self.eat(.comma)) try keys.append(try self.expectColName());
                    _ = try self.expect(.rparen);
                }
                var partial: ?[]const []const u8 = null;
                if (self.eatKw("partial")) {
                    try self.expectKw("cols");
                    _ = try self.expect(.lparen);
                    var cols = std.array_list.Managed([]const u8).init(self.arena);
                    try cols.append(try self.expectColName());
                    while (self.eat(.comma)) try cols.append(try self.expectColName());
                    _ = try self.expect(.rparen);
                    partial = try cols.toOwnedSlice();
                }
                write.mode = .{ .upsert = .{ .keys = try keys.toOwnedSlice(), .partial = partial } };
            } else if (self.eatKw("split")) {
                try self.expectKw("by");
                _ = try self.expect(.lparen);
                const col = try self.expectColName();
                _ = try self.expect(.rparen);
                try whints.append(.{ .key = "split", .value = .{ .ident = col }, .pos = pos });
                if (self.eatKw("jobs")) {
                    const n = try self.expect(.int);
                    const v = std.fmt.parseInt(i64, n.text, 10) catch
                        return self.fail(pos, "bad JOBS count `{s}`", .{n.text});
                    try whints.append(.{ .key = "jobs", .value = .{ .int = v }, .pos = pos });
                }
            } else if (self.isKw("with") and self.peekTag() == .lparen) {
                _ = self.advance();
                try self.parseWithHints(&whints);
            } else break;
        }

        try self.expectKw("as");
        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        try self.parseQuery(out, &stages);
        try stages.append(.{ .node = .{ .write = write }, .hints = try whints.toOwnedSlice(), .pos = pos });
        _ = try self.expect(.semi);
        try out.append(.{ .output = .{ .stages = try stages.toOwnedSlice(), .pos = pos } });
    }

    /// A terminal SELECT: query printed to stdout.
    fn parseTerminalQuery(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        try self.parseQuery(out, &stages);
        try stages.append(.{
            .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } },
            .hints = &.{},
            .pos = pos,
        });
        _ = try self.expect(.semi);
        try out.append(.{ .output = .{ .stages = try stages.toOwnedSlice(), .pos = pos } });
    }

    /// `WITH (k = v, k, ...)` residual options -> stage hints.
    fn parseWithHints(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
        _ = try self.expect(.lparen);
        while (!self.at(.rparen)) {
            const pos = self.curPos();
            const key = try self.expectIdent();
            var val: ast.HintVal = .flag;
            if (self.eat(.assign)) {
                if (self.at(.string)) {
                    val = .{ .str = self.advance().text };
                } else if (self.at(.int)) {
                    const t = self.advance();
                    val = .{ .int = std.fmt.parseInt(i64, t.text, 10) catch
                        return self.fail(pos, "bad number `{s}`", .{t.text}) };
                } else if (self.at(.ident)) {
                    val = .{ .ident = self.advance().text };
                } else {
                    return self.fail(self.curPos(), "expected a hint value, found {s}", .{self.curTag().describe()});
                }
            }
            try hints.append(.{ .key = key, .value = val, .pos = pos });
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
    }

    // --- query -> pipeline stages ---------------------------------------------

    /// Parse `[WITH ctes] core [UNION ALL BY NAME core]* [ANCHOR SCHEMA q]
    /// [ORDER BY ...] [LIMIT n [OFFSET m]]`, appending Let stmts for CTEs to
    /// `out` and pipeline stages to `stages`.
    fn parseQuery(self: *Parser, out: *std.array_list.Managed(ast.Stmt), stages: *std.array_list.Managed(ast.Stage)) Error!void {
        if (self.isKw("with") and !(self.peekTag() == .lparen)) {
            _ = self.advance();
            while (true) {
                const lpos = self.curPos();
                const name = try self.expectIdent();
                try self.expectKw("as");
                _ = try self.expect(.lparen);
                var cte_stages = std.array_list.Managed(ast.Stage).init(self.arena);
                try self.parseQuery(out, &cte_stages);
                _ = try self.expect(.rparen);
                try self.let_names.append(name);
                try out.append(.{ .binding = .{
                    .name = name,
                    .pipeline = .{ .stages = try cte_stages.toOwnedSlice(), .pos = lpos },
                    .pos = lpos,
                } });
                if (!self.eat(.comma)) break;
            }
        }

        const first = try self.parseSelectCore();

        if (self.isKw("union")) {
            try self.parseUnionTail(first, stages);
        } else {
            try stages.appendSlice(first.stages);
        }

        if (self.eatKw("order")) {
            try self.expectKw("by");
            var keys = std.array_list.Managed(ast.SortKey).init(self.arena);
            while (true) {
                var q = try self.parseQualNameTok();
                q = stripQual(q, &first.aliases);
                var desc = false;
                if (self.eatKw("desc")) {
                    desc = true;
                } else _ = self.eatKw("asc");
                try keys.append(.{ .field = q, .desc = desc });
                if (!self.eat(.comma)) break;
            }
            try stages.append(.{ .node = .{ .sort = .{ .keys = try keys.toOwnedSlice() } }, .hints = &.{}, .pos = self.curPos() });
        }

        if (self.eatKw("limit")) {
            const n = try self.expect(.int);
            const count = std.fmt.parseInt(u64, n.text, 10) catch
                return self.fail(self.curPos(), "bad LIMIT `{s}`", .{n.text});
            var offset: u64 = 0;
            if (self.eatKw("offset")) {
                const m = try self.expect(.int);
                offset = std.fmt.parseInt(u64, m.text, 10) catch
                    return self.fail(self.curPos(), "bad OFFSET `{s}`", .{m.text});
            }
            try stages.append(.{ .node = .{ .limit = .{ .count = count, .offset = offset } }, .hints = &.{}, .pos = self.curPos() });
        }
    }

    /// Set when a core is exactly `SELECT ['lit' AS col,] t.* FROM conn.table`
    /// — the only shape a UNION ALL BY NAME branch may take.
    const BranchInfo = struct { read: ast.Read, tag: ?[]const u8, tag_col: ?[]const u8 };

    const Core = struct {
        stages: []const ast.Stage,
        aliases: AliasSet,
        union_branch: ?BranchInfo,
    };

    /// SELECT [DISTINCT [ON (cols)]] items FROM source [alias] [PUSHDOWN(...)]
    /// [WITH (...)] [joins] [WHERE e] [GROUP BY keys]
    fn parseSelectCore(self: *Parser) Error!Core {
        const pos = self.curPos();
        try self.expectKw("select");

        var distinct = false;
        var distinct_on: ?[]const ast.QualName = null;
        if (self.eatKw("distinct")) {
            distinct = true;
            if (self.eatKw("on")) {
                _ = try self.expect(.lparen);
                var cols = std.array_list.Managed(ast.QualName).init(self.arena);
                try cols.append(try self.parseQualNameTok());
                while (self.eat(.comma)) try cols.append(try self.parseQualNameTok());
                _ = try self.expect(.rparen);
                distinct_on = try cols.toOwnedSlice();
            }
        }

        // Items are parsed before FROM, so alias qualifiers are stripped afterwards.
        const RawItem = union(enum) {
            item: ast.SelectItem,
            qstar: []const u8, // `t.*`
        };
        var raw_items = std.array_list.Managed(RawItem).init(self.arena);
        while (true) {
            if (self.eat(.star)) {
                if (self.eatKw("except") or self.eatKw("exclude")) {
                    _ = try self.expect(.lparen);
                    var names = std.array_list.Managed([]const u8).init(self.arena);
                    try names.append(try self.expectIdent());
                    while (self.eat(.comma)) try names.append(try self.expectIdent());
                    _ = try self.expect(.rparen);
                    try raw_items.append(.{ .item = .{ .star_except = try names.toOwnedSlice() } });
                } else if (self.eatKw("rename")) {
                    _ = try self.expect(.lparen);
                    var rens = std.array_list.Managed(ast.SelectItem.Rename).init(self.arena);
                    while (true) {
                        const from = try self.expectIdent();
                        try self.expectKw("as");
                        const to = try self.expectIdent();
                        try rens.append(.{ .from = from, .to = to });
                        if (!self.eat(.comma)) break;
                    }
                    _ = try self.expect(.rparen);
                    try raw_items.append(.{ .item = .{ .star_rename = try rens.toOwnedSlice() } });
                } else {
                    try raw_items.append(.{ .item = .star });
                }
            } else if (self.at(.ident) and self.peekTag() == .dot and
                self.i + 2 < self.toks.len and self.toks[self.i + 2].tag == .star)
            {
                const alias = self.advance().text;
                _ = self.advance(); // .
                _ = self.advance(); // *
                try raw_items.append(.{ .qstar = alias });
            } else {
                const e = try self.parseExpr();
                var name: ?[]const u8 = null;
                if (self.eatKw("as")) name = try self.expectColName();
                if (name) |n| {
                    try raw_items.append(.{ .item = .{ .computed = .{ .name = n, .expr = e } } });
                } else if (e.* == .field) {
                    try raw_items.append(.{ .item = .{ .field = e.field } });
                } else {
                    return self.fail(self.curPos(), "a computed SELECT item needs an alias (`expr AS name`)", .{});
                }
            }
            if (!self.eat(.comma)) break;
        }

        try self.expectKw("from");
        var aliases = AliasSet{};
        var read_hints = std.array_list.Managed(ast.Hint).init(self.arena);
        const src = try self.parseFromSource(&aliases, &read_hints);

        // Source clauses, any order: PUSHDOWN($$...$$), PAGINATE BY, RETRY, WITH (...).
        while (true) {
            if (self.eatKw("pushdown")) {
                _ = try self.expect(.lparen);
                const frag = try self.expect(.string);
                _ = try self.expect(.rparen);
                if (frag.text.len > 0)
                    try read_hints.append(.{ .key = "where", .value = .{ .str = frag.text }, .pos = pos });
            } else if (self.isKw("paginate")) {
                try self.parsePaginate(&read_hints);
            } else if (self.isKw("retry")) {
                try self.parseRetry(&read_hints);
            } else if (self.isKw("with") and self.peekTag() == .lparen) {
                _ = self.advance();
                try self.parseWithHints(&read_hints);
            } else break;
        }

        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        try stages.append(.{ .node = src, .hints = try read_hints.toOwnedSlice(), .pos = pos });

        // joins
        while (true) {
            const jk: ?ast.JoinKind = blk: {
                if (self.isKw("inner")) break :blk .inner;
                if (self.isKw("left")) break :blk .left;
                if (self.isKw("right")) break :blk .right;
                if (self.isKw("full")) break :blk .full;
                if (self.isKw("cross")) break :blk .cross;
                if (self.isKw("semi")) break :blk .semi;
                if (self.isKw("anti")) break :blk .anti;
                if (self.isKw("join")) break :blk .inner;
                break :blk null;
            };
            if (jk == null) break;
            const kind = jk.?;
            if (!self.isKw("join")) _ = self.advance(); // the kind word
            _ = self.eatKw("outer");
            try self.expectKw("join");
            const jpos = self.curPos();
            // CROSS JOIN UNNEST(SPLIT(col, ',')) AS name -> explode stage
            if (self.isKw("unnest")) {
                if (kind != .cross)
                    return self.fail(jpos, "UNNEST requires CROSS JOIN", .{});
                _ = self.advance();
                _ = try self.expect(.lparen);
                var field: []const u8 = undefined;
                var delim: ?[]const u8 = null;
                if (self.isKw("split") and self.peekTag() == .lparen) {
                    _ = self.advance();
                    _ = try self.expect(.lparen);
                    field = try self.expectIdent();
                    _ = try self.expect(.comma);
                    delim = (try self.expect(.string)).text;
                    _ = try self.expect(.rparen);
                } else {
                    field = try self.expectIdent();
                }
                _ = try self.expect(.rparen);
                var as_name: ?[]const u8 = null;
                if (self.eatKw("as")) as_name = try self.expectIdent();
                try stages.append(.{
                    .node = .{ .explode = .{ .field = field, .as_name = as_name, .delim = delim } },
                    .hints = &.{},
                    .pos = jpos,
                });
                continue;
            }
            const binding = try self.expectIdent();
            if (!self.isLet(binding))
                return self.fail(jpos, "JOIN right side `{s}` must be a WITH-defined CTE", .{binding});
            var jalias: ?[]const u8 = null;
            if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
                jalias = self.advance().text;
                aliases.add(jalias.?);
            }
            var left_key: ast.QualName = undefined;
            var right_key: ast.QualName = undefined;
            if (kind != .cross) {
                try self.expectKw("on");
                const a = try self.parseQualNameTok();
                if (!(self.eat(.assign) or self.eat(.eq)))
                    return self.fail(self.curPos(), "expected `=` in join condition, found {s}", .{self.curTag().describe()});
                const b = try self.parseQualNameTok();
                const a_right = qualHasPrefix(a, jalias orelse binding);
                const l = if (a_right) b else a;
                const r = if (a_right) a else b;
                left_key = stripQual(l, &aliases);
                right_key = stripPrefix(r, jalias orelse binding);
            } else {
                left_key = .{ .parts = &.{} };
                right_key = .{ .parts = &.{} };
            }
            try stages.append(.{
                .node = .{ .join = .{ .kind = kind, .binding = binding, .left_key = left_key, .right_key = right_key } },
                .hints = &.{},
                .pos = jpos,
            });
        }

        // WHERE -> filter
        if (self.eatKw("where")) {
            const fpos = self.curPos();
            var e = try self.parseExpr();
            e = try self.stripExpr(e, &aliases);
            try stages.append(.{ .node = .{ .filter = e }, .hints = &.{}, .pos = fpos });
        }

        // GROUP BY
        var group: []const ast.QualName = &.{};
        if (self.eatKw("group")) {
            try self.expectKw("by");
            var keys = std.array_list.Managed(ast.QualName).init(self.arena);
            while (true) {
                var q = try self.parseQualNameTok();
                q = stripQual(q, &aliases);
                try keys.append(q);
                if (!self.eat(.comma)) break;
            }
            group = try keys.toOwnedSlice();
        }

        // Resolve items: strip aliases; split into aggregate vs plain select.
        var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var aggs = std.array_list.Managed(ast.AggItem).init(self.arena);
        var union_tag: ?[]const u8 = null;
        var union_tag_col: ?[]const u8 = null;
        var star_count: usize = 0;
        for (raw_items.items) |ri| {
            switch (ri) {
                .qstar => |alias| {
                    if (!aliases.has(alias))
                        return self.fail(pos, "unknown alias `{s}` in `{s}.*`", .{ alias, alias });
                    try items.append(.star);
                    star_count += 1;
                },
                .item => |it| switch (it) {
                    .star => {
                        try items.append(.star);
                        star_count += 1;
                    },
                    .star_except, .star_rename => try items.append(it),
                    .field => |q| try items.append(.{ .field = stripQual(q, &aliases) }),
                    .computed => |c| {
                        const stripped = try self.stripExpr(c.expr, &aliases);
                        if (stripped.* == .call) {
                            if (aggFunc(stripped.call.name)) |f| {
                                const arg: ?*ast.Expr = if (stripped.call.args.len > 0) stripped.call.args[0] else null;
                                try aggs.append(.{ .name = c.name, .func = f, .arg = arg });
                                continue;
                            }
                        }
                        if (stripped.* == .str_lit and union_tag == null) {
                            union_tag = stripped.str_lit;
                            union_tag_col = c.name;
                        }
                        try items.append(.{ .computed = .{ .name = c.name, .expr = stripped } });
                    },
                },
            }
        }

        if (aggs.items.len > 0 or group.len > 0) {
            if (aggs.items.len == 0)
                return self.fail(pos, "GROUP BY without aggregate functions in SELECT", .{});
            // A group key naming a computed alias (`SELECT CAST(n AS INT) AS g ...
            // GROUP BY g`) needs the computation before the aggregate: emit the
            // non-agg items as a projection stage first. Agg args must then
            // reference projected columns. With plain-field keys only, aggregate
            // directly (args may be arbitrary expressions over source columns).
            var needs_pre = false;
            for (group) |k| {
                for (items.items) |it| {
                    if (it == .computed and k.parts.len == 1 and std.mem.eql(u8, it.computed.name, k.parts[0]))
                        needs_pre = true;
                }
            }
            if (needs_pre) {
                for (items.items) |it| {
                    if (it == .star or it == .star_except or it == .star_rename)
                        return self.fail(pos, "`*` cannot be combined with a computed GROUP BY key", .{});
                }
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            } else {
                for (items.items) |it| {
                    if (it != .field)
                        return self.fail(pos, "non-aggregate SELECT items in a GROUP BY query must be plain group-key columns (or aliases the GROUP BY names)", .{});
                }
            }
            try stages.append(.{
                .node = .{ .aggregate = .{ .aggs = try aggs.toOwnedSlice(), .by = group } },
                .hints = &.{},
                .pos = pos,
            });
        } else {
            // A lone `*` projects nothing new — omit the stage (matches BSL).
            const lone_star = items.items.len == 1 and items.items[0] == .star;
            if (!lone_star) {
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
        }

        if (distinct) {
            try stages.append(.{ .node = .{ .distinct = .{ .on = distinct_on } }, .hints = &.{}, .pos = pos });
        }

        // Union-branch shape: read + optional (tag computed + star), nothing else.
        var union_branch: ?BranchInfo = null;
        const s = stages.items;
        if (s.len <= 2 and s[0].node == .read) {
            const shape_ok = s.len == 1 or
                (s[1].node == .select and star_count == 1 and s[1].node.select.len <= 2);
            if (shape_ok) {
                union_branch = .{ .read = s[0].node.read, .tag = union_tag, .tag_col = union_tag_col };
                if (s.len == 2 and union_tag == null and s[1].node.select.len == 2)
                    union_branch = null; // computed col that isn't a literal tag
            }
        }

        return .{ .stages = try stages.toOwnedSlice(), .aliases = aliases, .union_branch = union_branch };
    }

    /// `UNION ALL BY NAME core... [ANCHOR SCHEMA qual]` — collapse the first core
    /// and every following core into one union_ stage.
    fn parseUnionTail(self: *Parser, first: Core, stages: *std.array_list.Managed(ast.Stage)) Error!void {
        const pos = self.curPos();
        var branches = std.array_list.Managed(ast.UnionBranch).init(self.arena);
        var tag_col: ?[]const u8 = null;

        const fb = first.union_branch orelse
            return self.fail(pos, "a UNION ALL BY NAME branch must be `SELECT ['tag' AS col,] t.* FROM <conn>.<table>`", .{});
        try branches.append(.{ .read = fb.read, .tag = fb.tag });
        tag_col = fb.tag_col;

        while (self.eatKw("union")) {
            try self.expectKw("all");
            try self.expectKw("by");
            try self.expectKw("name");
            const core = try self.parseSelectCore();
            const b = core.union_branch orelse
                return self.fail(self.curPos(), "a UNION ALL BY NAME branch must be `SELECT ['tag' AS col,] t.* FROM <conn>.<table>`", .{});
            if (b.tag_col) |tc| {
                if (tag_col == null) tag_col = tc;
                if (!std.mem.eql(u8, tag_col.?, tc))
                    return self.fail(self.curPos(), "all UNION branches must use the same tag column name (`{s}` vs `{s}`)", .{ tag_col.?, tc });
            }
            try branches.append(.{ .read = b.read, .tag = b.tag });
        }

        var hints = std.array_list.Managed(ast.Hint).init(self.arena);
        if (tag_col) |tc|
            try hints.append(.{ .key = "tag", .value = .{ .ident = tc }, .pos = pos });
        if (self.eatKw("anchor")) {
            try self.expectKw("schema");
            const q = try self.parseQualNameTok();
            try hints.append(.{ .key = "canon", .value = .{ .ident = q.last() }, .pos = pos });
        }

        try stages.append(.{
            .node = .{ .union_ = .{ .branches = try branches.toOwnedSlice(), .pos = pos } },
            .hints = try hints.toOwnedSlice(),
            .pos = pos,
        });
    }

    /// A FROM source: CSV path, BODY(schema), HTTP('url'), a CTE reference, or
    /// a connection-qualified table / QUERY($$...$$). Registers the alias.
    fn parseFromSource(self: *Parser, aliases: *AliasSet, read_hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
        var node: ast.Stage.Node = undefined;
        if (self.at(.string)) {
            node = .{ .read = .{ .connector = "csv", .form = .{ .path = self.advance().text } } };
        } else if (self.isKw("body")) {
            _ = self.advance();
            const schema = try self.parseBodySchema();
            node = .{ .read = .{ .connector = "request", .form = .{ .request = schema } } };
        } else if (self.isKw("http") and self.peekTag() == .lparen) {
            _ = self.advance();
            _ = try self.expect(.lparen);
            const url = try self.expect(.string);
            _ = try self.expect(.rparen);
            node = .{ .read = .{ .connector = "http", .form = .{ .path = url.text } } };
        } else if (self.isKw("each")) {
            return self.parseEachTableOf(read_hints);
        } else {
            const pos = self.curPos();
            const head = try self.expectIdent();
            if (self.at(.dot)) {
                // A dotted head is always a connection-qualified source (CTEs are
                // single names). Whether the connection exists is the analyzer's
                // diagnostic, not the parser's — matching `basalt check` behavior.
                _ = self.advance(); // .
                // conn.QUERY($$...$$) | conn.'/rest/path' | conn.schema.table
                if (self.isKw("query") and self.peekTag() == .lparen) {
                    _ = self.advance();
                    _ = try self.expect(.lparen);
                    const q = try self.expect(.string);
                    _ = try self.expect(.rparen);
                    node = .{ .read = .{ .connector = head, .form = .{ .query = q.text } } };
                } else if (self.at(.string)) {
                    // REST path relative to an http connection's base URL.
                    node = .{ .read = .{ .connector = head, .form = .{ .path = self.advance().text } } };
                } else {
                    var parts = std.array_list.Managed([]const u8).init(self.arena);
                    try parts.append(try self.expectIdent());
                    while (self.at(.dot) and self.peekTag() == .ident) {
                        _ = self.advance();
                        try parts.append(try self.expectIdent());
                    }
                    node = .{ .read = .{ .connector = head, .form = .{ .table = .{ .parts = try parts.toOwnedSlice() } } } };
                }
            } else if (self.isLet(head)) {
                node = .{ .ref = head };
            } else {
                return self.fail(pos, "unknown source `{s}`: not a CTE, connection, or path", .{head});
            }
        }
        if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
            aliases.add(self.advance().text);
        }
        return node;
    }

    /// `EACH TABLE OF (<conn>.QUERY($$...$$) | $param.path | '<json>' IN <conn>)
    ///  [AS (table_name, <tag_col>)] [ANCHOR SCHEMA qual]` — the discovered /
    /// json union forms. One row (or array element) per branch; the second AS
    /// name is the output tag column.
    fn parseEachTableOf(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
        const pos = self.curPos();
        try self.expectKw("each");
        try self.expectKw("table");
        try self.expectKw("of");
        _ = try self.expect(.lparen);

        var u = ast.Union{ .pos = pos };
        if (self.at(.dollar_ident)) {
            // JSON array from a param path: rendered by the interpolation pass.
            const q = try self.parseDollarPath();
            const joined = try std.mem.join(self.arena, ".", q.parts);
            u.discover_json = try std.fmt.allocPrint(self.arena, "${{{s}}}", .{joined});
        } else if (self.at(.string)) {
            u.discover_json = self.advance().text;
        } else {
            const conn = try self.expectIdent();
            if (!self.isConn(conn))
                return self.fail(pos, "unknown connection `{s}` in EACH TABLE OF", .{conn});
            _ = try self.expect(.dot);
            try self.expectKw("query");
            _ = try self.expect(.lparen);
            const q = try self.expect(.string);
            _ = try self.expect(.rparen);
            u.discover_conn = conn;
            u.discover_query = q.text;
        }
        _ = try self.expect(.rparen);

        if (u.discover_json.len > 0) {
            try self.expectKw("in");
            const conn = try self.expectIdent();
            if (!self.isConn(conn))
                return self.fail(pos, "unknown connection `{s}` in EACH TABLE OF ... IN", .{conn});
            u.discover_conn = conn;
        }

        if (self.eatKw("as")) {
            _ = try self.expect(.lparen);
            _ = try self.expectIdent(); // positional: the table-name column
            if (self.eat(.comma)) {
                const tag_col = try self.expectIdent();
                try hints.append(.{ .key = "tag", .value = .{ .ident = tag_col }, .pos = pos });
            }
            _ = try self.expect(.rparen);
        }
        if (self.eatKw("anchor")) {
            try self.expectKw("schema");
            const q = try self.parseQualNameTok();
            try hints.append(.{ .key = "canon", .value = .{ .ident = q.last() }, .pos = pos });
        }
        return .{ .union_ = u };
    }

    /// `PAGINATE BY page|offset|cursor (key = value, ...)` -> HTTP source hints.
    fn parsePaginate(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
        const pos = self.curPos();
        try self.expectKw("paginate");
        try self.expectKw("by");
        const mode = try self.expectIdent();
        const is_cursor = eqlNoCase(mode, "cursor");
        if (eqlNoCase(mode, "page")) {
            try hints.append(.{ .key = "page", .value = .flag, .pos = pos });
        } else if (eqlNoCase(mode, "offset")) {
            try hints.append(.{ .key = "offset", .value = .flag, .pos = pos });
        } else if (is_cursor) {
            try hints.append(.{ .key = "cursor", .value = .flag, .pos = pos });
        } else {
            return self.fail(pos, "PAGINATE BY expects page, offset, or cursor (got `{s}`)", .{mode});
        }
        if (self.eat(.lparen)) {
            while (!self.at(.rparen)) {
                const kpos = self.curPos();
                const key = try self.expectIdent();
                _ = try self.expect(.assign);
                // Friendly clause keys -> engine hint names.
                const hint_key = if (eqlNoCase(key, "param"))
                    (if (is_cursor) "cursor_param" else "page_param")
                else if (eqlNoCase(key, "size"))
                    "page_size"
                else if (eqlNoCase(key, "total"))
                    "total_field"
                else if (eqlNoCase(key, "field"))
                    "cursor_field"
                else if (eqlNoCase(key, "start"))
                    "start_page"
                else if (eqlNoCase(key, "max"))
                    "max_pages"
                else
                    key; // pass through (size_param, ...)
                if (self.at(.string)) {
                    try hints.append(.{ .key = hint_key, .value = .{ .str = self.advance().text }, .pos = kpos });
                } else if (self.at(.int)) {
                    const t = self.advance();
                    const v = std.fmt.parseInt(i64, t.text, 10) catch
                        return self.fail(kpos, "bad number `{s}`", .{t.text});
                    try hints.append(.{ .key = hint_key, .value = .{ .int = v }, .pos = kpos });
                } else {
                    return self.fail(self.curPos(), "expected a string or number, found {s}", .{self.curTag().describe()});
                }
                if (!self.eat(.comma)) break;
            }
            _ = try self.expect(.rparen);
        }
    }

    /// `RETRY n [ON (429, 503)]` -> retries / retry_statuses hints.
    fn parseRetry(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
        const pos = self.curPos();
        try self.expectKw("retry");
        const n = try self.expect(.int);
        const v = std.fmt.parseInt(i64, n.text, 10) catch
            return self.fail(pos, "bad RETRY count `{s}`", .{n.text});
        try hints.append(.{ .key = "retries", .value = .{ .int = v }, .pos = pos });
        if (self.eatKw("on")) {
            _ = try self.expect(.lparen);
            var codes = std.array_list.Managed(u8).init(self.arena);
            while (true) {
                const t = try self.expect(.int);
                if (codes.items.len > 0) try codes.append(',');
                try codes.appendSlice(t.text);
                if (!self.eat(.comma)) break;
            }
            _ = try self.expect(.rparen);
            try hints.append(.{ .key = "retry_statuses", .value = .{ .str = try codes.toOwnedSlice() }, .pos = pos });
        }
    }

    /// `BODY (col TYPE [NOT NULL], ...)` — the declared request-body schema,
    /// enforced row-by-row at bind time (a violation is the endpoint's 422).
    fn parseBodySchema(self: *Parser) Error![]const types.BodyCol {
        _ = try self.expect(.lparen);
        var cols = std.array_list.Managed(types.BodyCol).init(self.arena);
        while (!self.at(.rparen)) {
            const name = try self.expectIdent();
            // JSON columns ride as text (navigate/CAST downstream).
            const ty = if (self.isKw("json")) blk: {
                _ = self.advance();
                break :blk types.Type.init(.string);
            } else try self.parseTypeName();
            var not_null = false;
            if (self.eatKw("not")) {
                try self.expectKw("null");
                not_null = true;
            }
            try cols.append(.{ .name = name, .ty = ty, .not_null = not_null });
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
        return cols.toOwnedSlice();
    }

    // --- FOR EACH ROW OF -------------------------------------------------------

    fn parseForEach(self: *Parser) Error!ast.ForEach {
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
            // CSV discovery list: one loop row per CSV row.
            source = .{ .read = .{ .connector = "csv", .form = .{ .path = self.advance().text } } };
        } else if (self.at(.ident)) {
            const conn = try self.expectIdent();
            if (!self.isConn(conn))
                return self.fail(pos, "FOR EACH ROW OF: expected `$param.path` or `<conn>.QUERY($$...$$)`", .{});
            _ = try self.expect(.dot);
            try self.expectKw("query");
            _ = try self.expect(.lparen);
            const q = try self.expect(.string);
            _ = try self.expect(.rparen);
            source = .{ .read = .{ .connector = conn, .form = .{ .query = q.text } } };
        } else {
            return self.fail(self.curPos(), "FOR EACH ROW OF: expected `$param.path` or `<conn>.QUERY($$...$$)`", .{});
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

        var body = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof) and !self.isKw("end")) {
            try self.parseStatement(&body);
        }
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

    fn parseDollarPath(self: *Parser) Error!ast.QualName {
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
        };
    }

    // --- CASE statement ---------------------------------------------------------

    fn parseCaseStmt(self: *Parser) Error!ast.StmtMatch {
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

    // --- qualified names / alias stripping ---------------------------------------

    fn parseQualNameTok(self: *Parser) Error!ast.QualName {
        var parts = std.array_list.Managed([]const u8).init(self.arena);
        try parts.append(try self.expectColName());
        while (self.at(.dot) and (self.peekTag() == .ident or self.peekTag() == .string)) {
            _ = self.advance();
            try parts.append(try self.expectColName());
        }
        return .{ .parts = try parts.toOwnedSlice() };
    }

    /// Rewrite `alias.x` -> `x` in an expression tree.
    fn stripExpr(self: *Parser, e: *ast.Expr, aliases: *const AliasSet) Error!*ast.Expr {
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

    // --- expressions (Pratt) ------------------------------------------------------

    fn parseExpr(self: *Parser) Error!*ast.Expr {
        return self.parseBin(0);
    }

    const BinInfo = struct { op: ast.BinOp, lbp: u8 };

    fn binInfo(self: *Parser) ?BinInfo {
        // precedence: or(10) < and(20) < ??(30) < cmp(40) < add(50) < mul(60)
        switch (self.curTag()) {
            .eq, .assign => return .{ .op = .eq, .lbp = 40 },
            .ne => return .{ .op = .ne, .lbp = 40 },
            .lt => return .{ .op = .lt, .lbp = 40 },
            .le => return .{ .op = .le, .lbp = 40 },
            .gt => return .{ .op = .gt, .lbp = 40 },
            .ge => return .{ .op = .ge, .lbp = 40 },
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

    fn parseBin(self: *Parser, min_bp: u8) Error!*ast.Expr {
        var lhs = try self.parseUnary();
        while (true) {
            // postfix: IS [NOT] NULL / EMPTY, LIKE, IN — comparison strength (40)
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
                _ = self.advance(); // in
                _ = try self.expect(.lparen);
                var alt: ?*ast.Expr = null;
                while (true) {
                    const v = try self.parseExpr();
                    const cmp = try self.mk(.{ .binary = .{ .op = .eq, .l = lhs, .r = v } });
                    alt = if (alt) |acc| try self.mk(.{ .binary = .{ .op = .@"or", .l = acc, .r = cmp } }) else cmp;
                    if (!self.eat(.comma)) break;
                }
                _ = try self.expect(.rparen);
                lhs = if (negated) try self.mk(.{ .unary = .{ .op = .not, .e = alt.? } }) else alt.?;
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
            const info = self.binInfo() orelse break;
            if (info.lbp <= min_bp) break;
            _ = self.advance();
            const rhs = try self.parseBin(info.lbp);
            lhs = try self.mk(.{ .binary = .{ .op = info.op, .l = lhs, .r = rhs } });
        }
        return lhs;
    }

    fn parseUnary(self: *Parser) Error!*ast.Expr {
        if (self.eat(.minus)) {
            const e = try self.parseUnary();
            return self.mk(.{ .unary = .{ .op = .neg, .e = e } });
        }
        if (self.isKw("not") and !self.peekKw("like") and !self.peekKw("in")) {
            _ = self.advance();
            const e = try self.parseBin(25);
            return self.mk(.{ .unary = .{ .op = .not, .e = e } });
        }
        return self.parsePrimary();
    }

    fn parsePrimary(self: *Parser) Error!*ast.Expr {
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
                return self.mk(.{ .field = q });
            },
            .lparen => {
                _ = self.advance();
                const e = try self.parseExpr();
                _ = try self.expect(.rparen);
                return e;
            },
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
                if (eqlNoCase(t.text, "cast") and self.peekTag() == .lparen) {
                    _ = self.advance();
                    _ = self.advance();
                    const e = try self.parseExpr();
                    try self.expectKw("as");
                    const ty = try self.parseTypeName();
                    _ = try self.expect(.rparen);
                    return self.mk(.{ .cast = .{ .e = e, .ty = ty } });
                }
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
                    // Parse the value at comparison precedence so the LET's own
                    // `IN` isn't taken as the membership operator (`x IN (...)`).
                    // Parenthesize a comparison-valued binding if needed.
                    const value = try self.parseBin(40);
                    try self.expectKw("in");
                    const body = try self.parseExpr();
                    return self.mk(.{ .let_in = .{ .name = name, .value = value, .body = body } });
                }
                if (self.peekTag() == .lparen) {
                    _ = self.advance();
                    _ = self.advance();
                    var args = std.array_list.Managed(*ast.Expr).init(self.arena);
                    if (!self.at(.rparen)) {
                        if (self.at(.star) and self.peekTag() == .rparen) {
                            _ = self.advance(); // COUNT(*) — no argument
                        } else {
                            try args.append(try self.parseExpr());
                            while (self.eat(.comma)) try args.append(try self.parseExpr());
                        }
                    }
                    _ = try self.expect(.rparen);
                    const lower = try std.ascii.allocLowerString(self.arena, t.text);
                    return self.mk(.{ .call = .{ .name = lower, .args = try args.toOwnedSlice() } });
                }
                const q = try self.parseQualNameField();
                return self.mk(.{ .field = q });
            },
            else => return self.fail(self.curPos(), "expected an expression, found {s}", .{t.tag.describe()}),
        }
    }

    /// A column reference in an expression: `a`, `t.col`, `a.b.c` (with `?.`).
    fn parseQualNameField(self: *Parser) Error!ast.QualName {
        var parts = std.array_list.Managed([]const u8).init(self.arena);
        var safes = std.array_list.Managed(bool).init(self.arena);
        try parts.append(try self.expectIdent());
        while ((self.at(.dot) or self.at(.qdot)) and self.peekTag() == .ident) {
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
        };
    }

    /// CASE expression -> ast.Match (subject + `,` alternation, or guard form).
    fn parseCaseExpr(self: *Parser) Error!*ast.Expr {
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
};

// --- helpers ------------------------------------------------------------------

fn qualHasPrefix(q: ast.QualName, prefix: []const u8) bool {
    return q.parts.len > 1 and std.mem.eql(u8, q.parts[0], prefix);
}

fn stripPrefix(q: ast.QualName, prefix: []const u8) ast.QualName {
    if (qualHasPrefix(q, prefix)) return .{ .parts = q.parts[1..], .safe = if (q.safe.len > 0) q.safe[1..] else &.{} };
    return q;
}

fn stripQual(q: ast.QualName, aliases: *const AliasSet) ast.QualName {
    if (q.parts.len > 1 and aliases.has(q.parts[0]))
        return .{ .parts = q.parts[1..], .safe = if (q.safe.len > 0) q.safe[1..] else &.{} };
    return q;
}

// --- tests ----------------------------------------------------------------------

const testing = std.testing;

fn parseTest(a: std.mem.Allocator, src: []const u8) !ast.Program {
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parseSource(a, src, &diag) catch |e| {
        if (e == error.ParseFailed) std.debug.print("parse error {d}:{d}: {s}\n", .{ diag.line, diag.col, diag.msg });
        return e;
    };
}

test "sql: terminal select from csv becomes read+write stdout" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT id, amount FROM 'in.csv' WHERE status = 'paid' LIMIT 2;");
    try testing.expectEqual(@as(usize, 2), prog.stmts.len);
    try testing.expect(prog.stmts[0] == .kind);
    try testing.expectEqual(ast.Kind.batch, prog.stmts[0].kind.kind);
    const pl = prog.stmts[1].output;
    // read, filter, select, limit, write
    try testing.expectEqual(@as(usize, 5), pl.stages.len);
    try testing.expect(pl.stages[0].node == .read);
    try testing.expect(pl.stages[1].node == .filter);
    try testing.expect(pl.stages[2].node == .select);
    try testing.expect(pl.stages[3].node == .limit);
    try testing.expect(pl.stages[4].node == .write);
    try testing.expectEqualStrings("stdout", pl.stages[4].node.write.connector);
}

test "sql: LOAD INTO with GROUP BY becomes aggregate" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\SELECT status, COUNT(*) AS n, SUM(CAST(amount AS INT)) AS total
        \\FROM 'in.csv'
        \\GROUP BY status;
    );
    const pl = prog.stmts[1].output;
    try testing.expectEqual(@as(usize, 3), pl.stages.len); // read, aggregate, write
    const agg = pl.stages[1].node.aggregate;
    try testing.expectEqual(@as(usize, 2), agg.aggs.len);
    try testing.expectEqual(ast.AggFunc.count, agg.aggs[0].func);
    try testing.expect(agg.aggs[0].arg == null);
    try testing.expectEqual(ast.AggFunc.sum, agg.aggs[1].func);
    try testing.expectEqual(@as(usize, 1), agg.by.len);
    try testing.expectEqualStrings("csv", pl.stages[2].node.write.connector);
}

test "sql: CTE + LEFT JOIN with alias stripping" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH paid AS (
        \\  SELECT id, amount FROM 'in.csv' WHERE status = 'paid'
        \\)
        \\SELECT t.id, t.note, p.amount
        \\FROM 'in.csv' t
        \\LEFT JOIN paid p ON t.id = p.id;
    );
    // kind, binding, output
    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .binding);
    try testing.expectEqualStrings("paid", prog.stmts[1].binding.name);
    const pl = prog.stmts[2].output;
    try testing.expect(pl.stages[1].node == .join);
    const j = pl.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.left, j.kind);
    try testing.expectEqualStrings("paid", j.binding);
    try testing.expectEqualStrings("id", j.left_key.parts[0]);
    try testing.expectEqualStrings("id", j.right_key.parts[0]);
    // select items got their aliases stripped
    const sel = pl.stages[2].node.select;
    try testing.expectEqualStrings("id", sel[0].field.parts[0]);
    try testing.expectEqual(@as(usize, 1), sel[0].field.parts.len);
}

test "sql: UNION ALL BY NAME with tag literal and anchor" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\LOAD INTO sr.CT2_UNIFIED USING stream_load
        \\  UPSERT ON (CT2_EMPRESA, R_E_C_N_O_)
        \\  SPLIT BY (R_E_C_N_O_)
        \\AS
        \\SELECT '01' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2010 t
        \\UNION ALL BY NAME
        \\SELECT '02' AS CT2_EMPRESA, t.* FROM erp.dbo.CT2020 t
        \\ANCHOR SCHEMA erp.dbo.CT2010;
    );
    const pl = prog.stmts[3].output;
    try testing.expectEqual(@as(usize, 2), pl.stages.len); // union, write
    const u = pl.stages[0].node.union_;
    try testing.expectEqual(@as(usize, 2), u.branches.len);
    try testing.expectEqualStrings("01", u.branches[0].tag.?);
    try testing.expectEqualStrings("02", u.branches[1].tag.?);
    // hints: tag + canon
    try testing.expectEqualStrings("tag", pl.stages[0].hints[0].key);
    try testing.expectEqualStrings("CT2_EMPRESA", pl.stages[0].hints[0].value.ident);
    try testing.expectEqualStrings("canon", pl.stages[0].hints[1].key);
    try testing.expectEqualStrings("CT2010", pl.stages[0].hints[1].value.ident);
    // write: upsert composite + split hint
    const w = pl.stages[1].node.write;
    try testing.expectEqual(@as(usize, 2), w.mode.upsert.keys.len);
    try testing.expectEqualStrings("split", pl.stages[1].hints[0].key);
}

test "sql: PUSHDOWN fragment becomes a where hint on the read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT filial, valor FROM erp.dbo.SC5010
        \\  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
        \\WHERE valor > 0;
    );
    const pl = prog.stmts[2].output;
    try testing.expect(pl.stages[0].node == .read);
    try testing.expectEqualStrings("where", pl.stages[0].hints[0].key);
    try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", pl.stages[0].hints[0].value.str);
    try testing.expect(pl.stages[1].node == .filter);
}

test "sql: FOR EACH ROW OF json path with CASE dispatch" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE ENDPOINT '/fanout';
        \\PARAM job JSON FROM BODY;
        \\CREATE CONNECTION crm TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\FOR EACH ROW OF ($job.tables) AS (name, pk)
        \\  PARALLEL ON ERROR CONTINUE
        \\  CASE
        \\    WHEN pk IS EMPTY THEN
        \\      LOAD INTO sr.'crm_${lower(name)}' USING stream_load AS
        \\      SELECT * FROM crm.QUERY($$SELECT * FROM ${name}$$);
        \\    ELSE
        \\      LOAD INTO sr.'crm_${lower(name)}' USING stream_load
        \\        UPSERT ON ('${pk}') AS
        \\      SELECT *, now() AS extraction_timestamp
        \\      FROM crm.QUERY($$SELECT * FROM ${name}$$);
        \\  END CASE
        \\END FOR;
    );
    try testing.expect(prog.stmts[0].kind.kind == .http);
    try testing.expect(prog.stmts[1] == .param);
    try testing.expect(prog.stmts[1].param.is_json);
    const fe = prog.stmts[4].for_each;
    try testing.expectEqual(@as(usize, 2), fe.var_names.len);
    try testing.expect(fe.source == .json_path);
    try testing.expectEqualStrings("job", fe.source.json_path.parts[0]);
    try testing.expectEqual(@as(usize, 1), fe.body.len);
    const m = fe.body[0].match;
    try testing.expect(m.subject == null);
    try testing.expectEqual(@as(usize, 2), m.arms.len);
    try testing.expect(m.arms[0].guard != null);
    try testing.expect(m.arms[1].is_default);
    // arm 1: pipeline with read query + write (no select — lone star)
    const arm1 = m.arms[0].body[0].output;
    try testing.expectEqual(@as(usize, 2), arm1.stages.len);
    // arm 2: read, select(*, computed), write upsert
    const arm2 = m.arms[1].body[0].output;
    try testing.expectEqual(@as(usize, 3), arm2.stages.len);
    try testing.expectEqualStrings("crm_${lower(name)}", arm2.stages[2].node.write.target);
}

test "sql: connection credential convention injects env calls" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\SELECT 1 AS one FROM 'in.csv';
    );
    const conn = prog.stmts[1].connection;
    var saw_user = false;
    var saw_pass = false;
    for (conn.config) |attr| {
        if (std.mem.eql(u8, attr.key, "user")) {
            saw_user = true;
            try testing.expectEqualStrings("env", attr.value.call.name);
            try testing.expectEqualStrings("ERP_USER", attr.value.call.args[0].str_lit);
        }
        if (std.mem.eql(u8, attr.key, "password")) {
            saw_pass = true;
            try testing.expectEqualStrings("ERP_PASS", attr.value.call.args[0].str_lit);
        }
    }
    try testing.expect(saw_user and saw_pass);
}

test "sql: truncated expression is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "SELECT * FROM x.QUERY($$q$$) WHERE a >", &diag);
    try testing.expectError(error.ParseFailed, r);
}
