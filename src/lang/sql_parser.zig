//! Parser for the Basalt SQL dialect (language.md). Produces the SAME
//! `ast.Program` as the BSL parser — the planner, analyzer, pushdown, and
//! executor are shared ("one plan"). Only the surface differs.
//!
//! Mapping highlights (see language.md):
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
    "explain",  "costs",  "analyze",
};

fn isReservedAfterSource(name: []const u8) bool {
    for (reserved_after_source) |k| {
        if (eqlNoCase(name, k)) return true;
    }
    return false;
}

/// The file format of a path source is sniffed from its extension when the read
/// opens, so a computed path must still spell that extension out: everything after
/// the last `${...}` hole has to carry a `.ext`.
fn hasLiteralExt(tmpl: []const u8) bool {
    const tail = if (std.mem.lastIndexOfScalar(u8, tmpl, '}')) |i| tmpl[i + 1 ..] else tmpl;
    const dot = std.mem.lastIndexOfScalar(u8, tail, '.') orelse return false;
    return dot + 1 < tail.len;
}

const agg_names = [_]struct { n: []const u8, f: ast.AggFunc }{
    .{ .n = "count", .f = .count },
    .{ .n = "sum", .f = .sum },
    .{ .n = "avg", .f = .avg },
    .{ .n = "min", .f = .min },
    .{ .n = "max", .f = .max },
};

fn isGroupKey(group: []const ast.QualName, q: ast.QualName) bool {
    for (group) |k| {
        if (std.mem.eql(u8, k.last(), q.last())) return true;
    }
    return false;
}

/// The name the aggregate stage emits a `post` item under: a grouping key keeps its
/// own column name, a lifted aggregate its alias.
fn postItemName(it: ast.SelectItem) ?[]const u8 {
    return switch (it) {
        .field => |q| q.last(),
        .computed => |c| c.name,
        // A star cannot be resolved to names here; the caller declines to reorder.
        .star, .star_except, .star_rename => null,
    };
}

/// Does the SELECT list ask for a different column order than the aggregate stage
/// naturally emits? The stage writes every grouping key first and every aggregate
/// after, so `SELECT k1, SUM(x), k2` would silently come out `k1, k2, sum`. When
/// this returns true the caller adds the projection that puts the SELECT list back
/// in charge — and when it returns false nothing is added, keeping the common
/// keys-then-aggregates query one stage shorter.
fn aggOrderDiffers(post: []const ast.SelectItem, group: []const ast.QualName, aggs: []const ast.AggItem) bool {
    if (post.len != group.len + aggs.len) return false;
    for (post, 0..) |it, i| {
        const want = postItemName(it) orelse return false;
        const natural = if (i < group.len) group[i].last() else aggs[i - group.len].name;
        if (!std.mem.eql(u8, want, natural)) return true;
    }
    return false;
}

/// What to call a SELECT item in a diagnostic.
fn itemLabel(it: ast.SelectItem) []const u8 {
    return switch (it) {
        .star, .star_except, .star_rename => "*",
        .field => |q| q.last(),
        .computed => |c| c.name,
    };
}

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
    /// PARAM and LET names. `$p` parses to a plain single-part field, so this is
    /// the only way to tell a script constant from a column at parse time — which
    /// `constItemExpr` needs to allow one beside an aggregate.
    const_names: std.array_list.Managed([]const u8) = undefined,
    /// Bindings created by a derived table — `FROM (SELECT ...) x`. A derived table is
    /// an anonymous CTE, so it lowers to exactly what `WITH x AS (...)` produces; but
    /// it is discovered deep inside `parseSelectCore`, which has no statement list to
    /// append to. `parseQuery` drains this.
    pending_bindings: std.array_list.Managed(ast.Stmt) = undefined,
    /// Counter for naming derived tables that carry no alias.
    derived_n: usize = 0,

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
        if (self.at(.ident) or self.at(.qident)) return self.advance().text;
        return self.fail(self.curPos(), "expected identifier, found {s}", .{self.curTag().describe()});
    }

    /// An identifier in either spelling. Not the same as `isKw`, which stays
    /// bare-`ident` only: `"select"` is a column named select, never a keyword.
    fn atName(self: *Parser) bool {
        return self.at(.ident) or self.at(.qident);
    }
    fn expectKw(self: *Parser, kw: []const u8) Error!void {
        if (self.eatKw(kw)) return;
        return self.fail(self.curPos(), "expected `{s}`, found {s}", .{ kw, self.curTag().describe() });
    }
    /// A column name: identifier or quoted string (so '${var}' can build it).
    fn expectColName(self: *Parser) Error![]const u8 {
        if (self.atName() or self.at(.string)) return self.advance().text;
        return self.fail(self.curPos(), "expected a column name, found {s}", .{self.curTag().describe()});
    }

    /// Name for a computed item written without `AS`, joined from the source
    /// tokens so `COUNT(*)` becomes `count(*)`. ORDER BY synthesizes the same
    /// text for the same expression, which is what binds `ORDER BY COUNT(*)`
    /// to the SELECT item it repeats.
    fn synthName(self: *Parser, start: usize) Error![]const u8 {
        return self.synthRange(start, self.i);
    }

    fn synthRange(self: *Parser, start: usize, end: usize) Error![]const u8 {
        var buf = std.array_list.Managed(u8).init(self.arena);
        for (self.toks[start..end]) |t| {
            for (t.text) |c| buf.append(std.ascii.toLower(c)) catch return error.OutOfMemory;
        }
        return buf.toOwnedSlice() catch return error.OutOfMemory;
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
    fn isScriptConst(self: *Parser, name: []const u8) bool {
        for (self.const_names.items) |c| {
            if (std.mem.eql(u8, c, name)) return true;
        }
        return false;
    }

    pub fn parseProgram(self: *Parser) Error!ast.Program {
        self.conn_names = std.array_list.Managed([]const u8).init(self.arena);
        self.let_names = std.array_list.Managed([]const u8).init(self.arena);
        self.pending_bindings = std.array_list.Managed(ast.Stmt).init(self.arena);
        self.const_names = std.array_list.Managed([]const u8).init(self.arena);

        // A script that *opens* with EXPLAIN explains the whole script, offline and
        // without binding params — `basalt run` renders that plan without executing
        // anything. EXPLAIN anywhere else is a statement (`parseExplainStmt`), which
        // explains one query against the declarations above it, mid-run.
        var explain: ast.ExplainMode = .none;
        if (self.isKw("explain")) {
            _ = self.advance();
            if (self.isKw("costs"))
                return self.fail(self.curPos(), "EXPLAIN COSTS is not supported: basalt has no cost model", .{});
            explain = if (self.eatKw("analyze")) .analyze else .plan;
        }

        var stmts = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof)) {
            try self.parseStatement(&stmts);
        }
        if (stmts.items.len == 0)
            return self.fail(self.curPos(), "empty program: expected at least one statement", .{});

        const kind: ast.KindDecl = self.endpoint orelse
            .{ .kind = .batch, .config = &.{}, .pos = .{ .line = 1, .col = 1 } };
        try stmts.insert(0, .{ .kind = kind });
        return .{ .stmts = try stmts.toOwnedSlice(), .explain = explain };
    }

    /// One top-level (or arm-body) statement, appended to `out`.
    fn parseStatement(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        if (self.isKw("create")) return self.parseCreate(out);
        if (self.isKw("param")) {
            const p = try self.parseParam();
            try self.const_names.append(p.name);
            return out.append(.{ .param = p });
        }
        if (self.isKw("let")) {
            const l = try self.parseLetStmt();
            try self.const_names.append(l.name);
            return out.append(.{ .let_const = l });
        }
        if (self.isKw("print")) return out.append(.{ .print = try self.parsePrintStmt() });
        if (self.isKw("load")) return self.parseLoadInto(out);
        if (self.isKw("for")) return out.append(.{ .for_each = try self.parseForEach() });
        if (self.isKw("case")) return out.append(.{ .match = try self.parseCaseStmt() });
        if (self.isKw("throw")) return out.append(.{ .throw = try self.parseThrowStmt() });
        if (self.isKw("call")) return out.append(.{ .call = try self.parseCallStmt() });
        if (self.isKw("explain")) return self.parseExplainStmt(out);
        if (self.isKw("with") or self.isKw("select")) return self.parseTerminalQuery(out);
        return self.fail(self.curPos(), "expected a statement (CREATE / PARAM / LET / PRINT / THROW / EXPLAIN / LOAD INTO / SELECT / FOR / CASE / CALL), found {s}", .{self.curTag().describe()});
    }

    /// `EXPLAIN [ANALYZE] <query>;` in statement position — the same query forms a
    /// terminal statement takes (`SELECT`, `WITH ... SELECT`, `LOAD INTO ... AS`),
    /// explained against everything declared above it. A leading `EXPLAIN` is still
    /// consumed by `parseProgram` as the program-level prefix, which explains a whole
    /// script offline; this is the form that works anywhere else in one.
    fn parseExplainStmt(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        try self.expectKw("explain");
        if (self.isKw("costs"))
            return self.fail(self.curPos(), "EXPLAIN COSTS is not supported: basalt has no cost model", .{});
        const mode: ast.ExplainMode = if (self.eatKw("analyze")) .analyze else .plan;

        const base = out.items.len;
        if (self.isKw("load")) {
            try self.parseLoadInto(out);
        } else if (self.isKw("with") or self.isKw("select")) {
            try self.parseTerminalQuery(out);
        } else {
            return self.fail(self.curPos(), "expected SELECT, WITH or LOAD INTO after EXPLAIN, found {s}", .{self.curTag().describe()});
        }

        // A `WITH` hoists its CTEs into `out` as bindings ahead of the pipeline they
        // feed. Those stay ordinary statements — the query being explained is the one
        // pipeline the parse ended with.
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

    /// `LET name = <expr>;` — a script-scoped plan-time constant, referenced as
    /// `$name`. A bare `LET` in statement position is unambiguous: an expression is
    /// never a statement, so the expression-level `LET x = v IN body` can only be
    /// reached from inside an expression.
    fn parseLetStmt(self: *Parser) Error!ast.LetConst {
        const pos = self.curPos();
        try self.expectKw("let");
        const name = try self.expectIdent();
        _ = try self.expect(.assign);
        const expr = try self.parseExpr();
        _ = try self.expect(.semi);
        return .{ .name = name, .expr = expr, .pos = pos };
    }

    /// `PRINT <expr>;` — one progress line, evaluated where it stands. The argument
    /// is an ordinary expression, so `||`, `$param` and the loop variables of an
    /// enclosing `FOR EACH` / statement function body all work with no extra syntax.
    fn parsePrintStmt(self: *Parser) Error!ast.Print {
        const pos = self.curPos();
        try self.expectKw("print");
        const expr = try self.parseExpr();
        _ = try self.expect(.semi);
        return .{ .expr = expr, .pos = pos };
    }

    fn parseCreate(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        try self.expectKw("create");
        const replace = if (self.eatKw("or")) blk: {
            try self.expectKw("replace");
            break :blk true;
        } else false;
        if (self.eatKw("endpoint")) {
            const path = try self.expect(.string);
            var attrs = std.array_list.Managed(ast.Attr).init(self.arena);
            try attrs.append(.{ .key = "path", .value = try self.mk(.{ .str_lit = path.text }), .pos = pos });
            if (self.eatKw("doc")) {
                const doc = try self.expect(.string);
                try attrs.append(.{ .key = "doc", .value = try self.mk(.{ .str_lit = doc.text }), .pos = pos });
            }
            var buffer: ?ast.BufferDecl = null;
            if (self.eatKw("accept")) buffer = try self.parseAcceptBuffer(pos);
            _ = try self.expect(.semi);
            if (self.endpoint != null)
                return self.fail(pos, "duplicate CREATE ENDPOINT", .{});
            self.endpoint = .{ .kind = .http, .config = try attrs.toOwnedSlice(), .buffer = buffer, .pos = pos };
            return;
        }
        if (self.eatKw("connection")) {
            const conn = try self.parseConnection(pos);
            try self.conn_names.append(conn.name);
            return out.append(.{ .connection = conn });
        }
        if (self.eatKw("function")) {
            return out.append(.{ .func = try self.parseFunction(pos, replace) });
        }
        return self.fail(self.curPos(), "expected ENDPOINT, CONNECTION, or FUNCTION after CREATE", .{});
    }

    /// `ACCEPT BODY (schema) INTO BUFFER 'name' [AT 'dir'] [SEGMENT n MB|KB|GB]
    /// [MAX n MB|GB] [RETAIN UNTIL LOADED | RETAIN n HOURS]` — after CREATE
    /// ENDPOINT. `MAX` is the on-disk backpressure limit (503 beyond it).
    fn parseAcceptBuffer(self: *Parser, pos: Pos) Error!ast.BufferDecl {
        try self.expectKw("body");
        const schema = try self.parseBodySchema();
        try self.expectKw("into");
        try self.expectKw("buffer");
        const name = try self.expect(.string);
        var decl = ast.BufferDecl{ .name = name.text, .dir = "wal", .schema = schema, .pos = pos };
        while (true) {
            if (self.eatKw("at")) {
                decl.dir = (try self.expect(.string)).text;
            } else if (self.eatKw("segment")) {
                decl.segment_bytes = try self.parseByteSize();
            } else if (self.eatKw("max")) {
                decl.max_bytes = try self.parseByteSize();
            } else if (self.eatKw("retain")) {
                if (self.eatKw("until")) {
                    try self.expectKw("loaded");
                    decl.retain_hours = null;
                } else {
                    const n = try self.expect(.int);
                    try self.expectKw("hours");
                    decl.retain_hours = std.fmt.parseInt(u32, n.text, 10) catch
                        return self.fail(pos, "bad RETAIN hours `{s}`", .{n.text});
                }
            } else break;
        }
        return decl;
    }

    /// One dotted name atom: a plain identifier, or `IDENTIFIER(<expr>)` whose
    /// string value is computed per row (lowered to a `${...}` template).
    fn parseNameSegment(self: *Parser) Error![]const u8 {
        if (self.isKw("identifier") and self.peekTag() == .lparen) {
            _ = self.advance();
            _ = try self.expect(.lparen);
            const e = try self.parseExpr();
            _ = try self.expect(.rparen);
            return self.exprToTemplate(e);
        }
        return self.expectIdent();
    }

    /// A write-target atom: a name, a quoted string (interpolated as-is), or
    /// IDENTIFIER(<expr>) for a computed name.
    fn parseTargetSegment(self: *Parser) Error![]const u8 {
        if (self.isKw("identifier") and self.peekTag() == .lparen) {
            _ = self.advance();
            _ = try self.expect(.lparen);
            const e = try self.parseExpr();
            _ = try self.expect(.rparen);
            return self.exprToTemplate(e);
        }
        return self.expectColName();
    }

    /// Lower a reflection expression into the internal `${...}` template. A
    /// `concat(...)` / `||` chain becomes literal text spliced with `${expr}`
    /// holes; a bare string literal stays literal (no hole); anything else is
    /// one `${expr}` hole re-parsed and evaluated per row.
    /// The trailing clauses a union accepts, in either order: `PUSHDOWN(<expr>)`
    /// — one raw predicate descended into *every* branch's source query, the
    /// same `where` hint a plain read produces — and `ANCHOR SCHEMA <table>`,
    /// naming the branch whose columns are the reconciliation authority.
    fn parseUnionClauses(self: *Parser, hints: *std.array_list.Managed(ast.Hint), pos: Pos) Error!void {
        while (true) {
            if (self.eatKw("pushdown")) {
                _ = try self.expect(.lparen);
                const e = try self.parseExpr();
                _ = try self.expect(.rparen);
                const frag = try self.exprToTemplate(e);
                // An empty predicate is no predicate: `PUSHDOWN($f)` with `f`
                // unset reads the whole table rather than emitting `WHERE `.
                if (frag.len > 0)
                    try hints.append(.{ .key = "where", .value = .{ .str = frag }, .pos = pos });
            } else if (self.eatKw("anchor")) {
                try self.expectKw("schema");
                const q = try self.parseQualNameTok();
                try hints.append(.{ .key = "canon", .value = .{ .ident = q.last() }, .pos = pos });
            } else break;
        }
    }

    fn exprToTemplate(self: *Parser, e: *const ast.Expr) Error![]const u8 {
        var buf = std.array_list.Managed(u8).init(self.arena);
        try self.templatePart(&buf, e);
        return buf.toOwnedSlice();
    }

    fn templatePart(self: *Parser, buf: *std.array_list.Managed(u8), e: *const ast.Expr) Error!void {
        switch (e.*) {
            .str_lit => |s| try buf.appendSlice(s),
            .call => |c| {
                if (std.mem.eql(u8, c.name, "concat")) {
                    for (c.args) |arg| try self.templatePart(buf, arg);
                    return;
                }
                try buf.appendSlice("${");
                try self.unparse(buf, e);
                try buf.append('}');
            },
            else => {
                try buf.appendSlice("${");
                try self.unparse(buf, e);
                try buf.append('}');
            },
        }
    }

    /// Print an expression back as interpolation-hole text (the `${ <here> }`
    /// sub-language, which is the same expression grammar; `$name` already
    /// parsed to a bare field, so it prints as `name`).
    fn unparse(self: *Parser, buf: *std.array_list.Managed(u8), e: *const ast.Expr) Error!void {
        switch (e.*) {
            .null_lit => try buf.appendSlice("null"),
            .bool_lit => |b| try buf.appendSlice(if (b) "true" else "false"),
            .int_lit => |v| try buf.writer().print("{d}", .{v}),
            .float_lit => |v| try buf.writer().print("{d}", .{v}),
            .str_lit => |s| {
                try buf.append('\'');
                for (s) |ch| {
                    if (ch == '\'') try buf.append('\'');
                    try buf.append(ch);
                }
                try buf.append('\'');
            },
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

    /// `<int> KB|MB|GB` -> bytes.
    fn parseByteSize(self: *Parser) Error!u64 {
        const n = try self.expect(.int);
        const v = std.fmt.parseInt(u64, n.text, 10) catch
            return self.fail(self.curPos(), "bad size `{s}`", .{n.text});
        const unit = try self.expectIdent();
        if (eqlNoCase(unit, "kb")) return v << 10;
        if (eqlNoCase(unit, "mb")) return v << 20;
        if (eqlNoCase(unit, "gb")) return v << 30;
        return self.fail(self.curPos(), "expected KB, MB, or GB (got `{s}`)", .{unit});
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

    /// `CREATE [OR REPLACE] FUNCTION name(params) AS <body>` in both forms:
    /// an expression body (`AS <expr>;`) or a statement block (`AS <stmts> END;`).
    fn parseFunction(self: *Parser, pos: Pos, replace: bool) Error!ast.FnDecl {
        const name = try self.expectIdent();
        _ = try self.expect(.lparen);
        var params = std.array_list.Managed(ast.FnParam).init(self.arena);
        if (!self.at(.rparen)) {
            try params.append(try self.parseFnParam());
            while (self.eat(.comma)) try params.append(try self.parseFnParam());
        }
        _ = try self.expect(.rparen);
        var seen_default = false;
        for (params.items) |p| {
            if (p.default != null) {
                seen_default = true;
            } else if (seen_default) {
                return self.fail(pos, "`{s}`: parameter `{s}` without DEFAULT follows one with DEFAULT", .{ name, p.name });
            }
        }
        try self.expectKw("as");

        if (self.atStmtBody()) {
            // A statement function's parameters bind like loop variables (§9), so
            // they are script-scope constants inside the body for the same reason.
            const const_base = self.const_names.items.len;
            for (params.items) |p| try self.const_names.append(p.name);
            var body = std.array_list.Managed(ast.Stmt).init(self.arena);
            while (!self.at(.eof) and !self.isKw("end")) {
                try self.parseStatement(&body);
            }
            self.const_names.shrinkRetainingCapacity(const_base);
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

    /// Does the body after `AS` open a statement block? Exactly the keywords that
    /// begin a statement and can never begin a scalar expression.
    ///
    /// `CASE` is deliberately absent: `AS CASE WHEN ... END` stays the *expression*
    /// form (a scalar CASE), which is overwhelmingly what a function body means by
    /// it, and the statement CASE has its own `END CASE`. So a statement body that
    /// wants to branch has to lead with another statement keyword (or be reached
    /// through a `FOR` / `CALL`); a leading statement `CASE` is not spellable here.
    fn atStmtBody(self: *Parser) bool {
        return self.isKw("load") or self.isKw("for") or self.isKw("call") or
            self.isKw("print") or self.isKw("select") or self.isKw("with");
    }

    /// `name [TYPE] [DEFAULT <expr>]`. A bare name is untyped, exactly as before.
    fn parseFnParam(self: *Parser) Error!ast.FnParam {
        const name = try self.expectIdent();
        var ty: ?types.Type = null;
        if (!self.at(.comma) and !self.at(.rparen) and !self.isKw("default"))
            ty = try self.parseTypeName();
        var default: ?*ast.Expr = null;
        if (self.eatKw("default")) default = try self.parseExpr();
        return .{ .name = name, .ty = ty, .default = default };
    }

    /// `CALL name(args);` — invoke a statement-form function.
    fn parseCallStmt(self: *Parser) Error!ast.CallStmt {
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

    /// `THROW <message> [WHEN <condition>];` — both operands are ordinary
    /// expressions, so `$param`, `LET`s and every scalar function are available.
    /// `WHEN` is read greedily, which is what ends the message expression.
    fn parseThrowStmt(self: *Parser) Error!ast.Throw {
        const pos = self.curPos();
        try self.expectKw("throw");
        const message = try self.parseExpr();
        const when: ?*ast.Expr = if (self.eatKw("when")) try self.parseExpr() else null;
        _ = try self.expect(.semi);
        return .{ .message = message, .when = when, .pos = pos };
    }

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
        var header_name: ?[]const u8 = null;
        if (self.eatKw("from")) {
            if (self.eatKw("query")) {
                source = .query;
            } else if (self.eatKw("body")) {
                source = .body;
            } else if (self.eatKw("header")) {
                source = .header;
                if (self.eat(.lparen)) {
                    header_name = (try self.expect(.string)).text;
                    _ = try self.expect(.rparen);
                }
            } else {
                return self.fail(self.curPos(), "expected QUERY, BODY, or HEADER after FROM", .{});
            }
        }
        if (is_json and source == null) source = .body;
        _ = try self.expect(.semi);
        return .{ .name = name, .ty = ty, .default = default, .source = source, .header_name = header_name, .pos = pos, .is_json = is_json };
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

    fn parseLoadInto(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        try self.expectKw("load");
        try self.expectKw("into");

        var write: ast.Write = undefined;
        if (self.at(.string)) {
            write = .{ .connector = "csv", .form = null, .target = self.advance().text, .mode = .default };
        } else if (self.isKw("identifier") and self.peekTag() == .lparen) {
            // A computed *file* target, the sink counterpart of `FROM
            // IDENTIFIER(...)`: one output file per loop row or per param.
            // `conn.IDENTIFIER(...)` is a computed table and parses below.
            const ipos = self.curPos();
            _ = self.advance();
            _ = try self.expect(.lparen);
            const e = try self.parseExpr();
            _ = try self.expect(.rparen);
            const tmpl = try self.exprToTemplate(e);
            // The extension picks the writer, and which dispositions are legal
            // (`APPEND` accumulates for CSV, never for parquet), both of which are
            // settled at plan time — so it cannot come from a per-row value.
            if (!hasLiteralExt(tmpl))
                return self.fail(ipos, "dynamic target needs a literal extension (end it with `|| '.csv'`, `|| '.parquet'`, …)", .{});
            write = .{ .connector = "csv", .form = null, .target = tmpl, .mode = .default };
        } else {
            const conn = try self.expectIdent();
            if (!self.isConn(conn))
                return self.fail(pos, "unknown connection `{s}` in LOAD INTO (declare it with CREATE CONNECTION first)", .{conn});
            _ = try self.expect(.dot);
            var parts = std.array_list.Managed([]const u8).init(self.arena);
            try parts.append(try self.parseTargetSegment());
            while (self.eat(.dot)) try parts.append(try self.parseTargetSegment());
            const target = try std.mem.join(self.arena, ".", parts.items);
            write = .{ .connector = conn, .form = null, .target = target, .mode = .default };
        }

        if (self.eatKw("using")) write.form = try self.expectIdent();

        var whints = std.array_list.Managed(ast.Hint).init(self.arena);
        while (true) {
            if (self.eatKw("append")) {
                write.mode = .append;
            } else if (self.eatKw("replace")) {
                write.mode = .overwrite;
            } else if (self.eatKw("upsert")) {
                var keys = std.array_list.Managed([]const u8).init(self.arena);
                if (self.eatKw("on")) {
                    _ = try self.expect(.lparen);
                    try keys.append(try self.parseTargetSegment());
                    while (self.eat(.comma)) try keys.append(try self.parseTargetSegment());
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
        // A derived table anywhere below appends its binding to `pending_bindings`;
        // move them into the program before the statement that references them.
        defer if (self.pending_bindings.items.len > 0) {
            out.appendSlice(self.pending_bindings.items) catch {};
            self.pending_bindings.clearRetainingCapacity();
        };

        const first = try self.parseSelectCore();

        if (self.isKw("union")) {
            try self.parseUnionTail(first, stages);
        } else {
            try stages.appendSlice(first.stages);
        }

        var hidden_drop: ?[]const ast.SelectItem = null;
        if (self.eatKw("order")) {
            try self.expectKw("by");
            var keys = std.array_list.Managed(ast.SortKey).init(self.arena);
            while (true) {
                var q: ast.QualName = undefined;
                if (self.at(.ident) and self.peekTag() == .lparen) {
                    const start = self.i;
                    _ = try self.parseExpr();
                    const parts = try self.arena.alloc([]const u8, 1);
                    parts[0] = resolveExprAlias(first.expr_aliases, try self.synthName(start));
                    q = .{ .parts = parts };
                } else {
                    q = try self.parseQualNameTok();
                    q = stripQual(q, &first.aliases);
                }
                var desc = false;
                if (self.eatKw("desc")) {
                    desc = true;
                } else _ = self.eatKw("asc");
                try keys.append(.{ .field = q, .desc = desc });
                if (!self.eat(.comma)) break;
            }
            const sort_keys = try keys.toOwnedSlice();

            // A sort key the SELECT list does not project still has to reach the
            // sort operator. Carry it as a hidden column and drop it after the
            // LIMIT — standard SQL allows ordering by an unselected column, and
            // dropping it last leaves sort+limit adjacent so top-N still fuses.
            if (lastSelectIdx(stages.items)) |si| {
                const proj = stages.items[si].node.select;
                var wildcard = false;
                var names = std.array_list.Managed([]const u8).init(self.arena);
                for (proj) |it| switch (it) {
                    .field => |f| try names.append(f.last()),
                    .computed => |c| try names.append(c.name),
                    else => wildcard = true,
                };
                if (!wildcard) {
                    var extended = std.array_list.Managed(ast.SelectItem).init(self.arena);
                    try extended.appendSlice(proj);
                    for (sort_keys) |k| {
                        if (k.field.parts.len != 1) continue;
                        var have = false;
                        for (names.items) |m| {
                            if (std.mem.eql(u8, m, k.field.last())) have = true;
                        }
                        if (!have) try extended.append(.{ .field = k.field });
                    }
                    if (extended.items.len != proj.len) {
                        stages.items[si].node.select = try extended.toOwnedSlice();
                        var keep = std.array_list.Managed(ast.SelectItem).init(self.arena);
                        for (names.items) |m| {
                            const parts = try self.arena.alloc([]const u8, 1);
                            parts[0] = m;
                            try keep.append(.{ .field = .{ .parts = parts } });
                        }
                        hidden_drop = try keep.toOwnedSlice();
                    }
                }
            }
            try stages.append(.{ .node = .{ .sort = .{ .keys = sort_keys } }, .hints = &.{}, .pos = self.curPos() });
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

        if (hidden_drop) |keep|
            try stages.append(.{ .node = .{ .select = keep }, .hints = &.{}, .pos = self.curPos() });
    }

    /// Index of the projection a sort would read through, if the pipeline ends
    /// in one.
    fn lastSelectIdx(stages: []const ast.Stage) ?usize {
        if (stages.len == 0) return null;
        return if (stages[stages.len - 1].node == .select) stages.len - 1 else null;
    }

    /// Set when a core is exactly `SELECT ['lit' AS col,] t.* FROM conn.table`
    /// — the only shape a UNION ALL BY NAME branch may take.
    const BranchInfo = struct { read: ast.Read, tag: ?[]const u8, tag_col: ?[]const u8 };

    /// A computed SELECT item indexed by the text of its expression, so that
    /// `GROUP BY`/`ORDER BY` repeating the expression bind to the column the
    /// SELECT list already produced instead of asking for a second one.
    const ExprAlias = struct { synth: []const u8, out: []const u8 };

    fn resolveExprAlias(map: []const ExprAlias, synth: []const u8) []const u8 {
        for (map) |m| {
            if (std.mem.eql(u8, m.synth, synth)) return m.out;
        }
        return synth;
    }

    const Core = struct {
        stages: []const ast.Stage,
        aliases: AliasSet,
        union_branch: ?BranchInfo,
        expr_aliases: []const ExprAlias = &.{},
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

        const RawItem = union(enum) {
            item: ast.SelectItem,
            qstar: []const u8,
        };
        var raw_items = std.array_list.Managed(RawItem).init(self.arena);
        var expr_aliases = std.array_list.Managed(ExprAlias).init(self.arena);
        var win_funcs = std.array_list.Managed(ast.WindowFunc).init(self.arena);
        var win_part = std.array_list.Managed(ast.QualName).init(self.arena);
        var win_ord = std.array_list.Managed(ast.SortKey).init(self.arena);
        while (true) {
            // `ROW_NUMBER() OVER (PARTITION BY .. ORDER BY ..)` is recognised as a whole
            // select item, before the expression parser sees it: a window function is a
            // stage, not an expression, and lifting one out of the middle of an
            // arithmetic expression would need the machinery `IN (SELECT ...)` needs.
            if (try self.parseWindowItem(&win_funcs, &win_part, &win_ord)) {
                if (!self.eat(.comma)) break;
                continue;
            }
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
                _ = self.advance();
                _ = self.advance();
                try raw_items.append(.{ .qstar = alias });
            } else {
                const start = self.i;
                const e = try self.parseExpr();
                const synth: ?[]const u8 = if (e.* == .field) null else try self.synthRange(start, self.i);
                var name: ?[]const u8 = null;
                if (self.eatKw("as")) name = try self.expectColName();
                if (name) |n| {
                    if (synth) |sy| try expr_aliases.append(.{ .synth = sy, .out = n });
                    try raw_items.append(.{ .item = .{ .computed = .{ .name = n, .expr = e } } });
                } else if (e.* == .field) {
                    try raw_items.append(.{ .item = .{ .field = e.field } });
                } else {
                    const sy = synth.?;
                    try expr_aliases.append(.{ .synth = sy, .out = sy });
                    try raw_items.append(.{ .item = .{ .computed = .{ .name = sy, .expr = e } } });
                }
            }
            if (!self.eat(.comma)) break;
        }

        var aliases = AliasSet{};
        var read_hints = std.array_list.Managed(ast.Hint).init(self.arena);
        const src: ast.Stage.Node = if (self.eatKw("from"))
            try self.parseFromSource(&aliases, &read_hints)
        else
            // `SELECT 1;` — no FROM plans a one-row unit source.
            .{ .read = .{ .connector = "unit", .form = .unit } };

        while (true) {
            if (self.eatKw("pushdown")) {
                _ = try self.expect(.lparen);
                const e = try self.parseExpr();
                _ = try self.expect(.rparen);
                const frag = try self.exprToTemplate(e);
                if (frag.len > 0)
                    try read_hints.append(.{ .key = "where", .value = .{ .str = frag }, .pos = pos });
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

        while (true) {
            const jk: ?ast.JoinKind = blk: {
                if (self.isKw("inner")) break :blk .inner;
                if (self.isKw("left")) break :blk .left;
                if (self.isKw("right")) break :blk .right;
                if (self.isKw("full")) break :blk .full;
                if (self.isKw("semi")) break :blk .semi;
                if (self.isKw("anti")) break :blk .anti;
                if (self.isKw("cross")) break :blk .cross;
                if (self.isKw("join")) break :blk .inner;
                break :blk null;
            };
            const kind = jk orelse break;
            if (!self.isKw("join")) _ = self.advance();
            _ = self.eatKw("outer");
            try self.expectKw("join");
            const jpos = self.curPos();
            // `CROSS JOIN UNNEST(...)` is the row-expanding form and stays an
            // explode stage; `CROSS JOIN <cte>` is the cartesian product.
            if (kind == .cross and self.isKw("unnest")) {
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
            // `JOIN (SELECT ...) x ON ...` — the same lowering as a FROM-position
            // derived table, since a join's right side is named by binding anyway.
            const binding = if (self.at(.lparen)) try self.parseDerivedTable() else try self.expectIdent();
            if (!self.isLet(binding))
                return self.fail(jpos, "JOIN right side `{s}` must be a WITH-defined CTE", .{binding});
            var jalias: ?[]const u8 = null;
            if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
                jalias = self.advance().text;
                aliases.add(jalias.?);
            }
            var left_keys = std.array_list.Managed(ast.QualName).init(self.arena);
            var right_keys = std.array_list.Managed(ast.QualName).init(self.arena);
            if (kind == .cross) {
                if (self.isKw("on"))
                    return self.fail(self.curPos(), "CROSS JOIN takes no ON clause — it pairs every row with every row", .{});
            } else {
                if (!self.isKw("on"))
                    return self.fail(self.curPos(), "expected `ON <column> = <column>` after JOIN {s}", .{binding});
                _ = self.advance();
                // `ON a = b AND c = d`: plain column names only. Anything else
                // (a function call, a literal, a range test) belongs upstream.
                while (true) {
                    const a = try self.parseJoinKey();
                    if (!(self.eat(.assign) or self.eat(.eq))) return self.joinKeyFail();
                    const b = try self.parseJoinKey();
                    const a_right = qualHasPrefix(a, jalias orelse binding);
                    const l = if (a_right) b else a;
                    const r = if (a_right) a else b;
                    try left_keys.append(stripQual(l, &aliases));
                    try right_keys.append(stripPrefix(r, jalias orelse binding));
                    if (!self.eatKw("and")) break;
                }
            }
            var jhints = std.array_list.Managed(ast.Hint).init(self.arena);
            if (self.isKw("with") and self.peekTag() == .lparen) {
                _ = self.advance();
                try self.parseWithHints(&jhints);
            }
            try stages.append(.{
                .node = .{ .join = .{
                    .kind = kind,
                    .binding = binding,
                    .left_keys = try left_keys.toOwnedSlice(),
                    .right_keys = try right_keys.toOwnedSlice(),
                } },
                .hints = try jhints.toOwnedSlice(),
                .pos = jpos,
            });
        }

        if (self.eatKw("where")) {
            const fpos = self.curPos();
            var e = try self.parseExpr();
            e = try self.stripExpr(e, &aliases);
            try stages.append(.{ .node = .{ .filter = e }, .hints = &.{}, .pos = fpos });
        }

        var group: []const ast.QualName = &.{};
        if (self.eatKw("group")) {
            try self.expectKw("by");
            var keys = std.array_list.Managed(ast.QualName).init(self.arena);
            while (true) {
                const gstart = self.i;
                const ge = try self.parseExpr();
                var q: ast.QualName = undefined;
                if (ge.* == .int_lit) {
                    // `GROUP BY 1` is positional — it names the first SELECT
                    // item, the way DuckDB and Postgres read it.
                    const n = ge.int_lit;
                    if (n < 1 or n > @as(i64, @intCast(raw_items.items.len)))
                        return self.fail(pos, "GROUP BY position {d} is out of range", .{n});
                    const ri = raw_items.items[@intCast(n - 1)];
                    if (ri != .item) return self.fail(pos, "GROUP BY position {d} refers to `*`", .{n});
                    q = switch (ri.item) {
                        .field => |f| stripQual(f, &aliases),
                        .computed => |c| blk: {
                            const parts = try self.arena.alloc([]const u8, 1);
                            parts[0] = c.name;
                            break :blk ast.QualName{ .parts = parts };
                        },
                        else => return self.fail(pos, "GROUP BY position {d} refers to `*`", .{n}),
                    };
                } else if (ge.* == .field) {
                    q = stripQual(ge.field, &aliases);
                } else {
                    // A computed key names the expression exactly as the SELECT
                    // list named it, so both sides bind to the same column.
                    const parts = try self.arena.alloc([]const u8, 1);
                    parts[0] = resolveExprAlias(expr_aliases.items, try self.synthName(gstart));
                    q = .{ .parts = parts };
                }
                try keys.append(q);
                if (!self.eat(.comma)) break;
            }
            group = try keys.toOwnedSlice();
        }

        var having: ?*ast.Expr = null;
        if (self.eatKw("having")) having = try self.parseExpr();

        var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var aggs = std.array_list.Managed(ast.AggItem).init(self.arena);
        var union_tag: ?[]const u8 = null;
        var union_tag_col: ?[]const u8 = null;
        var star_count: usize = 0;
        // Output items as the SELECT list asked for them, used only when an
        // aggregate had to be lifted out of a surrounding expression — that is
        // the one case needing a projection after the aggregate stage.
        var post = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var lifted = false;
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
                        try post.append(.star);
                        star_count += 1;
                    },
                    .star_except, .star_rename => {
                        try items.append(it);
                        try post.append(it);
                    },
                    .field => |q| {
                        const stripped_q = stripQual(q, &aliases);
                        try items.append(.{ .field = stripped_q });
                        try post.append(.{ .field = try self.singleName(stripped_q.last()) });
                    },
                    .computed => |c| {
                        const stripped = try self.stripExpr(c.expr, &aliases);
                        if (stripped.* == .call) {
                            if (aggFunc(stripped.call.name)) |f| {
                                const arg: ?*ast.Expr = if (stripped.call.args.len > 0) stripped.call.args[0] else null;
                                try aggs.append(.{ .name = c.name, .func = f, .arg = arg, .distinct = stripped.call.distinct });
                                try post.append(.{ .field = try self.singleName(c.name) });
                                continue;
                            }
                        }
                        // An aggregate buried in a larger expression: the calls
                        // move to the aggregate stage and what is left becomes a
                        // projection over its output.
                        if (containsAgg(stripped)) {
                            const outer = try self.liftAggs(stripped, &aggs, expr_aliases.items, pos);
                            try post.append(.{ .computed = .{ .name = c.name, .expr = outer } });
                            lifted = true;
                            continue;
                        }
                        // `SELECT x AS y ... GROUP BY x` — an aliased grouping
                        // key. The aggregate emits the key under its own name,
                        // so the rename belongs after it; renaming beforehand
                        // would hide `x` from the GROUP BY that names it.
                        if (group.len > 0 and stripped.* == .field and isGroupKey(group, stripped.field)) {
                            try post.append(.{ .computed = .{ .name = c.name, .expr = stripped } });
                            lifted = true;
                            continue;
                        }
                        if (stripped.* == .str_lit and union_tag == null) {
                            union_tag = stripped.str_lit;
                            union_tag_col = c.name;
                        }
                        try items.append(.{ .computed = .{ .name = c.name, .expr = stripped } });
                        try post.append(.{ .field = try self.singleName(c.name) });
                    },
                },
            }
        }

        if (aggs.items.len > 0 or group.len > 0) {
            if (aggs.items.len == 0)
                return self.fail(pos, "GROUP BY without aggregate functions in SELECT", .{});
            for (aggs.items) |a| {
                if (a.distinct and a.func != .count)
                    return self.fail(pos, "DISTINCT is only supported inside COUNT", .{});
            }

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
                // The projection runs before the aggregate, so it has to carry
                // the columns the aggregate arguments read as well as the ones
                // the SELECT list asked for.
                var needed = std.array_list.Managed(ast.QualName).init(self.arena);
                for (aggs.items) |a| {
                    if (a.arg) |arg| try self.collectFields(arg, &needed);
                }
                for (needed.items) |q| {
                    if (q.parts.len != 1) continue;
                    var have = false;
                    for (items.items) |it| switch (it) {
                        .field => |f| {
                            if (std.mem.eql(u8, f.last(), q.last())) have = true;
                        },
                        .computed => |cc| {
                            if (std.mem.eql(u8, cc.name, q.last())) have = true;
                        },
                        else => {},
                    };
                    if (!have) try items.append(.{ .field = q });
                }
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            } else {
                // A constant item is one value for the whole query, so it belongs
                // to neither the aggregate nor the grouping — it moves to a
                // projection after the aggregate, the same place a lifted
                // `round(avg(x), 2)` puts its arithmetic. This is what lets an
                // aggregate result carry a run id or a tenant tag:
                // `SELECT $tag AS tag, region, COUNT(*) ... GROUP BY region`.
                var kept = std.array_list.Managed(ast.SelectItem).init(self.arena);
                for (items.items) |it| {
                    if (it == .computed and self.constItemExpr(it.computed.expr)) {
                        try self.repointPostItem(&post, it.computed.name, it.computed.expr);
                        lifted = true;
                        continue;
                    }
                    if (it != .field)
                        return self.fail(pos, "`{s}` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY", .{itemLabel(it)});
                    // A plain column that is not a grouping key has no single
                    // value per group. It used to be dropped from the output
                    // without a word, which is worse than refusing the query.
                    if (!isGroupKey(group, it.field))
                        return self.fail(pos, "`{s}` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY", .{it.field.last()});
                    try kept.append(it);
                }
                items = kept;
            }
            // Before the aggregate is sealed, since HAVING may name one that the
            // SELECT list does not.
            if (having) |h| {
                if (try self.addHavingAggs(h, &aggs, expr_aliases.items)) lifted = true;
            }
            const aggs_out = try aggs.toOwnedSlice();
            try stages.append(.{
                .node = .{ .aggregate = .{ .aggs = aggs_out, .by = group } },
                .hints = &.{},
                .pos = pos,
            });
            if (having) |h| {
                const hf = try self.havingRewrite(h, expr_aliases.items);
                try stages.append(.{ .node = .{ .filter = hf }, .hints = &.{}, .pos = pos });
            }
            // An interleaved SELECT list (`SELECT k, SUM(x), k2`) needs the same
            // projection a lifted aggregate does, for order rather than arithmetic.
            if (!lifted and aggOrderDiffers(post.items, group, aggs_out)) lifted = true;
            // Runs after HAVING, which reads the aggregate's own columns — the
            // projection would have already replaced them.
            if (lifted) {
                for (post.items) |it| {
                    if (it == .star or it == .star_except or it == .star_rename)
                        return self.fail(pos, "`*` cannot be combined with an aggregate inside an expression", .{});
                }
                try stages.append(.{ .node = .{ .select = try post.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
        } else {
            const lone_star = items.items.len == 1 and items.items[0] == .star;
            if (!lone_star) {
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
        }

        if (distinct) {
            try stages.append(.{ .node = .{ .distinct = .{ .on = distinct_on } }, .hints = &.{}, .pos = pos });
        }

        if (win_funcs.items.len > 0) {
            try stages.append(.{ .node = .{ .window = .{
                .funcs = try win_funcs.toOwnedSlice(),
                .partition_by = try win_part.toOwnedSlice(),
                .order_by = try win_ord.toOwnedSlice(),
            } }, .hints = &.{}, .pos = pos });
        }

        var union_branch: ?BranchInfo = null;
        const s = stages.items;
        if (s.len <= 2 and s[0].node == .read) {
            const shape_ok = s.len == 1 or
                (s[1].node == .select and star_count == 1 and s[1].node.select.len <= 2);
            if (shape_ok) {
                union_branch = .{ .read = s[0].node.read, .tag = union_tag, .tag_col = union_tag_col };
                if (s.len == 2 and union_tag == null and s[1].node.select.len == 2)
                    union_branch = null;
            }
        }

        return .{ .stages = try stages.toOwnedSlice(), .aliases = aliases, .union_branch = union_branch, .expr_aliases = try expr_aliases.toOwnedSlice() };
    }

    /// A `UNION ALL BY NAME` arm that is not a bare source: lower the whole core to a
    /// binding and hand back a branch that reads it. The union operator aligns arms by
    /// column name off their schemas, so it does not care whether an arm is a table or
    /// a query — only the parser used to.
    fn unionBranchFromCore(self: *Parser, core: Core) Error!ast.UnionBranch {
        self.derived_n += 1;
        const name = try std.fmt.allocPrint(self.arena, "__union{d}", .{self.derived_n});
        try self.let_names.append(name);
        try self.pending_bindings.append(.{ .binding = .{
            .name = name,
            .pipeline = .{ .stages = core.stages, .pos = self.curPos() },
            .pos = self.curPos(),
        } });
        const ref = try self.arena.alloc(ast.Stage, 1);
        ref[0] = .{ .node = .{ .ref = name }, .hints = &.{}, .pos = self.curPos() };
        return .{
            .read = .{ .connector = "csv", .form = .{ .path = "" } },
            .tag = null,
            .pipeline = .{ .stages = ref, .pos = self.curPos() },
        };
    }

    /// `UNION ALL BY NAME core... [ANCHOR SCHEMA qual]` — collapse the first core
    /// and every following core into one union_ stage.
    fn parseUnionTail(self: *Parser, first: Core, stages: *std.array_list.Managed(ast.Stage)) Error!void {
        const pos = self.curPos();
        var branches = std.array_list.Managed(ast.UnionBranch).init(self.arena);
        var tag_col: ?[]const u8 = null;

        if (first.union_branch) |fb| {
            try branches.append(.{ .read = fb.read, .tag = fb.tag });
            tag_col = fb.tag_col;
        } else {
            try branches.append(try self.unionBranchFromCore(first));
        }

        while (self.eatKw("union")) {
            try self.expectKw("all");
            try self.expectKw("by");
            try self.expectKw("name");
            const core = try self.parseSelectCore();
            if (core.union_branch) |b| {
                if (b.tag_col) |tc| {
                    if (tag_col == null) tag_col = tc;
                    if (!std.mem.eql(u8, tag_col.?, tc))
                        return self.fail(self.curPos(), "all UNION branches must use the same tag column name (`{s}` vs `{s}`)", .{ tag_col.?, tc });
                }
                try branches.append(.{ .read = b.read, .tag = b.tag });
            } else {
                try branches.append(try self.unionBranchFromCore(core));
            }
        }

        var hints = std.array_list.Managed(ast.Hint).init(self.arena);
        if (tag_col) |tc|
            try hints.append(.{ .key = "tag", .value = .{ .ident = tc }, .pos = pos });
        try self.parseUnionClauses(&hints, pos);

        try stages.append(.{
            .node = .{ .union_ = .{ .branches = try branches.toOwnedSlice(), .pos = pos } },
            .hints = try hints.toOwnedSlice(),
            .pos = pos,
        });
    }

    /// A FROM source: CSV path, IDENTIFIER(<expr>) for a computed path,
    /// BODY(schema), HTTP('url'), a CTE reference, or a connection-qualified
    /// table / QUERY($$...$$). Registers the alias.
    /// `( <query> ) [AS] alias` in a FROM or JOIN position. Lowers to a binding — the
    /// same statement `WITH alias AS (...)` produces — and returns its name, so the
    /// caller can reference it as a `.ref` source or as a join's right side. No new
    /// execution machinery: a derived table *is* a CTE that happened to be written
    /// inline. An unaliased one gets a generated name that no identifier can collide
    /// with.
    fn parseDerivedTable(self: *Parser) Error![]const u8 {
        const dpos = self.curPos();
        _ = try self.expect(.lparen);
        var sub_stages = std.array_list.Managed(ast.Stage).init(self.arena);
        // Bindings the inner query creates — its own `WITH`, or a derived table nested
        // inside it — go to a local list, NOT straight to `pending_bindings`. Handing
        // `parseQuery` the same list it drains into made it append that list to itself
        // and clear it, losing every binding recorded so far: `FROM (SELECT .. FROM
        // (SELECT ..) a) b` failed with "unknown binding `a`".
        var inner_bindings = std.array_list.Managed(ast.Stmt).init(self.arena);
        try self.parseQuery(&inner_bindings, &sub_stages);
        _ = try self.expect(.rparen);

        _ = self.eatKw("as");
        var name: []const u8 = undefined;
        if (self.at(.ident) and !self.isKw("on") and !self.isKw("join") and !self.isKw("where") and
            !self.isKw("group") and !self.isKw("order") and !self.isKw("limit") and !self.isKw("having"))
        {
            name = self.advance().text;
        } else {
            self.derived_n += 1;
            name = try std.fmt.allocPrint(self.arena, "__derived{d}", .{self.derived_n});
        }

        try self.let_names.append(name);
        // Inner bindings first: they are what this one reads from.
        try self.pending_bindings.appendSlice(inner_bindings.items);
        try self.pending_bindings.append(.{ .binding = .{
            .name = name,
            .pipeline = .{ .stages = try sub_stages.toOwnedSlice(), .pos = dpos },
            .pos = dpos,
        } });
        return name;
    }

    /// `<fn>() OVER (PARTITION BY .. ORDER BY ..) [AS name]` as a complete select item.
    /// Returns false when the next tokens are not that, having consumed nothing.
    ///
    /// Stage one covers the ranking functions, which need no frame: a frame only means
    /// anything to an aggregate over a window, and `ROWS BETWEEN` syntax is deliberately
    /// absent until something asks for it. Every partition and order column must also be
    /// projected — the stage appends its columns to the projection rather than replacing it.
    fn parseWindowItem(
        self: *Parser,
        funcs: *std.array_list.Managed(ast.WindowFunc),
        part: *std.array_list.Managed(ast.QualName),
        ord: *std.array_list.Managed(ast.SortKey),
    ) Error!bool {
        if (!self.at(.ident)) return false;
        const kind: ast.WinKind = blk: {
            var buf: [16]u8 = undefined;
            const t = self.cur().text;
            if (t.len >= buf.len) return false;
            const low = std.ascii.lowerString(buf[0..t.len], t);
            if (std.mem.eql(u8, low, "row_number")) break :blk .row_number;
            if (std.mem.eql(u8, low, "rank")) break :blk .rank;
            if (std.mem.eql(u8, low, "dense_rank")) break :blk .dense_rank;
            if (std.mem.eql(u8, low, "lag")) break :blk .lag;
            if (std.mem.eql(u8, low, "lead")) break :blk .lead;
            if (std.mem.eql(u8, low, "sum")) break :blk .sum;
            if (std.mem.eql(u8, low, "count")) break :blk .count;
            return false;
        };
        // Only commit once the whole `name ( ) OVER` prefix is present, so a column
        // called `rank` still parses as a column.
        if (self.peekTag() != .lparen) return false;
        const save = self.i;
        _ = self.advance();
        _ = self.advance();
        var arg: ?ast.QualName = null;
        var offset: i64 = 1;
        if (kind == .sum or kind == .count) {
            // `COUNT(*)` counts rows; anything else needs a column. Rewind rather than
            // fail, so a plain `SUM(x)` with no OVER is still an ordinary aggregate.
            if (self.eat(.star)) {
                if (kind == .sum) {
                    self.i = save;
                    return false;
                }
            } else if (self.at(.ident)) {
                arg = try self.singleName(self.advance().text);
            } else {
                self.i = save;
                return false;
            }
        } else if (kind == .lag or kind == .lead) {
            if (!self.at(.ident)) {
                self.i = save;
                return false;
            }
            arg = try self.singleName(self.advance().text);
            if (self.eat(.comma)) {
                const n = try self.expect(.int);
                offset = std.fmt.parseInt(i64, n.text, 10) catch
                    return self.fail(self.curPos(), "LAG/LEAD offset must be an integer", .{});
                if (offset < 0)
                    return self.fail(self.curPos(), "LAG/LEAD offset must not be negative — use the other function", .{});
            }
        }
        if (!self.eat(.rparen) or !self.isKw("over")) {
            self.i = save;
            return false;
        }
        const wpos = self.curPos();
        _ = self.advance();
        _ = try self.expect(.lparen);

        // One window per query in stage one: two functions may share a window, but two
        // different windows would need a stage each.
        var saw_part = false;
        if (self.eatKw("partition")) {
            try self.expectKw("by");
            saw_part = true;
            while (true) {
                try part.append(try self.singleName(try self.expectIdent()));
                if (!self.eat(.comma)) break;
            }
        }
        var saw_ord = false;
        if (self.eatKw("order")) {
            try self.expectKw("by");
            saw_ord = true;
            while (true) {
                const f = try self.singleName(try self.expectIdent());
                var desc = false;
                if (self.eatKw("desc")) desc = true else _ = self.eatKw("asc");
                try ord.append(.{ .field = f, .desc = desc });
                if (!self.eat(.comma)) break;
            }
        }
        _ = try self.expect(.rparen);
        if (funcs.items.len > 0 and (saw_part or saw_ord))
            return self.fail(wpos, "two window functions in one SELECT must share the same OVER (...) window", .{});
        // Ranking and the offsets need an order to count along. An aggregate does not:
        // with no ORDER BY its frame is the whole partition, which is share-of-total.
        if (ord.items.len == 0 and kind != .sum and kind != .count)
            return self.fail(wpos, "a window function needs ORDER BY inside OVER (...) to number by", .{});

        _ = self.eatKw("as");
        const name = if (self.at(.ident)) self.advance().text else @tagName(kind);
        try funcs.append(.{ .kind = kind, .out = name, .arg = arg, .offset = offset });
        return true;
    }

    fn parseFromSource(self: *Parser, aliases: *AliasSet, read_hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
        var node: ast.Stage.Node = undefined;
        if (self.at(.lparen)) {
            const name = try self.parseDerivedTable();
            aliases.add(name);
            return .{ .ref = name };
        } else if (self.at(.string)) {
            node = .{ .read = .{ .connector = "csv", .form = .{ .path = self.advance().text } } };
        } else if (self.isKw("identifier") and self.peekTag() == .lparen) {
            const pos = self.curPos();
            _ = self.advance();
            _ = try self.expect(.lparen);
            const e = try self.parseExpr();
            _ = try self.expect(.rparen);
            const tmpl = try self.exprToTemplate(e);
            if (!hasLiteralExt(tmpl))
                return self.fail(pos, "dynamic path needs a literal extension (end it with `|| '.csv'`, `|| '.parquet'`, …)", .{});
            node = .{ .read = .{ .connector = "csv", .form = .{ .path = tmpl } } };
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
        } else if (self.isKw("buffer") and self.peekTag() == .string) {
            _ = self.advance();
            const name = self.advance().text;
            var ref = ast.BufferRef{ .name = name };
            if (self.eatKw("at")) ref.dir = (try self.expect(.string)).text;
            if (self.eatKw("flush")) {
                const fpos = self.curPos();
                try self.expectKw("every");
                const n = try self.expect(.int);
                try self.expectKw("seconds");
                const secs = std.fmt.parseInt(i64, n.text, 10) catch
                    return self.fail(fpos, "bad FLUSH EVERY seconds `{s}`", .{n.text});
                try read_hints.append(.{ .key = "flush_secs", .value = .{ .int = secs }, .pos = fpos });
                if (self.eatKw("or")) {
                    const r = try self.expect(.int);
                    try self.expectKw("rows");
                    const rows = std.fmt.parseInt(i64, r.text, 10) catch
                        return self.fail(fpos, "bad FLUSH rows `{s}`", .{r.text});
                    try read_hints.append(.{ .key = "flush_rows", .value = .{ .int = rows }, .pos = fpos });
                }
            }
            node = .{ .read = .{ .connector = "buffer", .form = .{ .buffer = ref } } };
        } else if (self.isKw("each")) {
            return self.parseEachTableOf(read_hints);
        } else if (self.isKw("range") and self.peekTag() == .lparen) {
            _ = self.advance();
            _ = try self.expect(.lparen);
            const a = try self.parseExpr();
            var lo: *ast.Expr = try self.mk(.{ .int_lit = 0 });
            var hi = a;
            if (self.eat(.comma)) {
                lo = a;
                hi = try self.parseExpr();
            }
            _ = try self.expect(.rparen);
            node = .{ .read = .{ .connector = "range", .form = .{ .range = .{ .lo = lo, .hi = hi } } } };
        } else {
            const pos = self.curPos();
            const head = try self.expectIdent();
            if (self.at(.dot)) {
                _ = self.advance();
                if (self.isKw("query") and self.peekTag() == .lparen) {
                    _ = self.advance();
                    _ = try self.expect(.lparen);
                    const q = try self.expect(.string);
                    _ = try self.expect(.rparen);
                    node = .{ .read = .{ .connector = head, .form = .{ .query = q.text } } };
                } else if (self.at(.string)) {
                    node = .{ .read = .{ .connector = head, .form = .{ .path = self.advance().text } } };
                } else {
                    var parts = std.array_list.Managed([]const u8).init(self.arena);
                    try parts.append(try self.parseNameSegment());
                    while (self.at(.dot) and (self.peekTag() == .ident or self.peekTag() == .qident)) {
                        _ = self.advance();
                        try parts.append(try self.parseNameSegment());
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

    /// A parenthesized discovery sub-query: a full basalt SELECT pipeline with
    /// no trailing write, planned and executed in-engine by the runtime. CTEs
    /// are rejected — `parseQuery` hoists those into top-level bindings, and a
    /// nested query has nowhere to put them.
    fn parseSubQuery(self: *Parser, pos: Pos) Error!ast.Pipeline {
        var hoisted = std.array_list.Managed(ast.Stmt).init(self.arena);
        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        try self.parseQuery(&hoisted, &stages);
        if (hoisted.items.len > 0)
            return self.fail(pos, "a discovery sub-query may not declare CTEs", .{});
        return .{ .stages = try stages.toOwnedSlice(), .pos = pos };
    }

    /// `EACH TABLE OF (SELECT ... | <conn>.QUERY($$...$$) | $param.path
    ///  | '<json>' IN <conn>) [AS (table_name, <tag_col>)] [ANCHOR SCHEMA qual]`
    /// — the discovered / json union forms. One row (or array element) per
    /// branch; the second AS name is the output tag column. The SELECT form is
    /// a full basalt query planned and run in-engine.
    fn parseEachTableOf(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
        const pos = self.curPos();
        try self.expectKw("each");
        try self.expectKw("table");
        try self.expectKw("of");
        _ = try self.expect(.lparen);

        var u = ast.Union{ .pos = pos };
        if (self.at(.dollar_ident)) {
            const q = try self.parseDollarPath();
            const joined = try std.mem.join(self.arena, ".", q.parts);
            u.discover_json = try std.fmt.allocPrint(self.arena, "${{{s}}}", .{joined});
        } else if (self.at(.string)) {
            u.discover_json = self.advance().text;
        } else if (self.isKw("select")) {
            const pipe = try self.parseSubQuery(pos);
            u.discover_pipeline = pipe;
            // The discovered tables live on whatever connection the discovery
            // query itself reads from, unless a trailing `IN <conn>` says
            // otherwise.
            if (pipe.stages.len > 0 and pipe.stages[0].node == .read) {
                const c = pipe.stages[0].node.read.connector;
                if (self.isConn(c)) u.discover_conn = c;
            }
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

        if (u.discover_json.len > 0 or (u.discover_pipeline != null and self.isKw("in"))) {
            try self.expectKw("in");
            const conn = try self.expectIdent();
            if (!self.isConn(conn))
                return self.fail(pos, "unknown connection `{s}` in EACH TABLE OF ... IN", .{conn});
            u.discover_conn = conn;
        }
        if (u.discover_pipeline != null and u.discover_conn.len == 0)
            return self.fail(pos, "EACH TABLE OF (SELECT ...): add `IN <conn>` to say where the discovered tables live", .{});

        if (self.eatKw("as")) {
            _ = try self.expect(.lparen);
            _ = try self.expectIdent();
            if (self.eat(.comma)) {
                const tag_col = try self.expectIdent();
                try hints.append(.{ .key = "tag", .value = .{ .ident = tag_col }, .pos = pos });
            }
            _ = try self.expect(.rparen);
        }
        try self.parseUnionClauses(hints, pos);
        return .{ .union_ = u };
    }

    /// `PAGINATE BY page|offset|cursor (key = value, ...)` -> HTTP source hints.
    fn parsePaginate(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
        const pos = self.curPos();
        try self.expectKw("paginate");
        try self.expectKw("by");
        const mode = try self.expectIdent();
        const is_cursor = eqlNoCase(mode, "cursor");
        // http.zig reads the mode off a `paginate` hint; a bare flag key
        // planned fine but left paginate=.none, so only one page was fetched.
        if (eqlNoCase(mode, "page") or eqlNoCase(mode, "offset") or is_cursor) {
            try hints.append(.{ .key = "paginate", .value = .{ .ident = mode }, .pos = pos });
        } else {
            return self.fail(pos, "PAGINATE BY expects page, offset, or cursor (got `{s}`)", .{mode});
        }
        if (self.eat(.lparen)) {
            while (!self.at(.rparen)) {
                const kpos = self.curPos();
                const key = try self.expectIdent();
                _ = try self.expect(.assign);
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
                    key;
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
            source = .{ .read = .{ .connector = "csv", .form = .{ .path = self.advance().text } } };
        } else if (self.isKw("select")) {
            source = .{ .pipeline = try self.parseSubQuery(pos) };
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

        // The loop variables are script-scope constants inside the body, exactly as
        // a PARAM is: `$name` resolves per row before a row is read. Registering them
        // is what lets one sit beside an aggregate (`SELECT $tabela, COUNT(*)`),
        // which was refused while the same shape with a PARAM was allowed.
        const const_base = self.const_names.items.len;
        for (names.items) |n| try self.const_names.append(n);

        var body = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.eof) and !self.isKw("end")) {
            try self.parseStatement(&body);
        }
        // Scoped to the body: outside it the name is an ordinary column again.
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

    /// The one shape a join key may take. Kept deliberately narrow: the index is
    /// built on stored columns, so a computed key has to be computed first.
    fn joinKeyFail(self: *Parser) Error {
        return self.fail(self.curPos(), "join keys must be plain columns; compute them in the CTE / a select first", .{});
    }

    fn parseJoinKey(self: *Parser) Error!ast.QualName {
        if (!(self.atName() or self.at(.string))) return self.joinKeyFail();
        return self.parseQualNameTok();
    }

    fn parseQualNameTok(self: *Parser) Error!ast.QualName {
        var parts = std.array_list.Managed([]const u8).init(self.arena);
        try parts.append(try self.expectColName());
        while (self.at(.dot) and (self.peekTag() == .ident or self.peekTag() == .qident or self.peekTag() == .string)) {
            _ = self.advance();
            try parts.append(try self.expectColName());
        }
        return .{ .parts = try parts.toOwnedSlice() };
    }

    /// Rewrite `alias.x` -> `x` in an expression tree.
    /// Every column an expression reads. Used to keep a pre-aggregation
    /// projection from dropping inputs the aggregates still need.
    fn collectFields(self: *Parser, e: *const ast.Expr, out: *std.array_list.Managed(ast.QualName)) Error!void {
        switch (e.*) {
            .field => |q| out.append(q) catch return error.OutOfMemory,
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
            else => {},
        }
    }

    /// Render an expression exactly as `synthName` renders its source tokens,
    /// so a HAVING term can be matched to the SELECT item that computed it.
    fn exprKey(self: *Parser, e: *const ast.Expr, buf: *std.array_list.Managed(u8)) Error!void {
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
            else => buf.append('?') catch return error.OutOfMemory,
        }
    }

    /// Whether an aggregate call appears anywhere inside an expression. A
    /// SELECT item that is one outright is already handled; this finds the ones
    /// buried in arithmetic or a scalar call, like `round(avg(x), 2)`.
    fn containsAgg(e: *const ast.Expr) bool {
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

    /// Point the post-aggregate projection's entry for `name` at `expr` instead of
    /// at a column of that name. A computed item normally lands in the pre-aggregate
    /// projection and is referenced by name afterwards; when the item is lifted out
    /// of the pre-projection entirely, the reference has nothing to read and the
    /// expression has to be evaluated here instead.
    fn repointPostItem(
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
        // Every computed item appends its own post entry, so this is unreachable
        // in practice; append rather than lose the column if that ever changes.
        try post.append(.{ .computed = .{ .name = name, .expr = expr } });
        _ = self;
    }

    /// Whether a SELECT item is one value for the whole query, and so may sit in a
    /// grouped or ungrouped aggregate's select list without being an aggregate or a
    /// grouping key. That covers literals, arithmetic and string building over them,
    /// `now()`-style calls, and the PARAMs and LETs — all folded before a row is
    /// read. A column reference is not constant: it has no single value per group,
    /// which is the case the caller still refuses.
    ///
    /// `$p` and a bare column are the same single-part field here, so a declared
    /// PARAM or LET name wins — the same shadowing rule the rest of the language
    /// applies to `$name`.
    fn constItemExpr(self: *Parser, e: *const ast.Expr) bool {
        return switch (e.*) {
            .int_lit, .float_lit, .str_lit, .bool_lit, .null_lit => true,
            .field => |q| q.parts.len == 1 and self.isScriptConst(q.parts[0]),
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

    /// Lifts the aggregate calls out of a scalar expression, the same swap
    /// `havingRewrite` performs: each call becomes a column the aggregate stage
    /// produces, and what surrounds it becomes an ordinary projection over
    /// those columns. That is what lets `round(avg(x), 2)` be written at all —
    /// the aggregate itself is unchanged, only where the arithmetic runs.
    ///
    /// Naming goes through `exprKey`, so two mentions of the same aggregate
    /// collapse to one column, and a HAVING or ORDER BY naming that aggregate
    /// still binds to it.
    fn liftAggs(
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
                        // Through the alias map, so `sum(v) as s` and a later
                        // `sum(v) * 2` resolve to the one column named `s`
                        // rather than computing the sum twice.
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

    /// HAVING may filter on an aggregate the SELECT list never asked for
    /// (`... GROUP BY g HAVING COUNT(*) > 1` selecting only `g` and an average).
    /// That aggregate still has to be computed, so it is added as an ordinary
    /// output column and the post-aggregate projection drops it again — the
    /// same projection lifting installs. Returns whether anything was added.
    fn addHavingAggs(
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

    /// A one-part `QualName`, for referring to a column the previous stage named.
    fn singleName(self: *Parser, name: []const u8) Error!ast.QualName {
        const parts = self.arena.alloc([]const u8, 1) catch return error.OutOfMemory;
        parts[0] = name;
        return .{ .parts = parts };
    }

    /// HAVING runs after aggregation, so each aggregate call in it is really a
    /// reference to a column the aggregate already produced. Swap the calls for
    /// those columns and the clause becomes an ordinary filter stage.
    fn havingRewrite(self: *Parser, e: *ast.Expr, map: []const ExprAlias) Error!*ast.Expr {
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

    fn parseExpr(self: *Parser) Error!*ast.Expr {
        return self.parseBin(0);
    }

    const BinInfo = struct { op: ast.BinOp, lbp: u8 };

    /// Binding powers, low to high: `or` 10, `and` 20, unary `not` 25, `??` 30,
    /// comparisons 40, then the bitwise ladder `|` 42 / `^` 44 / `&` 46 /
    /// shifts 48, then `+ - ||` 50 and `* / %` 60. So `flags & 4 = 4` is a
    /// comparison of the AND, and `1 | 2 & 3` is `1 | (2 & 3)`.
    fn binInfo(self: *Parser) ?BinInfo {
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

    fn parseBin(self: *Parser, min_bp: u8) Error!*ast.Expr {
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
                _ = self.advance();
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
            if ((self.isKw("between") or (self.isKw("not") and self.peekKw("between"))) and min_bp < 40) {
                const negated = self.isKw("not");
                if (negated) _ = self.advance();
                _ = self.advance();
                // Bounds are parsed above `AND`'s binding power, since BETWEEN
                // uses AND as its own separator — otherwise the low bound would
                // swallow `AND <hi>` as a boolean operand.
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
        return lhs;
    }

    fn parseUnary(self: *Parser) Error!*ast.Expr {
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
            // A quoted name is only ever a column — no keyword, literal or
            // function-call reading applies, which is the whole point of quoting.
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
                if (eqlNoCase(t.text, "extract") and self.peekTag() == .lparen) {
                    // `EXTRACT(minute FROM ts)` — SQL spells this argument list
                    // with a keyword instead of a comma; normalise it to the
                    // ordinary two-argument call the evaluator knows.
                    _ = self.advance();
                    _ = self.advance();
                    const unit = try self.expectColName();
                    // Both spellings: `EXTRACT(minute FROM ts)` and the plain
                    // two-argument call `extract('minute', ts)`.
                    if (!self.eatKw("from")) _ = try self.expect(.comma);
                    const src = try self.parseExpr();
                    _ = try self.expect(.rparen);
                    const xargs = try self.arena.alloc(*ast.Expr, 2);
                    xargs[0] = try self.mk(.{ .str_lit = unit });
                    xargs[1] = src;
                    return self.mk(.{ .call = .{ .name = "extract", .args = xargs } });
                }
                if ((eqlNoCase(t.text, "cast") or eqlNoCase(t.text, "try_cast")) and self.peekTag() == .lparen) {
                    // `TRY_CAST` is `CAST` with the failure mode flipped: same
                    // syntax, same target types, null instead of an error.
                    const safe = eqlNoCase(t.text, "try_cast");
                    _ = self.advance();
                    _ = self.advance();
                    const e = try self.parseExpr();
                    try self.expectKw("as");
                    const ty = try self.parseTypeName();
                    _ = try self.expect(.rparen);
                    return self.mk(.{ .cast = .{ .e = e, .ty = ty, .safe = safe } });
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
                            try args.append(try self.parseExpr());
                            while (self.eat(.comma)) try args.append(try self.parseExpr());
                        }
                    }
                    _ = try self.expect(.rparen);
                    const lower = try std.ascii.allocLowerString(self.arena, t.text);
                    return self.mk(.{ .call = .{ .name = lower, .args = try args.toOwnedSlice(), .distinct = call_distinct } });
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

fn binOpText(op: ast.BinOp) []const u8 {
    return switch (op) {
        .add => "+",   .sub => "-",  .mul => "*",  .div => "/",  .mod => "%",
        .eq => "==",   .ne => "!=",  .lt => "<",   .le => "<=",  .gt => ">",
        .ge => ">=",   .@"and" => "and", .@"or" => "or",
        .bit_and => "&", .bit_or => "|", .bit_xor => "^", .shl => "<<", .shr => ">>",
    };
}

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

const testing = std.testing;

fn parseTest(a: std.mem.Allocator, src: []const u8) !ast.Program {
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parseSource(a, src, &diag) catch |e| {
        if (e == error.ParseFailed) std.debug.print("parse error {d}:{d}: {s}\n", .{ diag.line, diag.col, diag.msg });
        return e;
    };
}

test "sql expr: bitwise precedence slots between comparisons and additive" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    // `1 | 2 & 3` is `1 | (2 & 3)`
    const or_e = try parseExprStr(a, "1 | 2 & 3", &diag);
    try testing.expectEqual(ast.BinOp.bit_or, or_e.binary.op);
    try testing.expectEqual(@as(i64, 1), or_e.binary.l.int_lit);
    try testing.expectEqual(ast.BinOp.bit_and, or_e.binary.r.binary.op);

    // `flags & 4 = 4` is a comparison of the AND
    const cmp = try parseExprStr(a, "flags & 4 = 4", &diag);
    try testing.expectEqual(ast.BinOp.eq, cmp.binary.op);
    try testing.expectEqual(ast.BinOp.bit_and, cmp.binary.l.binary.op);

    // `1 + 1 << 2` is `(1 + 1) << 2`
    const sh = try parseExprStr(a, "1 + 1 << 2", &diag);
    try testing.expectEqual(ast.BinOp.shl, sh.binary.op);
    try testing.expectEqual(ast.BinOp.add, sh.binary.l.binary.op);

    // `a ^ b | c` is `(a ^ b) | c`; `2 * 3 & 1` is `(2 * 3) & 1`
    const xo = try parseExprStr(a, "a ^ b | c", &diag);
    try testing.expectEqual(ast.BinOp.bit_or, xo.binary.op);
    try testing.expectEqual(ast.BinOp.bit_xor, xo.binary.l.binary.op);
    const ml = try parseExprStr(a, "2 * 3 & 1", &diag);
    try testing.expectEqual(ast.BinOp.bit_and, ml.binary.op);
    try testing.expectEqual(ast.BinOp.mul, ml.binary.l.binary.op);

    // `~` binds like the other unaries; `and` is looser than every bitwise op
    const bn = try parseExprStr(a, "~x & 1", &diag);
    try testing.expectEqual(ast.BinOp.bit_and, bn.binary.op);
    try testing.expectEqual(ast.UnOp.bit_not, bn.binary.l.unary.op);
    const an = try parseExprStr(a, "x & 1 and y", &diag);
    try testing.expectEqual(ast.BinOp.@"and", an.binary.op);
    try testing.expectEqual(ast.BinOp.bit_and, an.binary.l.binary.op);

    // `||` is still concat, not two bitwise ORs
    const cc = try parseExprStr(a, "a || b", &diag);
    try testing.expectEqualStrings("concat", cc.call.name);

    try testing.expectEqualStrings("&", binOpText(.bit_and));
    try testing.expectEqualStrings("<<", binOpText(.shl));
    try testing.expectEqualStrings(">>", binOpText(.shr));
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
    try testing.expectEqual(@as(usize, 5), pl.stages.len);
    try testing.expect(pl.stages[0].node == .read);
    try testing.expect(pl.stages[1].node == .filter);
    try testing.expect(pl.stages[2].node == .select);
    try testing.expect(pl.stages[3].node == .limit);
    try testing.expect(pl.stages[4].node == .write);
    try testing.expectEqualStrings("stdout", pl.stages[4].node.write.connector);
}

test "sql: EXPLAIN is a statement anywhere in a script, not only the first" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'h', database = 'erp');
        \\EXPLAIN WITH paid AS (SELECT id, amount FROM 'in.csv' WHERE status = 'paid')
        \\SELECT id FROM paid;
    );
    // Not the first token, so the program-level prefix stays untouched.
    try testing.expectEqual(ast.ExplainMode.none, prog.explain);
    try testing.expectEqual(@as(usize, 4), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .connection);
    // The CTE stays an ordinary binding; only the pipeline it feeds is explained.
    try testing.expect(prog.stmts[2] == .binding);
    try testing.expectEqualStrings("paid", prog.stmts[2].binding.name);
    try testing.expect(prog.stmts[3] == .explain);
    const ex = prog.stmts[3].explain;
    try testing.expectEqual(ast.ExplainMode.plan, ex.mode);
    const last = ex.pipeline.stages[ex.pipeline.stages.len - 1];
    try testing.expect(last.node == .write);
    try testing.expectEqualStrings("stdout", last.node.write.connector);
}

test "sql: EXPLAIN ANALYZE and EXPLAIN LOAD INTO as later statements" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\SELECT id FROM 'in.csv';
        \\EXPLAIN ANALYZE SELECT id FROM 'in.csv';
        \\EXPLAIN LOAD INTO 'out.csv' AS SELECT id FROM 'in.csv';
    );
    try testing.expectEqual(ast.ExplainMode.none, prog.explain);
    try testing.expectEqual(@as(usize, 4), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .output);
    try testing.expect(prog.stmts[2] == .explain);
    try testing.expectEqual(ast.ExplainMode.analyze, prog.stmts[2].explain.mode);
    try testing.expect(prog.stmts[3] == .explain);
    try testing.expectEqual(ast.ExplainMode.plan, prog.stmts[3].explain.mode);
    const w = prog.stmts[3].explain.pipeline.stages[prog.stmts[3].explain.pipeline.stages.len - 1];
    try testing.expectEqualStrings("csv", w.node.write.connector);
    try testing.expectEqualStrings("out.csv", w.node.write.target);
}

test "sql: EXPLAIN COSTS is rejected in either position" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "EXPLAIN COSTS SELECT id FROM 'in.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no cost model") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\SELECT id FROM 'in.csv';
        \\EXPLAIN COSTS SELECT id FROM 'in.csv';
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no cost model") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\SELECT id FROM 'in.csv';
        \\EXPLAIN CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'h', database = 'erp');
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "after EXPLAIN") != null);
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
    try testing.expectEqual(@as(usize, 3), pl.stages.len);
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
    try testing.expectEqual(@as(usize, 3), prog.stmts.len);
    try testing.expect(prog.stmts[1] == .binding);
    try testing.expectEqualStrings("paid", prog.stmts[1].binding.name);
    const pl = prog.stmts[2].output;
    try testing.expect(pl.stages[1].node == .join);
    const j = pl.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.left, j.kind);
    try testing.expectEqualStrings("paid", j.binding);
    try testing.expectEqual(@as(usize, 1), j.left_keys.len);
    try testing.expectEqualStrings("id", j.left_keys[0].parts[0]);
    try testing.expectEqualStrings("id", j.right_keys[0].parts[0]);
    const sel = pl.stages[2].node.select;
    try testing.expectEqualStrings("id", sel[0].field.parts[0]);
    try testing.expectEqual(@as(usize, 1), sel[0].field.parts.len);
}

test "sql: multi-key ON, RIGHT/FULL/CROSS join kinds, and the plain-column rule" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id, day, v FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t FULL OUTER JOIN r ON t.id = r.id AND r.day = t.day;
    );
    const j = prog.stmts[2].output.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.full, j.kind);
    try testing.expectEqual(@as(usize, 2), j.left_keys.len);
    try testing.expectEqualStrings("id", j.left_keys[0].parts[0]);
    try testing.expectEqualStrings("id", j.right_keys[0].parts[0]);
    // Written `r.day = t.day`, so the pair has to be flipped back.
    try testing.expectEqualStrings("day", j.left_keys[1].parts[0]);
    try testing.expectEqualStrings("day", j.right_keys[1].parts[0]);

    const rp = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t RIGHT JOIN r ON t.id = r.id;
    );
    try testing.expectEqual(ast.JoinKind.right, rp.stmts[2].output.stages[1].node.join.kind);

    const cp = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t CROSS JOIN r;
    );
    const cj = cp.stmts[2].output.stages[1].node.join;
    try testing.expectEqual(ast.JoinKind.cross, cj.kind);
    try testing.expectEqual(@as(usize, 0), cj.left_keys.len);

    // CROSS JOIN UNNEST still expands rows rather than pairing them.
    const up = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM 'in.csv' CROSS JOIN UNNEST(tags) AS tag;
    );
    try testing.expect(up.stmts[1].output.stages[1].node == .explode);

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t JOIN r ON lower(t.id) = r.id;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "plain columns") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t CROSS JOIN r ON t.id = r.id;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "no ON clause") != null);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t JOIN r;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "expected `ON") != null);
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
    try testing.expectEqual(@as(usize, 2), pl.stages.len);
    const u = pl.stages[0].node.union_;
    try testing.expectEqual(@as(usize, 2), u.branches.len);
    try testing.expectEqualStrings("01", u.branches[0].tag.?);
    try testing.expectEqualStrings("02", u.branches[1].tag.?);
    try testing.expectEqualStrings("tag", pl.stages[0].hints[0].key);
    try testing.expectEqualStrings("CT2_EMPRESA", pl.stages[0].hints[0].value.ident);
    try testing.expectEqualStrings("canon", pl.stages[0].hints[1].key);
    try testing.expectEqualStrings("CT2010", pl.stages[0].hints[1].value.ident);
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
    const arm1 = m.arms[0].body[0].output;
    try testing.expectEqual(@as(usize, 2), arm1.stages.len);
    const arm2 = m.arms[1].body[0].output;
    try testing.expectEqual(@as(usize, 3), arm2.stages.len);
    try testing.expectEqualStrings("crm_${lower(name)}", arm2.stages[2].node.write.target);
}

test "sql: EACH TABLE OF (SELECT ...) parses an in-engine discovery pipeline" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION cat TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM EACH TABLE OF (
        \\  SELECT name, substr(name, 4, 2) AS emp FROM cat.tables WHERE name LIKE 'CT2%'
        \\) AS (table_name, emp);
    );
    const pl = prog.stmts[2].output;
    const u = pl.stages[0].node.union_;
    try testing.expectEqual(@as(usize, 0), u.discover_query.len);
    try testing.expectEqual(@as(usize, 0), u.discover_json.len);
    // The branch connection is inferred from the discovery query's own source.
    try testing.expectEqualStrings("cat", u.discover_conn);
    const disc = u.discover_pipeline orelse return error.TestExpectedPipeline;
    try testing.expect(disc.stages[0].node == .read);
    try testing.expectEqualStrings("cat", disc.stages[0].node.read.connector);
    try testing.expect(disc.stages[1].node == .filter);
    try testing.expect(disc.stages[2].node == .select);
    try testing.expectEqual(@as(usize, 2), disc.stages[2].node.select.len);
    try testing.expectEqualStrings("emp", pl.stages[0].hints[0].value.ident);
}

test "sql: EACH TABLE OF (SELECT ...) over a non-connection source needs IN <conn>" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM EACH TABLE OF (SELECT name, emp FROM 'cat.csv') AS (table_name, emp);
    , &diag));

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO 'out.csv' AS
        \\SELECT * FROM EACH TABLE OF (SELECT name, emp FROM 'cat.csv') IN erp AS (table_name, emp);
    );
    const u = prog.stmts[2].output.stages[0].node.union_;
    try testing.expectEqualStrings("erp", u.discover_conn);
    try testing.expect(u.discover_pipeline != null);
}

test "sql: FOR EACH ROW OF (SELECT ...) parses an in-engine discovery pipeline" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\FOR EACH ROW OF (SELECT name, lower(name) AS slug FROM 'catalog.csv' WHERE active = '1') AS (name, slug)
        \\  LOAD INTO 'out_${slug}.csv' AS SELECT * FROM '${slug}.csv';
        \\END FOR;
    );
    const fe = prog.stmts[1].for_each;
    try testing.expectEqual(@as(usize, 2), fe.var_names.len);
    try testing.expect(fe.source == .pipeline);
    const disc = fe.source.pipeline;
    try testing.expectEqual(@as(usize, 3), disc.stages.len);
    try testing.expect(disc.stages[0].node == .read);
    try testing.expectEqualStrings("csv", disc.stages[0].node.read.connector);
    try testing.expect(disc.stages[1].node == .filter);
    try testing.expectEqualStrings("slug", disc.stages[2].node.select[1].computed.name);
    try testing.expectEqual(@as(usize, 1), fe.body.len);
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

test "sql: ACCEPT INTO BUFFER declaration and FROM BUFFER source" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE ENDPOINT '/eventos' DOC 'telemetria'
        \\  ACCEPT BODY (device_id STRING NOT NULL, v INT)
        \\  INTO BUFFER 'ev' AT '/var/lib/basalt/wal' SEGMENT 16 MB RETAIN 24 HOURS;
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\LOAD INTO sr.eventos USING stream_load AS
        \\SELECT device_id, CAST(v AS INT) AS v
        \\FROM BUFFER 'ev' FLUSH EVERY 5 SECONDS OR 50000 ROWS
        \\WHERE device_id IS NOT NULL;
    );
    const kd = prog.stmts[0].kind;
    try testing.expect(kd.kind == .http);
    const buf = kd.buffer.?;
    try testing.expectEqualStrings("ev", buf.name);
    try testing.expectEqualStrings("/var/lib/basalt/wal", buf.dir);
    try testing.expectEqual(@as(u64, 16 << 20), buf.segment_bytes);
    try testing.expectEqual(@as(u32, 24), buf.retain_hours.?);
    try testing.expectEqual(@as(usize, 2), buf.schema.len);
    try testing.expect(buf.schema[0].not_null);

    const pl = prog.stmts[2].output;
    const rd = pl.stages[0].node.read;
    try testing.expectEqualStrings("buffer", rd.connector);
    try testing.expectEqualStrings("ev", rd.form.buffer.name);
    try testing.expectEqualStrings("", rd.form.buffer.dir);
    try testing.expectEqualStrings("flush_secs", pl.stages[0].hints[0].key);
    try testing.expectEqual(@as(i64, 5), pl.stages[0].hints[0].value.int);
    try testing.expectEqualStrings("flush_rows", pl.stages[0].hints[1].key);
    try testing.expectEqual(@as(i64, 50000), pl.stages[0].hints[1].value.int);
    try testing.expect(pl.stages[1].node == .filter);
}

test "sql: reflection lowering — IDENTIFIER / || / PUSHDOWN(expr) -> ${...} templates" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\PARAM tables JSON;
        \\CREATE CONNECTION fluig TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\FOR EACH ROW OF ($tables) AS (name, where)
        \\  PARALLEL ON ERROR CONTINUE
        \\  LOAD INTO sr.IDENTIFIER('fluig_' || lower($name))
        \\    USING stream_load UPSERT AS
        \\  SELECT *, now() AS extraction_timestamp
        \\  FROM fluig.dbo.IDENTIFIER($name)
        \\  PUSHDOWN($where);
        \\END FOR;
    );
    const fe = prog.stmts[4].for_each;
    const body = fe.body[0].output;
    const rd = body.stages[0].node.read;
    try testing.expectEqualStrings("dbo", rd.form.table.parts[0]);
    try testing.expectEqualStrings("${name}", rd.form.table.parts[1]);
    try testing.expectEqualStrings("where", body.stages[0].hints[0].key);
    try testing.expectEqualStrings("${where}", body.stages[0].hints[0].value.str);
    const w = body.stages[body.stages.len - 1].node.write;
    try testing.expectEqualStrings("fluig_${lower(name)}", w.target);
    try testing.expect(w.mode == .upsert);
    try testing.expectEqual(@as(usize, 0), w.mode.upsert.keys.len);
}

test "sql: PUSHDOWN($$literal$$) still lowers to a plain fragment (no hole)" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\LOAD INTO '/tmp/x.csv' AS
        \\SELECT filial FROM erp.dbo.T PUSHDOWN($$D_E_L_E_T_ <> '*'$$);
    );
    const st = prog.stmts[2].output.stages[0];
    try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", st.hints[0].value.str);
}

test "sql: FROM IDENTIFIER(expr) -> a computed path read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\LOAD INTO '/tmp/o.csv' AS
        \\SELECT * FROM IDENTIFIER('dir/' || name || '.csv');
    );
    const rd = prog.stmts[1].output.stages[0].node.read;
    try testing.expectEqualStrings("csv", rd.connector);
    try testing.expectEqualStrings("dir/${name}.csv", rd.form.path);
}

test "sql: FROM IDENTIFIER without a literal extension is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "LOAD INTO '/tmp/o.csv' AS SELECT * FROM IDENTIFIER('dir/' || name);", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expect(std.mem.indexOf(u8, diag.msg, "literal extension") != null);
}

test "sql: LOAD INTO IDENTIFIER(expr) -> a computed path write" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\LOAD INTO IDENTIFIER('dir/' || name || '.csv') AS
        \\SELECT * FROM 'in.csv';
    );
    const stages = prog.stmts[1].output.stages;
    const w = stages[stages.len - 1].node.write;
    try testing.expectEqualStrings("csv", w.connector);
    try testing.expectEqualStrings("dir/${name}.csv", w.target);
}

test "sql: LOAD INTO IDENTIFIER without a literal extension is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    // The extension picks the writer and decides which dispositions are legal,
    // so it has to be known before any row is read.
    const r = parseSource(a, "LOAD INTO IDENTIFIER('dir/' || name) AS SELECT * FROM 'in.csv';", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expect(std.mem.indexOf(u8, diag.msg, "literal extension") != null);
}

test "sql: IDENTIFIER in an UPSERT key -> per-row computed key column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\PARAM tables JSON;
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\FOR EACH ROW OF ($tables) AS (name, pk)
        \\  LOAD INTO sr.T
        \\    USING stream_load
        \\    UPSERT ON (IDENTIFIER(if($pk = '', $name || 'id', $pk))) AS
        \\  SELECT * FROM 'x.csv';
        \\END FOR;
    );
    const w = prog.stmts[3].for_each.body[0].output.stages[1].node.write;
    try testing.expectEqual(@as(usize, 1), w.mode.upsert.keys.len);
    try testing.expectEqualStrings("${if((pk == ''), concat(name, 'id'), pk)}", w.mode.upsert.keys[0]);
}

test "sql: CREATE FUNCTION — expression, typed/DEFAULT params, statement body, CALL" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE FUNCTION margin(rev FLOAT, cost FLOAT) AS (rev - cost) / rev;
        \\CREATE OR REPLACE FUNCTION round_to(x, n INT DEFAULT 2) AS round(x, n);
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (fe_host = 'h', database = 'b');
        \\CREATE FUNCTION sync_table(name, filter) AS
        \\  LOAD INTO sr.IDENTIFIER('bronze_' || lower($name)) USING stream_load UPSERT AS
        \\  SELECT * FROM erp.dbo.IDENTIFIER($name) PUSHDOWN($filter);
        \\END;
        \\CALL sync_table('SC5010', $$D_E_L_E_T_ <> '*'$$);
    );

    const margin = prog.stmts[1].func;
    try testing.expect(margin.body == .expr);
    try testing.expect(!margin.replace);
    try testing.expectEqual(@as(usize, 2), margin.params.len);
    try testing.expectEqualStrings("rev", margin.params[0].name);
    try testing.expectEqual(types.TypeKind.float, margin.params[0].ty.?.kind);
    try testing.expect(margin.params[0].default == null);

    const round_to = prog.stmts[2].func;
    try testing.expect(round_to.replace);
    try testing.expect(round_to.params[0].ty == null);
    try testing.expectEqual(types.TypeKind.int, round_to.params[1].ty.?.kind);
    try testing.expectEqual(@as(i64, 2), round_to.params[1].default.?.int_lit);

    const sync = prog.stmts[5].func;
    try testing.expect(sync.body == .stmts);
    try testing.expectEqual(@as(usize, 1), sync.body.stmts.len);
    const pipe = sync.body.stmts[0].output;
    try testing.expectEqualStrings("${name}", pipe.stages[0].node.read.form.table.parts[1]);
    try testing.expectEqualStrings("${filter}", pipe.stages[0].hints[0].value.str);
    try testing.expectEqualStrings("bronze_${lower(name)}", pipe.stages[pipe.stages.len - 1].node.write.target);

    const call = prog.stmts[6].call;
    try testing.expectEqualStrings("sync_table", call.name);
    try testing.expectEqual(@as(usize, 2), call.args.len);
    try testing.expectEqualStrings("SC5010", call.args[0].str_lit);
    try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", call.args[1].str_lit);
}

test "sql: CREATE FUNCTION AS CASE stays the expression form" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\CREATE FUNCTION grade(n) AS CASE WHEN n > 90 THEN 'a' ELSE 'b' END;
        \\SELECT grade(score) AS g FROM 'x.csv';
    );
    try testing.expect(prog.stmts[1].func.body == .expr);
    try testing.expect(prog.stmts[1].func.body.expr.* == .match);
}

test "sql: THROW — bare, WHEN-guarded, and inside a CASE arm" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\PARAM tbl STRING DEFAULT '';
        \\THROW 'tbl is required (e.g. -p tbl=SC5)' WHEN $tbl IS EMPTY;
        \\THROW 'unreachable branch';
        \\CASE WHEN $tbl = 'zz' THEN THROW 'no zz allowed'; END CASE;
        \\SELECT 1 AS n FROM 'x.csv';
    );

    const guarded = prog.stmts[2].throw;
    try testing.expectEqualStrings("tbl is required (e.g. -p tbl=SC5)", guarded.message.str_lit);
    try testing.expect(guarded.when.?.* == .is_null);
    try testing.expectEqual(ast.Expr.NullTest.is_empty, guarded.when.?.is_null.kind);
    try testing.expect(!guarded.when.?.is_null.negated);

    const bare = prog.stmts[3].throw;
    try testing.expectEqualStrings("unreachable branch", bare.message.str_lit);
    try testing.expect(bare.when == null);

    const armed = prog.stmts[4].match.arms[0].body[0].throw;
    try testing.expectEqualStrings("no zz allowed", armed.message.str_lit);
    try testing.expect(armed.when == null);
}

test "sql: a non-DEFAULT parameter may not follow a defaulted one" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a,
        \\CREATE FUNCTION f(a INT DEFAULT 1, b INT) AS a + b;
        \\SELECT f(1) AS v FROM 'x.csv';
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "without DEFAULT") != null);
}

fn firstPipelineStages(prog: ast.Program) []const ast.Stage {
    for (prog.stmts) |s| {
        if (s == .output) return s.output.stages;
    }
    unreachable;
}

/// Index of the aggregate stage. A pipeline ends in a write, so counting back
/// from the end finds that instead.
fn aggStageIndex(st: []const ast.Stage) usize {
    for (st, 0..) |s, i| {
        if (s.node == .aggregate) return i;
    }
    unreachable;
}

// `round(avg(x), 2)` is ordinary SQL that basalt rejected: an item that was not
// itself an aggregate call fell through to the group-key rule, whose message
// named a GROUP BY the query did not have. The calls now move to the aggregate
// stage and the arithmetic around them becomes a projection over its output.
test "sql: an aggregate inside a scalar expression lifts to the aggregate stage" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT g, ROUND(AVG(v), 2) AS m FROM 'x.csv' GROUP BY g;");
    const st = firstPipelineStages(prog);

    // read -> aggregate -> select, the projection being what evaluates round()
    const ai = aggStageIndex(st);
    const agg = st[ai].node.aggregate;
    try testing.expectEqual(@as(usize, 1), agg.aggs.len);
    try testing.expectEqualStrings("avg(v)", agg.aggs[0].name);
    try testing.expectEqual(ast.AggFunc.avg, agg.aggs[0].func);

    const sel = st[ai + 1].node.select;
    try testing.expectEqual(@as(usize, 2), sel.len);
    try testing.expectEqualStrings("g", sel[0].field.last());
    try testing.expectEqualStrings("m", sel[1].computed.name);
    // round's first argument is now the column the aggregate produced
    const arg0 = sel[1].computed.expr.call.args[0];
    try testing.expectEqualStrings("avg(v)", arg0.field.last());
}

test "sql: one aggregate mentioned twice becomes a single output column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT SUM(v) AS s, SUM(v) * 2 AS d FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    const agg = st[aggStageIndex(st)].node.aggregate;
    try testing.expectEqual(@as(usize, 1), agg.aggs.len);
    try testing.expectEqualStrings("s", agg.aggs[0].name);
}

// HAVING is allowed to filter on an aggregate the SELECT list never asked for.
// It was rejected with "unknown field `count(*)`", because nothing computed it.
test "sql: HAVING may name an aggregate the SELECT list omits" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT g, AVG(v) AS m FROM 'x.csv' GROUP BY g HAVING COUNT(*) > 1;");
    const st = firstPipelineStages(prog);

    const ai = aggStageIndex(st);
    const agg = st[ai].node.aggregate;
    try testing.expectEqual(@as(usize, 2), agg.aggs.len);
    try testing.expectEqualStrings("count(*)", agg.aggs[1].name);

    // ...and the extra column is projected away again, so it never reaches
    // output. The filter (HAVING) sits between, hence ai + 2.
    const sel = st[ai + 2].node.select;
    try testing.expectEqual(@as(usize, 2), sel.len);
    try testing.expectEqualStrings("g", sel[0].field.last());
    try testing.expectEqualStrings("m", sel[1].field.last());
}

// BETWEEN desugars to the pair of comparisons rather than becoming a node of
// its own, so everything downstream — the type checker, the evaluator, and
// above all pushdown — sees a shape it already handles.
test "sql: BETWEEN lowers to >= AND <=" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT * FROM 'x.csv' WHERE v BETWEEN 10 AND 30;");
    const st = firstPipelineStages(prog);
    var f: ?*const ast.Expr = null;
    for (st) |s| if (s.node == .filter) {
        f = s.node.filter;
    };
    const e = f.?;
    try testing.expectEqual(ast.BinOp.@"and", e.binary.op);
    try testing.expectEqual(ast.BinOp.ge, e.binary.l.binary.op);
    try testing.expectEqual(@as(i64, 10), e.binary.l.binary.r.int_lit);
    try testing.expectEqual(ast.BinOp.le, e.binary.r.binary.op);
    try testing.expectEqual(@as(i64, 30), e.binary.r.binary.r.int_lit);
}

test "sql: NOT BETWEEN negates the whole range" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT * FROM 'x.csv' WHERE v NOT BETWEEN 10 AND 30;");
    const st = firstPipelineStages(prog);
    var f: ?*const ast.Expr = null;
    for (st) |s| if (s.node == .filter) {
        f = s.node.filter;
    };
    try testing.expectEqual(ast.UnOp.not, f.?.unary.op);
    try testing.expectEqual(ast.BinOp.@"and", f.?.unary.e.binary.op);
}

// `SELECT x AS y ... GROUP BY x` is ordinary SQL. It was rejected unless the
// GROUP BY named the alias, because the rename was attempted before the
// aggregate rather than after it.
test "sql: a grouping key may be aliased in the SELECT list" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT g AS grp, COUNT(*) AS n FROM 'x.csv' GROUP BY g;");
    const st = firstPipelineStages(prog);
    const ai = aggStageIndex(st);
    try testing.expectEqualSlices(u8, "g", st[ai].node.aggregate.by[0].last());

    const sel = st[ai + 1].node.select;
    try testing.expectEqualStrings("grp", sel[0].computed.name);
    try testing.expectEqualStrings("g", sel[0].computed.expr.field.last());
    try testing.expectEqualStrings("n", sel[1].field.last());
}

// A column that is neither aggregated nor grouped has no one value per group.
// It used to be dropped from the output silently, which turns a malformed query
// into a plausible-looking wrong answer.
test "sql: an ungrouped column is refused, not silently dropped" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(a, "SELECT g, v, COUNT(*) AS n FROM 'x.csv' GROUP BY g;", &diag);
    try testing.expectError(error.ParseFailed, r);
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`v`") != null);
}

// `"..."` was a second string syntax inherited from BSL, so `SELECT "Exchange
// rate"` silently produced that constant repeated down the column instead of
// the column itself — there was no spelling that reached a name with a space in
// it. It is the ANSI quoted identifier now; `'...'` is the only string.
test "sql: a double-quoted name is a column reference, not a string" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT \"Exchange rate\" AS taxa FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    const sel = st[1].node.select;
    try testing.expectEqualStrings("taxa", sel[0].computed.name);
    try testing.expectEqualStrings("Exchange rate", sel[0].computed.expr.field.last());
}

test "sql: single quotes still make a string" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT 'Exchange rate' AS lit FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    const sel = st[1].node.select;
    try testing.expectEqualStrings("Exchange rate", sel[0].computed.expr.str_lit);
}

// A quoted name is never read as a keyword, which is the other half of why
// quoting exists: a column may legitimately be called `select` or `from`.
test "sql: a quoted name that spells a keyword is still a column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT \"select\" AS s FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    try testing.expectEqualStrings("select", st[1].node.select[0].computed.expr.field.last());
}

test "sql: a doubled quote inside a name is one literal quote" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT \"a\"\"b\" AS c FROM 'x.csv';");
    const st = firstPipelineStages(prog);
    try testing.expectEqualStrings("a\"b", st[1].node.select[0].computed.expr.field.last());
}

test "sql: an empty quoted name is a lex error, not an empty column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT \"\" FROM 'x.csv';", &diag));
}

// §6 documents `PUSHDOWN(...)` on a discovered union — one raw predicate
// descended into every branch — but no clause was ever parsed there, so the
// documented example was a syntax error. The runtime already applied a `where`
// hint to each branch; only the spelling was missing.
test "sql: PUSHDOWN on a discovered union becomes a per-branch where hint" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\SELECT * FROM EACH TABLE OF (erp.QUERY($$SELECT name, x FROM sys.tables$$))
        \\  AS (table_name, EMPRESA)
        \\  PUSHDOWN($$D_E_L_E_T_ <> '*'$$)
        \\  ANCHOR SCHEMA SC5010;
    );
    const st = firstPipelineStages(prog);
    var saw_where = false;
    var saw_canon = false;
    for (st[0].hints) |h| {
        if (std.mem.eql(u8, h.key, "where")) {
            saw_where = true;
            try testing.expectEqualStrings("D_E_L_E_T_ <> '*'", h.value.str);
        }
        if (std.mem.eql(u8, h.key, "canon")) saw_canon = true;
    }
    try testing.expect(saw_where);
    try testing.expect(saw_canon);
}

// The two clauses are order-independent, so a script that names the anchor
// first is not a syntax error.
test "sql: ANCHOR SCHEMA before PUSHDOWN parses the same" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'h', database = 'd');
        \\SELECT * FROM EACH TABLE OF (erp.QUERY($$SELECT name, x FROM sys.tables$$))
        \\  AS (table_name, EMPRESA)
        \\  ANCHOR SCHEMA SC5010
        \\  PUSHDOWN($$1 = 1$$);
    );
    const st = firstPipelineStages(prog);
    var n: usize = 0;
    for (st[0].hints) |h| {
        if (std.mem.eql(u8, h.key, "where") or std.mem.eql(u8, h.key, "canon")) n += 1;
    }
    try testing.expectEqual(@as(usize, 2), n);
}
test "sql: PRINT takes a literal or a `||` chain over params" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\PARAM tbl STRING DEFAULT 'sc5';
        \\PRINT 'starting';
        \\PRINT 'loading ' || $tbl;
        \\SELECT id FROM 'x.csv';
    );
    try testing.expectEqualStrings("starting", prog.stmts[2].print.expr.str_lit);

    const cat = prog.stmts[3].print.expr;
    try testing.expect(cat.* == .call);
    try testing.expectEqualStrings("concat", cat.call.name);
    try testing.expectEqualStrings("loading ", cat.call.args[0].str_lit);
    try testing.expectEqualStrings("tbl", cat.call.args[1].field.last());
}

test "sql: PRINT is a statement inside a FOR EACH body and a statement function" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a,
        \\CREATE FUNCTION note(msg) AS
        \\  PRINT 'note: ' || $msg;
        \\END;
        \\FOR EACH ROW OF (SELECT name FROM 'catalog.csv') AS (name)
        \\  PRINT 'company ' || $name;
        \\  LOAD INTO 'out.csv' AS SELECT id FROM 'x.csv';
        \\END FOR;
    );
    const note = prog.stmts[1].func;
    try testing.expect(note.body == .stmts);
    try testing.expect(note.body.stmts[0] == .print);

    const fe = prog.stmts[2].for_each;
    try testing.expectEqual(@as(usize, 2), fe.body.len);
    try testing.expect(fe.body[0] == .print);
    try testing.expectEqualStrings("name", fe.body[0].print.expr.call.args[1].field.last());
    try testing.expect(fe.body[1] == .output);
}

test "sql: PRINT without an expression is a parse error" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "PRINT;\nSELECT id FROM 'x.csv';", &diag));
}
