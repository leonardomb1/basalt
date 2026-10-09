//! Parser for the basalt SQL dialect (the book in docs/). Produces the
//! `ast.Program` the planner, analyzer, pushdown and executor share.
//!
//! Mapping highlights:
//!   CREATE ENDPOINT '/x'            -> KindDecl http (absent -> batch)
//!   PARAM x INT DEFAULT 7           -> Param (referenced as $x)
//!   CREATE CONNECTION c TYPE t ...  -> Connection (+ credential convention:
//!                                      user/password default to env(C_USER/C_PASS))
//!   LOAD INTO tgt USING f ... AS q  -> output Pipeline ending in a write stage
//!   terminal SELECT                 -> output Pipeline ending in `write stdout`
//!   WITH name AS (...)              -> Let binding (+ ref stage when sourced)
//!   FROM conn.tbl PUSHDOWN($$..$$)  -> read stage with a `where` hint
//!   WHERE / GROUP BY / ORDER BY ... -> filter / aggregate / sort / limit stages
//!   UNION [ALL] [BY NAME] + ANCHOR  -> union_ stage (+ distinct; tag col = literal-as-alias)
//!   FOR EACH ROW OF (...) AS (...)  -> ForEach (PARALLEL / ON ERROR -> hints)
//!   CASE ... THEN <stmts> END CASE  -> StmtMatch (plan-time dispatch)
//!
//! Most SQL is desugared here rather than given plan nodes of its own. A derived
//! table, a CTE, a join's reading side and a table-function call each lower to a
//! binding under a generated name, gathered in `pending_bindings` and drained by
//! `parseQuery` ahead of the statement that reads it; bindings share one namespace
//! per script, and binding under the written alias once let a second `r` silently
//! replace the first. Aliases resolve by name at parse time (`AliasSet`), so a
//! repeated alias is refused where it is written. `IN (SELECT ...)` becomes a semi
//! or anti join stage (NOT IN null-aware): a sentinel expression, matched by
//! pointer, holds its place in the WHERE until it is lifted out, so it is accepted
//! only as a top-level AND conjunct, and a WHERE lifts only the sentinels it
//! registered itself. A scalar subquery becomes a query LET referenced as `$name`,
//! a constant by plan time that can push down. BETWEEN becomes `>= AND <=`. Table
//! functions are expanded while parsing, so their declarations, including
//! `@include`d ones, must be in hand.
//!
//! Aggregation: the aggregate stage emits grouping keys first, then aggregates. A
//! projection before it carries the columns the aggregates read; one after it
//! restores the SELECT order, evaluates arithmetic around lifted aggregates
//! (`round(avg(x), 2)`), renames aliased keys, places constant items and drops
//! aggregates only HAVING needed. A column neither grouped nor aggregated is
//! refused, never silently dropped. Sort keys, `DISTINCT ON` keys and window
//! references the SELECT list leaves out ride along as hidden columns and are
//! dropped last. With a window, DISTINCT runs after it.
//!
//! `$name` is always script scope (a PARAM, LET, loop variable or statement-function
//! parameter, possibly from an `@include`); a bare name is a column even when a
//! PARAM shares it. A double-quoted name is always a column, never a keyword or a
//! string; `'...'` is the only string syntax.
//!
//! Stack safety: every later pass recurses on the expression tree, so one deeper
//! than `max_expr_depth` is refused (a 4000-element IN list once segfaulted in
//! `run`); AND/OR chains are balanced first, so long IN lists stay shallow. Query
//! nesting, unary nesting and table-function recursion are bounded too.
//!
//! With `Options.errors`, each failed statement is recorded and parsing resumes at
//! the next; an error inside a block statement (FOR, CASE, CREATE) ends the parse,
//! since resuming mid-body would report errors the block does not have.
//!
//! `Parser` keeps the token cursor, its state and small types here; its grammar
//! is in parse/, one file per area (`stmt.zig`, `decl.zig`, `query.zig`,
//! `tablefn.zig`, `window.zig`, `from.zig`, `control.zig`, `rewrite.zig`,
//! `expr.zig`), each method aliased back into `Parser` so `self.parseExpr()` reads
//! as before. The parser's tests are in `parse/tests_expr.zig`,
//! `parse/tests_query.zig`, `parse/tests_script.zig` and `parse/tests_window.zig`.

const std = @import("std");
const token = @import("token.zig");
const lexer = @import("sql_lexer.zig");
const ast = @import("ast.zig");
const aggregates = @import("aggregates.zig");
const expand = @import("expand.zig");
const types = @import("types.zig");

pub const Token = token.Token;
const Tag = token.Tag;
pub const Pos = ast.Pos;

pub const Diagnostic = struct {
    msg: []const u8,
    line: u32,
    col: u32,
    end_line: u32 = 0,
    end_col: u32 = 0,
};
pub const Error = error{ ParseFailed, OutOfMemory };

pub fn parseSource(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic) Error!ast.Program {
    return parseSourceWith(arena, src, diag, &.{});
}

/// `parseSource` for a file parsed after its `@include`s, whose connections are `known`.
pub fn parseSourceWith(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic, known: []const ast.Connection) Error!ast.Program {
    return parseSourceOpts(arena, src, diag, .{ .known_conns = known });
}

pub const Options = struct {
    known_conns: []const ast.Connection = &.{},
    known_fns: []const ast.FnDecl = &.{},
    known_tables: []const []const u8 = &.{},
    errors: ?*std.array_list.Managed(Diagnostic) = null,
};

pub fn parseSourceOpts(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic, opts: Options) Error!ast.Program {
    const toks = lexer.tokenize(arena, src) catch return error.OutOfMemory;
    var p = Parser{ .arena = arena, .toks = toks, .diag = diag };
    for (toks) |t| {
        if (t.tag == .invalid) return p.fail(.{ .line = t.line, .col = t.col }, "invalid token `{s}`", .{t.text});
    }
    p.known_conns = opts.known_conns;
    p.known_fns = opts.known_fns;
    p.known_tables = opts.known_tables;
    p.errors = opts.errors;
    return p.parseProgram();
}

/// A type name as a `PARAM` or `CAST` spells it, or null when it is none.
pub fn parseTypeStr(arena: std.mem.Allocator, src: []const u8) ?types.Type {
    const toks = lexer.tokenize(arena, src) catch return null;
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    var p = Parser{ .arena = arena, .toks = toks, .diag = &diag };
    const ty = p.parseTypeName() catch return null;
    if (!p.at(.eof)) return null;
    return ty;
}

/// Parse one standalone expression, the body of a `${ <expr> }` hole. Fails on trailing input.
pub fn parseExprStr(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic) Error!*ast.Expr {
    const toks = lexer.tokenize(arena, src) catch return error.OutOfMemory;
    var p = Parser{ .arena = arena, .toks = toks, .diag = diag };
    for (toks) |t| {
        if (t.tag == .invalid) return p.fail(.{ .line = t.line, .col = t.col }, "invalid token `{s}`", .{t.text});
    }
    const e = try p.parseExpr();
    if (!p.at(.eof)) return p.fail(p.curPos(), "unexpected trailing input in expression", .{});
    return e;
}

pub fn eqlNoCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

const MAX_ALIASES = 32;

pub const AliasSet = struct {
    pub const Kind = enum { left, right, reserved };

    names: [MAX_ALIASES][]const u8 = undefined,
    kinds: [MAX_ALIASES]Kind = undefined,
    n: usize = 0,

    fn put(self: *AliasSet, name: []const u8, kind: Kind) error{ AliasTaken, TooManyAliases }!void {
        for (self.names[0..self.n]) |a| if (std.mem.eql(u8, a, name)) return error.AliasTaken;
        if (self.n == MAX_ALIASES) return error.TooManyAliases;
        self.names[self.n] = name;
        self.kinds[self.n] = kind;
        self.n += 1;
    }
    fn strips(self: *const AliasSet, name: []const u8) bool {
        for (self.names[0..self.n], self.kinds[0..self.n]) |a, k| {
            if (std.mem.eql(u8, a, name)) return k == .left;
        }
        return false;
    }
    pub fn has(self: *const AliasSet, name: []const u8) bool {
        for (self.names[0..self.n], self.kinds[0..self.n]) |a, k| {
            if (std.mem.eql(u8, a, name)) return k != .reserved;
        }
        return false;
    }
};

const reserved_after_source = [_][]const u8{
    "where",    "group",    "order",  "limit",   "union",  "anchor",    "join",
    "inner",    "left",     "right",  "full",    "cross",  "semi",      "anti",
    "on",       "pushdown", "with",   "as",      "end",    "when",      "then",
    "else",     "case",     "select", "from",    "load",   "for",       "using",
    "natural",  "upsert",   "append", "replace", "split",  "jobs",      "offset",
    "paginate", "retry",    "create", "param",   "having", "and",       "or",
    "not",      "explain",  "costs",  "analyze", "except", "intersect", "window",
};

fn isKwIn(name: []const u8, kws: []const []const u8) bool {
    for (kws) |k| if (eqlNoCase(name, k)) return true;
    return false;
}

pub fn isReservedAfterSource(name: []const u8) bool {
    for (reserved_after_source) |k| {
        if (eqlNoCase(name, k)) return true;
    }
    return false;
}

/// Whether a computed path spells an extension after its last `${...}` hole: the
/// format is sniffed from it when the read opens.
pub fn hasLiteralExt(tmpl: []const u8) bool {
    const tail = if (std.mem.lastIndexOfScalar(u8, tmpl, '}')) |i| tmpl[i + 1 ..] else tmpl;
    const dot = std.mem.lastIndexOfScalar(u8, tail, '.') orelse return false;
    return dot + 1 < tail.len;
}

/// Whether `name` is an aggregate. Reserved: it parses as one before any user
/// function is looked up.
pub fn isAggName(name: []const u8) bool {
    return aggregates.lookup(name) != null;
}

pub fn isGroupKey(group: []const ast.QualName, q: ast.QualName) bool {
    for (group) |k| {
        if (std.mem.eql(u8, k.last(), q.last())) return true;
    }
    return false;
}

/// The name the aggregate stage emits a `post` item under: a grouping key's own
/// column name, a lifted aggregate's alias, or null for a star.
fn postItemName(it: ast.SelectItem) ?[]const u8 {
    return switch (it) {
        .field => |q| q.last(),
        .computed => |c| c.name,
        .star, .star_except, .star_rename => null,
    };
}

/// Whether the SELECT list orders columns otherwise than the aggregate stage emits
/// them (keys first), so the caller must add a reordering projection.
pub fn aggOrderDiffers(post: []const ast.SelectItem, group: []const ast.QualName, aggs: []const ast.AggItem) bool {
    if (post.len != group.len + aggs.len) return false;
    for (post, 0..) |it, i| {
        const want = postItemName(it) orelse return false;
        const natural = if (i < group.len) group[i].last() else aggs[i - group.len].name;
        if (!std.mem.eql(u8, want, natural)) return true;
    }
    return false;
}

fn sameName(a: ast.QualName, b: ast.QualName) bool {
    if (a.parts.len != b.parts.len) return false;
    for (a.parts, b.parts) |x, y| if (!std.mem.eql(u8, x, y)) return false;
    return true;
}

const KeyHome = union(enum) { as_is, renamed: []const u8, missing };

/// Where the SELECT list carries a sort or `DISTINCT ON` key: its own name, an alias,
/// or nowhere. A qualified key (`b.amt`) matches only the same reference.
pub fn keyHome(items: []const ast.SelectItem, k: ast.QualName) KeyHome {
    for (items) |it| switch (it) {
        .field => |f| if (sameName(f, k)) return .as_is,
        .computed => |c| if (k.parts.len == 1 and std.mem.eql(u8, c.name, k.last())) return .as_is,
        else => {},
    };
    if (k.parts.len == 1) for (items) |it| {
        if (it == .field and std.mem.eql(u8, it.field.last(), k.last())) return .as_is;
    };
    for (items) |it| {
        if (it == .computed and it.computed.expr.* == .field and sameName(it.computed.expr.field, k)) return .{ .renamed = it.computed.name };
    }
    return .missing;
}

/// The projection that keeps `items`' outputs and none of the hidden keys beside them;
/// a plain column is kept by reference, so `b.amt` still finds the right side's column.
pub fn keepOutputs(arena: std.mem.Allocator, items: []const ast.SelectItem) ![]const ast.SelectItem {
    const outs = try arena.alloc(ast.SelectItem, items.len);
    for (items, outs) |it, *o| o.* = switch (it) {
        .computed => |c| blk: {
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = c.name;
            break :blk .{ .field = .{ .parts = parts } };
        },
        else => it,
    };
    return outs;
}

/// What to call a SELECT item in a diagnostic.
pub fn itemLabel(it: ast.SelectItem) []const u8 {
    return switch (it) {
        .star, .star_except, .star_rename => "*",
        .field => |q| q.last(),
        .computed => |c| c.name,
    };
}

pub fn aggFunc(name: []const u8) ?ast.AggFunc {
    return aggregates.lookup(name);
}

pub const TableFrame = struct {
    names: []const []const u8,
    args: []const *ast.Expr,
    ctes: std.array_list.Managed([2][]const u8),
    id: usize,

    pub fn arg(self: *const TableFrame, name: []const u8) ?*ast.Expr {
        for (self.names, self.args) |n, a| if (std.mem.eql(u8, n, name)) return a;
        return null;
    }

    pub fn cte(self: *const TableFrame, name: []const u8) ?[]const u8 {
        for (self.ctes.items) |m| if (std.mem.eql(u8, m[0], name)) return m[1];
        return null;
    }
};

pub const max_tvf_depth = 16;

pub const Resource = struct { conn: []const u8, name: []const u8, path: []const u8, post: bool, hints: []const ast.Hint };

pub const Parser = struct {
    arena: std.mem.Allocator,
    toks: []const Token,
    i: usize = 0,
    diag: *Diagnostic,

    endpoint: ?ast.KindDecl = null,
    known_conns: []const ast.Connection = &.{},
    known_fns: []const ast.FnDecl = &.{},
    known_tables: []const []const u8 = &.{},
    errors: ?*std.array_list.Managed(Diagnostic) = null,
    table_fns: std.array_list.Managed(ast.FnDecl) = undefined,
    tvf: ?*TableFrame = null,
    tvf_depth: usize = 0,
    lambda_names: [24][]const u8 = undefined,
    lambda_n: usize = 0,
    conn_names: std.array_list.Managed([]const u8) = undefined,
    conn_types: std.array_list.Managed([]const u8) = undefined,
    resources: std.array_list.Managed(Resource) = undefined,
    let_names: std.array_list.Managed([]const u8) = undefined,
    const_names: std.array_list.Managed([]const u8) = undefined,
    pending_bindings: std.array_list.Managed(ast.Stmt) = undefined,
    derived_n: usize = 0,
    pending_semijoins: std.array_list.Managed(PendingSemiJoin) = undefined,
    in_where: bool = false,
    win_sink: ?*std.array_list.Managed(WinCall) = null,
    query_depth: usize = 0,
    expr_nest: usize = 0,
    unary_nest: usize = 0,

    pub const PendingSemiJoin = struct {
        sentinel: *ast.Expr,
        lhs: *ast.Expr,
        binding: []const u8,
        right_col: []const u8,
        negated: bool,
        pos: Pos,
    };

    pub fn cur(self: *Parser) Token {
        return self.toks[self.i];
    }
    pub fn curTag(self: *Parser) Tag {
        return self.toks[self.i].tag;
    }
    pub fn curPos(self: *Parser) Pos {
        const t = self.toks[self.i];
        return .{ .line = t.line, .col = t.col };
    }
    pub fn spanFrom(self: *Parser, start: Pos) ast.Span {
        const t = self.toks[if (self.i > 0) self.i - 1 else 0];
        return .{ .start = start, .end = .{ .line = t.end_line, .col = t.end_col } };
    }
    pub fn peekTag(self: *Parser) Tag {
        const j = self.i + 1;
        return if (j < self.toks.len) self.toks[j].tag else .eof;
    }
    pub fn peekTok(self: *Parser) Token {
        const j = self.i + 1;
        return if (j < self.toks.len) self.toks[j] else self.toks[self.toks.len - 1];
    }
    pub fn advance(self: *Parser) Token {
        const t = self.toks[self.i];
        if (t.tag != .eof) self.i += 1;
        return t;
    }
    pub fn at(self: *Parser, tag: Tag) bool {
        return self.curTag() == tag;
    }
    pub fn eat(self: *Parser, tag: Tag) bool {
        if (self.at(tag)) {
            _ = self.advance();
            return true;
        }
        return false;
    }
    pub fn isKw(self: *Parser, kw: []const u8) bool {
        const t = self.cur();
        return t.tag == .ident and eqlNoCase(t.text, kw);
    }
    pub fn peekKw(self: *Parser, kw: []const u8) bool {
        const t = self.peekTok();
        return t.tag == .ident and eqlNoCase(t.text, kw);
    }
    pub fn eatKw(self: *Parser, kw: []const u8) bool {
        if (self.isKw(kw)) {
            _ = self.advance();
            return true;
        }
        return false;
    }
    pub fn expect(self: *Parser, tag: Tag) Error!Token {
        if (self.at(tag)) return self.advance();
        return self.fail(self.curPos(), "expected {s}, found {s}", .{ tag.describe(), self.curTag().describe() });
    }
    pub fn expectIdent(self: *Parser) Error![]const u8 {
        if (self.at(.ident) or self.at(.qident)) return self.advance().text;
        return self.fail(self.curPos(), "expected identifier, found {s}", .{self.curTag().describe()});
    }

    pub fn prevPos(self: *Parser) Pos {
        const t = self.toks[if (self.i > 0) self.i - 1 else 0];
        return .{ .line = t.line, .col = t.col };
    }

    /// `AS` before a source's alias, eaten only when an alias follows, so `... AS SELECT`
    /// stays the statement's.
    pub fn eatAliasAs(self: *Parser) void {
        if (!self.isKw("as")) return;
        const next = self.peekTok();
        if (next.tag == .ident and !isReservedAfterSource(next.text)) _ = self.advance();
    }

    pub fn claimAlias(self: *Parser, aliases: *AliasSet, name: []const u8, pos: Pos, kind: AliasSet.Kind) Error!void {
        aliases.put(name, kind) catch |e| return switch (e) {
            error.AliasTaken => self.fail(pos, "`{s}` names two tables in this FROM; give one of them a different alias", .{name}),
            error.TooManyAliases => self.fail(pos, "more than {d} tables in one FROM", .{MAX_ALIASES}),
        };
    }

    /// An identifier in either spelling. `isKw` stays bare-ident only: `"select"` is a
    /// column named select, never a keyword.
    pub fn atName(self: *Parser) bool {
        return self.at(.ident) or self.at(.qident);
    }
    pub fn expectKw(self: *Parser, kw: []const u8) Error!void {
        if (self.eatKw(kw)) return;
        return self.fail(self.curPos(), "expected `{s}`, found {s}", .{ kw, self.curTag().describe() });
    }
    /// A select item's alias: `AS name`, or a bare name (`SUM(v) total`) unless the word
    /// is a clause that follows the item.
    pub fn itemAlias(self: *Parser) Error!?[]const u8 {
        if (self.eatKw("as")) return try self.expectColName();
        if (self.at(.qident)) return self.advance().text;
        if (self.at(.ident) and !isReservedAfterSource(self.toks[self.i].text) and !isKwIn(self.toks[self.i].text, &.{ "into", "over", "filter", "window", "qualify", "fetch" }))
            return self.advance().text;
        return null;
    }

    /// A column name: an identifier, or a quoted string so '${var}' can build it.
    pub fn expectColName(self: *Parser) Error![]const u8 {
        if (self.atName() or self.at(.string)) return self.advance().text;
        return self.fail(self.curPos(), "expected a column name, found {s}", .{self.curTag().describe()});
    }

    /// Name for a computed item written without `AS`, joined from its tokens (`count(*)`).
    /// ORDER BY synthesizes the same text, which binds it to the item it repeats.
    pub fn synthName(self: *Parser, start: usize) Error![]const u8 {
        return self.synthRange(start, self.i);
    }

    pub fn synthRange(self: *Parser, start: usize, end: usize) Error![]const u8 {
        var buf = std.array_list.Managed(u8).init(self.arena);
        for (self.toks[start..end]) |t| {
            for (t.text) |c| buf.append(std.ascii.toLower(c)) catch return error.OutOfMemory;
        }
        return buf.toOwnedSlice() catch return error.OutOfMemory;
    }

    pub fn fail(self: *Parser, pos: Pos, comptime fmt: []const u8, args: anytype) Error {
        self.diag.* = .{
            .msg = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory formatting diagnostic",
            .line = pos.line,
            .col = pos.col,
        };
        const lo = if (self.i >= 4) self.i - 4 else 0;
        const hi = @min(self.toks.len, self.i + 2);
        for (self.toks[lo..hi]) |t| {
            if (t.line == pos.line and t.col == pos.col and t.end_line > 0 and t.tag != .eof) {
                self.diag.end_line = t.end_line;
                self.diag.end_col = t.end_col;
                break;
            }
        }
        return error.ParseFailed;
    }

    pub fn mk(self: *Parser, e: ast.Expr) Error!*ast.Expr {
        const p = try self.arena.create(ast.Expr);
        p.* = e;
        return p;
    }

    pub fn isConn(self: *Parser, name: []const u8) bool {
        for (self.conn_names.items) |c| {
            if (std.mem.eql(u8, c, name)) return true;
        }
        return false;
    }
    pub fn isLet(self: *Parser, name: []const u8) bool {
        for (self.let_names.items) |c| {
            if (std.mem.eql(u8, c, name)) return true;
        }
        for (self.known_tables) |c| {
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

    pub const parseProgram = @import("parse/stmt.zig").parseProgram;
    pub const opensBlock = @import("parse/stmt.zig").opensBlock;
    pub const parseStatement = @import("parse/stmt.zig").parseStatement;
    pub const parseDescribe = @import("parse/stmt.zig").parseDescribe;
    pub const parseShow = @import("parse/stmt.zig").parseShow;
    pub const connType = @import("parse/stmt.zig").connType;
    pub const parseExplainStmt = @import("parse/stmt.zig").parseExplainStmt;
    pub const parseLetStmt = @import("parse/stmt.zig").parseLetStmt;
    pub const parsePrintStmt = @import("parse/stmt.zig").parsePrintStmt;
    pub const parseCreate = @import("parse/stmt.zig").parseCreate;
    pub const parseAcceptBuffer = @import("parse/stmt.zig").parseAcceptBuffer;
    pub const parseNameSegment = @import("parse/stmt.zig").parseNameSegment;
    pub const distinctOnOutputs = @import("parse/stmt.zig").distinctOnOutputs;
    pub const parseColRef = @import("parse/stmt.zig").parseColRef;
    pub const parseTargetSegment = @import("parse/stmt.zig").parseTargetSegment;
    pub const parseUnionClauses = @import("parse/stmt.zig").parseUnionClauses;
    pub const exprToTemplate = @import("parse/stmt.zig").exprToTemplate;
    pub const templatePart = @import("parse/stmt.zig").templatePart;
    pub const unparse = @import("parse/stmt.zig").unparse;
    pub const parseByteSize = @import("parse/stmt.zig").parseByteSize;
    pub const parseConnection = @import("parse/decl.zig").parseConnection;
    pub const parseHttpClauses = @import("parse/decl.zig").parseHttpClauses;
    pub const parseHttpCall = @import("parse/decl.zig").parseHttpCall;
    pub const isHttpVerb = @import("parse/decl.zig").isHttpVerb;
    pub const findResource = @import("parse/decl.zig").findResource;
    pub const parseResource = @import("parse/decl.zig").parseResource;
    pub const showResources = @import("parse/decl.zig").showResources;
    pub const hasHint = @import("parse/decl.zig").hasHint;
    pub const envCall = @import("parse/decl.zig").envCall;
    pub const parseFunction = @import("parse/decl.zig").parseFunction;
    pub const atStmtBody = @import("parse/decl.zig").atStmtBody;
    pub const atStmtCase = @import("parse/decl.zig").atStmtCase;
    pub const parseFnParam = @import("parse/decl.zig").parseFnParam;
    pub const parseCallStmt = @import("parse/decl.zig").parseCallStmt;
    pub const parseThrowStmt = @import("parse/decl.zig").parseThrowStmt;
    pub const parseParam = @import("parse/decl.zig").parseParam;
    pub const parseTypeName = @import("parse/decl.zig").parseTypeName;
    pub const expectU8 = @import("parse/decl.zig").expectU8;
    pub const parseLoadInto = @import("parse/query.zig").parseLoadInto;
    pub const parseTerminalQuery = @import("parse/query.zig").parseTerminalQuery;
    pub const parseWithHints = @import("parse/query.zig").parseWithHints;
    pub const parseQuery = @import("parse/query.zig").parseQuery;
    pub const lastSelectIdx = @import("parse/query.zig").lastSelectIdx;
    pub const distinctDropTail = @import("parse/query.zig").distinctDropTail;
    pub const BranchInfo = struct { read: ast.Read, tag: ?[]const u8, tag_col: ?[]const u8 };

    pub const ExprAlias = struct { synth: []const u8, out: []const u8 };

    pub const resolveExprAlias = @import("parse/query.zig").resolveExprAlias;
    pub const Core = struct {
        stages: []const ast.Stage,
        aliases: AliasSet,
        union_branch: ?BranchInfo,
        expr_aliases: []const ExprAlias = &.{},
    };

    pub const parseSelectCore = @import("parse/query.zig").parseSelectCore;
    pub const parseUsing = @import("parse/query.zig").parseUsing;
    pub const unionBranchFromStages = @import("parse/query.zig").unionBranchFromStages;
    pub const parseUnionTail = @import("parse/query.zig").parseUnionTail;
    pub const SetOpTok = struct {
        pub const Kind = enum { union_, intersect, except };
        kind: Kind,
        all: bool,
        named: bool,
        pos: Pos,
    };

    pub const parseSetOps = @import("parse/query.zig").parseSetOps;
    pub const setOpStages = @import("parse/query.zig").setOpStages;
    pub const appendUnion = @import("parse/query.zig").appendUnion;
    pub const findTableFn = @import("parse/tablefn.zig").findTableFn;
    pub const cteName = @import("parse/tablefn.zig").cteName;
    pub const checkTableBody = @import("parse/tablefn.zig").checkTableBody;
    pub const LateralKey = struct { left: ast.QualName, right: []const u8 };

    pub const TableCall = struct { binding: []const u8, keys: []const LateralKey = &.{} };

    pub const callTableFn = @import("parse/tablefn.zig").callTableFn;
    pub const Correlated = struct { param: []const u8, left: ast.QualName, marker: *ast.Expr };

    pub const decorrelate = @import("parse/tablefn.zig").decorrelate;
    pub const inTableFn = @import("parse/tablefn.zig").inTableFn;
    pub const parseDerivedTable = @import("parse/tablefn.zig").parseDerivedTable;
    pub const Derived = struct { binding: []const u8, alias: ?[]const u8, alias_pos: Pos };

    pub const parseJoinRead = @import("parse/tablefn.zig").parseJoinRead;
    pub const parseSubqueryPipeline = @import("parse/tablefn.zig").parseSubqueryPipeline;
    pub const firstOutName = @import("parse/tablefn.zig").firstOutName;
    pub const stageWord = @import("parse/tablefn.zig").stageWord;
    pub const stageMentions = @import("parse/tablefn.zig").stageMentions;
    pub const splitConj = @import("parse/tablefn.zig").splitConj;
    pub const eqOfMarker = @import("parse/tablefn.zig").eqOfMarker;
    pub const outputNameOf = @import("parse/tablefn.zig").outputNameOf;
    pub const containsExpr = @import("parse/tablefn.zig").containsExpr;
    pub const isSentinel = @import("parse/tablefn.zig").isSentinel;
    pub const liftSemiJoins = @import("parse/tablefn.zig").liftSemiJoins;
    pub const WinCall = @import("parse/window.zig").WinCall;
    pub const NamedWindow = @import("parse/window.zig").NamedWindow;
    pub const parseNullTreatment = @import("parse/window.zig").parseNullTreatment;
    pub const parseFilterClause = @import("parse/window.zig").parseFilterClause;
    pub const applyFilter = @import("parse/window.zig").applyFilter;
    pub const parseWindowCall = @import("parse/window.zig").parseWindowCall;
    pub const windowArgs = @import("parse/window.zig").windowArgs;
    pub const parseSpecBody = @import("parse/window.zig").parseSpecBody;
    pub const parseFrame = @import("parse/window.zig").parseFrame;
    pub const parseBound = @import("parse/window.zig").parseBound;
    pub const parseWindowClause = @import("parse/window.zig").parseWindowClause;
    pub const resolveSpec = @import("parse/window.zig").resolveSpec;
    pub const checkCall = @import("parse/window.zig").checkCall;
    pub const groupedOnly = @import("parse/window.zig").groupedOnly;
    pub const parseFromSource = @import("parse/from.zig").parseFromSource;
    pub const parseSubQuery = @import("parse/from.zig").parseSubQuery;
    pub const parseEachTableOf = @import("parse/from.zig").parseEachTableOf;
    pub const parsePaginate = @import("parse/from.zig").parsePaginate;
    pub const parseRetry = @import("parse/from.zig").parseRetry;
    pub const parseBodySchema = @import("parse/from.zig").parseBodySchema;
    pub const parseForEach = @import("parse/control.zig").parseForEach;
    pub const parseDollarPath = @import("parse/control.zig").parseDollarPath;
    pub const parseCaseStmt = @import("parse/control.zig").parseCaseStmt;
    pub const joinKeyFail = @import("parse/rewrite.zig").joinKeyFail;
    pub const parseJoinKey = @import("parse/rewrite.zig").parseJoinKey;
    pub const parseQualNameTok = @import("parse/rewrite.zig").parseQualNameTok;
    pub const collectFields = @import("parse/rewrite.zig").collectFields;
    pub const exprKey = @import("parse/rewrite.zig").exprKey;
    pub const containsAgg = @import("parse/rewrite.zig").containsAgg;
    pub const repointPostItem = @import("parse/rewrite.zig").repointPostItem;
    pub const constItemExpr = @import("parse/rewrite.zig").constItemExpr;
    pub const liftAggs = @import("parse/rewrite.zig").liftAggs;
    pub const addHavingAggs = @import("parse/rewrite.zig").addHavingAggs;
    pub const singleName = @import("parse/rewrite.zig").singleName;
    pub const havingRewrite = @import("parse/rewrite.zig").havingRewrite;
    pub const sidesOf = @import("parse/rewrite.zig").sidesOf;
    pub const hasBareField = @import("parse/rewrite.zig").hasBareField;
    pub const qualOne = @import("parse/rewrite.zig").qualOne;
    pub const namesOtherSide = @import("parse/rewrite.zig").namesOtherSide;
    pub const stripRightExpr = @import("parse/rewrite.zig").stripRightExpr;
    pub const stripExpr = @import("parse/rewrite.zig").stripExpr;
    pub const parseCallArg = @import("parse/expr.zig").parseCallArg;
    pub const lambdaHead = @import("parse/expr.zig").lambdaHead;
    pub const lambdaParam = @import("parse/expr.zig").lambdaParam;
    pub const parseExpr = @import("parse/expr.zig").parseExpr;
    pub const max_expr_depth = 1000;
    pub const max_paren_depth = 256;
    pub const max_query_depth = 32;

    pub const deeperThan = @import("parse/expr.zig").deeperThan;
    pub const balanceChain = @import("parse/expr.zig").balanceChain;
    pub const buildBalanced = @import("parse/expr.zig").buildBalanced;
    pub const BinInfo = struct { op: ast.BinOp, lbp: u8 };

    pub const binInfo = @import("parse/expr.zig").binInfo;
    pub const parseBin = @import("parse/expr.zig").parseBin;
    pub const parseUnary = @import("parse/expr.zig").parseUnary;
    pub const parsePrimary = @import("parse/expr.zig").parsePrimary;
    pub const parseQualNameField = @import("parse/expr.zig").parseQualNameField;
    pub const parseCaseExpr = @import("parse/expr.zig").parseCaseExpr;
    pub const parseSearch = @import("parse/expr.zig").parseSearch;
};

pub fn binOpText(op: ast.BinOp) []const u8 {
    return switch (op) {
        .add => "+",
        .sub => "-",
        .mul => "*",
        .div => "/",
        .mod => "%",
        .eq => "==",
        .ne => "!=",
        .lt => "<",
        .le => "<=",
        .gt => ">",
        .ge => ">=",
        .@"and" => "and",
        .@"or" => "or",
        .bit_and => "&",
        .bit_or => "|",
        .bit_xor => "^",
        .shl => "<<",
        .shr => ">>",
    };
}

pub fn splitAnd(e: *ast.Expr, out: *std.array_list.Managed(*ast.Expr)) !void {
    if (e.* == .binary and e.binary.op == .@"and") {
        try splitAnd(e.binary.l, out);
        return splitAnd(e.binary.r, out);
    }
    try out.append(e);
}

pub fn qualHasPrefix(q: ast.QualName, prefix: []const u8) bool {
    return q.parts.len > 1 and std.mem.eql(u8, q.parts[0], prefix);
}

pub fn stripPrefix(q: ast.QualName, prefix: []const u8) ast.QualName {
    if (qualHasPrefix(q, prefix)) return .{ .parts = q.parts[1..], .safe = if (q.safe.len > 0) q.safe[1..] else &.{} };
    return q;
}

/// Whether `name` is the FROM table's alias, which its own columns drop.
pub fn stripsAlias(aliases: *const AliasSet, name: []const u8) bool {
    return aliases.strips(name);
}

pub fn stripQual(q: ast.QualName, aliases: *const AliasSet) ast.QualName {
    if (!q.dollar and q.parts.len > 1 and aliases.strips(q.parts[0]))
        return .{ .parts = q.parts[1..], .safe = if (q.safe.len > 0) q.safe[1..] else &.{} };
    return q;
}

const testing = std.testing;

test {
    _ = @import("parse/control.zig");
    _ = @import("parse/decl.zig");
    _ = @import("parse/expr.zig");
    _ = @import("parse/from.zig");
    _ = @import("parse/query.zig");
    _ = @import("parse/rewrite.zig");
    _ = @import("parse/stmt.zig");
    _ = @import("parse/tablefn.zig");
    _ = @import("parse/window.zig");
    _ = @import("parse/tests_expr.zig");
    _ = @import("parse/tests_query.zig");
    _ = @import("parse/tests_script.zig");
    _ = @import("parse/tests_window.zig");
    _ = @import("parse/testing_util.zig");
}
