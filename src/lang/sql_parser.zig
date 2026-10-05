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
//!   UNION [ALL] [BY NAME] + ANCHOR  -> union_ stage (+ distinct; tag col = literal-as-alias)
//!   FOR EACH ROW OF (...) AS (...)  -> ForEach (PARALLEL / ON ERROR -> hints)
//!   CASE ... THEN <stmts> END CASE  -> StmtMatch (plan-time dispatch)

const std = @import("std");
const token = @import("token.zig");
const lexer = @import("sql_lexer.zig");
const ast = @import("ast.zig");
const aggregates = @import("aggregates.zig");
const expand = @import("expand.zig");
const types = @import("types.zig");

const Token = token.Token;
const Tag = token.Tag;
const Pos = ast.Pos;

pub const Diagnostic = struct {
    msg: []const u8,
    line: u32,
    col: u32,
    /// Just past the token the error points at, when it points at one; 0 else.
    end_line: u32 = 0,
    end_col: u32 = 0,
};
pub const Error = error{ ParseFailed, OutOfMemory };

/// Tokenize and parse a whole Basalt SQL program.
pub fn parseSource(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic) Error!ast.Program {
    return parseSourceWith(arena, src, diag, &.{});
}

/// `parseSource` for a file that follows others — an includer after its
/// `@include`s: `known` are the connections those declared, so a `conn.QUERY(...)`
/// or `EACH TABLE OF (conn...)` here resolves as it would in one file.
pub fn parseSourceWith(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic, known: []const ast.Connection) Error!ast.Program {
    return parseSourceOpts(arena, src, diag, .{ .known_conns = known });
}

/// What a parse may know beyond its own text, and how it treats an error.
pub const Options = struct {
    /// Connections declared by files parsed before this one (`@include`).
    known_conns: []const ast.Connection = &.{},
    /// Table functions declared by files parsed before this one (`@include`):
    /// a call is expanded while parsing, so the declaration must be in hand.
    known_fns: []const ast.FnDecl = &.{},
    /// Tables that exist where the script will run but that it does not declare —
    /// a notebook's other cells. `FROM name` / `JOIN name` read them as a binding
    /// reference the analyzer resolves, instead of "unknown source".
    known_tables: []const []const u8 = &.{},
    /// Keep going: every statement that fails to parse is recorded here, and
    /// parsing resumes at the next statement. A statement that opens a block
    /// (`FOR`, `CASE`, `CREATE`) has `;`s of its own, so an error inside one
    /// ends the parse — resuming mid-body would report the rest of the block as
    /// errors it does not have. Null: the first error fails the parse.
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

/// A type name as a `PARAM` or `CAST` spells it (`int`, `decimal(10,2)`,
/// `varchar(20)`, `timestamp`), or null when it is none.
pub fn parseTypeStr(arena: std.mem.Allocator, src: []const u8) ?types.Type {
    const toks = lexer.tokenize(arena, src) catch return null;
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    var p = Parser{ .arena = arena, .toks = toks, .diag = &diag };
    const ty = p.parseTypeName() catch return null;
    if (!p.at(.eof)) return null;
    return ty;
}

/// Tokenize and parse a single standalone expression (used to evaluate the
/// body of a `${ <expr> }` interpolation hole). Fails on trailing input.
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

fn eqlNoCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

const MAX_ALIASES = 32;

/// The FROM/JOIN names of one query. A left alias is stripped from `alias.x`; a
/// join's right alias is kept, since `b.x` may be a column the join renamed `x_r`;
/// an unaliased join side only reserves its binding name.
///
/// Resolution happens here, at parse time, by name — so two tables sharing a name
/// cannot be told apart later, and every `r.x` would silently go to the first `r`.
/// A repeated name is refused where it is written instead.
const AliasSet = struct {
    const Kind = enum { left, right, reserved };

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
    fn has(self: *const AliasSet, name: []const u8) bool {
        for (self.names[0..self.n], self.kinds[0..self.n]) |a, k| {
            if (std.mem.eql(u8, a, name)) return k != .reserved;
        }
        return false;
    }
};

/// Words that terminate an alias-free position (so `FROM t WHERE ...` doesn't
/// read WHERE as an alias).
const reserved_after_source = [_][]const u8{
    "where",   "group",    "order",   "limit",  "union",     "anchor", "join",
    "inner",   "left",     "right",   "full",   "cross",     "semi",   "anti",
    "on",      "pushdown", "with",    "as",     "end",       "when",   "then",
    "else",    "case",     "select",  "from",   "load",      "for",    "using",
    "upsert",  "append",   "replace", "split",  "jobs",      "offset", "paginate",
    "retry",   "create",   "param",   "having", "and",       "or",     "not",
    "explain", "costs",    "analyze", "except", "intersect",
};

fn isKwIn(name: []const u8, kws: []const []const u8) bool {
    for (kws) |k| if (eqlNoCase(name, k)) return true;
    return false;
}

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

/// Whether `name` is an aggregate function — reserved, since it is parsed as
/// one before any user function could be looked up.
pub fn isAggName(name: []const u8) bool {
    return aggregates.lookup(name) != null;
}

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
fn sameName(a: ast.QualName, b: ast.QualName) bool {
    if (a.parts.len != b.parts.len) return false;
    for (a.parts, b.parts) |x, y| if (!std.mem.eql(u8, x, y)) return false;
    return true;
}

/// Where a SELECT list already carries the column a sort or `DISTINCT ON` key
/// names: under the key's own name, under an alias the list gave it, or nowhere.
/// A qualified key (`b.amt`, a join's right side) matches only the same reference —
/// an output merely *called* `amt` may be the left side's.
const KeyHome = union(enum) { as_is, renamed: []const u8, missing };

fn keyHome(items: []const ast.SelectItem, k: ast.QualName) KeyHome {
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

/// The projection that keeps `items`' outputs and nothing a key smuggled in beside
/// them. A plain column is kept by its own reference, so `b.amt` still finds the
/// right side's column rather than whatever is called `amt`.
fn keepOutputs(arena: std.mem.Allocator, items: []const ast.SelectItem) ![]const ast.SelectItem {
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

fn itemLabel(it: ast.SelectItem) []const u8 {
    return switch (it) {
        .star, .star_except, .star_rename => "*",
        .field => |q| q.last(),
        .computed => |c| c.name,
    };
}

fn aggFunc(name: []const u8) ?ast.AggFunc {
    return aggregates.lookup(name);
}

/// One table-function call being expanded: its parameters bound to the call's
/// argument expressions, and the body's own `WITH` names mapped to names unique
/// to this call, so two calls in one query do not share a binding. While a
/// declaration is only being checked, `args` is empty and every `$param` stays
/// a plain reference.
const TableFrame = struct {
    names: []const []const u8,
    args: []const *ast.Expr,
    ctes: std.array_list.Managed([2][]const u8),
    id: usize,

    fn arg(self: *const TableFrame, name: []const u8) ?*ast.Expr {
        for (self.names, self.args) |n, a| if (std.mem.eql(u8, n, name)) return a;
        return null;
    }

    fn cte(self: *const TableFrame, name: []const u8) ?[]const u8 {
        for (self.ctes.items) |m| if (std.mem.eql(u8, m[0], name)) return m[1];
        return null;
    }
};

/// How deep table functions may call one another — and where a function that
/// reaches itself (through `OR REPLACE`) stops.
const max_tvf_depth = 16;

/// One `CREATE RESOURCE conn.name AS GET(...)`: the read it stands for.
const Resource = struct { conn: []const u8, name: []const u8, path: []const u8, post: bool, hints: []const ast.Hint };

pub const Parser = struct {
    arena: std.mem.Allocator,
    toks: []const Token,
    i: usize = 0,
    diag: *Diagnostic,

    endpoint: ?ast.KindDecl = null,
    /// Connections declared by files parsed before this one (`@include`).
    known_conns: []const ast.Connection = &.{},
    known_fns: []const ast.FnDecl = &.{},
    known_tables: []const []const u8 = &.{},
    errors: ?*std.array_list.Managed(Diagnostic) = null,
    /// `CREATE FUNCTION ... RETURNS TABLE` declarations so far — this file's and
    /// its includes'. A call takes the last of a name, as `OR REPLACE` means.
    table_fns: std.array_list.Managed(ast.FnDecl) = undefined,
    /// The table-function call whose body is being parsed, if any.
    tvf: ?*TableFrame = null,
    tvf_depth: usize = 0,
    /// The parameters of the lambdas whose bodies are being parsed, innermost last.
    lambda_names: [24][]const u8 = undefined,
    lambda_n: usize = 0,
    conn_names: std.array_list.Managed([]const u8) = undefined,
    /// Parallel to `conn_names`: each connection's connector type, which `SHOW
    /// TABLES` needs to phrase its catalog query.
    conn_types: std.array_list.Managed([]const u8) = undefined,
    /// `CREATE RESOURCE`s declared so far, which `FROM conn.name` expands.
    resources: std.array_list.Managed(Resource) = undefined,
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
    /// `IN (SELECT ...)` conjuncts found while parsing the current WHERE. Each
    /// is lifted into a semi/anti join stage beside the filter; the sentinel is
    /// the placeholder expression standing where the IN test sat, matched by
    /// POINTER identity so nothing else in the tree can be mistaken for it.
    pending_semijoins: std.array_list.Managed(PendingSemiJoin) = undefined,
    /// True only while the current WHERE clause parses — the one place an
    /// `IN (SELECT ...)` can be lifted into a join stage.
    in_where: bool = false,
    /// Depth of `parseQuery` nesting. A scalar subquery desugars into a query
    /// LET emitted through `pending_bindings`, which only a surrounding
    /// `parseQuery` drains — so it is only legal at depth > 0.
    query_depth: usize = 0,
    /// `parseExpr` nesting: the outermost call checks the finished tree's depth.
    expr_nest: usize = 0,
    /// `parseUnary` nesting — parentheses, calls, unary operators: the parser's
    /// own recursion, bounded before it can exhaust the stack.
    unary_nest: usize = 0,

    const PendingSemiJoin = struct {
        sentinel: *ast.Expr,
        lhs: *ast.Expr,
        binding: []const u8,
        right_col: []const u8,
        negated: bool,
        pos: Pos,
    };

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
    /// From `start` to the end of the last token consumed.
    fn spanFrom(self: *Parser, start: Pos) ast.Span {
        const t = self.toks[if (self.i > 0) self.i - 1 else 0];
        return .{ .start = start, .end = .{ .line = t.end_line, .col = t.end_col } };
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

    /// Where the token just consumed starts.
    fn prevPos(self: *Parser) Pos {
        const t = self.toks[if (self.i > 0) self.i - 1 else 0];
        return .{ .line = t.line, .col = t.col };
    }

    /// `AS` before a source's alias, which SQL allows and leaves optional: eaten
    /// only when an alias follows it, so `... AS SELECT` stays the statement's.
    fn eatAliasAs(self: *Parser) void {
        if (!self.isKw("as")) return;
        const next = self.peekTok();
        if (next.tag == .ident and !isReservedAfterSource(next.text)) _ = self.advance();
    }

    fn claimAlias(self: *Parser, aliases: *AliasSet, name: []const u8, pos: Pos, kind: AliasSet.Kind) Error!void {
        aliases.put(name, kind) catch |e| return switch (e) {
            error.AliasTaken => self.fail(pos, "`{s}` names two tables in this FROM; give one of them a different alias", .{name}),
            error.TooManyAliases => self.fail(pos, "more than {d} tables in one FROM", .{MAX_ALIASES}),
        };
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
    /// A select item's alias: `AS name`, or a bare name as SQL allows —
    /// `SUM(v) total` — unless the word is what comes after the item (`FROM`).
    fn itemAlias(self: *Parser) Error!?[]const u8 {
        if (self.eatKw("as")) return try self.expectColName();
        if (self.at(.qident)) return self.advance().text;
        if (self.at(.ident) and !isReservedAfterSource(self.toks[self.i].text) and !isKwIn(self.toks[self.i].text, &.{ "into", "over", "filter", "window", "qualify", "fetch" }))
            return self.advance().text;
        return null;
    }

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
        // The token the error names, when `pos` is the start of one nearby.
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
        self.const_names = std.array_list.Managed([]const u8).init(self.arena);
        self.pending_semijoins = std.array_list.Managed(PendingSemiJoin).init(self.arena);

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
            const start = self.i;
            const n0 = stmts.items.len;
            self.parseStatement(&stmts) catch |e| {
                const errs = self.errors orelse return e;
                if (e == error.OutOfMemory) return e;
                try errs.append(self.diag.*);
                // what the failed statement half-emitted (a derived table's binding)
                stmts.shrinkRetainingCapacity(n0);
                self.pending_bindings.clearRetainingCapacity();
                if (self.opensBlock(start)) break;
                while (!self.at(.eof) and !self.at(.semi)) _ = self.advance();
                _ = self.eat(.semi);
                continue;
            };
        }
        if (stmts.items.len == 0) {
            // with errors recorded, an empty program is what survived them
            const recovered = if (self.errors) |errs| errs.items.len > 0 else false;
            if (!recovered) return self.fail(self.curPos(), "empty program: expected at least one statement", .{});
        }

        const kind: ast.KindDecl = self.endpoint orelse
            .{ .kind = .batch, .config = &.{}, .pos = .{ .line = 1, .col = 1 } };
        try stmts.insert(0, .{ .kind = kind });
        return .{ .stmts = try stmts.toOwnedSlice(), .explain = explain };
    }

    /// Whether the statement starting at token `i` has a body of statements —
    /// `;`s inside it that are not its end.
    fn opensBlock(self: *Parser, i: usize) bool {
        if (i >= self.toks.len) return false;
        const t = self.toks[i];
        if (t.tag != .ident) return false;
        if (eqlNoCase(t.text, "for") or eqlNoCase(t.text, "case")) return true;
        if (!eqlNoCase(t.text, "create")) return false;
        // `CREATE [OR REPLACE] FUNCTION|ENDPOINT` — a connection or resource is one statement
        for (self.toks[i + 1 .. @min(self.toks.len, i + 4)]) |n| {
            if (n.tag == .ident and (eqlNoCase(n.text, "function") or eqlNoCase(n.text, "endpoint"))) return true;
        }
        return false;
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
            // A query LET may itself contain derived tables or scalar
            // subqueries; their bindings must precede it. (Ordinary statements
            // get this from `parseQuery`'s own drain.)
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

    /// `DESCRIBE <source>;` or `DESCRIBE <query>;` — the columns and engine types
    /// of a file, a table, a `conn.QUERY(...)` or a query, as rows. Lowered to an
    /// `EXPLAIN` in `describe` mode over `read | write stdout`, so every place that
    /// already walks an EXPLAIN (check, expand, for-each bodies) carries it too.
    fn parseDescribe(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
        const pos = self.curPos();
        _ = self.advance();
        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        if (self.isKw("select") or self.isKw("with")) {
            try self.parseQuery(out, &stages);
        } else {
            var aliases = AliasSet{};
            var hints = std.array_list.Managed(ast.Hint).init(self.arena);
            var node = try self.parseFromSource(&aliases, &hints);
            if (self.isKw("with") and self.peekTag() == .lparen) {
                _ = self.advance();
                try self.parseWithHints(&hints);
            }
            // Only the shape is wanted: a table or query read is asked for no rows.
            if (node == .read and (node.read.form == .table or node.read.form == .query)) node.read.where = "1 = 0";
            try stages.append(.{ .node = node, .hints = try hints.toOwnedSlice(), .pos = pos });
        }
        try stages.append(.{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = pos });
        _ = try self.expect(.semi);
        try out.append(.{ .explain = .{ .mode = .describe, .pipeline = .{ .stages = try stages.toOwnedSlice(), .pos = pos }, .pos = pos } });
    }

    /// `SHOW TABLES FROM <conn>[.<schema>] [LIKE '<pattern>'];` — one row per table
    /// or view, from the source's own catalog. Lowered to a terminal query over a
    /// `conn.QUERY(...)` on `information_schema`, which every SQL connector has.
    /// Spelled in upper case and aliased back: on a SQL Server database with a
    /// case-sensitive or binary collation (Protheus uses `Latin1_General_BIN`) only
    /// `INFORMATION_SCHEMA.TABLES` and `TABLE_NAME` resolve, and upper case is
    /// what postgres folds an unquoted name from and what mysql ignores.
    fn parseShow(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
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

    fn connType(self: *Parser, name: []const u8) ?[]const u8 {
        for (self.conn_names.items, self.conn_types.items) |n, t| {
            if (std.ascii.eqlIgnoreCase(n, name)) return t;
        }
        return null;
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
        // `LET x = (SELECT ...);` — the value is the query's single cell,
        // evaluated at run time in statement order (see `runScalarLet`).
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
            try self.conn_types.append(conn.connector);
            return out.append(.{ .connection = conn });
        }
        if (self.eatKw("function")) {
            return out.append(.{ .func = try self.parseFunction(pos, replace) });
        }
        if (self.eatKw("resource")) return self.parseResource(pos);
        return self.fail(self.curPos(), "expected ENDPOINT, CONNECTION, RESOURCE, or FUNCTION after CREATE", .{});
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

    /// `DISTINCT ON` runs over the projection, but its keys are written against the
    /// input, as in Postgres: `DISTINCT ON (grp) grp AS k` and `DISTINCT ON (grp) id`
    /// are both legal. A key the list renamed is repointed at its output name, in
    /// place; one it does not project is carried
    /// through as a hidden column. Returns the projection that drops those again,
    /// or null when none was added.
    fn distinctOnOutputs(self: *Parser, items: *std.array_list.Managed(ast.SelectItem), on: []ast.QualName) Error!?[]const ast.SelectItem {
        for (items.items) |it| switch (it) {
            .field, .computed => {},
            // A `*` already carries every input column under its own name.
            else => return null,
        };
        const visible = items.items.len;
        for (on) |*k| switch (keyHome(items.items[0..visible], k.*)) {
            .as_is => {},
            .renamed => |nm| k.* = try self.singleName(nm),
            .missing => try items.append(.{ .field = k.* }),
        };
        if (items.items.len == visible) return null;
        return keepOutputs(self.arena, items.items[0..visible]) catch return error.OutOfMemory;
    }

    /// A column in a name position: a (qualified) name, or `IDENTIFIER(<expr>)`
    /// for one computed per row.
    fn parseColRef(self: *Parser) Error!ast.QualName {
        if (self.isKw("identifier") and self.peekTag() == .lparen) {
            const parts = try self.arena.alloc([]const u8, 1);
            parts[0] = try self.parseNameSegment();
            return .{ .parts = parts };
        }
        return self.parseQualNameTok();
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
            .int_lit => |v| try buf.writer().print("{d}", .{v}),
            .bool_lit => |b| try buf.appendSlice(if (b) "true" else "false"),
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
        var hints = std.array_list.Managed(ast.Hint).init(self.arena);
        try self.parseHttpClauses(&hints);
        if (hints.items.len > 0 and !std.mem.eql(u8, connector, "http"))
            return self.fail(hints.items[0].pos, "PAGINATE / RETRY / WITH on a connection are for `http` connections only", .{});
        _ = try self.expect(.semi);
        if (!has_user) try attrs.append(.{ .key = "user", .value = try self.envCall(name, "_USER"), .pos = pos });
        if (!has_pass) try attrs.append(.{ .key = "password", .value = try self.envCall(name, "_PASS"), .pos = pos });
        return .{ .name = name, .connector = connector, .config = try attrs.toOwnedSlice(), .pos = pos, .hints = try hints.toOwnedSlice() };
    }

    /// `[PAGINATE ...] [RETRY ...] [WITH (...)]` in any order — the REST clauses a
    /// connection or a resource carries for every read of it.
    fn parseHttpClauses(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
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

    /// `GET(path [, name = value ...])` / `POST(path [, body = value] [, name = value ...])`
    /// after `conn.`: the path and each value are expressions (so `$params` and
    /// `||` work); every `name = value` other than `body` is a query parameter,
    /// URL-encoded when the request is built.
    fn parseHttpCall(self: *Parser, conn: []const u8, hints: *std.array_list.Managed(ast.Hint)) Error!ast.Read {
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
            const val = try self.exprToTemplate(try self.parseExpr());
            if (eqlNoCase(key, "body")) {
                if (!post) return self.fail(apos, "GET takes no body — use {s}.POST(path, body = ...)", .{conn});
                try hints.append(.{ .key = "body", .value = .{ .str = val }, .pos = apos });
            } else {
                try hints.append(.{ .key = try std.fmt.allocPrint(self.arena, "query:{s}", .{key}), .value = .{ .str = val }, .pos = apos });
            }
        }
        _ = try self.expect(.rparen);
        return .{ .connector = conn, .form = .{ .path = path } };
    }

    fn isHttpVerb(self: *Parser) bool {
        return (self.isKw("get") or self.isKw("post")) and self.peekTag() == .lparen;
    }

    fn findResource(self: *Parser, conn: []const u8, name: []const u8) ?Resource {
        for (self.resources.items) |r| {
            if (std.ascii.eqlIgnoreCase(r.conn, conn) and std.ascii.eqlIgnoreCase(r.name, name)) return r;
        }
        return null;
    }

    /// `CREATE RESOURCE conn.name AS GET(...) | POST(...) [PAGINATE ...] [RETRY ...]
    /// [WITH (...)];` — names one endpoint of an http connection, so `FROM
    /// conn.name` reads it like a table. Expanded here; the plan never sees it.
    fn parseResource(self: *Parser, pos: Pos) Error!void {
        const conn = try self.expectIdent();
        const kind = self.connType(conn) orelse
            return self.fail(pos, "CREATE RESOURCE: `{s}` is not a connection declared above", .{conn});
        if (!std.mem.eql(u8, kind, "http"))
            return self.fail(pos, "CREATE RESOURCE: `{s}` is a {s} connection — resources name endpoints of an http one", .{ conn, kind });
        _ = try self.expect(.dot);
        const name = try self.expectIdent();
        try self.expectKw("as");
        if (!self.isHttpVerb())
            return self.fail(self.curPos(), "CREATE RESOURCE {s}.{s}: expected GET(...) or POST(...) after AS", .{ conn, name });
        var hints = std.array_list.Managed(ast.Hint).init(self.arena);
        const rd = try self.parseHttpCall(conn, &hints);
        try self.parseHttpClauses(&hints);
        _ = try self.expect(.semi);
        const r = Resource{ .conn = conn, .name = name, .path = rd.form.path, .post = self.hasHint(hints.items, "method"), .hints = try hints.toOwnedSlice() };
        for (self.resources.items) |*old| {
            if (std.ascii.eqlIgnoreCase(old.conn, conn) and std.ascii.eqlIgnoreCase(old.name, name)) {
                old.* = r;
                return;
            }
        }
        try self.resources.append(r);
    }

    /// `SHOW TABLES FROM <http conn>`: its resources as rows (resource, method,
    /// path), built from `RANGE(n)` — the parser already holds the whole list.
    fn showResources(self: *Parser, conn: []const u8, like: ?[]const u8, pos: Pos) Error!ast.Pipeline {
        var mine = std.array_list.Managed(Resource).init(self.arena);
        for (self.resources.items) |r| {
            if (std.ascii.eqlIgnoreCase(r.conn, conn)) try mine.append(r);
        }
        const range_col = try self.mk(.{ .field = .{ .parts = try self.arena.dupe([]const u8, &.{"range"}) } });
        const cols = [_][]const u8{ "resource", "method", "path" };
        const items = try self.arena.alloc(ast.SelectItem, cols.len);
        for (cols, items, 0..) |name, *item, c| {
            var e = try self.mk(.{ .str_lit = "" });
            var k = mine.items.len;
            while (k > 0) {
                k -= 1;
                const r = mine.items[k];
                const v = try self.mk(.{ .str_lit = switch (c) {
                    0 => r.name,
                    1 => if (r.post) "POST" else "GET",
                    else => r.path,
                } });
                const idx = try self.mk(.{ .int_lit = @intCast(k) });
                const is_k = try self.mk(.{ .binary = .{ .op = .eq, .l = range_col, .r = idx } });
                e = try self.mk(.{ .cond = .{ .cond = is_k, .then = v, .els = e } });
            }
            item.* = .{ .computed = .{ .name = name, .expr = e } };
        }
        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        const lo = try self.mk(.{ .int_lit = 0 });
        const hi = try self.mk(.{ .int_lit = @intCast(mine.items.len) });
        try stages.append(.{ .node = .{ .read = .{ .connector = "range", .form = .{ .range = .{ .lo = lo, .hi = hi } } } }, .hints = &.{}, .pos = pos });
        try stages.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = pos });
        if (like) |pat| {
            const args = try self.arena.alloc(*ast.Expr, 2);
            args[0] = try self.mk(.{ .field = .{ .parts = try self.arena.dupe([]const u8, &.{"resource"}) } });
            args[1] = try self.mk(.{ .str_lit = pat });
            try stages.append(.{ .node = .{ .filter = try self.mk(.{ .call = .{ .name = "like", .args = args } }) }, .hints = &.{}, .pos = pos });
        }
        try stages.append(.{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = pos });
        return .{ .stages = try stages.toOwnedSlice(), .pos = pos };
    }

    fn hasHint(_: *Parser, hints: []const ast.Hint, key: []const u8) bool {
        for (hints) |h| {
            if (std.mem.eql(u8, h.key, key)) return true;
        }
        return false;
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
        if (self.eatKw("returns")) {
            try self.expectKw("table");
            try self.expectKw("as");
            if (!self.isKw("select") and !self.isKw("with"))
                return self.fail(self.curPos(), "`{s}`: a table function's body is a query — SELECT ... or WITH ...", .{name});
            const fd = ast.FnDecl{ .name = name, .params = try params.toOwnedSlice(), .body = .{ .table = try self.checkTableBody() }, .replace = replace, .pos = pos };
            try self.table_fns.append(fd);
            return fd;
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

    /// Does the body after `AS` open a statement block? The keywords that begin a
    /// statement and can never begin a scalar expression — and `CASE` when it is
    /// the statement form, which only its closing `END CASE` tells apart from the
    /// scalar `CASE ... END` a function body usually means by it.
    fn atStmtBody(self: *Parser) bool {
        return self.isKw("load") or self.isKw("for") or self.isKw("call") or
            self.isKw("print") or self.isKw("throw") or self.isKw("select") or self.isKw("with") or
            (self.isKw("case") and self.atStmtCase());
    }

    /// At a `CASE`: is it closed by `END CASE`? Walks the tokens ahead, pairing every
    /// block opener (`case`, `for`) with its `end` — an `END CASE` / `END FOR` closes
    /// one block, its second word not opening another — until this one's `end`.
    fn atStmtCase(self: *Parser) bool {
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

    /// Parse `[WITH ctes] core [UNION [ALL] [BY NAME] core]* [ANCHOR SCHEMA q]
    /// [ORDER BY ...] [LIMIT n [OFFSET m]]`, appending Let stmts for CTEs to
    /// `out` and pipeline stages to `stages`.
    fn parseQuery(self: *Parser, out: *std.array_list.Managed(ast.Stmt), stages: *std.array_list.Managed(ast.Stage)) Error!void {
        // Each level is a `parseSelectCore` frame, which is large: a hundred nested
        // derived tables overflowed the stack.
        if (self.query_depth >= max_query_depth)
            return self.fail(self.curPos(), "queries nest more than {d} levels deep", .{max_query_depth});
        self.query_depth += 1;
        defer self.query_depth -= 1;
        // A nested query is a fresh scope: its own WHERE decides where its IN
        // (SELECT ...) may stand, and the enclosing one's is restored after it.
        const outer_in_where = self.in_where;
        self.in_where = false;
        defer self.in_where = outer_in_where;
        if (self.isKw("with") and !(self.peekTag() == .lparen)) {
            _ = self.advance();
            while (true) {
                const lpos = self.curPos();
                const name = try self.cteName(try self.expectIdent());
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

        if (self.isKw("union") or self.isKw("intersect") or self.isKw("except")) {
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
                if (self.isKw("identifier") and self.peekTag() == .lparen) {
                    q = try self.parseColRef();
                } else if (self.at(.ident) and self.peekTag() == .lparen) {
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
            // `DISTINCT ON` may already be carrying hidden keys of its own, dropped by
            // a projection right after it. That drop moves to the end with this one,
            // so the sort can still read what the SELECT list left out.
            var visible: ?[]const ast.SelectItem = null;
            if (distinctDropTail(stages.items)) visible = stages.pop().?.node.select;
            if (lastSelectIdx(stages.items)) |si| {
                const proj = stages.items[si].node.select;
                var wildcard = false;
                var have_names = std.array_list.Managed([]const u8).init(self.arena);
                for (proj) |it| switch (it) {
                    .field => |f| try have_names.append(f.last()),
                    .computed => |c| try have_names.append(c.name),
                    else => wildcard = true,
                };
                if (!wildcard) {
                    var extended = std.array_list.Managed(ast.SelectItem).init(self.arena);
                    try extended.appendSlice(proj);
                    for (sort_keys) |*k| {
                        if (k.field.parts.len != 1) {
                            // `ORDER BY b.amt`: a join's right-side column, which an
                            // output merely called `amt` does not stand in for.
                            switch (keyHome(proj, k.field)) {
                                .as_is => {},
                                .renamed => |nm| k.field = try self.singleName(nm),
                                .missing => try extended.append(.{ .field = k.field }),
                            }
                            continue;
                        }
                        var have = false;
                        for (have_names.items) |m| {
                            if (std.mem.eql(u8, m, k.field.last())) have = true;
                        }
                        if (!have) try extended.append(.{ .field = k.field });
                    }
                    if (extended.items.len != proj.len) {
                        stages.items[si].node.select = try extended.toOwnedSlice();
                        hidden_drop = keepOutputs(self.arena, visible orelse proj) catch return error.OutOfMemory;
                    }
                }
            }
            if (hidden_drop == null) hidden_drop = visible;
            const sort_stage: ast.Stage = .{ .node = .{ .sort = .{ .keys = sort_keys } }, .hints = &.{}, .pos = self.curPos() };
            // `DISTINCT ON (k) ... ORDER BY k, t` keeps the first row per key in
            // ORDER BY order, as Postgres and DuckDB do: sorted after, the distinct
            // had already kept the first in input order, and "latest row per key"
            // silently returned the wrong rows. The distinct keeps first occurrences
            // in order, so its output stays sorted.
            const last = stages.items.len - 1;
            if (stages.items[last].node == .distinct and stages.items[last].node.distinct.on != null)
                try stages.insert(last, sort_stage)
            else
                try stages.append(sort_stage);
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
    /// A `DISTINCT ON` between the projection and the sort is looked through: it
    /// keeps whole rows, so a hidden sort key rides along. A plain `DISTINCT` is
    /// not — an extra column there would change which rows are duplicates.
    fn lastSelectIdx(stages: []const ast.Stage) ?usize {
        if (stages.len == 0) return null;
        const n = stages.len;
        if (stages[n - 1].node == .select) return n - 1;
        if (n >= 2 and stages[n - 1].node == .distinct and stages[n - 1].node.distinct.on != null and stages[n - 2].node == .select) return n - 2;
        return null;
    }

    /// Does the pipeline end in `select, DISTINCT ON, select` — the shape
    /// `distinctOnOutputs` leaves when it had to carry a hidden key?
    fn distinctDropTail(stages: []const ast.Stage) bool {
        const n = stages.len;
        return n >= 3 and stages[n - 1].node == .select and stages[n - 2].node == .distinct and
            stages[n - 2].node.distinct.on != null and stages[n - 3].node == .select;
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
        var distinct_on: ?[]ast.QualName = null;
        var distinct_drop: ?[]const ast.SelectItem = null;
        if (self.eatKw("distinct")) {
            distinct = true;
            if (self.eatKw("on")) {
                _ = try self.expect(.lparen);
                var cols = std.array_list.Managed(ast.QualName).init(self.arena);
                try cols.append(try self.parseColRef());
                while (self.eat(.comma)) try cols.append(try self.parseColRef());
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
        var win_outs = std.array_list.Managed([]const u8).init(self.arena);
        while (true) {
            // `ROW_NUMBER() OVER (PARTITION BY .. ORDER BY ..)` is recognised as a whole
            // select item, before the expression parser sees it: a window function is a
            // stage, not an expression, and lifting one out of the middle of an
            // arithmetic expression would need the machinery `IN (SELECT ...)` needs.
            if (try self.parseWindowItem(&win_funcs, &win_part, &win_ord)) {
                try win_outs.append(win_funcs.items[win_funcs.items.len - 1].out);
                if (!self.eat(.comma)) break;
                continue;
            }
            if (self.eat(.star)) {
                if (self.eatKw("except") or self.eatKw("exclude")) {
                    _ = try self.expect(.lparen);
                    var names = std.array_list.Managed([]const u8).init(self.arena);
                    // A name may be `IDENTIFIER(<expr>)`: a template rendered at run
                    // time, and a rendered `a, b` is two names.
                    try names.append(try self.parseNameSegment());
                    while (self.eat(.comma)) try names.append(try self.parseNameSegment());
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
                name = try self.itemAlias();
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
        // Computed join keys' columns (`__jkN`), dropped once the WHERE is in —
        // not right after the join, where a stage would hold the WHERE above it
        // and keep it from the sources.
        var key_cols = std.array_list.Managed([]const u8).init(self.arena);

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
            var kind = jk orelse break;
            if (!self.isKw("join")) _ = self.advance();
            _ = self.eatKw("outer");
            try self.expectKw("join");
            const jpos = self.curPos();
            const lateral = self.eatKw("lateral");
            if (lateral and (kind != .cross and kind != .inner and kind != .left))
                return self.fail(jpos, "LATERAL joins are CROSS, INNER or LEFT", .{});
            if (lateral and !(self.at(.ident) and self.peekTag() == .lparen))
                return self.fail(self.curPos(), "JOIN LATERAL takes a table function call — `JOIN LATERAL f(o.col) x`", .{});
            var lateral_keys: []const LateralKey = &.{};
            // `CROSS JOIN UNNEST(...)` is the row-expanding form and stays an
            // explode stage; `CROSS JOIN <cte>` is the cartesian product.
            if (kind == .cross and self.isKw("unnest")) {
                _ = self.advance();
                _ = try self.expect(.lparen);
                var field: []const u8 = undefined;
                var delim: ?[]const u8 = null;
                var json = false;
                if (self.isKw("json_each") and self.peekTag() == .lparen) {
                    _ = self.advance();
                    _ = try self.expect(.lparen);
                    field = try self.expectIdent();
                    _ = try self.expect(.rparen);
                    json = true;
                } else if (self.isKw("split") and self.peekTag() == .lparen) {
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
                    .node = .{ .explode = .{ .field = field, .as_name = as_name, .delim = delim, .json = json } },
                    .hints = &.{},
                    .pos = jpos,
                });
                continue;
            }
            // `JOIN (SELECT ...) x ON ...` — the same lowering as a FROM-position
            // derived table, since a join's right side is named by binding anyway;
            // and so a path or a connection's table, read as `(SELECT * FROM it)`.
            var jalias: ?[]const u8 = null;
            var binding: []const u8 = undefined;
            var bpos: Pos = undefined;
            const reads = self.at(.string) or
                (self.isKw("identifier") and self.peekTag() == .lparen) or
                (self.at(.ident) and self.peekTag() == .dot and !self.isLet(self.cur().text));
            if (self.at(.lparen) or reads) {
                const d = if (reads) try self.parseJoinRead() else try self.parseDerivedTable();
                binding = d.binding;
                jalias = d.alias;
                bpos = d.alias_pos;
            } else {
                binding = try self.expectIdent();
                bpos = self.prevPos();
                const written = binding;
                self.eatAliasAs();
                const has_alias = self.at(.lparen) or (self.at(.ident) and !isReservedAfterSource(self.cur().text));
                if (self.at(.lparen)) {
                    const fd = self.findTableFn(binding) orelse
                        return self.fail(bpos, "unknown table function `{s}` — declare it with CREATE FUNCTION {s}(...) RETURNS TABLE AS SELECT ...", .{ binding, binding });
                    const call = try self.callTableFn(fd, bpos, if (lateral) .lateral else .join);
                    binding = call.binding;
                    lateral_keys = call.keys;
                    // The call's own name qualifies its columns unless an alias follows.
                    if (!(self.at(.ident) and !isReservedAfterSource(self.cur().text))) jalias = written;
                } else if (if (self.tvf) |f| f.cte(binding) else null) |bname| {
                    binding = bname;
                    if (!has_alias) jalias = written;
                } else if (!self.isLet(binding))
                    return self.fail(jpos, "JOIN right side `{s}` must be a WITH-defined CTE", .{binding});
            }
            if (jalias) |ja| {
                try self.claimAlias(&aliases, ja, bpos, .right);
            } else if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
                jalias = self.advance().text;
                try self.claimAlias(&aliases, jalias.?, self.prevPos(), .right);
            } else try self.claimAlias(&aliases, binding, bpos, .reserved);
            var left_keys = std.array_list.Managed(ast.QualName).init(self.arena);
            var right_keys = std.array_list.Managed(ast.QualName).init(self.arena);
            var post_filters = std.array_list.Managed(*ast.Expr).init(self.arena);
            // Computed keys: each side's `SELECT *, expr AS __jkN`, and the names
            // to drop after the join.
            var left_computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
            var right_computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
            for (lateral_keys) |lk| {
                try left_keys.append(stripQual(lk.left, &aliases));
                const rp = try self.arena.alloc([]const u8, 1);
                rp[0] = lk.right;
                try right_keys.append(.{ .parts = rp });
            }
            // A LATERAL call's keys come from its arguments: CROSS is then an inner
            // join on them, and ON is optional (`ON TRUE` is Postgres' spelling).
            if (lateral and lateral_keys.len > 0 and kind == .cross) kind = .inner;
            if (lateral and self.isKw("on") and self.peekTag() == .ident and eqlNoCase(self.peekTok().text, "true")) {
                _ = self.advance();
                _ = self.advance();
            } else if (lateral and lateral_keys.len > 0 and !self.isKw("on")) {
                // keys from the call alone
            } else if (kind == .cross) {
                if (self.isKw("on"))
                    return self.fail(self.curPos(), "CROSS JOIN takes no ON clause — it pairs every row with every row", .{});
            } else {
                if (!self.isKw("on"))
                    return self.fail(self.curPos(), "expected `ON <column> = <column>` after JOIN {s}", .{binding});
                const opos = self.advance();
                // `ON a = b AND ...`, split at its ANDs: a column of one side equal
                // to a column of the other is a key; a condition on the right side
                // alone, or on no column (`1 = 1`), narrows the right side before
                // the join, which is right for an outer join too; any other — the
                // left side alone, both sides compared otherwise — filters the
                // joined rows, which only an inner join means.
                const rname = jalias orelse binding;
                const on = try self.parseExpr();
                var conj = std.array_list.Managed(*ast.Expr).init(self.arena);
                try splitAnd(on, &conj);
                var narrow: ?*ast.Expr = null;
                for (conj.items) |c| {
                    if (c.* == .binary and c.binary.op == .eq and c.binary.l.* == .field and c.binary.r.* == .field and
                        !c.binary.l.field.dollar and !c.binary.r.field.dollar)
                    {
                        const a = c.binary.l.field;
                        const b = c.binary.r.field;
                        const a_right = qualHasPrefix(a, rname);
                        if (a_right != qualHasPrefix(b, rname) or (!a_right and (a.parts.len == 1 or b.parts.len == 1))) {
                            const l = if (a_right) b else a;
                            const r = if (a_right) a else b;
                            try left_keys.append(stripQual(l, &aliases));
                            try right_keys.append(stripPrefix(r, rname));
                            continue;
                        }
                    }
                    // A computed key: `trim(b.code) = cast(a.code AS string)`, each
                    // side naming its own table only. Each side computes its key
                    // as a column of its own, dropped again after the join.
                    if (c.* == .binary and c.binary.op == .eq) {
                        const sl = try self.sidesOf(c.binary.l, rname);
                        const sr = try self.sidesOf(c.binary.r, rname);
                        const l_left = sl.other and !sl.right;
                        const r_left = sr.other and !sr.right;
                        const l_right = sl.right and !sl.other;
                        const r_right = sr.right and !sr.other;
                        if ((l_left and r_right) or (l_right and r_left)) {
                            const le = if (l_left) c.binary.l else c.binary.r;
                            const re = if (l_left) c.binary.r else c.binary.l;
                            if (le.* == .field) {
                                try left_keys.append(stripQual(le.field, &aliases));
                            } else {
                                self.derived_n += 1;
                                const k = try std.fmt.allocPrint(self.arena, "__jk{d}", .{self.derived_n});
                                try left_computed.append(.{ .computed = .{ .name = k, .expr = try self.stripExpr(le, &aliases) } });
                                try key_cols.append(k);
                                try left_keys.append(try self.qualOne(k));
                            }
                            if (re.* == .field) {
                                try right_keys.append(stripPrefix(re.field, rname));
                            } else {
                                self.derived_n += 1;
                                const k = try std.fmt.allocPrint(self.arena, "__jk{d}", .{self.derived_n});
                                try right_computed.append(.{ .computed = .{ .name = k, .expr = try self.stripRightExpr(re, rname) } });
                                try key_cols.append(k);
                                try right_keys.append(try self.qualOne(k));
                            }
                            continue;
                        }
                    }
                    if (!try self.namesOtherSide(c, rname)) {
                        const e = try self.stripRightExpr(c, rname);
                        narrow = if (narrow) |n| try self.mk(.{ .binary = .{ .op = .@"and", .l = n, .r = e } }) else e;
                    } else if (kind == .inner) {
                        try post_filters.append(c);
                    } else return self.fail(.{ .line = opos.line, .col = opos.col }, "this {s} JOIN's ON compares the left side otherwise than by `=` to a right column; only an inner join can take that — filter in WHERE, or in a CTE first", .{@tagName(kind)});
                }
                if (left_keys.items.len == 0)
                    return self.fail(.{ .line = opos.line, .col = opos.col }, "the ON of a join needs at least one `=` between a left and a right value to join by — `b.k = a.k`, or computed: `trim(b.k) = cast(a.k AS string)`", .{});
                if (narrow != null or right_computed.items.len > 0) {
                    // The right side, narrowed and its keys computed, as a binding
                    // of its own.
                    self.derived_n += 1;
                    const name = try std.fmt.allocPrint(self.arena, "__derived{d}_on", .{self.derived_n});
                    try self.let_names.append(name);
                    var st = std.array_list.Managed(ast.Stage).init(self.arena);
                    try st.append(.{ .node = .{ .ref = binding }, .hints = &.{}, .pos = jpos });
                    if (narrow) |n| try st.append(.{ .node = .{ .filter = n }, .hints = &.{}, .pos = jpos });
                    if (right_computed.items.len > 0) {
                        try right_computed.insert(0, .star);
                        try st.append(.{ .node = .{ .select = try right_computed.toOwnedSlice() }, .hints = &.{}, .pos = jpos });
                    }
                    try self.pending_bindings.append(.{ .binding = .{ .name = name, .pipeline = .{ .stages = try st.toOwnedSlice(), .pos = jpos }, .pos = jpos } });
                    if (jalias == null) jalias = binding;
                    binding = name;
                }
                if (left_computed.items.len > 0) {
                    try left_computed.insert(0, .star);
                    try stages.append(.{ .node = .{ .select = try left_computed.toOwnedSlice() }, .hints = &.{}, .pos = jpos });
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
                    .alias = jalias orelse binding,
                    .left_keys = try left_keys.toOwnedSlice(),
                    .right_keys = try right_keys.toOwnedSlice(),
                } },
                .hints = try jhints.toOwnedSlice(),
                .pos = jpos,
            });
            for (post_filters.items) |c| {
                const e = try self.stripExpr(c, &aliases);
                try stages.append(.{ .node = .{ .filter = e }, .hints = &.{}, .pos = jpos });
            }
            post_filters.clearRetainingCapacity();
        }

        if (self.eatKw("where")) {
            const fpos = self.curPos();
            // Semi joins this WHERE owns start here: a subquery inside it registers
            // and lifts its own, and one already pending belongs to an enclosing
            // WHERE. Taking them all turned the outer `k IN (...)` into a join
            // inside the subquery, where `k` does not exist.
            const sj_base = self.pending_semijoins.items.len;
            self.in_where = true;
            const raw = try self.parseExpr();
            self.in_where = false;
            // Lift `IN (SELECT ...)` conjuncts BEFORE stripExpr: the sentinels
            // are matched by pointer, and a rewritten tree would orphan them.
            const remaining = if (self.pending_semijoins.items.len > sj_base)
                try self.liftSemiJoins(raw, fpos)
            else
                raw;
            if (remaining) |r| {
                const e = try self.stripExpr(r, &aliases);
                try stages.append(.{ .node = .{ .filter = e }, .hints = &.{}, .pos = fpos });
            }
            for (self.pending_semijoins.items[sj_base..]) |sj| {
                // `lhs` was checked to be a field at parse; strip any alias
                // qualifier the same way join keys written in ON do.
                const lk = try self.arena.alloc(ast.QualName, 1);
                lk[0] = stripQual(sj.lhs.field, &aliases);
                const rk = try self.arena.alloc(ast.QualName, 1);
                const rparts = try self.arena.alloc([]const u8, 1);
                rparts[0] = sj.right_col;
                rk[0] = .{ .parts = rparts };
                try stages.append(.{
                    .node = .{ .join = .{
                        .kind = if (sj.negated) .anti else .semi,
                        .binding = sj.binding,
                        .left_keys = lk,
                        .right_keys = rk,
                        .null_aware = sj.negated,
                    } },
                    .hints = &.{},
                    .pos = sj.pos,
                });
            }
            self.pending_semijoins.shrinkRetainingCapacity(sj_base);
        }
        if (key_cols.items.len > 0) {
            const drop = try self.arena.alloc(ast.SelectItem, 1);
            drop[0] = .{ .star_except = try key_cols.toOwnedSlice() };
            try stages.append(.{ .node = .{ .select = drop }, .hints = &.{}, .pos = pos });
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
        if (distinct_on) |on| for (on) |*k| {
            k.* = stripQual(k.*, &aliases);
        };
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
        } else if (win_funcs.items.len == 0) {
            const lone_star = items.items.len == 1 and items.items[0] == .star;
            if (!lone_star) {
                if (distinct_on) |on| distinct_drop = try self.distinctOnOutputs(&items, on);
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
        }

        // With a window, DISTINCT waits for its output: deduplicating before it
        // compared rows the window had not yet filled, and `SELECT DISTINCT k,
        // SUM(v) OVER (PARTITION BY k)` came back one row per input row.
        if (distinct and win_funcs.items.len == 0) {
            try stages.append(.{ .node = .{ .distinct = .{ .on = distinct_on } }, .hints = &.{}, .pos = pos });
            if (distinct_drop) |outs| try stages.append(.{ .node = .{ .select = outs }, .hints = &.{}, .pos = pos });
        }

        if (win_funcs.items.len > 0) {
            // A window's columns are referenced only inside `OVER (...)`, so the
            // projection above — computed from the ordinary SELECT items — prunes them
            // and the window operator then cannot resolve them. The error even migrates
            // as you project more of them by hand. Carry each reference through as a
            // hidden column and drop it after the window, which is what an outer
            // `ORDER BY` already does for its own keys.
            var refs = std.array_list.Managed([]const u8).init(self.arena);
            for (win_part.items) |q| try refs.append(q.last());
            for (win_ord.items) |k| try refs.append(k.field.last());
            for (win_funcs.items) |f| {
                if (f.arg) |q| try refs.append(q.last());
            }
            for (refs.items) |name| {
                var have = false;
                for (items.items) |it| switch (it) {
                    // A star already carries everything — except what it leaves
                    // out or renames away, which the window still has to read.
                    .star => have = true,
                    .star_except => |ex| {
                        var dropped = false;
                        for (ex) |x| {
                            if (std.ascii.eqlIgnoreCase(x, name)) dropped = true;
                        }
                        if (!dropped) have = true;
                    },
                    .star_rename => |rs| {
                        var moved = false;
                        for (rs) |r| {
                            if (std.ascii.eqlIgnoreCase(r.from, name)) moved = true;
                        }
                        if (!moved) have = true;
                    },
                    .field => |q| {
                        if (std.ascii.eqlIgnoreCase(q.last(), name)) have = true;
                    },
                    .computed => |c| {
                        if (std.ascii.eqlIgnoreCase(c.name, name)) have = true;
                    },
                };
                if (!have) try items.append(.{ .field = try self.singleName(name) });
            }
            // The projection is emitted here rather than by the branch below, since the
            // hidden columns have to be in it.
            if (items.items.len > 0) {
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
            try stages.append(.{ .node = .{ .window = .{
                .funcs = try win_funcs.toOwnedSlice(),
                .partition_by = try win_part.toOwnedSlice(),
                .order_by = try win_ord.toOwnedSlice(),
            } }, .hints = &.{}, .pos = pos });
            // Back to what the SELECT asked for: its own items, the hidden columns
            // gone, the window's outputs after them.
            // A `*` here already sees the window's outputs, which are then named again
            // after it: `SELECT *, ROW_NUMBER() ... AS rn` came out `k, v, rn, rn`.
            var out_items = std.array_list.Managed(ast.SelectItem).init(self.arena);
            for (post.items) |it| try out_items.append(switch (it) {
                .star => .{ .star_except = win_outs.items },
                .star_except => |ex| .{ .star_except = try std.mem.concat(self.arena, []const u8, &.{ ex, win_outs.items }) },
                else => it,
            });
            for (win_outs.items) |name| try out_items.append(.{ .field = try self.singleName(name) });
            try stages.append(.{ .node = .{ .select = try out_items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            if (distinct) try stages.append(.{ .node = .{ .distinct = .{ .on = distinct_on } }, .hints = &.{}, .pos = pos });
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

    /// A union arm that is not a bare source: lower its stages to a binding and hand
    /// back a branch that reads it. The union operator aligns arms off their schemas,
    /// so it does not care whether an arm is a table or a query — only the parser
    /// used to.
    fn unionBranchFromStages(self: *Parser, branch_stages: []const ast.Stage) Error!ast.UnionBranch {
        self.derived_n += 1;
        const name = try std.fmt.allocPrint(self.arena, "__union{d}", .{self.derived_n});
        try self.let_names.append(name);
        try self.pending_bindings.append(.{ .binding = .{
            .name = name,
            .pipeline = .{ .stages = branch_stages, .pos = self.curPos() },
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

    /// `core (UNION [ALL | DISTINCT] [BY NAME] core)... [ANCHOR SCHEMA qual]` —
    /// collapse the cores into one union_ stage.
    ///
    /// Plain `UNION` lines branches up by position, `BY NAME` by column name; a chain
    /// is one or the other. A `UNION` without `ALL` removes duplicate rows from
    /// everything to its left, as in SQL: the prefix up to the last one is unioned
    /// and deduplicated, and any `UNION ALL` after it appends to that.
    fn parseUnionTail(self: *Parser, first: Core, stages: *std.array_list.Managed(ast.Stage)) Error!void {
        const pos = self.curPos();
        var cores = std.array_list.Managed(Core).init(self.arena);
        try cores.append(first);
        var ops = std.array_list.Managed(SetOpTok).init(self.arena);

        while (true) {
            const kind: SetOpTok.Kind = if (self.isKw("union")) .union_ else if (self.isKw("intersect")) .intersect else if (self.isKw("except")) .except else break;
            const opos = self.curPos();
            _ = self.advance();
            const all = self.eatKw("all");
            if (!all) _ = self.eatKw("distinct");
            var named = false;
            if (self.eatKw("by")) {
                try self.expectKw("name");
                named = true;
            }
            try ops.append(.{ .kind = kind, .all = all, .named = named, .pos = opos });
            try cores.append(try self.parseSelectCore());
        }

        for (ops.items) |o| if (o.kind != .union_) return self.parseSetOps(cores.items, ops.items, stages, pos);

        const by_name = ops.items[0].named;
        var last_distinct: usize = 0;
        for (ops.items, 1..) |o, i| {
            if (o.named != by_name)
                return self.fail(o.pos, "a chain of UNIONs lines its branches up one way: all `BY NAME` or all by position", .{});
            if (!o.all) last_distinct = i;
        }

        const cs = cores.items;
        if (!by_name) {
            if (self.isKw("anchor") or self.isKw("pushdown"))
                return self.fail(self.curPos(), "`{s}` applies to `UNION ALL BY NAME`; a plain UNION lines branches up by position", .{self.cur().text});
            if (last_distinct == 0) return self.appendUnion(cs, false, true, stages, pos);
            if (last_distinct == cs.len - 1) return self.appendUnion(cs, true, true, stages, pos);
            // `a UNION b UNION ALL c`: deduplicate a ∪ b, then append c.
            var prefix = std.array_list.Managed(ast.Stage).init(self.arena);
            try self.appendUnion(cs[0 .. last_distinct + 1], true, true, &prefix, pos);
            var branches = std.array_list.Managed(ast.UnionBranch).init(self.arena);
            try branches.append(try self.unionBranchFromStages(try prefix.toOwnedSlice()));
            for (cs[last_distinct + 1 ..]) |c| try branches.append(try self.unionBranchFromStages(c.stages));
            try stages.append(.{
                .node = .{ .union_ = .{ .branches = try branches.toOwnedSlice(), .positional = true, .pos = pos } },
                .hints = &.{},
                .pos = pos,
            });
            return;
        }
        if (last_distinct != 0 and last_distinct != cs.len - 1)
            return self.fail(pos, "`UNION BY NAME` followed by `UNION ALL BY NAME` is not supported; deduplicate the first part in a CTE", .{});
        return self.appendUnion(cs, last_distinct != 0, false, stages, pos);
    }

    const SetOpTok = struct {
        const Kind = enum { union_, intersect, except };
        kind: Kind,
        all: bool,
        named: bool,
        pos: Pos,
    };

    /// A chain holding `INTERSECT` or `EXCEPT`, all by position. `INTERSECT` binds
    /// tighter than `UNION` and `EXCEPT`, which then apply left to right — SQL's
    /// precedence, so `a UNION b INTERSECT c` is `a UNION (b INTERSECT c)`. Each step
    /// lowers to a two-branch union_ whose result the next step reads as a branch.
    fn parseSetOps(self: *Parser, cores: []const Core, ops: []const SetOpTok, stages: *std.array_list.Managed(ast.Stage), pos: Pos) Error!void {
        for (ops) |o| {
            if (o.named)
                return self.fail(o.pos, "`BY NAME` applies to a chain of UNIONs only; INTERSECT and EXCEPT line columns up by position", .{});
            if (o.all and o.kind != .union_) {
                const kw = if (o.kind == .intersect) "INTERSECT" else "EXCEPT";
                return self.fail(o.pos, "`{s} ALL` is not supported yet; `{s}` removes duplicates", .{ kw, kw });
            }
        }
        if (self.isKw("anchor") or self.isKw("pushdown"))
            return self.fail(self.curPos(), "`{s}` applies to `UNION ALL BY NAME`", .{self.cur().text});

        var terms = std.array_list.Managed([]const ast.Stage).init(self.arena);
        var joins = std.array_list.Managed(SetOpTok).init(self.arena);
        try terms.append(cores[0].stages);
        for (ops, cores[1..]) |o, c| {
            if (o.kind == .intersect) {
                const last = &terms.items[terms.items.len - 1];
                last.* = try self.setOpStages(last.*, c.stages, .intersect, false, pos);
            } else {
                try joins.append(o);
                try terms.append(c.stages);
            }
        }
        var acc = terms.items[0];
        for (joins.items, terms.items[1..]) |o, t| {
            acc = if (o.kind == .except)
                try self.setOpStages(acc, t, .except, false, pos)
            else
                try self.setOpStages(acc, t, .union_all, !o.all, pos);
        }
        try stages.appendSlice(acc);
    }

    /// `left <op> right` as a two-branch positional union_ stage, plus a DISTINCT for
    /// a plain UNION; intersect/except deduplicate on their own.
    fn setOpStages(self: *Parser, left: []const ast.Stage, right: []const ast.Stage, set: ast.SetOp, distinct: bool, pos: Pos) Error![]const ast.Stage {
        const branches = try self.arena.alloc(ast.UnionBranch, 2);
        branches[0] = try self.unionBranchFromStages(left);
        branches[1] = try self.unionBranchFromStages(right);
        var out = std.array_list.Managed(ast.Stage).init(self.arena);
        try out.append(.{ .node = .{ .union_ = .{ .branches = branches, .positional = true, .set = set, .pos = pos } }, .hints = &.{}, .pos = pos });
        if (distinct) try out.append(.{ .node = .{ .distinct = .{ .on = null } }, .hints = &.{}, .pos = pos });
        return out.toOwnedSlice();
    }

    /// One union_ stage over `cores`, then a whole-row DISTINCT when `distinct`.
    fn appendUnion(self: *Parser, cores: []const Core, distinct: bool, positional: bool, stages: *std.array_list.Managed(ast.Stage), pos: Pos) Error!void {
        var branches = std.array_list.Managed(ast.UnionBranch).init(self.arena);
        var tag_col: ?[]const u8 = null;
        for (cores) |core| {
            // A positional branch is always a general query: its tag literal is a
            // column like any other, not a by-name reconciliation tag.
            const b = if (positional) null else core.union_branch;
            if (b) |fb| {
                if (fb.tag_col) |tc| {
                    if (tag_col == null) tag_col = tc;
                    if (!std.mem.eql(u8, tag_col.?, tc))
                        return self.fail(self.curPos(), "all UNION branches must use the same tag column name (`{s}` vs `{s}`)", .{ tag_col.?, tc });
                }
                try branches.append(.{ .read = fb.read, .tag = fb.tag });
            } else {
                try branches.append(try self.unionBranchFromStages(core.stages));
            }
        }

        var hints = std.array_list.Managed(ast.Hint).init(self.arena);
        if (tag_col) |tc|
            try hints.append(.{ .key = "tag", .value = .{ .ident = tc }, .pos = pos });
        if (!positional) try self.parseUnionClauses(&hints, pos);

        try stages.append(.{
            .node = .{ .union_ = .{ .branches = try branches.toOwnedSlice(), .positional = positional, .pos = pos } },
            .hints = try hints.toOwnedSlice(),
            .pos = pos,
        });
        if (distinct) try stages.append(.{ .node = .{ .distinct = .{ .on = null } }, .hints = &.{}, .pos = pos });
    }

    /// A FROM source: CSV path, IDENTIFIER(<expr>) for a computed path,
    /// BODY(schema), HTTP('url'), a CTE reference, or a connection-qualified
    /// table / QUERY($$...$$). Registers the alias.
    /// `( <query> ) [AS] alias` in a FROM or JOIN position. Lowers to a binding — the
    /// same statement `WITH alias AS (...)` produces — so the caller can reference it
    /// as a `.ref` source or as a join's right side. No new execution machinery: a
    /// derived table *is* a CTE that happened to be written inline.
    ///
    /// The binding always gets a generated name no identifier can collide with; the
    /// alias names it only inside the query that wrote it. Bindings share one
    /// namespace per script, so binding under the alias let a second `r` anywhere
    /// replace the first — silently, when the two had the same columns.
    /// The table function called `name`, the latest declaration winning.
    fn findTableFn(self: *Parser, name: []const u8) ?ast.FnDecl {
        var i = self.table_fns.items.len;
        while (i > 0) {
            i -= 1;
            if (eqlNoCase(self.table_fns.items[i].name, name)) return self.table_fns.items[i];
        }
        return null;
    }

    /// A `WITH` name as bound: itself, or — inside a table function's body — a name
    /// unique to this call, which the body's own `FROM name` maps back to.
    fn cteName(self: *Parser, name: []const u8) Error![]const u8 {
        const f = self.tvf orelse return name;
        const bname = try std.fmt.allocPrint(self.arena, "__tvf{d}_{s}", .{ f.id, name });
        try f.ctes.append(.{ name, bname });
        return bname;
    }

    /// Parse a table function's body once where it is declared, so a mistake in it
    /// is reported there and not at the first call, and hand back its tokens for
    /// every call to re-parse. Nothing it would bind is kept.
    fn checkTableBody(self: *Parser) Error![]const Token {
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

    /// A `JOIN LATERAL f(o.col)` argument that names a column of the left side:
    /// where the body says `x = $param`, the join says `o.col = <x as output>`.
    const LateralKey = struct { left: ast.QualName, right: []const u8 };

    const TableCall = struct { binding: []const u8, keys: []const LateralKey = &.{} };

    /// `f(args)` in FROM or JOIN, the name already consumed: bind the arguments,
    /// re-parse the body with them, and lower the result to a binding exactly as a
    /// derived table is — so a filter on the call still descends to the source.
    /// `lateral` (a `JOIN LATERAL`) lets an argument be a left-side column; see
    /// `decorrelate`.
    fn callTableFn(self: *Parser, fd: ast.FnDecl, pos: Pos, place: enum { from, join, lateral }) Error!TableCall {
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
                    // The body sees a marker in its place, found again by pointer.
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

    const Correlated = struct { param: []const u8, left: ast.QualName, marker: *ast.Expr };

    /// Turn a body that a `JOIN LATERAL` passes a column into one the join can read
    /// once: each `x = $param` conjunct leaves the WHERE, `x` is made an output of
    /// the body (under its output name, or the parameter's when the SELECT list
    /// leaves it out), and the join matches it against the row's column. One read
    /// of the source and a hash join, instead of a query per row.
    ///
    /// Only where that changes nothing: the body is a read, joins, filters and a
    /// SELECT list. A LIMIT, DISTINCT, GROUP BY or window would see every row the
    /// equality used to keep out — "first item of the order" would become every
    /// item — so they are refused, as is any use of `$param` but `x = $param`
    /// (and the parameter echoed as a SELECT item, which is then `x`).
    fn decorrelate(self: *Parser, name: []const u8, pos: Pos, stages: *std.array_list.Managed(ast.Stage), inner: []const ast.Stmt, cor: []const Correlated) Error![]const LateralKey {
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

        // The SELECT list: a parameter echoed back is its column, and each
        // matched column needs an output name to join on.
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

    /// An error inside a table function's body is positioned in its declaration;
    /// say which call reached it.
    fn inTableFn(self: *Parser, e: Error, name: []const u8, pos: Pos) Error {
        // Named once, at the call the script wrote — not again at every level a
        // runaway `OR REPLACE` recursion went through (the defers have not run yet).
        if (e == error.ParseFailed and self.tvf_depth == 1)
            self.diag.msg = std.fmt.allocPrint(self.arena, "in `{s}` called at {d}:{d}: {s}", .{ name, pos.line, pos.col, self.diag.msg }) catch self.diag.msg;
        return e;
    }

    fn parseDerivedTable(self: *Parser) Error!Derived {
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
        // Inner bindings first: they are what this one reads from.
        try self.pending_bindings.appendSlice(inner_bindings.items);
        try self.pending_bindings.append(.{ .binding = .{
            .name = name,
            .pipeline = .{ .stages = try sub_stages.toOwnedSlice(), .pos = dpos },
            .pos = dpos,
        } });
        return .{ .binding = name, .alias = alias, .alias_pos = alias_pos };
    }

    const Derived = struct { binding: []const u8, alias: ?[]const u8, alias_pos: Pos };

    /// A join's right side that reads — a path, `IDENTIFIER(...)`, a connection's
    /// table or query — lowered as `(SELECT * FROM it) alias` would be: a binding
    /// of its own, its read hints (`WITH (sheet = ...)` after the alias) with it.
    fn parseJoinRead(self: *Parser) Error!Derived {
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

    /// A parenthesized query in expression or LET position, the `(` already
    /// consumed: parse it into a pipeline and route any bindings it creates to
    /// `pending_bindings`, exactly the way `parseDerivedTable` does.
    fn parseSubqueryPipeline(self: *Parser) Error!ast.Pipeline {
        const qpos = self.curPos();
        var sub_stages = std.array_list.Managed(ast.Stage).init(self.arena);
        var inner_bindings = std.array_list.Managed(ast.Stmt).init(self.arena);
        try self.parseQuery(&inner_bindings, &sub_stages);
        _ = try self.expect(.rparen);
        try self.pending_bindings.appendSlice(inner_bindings.items);
        return .{ .stages = try sub_stages.toOwnedSlice(), .pos = qpos };
    }

    /// Output name of a pipeline's single column, or null when there is not
    /// exactly one nameable column — walked back from the last stage the same
    /// way the schema will resolve at plan time.
    fn firstOutName(stages: []const ast.Stage) ?[]const u8 {
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
                // These keep the upstream column set; keep walking.
                .filter, .sort, .limit, .distinct => {},
                else => return null,
            }
        }
        return null;
    }

    /// What a stage is, for an error that refuses it.
    fn stageWord(n: ast.Stage.Node) []const u8 {
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
    fn stageMentions(st: ast.Stage, cor: []const Correlated) ?[]const u8 {
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

    fn splitConj(e: *ast.Expr, out: *std.array_list.Managed(*ast.Expr)) Error!void {
        if (e.* == .binary and e.binary.op == .@"and") {
            try splitConj(e.binary.l, out);
            try splitConj(e.binary.r, out);
        } else try out.append(e);
    }

    /// `col = $p` or `$p = col` for a `JOIN LATERAL` marker: which one, and `col`.
    fn eqOfMarker(e: *const ast.Expr, cor: []const Correlated) ?struct { cor: usize, col: ast.QualName } {
        if (e.* != .binary or e.binary.op != .eq) return null;
        const b = e.binary;
        for (cor, 0..) |c, i| {
            const other = if (b.l == c.marker) b.r else if (b.r == c.marker) b.l else continue;
            if (other.* != .field or other.field.dollar) return null;
            return .{ .cor = i, .col = other.field };
        }
        return null;
    }

    /// The name `col` leaves the SELECT list under, when the list passes it
    /// through — as itself, renamed by `AS`, or in a `*` — and null otherwise. A
    /// computed item that only shares the name (`trim(c) AS c`) is not it.
    fn outputNameOf(items: []const ast.SelectItem, col: ast.QualName) ?[]const u8 {
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

    /// Does `e` contain `needle` anywhere, by pointer? Used to reject an
    /// `IN (SELECT ...)` sentinel that sits under OR / NOT / any non-AND node,
    /// where dropping it would change the predicate's meaning.
    fn containsExpr(e: *const ast.Expr, needle: *const ast.Expr) bool {
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

    fn isSentinel(self: *Parser, e: *const ast.Expr) bool {
        for (self.pending_semijoins.items) |sj| {
            if (sj.sentinel == e) return true;
        }
        return false;
    }

    /// Remove the pending semi-join sentinels from the top-level AND spine of a
    /// WHERE predicate. Returns what remains of the predicate (null if the IN
    /// tests were all of it). A sentinel anywhere BUT the AND spine — under an
    /// OR, a NOT, a CASE — is an error: dropping it there would change what the
    /// predicate means, so those shapes stay unsupported rather than wrong.
    fn liftSemiJoins(self: *Parser, e: *ast.Expr, pos: Pos) Error!?*ast.Expr {
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
            if (std.mem.eql(u8, low, "min")) break :blk .min;
            if (std.mem.eql(u8, low, "max")) break :blk .max;
            if (std.mem.eql(u8, low, "avg")) break :blk .avg;
            // An aggregate with no window form — `median(x) OVER (...)` — was parsed
            // as a plain aggregate, and the error blamed a GROUP BY the query never had.
            if (aggFunc(low) != null and self.peekTag() == .lparen and self.overFollows(self.i))
                return self.fail(self.curPos(), "`{s}` is not a window function — over a window, basalt computes sum, count, min, max, avg, row_number, rank, dense_rank, lag and lead", .{low});
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
        if (kind == .sum or kind == .count or kind == .min or kind == .max or kind == .avg) {
            // `COUNT(*)` counts rows; anything else needs a column. Rewind rather than
            // fail, so a plain `SUM(x)` with no OVER is still an ordinary aggregate.
            if (self.eat(.star)) {
                // Only COUNT(*) is starrable.
                if (kind != .count) {
                    return self.notWindow(save);
                }
            } else if (self.at(.ident)) {
                arg = try self.singleName(self.advance().text);
            } else {
                return self.notWindow(save);
            }
        } else if (kind == .lag or kind == .lead) {
            if (!self.at(.ident)) {
                return self.notWindow(save);
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
        if (!self.eat(.rparen) or !self.isKw("over")) return self.notWindow(save);
        const wpos = self.curPos();
        _ = self.advance();
        _ = try self.expect(.lparen);

        // One window per query in stage one: two functions may share a window, but two
        // different windows would need a stage each.
        // Parse this function's window into locals, then either adopt it (first
        // function) or require that it matches — `MIN(v) OVER (w), MAX(v) OVER (w)` is
        // the natural pairing and must be allowed, while two *different* windows would
        // need a stage each.
        var this_part = std.array_list.Managed(ast.QualName).init(self.arena);
        var this_ord = std.array_list.Managed(ast.SortKey).init(self.arena);
        if (self.eatKw("partition")) {
            try self.expectKw("by");
            while (true) {
                try this_part.append(try self.singleName(try self.expectIdent()));
                if (!self.eat(.comma)) break;
            }
        }
        if (self.eatKw("order")) {
            try self.expectKw("by");
            while (true) {
                const f = try self.singleName(try self.expectIdent());
                var desc = false;
                if (self.eatKw("desc")) desc = true else _ = self.eatKw("asc");
                try this_ord.append(.{ .field = f, .desc = desc });
                if (!self.eat(.comma)) break;
            }
        }
        // `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, or `<n> PRECEDING`. The
        // end bound is always CURRENT ROW in this form; a FOLLOWING bound would need the
        // frame to look ahead and is not accepted yet.
        var this_frame: ast.WinFrame = .{};
        if (self.eatKw("rows")) {
            const fpos = self.curPos();
            switch (kind) {
                .sum, .count, .min, .max, .avg => {},
                else => return self.fail(fpos, "a frame applies to an aggregate over a window; ROW_NUMBER, RANK, DENSE_RANK, LAG and LEAD do not take one", .{}),
            }
            try self.expectKw("between");
            if (self.eatKw("unbounded")) {
                try self.expectKw("preceding");
                this_frame = .{ .rows = true, .unbounded = true };
            } else {
                const n = try self.expect(.int);
                try self.expectKw("preceding");
                const p2 = std.fmt.parseInt(i64, n.text, 10) catch
                    return self.fail(fpos, "a frame's PRECEDING count must be an integer", .{});
                if (p2 < 0) return self.fail(fpos, "a frame's PRECEDING count must not be negative", .{});
                this_frame = .{ .rows = true, .preceding = p2 };
            }
            try self.expectKw("and");
            try self.expectKw("current");
            try self.expectKw("row");
        }
        _ = try self.expect(.rparen);

        // Only PARTITION BY and ORDER BY have to match: a frame is per function.
        // Comparing just those two let a second frame through and then ignored it,
        // so a running total beside a moving sum came back as the moving sum.
        if (funcs.items.len == 0) {
            try part.appendSlice(this_part.items);
            try ord.appendSlice(this_ord.items);
        } else {
            var same = this_part.items.len == part.items.len and this_ord.items.len == ord.items.len;
            if (same) {
                for (this_part.items, part.items) |a2, b2| {
                    if (!std.mem.eql(u8, a2.last(), b2.last())) same = false;
                }
                for (this_ord.items, ord.items) |a2, b2| {
                    if (!std.mem.eql(u8, a2.field.last(), b2.field.last()) or a2.desc != b2.desc) same = false;
                }
            }
            if (!same)
                return self.fail(wpos, "two window functions in one SELECT must share the same OVER (...) window", .{});
        }
        // Ranking and the offsets need an order to count along. An aggregate does not:
        // with no ORDER BY its frame is the whole partition, which is share-of-total.
        if (this_ord.items.len == 0 and switch (kind) {
            .sum, .count, .min, .max, .avg => false,
            else => true,
        })
            return self.fail(wpos, "a window function needs ORDER BY inside OVER (...) to number by", .{});

        const name = (try self.itemAlias()) orelse @tagName(kind);
        try funcs.append(.{ .kind = kind, .out = name, .arg = arg, .offset = offset, .frame = this_frame });
        return true;
    }

    /// Rewind a `name(...)` that `parseWindowItem` does not take — unless an `OVER`
    /// follows its closing parenthesis. Then it is a window function whose argument
    /// is not a plain column, and parsing it as an ordinary aggregate blamed a
    /// GROUP BY the query never had.
    fn notWindow(self: *Parser, save: usize) Error!bool {
        self.i = save;
        if (self.overFollows(save))
            return self.fail(self.curPos(), "a window function takes a plain column (or `*` for COUNT) — compute `{s}(...)`'s argument in a CTE or derived table first", .{self.toks[save].text});
        return false;
    }

    /// Whether the call whose name is token `name_at` is followed, past its closing
    /// parenthesis, by `OVER`.
    fn overFollows(self: *Parser, name_at: usize) bool {
        var j = name_at + 1;
        var depth: usize = 0;
        while (j < self.toks.len) : (j += 1) {
            switch (self.toks[j].tag) {
                .lparen => depth += 1,
                .rparen => {
                    depth -= 1;
                    if (depth == 0) break;
                },
                .eof => return false,
                else => {},
            }
        }
        return j + 1 < self.toks.len and self.toks[j + 1].tag == .ident and eqlNoCase(self.toks[j + 1].text, "over");
    }

    fn parseFromSource(self: *Parser, aliases: *AliasSet, read_hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
        var node: ast.Stage.Node = undefined;
        if (self.at(.lparen)) {
            const d = try self.parseDerivedTable();
            if (d.alias) |a| try self.claimAlias(aliases, a, d.alias_pos, .left);
            return .{ .ref = d.binding };
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
            if (self.at(.lparen)) {
                const fd = self.findTableFn(head) orelse
                    return self.fail(pos, "unknown table function `{s}` — declare it with CREATE FUNCTION {s}(...) RETURNS TABLE AS SELECT ...", .{ head, head });
                const bname = (try self.callTableFn(fd, pos, .from)).binding;
                _ = self.eatKw("as");
                if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
                    try self.claimAlias(aliases, self.advance().text, self.prevPos(), .left);
                } else try self.claimAlias(aliases, head, pos, .left);
                return .{ .ref = bname };
            }
            if (self.tvf) |f| if (f.cte(head)) |bname| {
                node = .{ .ref = bname };
                self.eatAliasAs();
                if (self.at(.ident) and !isReservedAfterSource(self.cur().text))
                    try self.claimAlias(aliases, self.advance().text, self.prevPos(), .left);
                return node;
            };
            const http = if (self.connType(head)) |k| std.mem.eql(u8, k, "http") else false;
            if (self.at(.dot) and http) {
                _ = self.advance();
                if (self.isHttpVerb()) {
                    node = .{ .read = try self.parseHttpCall(head, read_hints) };
                } else if (self.at(.string)) {
                    node = .{ .read = .{ .connector = head, .form = .{ .path = self.advance().text } } };
                } else {
                    const npos = self.curPos();
                    const name = try self.expectIdent();
                    const r = self.findResource(head, name) orelse
                        return self.fail(npos, "`{s}.{s}`: no such resource — declare it with CREATE RESOURCE {s}.{s} AS GET('/...'), or read {s}.GET('/...')", .{ head, name, head, name, head });
                    try read_hints.appendSlice(r.hints);
                    node = .{ .read = .{ .connector = head, .form = .{ .path = r.path } } };
                }
            } else if (self.at(.dot)) {
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
        self.eatAliasAs();
        if (self.at(.ident) and !isReservedAfterSource(self.cur().text)) {
            try self.claimAlias(aliases, self.advance().text, self.prevPos(), .left);
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
        // Its CTEs, derived tables and table-function calls become bindings like
        // any query's, placed ahead of the statement that discovers through them
        // (`parseForEach` / the enclosing `parseQuery`). They used to be refused —
        // "may not declare CTEs" even of a query that only called a function.
        try self.pending_bindings.appendSlice(hoisted.items);
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
        // http_client.zig reads the mode off a `paginate` hint; a bare flag key
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

    /// `pre` receives the bindings the discovery source needs, which run ahead of
    /// the loop — not inside its body, where the body's first query would take them.
    fn parseForEach(self: *Parser, pre: *std.array_list.Managed(ast.Stmt)) Error!ast.ForEach {
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
        const start = self.curPos();
        var parts = std.array_list.Managed([]const u8).init(self.arena);
        try parts.append(try self.expectColName());
        while (self.at(.dot) and (self.peekTag() == .ident or self.peekTag() == .qident or self.peekTag() == .string)) {
            _ = self.advance();
            try parts.append(try self.expectColName());
        }
        return .{ .parts = try parts.toOwnedSlice(), .span = self.spanFrom(start) };
    }

    /// Rewrite `alias.x` -> `x` in an expression tree.
    /// Every column an expression reads. Used to keep a pre-aggregation
    /// projection from dropping inputs the aggregates still need.
    fn collectFields(self: *Parser, e: *const ast.Expr, out: *std.array_list.Managed(ast.QualName)) Error!void {
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
            // Every kind below used to fall to the `?` at the end, which made
            // `sum(v*2)` and `sum(v+100)` both key as `sum(?)` — so the second
            // aggregate bound to the first's accumulator and the answer was
            // silently `2 * sum(v*2)`. The key has to carry the argument's
            // structure, not just its shape.
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
            // Rendered, not collapsed: `count_if(json_any(a, t -> t = 1))` and the
            // same over `t = 2` are two aggregates, and one key would merge them.
            .lambda => |l| {
                for (l.params, 0..) |pp, k| {
                    if (k > 0) buf.append(',') catch return error.OutOfMemory;
                    buf.appendSlice(pp) catch return error.OutOfMemory;
                }
                buf.appendSlice("->") catch return error.OutOfMemory;
                try self.exprKey(l.body, buf);
            },
            .lambda_var => |n| buf.appendSlice(n) catch return error.OutOfMemory,
            // `match` and `let ... in` still collapse; they cannot appear as an
            // aggregate argument today, and a distinct key for them would need the
            // whole arm list rendered.
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
    /// Only a `$name` is a PARAM or LET; a bare name is a column even when a
    /// PARAM shares it.
    fn constItemExpr(self: *Parser, e: *const ast.Expr) bool {
        return switch (e.*) {
            .int_lit, .float_lit, .str_lit, .bool_lit, .null_lit => true,
            // Every `$name` is script scope — a PARAM, a LET, a loop variable or a
            // JSON-param path — so one value per query. Not only the ones declared
            // in this file: an `@include`d PARAM is parsed elsewhere, and a name
            // nothing binds is refused by name at plan time.
            .field => |q| q.dollar,
            // a lambda is constant when its body is, its parameter included
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

    /// Which sides `e` names: a column of the join's right side (`rname.col`),
    /// and any other — a left column, or one written without its table.
    fn sidesOf(self: *Parser, e: *ast.Expr, rname: []const u8) Error!struct { right: bool, other: bool } {
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

    fn qualOne(self: *Parser, name: []const u8) Error!ast.QualName {
        const parts = try self.arena.alloc([]const u8, 1);
        parts[0] = name;
        return .{ .parts = parts };
    }

    /// Whether `e` names a column other than one of the join's right side
    /// (`rname.col`) — a left column, or one written without its table.
    fn namesOtherSide(self: *Parser, e: *ast.Expr, rname: []const u8) Error!bool {
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

    /// `e` with the join's right name taken off its columns: `sra.x` is `x` to
    /// the right side's own rows.
    fn stripRightExpr(self: *Parser, e: *ast.Expr, rname: []const u8) Error!*ast.Expr {
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

    /// One argument of a call: an expression, or a lambda `x -> body` /
    /// `(acc, x) -> body` — which only the JSON array functions accept; anywhere
    /// else the type-checker says so.
    fn parseCallArg(self: *Parser) Error!*ast.Expr {
        const n = self.lambdaHead() orelse return self.parseExpr();
        const pos = self.curPos();
        const paren = self.at(.lparen);
        if (paren) _ = self.advance();
        const params = try self.arena.alloc([]const u8, n);
        for (params, 0..) |*pp, k| {
            if (k > 0) _ = self.advance(); // `,`
            const t = self.advance();
            for (params[0..k]) |prev| {
                if (std.mem.eql(u8, prev, t.text)) return self.fail(.{ .line = t.line, .col = t.col }, "lambda parameter `{s}` is named twice", .{t.text});
            }
            pp.* = t.text;
        }
        if (paren) _ = self.advance(); // `)`
        _ = self.advance(); // `->`
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

    /// The parameter count when the tokens ahead open a lambda — `x ->` or
    /// `(a, b, …) ->` — else null, and an ordinary argument follows.
    fn lambdaHead(self: *Parser) ?usize {
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
    fn lambdaParam(self: *Parser, name: []const u8) ?[]const u8 {
        var i = self.lambda_n;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, self.lambda_names[i], name)) return self.lambda_names[i];
        }
        return null;
    }

    fn parseExpr(self: *Parser) Error!*ast.Expr {
        const pos = self.curPos();
        self.expr_nest += 1;
        defer self.expr_nest -= 1;
        const e = try self.parseBin(0);
        if (self.expr_nest == 1 and deeperThan(e, max_expr_depth))
            return self.fail(pos, "expression nests more than {d} levels deep — a long chain of `+`, `||` or comparisons; split it, or build the value in steps", .{max_expr_depth});
        return e;
    }

    /// Every pass over an expression — typing, evaluation, pushdown — recurses on
    /// the tree, so a deep enough one overflowed the stack: a 4000-element `IN`
    /// list passed `check` and then segfaulted in `run`. AND/OR chains and IN
    /// lists are balanced (`balanceChain`), so this limit only meets a chain of an
    /// operator that cannot be regrouped.
    const max_expr_depth = 1000;
    const max_paren_depth = 256;
    const max_query_depth = 32;

    /// Whether `e` is more than `limit` levels deep. Stops descending at the limit,
    /// so the check cannot itself exhaust the stack.
    fn deeperThan(e: *const ast.Expr, limit: usize) bool {
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

    /// A left-deep run of one AND or OR, rebuilt balanced: `a OR b OR c OR d` is
    /// `(a OR b) OR (c OR d)`. Both are associative under SQL's three-valued logic,
    /// and the operands stay in order, so results and short-circuiting are
    /// unchanged; the depth drops from n to log n.
    fn balanceChain(self: *Parser, e: *ast.Expr) Error!*ast.Expr {
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

    fn buildBalanced(self: *Parser, op: ast.BinOp, items: []const *ast.Expr) Error!*ast.Expr {
        if (items.len == 1) return items[0];
        const mid = items.len / 2;
        return self.mk(.{ .binary = .{ .op = op, .l = try self.buildBalanced(op, items[0..mid]), .r = try self.buildBalanced(op, items[mid..]) } });
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
                const inpos = self.curPos();
                _ = self.advance();
                _ = try self.expect(.lparen);
                // `IN (SELECT ...)`: a semi join (anti when negated) against an
                // anonymous binding, not a value list. The test itself becomes
                // a join STAGE, so a sentinel expression stands in for it here
                // and the WHERE handler lifts it out — which is why the shape
                // is only accepted as a top-level AND conjunct of WHERE.
                //
                // NOT IN is a null-aware anti join: SQL's three-valued NOT IN.
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
        return self.balanceChain(lhs);
    }

    fn parseUnary(self: *Parser) Error!*ast.Expr {
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
                // Inside a table function's body, its parameters are the call's arguments.
                if (self.tvf) |f| if (q.parts.len == 1) if (f.arg(q.parts[0])) |a| return a;
                return self.mk(.{ .field = q });
            },
            .lparen => {
                // `(SELECT ...)` in expression position: a scalar subquery.
                // Desugars to an anonymous query LET emitted ahead of the
                // enclosing statement plus a `$name`-style reference here, so
                // the value is a plain constant by the time the outer pipeline
                // plans — and the comparison it sits in can push down.
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
                    // the subquery's value, bound as a LET is
                    return self.mk(.{ .field = .{ .parts = parts, .dollar = true } });
                }
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
                // `IDENTIFIER(<expr>)` names a column computed per row: a field whose
                // name is a `${...}` template, rendered before the pipeline is planned.
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
                    // An aggregate folds one argument; a second used to be dropped
                    // without a word, so `SUM(x, id)` answered `SUM(x)`.
                    if (aggregates.lookup(lower)) |f| if (args.items.len > aggregates.spec(f).max_args)
                        return self.fail(.{ .line = t.line, .col = t.col }, "`{s}` takes one argument, not {d}", .{ lower, args.items.len });
                    // the name alone: an error about the call is about its name
                    const name_span = ast.Span{ .start = .{ .line = t.line, .col = t.col }, .end = .{ .line = t.end_line, .col = t.end_col } };
                    return self.mk(.{ .call = .{ .name = lower, .args = try args.toOwnedSlice(), .distinct = call_distinct, .span = name_span } });
                }
                // inside a lambda's body, its parameter — not a column of that name
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
    fn parseQualNameField(self: *Parser) Error!ast.QualName {
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

/// The conjuncts of `e`, its top-level ANDs taken apart.
fn splitAnd(e: *ast.Expr, out: *std.array_list.Managed(*ast.Expr)) !void {
    if (e.* == .binary and e.binary.op == .@"and") {
        try splitAnd(e.binary.l, out);
        return splitAnd(e.binary.r, out);
    }
    try out.append(e);
}

fn qualHasPrefix(q: ast.QualName, prefix: []const u8) bool {
    return q.parts.len > 1 and std.mem.eql(u8, q.parts[0], prefix);
}

fn stripPrefix(q: ast.QualName, prefix: []const u8) ast.QualName {
    if (qualHasPrefix(q, prefix)) return .{ .parts = q.parts[1..], .safe = if (q.safe.len > 0) q.safe[1..] else &.{} };
    return q;
}

fn stripQual(q: ast.QualName, aliases: *const AliasSet) ast.QualName {
    if (!q.dollar and q.parts.len > 1 and aliases.strips(q.parts[0]))
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

test "sql: an aggregate refuses a second argument instead of dropping it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT SUM(x, id) AS s FROM 'in.csv';", &diag));
    try testing.expectEqualStrings("`sum` takes one argument, not 2", diag.msg);
    try testing.expectEqual(@as(u32, 8), diag.col);
    // Inside an expression too, where the call is lifted out of it.
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT round(stddev(x, 2), 2) AS s FROM 'in.csv';", &diag));
    try testing.expectEqualStrings("`stddev` takes one argument, not 2", diag.msg);
    _ = try parseSource(a, "SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS b, SUM(x) AS c FROM 'in.csv';", &diag);
}

test "sql: an aggregate with no window form says so before OVER" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    // It used to parse as a plain aggregate and blame a GROUP BY the query never had.
    inline for (.{ "median", "stddev" }) |f| {
        try testing.expectError(error.ParseFailed, parseSource(a, "SELECT g, " ++ f ++ "(x) OVER (PARTITION BY g) AS s FROM 'in.csv';", &diag));
        try testing.expect(std.mem.startsWith(u8, diag.msg, "`" ++ f ++ "` is not a window function"));
        try testing.expectEqual(@as(u32, 11), diag.col);
    }
    _ = try parseSource(a, "SELECT g, median(x) AS m FROM 'in.csv' GROUP BY g;", &diag);
    _ = try parseSource(a, "SELECT g, x, SUM(x) OVER (PARTITION BY g ORDER BY x) AS s FROM 'in.csv';", &diag);
}

test "sql: a table function lowers each call to a binding, its parameters bound to the arguments" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS
        \\  SELECT id, x FROM 'o.csv' WHERE x >= $minx;
        \\SELECT id FROM paid(8);
    , &diag);
    try testing.expect(prog.stmts[1].func.body == .table);
    const b = prog.stmts[2].binding;
    try testing.expect(std.mem.startsWith(u8, b.name, "__tvf") and std.mem.endsWith(u8, b.name, "_paid"));
    // `$minx` is the argument itself, not a reference left for later.
    const f = b.pipeline.stages[1].node.filter;
    try testing.expectEqual(@as(i64, 8), f.binary.r.int_lit);
    try testing.expectEqualStrings(b.name, prog.stmts[3].output.stages[0].node.ref);
}

test "sql: two calls in one query get their own bindings, the body's CTEs included" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE FUNCTION byg(k STRING) RETURNS TABLE AS
        \\  WITH s AS (SELECT g, x FROM 'o.csv') SELECT x FROM s WHERE g = $k;
        \\SELECT * FROM byg('a') l JOIN byg('b') r ON l.x = r.x;
    , &diag);
    var names = std.array_list.Managed([]const u8).init(a);
    for (prog.stmts) |st| if (st == .binding) try names.append(st.binding.name);
    // two `s`es and two calls, every one distinct; the plain name `s` is never bound
    try testing.expectEqual(@as(usize, 4), names.items.len);
    for (names.items, 0..) |n, i| {
        try testing.expect(!std.mem.eql(u8, n, "s"));
        for (names.items[0..i]) |m| try testing.expect(!std.mem.eql(u8, m, n));
    }
}

test "sql: a table function call is checked where it is written" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const decl = "CREATE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS SELECT id, x FROM 'o.csv' WHERE x >= $minx;\n";
    const cases = [_]struct { q: []const u8, msg: []const u8 }{
        .{ .q = "SELECT * FROM paid(1, 2);", .msg = "`paid` expects 0 to 1 argument(s), got 2" },
        .{ .q = "SELECT * FROM paid('x');", .msg = "`paid`: argument 1 (`minx`) expects INT, got STRING" },
        .{ .q = "SELECT * FROM paid(id);", .msg = "`paid`: argument 1 (`minx`) must be a constant — a literal, a `$param`, or an expression over them" },
        .{ .q = "SELECT * FROM nope(1);", .msg = "unknown table function `nope` — declare it with CREATE FUNCTION nope(...) RETURNS TABLE AS SELECT ..." },
        .{ .q = "CREATE OR REPLACE FUNCTION paid(minx INT DEFAULT 0) RETURNS TABLE AS SELECT * FROM paid($minx);\nSELECT * FROM paid(1);", .msg = "in `paid` called at 3:15: table function `paid` nests more than 16 calls deep — does it reach itself?" },
    };
    for (cases) |c| {
        var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        try testing.expectError(error.ParseFailed, parseSource(a, try std.mem.concat(a, u8, &.{ decl, c.q }), &diag));
        try testing.expectEqualStrings(c.msg, diag.msg);
    }
    // The body is checked at its declaration, not at the first call.
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "CREATE FUNCTION bad() RETURNS TABLE AS SELECT id FROM 'o.csv' WHERE;", &diag));
    try testing.expectEqual(@as(u32, 1), diag.line);
    try testing.expectError(error.ParseFailed, parseSource(a, "CREATE FUNCTION bad() RETURNS TABLE AS 1 + 1;", &diag));
    try testing.expectEqualStrings("`bad`: a table function's body is a query — SELECT ... or WITH ...", diag.msg);
}

test "sql: a lambda's parameter is its own node in the body, never a column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a, "SELECT json_filter(tags, t -> t = name AND t <> 'x') AS v FROM 'in.csv';", &diag);
    const call = prog.stmts[1].output.stages[1].node.select[0].computed.expr.call;
    const l = call.args[1].lambda;
    try testing.expectEqualStrings("t", l.params[0]);
    const lhs = l.body.binary.l.binary;
    try testing.expectEqualStrings("t", lhs.l.lambda_var);
    // a column named in the body stays a column
    try testing.expectEqualStrings("name", lhs.r.field.parts[0]);
    // outside the lambda, `t` is a column again
    const p2 = try parseSource(a, "SELECT json_any(tags, t -> t = 1) AS v, t FROM 'in.csv';", &diag);
    try testing.expect(p2.stmts[1].output.stages[1].node.select[1] == .field);
    // `->` was a syntax error before; `a - > b` still is
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT a - > b AS z FROM 'in.csv';", &diag));

    // several parameters in parentheses, one in parentheses, and a parenthesised
    // argument that is not a lambda
    const p3 = try parseSource(a, "SELECT json_reduce(tags, 0, (acc, t, i) -> acc + t + i) AS v FROM 'in.csv';", &diag);
    const l3 = p3.stmts[1].output.stages[1].node.select[0].computed.expr.call.args[2].lambda;
    try testing.expectEqual(@as(usize, 3), l3.params.len);
    try testing.expectEqualStrings("acc", l3.body.binary.l.binary.l.lambda_var);
    try testing.expectEqualStrings("i", l3.body.binary.r.lambda_var);
    const p4 = try parseSource(a, "SELECT json_any(tags, (t) -> t = 1) AS v FROM 'in.csv';", &diag);
    try testing.expectEqual(@as(usize, 1), p4.stmts[1].output.stages[1].node.select[0].computed.expr.call.args[1].lambda.params.len);
    const p5 = try parseSource(a, "SELECT coalesce((a), b) AS v FROM 'in.csv';", &diag);
    try testing.expect(p5.stmts[1].output.stages[1].node.select[0].computed.expr.call.args[0].* == .field);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT json_reduce(tags, 0, (x, x) -> x) AS v FROM 'in.csv';", &diag));
}

test "sql: JOIN LATERAL refuses a body the join could not apply per row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cases = [_]struct { body: []const u8, says: []const u8 }{
        .{ .body = "SELECT x FROM 't.csv' WHERE k = $p LIMIT 1", .says = "a LIMIT" },
        .{ .body = "SELECT DISTINCT x FROM 't.csv' WHERE k = $p", .says = "DISTINCT" },
        .{ .body = "SELECT k, COUNT(*) AS n FROM 't.csv' WHERE k = $p GROUP BY k", .says = "GROUP BY" },
        .{ .body = "SELECT x FROM 't.csv' WHERE k > $p", .says = "may only appear as `column = $p`" },
        .{ .body = "SELECT x FROM 't.csv' WHERE k = $p OR x = 1", .says = "may only appear as `column = $p`" },
        .{ .body = "SELECT x, upper($p) AS u FROM 't.csv' WHERE k = $p", .says = "SELECT item on its own" },
        .{ .body = "SELECT x FROM 't.csv'", .says = "never says `column = $p`" },
    };
    for (cases) |c| {
        var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const src = try std.fmt.allocPrint(a, "CREATE FUNCTION f(p) RETURNS TABLE AS {s}; SELECT o.id FROM 'o.csv' o CROSS JOIN LATERAL f(o.id) i;", .{c.body});
        try testing.expectError(error.ParseFailed, parseSource(a, src, &diag));
        if (std.mem.indexOf(u8, diag.msg, c.says) == null) {
            std.debug.print("for `{s}` got: {s}\n", .{ c.body, diag.msg });
            return error.TestUnexpectedResult;
        }
    }
    // Without LATERAL, a column argument says how to pass one.
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "CREATE FUNCTION f(p) RETURNS TABLE AS SELECT x FROM 't.csv' WHERE k = $p; SELECT o.id FROM 'o.csv' o CROSS JOIN f(o.id) i;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "JOIN LATERAL") != null);
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

test "sql: a name used for two tables in one FROM is refused where it repeats" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    // Aliases resolve by name at parse time, so a second `r` sent every `r.x` to
    // the first one, and the error named a column that exists — in the other `r`.
    try testing.expectError(error.ParseFailed, parseSource(a,
        \\SELECT r.b FROM (SELECT a FROM 'x.csv') r JOIN (SELECT b FROM 'y.csv') r ON r.a = r.b;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`r` names two tables") != null);
    try testing.expectEqual(@as(u32, 72), diag.col);

    try testing.expectError(error.ParseFailed, parseSource(a,
        \\WITH p AS (SELECT a FROM 'x.csv') SELECT a FROM 'x.csv' p JOIN p ON a = a;
    , &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`p` names two tables") != null);

    // An unaliased self-join has one name per side and keeps working.
    _ = try parseTest(a, "WITH p AS (SELECT a FROM 'x.csv') SELECT a FROM p JOIN p ON a = a;");
}

test "sql: a path or a connection's table on a JOIN's right side reads in a binding of its own, AS optional" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\CREATE CONNECTION sr TYPE starrocks OPTIONS (host = 'h', database = 'd');
        \\SELECT * FROM 'x.xlsx' AS xl JOIN sr.db.t AS t ON t.id = xl.id JOIN 'y.csv' y WITH (delimiter = ';') ON y.k = xl.k;
    );
    // two reads lowered to bindings, ahead of the statement that joins them
    const t_read = prog.stmts[2].binding;
    try testing.expectEqualStrings("__derived1_t", t_read.name);
    try testing.expectEqualStrings("sr", t_read.pipeline.stages[0].node.read.connector);
    const y_read = prog.stmts[3].binding;
    try testing.expectEqualStrings("delimiter", y_read.pipeline.stages[0].hints[0].key);
    const st = prog.stmts[4].output.stages;
    try testing.expectEqualStrings("csv", st[0].node.read.connector);
    try testing.expectEqualStrings("t", st[1].node.join.alias);
    try testing.expectEqualStrings("id", st[1].node.join.right_keys[0].parts[0]);
    try testing.expectEqualStrings("y", st[2].node.join.alias);
}

test "sql: ON beyond keys — the right side alone narrows it, the rest of an inner join filters after" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\SELECT * FROM 'a.csv' AS a JOIN 'b.csv' AS b ON 1 = 1 AND b.del <> '*' AND b.id = a.id AND a.v = 'x';
    );
    // b's read, then b narrowed by `1 = 1 AND del <> '*'` in its own names
    const narrowed = prog.stmts[2].binding;
    try testing.expectEqualStrings("__derived2_on", narrowed.name);
    try testing.expectEqualStrings("__derived1_b", narrowed.pipeline.stages[0].node.ref);
    const f = narrowed.pipeline.stages[1].node.filter;
    try testing.expectEqualStrings("del", f.binary.r.binary.l.field.parts[0]);
    const st = prog.stmts[3].output.stages;
    try testing.expectEqualStrings("__derived2_on", st[1].node.join.binding);
    try testing.expectEqualStrings("b", st[1].node.join.alias);
    try testing.expectEqual(@as(usize, 1), st[1].node.join.left_keys.len);
    // `a.v = 'x'` after the join
    try testing.expect(st[2].node == .filter);

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM 'a.csv' a LEFT JOIN 'b.csv' b ON b.id = a.id AND a.v = 'x';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "only an inner join") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM 'a.csv' a JOIN 'b.csv' b ON b.del <> '*';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "at least one") != null);
}

test "sql: a computed key in ON — each side computes its own column, dropped after the join" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a,
        \\SELECT * FROM 'a.csv' AS a JOIN 'b.csv' AS b ON trim(b.code) = cast(a.code AS string) AND b.del <> '*';
    );
    // b: read, then narrowed and its key computed
    const rb = prog.stmts[2].binding.pipeline.stages;
    try testing.expect(rb[1].node == .filter);
    try testing.expect(rb[2].node.select[0] == .star);
    try testing.expectEqualStrings("code", rb[2].node.select[1].computed.expr.call.args[0].field.parts[0]);
    const st = prog.stmts[3].output.stages;
    // a computes its key before the join, and the keys go after it
    try testing.expect(st[1].node.select[0] == .star);
    const lk = st[1].node.select[1].computed.name;
    const j = st[2].node.join;
    try testing.expectEqualStrings(lk, j.left_keys[0].parts[0]);
    try testing.expectEqualStrings(rb[2].node.select[1].computed.name, j.right_keys[0].parts[0]);
    try testing.expectEqual(@as(usize, 2), st[3].node.select[0].star_except.len);
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
    // a computed key is a key: `t` computes it before the join
    const ck = try parseTest(a,
        \\LOAD INTO 'out.csv' AS
        \\WITH r AS (SELECT id FROM 'r.csv')
        \\SELECT * FROM 'in.csv' t JOIN r ON lower(t.id) = r.id;
    );
    try testing.expectEqualStrings("id", ck.stmts[2].output.stages[2].node.join.right_keys[0].parts[0]);

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

test "sql: plain UNION [ALL] lines up by position; INTERSECT binds tighter than UNION and EXCEPT" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // Every positional branch is a general query — a bound pipeline — and a plain
    // UNION deduplicates with a whole-row DISTINCT after the union.
    const all = try parseTest(a, "SELECT k FROM 'x.csv' UNION ALL SELECT k FROM 'y.csv';");
    const ua = all.stmts[all.stmts.len - 1].output.stages;
    try testing.expect(ua[0].node.union_.positional);
    try testing.expect(ua[0].node.union_.branches[0].pipeline != null);
    try testing.expect(ua[1].node != .distinct);

    const dis = try parseTest(a, "SELECT k FROM 'x.csv' UNION SELECT k FROM 'y.csv';");
    const ud = dis.stmts[dis.stmts.len - 1].output.stages;
    try testing.expect(ud[1].node == .distinct);

    // `a UNION b UNION ALL c`: a ∪ b is deduplicated first, and c appended to it.
    const mixed = try parseTest(a, "SELECT k FROM 'x.csv' UNION SELECT k FROM 'y.csv' UNION ALL SELECT k FROM 'z.csv';");
    const um = mixed.stmts[mixed.stmts.len - 1].output.stages;
    try testing.expectEqual(@as(usize, 2), um[0].node.union_.branches.len);
    try testing.expect(um[1].node != .distinct);

    // `a UNION b INTERSECT c` is `a UNION (b INTERSECT c)`: the outer step is the
    // union, and its right branch reads the intersect.
    const prec = try parseTest(a, "SELECT k FROM 'x.csv' UNION SELECT k FROM 'y.csv' INTERSECT SELECT k FROM 'z.csv';");
    const up = prec.stmts[prec.stmts.len - 1].output.stages;
    try testing.expectEqual(ast.SetOp.union_all, up[0].node.union_.set);
    try testing.expect(up[1].node == .distinct);
    const inner = up[0].node.union_.branches[1].pipeline.?.stages[0].node.ref;
    const right = for (prec.stmts) |st| {
        if (st == .binding and std.mem.eql(u8, st.binding.name, inner)) break st.binding.pipeline.stages;
    } else return error.TestUnexpectedResult;
    try testing.expectEqual(ast.SetOp.intersect, right[0].node.union_.set);

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' EXCEPT ALL SELECT k FROM 'y.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`EXCEPT ALL` is not supported yet") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' INTERSECT BY NAME SELECT k FROM 'y.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "`BY NAME` applies to a chain of UNIONs only") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' UNION ALL BY NAME SELECT k FROM 'y.csv' UNION ALL SELECT k FROM 'z.csv';", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "all `BY NAME` or all by position") != null);
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'x.csv' UNION ALL SELECT k FROM 'y.csv' ANCHOR SCHEMA first;", &diag));
}

test "sql: long IN lists and AND/OR chains are balanced; deeper nesting is an error, not a stack overflow" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // A 5000-element IN list was a 5000-deep OR chain: `check` passed, and every
    // recursive pass in `run` then overflowed the stack.
    var src = std.array_list.Managed(u8).init(a);
    try src.appendSlice("SELECT id FROM 'x.csv' WHERE id IN (");
    for (0..5000) |i| try src.writer().print("{s}{d}", .{ if (i == 0) "" else ",", i });
    try src.appendSlice(") AND (");
    for (0..5000) |i| try src.writer().print("{s}v = {d}", .{ if (i == 0) "" else " OR ", i });
    try src.appendSlice(");");
    const prog = try parseTest(a, src.items);
    const pred = prog.stmts[prog.stmts.len - 1].output.stages[1].node.filter;
    try testing.expect(!Parser.deeperThan(pred, 40));

    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    src.clearRetainingCapacity();
    try src.appendSlice("SELECT v");
    for (0..2000) |_| try src.appendSlice(" + v");
    try src.appendSlice(" AS s FROM 'x.csv';");
    try testing.expectError(error.ParseFailed, parseSource(a, src.items, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "more than 1000 levels deep") != null);

    src.clearRetainingCapacity();
    try src.appendSlice("SELECT ");
    for (0..5000) |_| try src.append('(');
    try src.append('v');
    for (0..5000) |_| try src.append(')');
    try src.appendSlice(" FROM 'x.csv';");
    try testing.expectError(error.ParseFailed, parseSource(a, src.items, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "levels of parentheses") != null);

    src.clearRetainingCapacity();
    try src.appendSlice("SELECT COUNT(*) AS c FROM ");
    for (0..200) |_| try src.appendSlice("(SELECT * FROM ");
    try src.appendSlice("'x.csv'");
    for (0..200) |_| try src.appendSlice(") q");
    try src.append(';');
    try testing.expectError(error.ParseFailed, parseSource(a, src.items, &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "queries nest more than") != null);
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

test "sql: IDENTIFIER(expr) in a column position -> a field named by a template" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\LOAD INTO '/tmp/o.csv' AS
        \\SELECT IDENTIFIER($c), COUNT(*) AS n FROM 'in.csv'
        \\GROUP BY IDENTIFIER($c) ORDER BY IDENTIFIER('k_' || $c) DESC;
    , &diag);
    const stages = prog.stmts[prog.stmts.len - 1].output.stages;
    var saw_agg = false;
    var saw_sort = false;
    for (stages) |st| switch (st.node) {
        .aggregate => |ag| {
            saw_agg = true;
            try testing.expectEqualStrings("${c}", ag.by[0].parts[0]);
        },
        .sort => |so| {
            saw_sort = true;
            try testing.expectEqualStrings("k_${c}", so.keys[0].field.parts[0]);
            try testing.expect(so.keys[0].desc);
        },
        else => {},
    };
    try testing.expect(saw_agg and saw_sort);
}

test "sql: DESCRIBE lowers to a describe-mode EXPLAIN that asks a table for no rows; SHOW TABLES to a catalog query" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE CONNECTION erp TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\DESCRIBE erp.public.orders;
        \\DESCRIBE 'x.csv' WITH (delimiter = ';');
        \\DESC SELECT 1 AS x;
        \\SHOW TABLES FROM erp.public LIKE 'ord%';
    , &diag);
    const t = prog.stmts[2].explain;
    try testing.expectEqual(ast.ExplainMode.describe, t.mode);
    try testing.expectEqualStrings("1 = 0", t.pipeline.stages[0].node.read.where);
    try testing.expect(t.pipeline.stages[t.pipeline.stages.len - 1].node == .write);
    try testing.expectEqualStrings("delimiter", prog.stmts[3].explain.pipeline.stages[0].hints[0].key);
    try testing.expectEqual(ast.ExplainMode.describe, prog.stmts[4].explain.mode);
    const show = prog.stmts[5].output.stages[0].node.read;
    try testing.expectEqualStrings("erp", show.connector);
    try testing.expect(std.mem.indexOf(u8, show.form.query, "INFORMATION_SCHEMA.TABLES") != null);
    try testing.expect(std.mem.indexOf(u8, show.form.query, "TABLE_SCHEMA = 'public'") != null);
    try testing.expect(std.mem.indexOf(u8, show.form.query, "LIKE 'ord%'") != null);

    try testing.expectError(error.ParseFailed, parseSource(a, "SHOW TABLES FROM nope;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "not a connection") != null);
}

fn hintStr(hints: []const ast.Hint, key: []const u8) ?[]const u8 {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, key)) return switch (h.value) {
            .str, .ident => |s| s,
            else => null,
        };
    }
    return null;
}

test "sql: http reads — GET/POST calls, connection clauses, resources" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'https://x/v1')
        \\  RETRY 3 ON (429) WITH (items = 'data');
        \\PARAM code STRING DEFAULT 'br';
        \\CREATE RESOURCE rc.countries AS GET('/countries', region = 'europe')
        \\  PAGINATE BY page (param = 'p');
        \\SELECT * FROM rc.GET('/alpha/' || $code, fields = 'name');
        \\SELECT * FROM rc.POST('/search', body = $$ {"q": 1} $$);
        \\SELECT * FROM rc.countries WITH (items = 'rows');
        \\SHOW TABLES FROM rc LIKE 'c%';
    , &diag);
    const conn = prog.stmts[1].connection;
    try testing.expectEqual(@as(i64, 3), conn.hints[0].value.int);
    try testing.expectEqualStrings("data", hintStr(conn.hints, "items").?);

    const get = prog.stmts[3].output.stages[0];
    try testing.expectEqualStrings("rc", get.node.read.connector);
    try testing.expectEqualStrings("/alpha/${code}", get.node.read.form.path);
    try testing.expectEqualStrings("name", hintStr(get.hints, "query:fields").?);

    const post = prog.stmts[4].output.stages[0];
    try testing.expectEqualStrings("/search", post.node.read.form.path);
    try testing.expectEqualStrings("post", hintStr(post.hints, "method").?);
    try testing.expectEqualStrings(" {\"q\": 1} ", hintStr(post.hints, "body").?);

    // A resource expands to its read; the read's own clauses come after, so win.
    const res = prog.stmts[5].output.stages[0];
    try testing.expectEqualStrings("/countries", res.node.read.form.path);
    try testing.expectEqualStrings("europe", hintStr(res.hints, "query:region").?);
    try testing.expectEqualStrings("page", hintStr(res.hints, "paginate").?);
    try testing.expectEqualStrings("rows", res.hints[res.hints.len - 1].value.str);

    const show = prog.stmts[6].output.stages;
    try testing.expectEqualStrings("range", show[0].node.read.connector);
    try testing.expect(show[1].node == .select);
    try testing.expect(show[2].node == .filter);

    const bad = [_]struct { src: []const u8, msg: []const u8 }{
        .{ .src = "CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'u'); SELECT * FROM rc.nope;", .msg = "no such resource" },
        .{ .src = "CREATE CONNECTION rc TYPE http OPTIONS (base_url = 'u'); SELECT * FROM rc.GET('/x', body = 'b');", .msg = "GET takes no body" },
        .{ .src = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h') RETRY 2;", .msg = "http` connections only" },
        .{ .src = "CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h'); CREATE RESOURCE pg.t AS GET('/t');", .msg = "postgres connection" },
        .{ .src = "CREATE RESOURCE nope.t AS GET('/t');", .msg = "not a connection" },
    };
    for (bad) |b| {
        try testing.expectError(error.ParseFailed, parseSource(a, b.src, &diag));
        try testing.expect(std.mem.indexOf(u8, diag.msg, b.msg) != null);
    }
}

test "sql: UNNEST(JSON_EACH(col)) is a json explode" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const prog = try parseTest(a, "SELECT id, tag FROM 'x.csv' CROSS JOIN UNNEST(JSON_EACH(tags)) AS tag;");
    const ex = prog.stmts[1].output.stages[1].node.explode;
    try testing.expectEqualStrings("tags", ex.field);
    try testing.expect(ex.json);
    try testing.expect(ex.delim == null);
}

test "sql: a statement function may open with the statement CASE; the scalar CASE stays an expression" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a,
        \\CREATE FUNCTION pick(x) AS CASE $x WHEN 'a' THEN 1 ELSE CASE WHEN $x = 'b' THEN 2 ELSE 0 END END;
        \\CREATE FUNCTION branch(x) AS
        \\  CASE $x
        \\    WHEN 'a' THEN PRINT 'a'; FOR EACH ROW OF (SELECT 1 AS n) AS (n) PRINT $n; END FOR;
        \\    ELSE PRINT CASE WHEN $x = '' THEN 'blank' ELSE $x END;
        \\  END CASE;
        \\END;
        \\SELECT pick('b') AS p;
    , &diag);
    try testing.expect(prog.stmts[1].func.body == .expr);
    try testing.expect(prog.stmts[2].func.body == .stmts);
    try testing.expect(prog.stmts[2].func.body.stmts[0] == .match);
}

test "sql: EXCEPT accepts IDENTIFIER(expr), lowered to a template name" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parseSource(a, "LOAD INTO '/tmp/o.csv' AS SELECT * EXCEPT (id, IDENTIFIER($ex)) FROM 'in.csv';", &diag);
    const items = prog.stmts[1].output.stages[1].node.select;
    try testing.expectEqualStrings("id", items[0].star_except[0]);
    try testing.expectEqualStrings("${ex}", items[0].star_except[1]);
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

test "sql: a computed GROUP BY key keeps the columns a CASE inside an aggregate reads" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const prog = try parseTest(ar.allocator(),
        \\SELECT CAST(DATE_TRUNC('month', d) AS DATE) AS m,
        \\       SUM(CASE WHEN status = 'x' THEN 1 ELSE 0 END) AS c
        \\FROM 't.csv' GROUP BY CAST(DATE_TRUNC('month', d) AS DATE);
    );
    const stages = prog.stmts[1].output.stages;
    // read | select (pre-aggregation) | aggregate | write
    try std.testing.expect(stages[1].node == .select);
    var has_status = false;
    for (stages[1].node.select) |it| {
        if (it == .field and std.mem.eql(u8, it.field.last(), "status")) has_status = true;
    }
    try std.testing.expect(has_status);
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

test "sql: IN (SELECT ...) lifts to a semi join stage beside the filter" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const prog = try parseSource(a, "SELECT k FROM 'f.csv' WHERE v < 10 AND k IN (SELECT k FROM 'd.csv') AND v > 2;", &diag);
    // The subquery becomes a binding ahead of the output statement.
    try testing.expect(prog.stmts[1] == .binding);
    const bname = prog.stmts[1].binding.name;
    const stages = prog.stmts[2].output.stages;
    // read | filter (both remaining conjuncts, IN dropped) | semi join | select
    try testing.expect(stages[1].node == .filter);
    try testing.expect(!containsTrueLit(stages[1].node.filter));
    try testing.expect(stages[2].node == .join);
    try testing.expectEqual(ast.JoinKind.semi, stages[2].node.join.kind);
    try testing.expectEqualStrings(bname, stages[2].node.join.binding);
    try testing.expectEqualStrings("k", stages[2].node.join.left_keys[0].parts[0]);
    try testing.expectEqualStrings("k", stages[2].node.join.right_keys[0].parts[0]);

    // NOT IN is the anti join; the IN being the whole WHERE leaves no filter.
    const prog2 = try parseSource(a, "SELECT k FROM 'f.csv' WHERE k NOT IN (SELECT k FROM 'd.csv');", &diag);
    const st2 = prog2.stmts[2].output.stages;
    try testing.expect(st2[1].node == .join);
    try testing.expectEqual(ast.JoinKind.anti, st2[1].node.join.kind);
}

/// True when a `bool_lit true` survives anywhere in the tree — a leftover
/// sentinel would read as an always-true conjunct, silently widening a filter.
fn containsTrueLit(e: *const ast.Expr) bool {
    return switch (e.*) {
        .bool_lit => |b| b,
        .binary => |b| containsTrueLit(b.l) or containsTrueLit(b.r),
        .unary => |u| containsTrueLit(u.e),
        else => false,
    };
}

test "sql: IN (SELECT ...) under OR / outside WHERE / multi-column are parse errors" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'f.csv' WHERE v = 1 OR k IN (SELECT k FROM 'd.csv');", &diag));
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT (k IN (SELECT k FROM 'd.csv')) AS f FROM 'f.csv';", &diag));
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT k FROM 'f.csv' WHERE k IN (SELECT k, v FROM 'd.csv');", &diag));
    // The literal-list form is untouched by any of this.
    const prog = try parseSource(a, "SELECT k FROM 'f.csv' WHERE k IN (1, 2, 3);", &diag);
    try testing.expect(prog.stmts[1].output.stages[1].node == .filter);
}

test "sql: scalar subquery desugars to a query LET ahead of the statement" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };

    const prog = try parseSource(a, "SELECT k FROM 'f.csv' WHERE v > (SELECT max(v) FROM 'f.csv');", &diag);
    try testing.expect(prog.stmts[1] == .let_const);
    const l = prog.stmts[1].let_const;
    try testing.expect(l.expr == null);
    try testing.expect(l.query != null);
    // The reference left behind is a field ref carrying the LET's name.
    const filt = prog.stmts[2].output.stages[1].node.filter;
    try testing.expectEqualStrings(l.name, filt.binary.r.field.parts[0]);

    // Explicit form: LET x = (SELECT ...);
    const prog2 = try parseSource(a, "LET hi = (SELECT max(v) FROM 'f.csv');\nSELECT k FROM 'f.csv' WHERE v > $hi;", &diag);
    try testing.expect(prog2.stmts[1] == .let_const);
    try testing.expect(prog2.stmts[1].let_const.query != null);
    try testing.expectEqualStrings("hi", prog2.stmts[1].let_const.name);
}

test "parse with recovery: every statement that fails is recorded, the rest still parse; a broken block ends it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    var errs = std.array_list.Managed(Diagnostic).init(a);

    const prog = try parseSourceOpts(a,
        \\SELECT 1 AS x;
        \\SELECT 1 +;
        \\SELECT 2 AS y;
        \\SELECT (1 AS w;
        \\SELECT 3 AS z;
    , &diag, .{ .errors = &errs });
    try testing.expectEqual(@as(usize, 2), errs.items.len);
    try testing.expectEqual(@as(u32, 2), errs.items[0].line);
    try testing.expectEqual(@as(u32, 4), errs.items[1].line);
    var outputs: usize = 0;
    for (prog.stmts) |s| if (s == .output) {
        outputs += 1;
    };
    try testing.expectEqual(@as(usize, 3), outputs);

    // inside a FOR body the next `;` is not the statement's end: stop there
    errs.clearRetainingCapacity();
    _ = try parseSourceOpts(a,
        \\SELECT 1 AS x;
        \\FOR EACH ROW OF (SELECT 1 AS n) AS (n)
        \\  SELECT 1 +;
        \\  SELECT 2 AS y;
        \\END FOR;
        \\SELECT 3 +;
    , &diag, .{ .errors = &errs });
    try testing.expectEqual(@as(usize, 1), errs.items.len);

    // without an error list, the first error still fails the parse
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT 1 +; SELECT 2 AS y;", &diag));
}

test "known tables: a name the session holds reads as a binding reference, FROM and JOIN" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    try testing.expectError(error.ParseFailed, parseSource(a, "SELECT * FROM enrich;", &diag));
    try testing.expect(std.mem.indexOf(u8, diag.msg, "unknown source `enrich`") != null);

    const prog = try parseSourceOpts(a,
        \\WITH r AS (SELECT range AS id FROM RANGE(3))
        \\SELECT * FROM enrich JOIN r ON enrich.id = r.id;
        \\SELECT * FROM r JOIN enrich ON r.id = enrich.id;
    , &diag, .{ .known_tables = &.{"enrich"} });
    var refs: usize = 0;
    for (prog.stmts) |s| if (s == .output) {
        if (s.output.stages[0].node == .ref and std.mem.eql(u8, s.output.stages[0].node.ref, "enrich")) refs += 1;
        for (s.output.stages) |st| if (st.node == .join and std.mem.eql(u8, st.node.join.binding, "enrich")) {
            refs += 1;
        };
    };
    try testing.expectEqual(@as(usize, 2), refs);

    try testing.expect(parseTypeStr(a, "decimal(10,2)").?.scale == 2);
    try testing.expect(parseTypeStr(a, "varchar(20)").?.kind == .string);
    try testing.expect(parseTypeStr(a, "timestamp").?.kind == .timestamp);
    try testing.expect(parseTypeStr(a, "nope") == null);
    try testing.expect(parseTypeStr(a, "int int") == null);
}

test "sql: a select item's alias may omit AS, but not swallow the clause after it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const prog = try parseTest(a, "SELECT category, SUM(value) total, MAX(value) \"top\" FROM 'in.csv' GROUP BY category;");
    try testing.expect(prog.stmts.len == 2);
    // `FROM`, `WHERE`, `GROUP` after an item are clauses, not names
    _ = try parseTest(a, "SELECT id FROM 'in.csv' WHERE id > 1 ORDER BY id LIMIT 2;");
    _ = try parseTest(a, "SELECT 1 x, 'a' y;");
}
