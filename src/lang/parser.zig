//! Recursive-descent parser for statements + a Pratt parser for expressions.
//! All AST nodes are allocated in a caller-provided arena. On failure the parser
//! fills `*Diagnostic` (first error wins) and returns `error.ParseFailed`.

const std = @import("std");
const tok = @import("token.zig");
const lex = @import("lexer.zig");
const ast = @import("ast.zig");
const types = @import("types.zig");

const Token = tok.Token;
const Tag = tok.Tag;
const Pos = ast.Pos;

pub const Diagnostic = struct { msg: []const u8, line: u32, col: u32 };

pub const Error = error{ ParseFailed, OutOfMemory };

const OpInfo = struct { op: ast.BinOp, lbp: u8, rbp: u8 };

/// Tokenize and parse a whole program from source.
pub fn parseSource(arena: std.mem.Allocator, src: []const u8, diag: *Diagnostic) Error!ast.Program {
    const toks = try lex.tokenize(arena, src);
    var p = Parser{ .arena = arena, .toks = toks, .diag = diag };
    for (toks) |t| {
        if (t.tag == .invalid) return p.fail(.{ .line = t.line, .col = t.col }, "invalid token `{s}`", .{t.text});
    }
    return p.parseProgram();
}

pub const Parser = struct {
    arena: std.mem.Allocator,
    toks: []const Token,
    i: usize = 0,
    diag: *Diagnostic,

    // --- cursor helpers ---

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
    fn isKw(self: *Parser, kw: []const u8) bool {
        const t = self.cur();
        return t.tag == .ident and std.mem.eql(u8, t.text, kw);
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
    /// A name part: an identifier or a `${var}` interpolation placeholder (whose
    /// text is carried through verbatim and rendered at plan time).
    fn atName(self: *Parser) bool {
        return self.at(.ident) or self.at(.interp);
    }
    fn expectName(self: *Parser) Error![]const u8 {
        if (self.atName()) return self.advance().text;
        return self.fail(self.curPos(), "expected identifier, found {s}", .{self.curTag().describe()});
    }
    /// A column name that may also be given as a quoted string, so `${var}` can
    /// build it (e.g. an upsert key `"${name}_EMPRESA"`). Used for column lists,
    /// not table references.
    fn expectColName(self: *Parser) Error![]const u8 {
        if (self.atName() or self.at(.string)) return self.advance().text;
        return self.fail(self.curPos(), "expected a column name, found {s}", .{self.curTag().describe()});
    }
    fn expectKw(self: *Parser, kw: []const u8) Error!void {
        if (self.eatKw(kw)) return;
        return self.fail(self.curPos(), "expected `{s}`, found {s}", .{ kw, self.curTag().describe() });
    }

    fn fail(self: *Parser, pos: Pos, comptime fmt: []const u8, args: anytype) Error {
        self.diag.* = .{
            .msg = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory formatting diagnostic",
            .line = pos.line,
            .col = pos.col,
        };
        return error.ParseFailed;
    }

    // --- program / statements ---

    pub fn parseProgram(self: *Parser) Error!ast.Program {
        var stmts = std.array_list.Managed(ast.Stmt).init(self.arena);
        var first = true;
        while (!self.at(.eof)) {
            try stmts.append(try self.parseStmt(first));
            first = false;
        }
        return .{ .stmts = try stmts.toOwnedSlice() };
    }

    fn parseStmt(self: *Parser, is_first: bool) Error!ast.Stmt {
        if (self.at(.at)) {
            if (!is_first) return self.fail(self.curPos(), "the @kind tag must be the first declaration", .{});
            return .{ .kind = try self.parseKind() };
        }
        if (is_first) return self.fail(self.curPos(), "script must begin with a @kind tag (@batch or @http)", .{});

        if (self.isKw("param")) return .{ .param = try self.parseParam() };
        if (self.isKw("connection")) return .{ .connection = try self.parseConnection() };
        if (self.isKw("let")) return .{ .binding = try self.parseLet() };
        if (self.isKw("fn")) return .{ .func = try self.parseFnDecl() };
        if (self.isKw("for")) return .{ .for_each = try self.parseForEach() };
        if (self.isKw("match")) return .{ .match = try self.parseStmtMatch() };
        if (self.at(.ident)) return .{ .output = try self.parsePipeline() };
        return self.fail(self.curPos(), "expected a declaration (param/connection/let) or a pipeline", .{});
    }

    fn parseKind(self: *Parser) Error!ast.KindDecl {
        const pos = self.curPos();
        _ = try self.expect(.at);
        const name = try self.expectIdent();
        const kind: ast.Kind = if (std.mem.eql(u8, name, "batch"))
            .batch
        else if (std.mem.eql(u8, name, "http"))
            .http
        else
            return self.fail(pos, "unknown @kind `{s}` (expected batch or http)", .{name});

        var config: []const ast.Attr = &[_]ast.Attr{};
        if (self.eat(.lparen)) {
            config = try self.parseParenAttrs();
            _ = try self.expect(.rparen);
        }
        return .{ .kind = kind, .config = config, .pos = pos };
    }

    fn parseParenAttrs(self: *Parser) Error![]const ast.Attr {
        var list = std.array_list.Managed(ast.Attr).init(self.arena);
        if (self.at(.rparen)) return try list.toOwnedSlice();
        while (true) {
            const pos = self.curPos();
            const key = try self.expectIdent();
            _ = try self.expect(.assign);
            const val = try self.parseExpr();
            try list.append(.{ .key = key, .value = val, .pos = pos });
            if (!self.eat(.comma)) break;
        }
        return try list.toOwnedSlice();
    }

    fn parseParam(self: *Parser) Error!ast.Param {
        const pos = self.curPos();
        try self.expectKw("param");
        const name = try self.expectIdent();
        // `json` is a pseudo-type: the value is a JSON document, not a column.
        var is_json = false;
        var ty: types.Type = undefined;
        if (self.at(.ident) and std.mem.eql(u8, self.cur().text, "json")) {
            _ = self.advance();
            is_json = true;
            ty = types.Type.init(.string); // placeholder; never used for JSON params
        } else {
            ty = try self.parseTypeName();
        }
        var default: ?*ast.Expr = null;
        if (self.eat(.assign)) default = try self.parseExpr();
        var source: ?ast.ParamSource = null;
        if (self.eatKw("from")) {
            const sname = try self.expectIdent();
            source = if (std.mem.eql(u8, sname, "query"))
                .query
            else if (std.mem.eql(u8, sname, "body"))
                .body
            else if (std.mem.eql(u8, sname, "header"))
                .header
            else
                return self.fail(self.curPos(), "unknown param source `{s}` (expected query, body, or header)", .{sname});
        }
        return .{ .name = name, .ty = ty, .default = default, .source = source, .pos = pos, .is_json = is_json };
    }

    fn parseConnection(self: *Parser) Error!ast.Connection {
        const pos = self.curPos();
        try self.expectKw("connection");
        const name = try self.expectIdent();
        _ = try self.expect(.assign);
        const connector = try self.expectIdent();
        // config: a run of `ident = expr` pairs, delimited by `ident =` lookahead.
        var list = std.array_list.Managed(ast.Attr).init(self.arena);
        while (self.at(.ident) and self.peekTag() == .assign) {
            const apos = self.curPos();
            const key = self.advance().text;
            _ = try self.expect(.assign);
            const val = try self.parseExpr();
            try list.append(.{ .key = key, .value = val, .pos = apos });
        }
        return .{ .name = name, .connector = connector, .config = try list.toOwnedSlice(), .pos = pos };
    }

    /// `fn name(p1, p2, ...) = <expr>`
    fn parseFnDecl(self: *Parser) Error!ast.FnDecl {
        const pos = self.curPos();
        try self.expectKw("fn");
        const name = try self.expectIdent();
        _ = try self.expect(.lparen);
        var params = std.array_list.Managed([]const u8).init(self.arena);
        if (!self.at(.rparen)) {
            try params.append(try self.expectIdent());
            while (self.eat(.comma)) try params.append(try self.expectIdent());
        }
        _ = try self.expect(.rparen);
        _ = try self.expect(.assign);
        const body = try self.parseExpr();
        return .{ .name = name, .params = try params.toOwnedSlice(), .body = body, .pos = pos };
    }

    fn parseLet(self: *Parser) Error!ast.Let {
        const pos = self.curPos();
        try self.expectKw("let");
        const name = try self.expectIdent();
        _ = try self.expect(.assign);
        const pipeline = try self.parsePipeline();
        return .{ .name = name, .pipeline = pipeline, .pos = pos };
    }

    /// `for <var> in <source-read> @[...] <body-pipeline>`. The source is a read
    /// without the leading `read` keyword (e.g. `mssql query "..."`); the body is
    /// the single pipeline that follows.
    fn parseForEach(self: *Parser) Error!ast.ForEach {
        const pos = self.curPos();
        try self.expectKw("for");
        var names = std.array_list.Managed([]const u8).init(self.arena);
        try names.append(try self.expectIdent());
        while (self.eat(.comma)) try names.append(try self.expectIdent());
        try self.expectKw("in");
        const source = try self.parseForSource();
        const hints = try self.parseHints();
        const body = try self.parsePipeline();
        return .{ .var_names = try names.toOwnedSlice(), .source = source, .hints = hints, .body = body, .pos = pos };
    }

    /// A discovery `read` (`<conn> table|query|<path>`) or a JSON-array path
    /// (`job.tables`). Distinguished by lookahead: a read has `table`/`query`/a
    /// string after the connector; anything else is a (dotted) param path.
    fn parseForSource(self: *Parser) Error!ast.ForSource {
        const save = self.i;
        _ = try self.expectIdent();
        const is_read = self.isKw("table") or self.isKw("query") or self.at(.string);
        self.i = save;
        if (is_read) return .{ .read = try self.parseRead() };
        return .{ .json_path = try self.parseQualName() };
    }

    // --- statement-level match (plan-time structural dispatch) ---

    /// Mirrors `parseMatchExpr` but each arm body is a `{ ... }` statement block.
    fn parseStmtMatch(self: *Parser) Error!ast.StmtMatch {
        const pos = self.curPos();
        _ = self.advance(); // match
        var subject: ?*ast.Expr = null;
        if (!self.isKw("end") and !self.isWildcard()) {
            const save = self.i;
            const e = try self.parseExpr();
            if (self.at(.fat_arrow)) {
                self.i = save; // guard form: rewind, no subject
            } else {
                subject = e;
            }
        }
        var arms = std.array_list.Managed(ast.StmtArm).init(self.arena);
        while (!self.isKw("end")) try arms.append(try self.parseStmtArm(subject != null));
        try self.expectKw("end");
        return .{ .subject = subject, .arms = try arms.toOwnedSlice(), .pos = pos };
    }

    fn parseStmtArm(self: *Parser, subject_form: bool) Error!ast.StmtArm {
        if (self.isWildcard()) {
            _ = self.advance();
            _ = try self.expect(.fat_arrow);
            return .{ .pats = &[_]*ast.Expr{}, .guard = null, .body = try self.parseBlock(), .is_default = true };
        }
        if (subject_form) {
            var pats = std.array_list.Managed(*ast.Expr).init(self.arena);
            try pats.append(try self.parseExpr());
            while (self.eat(.pipe)) try pats.append(try self.parseExpr());
            _ = try self.expect(.fat_arrow);
            return .{ .pats = try pats.toOwnedSlice(), .guard = null, .body = try self.parseBlock(), .is_default = false };
        }
        const g = try self.parseExpr();
        _ = try self.expect(.fat_arrow);
        return .{ .pats = &[_]*ast.Expr{}, .guard = g, .body = try self.parseBlock(), .is_default = false };
    }

    /// A `{ ... }` block of zero or more statements (an empty block is an explicit no-op).
    fn parseBlock(self: *Parser) Error![]const ast.Stmt {
        _ = try self.expect(.lbrace);
        var stmts = std.array_list.Managed(ast.Stmt).init(self.arena);
        while (!self.at(.rbrace) and !self.at(.eof)) try stmts.append(try self.parseStmt(false));
        _ = try self.expect(.rbrace);
        return try stmts.toOwnedSlice();
    }

    // --- pipelines / stages ---

    fn parsePipeline(self: *Parser) Error!ast.Pipeline {
        const pos = self.curPos();
        var stages = std.array_list.Managed(ast.Stage).init(self.arena);
        try stages.append(try self.parseStage());
        while (self.eat(.pipe)) try stages.append(try self.parseStage());
        return .{ .stages = try stages.toOwnedSlice(), .pos = pos };
    }

    fn parseStage(self: *Parser) Error!ast.Stage {
        const pos = self.curPos();
        const node = try self.parseStageNode();
        const hints = try self.parseHints();
        return .{ .node = node, .hints = hints, .pos = pos };
    }

    fn parseStageNode(self: *Parser) Error!ast.Stage.Node {
        const name = try self.expectIdent();
        if (std.mem.eql(u8, name, "read")) return .{ .read = try self.parseRead() };
        if (std.mem.eql(u8, name, "union")) return .{ .union_ = try self.parseUnion() };
        if (std.mem.eql(u8, name, "filter")) return .{ .filter = try self.parseExpr() };
        if (std.mem.eql(u8, name, "select")) return .{ .select = try self.parseSelect() };
        if (std.mem.eql(u8, name, "explode")) return .{ .explode = try self.parseExplode() };
        if (std.mem.eql(u8, name, "limit")) return .{ .limit = try self.parseLimit() };
        if (std.mem.eql(u8, name, "write")) return .{ .write = try self.parseWrite() };
        if (std.mem.eql(u8, name, "distinct")) return .{ .distinct = try self.parseDistinct() };
        if (std.mem.eql(u8, name, "sort")) return .{ .sort = try self.parseSort() };
        if (std.mem.eql(u8, name, "aggregate")) return .{ .aggregate = try self.parseAggregate() };
        if (std.mem.eql(u8, name, "join")) return .{ .join = try self.parseJoin() };
        // otherwise: a binding used as a source.
        return .{ .ref = name };
    }

    /// `union from <conn> <src> as "<tag>" ...` (explicit branches) or
    /// `union <conn> tables "<query -> (table, tag)>"` (discovered). The `tag`/`canon`
    /// options ride on the stage's `@[...]` hints, parsed by parseStage.
    fn parseUnion(self: *Parser) Error!ast.Union {
        const pos = self.curPos();
        if (self.isKw("from")) {
            var branches = std.array_list.Managed(ast.UnionBranch).init(self.arena);
            while (self.eatKw("from")) {
                const rd = try self.parseRead();
                try self.expectKw("as");
                const tag = (try self.expect(.string)).text;
                try branches.append(.{ .read = rd, .tag = tag });
            }
            return .{ .branches = try branches.toOwnedSlice(), .pos = pos };
        }
        const conn = try self.expectIdent();
        // `union <conn> json "<array>"`: branch list comes from a JSON array of
        // {table, tag} objects (e.g. a `${source}` field from the request body),
        // instead of a discovery query.
        if (self.eatKw("json")) {
            const j = (try self.expect(.string)).text;
            return .{ .discover_conn = conn, .discover_json = j, .pos = pos };
        }
        try self.expectKw("tables");
        const q = (try self.expect(.string)).text;
        return .{ .discover_conn = conn, .discover_query = q, .pos = pos };
    }

    fn parseRead(self: *Parser) Error!ast.Read {
        const connector = try self.expectIdent();
        if (std.mem.eql(u8, connector, "request")) return .{ .connector = connector, .form = .request };
        if (self.eatKw("table")) {
            return .{ .connector = connector, .form = .{ .table = try self.parseQualName() } };
        }
        if (self.eatKw("query")) {
            const s = try self.expect(.string);
            return .{ .connector = connector, .form = .{ .query = s.text } };
        }
        if (self.at(.string)) {
            return .{ .connector = connector, .form = .{ .path = self.advance().text } };
        }
        return self.fail(self.curPos(), "expected `table`, `query`, or a quoted path after `read {s}`", .{connector});
    }

    fn parseSelect(self: *Parser) Error![]const ast.SelectItem {
        var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
        while (true) {
            try items.append(try self.parseSelectItem());
            if (!self.eat(.comma)) break;
        }
        return try items.toOwnedSlice();
    }

    fn parseSelectItem(self: *Parser) Error!ast.SelectItem {
        if (self.eat(.star)) {
            if (self.eatKw("except")) {
                _ = try self.expect(.lparen);
                var names = std.array_list.Managed([]const u8).init(self.arena);
                try names.append(try self.expectIdent());
                while (self.eat(.comma)) try names.append(try self.expectIdent());
                _ = try self.expect(.rparen);
                return .{ .star_except = try names.toOwnedSlice() };
            }
            return .star;
        }
        // Computed column: `name = expr`. The alias may be a bare ident or a quoted
        // string — a string lets `${var}` build the name (e.g. a per-table
        // `"${name}_EMPRESA" = emp`). Bare identifiers cannot be templated.
        if ((self.at(.ident) or self.at(.string)) and self.peekTag() == .assign) {
            const name = self.advance().text;
            _ = try self.expect(.assign);
            const e = try self.parseExpr();
            return .{ .computed = .{ .name = name, .expr = e } };
        }
        return .{ .field = try self.parseQualName() };
    }

    fn parseExplode(self: *Parser) Error!ast.Explode {
        const field = try self.expectIdent();
        var as_name: ?[]const u8 = null;
        if (self.eatKw("as")) as_name = try self.expectIdent();
        var delim: ?[]const u8 = null;
        if (self.eatKw("on")) delim = (try self.expect(.string)).text;
        return .{ .field = field, .as_name = as_name, .delim = delim };
    }

    fn parseLimit(self: *Parser) Error!ast.Limit {
        const count = try self.expectU64();
        var offset: u64 = 0;
        if (self.eatKw("offset")) offset = try self.expectU64();
        return .{ .count = count, .offset = offset };
    }

    fn parseDistinct(self: *Parser) Error!ast.Distinct {
        if (self.eatKw("on")) {
            var keys = std.array_list.Managed(ast.QualName).init(self.arena);
            try keys.append(try self.parseQualName());
            while (self.eat(.comma)) try keys.append(try self.parseQualName());
            return .{ .on = try keys.toOwnedSlice() };
        }
        return .{ .on = null };
    }

    fn parseSort(self: *Parser) Error!ast.Sort {
        var keys = std.array_list.Managed(ast.SortKey).init(self.arena);
        while (true) {
            const field = try self.parseQualName();
            var desc = false;
            if (self.eatKw("desc")) {
                desc = true;
            } else {
                _ = self.eatKw("asc");
            }
            try keys.append(.{ .field = field, .desc = desc });
            if (!self.eat(.comma)) break;
        }
        return .{ .keys = try keys.toOwnedSlice() };
    }

    fn parseAggregate(self: *Parser) Error!ast.Aggregate {
        var aggs = std.array_list.Managed(ast.AggItem).init(self.arena);
        while (true) {
            const name = try self.expectIdent();
            _ = try self.expect(.assign);
            const fname = try self.expectIdent();
            const func: ast.AggFunc = if (std.mem.eql(u8, fname, "count"))
                .count
            else if (std.mem.eql(u8, fname, "sum"))
                .sum
            else if (std.mem.eql(u8, fname, "avg"))
                .avg
            else if (std.mem.eql(u8, fname, "min"))
                .min
            else if (std.mem.eql(u8, fname, "max"))
                .max
            else
                return self.fail(self.curPos(), "unknown aggregate function `{s}`", .{fname});
            _ = try self.expect(.lparen);
            var arg: ?*ast.Expr = null;
            if (!self.at(.rparen)) arg = try self.parseExpr();
            _ = try self.expect(.rparen);
            try aggs.append(.{ .name = name, .func = func, .arg = arg });
            if (!self.eat(.comma)) break;
        }
        var by: []const ast.QualName = &[_]ast.QualName{};
        if (self.eatKw("by")) {
            var g = std.array_list.Managed(ast.QualName).init(self.arena);
            try g.append(try self.parseQualName());
            while (self.eat(.comma)) try g.append(try self.parseQualName());
            by = try g.toOwnedSlice();
        }
        return .{ .aggs = try aggs.toOwnedSlice(), .by = by };
    }

    fn parseJoin(self: *Parser) Error!ast.Join {
        var kind: ast.JoinKind = .inner;
        if (self.joinKind()) |k| {
            _ = self.advance();
            kind = k;
        }
        const binding = try self.expectIdent();
        var left_key: ast.QualName = .{ .parts = &.{} };
        var right_key: ast.QualName = .{ .parts = &.{} };
        if (kind != .cross) {
            try self.expectKw("on");
            left_key = try self.parseQualName();
            // accept `=` or `==`
            if (!self.eat(.assign) and !self.eat(.eq))
                return self.fail(self.curPos(), "expected `=` in join condition, found {s}", .{self.curTag().describe()});
            right_key = try self.parseQualName();
        }
        return .{ .kind = kind, .binding = binding, .left_key = left_key, .right_key = right_key };
    }

    fn joinKind(self: *Parser) ?ast.JoinKind {
        const t = self.cur();
        if (t.tag != .ident) return null;
        const map = .{
            .{ "inner", ast.JoinKind.inner },
            .{ "left", ast.JoinKind.left },
            .{ "right", ast.JoinKind.right },
            .{ "full", ast.JoinKind.full },
            .{ "semi", ast.JoinKind.semi },
            .{ "anti", ast.JoinKind.anti },
            .{ "cross", ast.JoinKind.cross },
        };
        inline for (map) |m| {
            if (std.mem.eql(u8, t.text, m[0])) return m[1];
        }
        return null;
    }

    fn parseWrite(self: *Parser) Error!ast.Write {
        const connector = try self.expectIdent();
        // Bare connector with no target, e.g. `write stdout`.
        if (!self.atName() and !self.at(.string)) {
            return .{ .connector = connector, .form = null, .target = "", .mode = .default };
        }
        var form: ?[]const u8 = null;
        var target: []const u8 = undefined;
        if (self.at(.string)) {
            // A string is always the target (a templated `"proth_${name}"`); a form
            // is never a string, so there is no second name to look for.
            target = self.advance().text;
        } else {
            // A name is form-or-target: if another name/string (not a mode keyword)
            // follows, the first was the form (e.g. `stream_load <target>`).
            const first = try joinQual(self.arena, try self.parseQualName());
            if ((self.atName() or self.at(.string)) and !self.isModeKw()) {
                form = first;
                target = try self.parseWriteTarget();
            } else {
                target = first;
            }
        }
        const mode = try self.parseWriteMode();
        return .{ .connector = connector, .form = form, .target = target, .mode = mode };
    }
    /// A write target: a quoted string (so `${var}` can build it) or a qualified name.
    fn parseWriteTarget(self: *Parser) Error![]const u8 {
        if (self.at(.string)) return self.advance().text;
        return joinQual(self.arena, try self.parseQualName());
    }

    fn isModeKw(self: *Parser) bool {
        const t = self.cur();
        if (t.tag != .ident) return false;
        inline for (.{ "append", "overwrite", "upsert", "partial" }) |k| {
            if (std.mem.eql(u8, t.text, k)) return true;
        }
        return false;
    }

    fn parseWriteMode(self: *Parser) Error!ast.WriteMode {
        if (self.eatKw("append")) return .append;
        if (self.eatKw("overwrite")) return .overwrite;
        if (self.eatKw("upsert")) {
            try self.expectKw("on");
            var keys = std.array_list.Managed([]const u8).init(self.arena);
            try keys.append(try self.expectColName());
            while (self.eat(.comma)) try keys.append(try self.expectColName());
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
            return .{ .upsert = .{ .keys = try keys.toOwnedSlice(), .partial = partial } };
        }
        return .default;
    }

    fn parseHints(self: *Parser) Error![]const ast.Hint {
        if (!(self.at(.at) and self.peekTag() == .lbracket)) return &[_]ast.Hint{};
        _ = self.advance(); // @
        _ = self.advance(); // [
        var list = std.array_list.Managed(ast.Hint).init(self.arena);
        if (!self.at(.rbracket)) {
            while (true) {
                const pos = self.curPos();
                const key = try self.expectIdent();
                var val: ast.HintVal = .flag;
                if (self.eat(.assign)) val = try self.parseHintVal();
                try list.append(.{ .key = key, .value = val, .pos = pos });
                if (!self.eat(.comma)) break;
            }
        }
        _ = try self.expect(.rbracket);
        return try list.toOwnedSlice();
    }

    fn parseHintVal(self: *Parser) Error!ast.HintVal {
        const t = self.cur();
        switch (t.tag) {
            .string => {
                _ = self.advance();
                return .{ .str = t.text };
            },
            .int => {
                _ = self.advance();
                const n = try self.i64Of(t);
                if (self.at(.ident)) return .{ .size = .{ .n = n, .unit = self.advance().text } };
                return .{ .int = n };
            },
            .ident, .interp => {
                _ = self.advance();
                return .{ .ident = t.text };
            },
            else => return self.fail(self.curPos(), "expected a hint value, found {s}", .{t.tag.describe()}),
        }
    }

    // --- expressions (Pratt) ---

    fn parseExpr(self: *Parser) Error!*ast.Expr {
        return self.parseBin(0);
    }

    fn parseBin(self: *Parser, min_bp: u8) Error!*ast.Expr {
        var lhs = try self.parseUnary();
        while (self.peekBinOp()) |info| {
            if (info.lbp < min_bp) break;
            _ = self.advance();
            const rhs = try self.parseBin(info.rbp);
            lhs = try self.mk(.{ .binary = .{ .op = info.op, .l = lhs, .r = rhs } });
        }
        return lhs;
    }

    fn peekBinOp(self: *Parser) ?OpInfo {
        const t = self.cur();
        return switch (t.tag) {
            .star => .{ .op = .mul, .lbp = 9, .rbp = 10 },
            .slash => .{ .op = .div, .lbp = 9, .rbp = 10 },
            .percent => .{ .op = .mod, .lbp = 9, .rbp = 10 },
            .plus => .{ .op = .add, .lbp = 7, .rbp = 8 },
            .minus => .{ .op = .sub, .lbp = 7, .rbp = 8 },
            .eq => .{ .op = .eq, .lbp = 5, .rbp = 6 },
            .ne => .{ .op = .ne, .lbp = 5, .rbp = 6 },
            .lt => .{ .op = .lt, .lbp = 5, .rbp = 6 },
            .le => .{ .op = .le, .lbp = 5, .rbp = 6 },
            .gt => .{ .op = .gt, .lbp = 5, .rbp = 6 },
            .ge => .{ .op = .ge, .lbp = 5, .rbp = 6 },
            .ident => if (std.mem.eql(u8, t.text, "and"))
                OpInfo{ .op = .@"and", .lbp = 3, .rbp = 4 }
            else if (std.mem.eql(u8, t.text, "or"))
                OpInfo{ .op = .@"or", .lbp = 1, .rbp = 2 }
            else
                null,
            else => null,
        };
    }

    fn parseUnary(self: *Parser) Error!*ast.Expr {
        if (self.isKw("not")) {
            _ = self.advance();
            return self.mk(.{ .unary = .{ .op = .not, .e = try self.parseUnary() } });
        }
        if (self.at(.minus)) {
            _ = self.advance();
            return self.mk(.{ .unary = .{ .op = .neg, .e = try self.parseUnary() } });
        }
        return self.parsePostfix();
    }

    fn parsePostfix(self: *Parser) Error!*ast.Expr {
        var e = try self.parsePrimary();
        while (self.isKw("is")) {
            _ = self.advance();
            const negated = self.eatKw("not");
            try self.expectKw("null");
            e = try self.mk(.{ .is_null = .{ .e = e, .negated = negated } });
        }
        return e;
    }

    fn parsePrimary(self: *Parser) Error!*ast.Expr {
        const t = self.cur();
        switch (t.tag) {
            .int => {
                _ = self.advance();
                return self.mk(.{ .int_lit = try self.i64Of(t) });
            },
            .float => {
                _ = self.advance();
                const f = std.fmt.parseFloat(f64, t.text) catch
                    return self.fail(.{ .line = t.line, .col = t.col }, "invalid float `{s}`", .{t.text});
                return self.mk(.{ .float_lit = f });
            },
            .string => {
                _ = self.advance();
                return self.mk(.{ .str_lit = t.text });
            },
            .lparen => {
                _ = self.advance();
                const e = try self.parseExpr();
                _ = try self.expect(.rparen);
                return e;
            },
            .ident => return self.parseIdentPrimary(),
            else => return self.fail(self.curPos(), "expected an expression, found {s}", .{t.tag.describe()}),
        }
    }

    fn parseIdentPrimary(self: *Parser) Error!*ast.Expr {
        const t = self.cur();
        if (std.mem.eql(u8, t.text, "true")) {
            _ = self.advance();
            return self.mk(.{ .bool_lit = true });
        }
        if (std.mem.eql(u8, t.text, "false")) {
            _ = self.advance();
            return self.mk(.{ .bool_lit = false });
        }
        if (std.mem.eql(u8, t.text, "null")) {
            _ = self.advance();
            return self.mk(.null_lit);
        }
        if (std.mem.eql(u8, t.text, "if")) return self.parseIf();
        if (std.mem.eql(u8, t.text, "match")) return self.parseMatchExpr();
        if (std.mem.eql(u8, t.text, "cast")) return self.parseCast();
        if (self.peekTag() == .lparen) return self.parseCall();
        return self.mk(.{ .field = try self.parseQualName() });
    }

    fn parseIf(self: *Parser) Error!*ast.Expr {
        _ = self.advance(); // if
        _ = try self.expect(.lparen);
        const c = try self.parseExpr();
        _ = try self.expect(.comma);
        const a = try self.parseExpr();
        _ = try self.expect(.comma);
        const b = try self.parseExpr();
        _ = try self.expect(.rparen);
        return self.mk(.{ .cond = .{ .cond = c, .then = a, .els = b } });
    }

    fn parseCast(self: *Parser) Error!*ast.Expr {
        _ = self.advance(); // cast
        _ = try self.expect(.lparen);
        const e = try self.parseExpr();
        try self.expectKw("as");
        const ty = try self.parseTypeName();
        _ = try self.expect(.rparen);
        return self.mk(.{ .cast = .{ .e = e, .ty = ty } });
    }

    fn parseCall(self: *Parser) Error!*ast.Expr {
        const name = self.advance().text;
        _ = try self.expect(.lparen);
        var args = std.array_list.Managed(*ast.Expr).init(self.arena);
        if (!self.at(.rparen)) {
            try args.append(try self.parseExpr());
            while (self.eat(.comma)) try args.append(try self.parseExpr());
        }
        _ = try self.expect(.rparen);
        return self.mk(.{ .call = .{ .name = name, .args = try args.toOwnedSlice() } });
    }

    fn parseMatchExpr(self: *Parser) Error!*ast.Expr {
        _ = self.advance(); // match
        var subject: ?*ast.Expr = null;
        // A subject is present unless the next thing is `end`, a `_` arm, or an
        // expression immediately followed by `=>` (the guard form).
        if (!self.isKw("end") and !self.isWildcard()) {
            const save = self.i;
            const e = try self.parseExpr();
            if (self.at(.fat_arrow)) {
                self.i = save; // guard form: rewind, no subject
            } else {
                subject = e;
            }
        }
        var arms = std.array_list.Managed(ast.MatchArm).init(self.arena);
        while (!self.isKw("end")) try arms.append(try self.parseMatchArm(subject != null));
        try self.expectKw("end");
        return self.mk(.{ .match = .{ .subject = subject, .arms = try arms.toOwnedSlice() } });
    }

    fn parseMatchArm(self: *Parser, subject_form: bool) Error!ast.MatchArm {
        if (self.isWildcard()) {
            _ = self.advance();
            _ = try self.expect(.fat_arrow);
            return .{ .pats = &[_]*ast.Expr{}, .guard = null, .value = try self.parseExpr(), .is_default = true };
        }
        if (subject_form) {
            var pats = std.array_list.Managed(*ast.Expr).init(self.arena);
            try pats.append(try self.parseExpr());
            while (self.eat(.pipe)) try pats.append(try self.parseExpr());
            _ = try self.expect(.fat_arrow);
            return .{ .pats = try pats.toOwnedSlice(), .guard = null, .value = try self.parseExpr(), .is_default = false };
        }
        const g = try self.parseExpr();
        _ = try self.expect(.fat_arrow);
        return .{ .pats = &[_]*ast.Expr{}, .guard = g, .value = try self.parseExpr(), .is_default = false };
    }

    fn isWildcard(self: *Parser) bool {
        const t = self.cur();
        return t.tag == .ident and std.mem.eql(u8, t.text, "_");
    }

    // --- shared helpers ---

    fn parseQualName(self: *Parser) Error!ast.QualName {
        var parts = std.array_list.Managed([]const u8).init(self.arena);
        try parts.append(try self.expectName());
        while (self.eat(.dot)) try parts.append(try self.expectName());
        return .{ .parts = try parts.toOwnedSlice() };
    }

    fn parseTypeName(self: *Parser) Error!types.Type {
        const pos = self.curPos();
        const name = try self.expectIdent();
        const Map = struct { n: []const u8, k: types.TypeKind };
        const simple = [_]Map{
            .{ .n = "bool", .k = .bool },
            .{ .n = "int", .k = .int },
            .{ .n = "float", .k = .float },
            .{ .n = "string", .k = .string },
            .{ .n = "bytes", .k = .bytes },
            .{ .n = "date", .k = .date },
            .{ .n = "time", .k = .time },
            .{ .n = "timestamp", .k = .timestamp },
        };
        for (simple) |m| {
            if (std.mem.eql(u8, name, m.n)) return types.Type.init(m.k);
        }
        if (std.mem.eql(u8, name, "decimal")) {
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

    fn mk(self: *Parser, e: ast.Expr) Error!*ast.Expr {
        const p = try self.arena.create(ast.Expr);
        p.* = e;
        return p;
    }

    fn i64Of(self: *Parser, t: Token) Error!i64 {
        return std.fmt.parseInt(i64, t.text, 10) catch
            self.fail(.{ .line = t.line, .col = t.col }, "invalid integer `{s}`", .{t.text});
    }
    fn expectU64(self: *Parser) Error!u64 {
        const t = try self.expect(.int);
        return std.fmt.parseInt(u64, t.text, 10) catch
            self.fail(.{ .line = t.line, .col = t.col }, "invalid integer `{s}`", .{t.text});
    }
    fn expectU8(self: *Parser) Error!u8 {
        const t = try self.expect(.int);
        return std.fmt.parseInt(u8, t.text, 10) catch
            self.fail(.{ .line = t.line, .col = t.col }, "invalid number `{s}`", .{t.text});
    }
};

fn joinQual(arena: std.mem.Allocator, q: ast.QualName) error{OutOfMemory}![]const u8 {
    if (q.parts.len == 1) return q.parts[0];
    return std.mem.join(arena, ".", q.parts);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn parseTest(arena: std.mem.Allocator, src: []const u8) !ast.Program {
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    return parseSource(arena, src, &diag) catch |e| {
        std.debug.print("parse error {d}:{d}: {s}\n", .{ diag.line, diag.col, diag.msg });
        return e;
    };
}

test "parse a batch ETL script" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const src =
        \\@batch
        \\param since timestamp
        \\connection mssql = sqlserver
        \\  host = "sql.internal" port = 1433 user = env("U") password = secret("P")
        \\let recent =
        \\  read mssql query "select id, total from orders where updated_at > :since"
        \\  | filter total > 0
        \\  | select id, amount = total
        \\recent | write sr stream_load orders upsert on id @[format = csv, batch_bytes = 100mb]
    ;
    const prog = try parseTest(ar.allocator(), src);
    try std.testing.expectEqual(@as(usize, 5), prog.stmts.len);
    try std.testing.expect(prog.stmts[0] == .kind);
    try std.testing.expectEqual(ast.Kind.batch, prog.stmts[0].kind.kind);
    try std.testing.expect(prog.stmts[1] == .param);
    try std.testing.expect(prog.stmts[2] == .connection);
    try std.testing.expect(prog.stmts[3] == .binding);
    try std.testing.expect(prog.stmts[4] == .output);

    // the output pipeline: ref(recent) | write
    const out = prog.stmts[4].output;
    try std.testing.expectEqual(@as(usize, 2), out.stages.len);
    try std.testing.expect(out.stages[0].node == .ref);
    try std.testing.expect(out.stages[1].node == .write);
    const w = out.stages[1].node.write;
    try std.testing.expectEqualStrings("stream_load", w.form.?);
    try std.testing.expectEqualStrings("orders", w.target);
    try std.testing.expect(w.mode == .upsert);
    try std.testing.expectEqual(@as(usize, 2), out.stages[1].hints.len);
}

test "parse precedence and conditionals" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const src =
        \\@batch
        \\read x query "q"
        \\  | filter a + b * c > 10 and d is not null
        \\  | select tier = if(amount > 100, "gold", "silver"),
        \\           grade = match status "paid" | "ok" => "done" _ => "open" end
    ;
    const prog = try parseTest(ar.allocator(), src);
    const stages = prog.stmts[1].output.stages;
    // filter: top of expr tree is `and`
    const pred = stages[1].node.filter;
    try std.testing.expect(pred.* == .binary);
    try std.testing.expectEqual(ast.BinOp.@"and", pred.binary.op);
    // left of `and` is a comparison `>`
    try std.testing.expectEqual(ast.BinOp.gt, pred.binary.l.binary.op);
    // select has two computed items; second is a match with a subject
    const items = stages[2].node.select;
    try std.testing.expectEqual(@as(usize, 2), items.len);
    try std.testing.expect(items[0].computed.expr.* == .cond);
    const m = items[1].computed.expr.match;
    try std.testing.expect(m.subject != null);
    try std.testing.expectEqual(@as(usize, 2), m.arms.len);
    try std.testing.expectEqual(@as(usize, 2), m.arms[0].pats.len); // "paid" | "ok"
    try std.testing.expect(m.arms[1].is_default);
}

test "parse a for-each over a table list" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const src =
        \\@batch
        \\connection mssql = sqlserver host = "h"
        \\connection sr = starrocks
        \\for tbl, pk in mssql query "SELECT name, pk FROM meta" @[mode = parallel, on_error = continue]
        \\  read mssql table dbo.${tbl}
        \\    | write sr stream_load ${tbl} upsert on ${pk}
    ;
    const prog = try parseTest(ar.allocator(), src);
    try std.testing.expect(prog.stmts[3] == .for_each);
    const fe = prog.stmts[3].for_each;
    try std.testing.expectEqual(@as(usize, 2), fe.var_names.len);
    try std.testing.expectEqualStrings("tbl", fe.var_names[0]);
    try std.testing.expectEqualStrings("pk", fe.var_names[1]);
    try std.testing.expect(fe.source == .read);
    try std.testing.expectEqualStrings("mssql", fe.source.read.connector);
    try std.testing.expect(fe.source.read.form == .query);
    try std.testing.expectEqual(@as(usize, 2), fe.hints.len);
    try std.testing.expectEqual(@as(usize, 2), fe.body.stages.len);
    const rd = fe.body.stages[0].node.read;
    try std.testing.expectEqual(@as(usize, 2), rd.form.table.parts.len);
    try std.testing.expectEqualStrings("${tbl}", rd.form.table.parts[1]);
    const w = fe.body.stages[1].node.write;
    try std.testing.expectEqualStrings("stream_load", w.form.?);
    try std.testing.expectEqualStrings("${tbl}", w.target);
    try std.testing.expect(w.mode == .upsert);
    try std.testing.expectEqualStrings("${pk}", w.mode.upsert.keys[0]);
}

test "for-each over a JSON array param" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const prog = try parseTest(ar.allocator(),
        "@batch\nparam job json from body\n" ++
        "for name in job.tables @[mode = parallel]\n  read csv \"${name}.csv\" | write stdout");
    const fe = prog.stmts[2].for_each;
    try std.testing.expect(fe.source == .json_path);
    try std.testing.expectEqual(@as(usize, 2), fe.source.json_path.parts.len);
    try std.testing.expectEqualStrings("job", fe.source.json_path.parts[0]);
    try std.testing.expectEqualStrings("tables", fe.source.json_path.parts[1]);
    try std.testing.expectEqualStrings("name", fe.var_names[0]);
}

test "parse a union (explicit + discovered)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const src =
        \\@batch
        \\union from erp table CT2010 as "01" from erp table CT2020 as "02"
        \\  @[tag = CT2_EMPRESA, canon = CT2010]
        \\  | write sr stream_load CT2_UNIFIED upsert on CT2_EMPRESA, R_E_C_N_O_
    ;
    const prog = try parseTest(ar.allocator(), src);
    const out = prog.stmts[1].output;
    try std.testing.expect(out.stages[0].node == .union_);
    const un = out.stages[0].node.union_;
    try std.testing.expectEqual(@as(usize, 2), un.branches.len);
    try std.testing.expectEqualStrings("erp", un.branches[0].read.connector);
    try std.testing.expectEqualStrings("CT2010", un.branches[0].read.form.table.last());
    try std.testing.expectEqualStrings("01", un.branches[0].tag.?);
    try std.testing.expectEqual(@as(usize, 2), out.stages[0].hints.len);
    try std.testing.expect(out.stages[1].node == .write);

    // discovered form
    const prog2 = try parseTest(ar.allocator(), "@batch\nunion erp tables \"SELECT name, x FROM t\" @[tag = src]\n  | write csv \"/o\"");
    const un2 = prog2.stmts[1].output.stages[0].node.union_;
    try std.testing.expectEqualStrings("erp", un2.discover_conn);
    try std.testing.expectEqualStrings("SELECT name, x FROM t", un2.discover_query);
    try std.testing.expectEqual(@as(usize, 0), un2.branches.len);
}

test "write stdout: bare connector with no target" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const prog = try parseTest(ar.allocator(), "@batch\nread csv \"x\" | write stdout");
    const out = prog.stmts[1].output;
    const w = out.stages[out.stages.len - 1].node.write;
    try std.testing.expectEqualStrings("stdout", w.connector);
    try std.testing.expectEqualStrings("", w.target);
    try std.testing.expect(w.form == null);
}

test "statement-level match: subject form with block arms + default" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const prog = try parseTest(ar.allocator(),
        "@batch\nparam env string = \"prod\"\n" ++
        "match env\n  \"prod\" => { read csv \"x\" | write stdout }\n  _ => { read csv \"y\" | write stdout }\nend");
    const m = prog.stmts[2].match;
    try std.testing.expect(m.subject != null);
    try std.testing.expectEqual(@as(usize, 2), m.arms.len);
    try std.testing.expect(!m.arms[0].is_default);
    try std.testing.expectEqual(@as(usize, 1), m.arms[0].pats.len);
    try std.testing.expect(m.arms[1].is_default);
    try std.testing.expectEqual(@as(usize, 1), m.arms[0].body.len);
    try std.testing.expect(m.arms[0].body[0] == .output);
}

test "statement-level match: guard form" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const prog = try parseTest(ar.allocator(),
        "@batch\nparam t string = \"SD1010\"\n" ++
        "match\n  starts_with(t, \"SD1\") => { read csv \"x\" | write stdout }\nend");
    const m = prog.stmts[2].match;
    try std.testing.expect(m.subject == null);
    try std.testing.expectEqual(@as(usize, 1), m.arms.len);
    try std.testing.expect(m.arms[0].guard != null);
}

test "missing @kind is an error" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var diag: Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const r = parseSource(ar.allocator(), "read x query \"q\"", &diag);
    try std.testing.expectError(error.ParseFailed, r);
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "@kind") != null);
}

test "guard-form match has no subject" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const src =
        \\@batch
        \\read x query "q"
        \\  | select tier = match amount >= 100 => "gold" _ => "std" end
    ;
    const prog = try parseTest(ar.allocator(), src);
    const m = prog.stmts[1].output.stages[1].node.select[0].computed.expr.match;
    try std.testing.expect(m.subject == null);
    try std.testing.expect(m.arms[0].guard != null);
    try std.testing.expect(m.arms[1].is_default);
}
