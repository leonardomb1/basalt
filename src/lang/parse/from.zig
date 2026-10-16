//! FROM sources: paths, connections' tables and queries, HTTP calls and resources,
//! buffers, `EACH TABLE OF`, RANGE, and the read clauses (PAGINATE, RETRY, BODY).

const Parser = @import("../sql_parser.zig").Parser;
const AliasSet = @import("../sql_parser.zig").AliasSet;
const Error = @import("../sql_parser.zig").Error;
const Pos = @import("../sql_parser.zig").Pos;
const ast = @import("../ast.zig");
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const hasLiteralExt = @import("../sql_parser.zig").hasLiteralExt;
const std = @import("std");

/// A FROM source: a path, `IDENTIFIER(<expr>)`, HTTP('url'), a CTE, or a
/// connection's table or QUERY. Registers the alias.
pub fn parseFromSource(self: *Parser, aliases: *AliasSet, read_hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
    var node: ast.Stage.Node = undefined;
    var own: []const u8 = "";
    if (self.at(.lparen)) {
        const d = try self.parseDerivedTable();
        if (d.alias) |a| try self.claimAlias(aliases, a, d.alias_pos, .left);
        return .{ .ref = d.binding };
    } else if (self.at(.string)) {
        node = .{ .read = .{ .connector = "csv", .form = .{ .path = self.advance().text } } };
        own = pathStem(node.read.form.path);
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
    } else if (self.isKw("http") and self.peekTag() == .lparen) {
        _ = self.advance();
        _ = try self.expect(.lparen);
        const url = try self.expect(.string);
        _ = try self.expect(.rparen);
        node = .{ .read = .{ .connector = "http", .form = .{ .path = url.text } } };
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
            const bname = (try self.callTableFn(head, pos, .from)).binding;
            if (!try claimFromAlias(self, aliases)) try self.claimAlias(aliases, head, pos, .left);
            return .{ .ref = bname };
        }
        if (self.tvf) |f| if (f.cte(head)) |bname| {
            if (!try claimFromAlias(self, aliases)) aliases.implicit = head;
            return .{ .ref = bname };
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
                own = node.read.form.table.last();
            }
        } else if (self.isLet(head)) {
            node = .{ .ref = head };
            own = head;
        } else {
            return self.fail(pos, "unknown source `{s}`: not a CTE, connection, or path", .{head});
        }
    }
    if (!try claimFromAlias(self, aliases)) aliases.implicit = own;
    return node;
}

/// `[AS] alias` after a FROM source, claimed for the FROM table; false when none follows.
fn claimFromAlias(self: *Parser, aliases: *AliasSet) Error!bool {
    self.eatAliasAs();
    if (!self.atAlias()) return false;
    try self.claimAlias(aliases, self.advance().text, self.prevPos(), .left);
    return true;
}

/// A path's file name up to its first dot (`l` of `'dir/l.csv'`), the name SQL lets
/// an unaliased file go by; none for a templated one.
fn pathStem(path: []const u8) []const u8 {
    if (std.mem.indexOf(u8, path, "${") != null) return "";
    const base = path[if (std.mem.lastIndexOfAny(u8, path, "/\\")) |i| i + 1 else 0..];
    return base[0 .. std.mem.indexOfScalar(u8, base, '.') orelse base.len];
}

/// A discovery sub-query: a full SELECT pipeline with no write, run in-engine. Its CTEs
/// and derived tables become bindings ahead of the statement that discovers through it.
pub fn parseSubQuery(self: *Parser, pos: Pos) Error!ast.Pipeline {
    var hoisted = std.array_list.Managed(ast.Stmt).init(self.arena);
    var stages = std.array_list.Managed(ast.Stage).init(self.arena);
    try self.parseQuery(&hoisted, &stages);
    try self.pending_bindings.appendSlice(hoisted.items);
    return .{ .stages = try stages.toOwnedSlice(), .pos = pos };
}

/// `EACH TABLE OF (SELECT ... | conn.QUERY(...) | $param.path | '<json>' IN conn)
/// [AS (name, tag)] [ANCHOR SCHEMA q]`: one branch per row, on the discovery
/// query's connection unless `IN conn` says otherwise.
pub fn parseEachTableOf(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!ast.Stage.Node {
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

/// `PAGINATE BY page|offset|cursor (...)` -> HTTP hints. The mode goes in a `paginate`
/// hint: a bare flag once left pagination off and fetched one page.
pub fn parsePaginate(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
    const pos = self.curPos();
    try self.expectKw("paginate");
    try self.expectKw("by");
    const mode = try self.expectIdent();
    const is_cursor = eqlNoCase(mode, "cursor");
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

pub fn parseRetry(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
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
