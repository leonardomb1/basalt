//! A query: LOAD INTO's target and mode, WITH, the select core, and the unions and
//! set operations that combine cores.

const Parser = @import("../sql_parser.zig").Parser;
const AliasSet = @import("../sql_parser.zig").AliasSet;
const BranchInfo = Parser.BranchInfo;
const Core = Parser.Core;
const Error = @import("../sql_parser.zig").Error;
const ExprAlias = Parser.ExprAlias;
const LateralKey = Parser.LateralKey;
const Pos = @import("../sql_parser.zig").Pos;
const SetOpTok = Parser.SetOpTok;
const aggFunc = @import("../sql_parser.zig").aggFunc;
const aggOrderDiffers = @import("../sql_parser.zig").aggOrderDiffers;
const ast = @import("../ast.zig");
const containsAgg = Parser.containsAgg;
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const hasLiteralExt = @import("../sql_parser.zig").hasLiteralExt;
const isGroupKey = @import("../sql_parser.zig").isGroupKey;
const isReservedAfterSource = @import("../sql_parser.zig").isReservedAfterSource;
const itemLabel = @import("../sql_parser.zig").itemLabel;
const keepOutputs = @import("../sql_parser.zig").keepOutputs;
const keyHome = @import("../sql_parser.zig").keyHome;
const max_query_depth = Parser.max_query_depth;
const qualHasPrefix = @import("../sql_parser.zig").qualHasPrefix;
const splitAnd = @import("../sql_parser.zig").splitAnd;
const std = @import("std");
const stripPrefix = @import("../sql_parser.zig").stripPrefix;
const stripQual = @import("../sql_parser.zig").stripQual;

/// A computed file target (`IDENTIFIER(...)`) must spell its extension literally: it
/// picks the writer and the legal dispositions at plan time, before any row is read.
pub fn parseLoadInto(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    const pos = self.curPos();
    try self.expectKw("load");
    try self.expectKw("into");

    var write: ast.Write = undefined;
    if (self.at(.string)) {
        write = .{ .connector = "csv", .form = null, .target = self.advance().text, .mode = .default };
    } else if (self.isKw("identifier") and self.peekTag() == .lparen) {
        const ipos = self.curPos();
        _ = self.advance();
        _ = try self.expect(.lparen);
        const e = try self.parseExpr();
        _ = try self.expect(.rparen);
        const tmpl = try self.exprToTemplate(e);
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

pub fn parseTerminalQuery(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
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

pub fn parseWithHints(self: *Parser, hints: *std.array_list.Managed(ast.Hint)) Error!void {
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

/// `[WITH ctes] core [set ops] [ORDER BY ...] [LIMIT n [OFFSET m]]`. Hidden sort keys are
/// dropped after LIMIT so sort+limit stay adjacent for top-N; `DISTINCT ON ... ORDER BY`
/// sorts first, so the row kept per key is the first in ORDER BY order.
pub fn parseQuery(self: *Parser, out: *std.array_list.Managed(ast.Stmt), stages: *std.array_list.Managed(ast.Stage)) Error!void {
    if (self.query_depth >= max_query_depth)
        return self.fail(self.curPos(), "queries nest more than {d} levels deep", .{max_query_depth});
    self.query_depth += 1;
    defer self.query_depth -= 1;
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

/// Index of the projection a sort reads through. A `DISTINCT ON` is looked through
/// (it keeps whole rows); a plain `DISTINCT` is not, as an extra column changes duplicates.
pub fn lastSelectIdx(stages: []const ast.Stage) ?usize {
    if (stages.len == 0) return null;
    const n = stages.len;
    if (stages[n - 1].node == .select) return n - 1;
    if (n >= 2 and stages[n - 1].node == .distinct and stages[n - 1].node.distinct.on != null and stages[n - 2].node == .select) return n - 2;
    return null;
}

/// Whether the pipeline ends `select, DISTINCT ON, select`, as `distinctOnOutputs`
/// leaves it when carrying a hidden key.
pub fn distinctDropTail(stages: []const ast.Stage) bool {
    const n = stages.len;
    return n >= 3 and stages[n - 1].node == .select and stages[n - 2].node == .distinct and
        stages[n - 2].node.distinct.on != null and stages[n - 3].node == .select;
}

pub fn resolveExprAlias(map: []const ExprAlias, synth: []const u8) []const u8 {
    for (map) |m| {
        if (std.mem.eql(u8, m.synth, synth)) return m.out;
    }
    return synth;
}

/// `SELECT [DISTINCT [ON (cols)]] items FROM source [joins] [WHERE] [GROUP BY] [HAVING]`.
/// An ON splits at its ANDs: cross-side equalities are keys, right-only conditions narrow
/// the right side first, anything else filters the joined rows (inner joins only).
pub fn parseSelectCore(self: *Parser) Error!Core {
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
    // Where each window item stood among the others, so its column comes back there.
    var win_raw_at = std.array_list.Managed(usize).init(self.arena);
    while (true) {
        if (try self.parseWindowItem(&win_funcs, &win_part, &win_ord)) {
            try win_outs.append(win_funcs.items[win_funcs.items.len - 1].out);
            try win_raw_at.append(raw_items.items.len);
            if (!self.eat(.comma)) break;
            continue;
        }
        if (self.eat(.star)) {
            if (self.eatKw("except") or self.eatKw("exclude")) {
                _ = try self.expect(.lparen);
                var names = std.array_list.Managed([]const u8).init(self.arena);
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
        var left_computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var right_computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var deferred = std.array_list.Managed(ast.DeferredKey).init(self.arena);
        var residual: ?*ast.Expr = null;
        for (lateral_keys) |lk| {
            try left_keys.append(stripQual(lk.left, &aliases));
            const rp = try self.arena.alloc([]const u8, 1);
            rp[0] = lk.right;
            try right_keys.append(.{ .parts = rp });
        }
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
                    if (!sl.right and !sr.right and sl.other and sr.other and
                        (try self.hasBareField(c.binary.l) or try self.hasBareField(c.binary.r)))
                    {
                        self.derived_n += 1;
                        const ln = try std.fmt.allocPrint(self.arena, "__jk{d}", .{self.derived_n});
                        self.derived_n += 1;
                        const rn = try std.fmt.allocPrint(self.arena, "__jk{d}", .{self.derived_n});
                        try deferred.append(.{
                            .a = try self.stripExpr(c.binary.l, &aliases),
                            .b = try self.stripExpr(c.binary.r, &aliases),
                            .left_name = ln,
                            .right_name = rn,
                            .pos = .{ .line = opos.line, .col = opos.col },
                        });
                        try key_cols.append(ln);
                        try key_cols.append(rn);
                        continue;
                    }
                }
                const right_only = !try self.namesOtherSide(c, rname);
                if (right_only and (kind == .inner or kind == .left)) {
                    const e = try self.stripRightExpr(c, rname);
                    narrow = if (narrow) |n| try self.mk(.{ .binary = .{ .op = .@"and", .l = n, .r = e } }) else e;
                } else if (kind == .inner) {
                    try post_filters.append(c);
                } else {
                    const e = try self.stripExpr(c, &aliases);
                    residual = if (residual) |r| try self.mk(.{ .binary = .{ .op = .@"and", .l = r, .r = e } }) else e;
                }
            }
            if (left_keys.items.len == 0 and deferred.items.len == 0)
                return self.fail(.{ .line = opos.line, .col = opos.col }, "the ON of a join needs at least one `=` between a left and a right value to join by — `b.k = a.k`, or computed: `trim(b.k) = cast(a.k AS string)`", .{});
            if (narrow != null or right_computed.items.len > 0) {
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
                .deferred = try deferred.toOwnedSlice(),
                .residual = residual,
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
        const sj_base = self.pending_semijoins.items.len;
        self.in_where = true;
        const raw = try self.parseExpr();
        self.in_where = false;
        const remaining = if (self.pending_semijoins.items.len > sj_base)
            try self.liftSemiJoins(raw, fpos)
        else
            raw;
        if (remaining) |r| {
            const e = try self.stripExpr(r, &aliases);
            try stages.append(.{ .node = .{ .filter = e }, .hints = &.{}, .pos = fpos });
        }
        for (self.pending_semijoins.items[sj_base..]) |sj| {
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
    var post = std.array_list.Managed(ast.SelectItem).init(self.arena);
    var lifted = false;
    if (distinct_on) |on| for (on) |*k| {
        k.* = stripQual(k.*, &aliases);
    };
    const post_before = try self.arena.alloc(usize, raw_items.items.len + 1);
    for (raw_items.items, 0..) |ri, ri_at| {
        post_before[ri_at] = post.items.len;
        switch (ri) {
            .qstar => |alias| {
                if (!aliases.has(alias))
                    return self.fail(pos, "unknown alias `{s}` in `{s}.*`", .{ alias, alias });
                try items.append(.star);
                try post.append(.star);
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
                    if (containsAgg(stripped)) {
                        const outer = try self.liftAggs(stripped, &aggs, expr_aliases.items, pos);
                        try post.append(.{ .computed = .{ .name = c.name, .expr = outer } });
                        lifted = true;
                        continue;
                    }
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

    post_before[raw_items.items.len] = post.items.len;

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
            var kept = std.array_list.Managed(ast.SelectItem).init(self.arena);
            for (items.items) |it| {
                if (it == .computed and self.constItemExpr(it.computed.expr)) {
                    try self.repointPostItem(&post, it.computed.name, it.computed.expr);
                    lifted = true;
                    continue;
                }
                if (it != .field)
                    return self.fail(pos, "`{s}` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY", .{itemLabel(it)});
                if (!isGroupKey(group, it.field))
                    return self.fail(pos, "`{s}` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY", .{it.field.last()});
                try kept.append(it);
            }
            items = kept;
        }
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
        if (!lifted and aggOrderDiffers(post.items, group, aggs_out)) lifted = true;
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

    if (distinct and win_funcs.items.len == 0) {
        try stages.append(.{ .node = .{ .distinct = .{ .on = distinct_on } }, .hints = &.{}, .pos = pos });
        if (distinct_drop) |outs| try stages.append(.{ .node = .{ .select = outs }, .hints = &.{}, .pos = pos });
    }

    if (win_funcs.items.len > 0) {
        var refs = std.array_list.Managed([]const u8).init(self.arena);
        for (win_part.items) |q| try refs.append(q.last());
        for (win_ord.items) |k| try refs.append(k.field.last());
        for (win_funcs.items) |f| {
            if (f.arg) |q| try refs.append(q.last());
        }
        for (refs.items) |name| {
            var have = false;
            for (items.items) |it| switch (it) {
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
        if (items.items.len > 0) {
            try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
        }
        try stages.append(.{ .node = .{ .window = .{
            .funcs = try win_funcs.toOwnedSlice(),
            .partition_by = try win_part.toOwnedSlice(),
            .order_by = try win_ord.toOwnedSlice(),
        } }, .hints = &.{}, .pos = pos });
        var out_items = std.array_list.Managed(ast.SelectItem).init(self.arena);
        for (0..post.items.len + 1) |at| {
            for (win_outs.items, win_raw_at.items) |name, raw_at| {
                if (post_before[raw_at] == at) try out_items.append(.{ .field = try self.singleName(name) });
            }
            if (at == post.items.len) break;
            try out_items.append(switch (post.items[at]) {
                .star => .{ .star_except = win_outs.items },
                .star_except => |ex| .{ .star_except = try std.mem.concat(self.arena, []const u8, &.{ ex, win_outs.items }) },
                else => post.items[at],
            });
        }
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

/// A union arm that is not a bare source, lowered to a binding and read back: the
/// union aligns arms by their schemas.
pub fn unionBranchFromStages(self: *Parser, branch_stages: []const ast.Stage) Error!ast.UnionBranch {
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

/// Collapse `core (UNION [ALL|DISTINCT] [BY NAME] core)... [ANCHOR SCHEMA q]` into one
/// union_ stage. A chain is all positional or all by name; a `UNION` without ALL
/// deduplicates everything to its left.
pub fn parseUnionTail(self: *Parser, first: Core, stages: *std.array_list.Managed(ast.Stage)) Error!void {
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

/// A chain holding INTERSECT or EXCEPT, by position. INTERSECT binds tighter, the
/// rest apply left to right; each step is a two-branch union_ the next reads.
pub fn parseSetOps(self: *Parser, cores: []const Core, ops: []const SetOpTok, stages: *std.array_list.Managed(ast.Stage), pos: Pos) Error!void {
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

/// `left <op> right` as a positional union_, plus a DISTINCT for a plain UNION.
pub fn setOpStages(self: *Parser, left: []const ast.Stage, right: []const ast.Stage, set: ast.SetOp, distinct: bool, pos: Pos) Error![]const ast.Stage {
    const branches = try self.arena.alloc(ast.UnionBranch, 2);
    branches[0] = try self.unionBranchFromStages(left);
    branches[1] = try self.unionBranchFromStages(right);
    var out = std.array_list.Managed(ast.Stage).init(self.arena);
    try out.append(.{ .node = .{ .union_ = .{ .branches = branches, .positional = true, .set = set, .pos = pos } }, .hints = &.{}, .pos = pos });
    if (distinct) try out.append(.{ .node = .{ .distinct = .{ .on = null } }, .hints = &.{}, .pos = pos });
    return out.toOwnedSlice();
}

pub fn appendUnion(self: *Parser, cores: []const Core, distinct: bool, positional: bool, stages: *std.array_list.Managed(ast.Stage), pos: Pos) Error!void {
    var branches = std.array_list.Managed(ast.UnionBranch).init(self.arena);
    var tag_col: ?[]const u8 = null;
    for (cores) |core| {
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
