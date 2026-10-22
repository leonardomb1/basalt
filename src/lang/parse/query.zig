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
const Window = @import("window.zig");
const Correlate = @import("correlate.zig");
const aggregates = @import("../aggregates.zig");
const aggOrderDiffers = @import("../sql_parser.zig").aggOrderDiffers;
const ast = @import("../ast.zig");
const containsAgg = Parser.containsAgg;
const hasFieldOf = @import("rewrite.zig").hasFieldOf;
const eqlNoCase = @import("../sql_parser.zig").eqlNoCase;
const hasLiteralExt = @import("../sql_parser.zig").hasLiteralExt;
const isGroupKey = @import("../sql_parser.zig").isGroupKey;
const itemLabel = @import("../sql_parser.zig").itemLabel;
const itemPos = @import("../sql_parser.zig").itemPos;
const keepOutputs = @import("../sql_parser.zig").keepOutputs;
const nameApart = @import("../sql_parser.zig").nameApart;
const ownsStrippedKey = @import("../sql_parser.zig").ownsStrippedKey;
const keyHome = @import("../sql_parser.zig").keyHome;
const max_query_depth = Parser.max_query_depth;
const qualHasPrefix = @import("../sql_parser.zig").qualHasPrefix;
const sidesOf = @import("rewrite.zig").sidesOf;
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
            const cpos = self.curPos();
            var sp = ast.SplitBy{ .col = try self.expectColName(), .pos = cpos };
            _ = try self.expect(.rparen);
            if (self.eatKw("jobs")) {
                const jpos = self.curPos();
                const n = try self.expect(.int);
                const v = std.fmt.parseInt(usize, n.text, 10) catch 0;
                if (v == 0) return self.fail(jpos, "`JOBS` takes a positive lane count, not `{s}`", .{n.text});
                sp.jobs = v;
            }
            write.split = sp;
        } else if (self.isKw("with") and self.peekTag() == .lparen) {
            _ = self.advance();
            try self.parseWithHints(&whints);
        } else break;
    }

    try self.expectKw("as");
    var stages = std.array_list.Managed(ast.Stage).init(self.arena);
    self.top_names = .load;
    try self.parseQuery(out, &stages);
    try stages.append(.{ .node = .{ .write = write }, .hints = try whints.toOwnedSlice(), .pos = pos });
    _ = try self.expect(.semi);
    try out.append(.{ .output = .{ .stages = try stages.toOwnedSlice(), .pos = pos } });
}

pub fn parseTerminalQuery(self: *Parser, out: *std.array_list.Managed(ast.Stmt)) Error!void {
    const pos = self.curPos();
    var stages = std.array_list.Managed(ast.Stage).init(self.arena);
    self.top_names = .show;
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
    const out_names = self.top_names;
    self.top_names = .nested;
    const outer_in_where = self.in_where;
    self.in_where = false;
    defer self.in_where = outer_in_where;
    const outer_corr = self.corr;
    self.corr = null;
    defer self.corr = outer_corr;
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
    var uses = @import("textcols.zig").ColUses{ .parent = self.col_uses };
    self.col_uses = &uses;
    defer self.col_uses = uses.parent;
    defer if (self.pending_bindings.items.len > 0) {
        out.appendSlice(self.pending_bindings.items) catch {};
        self.pending_bindings.clearRetainingCapacity();
    };

    self.top_names = out_names;
    const first = try self.parseSelectCore();

    var grouped: ?[]const ast.QualName = null;
    const set_op = self.isKw("union") or self.isKw("intersect") or self.isKw("except");
    if (set_op) {
        try self.parseUnionTail(first, stages);
    } else {
        try stages.appendSlice(first.stages);
        if (stages.items[stages.items.len - 1].node != .distinct) grouped = first.group;
    }

    var hidden_drop: ?[]const ast.SelectItem = null;
    if (self.eatKw("order")) {
        try self.expectKw("by");
        var order_quals = std.array_list.Managed(Parser.QualRef).init(self.arena);
        const outer_quals = self.qual_sink;
        self.qual_sink = &order_quals;
        defer self.qual_sink = outer_quals;
        const outer_keys = self.key_aliases;
        self.key_aliases = &first.aliases;
        defer self.key_aliases = outer_keys;
        var keys = std.array_list.Managed(ast.SortKey).init(self.arena);
        var stripped = std.array_list.Managed(bool).init(self.arena);
        var computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var sort_aggs = std.array_list.Managed([]const u8).init(self.arena);
        while (true) {
            var q: ast.QualName = undefined;
            var was_qual = false;
            if (self.isKw("identifier") and self.peekTag() == .lparen) {
                q = try self.parseColRef();
            } else {
                const start = self.i;
                const kpos = self.curPos();
                var scratch = std.array_list.Managed(Parser.WinCall).init(self.arena);
                const e = blk: {
                    const outer_sink = self.win_sink;
                    self.win_sink = &scratch;
                    defer self.win_sink = outer_sink;
                    break :blk try self.parseExpr();
                };
                if (scratch.items.len > 0) {
                    const synth = try self.synthName(start);
                    q = try self.singleName(resolveExprAlias(first.expr_aliases, synth));
                    if (q.parts[0].ptr == synth.ptr)
                        return self.fail(kpos, "a window function in ORDER BY must repeat one the SELECT list computes — or give that one an alias and order by it", .{});
                } else switch (e.*) {
                    .int_lit => |n| {
                        const outs = first.out_keys orelse
                            return self.fail(kpos, "ORDER BY {d} needs a SELECT list without `*` — name the column instead", .{n});
                        if (n < 1 or n > outs.len)
                            return self.fail(kpos, "ORDER BY position {d} is out of range", .{n});
                        q = stripQual(outs[@intCast(n - 1)], &first.aliases);
                    },
                    .str_lit => |s| {
                        try unambiguous(self, first.dups, s, "ORDER BY", kpos);
                        q = try self.singleName(s);
                    },
                    .field => |f| {
                        if (f.parts.len == 1 and first.out_keys != null and namesOutput(first.out_keys, f.parts[0]))
                            if (self.col_uses) |u| u.outputUse(f);
                        if (f.parts.len == 1) try unambiguous(self, first.dups, f.parts[0], "ORDER BY", kpos);
                        q = try distinctSortKey(self, stages.items, f, stripQual(f, &first.aliases), kpos);
                        was_qual = q.parts.len < f.parts.len;
                        if (f.parts.len == 1 and namesOutput(first.out_keys, f.parts[0])) q.sides = &.{};
                    },
                    else => {
                        const b = try self.bindName(e, first.expr_aliases);
                        q = try self.singleName(b.name);
                        if (!b.item) {
                            if (grouped) |g| {
                                const r = try groupedSortKey(self, e, g, first, stages.items, &sort_aggs, kpos);
                                if (r.* == .field and r.field.parts.len == 1) {
                                    q = r.field;
                                } else if (!providesName(computed.items, b.name)) {
                                    try computed.append(.{ .computed = .{ .name = b.name, .expr = r } });
                                }
                            } else {
                                if (containsAgg(e) and first.group != null and !set_op)
                                    return self.fail(kpos, "with SELECT DISTINCT, an aggregate in ORDER BY must be one the SELECT list computes", .{});
                                if (containsAgg(e))
                                    return self.fail(kpos, "an aggregate in ORDER BY must repeat one the SELECT list computes — or give that one an alias and order by it", .{});
                                if (!providesName(computed.items, b.name))
                                    try computed.append(.{ .computed = .{ .name = b.name, .expr = try self.stripExpr(e, &first.aliases) } });
                            }
                        }
                    },
                }
            }
            var desc = false;
            if (self.eatKw("desc")) {
                desc = true;
            } else _ = self.eatKw("asc");
            try keys.append(.{ .field = q, .desc = desc, .nulls_first = try self.parseNullsFirst(desc) });
            try stripped.append(was_qual);
            if (!self.eat(.comma)) break;
        }
        try self.settleQuals(order_quals.items, &first.aliases, outer_quals);
        try self.noteOwnCols(stages.items, order_quals.items, &first.aliases);
        const sort_keys = try keys.toOwnedSlice();

        var visible: ?[]const ast.SelectItem = null;
        if (distinctDropTail(stages.items)) visible = stages.pop().?.node.select;
        var extend_proj = false;
        if (lastSelectIdx(stages.items)) |si| {
            const proj = stages.items[si].node.select;
            var wildcard = false;
            var have_names = std.array_list.Managed([]const u8).init(self.arena);
            for (proj) |it| switch (it) {
                .field => |f| try have_names.append(f.last()),
                .computed => |c| try have_names.append(c.name),
                else => wildcard = true,
            };
            extend_proj = !wildcard;
            if (!wildcard) {
                var extended = std.array_list.Managed(ast.SelectItem).init(self.arena);
                try extended.appendSlice(proj);
                for (sort_keys, stripped.items) |*k, sq| {
                    if (sq and try placeStrippedKey(self, &extended, proj.len, &k.field)) continue;
                    if (k.field.parts.len != 1) {
                        try placeKey(self, &extended, proj.len, &k.field);
                        continue;
                    }
                    if (ast.nameIn(have_names.items, k.field.last())) continue;
                    try have_names.append(k.field.last());
                    const hidden = for (computed.items) |c| {
                        if (std.mem.eql(u8, c.computed.name, k.field.last())) break c;
                    } else ast.SelectItem{ .field = k.field };
                    try extended.append(hidden);
                }
                if (extended.items.len != proj.len) {
                    nameApart(self.arena, extended.items[0..proj.len]) catch return error.OutOfMemory;
                    hidden_drop = keepOutputs(self.arena, visible orelse extended.items[0..proj.len]) catch return error.OutOfMemory;
                    stages.items[si].node.select = try extended.toOwnedSlice();
                }
            }
        }
        if (!extend_proj and visible == null and stages.items[stages.items.len - 1].node == .select)
            hidden_drop = try self.keepSortedCopies(&stages.items[stages.items.len - 1], sort_keys);
        if (!extend_proj and (computed.items.len > 0 or sort_aggs.items.len > 0)) {
            var names = std.array_list.Managed([]const u8).init(self.arena);
            try names.appendSlice(sort_aggs.items);
            if (computed.items.len > 0) {
                const tail = stages.items[stages.items.len - 1].node;
                if (visible != null or (tail == .distinct and tail.distinct.on != null))
                    return self.fail(self.curPos(), "ORDER BY with DISTINCT ON sorts by columns or SELECT items, not by a new expression — add it to the SELECT list", .{});
                var ext = std.array_list.Managed(ast.SelectItem).init(self.arena);
                try ext.append(.star);
                for (computed.items) |c| {
                    try ext.append(c);
                    try names.append(c.computed.name);
                }
                try stages.append(.{ .node = .{ .select = try ext.toOwnedSlice() }, .hints = &.{}, .pos = self.curPos() });
            }
            hidden_drop = try self.arena.dupe(ast.SelectItem, &.{.{ .star_except = try names.toOwnedSlice() }});
        }
        if (hidden_drop == null) hidden_drop = visible;
        const sort_stage: ast.Stage = .{ .node = .{ .sort = .{ .keys = sort_keys } }, .hints = &.{}, .pos = self.curPos() };
        const last = stages.items.len - 1;
        if (stages.items[last].node == .distinct and stages.items[last].node.distinct.on != null)
            try stages.insert(last, sort_stage)
        else
            try stages.append(sort_stage);
    }

    const lpos = self.curPos();
    if (try parseLimit(self)) |lim|
        try stages.append(.{ .node = .{ .limit = lim }, .hints = &.{}, .pos = lpos });

    if (hidden_drop) |keep|
        try stages.append(.{ .node = .{ .select = keep }, .hints = &.{}, .pos = self.curPos() });
    if (first.final_names) |fin|
        try stages.append(.{ .node = .{ .select = fin }, .hints = &.{}, .pos = self.curPos() });
    self.query_where_end = if (set_op) null else first.where_end;
    if (stages.items.len > 0 and stages.items[0].node == .read) {
        const rd = &stages.items[0].node.read;
        if (std.mem.eql(u8, rd.connector, "csv") and rd.form == .path) rd.text_cols = try uses.textCols(self.arena);
    }
}

/// Under a plain DISTINCT a sort key must be a column the SELECT list outputs, as SQL
/// requires: a key spelled as an item's column takes that item's name, and one a list
/// without `*` lacks, or a USING side's copy, which no `*` keeps, is refused under the
/// name the query wrote.
/// Whether a bare ORDER BY `name` is an output column, which SQL reads first: every
/// one under `*` (`out_keys` null), whose clashing names the join already set
/// apart, else one the SELECT list names; only an input column past those can be
/// ambiguous.
fn namesOutput(out_keys: ?[]const ast.QualName, name: []const u8) bool {
    const keys = out_keys orelse return true;
    for (keys) |k| if (std.mem.eql(u8, k.last(), name)) return true;
    return false;
}

fn distinctSortKey(self: *Parser, stages: []const ast.Stage, written: ast.QualName, q: ast.QualName, at: Pos) Error!ast.QualName {
    const n = stages.len;
    if (n < 2 or stages[n - 1].node != .distinct or stages[n - 1].node.distinct.on != null or stages[n - 2].node != .select) return q;
    const items = stages[n - 2].node.select;
    switch (keyHome(items, q)) {
        .as_is => return q,
        .renamed => |nm| return self.singleName(nm),
        .missing => {},
    }
    const listed = for (items) |it| {
        if (it != .field and it != .computed) break false;
    } else true;
    if (!listed and !std.mem.startsWith(u8, q.last(), "__")) return q;
    return self.fail(at, "for SELECT DISTINCT, ORDER BY `{s}` must appear in the select list", .{try std.mem.join(self.arena, ".", written.parts)});
}

/// What a bare GROUP BY name groups by. SQL reads it as an input column first and as
/// a SELECT item's alias only when no input column has the name: one the list reads
/// (`SELECT g`, or `upper(g) AS g`) surely exists, so the input wins; otherwise the
/// alias is taken, as the parser cannot see the input's columns.
fn groupNameHome(self: *Parser, raw: anytype, n: []const u8, aliases: *const AliasSet) Error!enum { input, input_under_alias, alias } {
    for (raw) |ri| if (ri == .item and ri.item == .field) {
        const f = stripQual(ri.item.field, aliases);
        if (f.parts.len == 1 and std.mem.eql(u8, f.last(), n)) return .input;
    };
    for (raw) |ri| if (ri == .item and ri.item == .computed and std.mem.eql(u8, ri.item.computed.name, n)) {
        const e = try self.stripExpr(ri.item.computed.expr, aliases);
        if (e.* == .field and !e.field.dollar and e.field.parts.len == 1 and std.mem.eql(u8, e.field.parts[0], n)) return .input;
        return if (Window.refsName(e, n)) .input_under_alias else .alias;
    };
    return .input;
}

/// `DISTINCT ON` keys are written against the input, as in Postgres: a key the list
/// renamed is repointed at its output, one it does not project rides as a hidden
/// column. Returns the projection that drops those again, or null.
fn distinctOnOutputs(self: *Parser, items: *std.array_list.Managed(ast.SelectItem), on: []ast.QualName, stripped: []const bool) Error!?[]const ast.SelectItem {
    for (items.items) |it| switch (it) {
        .field, .computed => {},
        else => return null,
    };
    const visible = items.items.len;
    for (on, stripped) |*k, sq| {
        if (!(sq and try placeStrippedKey(self, items, visible, k))) try placeKey(self, items, visible, k);
    }
    if (items.items.len == visible) return null;
    nameApart(self.arena, items.items[0..visible]) catch return error.OutOfMemory;
    return keepOutputs(self.arena, items.items[0..visible]) catch return error.OutOfMemory;
}

/// A sort or `DISTINCT ON` key written with the FROM table (`a.s`, stripped to `s`)
/// where the outputs `items[0..visible]` have a column of its name: that column when
/// it is the key's own, else a hidden copy of the key. False when none has the name.
fn placeStrippedKey(self: *Parser, items: *std.array_list.Managed(ast.SelectItem), visible: usize, k: *ast.QualName) Error!bool {
    const vis = items.items[0..visible];
    if (ownsStrippedKey(vis, k.*)) return true;
    if (!providesName(vis, k.last())) return false;
    const b = try self.bindName(try self.mk(.{ .field = k.* }), &.{});
    if (!providesName(items.items, b.name)) try items.append(.{ .computed = .{ .name = b.name, .expr = try self.mk(.{ .field = k.* }) } });
    k.* = try self.singleName(b.name);
    return true;
}

/// A key read from the output that carries it, as itself or renamed, else carried
/// beside the outputs `items[0..visible]`.
fn placeKey(self: *Parser, items: *std.array_list.Managed(ast.SelectItem), visible: usize, k: *ast.QualName) Error!void {
    switch (keyHome(items.items[0..visible], k.*)) {
        .as_is => {},
        .renamed => |nm| k.* = try self.singleName(nm),
        .missing => try items.append(.{ .field = k.* }),
    }
}

/// Refuses an ORDER BY or GROUP BY on a name two SELECT items share, unless they are
/// the same expression, as SQL does.
fn unambiguous(self: *Parser, dups: []const Parser.DupName, name: []const u8, clause: []const u8, at: Pos) Error!void {
    for (dups) |d| if (std.mem.eql(u8, d.name, name) and !d.same)
        return self.fail(at, "{s} `{s}` is ambiguous: two SELECT items are named so — alias one apart", .{ clause, name });
}

/// Two SELECT items named alike. A shown result keeps both names, as SQL does: the
/// later item runs under a hidden name (`renamed` holds it beside the shown one) and
/// a projection at the very end restores it, so nothing that reads columns by name
/// in between mixes them up. A LOAD target or an enclosing query needs the names
/// apart, so there it is refused. Two plain columns of which one is written with a
/// joined table (`b.s`, `a.s`) are not a repeat: the later is named `s_r`, as a
/// join's colliding columns are.
fn nameRepeats(
    self: *Parser,
    mode: Parser.OutNames,
    raw: anytype,
    exprs: []Parser.ExprAlias,
    wins: []Parser.WinCall,
    aliases: *const AliasSet,
    dups: *std.array_list.Managed(Parser.DupName),
    renamed: *std.array_list.Managed([2][]const u8),
    pos: Pos,
) Error!void {
    const Seen = struct { name: []const u8, key: []const u8, field: bool, qualified: bool };
    var seen = std.array_list.Managed(Seen).init(self.arena);
    var star = false;
    var ea: usize = 0;
    for (raw) |*ri| {
        var key = std.array_list.Managed(u8).init(self.arena);
        var field = false;
        var qualified = false;
        var alias_at: ?usize = null;
        const name: []const u8 = switch (ri.*) {
            .win => |wi| blk: {
                alias_at = ea;
                ea += 1;
                try key.appendSlice(exprs[alias_at.?].synth);
                break :blk wins[wi].out.?;
            },
            .qstar => {
                star = true;
                continue;
            },
            .item => |it| switch (it) {
                .field => |f| blk: {
                    const s = stripQual(f, aliases);
                    const fe: ast.Expr = .{ .field = s };
                    try self.exprKey(&fe, &key);
                    field = true;
                    qualified = s.parts.len > 1;
                    break :blk s.last();
                },
                .computed => |c| blk: {
                    if (c.expr.* != .field or c.expr.field.dollar) {
                        alias_at = ea;
                        ea += 1;
                    }
                    try self.exprKey(try self.stripExpr(c.expr, aliases), &key);
                    break :blk c.name;
                },
                .star, .star_except, .star_rename, .star_of => {
                    star = true;
                    continue;
                },
            },
        };
        const prev = for (seen.items) |s| {
            if (std.mem.eql(u8, s.name, name)) break s;
        } else null;
        const p = prev orelse {
            try seen.append(.{ .name = name, .key = key.items, .field = field, .qualified = qualified });
            continue;
        };
        if (field and p.field and (qualified or p.qualified)) continue;
        if (mode == .load) return self.fail(pos, "LOAD INTO writes each column under its own name, and two SELECT items are named `{s}` — give one a different alias", .{name});
        const same = std.mem.eql(u8, p.key, key.items);
        for (dups.items) |*d| {
            if (std.mem.eql(u8, d.name, name)) {
                d.same = d.same and same;
                break;
            }
        } else try dups.append(.{ .name = name, .same = same });
        const hidden = try self.freshName("__d{d}");
        switch (ri.*) {
            .win => |wi| wins[wi].out = hidden,
            .item => |it| switch (it) {
                .field => |f| {
                    const e = try self.mk(.{ .field = f });
                    ri.* = .{ .item = .{ .computed = .{ .name = hidden, .expr = e } } };
                },
                .computed => |c| {
                    const e = c.expr;
                    ri.* = .{ .item = .{ .computed = .{ .name = hidden, .expr = e } } };
                },
                else => unreachable,
            },
            .qstar => unreachable,
        }
        if (alias_at) |i| exprs[i].out = hidden;
        try renamed.append(.{ hidden, name });
    }
    if (star and renamed.items.len > 0)
        return self.fail(pos, "two SELECT items are named `{s}` — give one a different alias", .{renamed.items[0][1]});
}

/// An ORDER BY expression of a grouped query, read past the aggregate: a grouping
/// expression it spells is that key's column, an aggregate the SELECT list leaves out
/// is computed beside the others and named in `added` (dropped after the LIMIT, as
/// HAVING's are), and any other column is refused, as in the SELECT list.
fn groupedSortKey(self: *Parser, e: *ast.Expr, group: []const ast.QualName, first: Core, stages: []ast.Stage, added: *std.array_list.Managed([]const u8), pos: Pos) Error!*ast.Expr {
    const keyed = try self.stripExpr(try self.groupKeyRefs(e, group, first.expr_aliases), &first.aliases);
    var ai = stages.len;
    while (ai > 0 and stages[ai - 1].node != .aggregate) ai -= 1;
    if (ai == 0) return keyed;
    const ag = &stages[ai - 1].node.aggregate;
    var aggs = std.array_list.Managed(ast.AggItem).init(self.arena);
    try aggs.appendSlice(ag.aggs);
    const r = try self.liftAggs(keyed, &aggs, first.expr_aliases, pos);
    try self.groupedOnly(r, group, aggs.items, &.{}, pos);
    if (aggs.items.len == ag.aggs.len) return r;
    for (aggs.items[ag.aggs.len..]) |a| {
        if (a.distinct and a.func != .count)
            return self.fail(pos, "DISTINCT is only supported inside COUNT", .{});
        try added.append(a.name);
        if (a.arg) |arg| try carryAggInput(self, stages, ai - 1, arg);
    }
    ag.aggs = try aggs.toOwnedSlice();
    return r;
}

/// The columns `arg` reads, added to the projection the aggregate at `ai` reads
/// through when it names its columns one by one.
fn carryAggInput(self: *Parser, stages: []ast.Stage, ai: usize, arg: *const ast.Expr) Error!void {
    if (ai == 0 or stages[ai - 1].node != .select) return;
    const pre = stages[ai - 1].node.select;
    for (pre) |it| if (it != .field and it != .computed) return;
    var needed = std.array_list.Managed(ast.QualName).init(self.arena);
    try self.collectFields(arg, &needed);
    var ext = std.array_list.Managed(ast.SelectItem).init(self.arena);
    try ext.appendSlice(pre);
    for (needed.items) |q| {
        if (q.parts.len == 1 and !q.dollar and !providesName(ext.items, q.last())) try ext.append(.{ .field = q });
    }
    stages[ai - 1].node = .{ .select = try ext.toOwnedSlice() };
}

/// `LIMIT n|ALL`, `OFFSET m [ROWS]` and `FETCH FIRST|NEXT [n] ROWS ONLY`, OFFSET before
/// or after the other, as SQL allows; FETCH is the standard's spelling of LIMIT.
fn parseLimit(self: *Parser) Error!?ast.Limit {
    var lim: ast.Limit = .{ .count = std.math.maxInt(u64) };
    var counted = false;
    var offset = false;
    while (true) {
        const kpos = self.curPos();
        if (self.isKw("limit") or self.isKw("fetch")) {
            if (counted) return self.fail(kpos, "a query takes one LIMIT or FETCH FIRST", .{});
            counted = true;
            if (self.eatKw("limit")) {
                if (!self.eatKw("all")) try limitCount(self, "LIMIT", &lim.count, &lim.count_expr);
                continue;
            }
            _ = self.advance();
            if (!self.eatKw("first") and !self.eatKw("next"))
                return self.fail(self.curPos(), "expected `FIRST` or `NEXT` after FETCH", .{});
            if (self.isKw("row") or self.isKw("rows")) lim.count = 1 else try limitCount(self, "FETCH FIRST", &lim.count, &lim.count_expr);
            if (!self.eatKw("row") and !self.eatKw("rows"))
                return self.fail(self.curPos(), "expected `ROWS ONLY` after FETCH FIRST n", .{});
            if (self.isKw("with"))
                return self.fail(self.curPos(), "FETCH FIRST ... WITH TIES is not supported; write ROWS ONLY", .{});
            try self.expectKw("only");
        } else if (self.eatKw("offset")) {
            if (offset) return self.fail(kpos, "a query takes one OFFSET", .{});
            offset = true;
            try limitCount(self, "OFFSET", &lim.offset, &lim.offset_expr);
            _ = self.eatKw("row") or self.eatKw("rows");
        } else break;
    }
    return if (counted or offset) lim else null;
}

/// A LIMIT or OFFSET value: an integer literal is read here, and a literal with a
/// fraction is rounded half away from zero, as SQL rounds the exact number it spells
/// (`LIMIT 2.5` is 3); any other constant, an expression of `$name`s and literals, is
/// kept to evaluate once those have values. A column has no single value to take, so
/// it is refused.
fn limitCount(self: *Parser, what: []const u8, n: *u64, expr: *?*ast.Expr) Error!void {
    const vpos = self.curPos();
    const e = try self.parseExpr();
    const neg = e.* == .unary and e.unary.op == .neg;
    const base = if (neg) e.unary.e else e;
    if (base.* == .float_lit or base.* == .dec_lit) {
        var r: f64 = undefined;
        var shown: f64 = undefined;
        if (base.* == .float_lit) {
            const f = base.float_lit;
            r = if (@abs(f - @trunc(f)) == 0.5) 2 * @round(f / 2) else @round(f);
            shown = f;
        } else {
            const d = base.dec_lit;
            const p = std.math.powi(i128, 10, d.scale) catch return self.fail(vpos, "{s} is out of range", .{what});
            var q = @divTrunc(d.unscaled, p);
            if (@abs(@rem(d.unscaled, p)) * 2 >= p) q += 1;
            r = @floatFromInt(q);
            shown = @as(f64, @floatFromInt(d.unscaled)) / @as(f64, @floatFromInt(p));
        }
        if (neg) {
            r = -r;
            shown = -shown;
        }
        if (r < 0) return self.fail(vpos, "{s} must not be negative, got {d}", .{ what, shown });
        if (!(r < 0x1p63)) return self.fail(vpos, "{s} {d} is out of range", .{ what, shown });
        n.* = @intFromFloat(@abs(r));
        return;
    }
    if (e.* == .int_lit) {
        if (e.int_lit < 0) return self.fail(vpos, "{s} must not be negative, got {d}", .{ what, e.int_lit });
        n.* = @intCast(e.int_lit);
        return;
    }
    if (!self.constItemExpr(e))
        return self.fail(vpos, "{s} takes a constant: an integer, a `$param` or LET, or an expression of them, not a column", .{what});
    expr.* = e;
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
/// Without a key it all stays the join's condition, run as a nested-loop join.
pub fn parseSelectCore(self: *Parser) Error!Core {
    const pos = self.curPos();
    try self.expectKw("select");
    const out_names = self.top_names;
    self.top_names = .nested;
    var quals = std.array_list.Managed(Parser.QualRef).init(self.arena);
    const outer_quals = self.qual_sink;
    self.qual_sink = &quals;
    defer self.qual_sink = outer_quals;
    const outer_keys = self.key_aliases;
    self.key_aliases = null;
    defer self.key_aliases = outer_keys;
    var scope = Parser.CorrScope{ .scalars = .init(self.arena) };
    const outer_corr = self.corr;
    defer self.corr = outer_corr;

    var distinct = false;
    var distinct_on: ?[]ast.QualName = null;
    var distinct_drop: ?[]const ast.SelectItem = null;
    if (self.eatKw("distinct")) {
        distinct = true;
        if (self.eatKw("on")) {
            _ = try self.expect(.lparen);
            var cols = std.array_list.Managed(ast.QualName).init(self.arena);
            while (true) {
                const cpos = self.curPos();
                const q = try self.parseColRef();
                try self.noteQual(q, cpos);
                try cols.append(q);
                if (!self.eat(.comma)) break;
            }
            _ = try self.expect(.rparen);
            distinct_on = try cols.toOwnedSlice();
        }
    }

    const RawItem = union(enum) {
        item: ast.SelectItem,
        qstar: []const u8,
        win: usize,
    };
    var raw_items = std.array_list.Managed(RawItem).init(self.arena);
    var expr_aliases = std.array_list.Managed(ExprAlias).init(self.arena);
    var win_calls = std.array_list.Managed(Parser.WinCall).init(self.arena);
    const outer_sink = self.win_sink;
    self.win_sink = &win_calls;
    defer self.win_sink = outer_sink;
    self.corr = &scope;
    while (true) {
        if (self.at(.star) or (self.at(.ident) and self.peekTag() == .dot and self.i + 2 < self.toks.len and self.toks[self.i + 2].tag == .star))
            if (self.col_uses) |u| u.opaqueUse(false);
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
            const before = win_calls.items.len;
            const e = try self.parseExpr();
            if (e.* == .field and e.field.parts.len == 1 and win_calls.items.len == before + 1 and
                std.mem.eql(u8, e.field.parts[0], win_calls.items[before].hidden))
            {
                const sy = try self.synthRange(start, self.i);
                const out = (try self.itemAlias()) orelse win_calls.items[before].name;
                win_calls.items[before].out = out;
                try expr_aliases.append(.{ .synth = sy, .out = out });
                try raw_items.append(.{ .win = before });
                if (!self.eat(.comma)) break;
                continue;
            }
            const synth: ?[]const u8 = if (e.* == .field and !e.field.dollar) null else try self.synthRange(start, self.i);
            const name = try self.itemAlias();
            if (synth) |sy| {
                var key = std.array_list.Managed(u8).init(self.arena);
                const keyed = win_calls.items.len == before;
                if (keyed) try self.exprKey(e, &key) else try key.appendSlice(sy);
                try expr_aliases.append(.{ .synth = try key.toOwnedSlice(), .out = name orelse sy, .expr = if (keyed) e else null });
            }
            if (name) |n| {
                try raw_items.append(.{ .item = .{ .computed = .{ .name = n, .expr = e } } });
            } else if (e.* == .field and !e.field.dollar) {
                try raw_items.append(.{ .item = .{ .field = e.field } });
            } else {
                try raw_items.append(.{ .item = .{ .computed = .{ .name = synth.?, .expr = e } } });
            }
        }
        if (!self.eat(.comma)) break;
    }

    self.win_sink = null;
    self.corr = null;

    var aliases = AliasSet{};
    var read_hints = std.array_list.Managed(ast.Hint).init(self.arena);
    const src: ast.Stage.Node = if (self.eatKw("from"))
        try self.parseFromSource(&aliases, &read_hints)
    else
        .{ .read = .{ .connector = "unit", .form = .unit } };

    while (true) {
        if (self.eatKw("pushdown")) {
            _ = try self.expect(.lparen);
            self.qual_sink = null;
            const e = try self.parseExpr();
            self.qual_sink = &quals;
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
        if (self.isKw("natural"))
            return self.fail(self.curPos(), "NATURAL JOIN is not supported — name the shared columns: `JOIN t USING (a, b)`", .{});
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
                const dpos = self.curPos();
                const dt = try self.expect(.string);
                if (dt.text.len == 0) return self.fail(dpos, "SPLIT needs a non-empty delimiter", .{});
                delim = dt.text;
                _ = try self.expect(.rparen);
            } else {
                field = try self.expectIdent();
            }
            if (self.col_uses) |u| try u.bareUse(self.arena, try self.singleName(field));
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
        var read_join_hints: []const ast.Hint = &.{};
        var binding: []const u8 = undefined;
        var bpos: Pos = undefined;
        var own: []const u8 = "";
        const reads = self.at(.string) or
            (self.isKw("identifier") and self.peekTag() == .lparen) or
            (self.at(.ident) and self.peekTag() == .dot and !self.isLet(self.cur().text));
        if (self.at(.lparen) or reads) {
            const d = if (reads) try self.parseJoinRead() else try self.parseDerivedTable();
            binding = d.binding;
            jalias = d.alias;
            bpos = d.alias_pos;
            read_join_hints = d.join_hints;
            own = d.own;
        } else {
            binding = try self.expectIdent();
            bpos = self.prevPos();
            const written = binding;
            self.eatAliasAs();
            const has_alias = self.at(.lparen) or self.atAlias();
            if (self.at(.lparen)) {
                const call = try self.callTableFn(binding, bpos, if (lateral) .lateral else .join);
                binding = call.binding;
                lateral_keys = call.keys;
                if (!self.atAlias()) jalias = written;
            } else if (if (self.tvf) |f| f.cte(binding) else null) |bname| {
                binding = bname;
                if (!has_alias) jalias = written;
            } else if (!self.isLet(binding))
                return self.fail(jpos, "JOIN right side `{s}` must be a WITH-defined CTE", .{binding});
        }
        if (jalias) |ja| {
            try self.claimAlias(&aliases, ja, bpos, .right);
        } else if (self.atAlias()) {
            jalias = self.advance().text;
            try self.claimAlias(&aliases, jalias.?, self.prevPos(), .right);
        } else if (own.len > 0 and !aliases.knows(own)) {
            jalias = own;
            try self.claimAlias(&aliases, own, bpos, .right);
        } else try self.claimAlias(&aliases, binding, bpos, .reserved);
        var left_keys = std.array_list.Managed(ast.QualName).init(self.arena);
        var right_keys = std.array_list.Managed(ast.QualName).init(self.arena);
        var pins = std.array_list.Managed(ast.KeyPin).init(self.arena);
        var post_filters = std.array_list.Managed(*ast.Expr).init(self.arena);
        var left_computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var right_computed = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var deferred = std.array_list.Managed(ast.DeferredKey).init(self.arena);
        var residual: ?*ast.Expr = null;
        var using_sel: ?[]const ast.SelectItem = null;
        for (lateral_keys) |lk| {
            try left_keys.append(stripQual(lk.left, &aliases));
            try right_keys.append(try self.singleName(lk.right));
            try pins.append(.{ .left = true, .right = true });
        }
        if (lateral and lateral_keys.len > 0 and kind == .cross) kind = .inner;
        if (lateral and self.isKw("on") and self.peekTag() == .ident and eqlNoCase(self.peekTok().text, "true")) {
            _ = self.advance();
            _ = self.advance();
        } else if (lateral and lateral_keys.len > 0 and !self.isKw("on")) {} else if (kind == .cross) {
            if (self.isKw("on") or self.isKw("using"))
                return self.fail(self.curPos(), "CROSS JOIN takes no ON or USING clause — it pairs every row with every row", .{});
        } else if (self.eatKw("using")) {
            const u = try self.parseUsing(kind, binding, jpos, &left_keys, &right_keys);
            using_sel = u.select;
            if (jalias == null) jalias = binding;
            aliases.setUsing(jalias.?, u.keys);
            binding = u.binding;
        } else {
            if (!self.isKw("on"))
                return self.fail(self.curPos(), "expected `ON <column> = <column>` or `USING (<column>, ...)` after JOIN {s}", .{binding});
            const opos = self.advance();
            const rname = jalias orelse binding;
            const on = try self.parseExpr();
            var conj = std.array_list.Managed(*ast.Expr).init(self.arena);
            try splitAnd(on, &conj);
            var narrow: ?*ast.Expr = null;
            for (conj.items) |c| {
                if (ast.hasSearch(c)) {
                    if (kind == .inner) {
                        try post_filters.append(c);
                    } else {
                        const e = try self.stripExpr(c, &aliases);
                        residual = if (residual) |r| try self.mk(.{ .binary = .{ .op = .@"and", .l = r, .r = e } }) else e;
                    }
                    continue;
                }
                if (c.* == .binary and c.binary.op == .eq and c.binary.l.* == .field and c.binary.r.* == .field and
                    !c.binary.l.field.dollar and !c.binary.r.field.dollar)
                {
                    const a = c.binary.l.field;
                    const b = c.binary.r.field;
                    const sa = keySide(a, rname);
                    const sb = keySide(b, rname);
                    if (sa != sb or sa == .loose) {
                        const flip = sa == .right or sb == .left;
                        const l = if (flip) b else a;
                        const r = if (flip) a else b;
                        try left_keys.append(stripQual(l, &aliases));
                        try right_keys.append(stripPrefix(r, rname));
                        try pins.append(.{ .left = keySide(l, rname) == .left, .right = keySide(r, rname) == .right });
                        continue;
                    }
                }
                if (c.* == .binary and c.binary.op == .eq) {
                    const sl = sidesOf(c.binary.l, rname);
                    const sr = sidesOf(c.binary.r, rname);
                    const l_left = sl.other and !sl.right;
                    const r_left = sr.other and !sr.right;
                    const l_right = sl.right and !sl.other;
                    const r_right = sr.right and !sr.other;
                    if ((l_left and r_right) or (l_right and r_left)) {
                        const le = if (l_left) c.binary.l else c.binary.r;
                        const re = if (l_left) c.binary.r else c.binary.l;
                        try pins.append(.{ .left = le.* == .field and le.field.parts.len > 1, .right = true });
                        if (le.* == .field) {
                            try left_keys.append(stripQual(le.field, &aliases));
                        } else {
                            const k = try self.freshName("__jk{d}");
                            try left_computed.append(.{ .computed = .{ .name = k, .expr = try self.stripExpr(le, &aliases) } });
                            try key_cols.append(k);
                            try left_keys.append(try self.singleName(k));
                        }
                        if (re.* == .field) {
                            try right_keys.append(stripPrefix(re.field, rname));
                        } else {
                            const k = try self.freshName("__jk{d}");
                            try right_computed.append(.{ .computed = .{ .name = k, .expr = try self.stripRightExpr(re, rname) } });
                            try key_cols.append(k);
                            try right_keys.append(try self.singleName(k));
                        }
                        continue;
                    }
                    if (!sl.right and !sr.right and sl.other and sr.other and
                        (hasFieldOf(c.binary.l, false) or hasFieldOf(c.binary.r, false)))
                    {
                        const ln = try self.freshName("__jk{d}");
                        const rn = try self.freshName("__jk{d}");
                        try deferred.append(.{
                            .a = try self.stripExpr(c.binary.l, &aliases),
                            .b = try self.stripExpr(c.binary.r, &aliases),
                            .left_name = ln,
                            .right_name = rn,
                            .pos = .{ .line = opos.line, .col = opos.col, .file = opos.file },
                            .a_left = hasFieldOf(c.binary.l, true),
                            .b_left = hasFieldOf(c.binary.r, true),
                        });
                        try key_cols.append(ln);
                        try key_cols.append(rn);
                        continue;
                    }
                }
                const right_only = !sidesOf(c, rname).other;
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
            if (left_keys.items.len == 0 and deferred.items.len == 0) {
                for (post_filters.items) |c| {
                    const e = try self.stripExpr(c, &aliases);
                    residual = if (residual) |r| try self.mk(.{ .binary = .{ .op = .@"and", .l = r, .r = e } }) else e;
                }
                post_filters.clearRetainingCapacity();
            }
            if (narrow != null or right_computed.items.len > 0) {
                const name = try self.freshName("__derived{d}_on");
                var st = std.array_list.Managed(ast.Stage).init(self.arena);
                try st.append(.{ .node = .{ .ref = binding }, .hints = &.{}, .pos = jpos });
                if (narrow) |n| try st.append(.{ .node = .{ .filter = n }, .hints = &.{}, .pos = jpos });
                if (right_computed.items.len > 0) {
                    try right_computed.insert(0, .star);
                    try st.append(.{ .node = .{ .select = try right_computed.toOwnedSlice() }, .hints = &.{}, .pos = jpos });
                }
                try self.bindHidden(name, try st.toOwnedSlice(), jpos);
                if (jalias == null) jalias = binding;
                binding = name;
            }
            if (left_computed.items.len > 0) {
                try left_computed.insert(0, .star);
                try stages.append(.{ .node = .{ .select = try left_computed.toOwnedSlice() }, .hints = &.{}, .pos = jpos });
            }
        }
        var jhints = std.array_list.Managed(ast.Hint).init(self.arena);
        try jhints.appendSlice(read_join_hints);
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
                .pins = try pins.toOwnedSlice(),
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
        if (using_sel) |items| try stages.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = jpos });
    }

    var corr_hidden = std.array_list.Managed([]const u8).init(self.arena);
    {
        var fpos = pos;
        var remaining: ?*ast.Expr = null;
        const sj_base = self.pending_semijoins.items.len;
        if (self.eatKw("where")) {
            fpos = self.curPos();
            self.in_where = true;
            self.corr = &scope;
            const raw = try self.parseExpr();
            self.in_where = false;
            self.corr = null;
            remaining = if (self.pending_semijoins.items.len > sj_base)
                try self.liftSemiJoins(raw, fpos)
            else
                raw;
        }
        try Correlate.lowerWhere(self, &stages, &aliases, remaining, fpos, sj_base, scope.scalars.items, &corr_hidden);
    }
    const where_end = stages.items.len + @intFromBool(key_cols.items.len > 0);
    if (key_cols.items.len > 0) {
        const drop = try self.arena.dupe(ast.SelectItem, &.{.{ .star_except = try key_cols.toOwnedSlice() }});
        try stages.append(.{ .node = .{ .select = drop }, .hints = &.{}, .pos = pos });
    }

    self.key_aliases = &aliases;
    for (expr_aliases.items) |*m| if (m.expr) |e| {
        var key = std.array_list.Managed(u8).init(self.arena);
        try self.exprKey(e, &key);
        m.synth = try key.toOwnedSlice();
    };

    var dups = std.array_list.Managed(Parser.DupName).init(self.arena);
    var renamed = std.array_list.Managed([2][]const u8).init(self.arena);
    try nameRepeats(self, out_names, raw_items.items, expr_aliases.items, win_calls.items, &aliases, &dups, &renamed, pos);

    var out_keys: ?[]const ast.QualName = null;
    var final_names: ?[]const ast.SelectItem = null;
    {
        var keys = std.array_list.Managed(ast.QualName).init(self.arena);
        var shown = std.array_list.Managed(ast.SelectItem).init(self.arena);
        var all = true;
        for (raw_items.items) |ri| {
            const k: ast.QualName = switch (ri) {
                .win => |wi| try self.singleName(win_calls.items[wi].out.?),
                .qstar => {
                    all = false;
                    continue;
                },
                .item => |it| switch (it) {
                    .field => |f| f,
                    .computed => |c| try self.singleName(c.name),
                    .star, .star_except, .star_rename, .star_of => {
                        all = false;
                        continue;
                    },
                },
            };
            try keys.append(k);
            try shown.append(if (ri == .item and ri.item == .field)
                .{ .field = stripQual(k, &aliases) }
            else
                .{ .computed = .{ .name = k.parts[0], .expr = try self.mk(.null_lit) } });
        }
        if (all) out_keys = try keys.toOwnedSlice();
        if (all and renamed.items.len > 0) {
            nameApart(self.arena, shown.items) catch return error.OutOfMemory;
            const fin = keepOutputs(self.arena, shown.items) catch return error.OutOfMemory;
            const restored = try self.arena.dupe(ast.SelectItem, fin);
            for (restored) |*it| for (renamed.items) |r| {
                if (it.* == .field and it.field.parts.len == 1 and std.mem.eql(u8, it.field.parts[0], r[0])) {
                    const e = try self.mk(.{ .field = it.field });
                    it.* = .{ .computed = .{ .name = r[1], .expr = e } };
                }
            };
            final_names = restored;
        }
    }

    var group: []const ast.QualName = &.{};
    var group_hidden = std.array_list.Managed(ast.SelectItem).init(self.arena);
    var input_named = std.array_list.Managed([]const u8).init(self.arena);
    if (self.eatKw("group")) {
        try self.expectKw("by");
        var keys = std.array_list.Managed(ast.QualName).init(self.arena);
        while (true) {
            const gpos = self.curPos();
            const ge = try self.parseExpr();
            var q: ast.QualName = undefined;
            if (ge.* == .int_lit) {
                const n = ge.int_lit;
                if (n < 1 or n > @as(i64, @intCast(raw_items.items.len)))
                    return self.fail(gpos, "GROUP BY position {d} is out of range", .{n});
                const ri = raw_items.items[@intCast(n - 1)];
                if (ri == .win) return self.fail(gpos, "GROUP BY position {d} is a window function — group on it in an outer query", .{n});
                if (ri != .item) return self.fail(gpos, "GROUP BY position {d} refers to `*`", .{n});
                q = switch (ri.item) {
                    .field => |f| stripQual(f, &aliases),
                    .computed => |c| blk: {
                        if (containsAgg(c.expr))
                            return self.fail(gpos, "GROUP BY position {d} names the aggregate `{s}` — a position may only name a grouping key", .{ n, c.name });
                        break :blk try self.singleName(c.name);
                    },
                    else => return self.fail(gpos, "GROUP BY position {d} refers to `*`", .{n}),
                };
            } else if (ge.* == .field) {
                if (ge.field.parts.len == 1) {
                    const n = ge.field.parts[0];
                    switch (try groupNameHome(self, raw_items.items, n, &aliases)) {
                        .input => {},
                        .input_under_alias => try input_named.append(n),
                        .alias => try unambiguous(self, dups.items, n, "GROUP BY", gpos),
                    }
                }
                q = stripQual(ge.field, &aliases);
            } else {
                if (containsAgg(ge)) return self.fail(gpos, "GROUP BY cannot group on an aggregate", .{});
                const b = try self.bindName(ge, expr_aliases.items);
                if (!b.item and !providesName(group_hidden.items, b.name)) try group_hidden.append(.{ .computed = .{ .name = b.name, .expr = try self.stripExpr(ge, &aliases) } });
                q = try self.singleName(b.name);
            }
            try keys.append(q);
            if (!self.eat(.comma)) break;
        }
        group = try keys.toOwnedSlice();
        if (input_named.items.len > 0) {
            var kept_aliases = std.array_list.Managed(ExprAlias).init(self.arena);
            for (expr_aliases.items) |m| if (!ast.nameIn(input_named.items, m.out)) try kept_aliases.append(m);
            expr_aliases = kept_aliases;
        }
    }

    var having: ?*ast.Expr = null;
    if (self.eatKw("having")) having = try self.parseExpr();

    var named = std.array_list.Managed(Parser.NamedWindow).init(self.arena);
    if (self.eatKw("window")) try self.parseWindowClause(&named);
    const specs = try self.arena.alloc(Window.Spec, win_calls.items.len);
    for (win_calls.items, specs) |*c, *sp| {
        const r = try self.resolveSpec(c.spec, named.items, 0);
        try self.checkCall(c.*, r);
        if (c.arg) |a| c.arg = try self.stripExpr(a, &aliases);
        const part = try self.arena.alloc(*ast.Expr, r.partition.len);
        for (r.partition, part) |e, *o| o.* = try self.stripExpr(e, &aliases);
        const ord = try self.arena.alloc(Window.OrderExpr, r.order.len);
        for (r.order, ord) |k, *o| o.* = .{ .e = try self.stripExpr(k.e, &aliases), .desc = k.desc, .nulls_first = k.nulls_first };
        sp.* = .{ .partition = part, .order = ord, .frame = r.frame, .pos = r.pos };
    }

    var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
    var aggs = std.array_list.Managed(ast.AggItem).init(self.arena);
    var union_tag: ?[]const u8 = null;
    var union_tag_col: ?[]const u8 = null;
    var star_count: usize = 0;
    var post = std.array_list.Managed(ast.SelectItem).init(self.arena);
    var lifted = false;
    var on_stripped: []bool = &.{};
    if (distinct_on) |on| {
        on_stripped = try self.arena.alloc(bool, on.len);
        for (on, on_stripped) |*k, *st| {
            const sq = stripQual(k.*, &aliases);
            st.* = sq.parts.len < k.parts.len;
            k.* = sq;
        }
    }
    var win_post = std.array_list.Managed(usize).init(self.arena);
    var using_drop = std.array_list.Managed([]const u8).init(self.arena);
    var using_copies = std.array_list.Managed(ast.SelectItem.Rename).init(self.arena);
    var using_hidden = std.array_list.Managed([]const u8).init(self.arena);
    try aliases.usingCols(&using_drop, &using_copies, &using_hidden);
    try using_hidden.appendSlice(corr_hidden.items);
    const copy_names = using_hidden.items;
    for (raw_items.items) |*ri| if (ri.* == .item and ri.item == .field) {
        const q = ri.item.field;
        if (!q.dollar and q.parts.len == 2 and aliases.usingCopy(q.parts[0], q.parts[1]) != null)
            ri.* = .{ .item = .{ .computed = .{ .name = q.last(), .expr = try self.mk(.{ .field = q }) } } };
    };
    for (raw_items.items) |ri| {
        switch (ri) {
            .win => |wi| try post.append(.{ .computed = .{ .name = win_calls.items[wi].out.?, .expr = try self.mk(.{ .field = try self.singleName(win_calls.items[wi].hidden) }) } }),
            .qstar => |alias| {
                if (!aliases.knows(alias))
                    return self.fail(pos, "unknown alias `{s}` in `{s}.*`", .{ alias, alias });
                const rights = try aliases.rights(self.arena);
                if (rights.len == 0) {
                    try items.append(.star);
                    try post.append(.star);
                    star_count += 1;
                } else {
                    const so: ast.SelectItem = .{ .star_of = .{ .rel = if (aliases.strips(alias)) "" else alias, .rights = rights, .drop = using_drop.items, .copies = using_copies.items } };
                    try items.append(if (win_calls.items.len > 0) .star else so);
                    try post.append(so);
                }
            },
            .item => |it| switch (it) {
                .star => {
                    const all: ast.SelectItem = if (copy_names.len > 0) .{ .star_except = copy_names } else .star;
                    try items.append(all);
                    try post.append(all);
                    star_count += 1;
                },
                .star_except => |ex| {
                    const all: ast.SelectItem = .{ .star_except = try std.mem.concat(self.arena, []const u8, &.{ ex, copy_names }) };
                    try items.append(all);
                    try post.append(all);
                },
                .star_rename, .star_of => {
                    if (it == .star_rename and copy_names.len > 0) {
                        const drop = try self.arena.dupe(ast.SelectItem, &.{.{ .star_except = copy_names }});
                        try stages.append(.{ .node = .{ .select = drop }, .hints = &.{}, .pos = pos });
                    }
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
                    if (Window.refsWindow(stripped, win_calls.items)) {
                        try win_post.append(post.items.len);
                        try post.append(.{ .computed = .{ .name = c.name, .expr = stripped } });
                        continue;
                    }
                    if (stripped.* == .call) {
                        if (aggregates.lookup(stripped.call.name)) |f| {
                            const arg: ?*ast.Expr = if (stripped.call.args.len > 0) stripped.call.args[0] else null;
                            try aggs.append(.{ .name = c.name, .func = f, .arg = arg, .distinct = stripped.call.distinct });
                            try post.append(.{ .field = try self.singleName(c.name) });
                            continue;
                        }
                    }
                    if (containsAgg(stripped)) {
                        const keyed = if (group.len > 0) try self.stripExpr(try self.groupKeyRefs(c.expr, group, expr_aliases.items), &aliases) else stripped;
                        const outer = try self.liftAggs(keyed, &aggs, expr_aliases.items, pos);
                        try self.groupedOnly(outer, group, aggs.items, &.{}, pos);
                        try post.append(.{ .computed = .{ .name = c.name, .expr = outer } });
                        lifted = true;
                        continue;
                    }
                    if (group.len > 0 and stripped.* == .field and isGroupKey(group, stripped.field)) {
                        try post.append(.{ .computed = .{ .name = c.name, .expr = stripped } });
                        lifted = true;
                        continue;
                    }
                    if (group.len > 0 and (!isGroupKey(group, try self.singleName(c.name)) or ast.nameIn(input_named.items, c.name)) and !self.constItemExpr(stripped)) {
                        const keyed = try self.stripExpr(try self.groupKeyRefs(c.expr, group, expr_aliases.items), &aliases);
                        try self.groupedOnly(keyed, group, aggs.items, &.{}, pos);
                        try post.append(.{ .computed = .{ .name = c.name, .expr = keyed } });
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

    for (aggs.items) |a| if (a.arg) |arg| if (Window.refsWindow(arg, win_calls.items))
        return self.fail(pos, "an aggregate cannot take a window function's result — compute the window in a derived table or CTE first", .{});
    var agg_mode = aggs.items.len > 0 or group.len > 0;
    if (!agg_mode) {
        for (win_post.items) |at| {
            if (containsAgg(post.items[at].computed.expr)) agg_mode = true;
        }
        for (win_calls.items, specs) |c, sp| {
            if (c.arg) |a| if (containsAgg(a)) {
                agg_mode = true;
            };
            for (sp.partition) |e| if (containsAgg(e)) {
                agg_mode = true;
            };
            for (sp.order) |k| if (containsAgg(k.e)) {
                agg_mode = true;
            };
        }
    }
    if (agg_mode and win_calls.items.len > 0) {
        const L = struct {
            fn lift(p: *Parser, e: *ast.Expr, a: *std.array_list.Managed(ast.AggItem), m: []const ExprAlias, at: Pos) Error!*ast.Expr {
                return if (containsAgg(e)) p.liftAggs(e, a, m, at) else e;
            }
        };
        const ea = expr_aliases.items;
        for (win_post.items) |at| {
            const it = &post.items[at].computed;
            it.expr = try L.lift(self, try self.groupKeyRefs(it.expr, group, ea), &aggs, ea, pos);
            lifted = true;
        }
        for (win_calls.items, specs) |*c, *sp| {
            if (c.arg) |a| c.arg = try L.lift(self, try self.groupKeyRefs(a, group, ea), &aggs, ea, pos);
            const part = try self.arena.dupe(*ast.Expr, sp.partition);
            for (part) |*e| e.* = try L.lift(self, try self.groupKeyRefs(e.*, group, ea), &aggs, ea, pos);
            const ord = try self.arena.dupe(Window.OrderExpr, sp.order);
            for (ord) |*k| k.e = try L.lift(self, try self.groupKeyRefs(k.e, group, ea), &aggs, ea, pos);
            sp.partition = part;
            sp.order = ord;
        }
        for (aggs.items) |a| if (a.arg) |arg| if (Window.refsWindow(arg, win_calls.items))
            return self.fail(pos, "an aggregate cannot take a window function's result — compute the window in a derived table or CTE first", .{});
        for (win_post.items) |at| try self.groupedOnly(post.items[at].computed.expr, group, aggs.items, win_calls.items, pos);
        for (win_calls.items, specs) |c, sp| {
            if (c.arg) |a| try self.groupedOnly(a, group, aggs.items, win_calls.items, pos);
            for (sp.partition) |e| try self.groupedOnly(e, group, aggs.items, win_calls.items, pos);
            for (sp.order) |k| try self.groupedOnly(k.e, group, aggs.items, win_calls.items, pos);
        }
    }

    var having_filter: ?*ast.Expr = null;
    if (agg_mode) {
        if (having) |h| {
            const before = aggs.items.len;
            const keyed = if (group.len > 0) try self.groupKeyRefs(h, group, expr_aliases.items) else h;
            having_filter = try self.liftAggs(try self.stripExpr(keyed, &aliases), &aggs, expr_aliases.items, pos);
            if (aggs.items.len > before) lifted = true;
        }
        if (aggs.items.len == 0 and win_calls.items.len == 0)
            return self.fail(pos, "GROUP BY without aggregate functions in SELECT or HAVING", .{});
        for (aggs.items) |a| {
            if (a.distinct and a.func != .count)
                return self.fail(pos, "DISTINCT is only supported inside COUNT", .{});
        }

        try items.appendSlice(group_hidden.items);
        var needs_pre = false;
        for (group) |k| {
            for (items.items) |it| {
                if (it == .computed and k.parts.len == 1 and std.mem.eql(u8, it.computed.name, k.parts[0]))
                    needs_pre = true;
            }
        }
        var kept = std.array_list.Managed(ast.SelectItem).init(self.arena);
        for (items.items) |it| {
            switch (it) {
                .star, .star_except, .star_rename, .star_of => if (needs_pre)
                    return self.fail(pos, "`*` cannot be combined with a computed GROUP BY key", .{}),
                .computed => |c| {
                    if (isGroupKey(group, try self.singleName(c.name))) {
                        try kept.append(it);
                        continue;
                    }
                    if (self.constItemExpr(c.expr)) {
                        try self.repointPostItem(&post, c.name, c.expr);
                        lifted = true;
                        continue;
                    }
                },
                .field => |f| if (isGroupKey(group, f)) {
                    try kept.append(it);
                    continue;
                },
            }
            return self.fail(itemPos(it) orelse pos, "`{s}` is neither an aggregate nor a grouping key — wrap it in an aggregate, or name it in GROUP BY", .{itemLabel(it)});
        }
        if (needs_pre) {
            var needed = std.array_list.Managed(ast.QualName).init(self.arena);
            for (aggs.items) |a| {
                if (a.arg) |arg| try self.collectFields(arg, &needed);
            }
            try needed.appendSlice(group);
            for (needed.items) |q0| {
                const q = stripQual(q0, &aliases);
                if (q.parts.len != 1) continue;
                const have = for (kept.items) |it| {
                    if (std.mem.eql(u8, itemLabel(it), q.last())) break true;
                } else false;
                if (!have) try kept.append(.{ .field = q });
            }
            try stages.append(.{ .node = .{ .select = try kept.toOwnedSlice() }, .hints = &.{}, .pos = pos });
        }
        const aggs_out = try aggs.toOwnedSlice();
        try stages.append(.{
            .node = .{ .aggregate = .{ .aggs = aggs_out, .by = group } },
            .hints = &.{},
            .pos = pos,
        });
        if (having_filter) |hf| {
            try self.groupedOnly(hf, group, aggs_out, &.{}, pos);
            try stages.append(.{ .node = .{ .filter = hf }, .hints = &.{}, .pos = pos });
        }
        if (!lifted and aggOrderDiffers(post.items, group, aggs_out)) lifted = true;
        if (lifted and win_calls.items.len == 0) {
            for (post.items) |it| {
                if (it == .star or it == .star_except or it == .star_rename)
                    return self.fail(pos, "`*` cannot be combined with an aggregate inside an expression", .{});
            }
            try stages.append(.{ .node = .{ .select = try post.toOwnedSlice() }, .hints = &.{}, .pos = pos });
        }
    } else if (win_calls.items.len == 0) {
        const lone_star = items.items.len == 1 and items.items[0] == .star;
        if (!lone_star) {
            if (distinct_on) |on| distinct_drop = try distinctOnOutputs(self, &items, on, on_stripped);
            try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            if (distinct_on) |on| if (distinct_drop == null) {
                const ks = try self.arena.alloc(ast.SortKey, on.len);
                for (on, ks) |k, *sk| sk.* = .{ .field = k, .desc = false, .nulls_first = false };
                distinct_drop = try self.keepSortedCopies(&stages.items[stages.items.len - 1], ks);
            };
        }
    }

    if (distinct and win_calls.items.len == 0) {
        try stages.append(.{ .node = .{ .distinct = .{ .on = distinct_on } }, .hints = &.{}, .pos = pos });
        if (distinct_drop) |outs| try stages.append(.{ .node = .{ .select = outs }, .hints = &.{}, .pos = pos });
    }

    if (win_calls.items.len > 0) {
        var low = Window.Lower.init(self, win_calls.items);
        if (!agg_mode) for (win_post.items) |at| {
            const it = &post.items[at].computed;
            it.expr = try low.carryExpr(it.expr);
        };
        const wstages = try low.stages(specs, pos);
        if (agg_mode) {
            if (low.pre.items.len > 0) {
                try low.pre.insert(0, .star);
                try stages.append(.{ .node = .{ .select = try low.pre.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
        } else {
            for (low.carries.items) |q| {
                if (!providesName(items.items, q.parts[0])) try items.append(.{ .field = q });
            }
            try items.appendSlice(low.pre.items);
            if (items.items.len > 0) {
                try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
            }
        }
        try stages.appendSlice(wstages);
        var drop = std.array_list.Managed([]const u8).init(self.arena);
        try drop.appendSlice(low.hidden.items);
        for (win_calls.items) |c| if (c.out) |o| try drop.append(o);
        var out_items = std.array_list.Managed(ast.SelectItem).init(self.arena);
        for (post.items) |it| try out_items.append(switch (it) {
            .star => .{ .star_except = drop.items },
            .star_of => |so| .{ .star_of = .{ .rel = so.rel, .rights = so.rights, .drop = try std.mem.concat(self.arena, []const u8, &.{ so.drop, drop.items }), .copies = so.copies } },
            .star_except => |ex| .{ .star_except = try std.mem.concat(self.arena, []const u8, &.{ ex, drop.items }) },
            else => it,
        });
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

    try self.settleQuals(quals.items, &aliases, outer_quals);
    try self.noteOwnCols(stages.items, quals.items, &aliases);
    return .{
        .stages = try stages.toOwnedSlice(),
        .aliases = aliases,
        .union_branch = union_branch,
        .expr_aliases = try expr_aliases.toOwnedSlice(),
        .out_keys = out_keys,
        .group = if (agg_mode and win_calls.items.len == 0) group else null,
        .dups = try dups.toOwnedSlice(),
        .final_names = final_names,
        .where_end = where_end,
    };
}

const KeySide = enum { left, right, loose };

/// The side an ON key column names by its qualifier: the joined table's (`rname.x`),
/// any other table's, or none written.
fn keySide(q: ast.QualName, rname: []const u8) KeySide {
    if (q.parts.len == 1) return .loose;
    return if (qualHasPrefix(q, rname)) .right else .left;
}

/// Whether a projection already yields a column called `name`, so a window's
/// reference to it need not be carried.
pub fn providesName(items: []const ast.SelectItem, name: []const u8) bool {
    for (items) |it| switch (it) {
        .star, .star_of => return true,
        .star_except => |ex| {
            for (ex) |x| {
                if (std.ascii.eqlIgnoreCase(x, name)) break;
            } else return true;
        },
        .star_rename => |rs| {
            for (rs) |r| {
                if (std.ascii.eqlIgnoreCase(r.from, name)) break;
            } else return true;
        },
        .field => |q| if (std.ascii.eqlIgnoreCase(q.last(), name)) return true,
        .computed => |c| if (std.ascii.eqlIgnoreCase(c.name, name)) return true,
    };
    return false;
}

/// A union arm that is not a bare source, lowered to a binding and read back: the
/// union aligns arms by their schemas.
pub fn unionBranchFromStages(self: *Parser, branch_stages: []const ast.Stage) Error!ast.UnionBranch {
    const name = try self.freshName("__union{d}");
    try self.bindHidden(name, branch_stages, self.curPos());
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
    if (by_name and first.dups.len > 0)
        return self.fail(pos, "UNION BY NAME lines columns up by name, and two SELECT items are named `{s}` — give one a different alias", .{first.dups[0].name});
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

/// `USING (a, b)`: the right side's columns of those names are renamed apart, the
/// join keys on them, and a select after the join lists each name once, first,
/// as SQL does — the left value, or under a right or full join the first non-null
/// of the two. Each side's own copy stays, the left one renamed apart too, for
/// `t.k` and `t.*` to name.
pub fn parseUsing(
    self: *Parser,
    kind: ast.JoinKind,
    binding: []const u8,
    jpos: Pos,
    left_keys: *std.array_list.Managed(ast.QualName),
    right_keys: *std.array_list.Managed(ast.QualName),
) Error!struct { binding: []const u8, select: []const ast.SelectItem, keys: []const AliasSet.UsingKey } {
    _ = try self.expect(.lparen);
    var renames = std.array_list.Managed(ast.SelectItem.Rename).init(self.arena);
    var left_renames = std.array_list.Managed(ast.SelectItem.Rename).init(self.arena);
    var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
    var keys = std.array_list.Managed(AliasSet.UsingKey).init(self.arena);
    while (true) {
        const k = try self.expectIdent();
        if (self.col_uses) |u| try u.bareUse(self.arena, try self.singleName(k));
        const rk = try self.freshName("__uk{d}");
        const lk = try std.fmt.allocPrint(self.arena, "__ul{d}", .{self.derived_n});
        try renames.append(.{ .from = k, .to = rk });
        try left_renames.append(.{ .from = k, .to = lk });
        try left_keys.append(try self.singleName(k));
        try right_keys.append(try self.singleName(rk));
        try keys.append(.{ .key = k, .left = (try self.singleName(lk)).parts, .right = (try self.singleName(rk)).parts });
        const left = try self.mk(.{ .field = try self.singleName(k) });
        const value = if (kind == .right or kind == .full) blk: {
            const args = try self.arena.alloc(*ast.Expr, 2);
            args[0] = left;
            args[1] = try self.mk(.{ .field = try self.singleName(rk) });
            break :blk try self.mk(.{ .call = .{ .name = "coalesce", .args = args } });
        } else left;
        try items.append(.{ .computed = .{ .name = k, .expr = value } });
        if (!self.eat(.comma)) break;
    }
    _ = try self.expect(.rparen);
    try items.append(.{ .star_rename = try left_renames.toOwnedSlice() });

    const name = try self.freshName("__derived{d}_using");
    const st = try self.arena.alloc(ast.Stage, 2);
    st[0] = .{ .node = .{ .ref = binding }, .hints = &.{}, .pos = jpos };
    st[1] = .{ .node = .{ .select = try self.arena.dupe(ast.SelectItem, &.{.{ .star_rename = try renames.toOwnedSlice() }}) }, .hints = &.{}, .pos = jpos };
    try self.bindHidden(name, st, jpos);
    return .{ .binding = name, .select = try items.toOwnedSlice(), .keys = try keys.toOwnedSlice() };
}
