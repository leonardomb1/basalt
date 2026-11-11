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
const msgfmt = @import("../../format/msgfmt.zig");
const decimal = @import("../../exec/decimal.zig");
const textconv = @import("../../exec/textconv.zig");
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

    var change_items: ?[]const ast.SelectItem = null;
    if (self.eatKw("using")) try self.parseLoadUsing(&write, &change_items);

    var whints = std.array_list.Managed(ast.Hint).init(self.arena);
    while (true) {
        if (write.form == null and self.eatKw("using")) {
            try self.parseLoadUsing(&write, &change_items);
        } else if (self.eatKw("append")) {
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
    if (change_items) |items| try self.changeStages(&stages, &write, items, pos);
    try stages.append(.{ .node = .{ .write = write }, .hints = try whints.toOwnedSlice(), .pos = pos });
    _ = try self.expect(.semi);
    try out.append(.{ .output = .{ .stages = try stages.toOwnedSlice(), .pos = pos } });
}

/// `USING <name> [ ( key = expr, ... ) ]`, after `USING`. A load path takes no
/// arguments; `changes` takes `sequence_by`, `position` (both required),
/// `delete_when` and `name` (a string), which become the select stage `items`
/// that computes the hidden change columns ahead of the write.
pub fn parseLoadUsing(self: *Parser, write: *ast.Write, items: *?[]const ast.SelectItem) Error!void {
    const pos = self.curPos();
    const name = try self.expectIdent();
    write.form = name;
    if (std.mem.eql(u8, name, "stream_load"))
        try self.notes.append(self.arena, .{ .pos = pos, .msg = "`USING stream_load` is deprecated and will be removed: a starrocks or doris table always loads by Stream Load, so leave it out; `USING` names how a load applies, as `USING changes (...)`" });
    if (!self.at(.lparen)) {
        if (std.mem.eql(u8, name, "changes")) return self.fail(pos, "`USING changes` takes its arguments: `USING changes (sequence_by = <col>, position = <col>[, delete_when = <condition>])`", .{});
        return;
    }
    if (!std.mem.eql(u8, name, "changes")) return self.fail(pos, "`USING {s}` takes no arguments; only `USING changes (...)` does", .{name});
    _ = self.advance();
    var seq: ?*ast.Expr = null;
    var del: ?*ast.Expr = null;
    var at: ?*ast.Expr = null;
    var reads = std.array_list.Managed(ast.QualName).init(self.arena);
    while (!self.at(.rparen)) {
        const kpos = self.curPos();
        const key = try self.expectIdent();
        _ = try self.expect(.assign);
        if (std.mem.eql(u8, key, "name"))
            return self.fail(kpos, "`USING changes` takes no `name`: the table keeps no position of its own, `$since` is the largest `position` among its rows", .{});
        const slot: *?*ast.Expr = if (std.mem.eql(u8, key, "sequence_by")) &seq else if (std.mem.eql(u8, key, "delete_when")) &del else if (std.mem.eql(u8, key, "position")) &at else return self.fail(kpos, "unknown `USING changes` argument `{s}` (it takes sequence_by, position and delete_when)", .{key});
        if (slot.* != null) return self.fail(kpos, "`USING changes` names `{s}` twice", .{key});
        slot.* = try self.parseExpr();
        try self.collectFields(slot.*.?, &reads);
        if (!self.eat(.comma)) break;
    }
    _ = try self.expect(.rparen);
    const s = seq orelse return self.fail(pos, "`USING changes` needs `sequence_by`, which orders a key's changes", .{});
    const p = at orelse return self.fail(pos, "`USING changes` needs `position`, the column whose largest value in the table is where the next run reads from", .{});
    if (p.* != .field or p.field.parts.len != 1 or p.field.dollar)
        return self.fail(pos, "`position` in `USING changes` is a column, kept in the table so the next run reads past its largest value; name the expression in the SELECT (`..., <expr> AS lsn`) and give `position = lsn`", .{});
    const kept = p.field.parts[0];
    var drop = std.array_list.Managed([]const u8).init(self.arena);
    for (reads.items) |q| if (!q.dollar and q.parts.len == 1) if (!ast.nameIn(drop.items, q.parts[0]) and !std.mem.eql(u8, q.parts[0], kept)) try drop.append(q.parts[0]);
    write.changes = .{ .position = kept, .drop = try drop.toOwnedSlice(), .pos = pos };
    const false_lit = try self.mk(.{ .bool_lit = false });
    items.* = try self.arena.dupe(ast.SelectItem, &.{
        .star,
        .{ .computed = .{ .name = ast.Changes.seq_col, .expr = try self.changeNeed(s, "sequence_by", write.target) } },
        .{ .computed = .{ .name = ast.Changes.del_col, .expr = try self.changeNeed(del orelse false_lit, "delete_when", write.target) } },
        .{ .computed = .{ .name = ast.Changes.pos_col, .expr = try self.changeNeed(p, "position", write.target) } },
    });
}

/// The column `UNNEST` takes: `col`, or `t.col` qualified by its table's name or alias.
pub fn unnestCol(self: *Parser) Error!ast.QualName {
    const first = try self.expectIdent();
    if (!self.eat(.dot)) return ast.QualName.one(self.arena, first);
    return .{ .parts = try self.arena.dupe([]const u8, &.{ first, try self.expectIdent() }) };
}

/// The WHERE filters from `from` on, each conjunct that names no column an UNNEST
/// makes moved below the UNNESTs in front of it, which give the same rows either
/// way: below them a computed list (`UNNEST(text_chunks(body, ...))`) is built only for
/// the rows the filter keeps, not for every row and then mostly dropped.
fn sinkPastUnnests(self: *Parser, stages: *std.array_list.Managed(ast.Stage), from: usize) Error!void {
    var i = from;
    while (i < stages.items.len) : (i += 1) {
        if (stages.items[i].node != .filter) continue;
        var made = std.array_list.Managed([]const u8).init(self.arena);
        var b = i;
        var unnests = false;
        while (b > 0) : (b -= 1) {
            const st = stages.items[b - 1].node;
            switch (st) {
                .explode => |ex| {
                    try made.append(ex.field);
                    if (ex.as_name) |n| try made.append(n);
                    unnests = true;
                },
                .select => |items| {
                    if (items.len != 2 or items[0] != .star or items[1] != .computed or !std.mem.startsWith(u8, items[1].computed.name, "\x00unnest")) break;
                    try made.append(items[1].computed.name);
                },
                else => break,
            }
        }
        if (!unnests) continue;
        var conj = std.array_list.Managed(*ast.Expr).init(self.arena);
        try splitAnd(stages.items[i].node.filter, &conj);
        var down = std.array_list.Managed(*ast.Expr).init(self.arena);
        var stay = std.array_list.Managed(*ast.Expr).init(self.arena);
        for (conj.items) |c| try (if (namesAny(c, made.items)) &stay else &down).append(c);
        const moved = (try self.andAll(down.items)) orelse continue;
        const pos = stages.items[i].pos;
        const rest = try self.andAll(stay.items);
        if (rest) |r| {
            stages.items[i].node = .{ .filter = r };
        } else _ = stages.orderedRemove(i);
        try stages.insert(b, .{ .node = .{ .filter = moved }, .hints = &.{}, .pos = pos });
        if (rest != null) i += 1;
    }
}

/// Whether `e` reads a column of `names`, bare or as a struct's head (`c.text`).
fn namesAny(e: *const ast.Expr, names: []const []const u8) bool {
    const S = struct {
        fn hit(ns: []const []const u8, n: *const ast.Expr) ?bool {
            if (n.* != .field) return null;
            for (ns) |name| if (std.ascii.eqlIgnoreCase(n.field.parts[0], name)) return true;
            return false;
        }
    };
    return ast.anyExpr(e, names, S.hit);
}

/// `e` as `__change_need(e, what, target)`: a NULL `sequence_by` or `position`
/// fails the run, a NULL `delete_when` is false.
pub fn changeNeed(self: *Parser, e: *ast.Expr, what: []const u8, target: []const u8) Error!*ast.Expr {
    const args = try self.arena.dupe(*ast.Expr, &.{ e, try self.mk(.{ .str_lit = what }), try self.mk(.{ .str_lit = target }) });
    return self.mk(.{ .call = .{ .name = ast.Changes.need_fn, .args = args } });
}

/// The stages that resolve a change feed ahead of its write, whatever the target:
/// `items` computes the hidden change columns; a window ranks each key's changes by
/// `sequence_by`, latest first; a filter keeps the latest, failing on two at one
/// sequence; and a select drops the rank, the sequence and the columns only the
/// `USING changes` arguments read, leaving the target an ordinary UPSERT of the
/// latest change per key, its delete as `_deleted`. Its `position` column is the
/// largest of all the key's changes, not the latest's alone, so the largest in the
/// table is the largest the query read, and the next run reads past every change
/// this one did. Without
/// `UPSERT ON (...)` keys there is nothing to rank by, so only `items`, for
/// analysis to refuse the write with the reason. Under `PARTIAL COLS` a matched
/// row takes the position and `_deleted` too, or the table would not move on.
pub fn changeStages(self: *Parser, stages: *std.array_list.Managed(ast.Stage), write: *ast.Write, items: []const ast.SelectItem, pos: ast.Pos) Error!void {
    if (write.mode != .upsert or write.mode.upsert.keys.len == 0)
        return stages.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = pos });
    const keys = write.mode.upsert.keys;
    if (write.mode.upsert.partial) |cols| {
        var with = std.array_list.Managed([]const u8).init(self.arena);
        try with.appendSlice(cols);
        for ([_][]const u8{ write.changes.?.position, ast.Changes.deleted_col }) |c| if (!ast.nameIn(with.items, c)) try with.append(c);
        write.mode.upsert.partial = try with.toOwnedSlice();
    }
    const field = struct {
        fn of(p: *Parser, n: []const u8) Error!*ast.Expr {
            return p.mk(.{ .field = .{ .parts = try p.arena.dupe([]const u8, &.{n}) } });
        }
    }.of;
    const seq = ast.QualName{ .parts = try self.arena.dupe([]const u8, &.{ast.Changes.seq_col}) };
    const part = try self.arena.alloc(ast.QualName, keys.len);
    for (part, keys) |*q, k| q.* = .{ .parts = try self.arena.dupe([]const u8, &.{k}) };
    const whole = ast.WinFrame{ .unit = .rows, .start = .unbounded_preceding, .end = .unbounded_following };
    const funcs = try self.arena.dupe(ast.WindowFunc, &.{
        .{ .func = .{ .win = .row_number }, .out = ast.Changes.rank_col },
        .{ .func = .{ .win = .lead }, .out = ast.Changes.next_col, .arg = seq },
        .{ .func = .{ .agg = .max }, .out = ast.Changes.top_col, .arg = .{ .parts = try self.arena.dupe([]const u8, &.{ast.Changes.pos_col}) }, .frame = whole },
    });
    const order = try self.arena.dupe(ast.SortKey, &.{.{ .field = seq, .desc = true, .nulls_first = false }});
    const args = try self.arena.alloc(*ast.Expr, 4 + keys.len);
    args[0] = try field(self, ast.Changes.rank_col);
    args[1] = try field(self, ast.Changes.seq_col);
    args[2] = try field(self, ast.Changes.next_col);
    args[3] = try self.mk(.{ .str_lit = try std.mem.join(self.arena, ", ", keys) });
    for (args[4..], keys) |*a, k| a.* = try field(self, k);
    var drop = std.array_list.Managed([]const u8).init(self.arena);
    const ch = write.changes.?;
    try drop.appendSlice(&.{ ast.Changes.seq_col, ast.Changes.del_col, ast.Changes.rank_col, ast.Changes.next_col, ast.Changes.pos_col, ast.Changes.top_col, ch.position, ast.Changes.deleted_col });
    for (ch.drop) |d| if (!ast.nameIn(keys, d)) try drop.append(d);
    try stages.appendSlice(&.{
        .{ .node = .{ .select = items }, .hints = &.{}, .pos = pos },
        .{ .node = .{ .window = .{ .funcs = funcs, .partition_by = part, .order_by = order } }, .hints = &.{}, .pos = pos },
        .{ .node = .{ .filter = try self.mk(.{ .call = .{ .name = ast.Changes.last_fn, .args = args } }) }, .hints = &.{}, .pos = pos },
        .{ .node = .{ .select = try self.arena.dupe(ast.SelectItem, &.{
            .{ .star_except = try drop.toOwnedSlice() },
            .{ .computed = .{ .name = ch.position, .expr = try field(self, ast.Changes.top_col) } },
            .{ .computed = .{ .name = ast.Changes.deleted_col, .expr = try field(self, ast.Changes.del_col) } },
        }) }, .hints = &.{}, .pos = pos },
    });
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
            } else if (self.at(.dollar_ident)) {
                val = .{ .ident = try std.mem.concat(self.arena, u8, &.{ "$", self.advance().text }) };
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
                } else if (e.* == .field and !structish(e.field, &first.aliases)) {
                    const f = e.field;
                    if (f.parts.len == 1 and first.out_keys != null and namesOutput(first.out_keys, f.parts[0]))
                        if (self.col_uses) |u| u.outputUse(f);
                    if (f.parts.len == 1) try unambiguous(self, first.dups, f.parts[0], "ORDER BY", kpos);
                    q = try distinctSortKey(self, stages.items, f, stripQual(f, &first.aliases), kpos);
                    was_qual = q.parts.len < f.parts.len;
                    if (f.parts.len == 1 and namesOutput(first.out_keys, f.parts[0])) q.sides = &.{};
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
    if (try parseLimit(self)) |lim| {
        if (lim.with_ties) try withTies(self, stages, lim, lpos) else try stages.append(.{ .node = .{ .limit = lim }, .hints = &.{}, .pos = lpos });
    }

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
/// Whether a dotted name can only be a struct column's field: its first part names
/// no table of the FROM, or it has more parts than a table's column takes. Such a
/// key is computed, as an expression key is, since no column has its name.
fn structish(q: ast.QualName, aliases: *const AliasSet) bool {
    if (q.dollar) return false;
    return q.parts.len > 2 or (q.parts.len == 2 and !aliases.knows(q.parts[0]));
}

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
                if (self.isKw("with") and self.i + 1 < self.toks.len and eqlNoCase(self.toks[self.i + 1].text, "ties")) {
                    _ = self.advance();
                    _ = self.advance();
                    lim.with_ties = true;
                }
                continue;
            }
            _ = self.advance();
            if (!self.eatKw("first") and !self.eatKw("next"))
                return self.fail(self.curPos(), "expected `FIRST` or `NEXT` after FETCH", .{});
            if (self.isKw("row") or self.isKw("rows")) lim.count = 1 else try limitCount(self, "FETCH FIRST", &lim.count, &lim.count_expr);
            if (!self.eatKw("row") and !self.eatKw("rows"))
                return self.fail(self.curPos(), "expected `ROWS ONLY` after FETCH FIRST n", .{});
            if (self.eatKw("with")) {
                try self.expectKw("ties");
                lim.with_ties = true;
            } else try self.expectKw("only");
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
    if (base.* == .float_lit) {
        const f = if (neg) -base.float_lit else base.float_lit;
        const r = textconv.floatToInt(f, .half_even);
        if (if (r) |x| x < 0 else f < 0) return self.fail(vpos, "{s} must not be negative, got {d}", .{ what, f });
        n.* = @intCast(r orelse return self.fail(vpos, "{s} {d} is out of range", .{ what, f }));
        return;
    }
    if (base.* == .dec_lit) {
        const d = base.dec_lit;
        const q = decimal.roundWhole(.{ .unscaled = d.unscaled, .scale = d.scale }) orelse return self.fail(vpos, "{s} is out of range", .{what});
        var r: f64 = @floatFromInt(q);
        var shown = @as(f64, @floatFromInt(d.unscaled)) / @as(f64, @floatFromInt(decimal.pow10_i128[d.scale]));
        if (neg) {
            r = -r;
            shown = -shown;
        }
        if (r < 0) return self.fail(vpos, "{s} must not be negative, got {d}", .{ what, shown });
        n.* = @intCast(textconv.floatToInt(@abs(r), .exact) orelse return self.fail(vpos, "{s} {d} is out of range", .{ what, shown }));
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
        if (self.isKw("pushdown")) {
            try self.notePushdown();
            _ = self.advance();
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
            var col: ast.QualName = undefined;
            var delim: ?[]const u8 = null;
            var json = false;
            if (self.isKw("json_each") and self.peekTag() == .lparen) {
                _ = self.advance();
                _ = try self.expect(.lparen);
                col = try self.unnestCol();
                _ = try self.expect(.rparen);
                json = true;
            } else if (self.isKw("split") and self.peekTag() == .lparen) {
                _ = self.advance();
                _ = try self.expect(.lparen);
                col = try self.unnestCol();
                _ = try self.expect(.comma);
                const dpos = self.curPos();
                const dt = try self.expect(.string);
                if (dt.text.len == 0) return self.fail(dpos, "SPLIT needs a non-empty delimiter", .{});
                delim = dt.text;
                _ = try self.expect(.rparen);
            } else if (self.at(.ident) and self.peekTag() == .lparen) {
                const e = try self.stripExpr(try self.parseExpr(), &aliases);
                const hidden = try std.fmt.allocPrint(self.arena, "\x00unnest{d}", .{stages.items.len});
                const items = try self.arena.dupe(ast.SelectItem, &.{ .star, .{ .computed = .{ .name = hidden, .expr = e } } });
                try stages.append(.{ .node = .{ .select = items }, .hints = &.{}, .pos = jpos });
                _ = try self.expect(.rparen);
                const as_name = if (self.eatKw("as")) try self.expectIdent() else "unnest";
                try stages.append(.{ .node = .{ .explode = .{ .field = hidden, .as_name = as_name } }, .hints = &.{}, .pos = jpos });
                continue;
            } else {
                col = try self.unnestCol();
            }
            col = stripQual(col, &aliases);
            if (self.col_uses) |u| try u.bareUse(self.arena, col);
            _ = try self.expect(.rparen);
            var as_name: ?[]const u8 = null;
            if (self.eatKw("as")) as_name = try self.expectIdent();
            try stages.append(.{
                .node = .{ .explode = .{ .field = col.parts[col.parts.len - 1], .rel = if (col.parts.len > 1) col.parts[col.parts.len - 2] else "", .as_name = as_name, .delim = delim, .json = json } },
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
        var residual = std.array_list.Managed(*ast.Expr).init(self.arena);
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
            var narrow_l = std.array_list.Managed(*ast.Expr).init(self.arena);
            for (conj.items) |c| {
                if (ast.hasSearch(c)) {
                    if (kind == .inner) {
                        try post_filters.append(c);
                    } else {
                        const e = try self.stripExpr(c, &aliases);
                        try residual.append(e);
                    }
                    continue;
                }
                if (c.* == .binary and c.binary.op == .eq and c.binary.l.* == .field and c.binary.r.* == .field and
                    !c.binary.l.field.dollar and !c.binary.r.field.dollar and
                    !structish(c.binary.l.field, &aliases) and !structish(c.binary.r.field, &aliases))
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
                        if (le.* == .field and !structish(le.field, &aliases)) {
                            try left_keys.append(stripQual(le.field, &aliases));
                        } else {
                            const k = try self.freshName("__jk{d}");
                            try left_computed.append(.{ .computed = .{ .name = k, .expr = try self.stripExpr(le, &aliases) } });
                            try key_cols.append(k);
                            try left_keys.append(try self.singleName(k));
                        }
                        if (re.* == .field and !structish(re.field, &aliases)) {
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
                    try narrow_l.append(e);
                } else if (kind == .inner) {
                    try post_filters.append(c);
                } else {
                    const e = try self.stripExpr(c, &aliases);
                    try residual.append(e);
                }
            }
            if (left_keys.items.len == 0 and deferred.items.len == 0) {
                for (post_filters.items) |c| {
                    const e = try self.stripExpr(c, &aliases);
                    try residual.append(e);
                }
                post_filters.clearRetainingCapacity();
            }
            const narrow = try self.andAll(narrow_l.items);
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
                .residual = try self.andAll(residual.items),
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

    while (self.isKw("pivot") or self.isKw("unpivot")) {
        if (self.isKw("pivot")) try parsePivot(self, &stages, &aliases) else try parseUnpivot(self, &stages, &aliases);
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
        const where_at = stages.items.len;
        try Correlate.lowerWhere(self, &stages, &aliases, remaining, fpos, sj_base, scope.scalars.items, &corr_hidden);
        try sinkPastUnnests(self, &stages, where_at);
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
    var grouping_sets: ?KeySets = null;
    const group_pos = self.curPos();
    if (self.eatKw("group")) {
        try self.expectKw("by");
        var keys = std.array_list.Managed(ast.QualName).init(self.arena);
        const Keys = struct {
            p: *Parser,
            raw: @TypeOf(&raw_items),
            aliases: *AliasSet,
            dups: []const Parser.DupName,
            hidden: *std.array_list.Managed(ast.SelectItem),
            input_named: *std.array_list.Managed([]const u8),
            expr_aliases: @TypeOf(&expr_aliases),
            keys: *std.array_list.Managed(ast.QualName),

            /// One key expression, its place in `keys`; a key named twice keeps one.
            pub fn parse(k: @This()) Error!usize {
                const self_ = k.p;
                const gpos = self_.curPos();
                const ge = try self_.parseExpr();
                var q: ast.QualName = undefined;
                if (ge.* == .int_lit) {
                    const n = ge.int_lit;
                    if (n < 1 or n > @as(i64, @intCast(k.raw.items.len)))
                        return self_.fail(gpos, "GROUP BY position {d} is out of range", .{n});
                    const ri = k.raw.items[@intCast(n - 1)];
                    if (ri == .win) return self_.fail(gpos, "GROUP BY position {d} is a window function — group on it in an outer query", .{n});
                    if (ri != .item) return self_.fail(gpos, "GROUP BY position {d} refers to `*`", .{n});
                    q = switch (ri.item) {
                        .field => |f| stripQual(f, k.aliases),
                        .computed => |c| blk: {
                            if (containsAgg(c.expr))
                                return self_.fail(gpos, "GROUP BY position {d} names the aggregate `{s}` — a position may only name a grouping key", .{ n, c.name });
                            break :blk try self_.singleName(c.name);
                        },
                        else => return self_.fail(gpos, "GROUP BY position {d} refers to `*`", .{n}),
                    };
                } else if (ge.* == .field and !structish(ge.field, k.aliases)) {
                    if (ge.field.parts.len == 1) {
                        const n = ge.field.parts[0];
                        switch (try groupNameHome(self_, k.raw.items, n, k.aliases)) {
                            .input => {},
                            .input_under_alias => try k.input_named.append(n),
                            .alias => try unambiguous(self_, k.dups, n, "GROUP BY", gpos),
                        }
                    }
                    q = stripQual(ge.field, k.aliases);
                } else {
                    if (containsAgg(ge)) return self_.fail(gpos, "GROUP BY cannot group on an aggregate", .{});
                    const b = try self_.bindName(ge, k.expr_aliases.items);
                    if (!b.item and !providesName(k.hidden.items, b.name)) try k.hidden.append(.{ .computed = .{ .name = b.name, .expr = try self_.stripExpr(ge, k.aliases) } });
                    q = try self_.singleName(b.name);
                }
                for (k.keys.items, 0..) |have, i| if (qualEql(have, q)) return i;
                try k.keys.append(q);
                return k.keys.items.len - 1;
            }
        };
        const key_parser = Keys{ .p = self, .raw = &raw_items, .aliases = &aliases, .dups = dups.items, .hidden = &group_hidden, .input_named = &input_named, .expr_aliases = &expr_aliases, .keys = &keys };
        var plain = std.array_list.Managed(usize).init(self.arena);
        var factors = std.array_list.Managed(KeySets).init(self.arena);
        while (true) {
            if (atGroupingStart(self)) {
                try factors.append(try parseGroupingElement(self, key_parser));
            } else {
                const at = try key_parser.parse();
                if (std.mem.indexOfScalar(usize, plain.items, at) == null) try plain.append(at);
            }
            if (!self.eat(.comma)) break;
        }
        if (factors.items.len > 0) grouping_sets = try crossSets(self, plain.items, factors.items, group_pos);
        group = try keys.toOwnedSlice();
        if (input_named.items.len > 0) {
            var kept_aliases = std.array_list.Managed(ExprAlias).init(self.arena);
            for (expr_aliases.items) |m| if (!ast.nameIn(input_named.items, m.out)) try kept_aliases.append(m);
            expr_aliases = kept_aliases;
        }
    }

    var having: ?*ast.Expr = null;
    if (self.eatKw("having")) having = try self.parseExpr();

    const real_keys = group;
    const grouping_used = try self.arena.alloc(bool, group.len);
    @memset(grouping_used, false);
    if (grouping_sets != null) {
        for (raw_items.items) |*ri| if (ri.* == .item and ri.item == .computed) {
            ri.item.computed.expr = try groupingCalls(self, ri.item.computed.expr, group, grouping_used, group_pos);
        };
        if (having) |h| having = try groupingCalls(self, h, group, grouping_used, group_pos);
        var all = std.array_list.Managed(ast.QualName).init(self.arena);
        try all.appendSlice(group);
        for (group, grouping_used) |k, used| if (used) try all.append(try self.singleName(try groupingName(self, k)));
        group = try all.toOwnedSlice();
    }

    var qualify: ?*ast.Expr = null;
    const qualify_pos = self.curPos();
    self.win_sink = &win_calls;
    if (self.eatKw("qualify")) qualify = try self.parseExpr();
    self.win_sink = null;

    var named = std.array_list.Managed(Parser.NamedWindow).init(self.arena);
    if (self.eatKw("window")) try self.parseWindowClause(&named);
    self.win_sink = &win_calls;
    if (qualify == null and self.eatKw("qualify")) qualify = try self.parseExpr();
    self.win_sink = null;
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
        if (structish(q, &aliases) and !std.mem.eql(u8, q.last(), "*"))
            ri.* = .{ .item = .{ .computed = .{ .name = q.last(), .expr = try self.mk(.{ .field = q }) } } };
    };
    for (raw_items.items) |ri| {
        switch (ri) {
            .win => |wi| try post.append(.{ .computed = .{ .name = win_calls.items[wi].out.?, .expr = try self.mk(.{ .field = try self.singleName(win_calls.items[wi].hidden) }) } }),
            .qstar => |alias| {
                if (!aliases.knows(alias)) {
                    const parts = try self.arena.dupe([]const u8, &.{ alias, "*" });
                    try items.append(if (win_calls.items.len > 0) .star else .{ .field = .{ .parts = parts } });
                    try post.append(.{ .field = .{ .parts = parts } });
                    continue;
                }
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

    if (qualify) |q| {
        const named_out = try qualifyNames(self, q, win_calls.items, post.items);
        qualify = try self.stripExpr(named_out, &aliases);
        if (!Window.refsWindow(qualify.?, win_calls.items))
            return self.fail(qualify_pos, "QUALIFY filters on a window function's result, and this one names none — use WHERE or HAVING", .{});
    }
    for (aggs.items) |a| if (a.arg) |arg| if (Window.refsWindow(arg, win_calls.items))
        return self.fail(pos, "an aggregate cannot take a window function's result — compute the window in a derived table or CTE first", .{});
    var agg_mode = aggs.items.len > 0 or group.len > 0;
    if (!agg_mode) {
        for (win_post.items) |at| {
            if (containsAgg(post.items[at].computed.expr)) agg_mode = true;
        }
        if (qualify) |q| if (containsAgg(q)) {
            agg_mode = true;
        };
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
        if (qualify) |q| {
            qualify = try L.lift(self, try self.groupKeyRefs(q, group, ea), &aggs, ea, pos);
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
        if (qualify) |q| try self.groupedOnly(q, group, aggs.items, win_calls.items, pos);
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
            if (a.distinct and a.func != .count and a.func != .array_agg and a.func != .string_agg)
                return self.fail(pos, "DISTINCT is only supported inside COUNT, ARRAY_AGG and STRING_AGG", .{});
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
                const have = for (kept.items) |it| {
                    if (it == .field and qualEql(it.field, q)) break true;
                    if (q.parts.len == 1 and std.mem.eql(u8, itemLabel(it), q.last())) break true;
                } else false;
                if (!have) try kept.append(.{ .field = q });
            }
            try stages.append(.{ .node = .{ .select = try kept.toOwnedSlice() }, .hints = &.{}, .pos = pos });
        }
        const aggs_out = try aggs.toOwnedSlice();
        var agg_by = group;
        var key_names: []const ast.SelectItem.Rename = &.{};
        var on_empty: []const []const u8 = &.{};
        if (grouping_sets) |sets| {
            const expanded = try expandGroupingSets(self, &stages, real_keys, sets, grouping_used, pos);
            agg_by = expanded.by;
            key_names = expanded.names;
            on_empty = expanded.on_empty;
            lifted = true;
        }
        try stages.append(.{
            .node = .{ .aggregate = .{ .aggs = aggs_out, .by = agg_by, .on_empty = on_empty } },
            .hints = &.{},
            .pos = pos,
        });
        if (grouping_sets != null) {
            const back = try self.arena.alloc(ast.SelectItem, 1);
            back[0] = .{ .star_rename = key_names };
            try stages.append(.{ .node = .{ .select = back }, .hints = &.{}, .pos = pos });
        }
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
        if (!agg_mode) if (qualify) |q| {
            qualify = try low.carryExpr(q);
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
        if (qualify) |q| try stages.append(.{ .node = .{ .filter = q }, .hints = &.{}, .pos = pos });
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

/// `ORDER BY k LIMIT n WITH TIES`: the rows ranked by the sort's keys and those of
/// rank `n` or better kept, then sorted, so a row tied with the last stays.
fn withTies(self: *Parser, stages: *std.array_list.Managed(ast.Stage), lim: ast.Limit, pos: Pos) Error!void {
    const last = stages.items.len - 1;
    if (stages.items.len == 0 or stages.items[last].node != .sort)
        return self.fail(pos, "WITH TIES needs an ORDER BY to say what ties", .{});
    if (lim.offset != 0 or lim.offset_expr != null) return self.fail(pos, "WITH TIES takes no OFFSET", .{});
    const rank_col = "__rk";
    const funcs = try self.arena.alloc(ast.WindowFunc, 1);
    funcs[0] = .{ .func = .{ .win = .rank }, .out = rank_col };
    const sort = stages.items[last];
    const bound = lim.count_expr orelse try self.mk(.{ .int_lit = std.math.cast(i64, lim.count) orelse std.math.maxInt(i64) });
    const keep = try self.mk(.{ .binary = .{ .op = .le, .l = try self.mk(.{ .field = try self.singleName(rank_col) }), .r = bound } });
    stages.items[last] = .{ .node = .{ .window = .{ .funcs = funcs, .order_by = sort.node.sort.keys } }, .hints = &.{}, .pos = pos };
    try stages.append(.{ .node = .{ .filter = keep }, .hints = &.{}, .pos = pos });
    try stages.append(sort);
    const drop = try self.arena.alloc([]const u8, 1);
    drop[0] = rank_col;
    const rest = try self.arena.alloc(ast.SelectItem, 1);
    rest[0] = .{ .star_except = drop };
    try stages.append(.{ .node = .{ .select = rest }, .hints = &.{}, .pos = pos });
}

/// A PIVOT value's column name: what it is written as, or its AS.
fn pivotValueName(self: *Parser, v: *const ast.Expr, pos: Pos) Error![]const u8 {
    return switch (v.*) {
        .str_lit => |s| s,
        .int_lit => |n| std.fmt.allocPrint(self.arena, "{d}", .{n}),
        else => self.fail(pos, "name this PIVOT value with AS", .{}),
    };
}

/// `AS name` after a PIVOT or UNPIVOT: the bare `name.` its output answers to.
fn pivotAlias(self: *Parser, aliases: *AliasSet) Error!void {
    if (!self.eatKw("as")) return;
    const pos = self.curPos();
    const name = try self.expectIdent();
    aliases.put(name, .left) catch return self.fail(pos, "the name `{s}` is already taken in this FROM", .{name});
}

/// `PIVOT (agg(x) [AS a], ... FOR col IN (value [AS name], ...)) [AS p]`: one aggregate
/// per value, over the rows holding it, grouped on every other input column.
fn parsePivot(self: *Parser, stages: *std.array_list.Managed(ast.Stage), aliases: *AliasSet) Error!void {
    const pos = self.curPos();
    try self.expectKw("pivot");
    _ = try self.expect(.lparen);
    const Asked = struct { call: ast.Expr.Call, alias: ?[]const u8 };
    var asked = std.array_list.Managed(Asked).init(self.arena);
    while (true) {
        const apos = self.curPos();
        const e = try self.stripExpr(try self.parseExpr(), aliases);
        if (e.* != .call or aggregates.lookup(e.call.name) == null) return self.fail(apos, "PIVOT takes aggregates before its FOR", .{});
        try asked.append(.{ .call = e.call, .alias = if (self.eatKw("as")) try self.expectIdent() else null });
        if (!self.eat(.comma)) break;
    }
    try self.expectKw("for");
    const col = stripQual(try self.parseColRef(), aliases);
    try self.expectKw("in");
    _ = try self.expect(.lparen);
    var items = std.array_list.Managed(ast.AggItem).init(self.arena);
    while (true) {
        const vpos = self.curPos();
        const v = try self.parseExpr();
        const value_name = if (self.eatKw("as")) try self.expectIdent() else try pivotValueName(self, v, vpos);
        const cond = try self.mk(.{ .binary = .{ .op = .eq, .l = try self.mk(.{ .field = col }), .r = v } });
        for (asked.items) |a| {
            const f = try self.applyFilter(a.call.name, @constCast(a.call.args), cond, vpos);
            const name = if (a.alias) |al|
                try std.fmt.allocPrint(self.arena, "{s}_{s}", .{ value_name, al })
            else if (asked.items.len > 1)
                try std.fmt.allocPrint(self.arena, "{s}_{s}", .{ value_name, a.call.name })
            else
                value_name;
            try items.append(.{ .name = name, .func = aggregates.lookup(f.name).?, .arg = if (f.args.len > 0) f.args[0] else null, .distinct = a.call.distinct });
        }
        if (!self.eat(.comma)) break;
    }
    _ = try self.expect(.rparen);
    _ = try self.expect(.rparen);
    var fields = std.array_list.Managed(ast.QualName).init(self.arena);
    for (asked.items) |a| for (a.call.args) |arg| try self.collectFields(arg, &fields);
    var rest = std.array_list.Managed([]const u8).init(self.arena);
    try rest.append(col.last());
    for (fields.items) |q| if (!q.dollar and !ast.nameIn(rest.items, q.last())) try rest.append(q.last());
    try stages.append(.{ .node = .{ .aggregate = .{ .aggs = try items.toOwnedSlice(), .by = &.{}, .by_rest = try rest.toOwnedSlice() } }, .hints = &.{}, .pos = pos });
    try pivotAlias(self, aliases);
}

/// `UNPIVOT [INCLUDE NULLS | EXCLUDE NULLS] (value FOR name IN (col [AS 'label'], ...))
/// [AS u]`: each input row made one row per column listed, NULLs left out unless asked
/// for, the other columns kept.
fn parseUnpivot(self: *Parser, stages: *std.array_list.Managed(ast.Stage), aliases: *AliasSet) Error!void {
    const pos = self.curPos();
    try self.expectKw("unpivot");
    var keep_nulls = false;
    if (self.eatKw("include")) {
        try self.expectKw("nulls");
        keep_nulls = true;
    } else if (self.eatKw("exclude")) try self.expectKw("nulls");
    _ = try self.expect(.lparen);
    const value = try self.expectIdent();
    try self.expectKw("for");
    const label = try self.expectIdent();
    try self.expectKw("in");
    _ = try self.expect(.lparen);
    const Col = struct { q: ast.QualName, label: []const u8 };
    var cols = std.array_list.Managed(Col).init(self.arena);
    while (true) {
        const q = stripQual(try self.parseColRef(), aliases);
        var name = q.last();
        if (self.eatKw("as")) name = (try self.expect(.string)).text;
        try cols.append(.{ .q = q, .label = name });
        if (!self.eat(.comma)) break;
    }
    _ = try self.expect(.rparen);
    _ = try self.expect(.rparen);

    const hidden = "__up";
    var ids = std.array_list.Managed(u8).init(self.arena);
    for (cols.items, 0..) |_, i| {
        if (i > 0) try ids.append(',');
        try ids.writer().print("{d}", .{i});
    }
    const with_id = try self.arena.alloc(ast.SelectItem, 2);
    with_id[0] = .star;
    with_id[1] = .{ .computed = .{ .name = hidden, .expr = try self.mk(.{ .str_lit = ids.items }) } };
    try stages.append(.{ .node = .{ .select = with_id }, .hints = &.{}, .pos = pos });
    try stages.append(.{ .node = .{ .explode = .{ .field = hidden, .as_name = null, .delim = "," } }, .hints = &.{}, .pos = pos });

    var label_e = try self.mk(.null_lit);
    var value_e = try self.mk(.null_lit);
    var i = cols.items.len;
    while (i > 0) {
        i -= 1;
        const id = try std.fmt.allocPrint(self.arena, "{d}", .{i});
        const at = try self.mk(.{ .binary = .{ .op = .eq, .l = try self.mk(.{ .field = try self.singleName(hidden) }), .r = try self.mk(.{ .str_lit = id }) } });
        label_e = try self.mk(.{ .cond = .{ .cond = at, .then = try self.mk(.{ .str_lit = cols.items[i].label }), .els = label_e } });
        value_e = try self.mk(.{ .cond = .{ .cond = at, .then = try self.mk(.{ .field = cols.items[i].q }), .els = value_e } });
    }
    var dropped = std.array_list.Managed([]const u8).init(self.arena);
    try dropped.append(hidden);
    for (cols.items) |c| try dropped.append(c.q.last());
    const out = try self.arena.alloc(ast.SelectItem, 3);
    out[0] = .{ .star_except = try dropped.toOwnedSlice() };
    out[1] = .{ .computed = .{ .name = label, .expr = label_e } };
    out[2] = .{ .computed = .{ .name = value, .expr = value_e } };
    try stages.append(.{ .node = .{ .select = out }, .hints = &.{}, .pos = pos });
    if (!keep_nulls) {
        const present = try self.mk(.{ .is_null = .{ .e = try self.mk(.{ .field = try self.singleName(value) }), .negated = true } });
        try stages.append(.{ .node = .{ .filter = present }, .hints = &.{}, .pos = pos });
    }
    try pivotAlias(self, aliases);
}

/// Grouping sets as lists of places in the GROUP BY's keys.
const KeySets = []const []const usize;

const set_col = "__gs";
const max_grouping_sets = 4096;

/// Whether the parser stands at `ROLLUP (`, `CUBE (` or `GROUPING SETS (`.
fn atGroupingStart(self: *Parser) bool {
    if ((self.isKw("rollup") or self.isKw("cube")) and self.peekTag() == .lparen) return true;
    return self.isKw("grouping") and self.i + 1 < self.toks.len and eqlNoCase(self.toks[self.i + 1].text, "sets");
}

/// The sets one `ROLLUP (...)`, `CUBE (...)` or `GROUPING SETS (...)` stands for.
pub fn parseGroupingElement(self: *Parser, keys: anytype) Error!KeySets {
    const pos = self.curPos();
    var out = std.array_list.Managed([]const usize).init(self.arena);
    if (self.eatKw("grouping")) {
        try self.expectKw("sets");
        _ = try self.expect(.lparen);
        while (true) {
            if (atGroupingStart(self)) {
                try out.appendSlice(try parseGroupingElement(self, keys));
            } else try out.append(try parseKeyGroup(self, keys));
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
        return out.toOwnedSlice();
    }
    const cube = self.isKw("cube");
    _ = self.advance();
    _ = try self.expect(.lparen);
    var items = std.array_list.Managed([]const usize).init(self.arena);
    while (true) {
        try items.append(try parseKeyGroup(self, keys));
        if (!self.eat(.comma)) break;
    }
    _ = try self.expect(.rparen);
    if (!cube) {
        var n = items.items.len + 1;
        while (n > 0) {
            n -= 1;
            try out.append(try unionOf(self, items.items[0..n]));
        }
        return out.toOwnedSlice();
    }
    if (items.items.len > 12) return self.fail(pos, "CUBE of {d} items makes too many grouping sets — name them in GROUPING SETS", .{items.items.len});
    var mask: usize = (@as(usize, 1) << @intCast(items.items.len));
    while (mask > 0) {
        mask -= 1;
        var some = std.array_list.Managed([]const usize).init(self.arena);
        for (items.items, 0..) |it, i| if (mask & (@as(usize, 1) << @intCast(items.items.len - 1 - i)) != 0) try some.append(it);
        try out.append(try unionOf(self, some.items));
    }
    return out.toOwnedSlice();
}

/// `()`, `(a, b)` or a lone `a`; `(a + 1) * 2` is one key that opens with a parenthesis.
fn parseKeyGroup(self: *Parser, keys: anytype) Error![]const usize {
    var out = std.array_list.Managed(usize).init(self.arena);
    if (self.at(.lparen) and tupleAhead(self)) {
        _ = self.advance();
        if (self.eat(.rparen)) return out.toOwnedSlice();
        while (true) {
            try out.append(try keys.parse());
            if (!self.eat(.comma)) break;
        }
        _ = try self.expect(.rparen);
        return out.toOwnedSlice();
    }
    try out.append(try keys.parse());
    return out.toOwnedSlice();
}

/// Whether the parenthesis here closes right before a `,` or a `)`.
fn tupleAhead(self: *Parser) bool {
    var depth: usize = 0;
    var i = self.i;
    while (i < self.toks.len) : (i += 1) {
        switch (self.toks[i].tag) {
            .lparen => depth += 1,
            .rparen => {
                depth -= 1;
                if (depth == 0) {
                    if (i + 1 >= self.toks.len) return false;
                    const t = self.toks[i + 1].tag;
                    return t == .comma or t == .rparen;
                }
            },
            else => {},
        }
    }
    return false;
}

fn unionOf(self: *Parser, lists: []const []const usize) Error![]const usize {
    var out = std.array_list.Managed(usize).init(self.arena);
    for (lists) |l| for (l) |k| if (std.mem.indexOfScalar(usize, out.items, k) == null) try out.append(k);
    return out.toOwnedSlice();
}

/// Every combination of one set from each of `factors`, the plain keys in all of them.
fn crossSets(self: *Parser, plain: []const usize, factors: []const KeySets, pos: Pos) Error!KeySets {
    var acc = std.array_list.Managed([]const usize).init(self.arena);
    try acc.append(plain);
    for (factors) |f| {
        var next = std.array_list.Managed([]const usize).init(self.arena);
        for (acc.items) |have| for (f) |set| {
            try next.append(try unionOf(self, &.{ have, set }));
            if (next.items.len > max_grouping_sets) return self.fail(pos, "this GROUP BY makes more than {d} grouping sets", .{max_grouping_sets});
        };
        acc = next;
    }
    return acc.toOwnedSlice();
}

fn groupingName(self: *Parser, k: ast.QualName) Error![]const u8 {
    return std.fmt.allocPrint(self.arena, "__gb_{s}", .{k.last()});
}

fn groupedKeyName(self: *Parser, k: ast.QualName) Error![]const u8 {
    return std.fmt.allocPrint(self.arena, "__gk_{s}", .{k.last()});
}

/// `e` with each `GROUPING(a, b)` turned into its hidden columns, a bit each, the last
/// key the lowest, and `used` marking those keys.
fn groupingCalls(self: *Parser, e: *ast.Expr, keys: []const ast.QualName, used: []bool, pos: Pos) Error!*ast.Expr {
    const Cx = struct {
        p: *Parser,
        keys: []const ast.QualName,
        used: []bool,
        pos: Pos,
        fn recur(cx: @This(), n: *const ast.Expr) Error!*ast.Expr {
            if (n.* != .call or !std.mem.eql(u8, n.call.name, "grouping")) return ast.rebuildExpr(cx.p.arena, n, cx, recur);
            if (n.call.args.len == 0) return cx.p.fail(cx.pos, "GROUPING takes the grouping keys it reports on", .{});
            var acc: ?*ast.Expr = null;
            for (n.call.args) |a| {
                if (a.* != .field) return cx.p.fail(cx.pos, "GROUPING takes grouping keys by name", .{});
                const at = for (cx.keys, 0..) |k, i| {
                    if (std.mem.eql(u8, k.last(), a.field.last())) break i;
                } else return cx.p.fail(cx.pos, "GROUPING names `{s}`, which is not a grouping key", .{a.field.last()});
                cx.used[at] = true;
                const bit = try cx.p.mk(.{ .field = try cx.p.singleName(try groupingName(cx.p, cx.keys[at])) });
                acc = if (acc) |have| blk: {
                    const two = try cx.p.mk(.{ .int_lit = 2 });
                    const doubled = try cx.p.mk(.{ .binary = .{ .op = .mul, .l = have, .r = two } });
                    break :blk try cx.p.mk(.{ .binary = .{ .op = .add, .l = doubled, .r = bit } });
                } else bit;
            }
            return acc.?;
        }
    };
    return Cx.recur(.{ .p = self, .keys = keys, .used = used, .pos = pos }, e);
}

const ExpandedSets = struct { by: []const ast.QualName, names: []const ast.SelectItem.Rename, on_empty: []const []const u8 };

/// Each input row made one row per grouping set, a hidden key saying which, and each
/// GROUP BY key a hidden copy that is NULL in the sets without it. The aggregate groups
/// on those, so a key still reads as itself inside `SUM(a)`, and a NULL a key's own
/// data holds stays apart from the NULL of a set without it. A set of no keys answers
/// even over no rows, as a plain aggregate does (`on_empty`, those sets' ids).
fn expandGroupingSets(self: *Parser, stages: *std.array_list.Managed(ast.Stage), keys: []const ast.QualName, sets: KeySets, used: []const bool, pos: Pos) Error!ExpandedSets {
    var ids = std.array_list.Managed(u8).init(self.arena);
    for (sets, 0..) |_, i| {
        if (i > 0) try ids.append(',');
        try ids.writer().print("{d}", .{i});
    }
    const with_id = try self.arena.alloc(ast.SelectItem, 2);
    with_id[0] = .star;
    with_id[1] = .{ .computed = .{ .name = set_col, .expr = try self.mk(.{ .str_lit = ids.items }) } };
    try stages.append(.{ .node = .{ .select = with_id }, .hints = &.{}, .pos = pos });
    try stages.append(.{ .node = .{ .explode = .{ .field = set_col, .as_name = null, .delim = "," } }, .hints = &.{}, .pos = pos });

    var items = std.array_list.Managed(ast.SelectItem).init(self.arena);
    try items.append(.star);
    var by = std.array_list.Managed(ast.QualName).init(self.arena);
    var names = std.array_list.Managed(ast.SelectItem.Rename).init(self.arena);
    for (keys, used, 0..) |k, is_used, at| {
        var without: ?*ast.Expr = null;
        for (sets, 0..) |set, i| {
            if (std.mem.indexOfScalar(usize, set, at) != null) continue;
            const id = try std.fmt.allocPrint(self.arena, "{d}", .{i});
            const col = try self.mk(.{ .field = try self.singleName(set_col) });
            const eq = try self.mk(.{ .binary = .{ .op = .eq, .l = col, .r = try self.mk(.{ .str_lit = id }) } });
            without = if (without) |w| try self.mk(.{ .binary = .{ .op = .@"or", .l = w, .r = eq } }) else eq;
        }
        const name = try groupedKeyName(self, k);
        const own = try self.mk(.{ .field = k });
        const cond = without orelse try self.mk(.{ .bool_lit = false });
        const copy = try self.mk(.{ .cond = .{ .cond = cond, .then = try self.mk(.null_lit), .els = own } });
        try items.append(.{ .computed = .{ .name = name, .expr = copy } });
        try by.append(try self.singleName(name));
        try names.append(.{ .from = name, .to = k.last() });
        if (is_used) {
            const bit_name = try groupingName(self, k);
            const bit = try self.mk(.{ .cond = .{ .cond = cond, .then = try self.mk(.{ .int_lit = 1 }), .els = try self.mk(.{ .int_lit = 0 }) } });
            try items.append(.{ .computed = .{ .name = bit_name, .expr = bit } });
            try by.append(try self.singleName(bit_name));
        }
    }
    try by.append(try self.singleName(set_col));
    try stages.append(.{ .node = .{ .select = try items.toOwnedSlice() }, .hints = &.{}, .pos = pos });
    var totals = std.array_list.Managed([]const u8).init(self.arena);
    for (sets, 0..) |set, i| if (set.len == 0) try totals.append(try std.fmt.allocPrint(self.arena, "{d}", .{i}));
    return .{ .by = try by.toOwnedSlice(), .names = try names.toOwnedSlice(), .on_empty = try totals.toOwnedSlice() };
}

/// QUALIFY's `e` with each bare name of a window function's output, or of a SELECT
/// item computed over one, replaced by what it stands for, as the filter runs before
/// the SELECT list is made.
fn qualifyNames(self: *Parser, e: *ast.Expr, calls: []const Parser.WinCall, post: []const ast.SelectItem) Error!*ast.Expr {
    const Cx = struct {
        p: *Parser,
        calls: []const Parser.WinCall,
        post: []const ast.SelectItem,
        fn recur(cx: @This(), n: *const ast.Expr) Error!*ast.Expr {
            if (n.* == .field and n.field.parts.len == 1 and !n.field.dollar) {
                const name = n.field.parts[0];
                for (cx.calls) |c| if (c.out) |o| if (std.mem.eql(u8, o, name))
                    return cx.p.mk(.{ .field = try cx.p.singleName(c.hidden) });
                for (cx.post) |it| if (it == .computed and std.mem.eql(u8, it.computed.name, name) and Window.refsWindow(it.computed.expr, cx.calls))
                    return it.computed.expr;
            }
            return ast.rebuildExpr(cx.p.arena, n, cx, recur);
        }
    };
    return Cx.recur(.{ .p = self, .calls = calls, .post = post }, e);
}

/// Whether two column names are the same name, part by part.
fn qualEql(a: ast.QualName, b: ast.QualName) bool {
    if (a.parts.len != b.parts.len) return false;
    for (a.parts, b.parts) |x, y| if (!std.mem.eql(u8, x, y)) return false;
    return true;
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
    }
    if (self.isKw("anchor") or self.isKw("pushdown"))
        return self.fail(self.curPos(), "`{s}` applies to `UNION ALL BY NAME`", .{self.cur().text});

    var terms = std.array_list.Managed([]const ast.Stage).init(self.arena);
    var joins = std.array_list.Managed(SetOpTok).init(self.arena);
    try terms.append(cores[0].stages);
    for (ops, cores[1..]) |o, c| {
        if (o.kind == .intersect) {
            const last = &terms.items[terms.items.len - 1];
            last.* = try self.setOpStages(last.*, c.stages, .intersect, false, o.all, pos);
        } else {
            try joins.append(o);
            try terms.append(c.stages);
        }
    }
    var acc = terms.items[0];
    for (joins.items, terms.items[1..]) |o, t| {
        acc = if (o.kind == .except)
            try self.setOpStages(acc, t, .except, false, o.all, pos)
        else
            try self.setOpStages(acc, t, .union_all, !o.all, false, pos);
    }
    try stages.appendSlice(acc);
}

/// `left <op> right` as a positional union_, plus a DISTINCT for a plain UNION.
pub fn setOpStages(self: *Parser, left: []const ast.Stage, right: []const ast.Stage, set: ast.SetOp, distinct: bool, all: bool, pos: Pos) Error![]const ast.Stage {
    const branches = try self.arena.alloc(ast.UnionBranch, 2);
    branches[0] = try self.unionBranchFromStages(left);
    branches[1] = try self.unionBranchFromStages(right);
    var out = std.array_list.Managed(ast.Stage).init(self.arena);
    try out.append(.{ .node = .{ .union_ = .{ .branches = branches, .positional = true, .set = set, .set_all = all, .pos = pos } }, .hints = &.{}, .pos = pos });
    if (distinct) try out.append(.{ .node = .{ .distinct = .{ .on = null } }, .hints = &.{}, .pos = pos });
    return out.toOwnedSlice();
}

pub fn appendUnion(self: *Parser, cores_in: []const Core, distinct: bool, positional: bool, stages: *std.array_list.Managed(ast.Stage), pos: Pos) Error!void {
    var schema_from: ?ast.Hint = null;
    const cores = if (positional) cores_in else try liftSchemaFrom(self, cores_in, &schema_from);
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
    if (schema_from) |h| try hints.append(h);
    if (!positional) try self.parseUnionClauses(&hints, pos);

    try stages.append(.{
        .node = .{ .union_ = .{ .branches = try branches.toOwnedSlice(), .positional = positional, .pos = pos } },
        .hints = try hints.toOwnedSlice(),
        .pos = pos,
    });
    if (distinct) try stages.append(.{ .node = .{ .distinct = .{ .on = null } }, .hints = &.{}, .pos = pos });
}

/// `cores` with a `WITH (schema_from = ...)` written after the last branch taken
/// off that branch's read into `out`: it names the union's schema, which no
/// single read has.
fn liftSchemaFrom(self: *Parser, cores: []const Core, out: *?ast.Hint) Error![]const Core {
    if (cores.len == 0) return cores;
    const last = cores[cores.len - 1];
    if (last.stages.len == 0) return cores;
    const hs = last.stages[0].hints;
    const at = for (hs, 0..) |h, i| {
        if (std.mem.eql(u8, h.key, "schema_from")) break i;
    } else return cores;
    out.* = hs[at];
    const kept = try self.arena.alloc(ast.Hint, hs.len - 1);
    @memcpy(kept[0..at], hs[0..at]);
    @memcpy(kept[at..], hs[at + 1 ..]);
    const st = try self.arena.dupe(ast.Stage, last.stages);
    st[0].hints = kept;
    const cs = try self.arena.dupe(Core, cores);
    cs[cs.len - 1].stages = st;
    return cs;
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
        const lk = try msgfmt.allocPrint(self.arena, "__ul{d}", .{self.derived_n});
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
