//! Subqueries in expression position that reference the query around them,
//! decorrelated while parsing into ordinary joins and aggregates, plus `EXISTS`.
//!
//! A subquery's qualified columns that its own FROM does not know are handed out
//! to the query around it (`QualRef`); in the SELECT list or WHERE of a select core
//! (`Parser.corr` set) `captureSubquery` takes them back, and they are its
//! correlation. Each must sit in a conjunct of the subquery's WHERE (or of an inner
//! JOIN's ON, the same thing), never in its SELECT list, HAVING, ORDER BY, a
//! derived table or a set operation. A conjunct `inner = outer` is a join key, read
//! on the right from a hidden column `__sq<n>_k<i>`; any other is a residual
//! condition over both rows, its inner columns hidden as `__sq<n>_c<i>`. The outer
//! columns are checked and stripped only once the outer FROM is known
//! (`lowerWhere`), since the SELECT list is parsed before it.
//!
//!   EXISTS / NOT EXISTS   semi / anti join on the keys, residual as the join's;
//!                         a NULL key matches nothing. Uncorrelated: a query LET.
//!   x IN (SELECT y ...)   semi join on (keys, x) = (keys, y).
//!   x NOT IN (...)        anti join on the keys with residual `x = y OR x IS NULL
//!                         OR y IS NULL`: SQL's three-valued NOT IN per outer row.
//!   (SELECT agg ...)      LEFT JOIN to the aggregate grouped by the keys; a row
//!                         that matches nothing takes the subquery's value over no
//!                         rows (COUNT 0, `coalesce(sum(v), 0)` 0), not NULL.
//!   (SELECT v ...)        LEFT JOIN to `count(*)`, `min(v)` grouped by the keys;
//!                         `__one_row` fails the run when a row gets more than one.
//!
//! A scalar subquery correlates by equalities only, and LIMIT, windows, GROUP BY
//! and set operations inside a correlated subquery are refused: each would apply
//! per outer row, which one join cannot.

const std = @import("std");
const ast = @import("../ast.zig");
const sp = @import("../sql_parser.zig");
const Parser = sp.Parser;
const AliasSet = sp.AliasSet;
const Error = sp.Error;
const Pos = sp.Pos;
const QualRef = Parser.QualRef;
const Corr = Parser.Corr;
const splitAnd = sp.splitAnd;
const stripQual = sp.stripQual;
const stageWord = @import("tablefn.zig").stageWord;

pub const Captured = struct { pipe: ast.Pipeline, where_end: ?usize, refs: []const QualRef };

/// A subquery in expression position, `(` consumed. Where correlation is allowed,
/// the columns it handed out to the query around it are taken back as its `refs`.
pub fn captureSubquery(self: *Parser) Error!Captured {
    const sink = self.qual_sink;
    const base = if (sink) |s| s.items.len else 0;
    const pipe = try self.parseSubqueryPipeline();
    const we = self.query_where_end;
    var refs: []const QualRef = &.{};
    if (self.corr != null) if (sink) |s| if (s.items.len > base) {
        refs = try self.arena.dupe(QualRef, s.items[base..]);
        s.shrinkRetainingCapacity(base);
    };
    for (refs) |r| if (r.depth > 1)
        return self.fail(r.pos, "`{s}.{s}` reaches the query around this subquery from a derived table, CTE or subquery nested inside it — a correlated subquery may reference only the query directly around it, from its own WHERE", .{ r.q.parts[0], r.q.last() });
    return .{ .pipe = pipe, .where_end = we, .refs = refs };
}

/// Refuses a correlated column whose table the outer query, now parsed, lacks.
pub fn checkCorrRefs(self: *Parser, refs: []const QualRef, aliases: *const AliasSet) Error!void {
    for (refs) |r| {
        const t = r.q.parts[0];
        if (!aliases.knows(t))
            return self.fail(r.pos, "unknown table `{s}` in `{s}.{s}` — neither this subquery's FROM nor that of the query directly around it names or aliases `{s}` (a subquery may reference only the query directly around it)", .{ t, t, r.q.last(), t });
    }
}

const Outer = struct {
    names: []const []const u8,

    fn isQ(self: Outer, q: ast.QualName) bool {
        if (q.dollar or q.parts.len < 2) return false;
        for (self.names) |n| if (std.mem.eql(u8, n, q.parts[0])) return true;
        return false;
    }

    fn first(self: Outer, e: *const ast.Expr) ?ast.QualName {
        const Ctx = struct { o: Outer, out: *?ast.QualName };
        const S = struct {
            fn hit(cx: Ctx, n: *const ast.Expr) ?bool {
                if (n.* != .field) return null;
                if (!cx.o.isQ(n.field)) return false;
                cx.out.* = n.field;
                return true;
            }
        };
        var out: ?ast.QualName = null;
        _ = ast.anyExpr(e, Ctx{ .o = self, .out = &out }, S.hit);
        return out;
    }

    fn inStage(self: Outer, st: ast.Stage) ?ast.QualName {
        switch (st.node) {
            .filter => |e| return self.first(e),
            .select => |items| for (items) |it| switch (it) {
                .field => |f| if (self.isQ(f)) return f,
                .computed => |c| if (self.first(c.expr)) |q| return q,
                else => {},
            },
            .aggregate => |ag| {
                for (ag.aggs) |a| if (a.arg) |x| if (self.first(x)) |q| return q;
                for (ag.by) |q| if (self.isQ(q)) return q;
            },
            .join => |j| {
                for (j.left_keys) |q| if (self.isQ(q)) return q;
                for (j.right_keys) |q| if (self.isQ(q)) return q;
                if (j.residual) |r| if (self.first(r)) |q| return q;
                for (j.deferred) |d| {
                    if (self.first(d.a)) |q| return q;
                    if (self.first(d.b)) |q| return q;
                }
            },
            .sort => |s| for (s.keys) |k| if (self.isQ(k.field)) return k.field,
            .distinct => |d| if (d.on) |on| for (on) |q| if (self.isQ(q)) return q,
            else => {},
        }
        return null;
    }
};

fn placeWord(n: ast.Stage.Node, after_agg: bool) []const u8 {
    return switch (n) {
        .select => "its SELECT list",
        .filter => if (after_agg) "its HAVING" else "its WHERE",
        .aggregate => "an aggregate of its SELECT list",
        .join => "a JOIN ... ON or IN (SELECT ...) of its own",
        .sort => "its ORDER BY",
        .distinct => "its DISTINCT ON",
        else => stageWord(n),
    };
}

/// A correlated subquery being decorrelated: its stages, editable, the end of its
/// FROM ... WHERE part, and the hidden-name number it takes.
const Work = struct {
    stages: std.array_list.Managed(ast.Stage),
    where_end: usize,
    outer: Outer,
    id: usize,
    refs: []const QualRef,
    pos: Pos,

    outer_keys: std.array_list.Managed(ast.QualName),
    right_keys: std.array_list.Managed(ast.QualName),
    proj: std.array_list.Managed(ast.SelectItem),
    residual: ?*ast.Expr = null,
    outer_only: ?*ast.Expr = null,

    fn name(self: *Work, p: *Parser, comptime suffix: []const u8, i: usize) Error![]const u8 {
        return std.fmt.allocPrint(p.arena, "__sq{d}_" ++ suffix ++ "{d}", .{ self.id, i });
    }

    fn hidden(self: *const Work, p: *Parser) Error![]const []const u8 {
        var out = std.array_list.Managed([]const u8).init(p.arena);
        for (self.proj.items) |it| try out.append(it.computed.name);
        return out.toOwnedSlice();
    }
};

/// Checks where the correlation sits and pulls it out of the subquery's WHERE:
/// keys into `outer_keys`/`right_keys` with their inner side projected, the rest
/// into `residual` when `residual_ok`, else refused naming the condition; one of
/// the outer row alone goes to `outer_only` there.
fn prepare(self: *Parser, cap: Captured, what: []const u8, residual_ok: bool) Error!Work {
    const pos = cap.pipe.pos;
    const we = cap.where_end orelse
        return self.fail(cap.refs[0].pos, "{s}: a correlated subquery cannot be a UNION, INTERSECT or EXCEPT — correlate each branch's query, or join", .{what});
    var names = std.array_list.Managed([]const u8).init(self.arena);
    for (cap.refs) |r| {
        for (names.items) |n| {
            if (std.mem.eql(u8, n, r.q.parts[0])) break;
        } else try names.append(r.q.parts[0]);
    }
    const outer = Outer{ .names = names.items };
    for (cap.refs) |r| {
        const found = for (cap.pipe.stages) |st| {
            if (outer.inStage(st) != null) break true;
        } else false;
        if (!found)
            return self.fail(r.pos, "`{s}.{s}` reaches the query around this subquery from a derived table, CTE or subquery nested inside it — a correlated subquery may reference only the query directly around it, from its own WHERE", .{ r.q.parts[0], r.q.last() });
    }
    self.derived_n += 1;
    var w = Work{
        .stages = .init(self.arena),
        .where_end = we,
        .outer = outer,
        .id = self.derived_n,
        .refs = cap.refs,
        .pos = pos,
        .outer_keys = .init(self.arena),
        .right_keys = .init(self.arena),
        .proj = .init(self.arena),
    };
    try w.stages.appendSlice(cap.pipe.stages);

    var after_agg = false;
    for (w.stages.items, 0..) |st, i| {
        if (st.node == .aggregate) after_agg = true;
        const q = outer.inStage(st) orelse continue;
        if (i < we and st.node == .filter) continue;
        return self.fail(refPos(cap.refs, q), "{s}: `{s}.{s}` correlates the subquery in {s} — only conditions in a correlated subquery's WHERE may reference the query around it", .{ what, q.parts[0], q.last(), placeWord(st.node, after_agg) });
    }

    var cc_n: usize = 0;
    var i: usize = 0;
    while (i < w.where_end) {
        const st = &w.stages.items[i];
        if (st.node != .filter or outer.first(st.node.filter) == null) {
            i += 1;
            continue;
        }
        var conj = std.array_list.Managed(*ast.Expr).init(self.arena);
        try splitAnd(st.node.filter, &conj);
        var keep = std.array_list.Managed(*ast.Expr).init(self.arena);
        for (conj.items) |c| {
            if (outer.first(c) == null) {
                try keep.append(c);
                continue;
            }
            if (eqKey(outer, c)) |k| {
                const kn = try w.name(self, "k", w.right_keys.items.len);
                try w.outer_keys.append(k.outer);
                try w.right_keys.append(try self.singleName(kn));
                try w.proj.append(.{ .computed = .{ .name = kn, .expr = k.inner } });
                continue;
            }
            if (!residual_ok and !hasInner(outer, c)) {
                w.outer_only = if (w.outer_only) |acc| try self.mk(.{ .binary = .{ .op = .@"and", .l = acc, .r = c } }) else c;
                continue;
            }
            if (!residual_ok) {
                var buf = std.array_list.Managed(u8).init(self.arena);
                try self.unparse(&buf, c);
                const shown = try std.mem.replaceOwned(u8, self.arena, buf.items, " == ", " = ");
                return self.fail(refPos(cap.refs, outer.first(c).?), "{s}: the correlated condition `{s}` is not an equality between a column of the subquery and one of the query around it — a scalar subquery correlates by `inner = outer` only; for a range or an inequality, join the two queries and aggregate with GROUP BY", .{ what, shown });
            }
            const r = try hideInner(self, &w, c, &cc_n);
            w.residual = if (w.residual) |acc| try self.mk(.{ .binary = .{ .op = .@"and", .l = acc, .r = r } }) else r;
        }
        if (keep.items.len == 0) {
            _ = w.stages.orderedRemove(i);
            w.where_end -= 1;
            continue;
        }
        var e = keep.items[0];
        for (keep.items[1..]) |r| e = try self.mk(.{ .binary = .{ .op = .@"and", .l = e, .r = r } });
        st.node = .{ .filter = e };
        i += 1;
    }
    return w;
}

fn refPos(refs: []const QualRef, q: ast.QualName) Pos {
    if (q.span) |s| return s.start;
    for (refs) |r| if (ast.QualName.eql(r.q, q)) return r.pos;
    return refs[0].pos;
}

/// `inner = outer` either way round: the outer side a column of the query around,
/// the inner one an expression of the subquery's own columns only.
fn eqKey(outer: Outer, c: *ast.Expr) ?struct { outer: ast.QualName, inner: *ast.Expr } {
    if (c.* != .binary or c.binary.op != .eq) return null;
    const sides = [_][2]*ast.Expr{ .{ c.binary.l, c.binary.r }, .{ c.binary.r, c.binary.l } };
    for (sides) |s| {
        if (s[0].* != .field or !outer.isQ(s[0].field)) continue;
        if (outer.first(s[1]) != null or !innerField(s[1])) continue;
        return .{ .outer = s[0].field, .inner = s[1] };
    }
    return null;
}

/// Whether `c` reads a column of the subquery, not only the outer row's.
fn hasInner(outer: Outer, c: *const ast.Expr) bool {
    const S = struct {
        fn hit(o: Outer, n: *const ast.Expr) ?bool {
            return if (n.* == .field) !n.field.dollar and !o.isQ(n.field) else null;
        }
    };
    return ast.anyExpr(c, outer, S.hit);
}

fn innerField(e: *const ast.Expr) bool {
    const S = struct {
        fn hit(_: void, n: *const ast.Expr) ?bool {
            return if (n.* == .field) !n.field.dollar else null;
        }
    };
    return ast.anyExpr(e, {}, S.hit);
}

/// `c` with each column of the subquery read from a hidden column of the right
/// side, which the projection adds; the outer columns are left as written.
fn hideInner(self: *Parser, w: *Work, c: *ast.Expr, n: *usize) Error!*ast.Expr {
    const Ctx = struct { p: *Parser, w: *Work, n: *usize };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field and !node.field.dollar and !cx.w.outer.isQ(node.field)) {
                const nm = try cx.w.name(cx.p, "c", cx.n.*);
                cx.n.* += 1;
                try cx.w.proj.append(.{ .computed = .{ .name = nm, .expr = @constCast(node) } });
                return cx.p.mk(.{ .field = try cx.p.singleName(nm) });
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .w = w, .n = n }, c);
}

/// Whether a projection's items pass the column `n` on.
fn carries(items: []const ast.SelectItem, n: []const u8) bool {
    for (items) |it| switch (it) {
        .star, .star_rename => return true,
        .star_of => |so| if (so.rel.len == 0) return true,
        .star_except => |ex| {
            for (ex) |x| {
                if (std.mem.eql(u8, x, n)) break;
            } else return true;
        },
        .field => |f| if (std.mem.eql(u8, f.last(), n)) return true,
        .computed => |c| if (std.mem.eql(u8, c.name, n)) return true,
    };
    return false;
}

/// Adds the projection after the WHERE (`*` and the hidden columns) and has each
/// select that was at `where_end..to` carry those columns on.
fn carryHidden(self: *Parser, w: *Work, to: usize) Error!void {
    const items = try self.arena.alloc(ast.SelectItem, w.proj.items.len + 1);
    items[0] = .star;
    @memcpy(items[1..], w.proj.items);
    try w.stages.insert(w.where_end, .{ .node = .{ .select = items }, .hints = &.{}, .pos = w.pos });
    const names = try w.hidden(self);
    for (w.stages.items[w.where_end + 1 .. to + 1]) |*st| if (st.node == .select) {
        var ext = std.array_list.Managed(ast.SelectItem).init(self.arena);
        try ext.appendSlice(st.node.select);
        for (names) |n| if (!carries(ext.items, n)) try ext.append(.{ .field = try self.singleName(n) });
        st.node = .{ .select = try ext.toOwnedSlice() };
    };
}

fn fieldExpr(self: *Parser, n: []const u8) Error!*ast.Expr {
    return self.mk(.{ .field = try self.singleName(n) });
}

fn plainLimit(l: ast.Limit) bool {
    return l.count >= 1 and l.offset == 0 and l.count_expr == null and l.offset_expr == null;
}

fn corrOf(w: *Work) Error!Corr {
    return .{
        .outer_keys = try w.outer_keys.toOwnedSlice(),
        .right_keys = try w.right_keys.toOwnedSlice(),
        .residual = w.residual,
        .refs = w.refs,
    };
}

fn sentinelFor(self: *Parser, w: *Work, lhs: ?*ast.Expr, negated: bool, what: []const u8, pos: Pos) Error!*ast.Expr {
    const bname = try std.fmt.allocPrint(self.arena, "__sq{d}", .{w.id});
    try self.bindHidden(bname, try w.stages.toOwnedSlice(), pos);
    const sentinel = try self.mk(.{ .bool_lit = true });
    try self.pending_semijoins.append(.{
        .sentinel = sentinel,
        .lhs = lhs,
        .binding = bname,
        .negated = negated,
        .pos = pos,
        .what = what,
        .corr = try corrOf(w),
    });
    return sentinel;
}

/// `[NOT] EXISTS (subquery)`, `EXISTS` consumed. Uncorrelated it is a query LET
/// (does the subquery return a row); correlated, a semi or anti join held in the
/// WHERE by a sentinel.
pub fn existsSubquery(self: *Parser, negated: bool, pos: Pos) Error!*ast.Expr {
    const what = if (negated) "NOT EXISTS" else "EXISTS";
    if (self.query_depth == 0)
        return self.fail(pos, "EXISTS (SELECT ...) is only supported inside a query — bind it first with `LET x = (SELECT count(*) > 0 FROM ...);`", .{});
    _ = try self.expect(.lparen);
    if (!self.isKw("select") and !self.isKw("with"))
        return self.fail(self.curPos(), "EXISTS takes a subquery: `EXISTS (SELECT ...)`", .{});
    const cap = try self.captureSubquery();
    if (cap.refs.len == 0) {
        const n = try self.freshName("__exn{d}");
        const stages = try self.arena.alloc(ast.Stage, cap.pipe.stages.len + 3);
        @memcpy(stages[0..cap.pipe.stages.len], cap.pipe.stages);
        const k = cap.pipe.stages.len;
        const aggs = try self.arena.dupe(ast.AggItem, &.{.{ .name = n, .func = .count, .arg = null }});
        const zero = try self.mk(.{ .int_lit = 0 });
        const test_ = try self.mk(.{ .binary = .{ .op = .gt, .l = try fieldExpr(self, n), .r = zero } });
        stages[k] = .{ .node = .{ .limit = .{ .count = 1 } }, .hints = &.{}, .pos = pos };
        stages[k + 1] = .{ .node = .{ .aggregate = .{ .aggs = aggs, .by = &.{} } }, .hints = &.{}, .pos = pos };
        stages[k + 2] = .{ .node = .{ .select = try self.arena.dupe(ast.SelectItem, &.{.{ .computed = .{ .name = "exists", .expr = test_ } }}) }, .hints = &.{}, .pos = pos };
        const name = try self.freshName("__scalar{d}");
        try self.pending_bindings.append(.{ .let_const = .{ .name = name, .expr = null, .query = .{ .stages = stages, .pos = cap.pipe.pos }, .pos = pos } });
        var q = try self.singleName(name);
        q.dollar = true;
        const e = try self.mk(.{ .field = q });
        return if (negated) self.mk(.{ .unary = .{ .op = .not, .e = e } }) else e;
    }
    if (!self.in_where)
        return self.fail(pos, "a correlated {s} is only supported as a top-level AND condition of WHERE", .{what});
    var w = try prepare(self, cap, what, true);
    for (w.stages.items[w.where_end..]) |st| switch (st.node) {
        .select, .distinct, .sort => {},
        .limit => |l| if (!plainLimit(l))
            return self.fail(pos, "a correlated {s} with LIMIT 0 or an OFFSET is not supported — the limit would apply per row of the query around it", .{what}),
        .aggregate => return self.fail(pos, "a correlated {s} over an aggregate or GROUP BY is not supported — test the rows themselves (an ungrouped aggregate always returns a row), or join and group", .{what}),
        else => return self.fail(pos, "a correlated {s} with {s} is not supported — it would apply per row of the query around it", .{ what, stageWord(st.node) }),
    };
    w.stages.shrinkRetainingCapacity(w.where_end);
    if (w.proj.items.len == 0) try w.proj.append(.{ .computed = .{ .name = try w.name(self, "x", 0), .expr = try self.mk(.{ .bool_lit = true }) } });
    try w.stages.append(.{ .node = .{ .select = try self.arena.dupe(ast.SelectItem, w.proj.items) }, .hints = &.{}, .pos = w.pos });
    return sentinelFor(self, &w, null, negated, what, pos);
}

/// `x [NOT] IN (SELECT y ...)` whose subquery is correlated: a semi join on the
/// keys and `x = y`, or an anti join on the keys whose residual keeps NOT IN's
/// three-valued answer for each outer row.
pub fn correlatedIn(self: *Parser, lhs: *ast.Expr, cap: Captured, negated: bool, pos: Pos) Error!*ast.Expr {
    const what = if (negated) "NOT IN (SELECT ...)" else "IN (SELECT ...)";
    const col = self.firstOutName(cap.pipe.stages) orelse
        return self.fail(pos, "the subquery of IN must produce exactly one named column", .{});
    var w = try prepare(self, cap, what, true);
    for (w.stages.items[w.where_end..]) |st| switch (st.node) {
        .select, .sort => {},
        .distinct => |d| if (d.on != null) return self.fail(pos, "a correlated {s} with DISTINCT ON is not supported", .{what}),
        .aggregate => return self.fail(pos, "a correlated {s} over an aggregate is not supported — compare with a scalar subquery instead: `x = (SELECT max(...) FROM ... WHERE ...)`", .{what}),
        .limit => return self.fail(pos, "a correlated {s} with LIMIT or OFFSET is not supported — the limit would apply per row of the query around it", .{what}),
        else => return self.fail(pos, "a correlated {s} with {s} is not supported — it would apply per row of the query around it", .{ what, stageWord(st.node) }),
    };
    try carryHidden(self, &w, w.stages.items.len);
    const vn = try w.name(self, "v", 0);
    var fin = std.array_list.Managed(ast.SelectItem).init(self.arena);
    try fin.append(.{ .computed = .{ .name = vn, .expr = try fieldExpr(self, col) } });
    for (try w.hidden(self)) |n| try fin.append(.{ .field = try self.singleName(n) });
    try w.stages.append(.{ .node = .{ .select = try fin.toOwnedSlice() }, .hints = &.{}, .pos = w.pos });
    const v = try fieldExpr(self, vn);
    if (!negated) {
        try w.outer_keys.append(lhs.field);
        try w.right_keys.append(try self.singleName(vn));
    } else {
        const eq = try self.mk(.{ .binary = .{ .op = .eq, .l = lhs, .r = v } });
        const ln = try self.mk(.{ .is_null = .{ .e = lhs, .negated = false } });
        const rn = try self.mk(.{ .is_null = .{ .e = v, .negated = false } });
        const any = try self.mk(.{ .binary = .{ .op = .@"or", .l = try self.mk(.{ .binary = .{ .op = .@"or", .l = eq, .r = ln } }), .r = rn } });
        w.residual = if (w.residual) |r| try self.mk(.{ .binary = .{ .op = .@"and", .l = r, .r = any } }) else any;
    }
    return sentinelFor(self, &w, lhs, negated, what, pos);
}

/// A correlated scalar subquery: a LEFT JOIN registered with the select core and
/// the expression that reads its value. With an aggregate the subquery groups by
/// the keys and an unmatched row takes its value over no rows; without one, it
/// counts the rows per key and more than one fails the run.
pub fn correlatedScalar(self: *Parser, cap: Captured, pos: Pos) Error!*ast.Expr {
    const what = "scalar subquery";
    var w = try prepare(self, cap, what, false);
    if (w.right_keys.items.len == 0)
        return self.fail(pos, "a correlated scalar subquery needs an equality `inner_column = outer_column` in its WHERE — a condition on the outer row alone does not say which rows of the subquery belong to it", .{});
    const st = w.stages.items;
    var ai: ?usize = null;
    for (st[w.where_end..], w.where_end..) |s, i| if (s.node == .aggregate) {
        ai = i;
        break;
    };
    const vn = try w.name(self, "v", 0);
    var hidden = std.array_list.Managed([]const u8).init(self.arena);
    for (w.right_keys.items) |k| try hidden.append(k.last());
    try hidden.append(vn);
    var value: *ast.Expr = undefined;
    var empty_out: *ast.Expr = try self.mk(.null_lit);
    if (ai) |a| {
        const ag = st[a].node.aggregate;
        if (ag.by.len > 0)
            return self.fail(pos, "a correlated scalar subquery with GROUP BY is not supported — it may return several rows per row of the query around it; aggregate without GROUP BY", .{});
        var having: ?*ast.Expr = null;
        var post: ?*ast.Expr = null;
        var j = a + 1;
        if (j < st.len and st[j].node == .filter) {
            having = st[j].node.filter;
            j += 1;
        }
        if (j < st.len and st[j].node == .select) {
            const items = st[j].node.select;
            if (items.len != 1) return self.fail(pos, "a scalar subquery must produce exactly one column", .{});
            post = switch (items[0]) {
                .computed => |c| c.expr,
                .field => |f| try self.mk(.{ .field = f }),
                else => return self.fail(pos, "a scalar subquery must produce exactly one column", .{}),
            };
            j += 1;
        }
        for (st[j..]) |s| switch (s.node) {
            .sort, .distinct => {},
            .limit => |l| if (!plainLimit(l)) return self.fail(pos, "a correlated scalar subquery with LIMIT 0 or an OFFSET is not supported", .{}),
            else => return self.fail(pos, "a correlated scalar subquery with {s} after its aggregate is not supported", .{stageWord(s.node)}),
        };
        for (st[w.where_end..a]) |s| if (s.node != .select)
            return self.fail(pos, "a correlated scalar subquery with {s} is not supported", .{stageWord(s.node)});
        const v = post orelse blk: {
            if (ag.aggs.len != 1) return self.fail(pos, "a scalar subquery must produce exactly one column", .{});
            break :blk try fieldExpr(self, ag.aggs[0].name);
        };
        try carryHidden(self, &w, a);
        const a2 = a + 1;
        var agg = ag;
        agg.by = try self.arena.dupe(ast.QualName, w.right_keys.items);
        w.stages.items[a2].node = .{ .aggregate = agg };
        w.stages.shrinkRetainingCapacity(a2 + 1);
        const nul = try self.mk(.null_lit);
        const out = if (having) |h| try self.mk(.{ .cond = .{ .cond = h, .then = v, .els = nul } }) else v;
        var fin = std.array_list.Managed(ast.SelectItem).init(self.arena);
        for (w.right_keys.items) |k| try fin.append(.{ .field = k });
        try fin.append(.{ .computed = .{ .name = vn, .expr = out } });
        try w.stages.append(.{ .node = .{ .select = try fin.toOwnedSlice() }, .hints = &.{}, .pos = w.pos });

        const empty = try emptyValue(self, v, ag.aggs);
        empty_out = if (having) |h| try self.mk(.{ .cond = .{ .cond = try emptyValue(self, h, ag.aggs), .then = empty, .els = nul } }) else empty;
        value = try fieldExpr(self, vn);
        if (empty_out.* != .null_lit) {
            const unmatched = try self.mk(.{ .is_null = .{ .e = try fieldExpr(self, w.right_keys.items[0].last()), .negated = false } });
            value = try self.mk(.{ .cond = .{ .cond = unmatched, .then = empty_out, .els = value } });
        }
    } else {
        const col = self.firstOutName(cap.pipe.stages) orelse
            return self.fail(pos, "a scalar subquery must produce exactly one named column", .{});
        for (st[w.where_end..]) |s| switch (s.node) {
            .select, .sort => {},
            .distinct => |d| if (d.on != null) return self.fail(pos, "a correlated scalar subquery with DISTINCT ON is not supported", .{}),
            .limit => return self.fail(pos, "a correlated scalar subquery with LIMIT or OFFSET is not supported — the limit would apply per row of the query around it; pick the row with an aggregate (`max`, `min`) instead", .{}),
            else => return self.fail(pos, "a correlated scalar subquery with {s} is not supported", .{stageWord(s.node)}),
        };
        try carryHidden(self, &w, w.stages.items.len);
        const nn = try w.name(self, "n", 0);
        try hidden.append(nn);
        const aggs = try self.arena.dupe(ast.AggItem, &.{
            .{ .name = nn, .func = .count, .arg = null },
            .{ .name = vn, .func = .min, .arg = try fieldExpr(self, col) },
        });
        try w.stages.append(.{ .node = .{ .aggregate = .{ .aggs = aggs, .by = try self.arena.dupe(ast.QualName, w.right_keys.items) } }, .hints = &.{}, .pos = w.pos });
        const args = try self.arena.dupe(*ast.Expr, &.{ try fieldExpr(self, nn), try fieldExpr(self, vn) });
        value = try self.mk(.{ .call = .{ .name = "__one_row", .args = args } });
    }
    if (w.outer_only) |p| value = try self.mk(.{ .cond = .{ .cond = p, .then = value, .els = empty_out } });
    const bname = try std.fmt.allocPrint(self.arena, "__sq{d}", .{w.id});
    try self.bindHidden(bname, try w.stages.toOwnedSlice(), pos);
    try self.corr.?.scalars.append(.{
        .binding = bname,
        .corr = try corrOf(&w),
        .hidden = try hidden.toOwnedSlice(),
        .in_where = self.in_where,
        .pos = pos,
    });
    return value;
}

/// The value an aggregate expression takes over no rows: COUNT and COUNT_IF 0,
/// every other aggregate NULL.
fn emptyValue(self: *Parser, e: *ast.Expr, aggs: []const ast.AggItem) Error!*ast.Expr {
    const Ctx = struct { p: *Parser, aggs: []const ast.AggItem };
    const S = struct {
        fn recur(cx: Ctx, node: *const ast.Expr) Error!*ast.Expr {
            if (node.* == .field and !node.field.dollar and node.field.parts.len == 1) {
                for (cx.aggs) |a| if (std.mem.eql(u8, a.name, node.field.parts[0])) {
                    return cx.p.mk(if (a.func == .count or a.func == .count_if) .{ .int_lit = 0 } else .null_lit);
                };
            }
            return ast.rebuildExpr(cx.p.arena, node, cx, recur);
        }
    };
    return S.recur(.{ .p = self, .aggs = aggs }, e);
}

/// The stages a WHERE and the core's correlated subqueries lower to, once the
/// outer FROM is known: the conditions that read no correlated value, the LEFT
/// JOINs of the scalar subqueries, the conditions that read one, then the semi and
/// anti joins of IN and EXISTS. `hidden` gets the joined columns no `*` may show.
pub fn lowerWhere(
    self: *Parser,
    stages: *std.array_list.Managed(ast.Stage),
    aliases: *AliasSet,
    remaining: ?*ast.Expr,
    fpos: Pos,
    sj_base: usize,
    scalars: []const Parser.CorrScalar,
    hidden: *std.array_list.Managed([]const u8),
) Error!void {
    for (scalars) |s| try checkCorrRefs(self, s.corr.refs, aliases);
    for (self.pending_semijoins.items[sj_base..]) |sj| if (sj.corr) |c| try checkCorrRefs(self, c.refs, aliases);

    var pre: ?*ast.Expr = remaining;
    var post: ?*ast.Expr = null;
    var where_names = std.array_list.Managed([]const u8).init(self.arena);
    for (scalars) |s| if (s.in_where) try where_names.appendSlice(s.hidden);
    if (remaining != null and where_names.items.len > 0) {
        pre = null;
        var conj = std.array_list.Managed(*ast.Expr).init(self.arena);
        try splitAnd(remaining.?, &conj);
        for (conj.items) |c| {
            const reads = for (where_names.items) |n| {
                if (@import("window.zig").refsName(c, n)) break true;
            } else false;
            const slot = if (reads) &post else &pre;
            slot.* = if (slot.*) |acc| try self.mk(.{ .binary = .{ .op = .@"and", .l = acc, .r = c } }) else c;
        }
    }
    if (pre) |e| try stages.append(.{ .node = .{ .filter = try self.stripExpr(e, aliases) }, .hints = &.{}, .pos = fpos });
    for (scalars) |s| {
        try self.claimAlias(aliases, s.binding, s.pos, .reserved);
        try hidden.appendSlice(s.hidden);
        try stages.append(.{ .node = .{ .join = try corrJoin(self, .left, s.binding, s.corr, aliases) }, .hints = &.{}, .pos = s.pos });
    }
    if (post) |e| try stages.append(.{ .node = .{ .filter = try self.stripExpr(e, aliases) }, .hints = &.{}, .pos = fpos });
    for (self.pending_semijoins.items[sj_base..]) |sj| {
        const kind: ast.JoinKind = if (sj.negated) .anti else .semi;
        const j: ast.Join = if (sj.corr) |c| try corrJoin(self, kind, sj.binding, c, aliases) else .{
            .kind = kind,
            .binding = sj.binding,
            .left_keys = try self.arena.dupe(ast.QualName, &.{stripQual(sj.lhs.?.field, aliases)}),
            .right_keys = try self.arena.dupe(ast.QualName, &.{try self.singleName(sj.right_col)}),
            .pins = &.{.{ .left = true, .right = true }},
            .null_aware = sj.negated,
        };
        try stages.append(.{ .node = .{ .join = j }, .hints = &.{}, .pos = sj.pos });
    }
    self.pending_semijoins.shrinkRetainingCapacity(sj_base);
}

fn corrJoin(self: *Parser, kind: ast.JoinKind, binding: []const u8, c: Corr, aliases: *const AliasSet) Error!ast.Join {
    const lk = try self.arena.alloc(ast.QualName, c.outer_keys.len);
    for (c.outer_keys, lk) |q, *o| o.* = stripQual(q, aliases);
    const pins = try self.arena.alloc(ast.KeyPin, lk.len);
    @memset(pins, .{ .left = true, .right = true });
    return .{
        .kind = kind,
        .binding = binding,
        .alias = binding,
        .left_keys = lk,
        .right_keys = c.right_keys,
        .pins = pins,
        .residual = if (c.residual) |r| try self.stripExpr(r, aliases) else null,
    };
}
