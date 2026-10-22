//! Filters moved toward the sources: below a join to the side whose columns they
//! name (with a twin on the other side's key), and below a renaming select.
//!
//! A WHERE splits at its ANDs and each conjunct moves on its own. One naming only
//! the left input goes below an inner, cross, LEFT, semi or anti join, never below
//! a RIGHT or FULL one, which null-extends the left. One naming only a joined
//! side's alias goes into that side under an inner, cross or RIGHT join, never a
//! LEFT or FULL one, which null-extends it; on the way it may pass any join but a
//! RIGHT or FULL one. A bare name counts as the left's only when the right side's
//! names are known (its select list, or a local file's header or footer) and lack
//! it, or when it was written with the FROM table's alias and that table has it.
//! No expression is volatile and subqueries are joins by now, so a conjunct of
//! the left's columns gives each row the same answer above the join as below it.

const ast = @import("../../lang/ast.zig");
const qual = @import("../pushdown.zig").qual;
const std = @import("std");
pub const Dialect = @import("../pushdown.zig").Dialect;
pub const bin = @import("testing_util.zig").bin;
const dimBindings = @import("testing_util.zig").dimBindings;
const eqFilter = @import("testing_util.zig").eqFilter;
const filterStage = @import("testing_util.zig").filterStage;
pub const fld = @import("testing_util.zig").fld;
const intLit = @import("testing_util.zig").intLit;
const joinStage = @import("testing_util.zig").joinStage;
const offlineSchema = @import("../analyze/targets.zig").offlineSchema;
const projStage = @import("testing_util.zig").projStage;
pub const qfld = @import("testing_util.zig").qfld;
const readStage = @import("testing_util.zig").readStage;
const serialWhere = @import("../pushdown.zig").serialWhere;
const writeStage = @import("testing_util.zig").writeStage;

fn hoistableKind(k: ast.JoinKind) bool {
    return switch (k) {
        .inner, .cross, .left, .semi, .anti => true,
        .right, .full => false,
    };
}

fn bindingNames(
    arena: std.mem.Allocator,
    bindings: *const std.StringHashMap(ast.Pipeline),
    name: []const u8,
) !?[]const []const u8 {
    const pipe = bindings.get(name) orelse return null;
    var out = std.array_list.Managed([]const u8).init(arena);
    if (!try stageNames(bindings, pipe.stages, &out, 0)) return null;
    for (out.items) |n| if (std.mem.indexOf(u8, n, "${") != null) return null;
    return try out.toOwnedSlice();
}

/// Every name the output of `stages` has, appended to `out`; false when unknowable.
/// A local file read is asked for its header or footer.
fn stageNames(
    bindings: *const std.StringHashMap(ast.Pipeline),
    stages: []const ast.Stage,
    out: *std.array_list.Managed([]const u8),
    depth: usize,
) !bool {
    if (depth > 16) return false;
    var i = stages.len;
    while (i > 0) {
        i -= 1;
        switch (stages[i].node) {
            .filter, .limit, .sort, .distinct => {},
            .select => |items| {
                var input = std.array_list.Managed([]const u8).init(out.allocator);
                for (items) |it| switch (it) {
                    .star, .star_except, .star_rename => {
                        if (input.items.len == 0 and !try stageNames(bindings, stages[0..i], &input, depth + 1)) return false;
                        break;
                    },
                    .star_of => return false,
                    else => {},
                };
                for (items) |it| switch (it) {
                    .field => |q| try out.append(q.last()),
                    .computed => |c| try out.append(c.name),
                    .star => try out.appendSlice(input.items),
                    .star_except => |names| for (input.items) |n| {
                        for (names) |x| {
                            if (std.mem.eql(u8, x, n)) break;
                        } else try out.append(n);
                    },
                    .star_rename => |rs| for (input.items) |n| {
                        var to = n;
                        for (rs) |r| if (std.mem.eql(u8, r.from, n)) {
                            to = r.to;
                        };
                        try out.append(to);
                    },
                    .star_of => unreachable,
                };
                return true;
            },
            .window => |wd| for (wd.funcs) |f| try out.append(f.out),
            .aggregate => |ag| {
                for (ag.by) |q| try out.append(q.last());
                for (ag.aggs) |a| try out.append(a.name);
                return true;
            },
            .read => |rd| {
                const sch = offlineSchema(out.allocator, rd, stages[i].hints) orelse return false;
                for (sch.fields) |f| try out.append(f.name);
                return true;
            },
            .ref => |r| {
                const pipe = bindings.get(r) orelse return false;
                return stageNames(bindings, pipe.stages, out, depth + 1);
            },
            else => return false,
        }
    }
    return false;
}

/// Whether every source `stages` reads has columns known offline: a local file whose
/// header or footer says them, through refs and each joined side. A select over a
/// source only its database can name (or a union) keeps every item, so a column the
/// database lacks still fails the read rather than vanishing with a pruned item.
pub fn sourcesKnown(arena: std.mem.Allocator, bindings: *const std.StringHashMap(ast.Pipeline), stages: []const ast.Stage) bool {
    return sourcesKnownAt(arena, bindings, stages, 0);
}

fn sourcesKnownAt(arena: std.mem.Allocator, bindings: *const std.StringHashMap(ast.Pipeline), stages: []const ast.Stage, depth: usize) bool {
    if (depth > 16 or stages.len == 0) return false;
    switch (stages[0].node) {
        .read => |rd| if (offlineSchema(arena, rd, stages[0].hints) == null) return false,
        .ref => |r| {
            const pipe = bindings.get(r) orelse return false;
            if (!sourcesKnownAt(arena, bindings, pipe.stages, depth + 1)) return false;
        },
        else => return false,
    }
    for (stages[1..]) |st| if (st.node == .join) {
        const pipe = bindings.get(st.node.join.binding) orelse return false;
        if (!sourcesKnownAt(arena, bindings, pipe.stages, depth + 1)) return false;
    };
    return true;
}

fn isRightName(name: []const u8, right: []const []const u8) bool {
    for (right) |r| {
        if (std.mem.eql(u8, name, r)) return true;
        if (!std.mem.startsWith(u8, name, r)) continue;
        const tail = name[r.len..];
        if (std.mem.eql(u8, tail, "_r")) return true;
        if (tail.len > 2 and std.mem.startsWith(u8, tail, "_r")) {
            var all_digits = true;
            for (tail[2..]) |c| if (!std.ascii.isDigit(c)) {
                all_digits = false;
            };
            if (all_digits) return true;
        }
    }
    return false;
}

/// Whether evaluating `e` on some row could fail: a CAST of a column, arithmetic
/// (overflow, division by zero), or a call not known to be total. Such a conjunct
/// never crosses a join, where it would meet rows the join drops.
pub fn mayRaise(e: *const ast.Expr) bool {
    const S = struct {
        const total = [_][]const u8{ "upper", "lower", "length", "strlen", "starts_with", "ends_with", "contains", "like", "trim", "coalesce", "nullif" };
        fn hit(_: void, n: *const ast.Expr) ?bool {
            return switch (n.*) {
                .cast => |c| if (!c.safe and ast.anyExpr(c.e, {}, field)) true else null,
                .unary => |u| if (u.op == .neg) true else null,
                .binary => |b| switch (b.op) {
                    .eq, .ne, .lt, .le, .gt, .ge, .@"and", .@"or", .distinct, .not_distinct => null,
                    else => true,
                },
                .call => |c| for (total) |t| {
                    if (std.ascii.eqlIgnoreCase(t, c.name)) break null;
                } else true,
                .lambda => true,
                else => null,
            };
        }
        fn field(_: void, n: *const ast.Expr) ?bool {
            return if (n.* == .field) true else null;
        }
    };
    return ast.anyExpr(e, {}, S.hit);
}

const Own = struct { written: []const []const u8 = &.{}, has: []const []const u8 = &.{} };

/// The columns written with the FROM table's alias (`f.x`, stripped to `x`), when only
/// that table's joins and filters come before, and those of them the table, `stages[0]`,
/// is known to have.
fn fromTableCols(
    arena: std.mem.Allocator,
    bindings: *const std.StringHashMap(ast.Pipeline),
    stages: []const ast.Stage,
) !Own {
    var own = std.array_list.Managed([]const u8).init(arena);
    for (stages[1..]) |st| switch (st.node) {
        .join => |jj| for (jj.own_cols) |q| try own.append(q.last()),
        .filter => {},
        else => return .{},
    };
    if (own.items.len == 0) return .{};
    var head = std.array_list.Managed([]const u8).init(arena);
    if (!try stageNames(bindings, stages[0..1], &head, 0)) return .{};
    var has = std.array_list.Managed([]const u8).init(arena);
    for (own.items) |n| for (head.items) |h| {
        if (std.mem.eql(u8, n, h)) {
            try has.append(n);
            break;
        }
    };
    return .{ .written = own.items, .has = has.items };
}

/// Whether `e` names only the join's left input: a name qualified by another table, a
/// name the right side lacks, or one written with the FROM table's alias that the
/// table has (a right column of its name was renamed apart). One the table may lack
/// stays, for the join to name it as written.
fn refsOnlyProbe(
    arena: std.mem.Allocator,
    e: *const ast.Expr,
    right: []const []const u8,
    own: Own,
    j: ast.Join,
) !bool {
    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    names: for (list.items) |q| {
        if (q.dollar) continue;
        if (q.parts.len > 1) {
            for (q.parts[0 .. q.parts.len - 1]) |rel| {
                if (std.mem.eql(u8, rel, j.binding) or std.mem.eql(u8, rel, j.alias)) return false;
            }
            continue;
        }
        if (q.sides.len == 0 and ast.nameIn(own.written, q.last())) {
            if (ast.nameIn(own.has, q.last())) continue :names;
            return false;
        }
        if (isRightName(q.last(), right)) return false;
    }
    return true;
}

const QualWalk = struct { arena: std.mem.Allocator, list: *std.array_list.Managed(ast.QualName) };

fn collectQualsRecur(cx: QualWalk, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        try cx.list.append(e.field);
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, collectQualsRecur);
}

pub fn collectQuals(arena: std.mem.Allocator, e: *const ast.Expr, list: *std.array_list.Managed(ast.QualName)) !void {
    _ = try collectQualsRecur(.{ .arena = arena, .list = list }, e);
}

const KeySwap = struct {
    arena: std.mem.Allocator,
    map: *const std.StringHashMap([]const u8),
};

fn swapKeysRecur(cx: KeySwap, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        {
            const nm = e.field.last();
            if (cx.map.get(nm)) |left| {
                const parts = try cx.arena.alloc([]const u8, 1);
                parts[0] = left;
                return mkExpr(cx.arena, .{ .field = .{ .parts = parts } });
            }
        }
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, swapKeysRecur);
}

pub fn mkExpr(arena: std.mem.Allocator, e: ast.Expr) !*ast.Expr {
    const p = try arena.create(ast.Expr);
    p.* = e;
    return p;
}

/// A filter over the right side's join keys alone, restated over the left keys so
/// it runs before an inner join: only references written with the right side's
/// name (`c.id`) qualify, since a bare or other-qualified name may be a left
/// column that shares a key's name.
fn deriveProbePredicate(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    e: *const ast.Expr,
    j: ast.Join,
) !?*ast.Expr {
    if (j.kind != .inner) return null;
    if (j.left_keys.len == 0 or j.left_keys.len != j.right_keys.len) return null;

    var map = std.StringHashMap([]const u8).init(gpa);
    defer map.deinit();
    for (j.right_keys, j.left_keys) |r, l| try map.put(r.last(), l.last());

    const side = if (j.alias.len > 0) j.alias else j.binding;
    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    if (list.items.len == 0) return null;
    for (list.items) |q| {
        if (q.parts.len != 2 or !std.mem.eql(u8, q.parts[0], side)) return null;
        if (map.get(q.last()) == null) return null;
    }
    return try swapKeysRecur(.{ .arena = arena, .map = &map }, e);
}

pub fn hoistThroughJoins(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    stages: []const ast.Stage,
    bindings: *const std.StringHashMap(ast.Pipeline),
) !?[]const ast.Stage {
    if (stages.len < 3) return null;
    var list = std.array_list.Managed(ast.Stage).init(arena);
    try list.appendSlice(stages);
    var changed = false;

    var rounds: usize = 0;
    while (rounds < list.items.len) : (rounds += 1) {
        var moved_this_round = false;
        var i: usize = 1;
        while (i < list.items.len) : (i += 1) {
            if (list.items[i].node != .filter) continue;
            if (list.items[i - 1].node != .join) continue;
            const j = list.items[i - 1].node.join;
            if (!hoistableKind(j.kind)) continue;
            const right: []const []const u8 = if (j.kind == .semi or j.kind == .anti)
                &.{}
            else
                (try bindingNames(arena, bindings, j.binding)) orelse continue;
            const own = try fromTableCols(arena, bindings, list.items[0..i]);
            const f = list.items[i];
            var parts = std.array_list.Managed(*ast.Expr).init(arena);
            try splitAnd(f.node.filter, &parts);
            var below: ?*ast.Expr = null;
            var above: ?*ast.Expr = null;
            for (parts.items) |c| {
                if (!ast.hasSearch(c) and !mayRaise(c) and try refsOnlyProbe(arena, c, right, own, j)) {
                    below = try andWith(arena, below, c);
                } else above = try andWith(arena, above, c);
            }
            const moved = below orelse continue;
            const join = list.items[i - 1];
            list.items[i - 1] = .{ .node = .{ .filter = moved }, .hints = f.hints, .pos = f.pos };
            list.items[i] = join;
            if (above) |rest| try list.insert(i + 1, .{ .node = .{ .filter = rest }, .hints = f.hints, .pos = f.pos });
            moved_this_round = true;
            changed = true;
        }
        if (!moved_this_round) break;
    }

    var k: usize = 0;
    while (k + 1 < list.items.len) : (k += 1) {
        if (list.items[k].node != .join) continue;
        const j = list.items[k].node.join;
        var m = k + 1;
        var inserted: usize = 0;
        while (m < list.items.len and list.items[m].node == .filter) : (m += 1) {
            if (ast.hasSearch(list.items[m].node.filter) or mayRaise(list.items[m].node.filter)) continue;
            const derived = (try deriveProbePredicate(arena, gpa, list.items[m].node.filter, j)) orelse continue;
            try list.insert(k, .{ .node = .{ .filter = derived }, .hints = &.{}, .pos = list.items[m].pos });
            inserted += 1;
            m += 1;
            changed = true;
        }
        k += inserted;
    }

    if (!changed) return null;
    return try list.toOwnedSlice();
}

pub fn hoistFilters(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    stages: []const ast.Stage,
    bindings: *const std.StringHashMap(ast.Pipeline),
) !?[]const ast.Stage {
    var cur = stages;
    var changed = false;
    if (try hoistThroughSelects(arena, cur)) |s| {
        cur = s;
        changed = true;
    }
    if (try hoistThroughJoins(arena, gpa, cur, bindings)) |s| {
        cur = s;
        changed = true;
        if (try hoistThroughSelects(arena, cur)) |t| cur = t;
    }
    if (try pushIntoJoinSides(arena, cur)) |s| {
        cur = s;
        changed = true;
    }
    return if (changed) cur else null;
}

pub fn pushIntoJoinSides(arena: std.mem.Allocator, stages: []const ast.Stage) !?[]const ast.Stage {
    var list = std.array_list.Managed(ast.Stage).init(arena);
    try list.appendSlice(stages);
    var changed = false;
    var i: usize = 1;
    while (i < list.items.len) {
        if (list.items[i].node != .filter) {
            i += 1;
            continue;
        }
        var parts = std.array_list.Managed(*ast.Expr).init(arena);
        try splitAnd(list.items[i].node.filter, &parts);
        var stay: ?*ast.Expr = null;
        var moved = false;
        for (parts.items) |c| {
            const found = if (ast.hasSearch(c) or mayRaise(c)) null else try rightAliasOf(arena, c);
            const target = found orelse {
                stay = try andWith(arena, stay, c);
                continue;
            };
            var k = i;
            const at: ?usize = while (k > 0) {
                k -= 1;
                switch (list.items[k].node) {
                    .filter => {},
                    .join => |j| {
                        if (std.mem.eql(u8, j.alias, target) or std.mem.eql(u8, j.binding, target)) {
                            if (j.kind != .inner and j.kind != .cross and j.kind != .right) break null;
                            break k;
                        }
                        if (!hoistableKind(j.kind)) break null;
                    },
                    else => break null,
                }
            } else null;
            const jk = at orelse {
                stay = try andWith(arena, stay, c);
                continue;
            };
            var j = list.items[jk].node.join;
            j.right_filter = try andWith(arena, j.right_filter, try unqualify(arena, c, target));
            list.items[jk].node = .{ .join = j };
            moved = true;
        }
        if (!moved) {
            i += 1;
            continue;
        }
        changed = true;
        if (stay) |rest| {
            list.items[i].node = .{ .filter = rest };
            i += 1;
        } else _ = list.orderedRemove(i);
    }
    if (!changed) return null;
    return try list.toOwnedSlice();
}

fn rightAliasOf(arena: std.mem.Allocator, e: *const ast.Expr) !?[]const u8 {
    var refs = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &refs);
    var alias: ?[]const u8 = null;
    for (refs.items) |q| {
        if (q.dollar) continue;
        if (q.parts.len != 2) return null;
        if (alias) |a| {
            if (!std.mem.eql(u8, a, q.parts[0])) return null;
        } else alias = q.parts[0];
    }
    return alias;
}

const Unqualify = struct { arena: std.mem.Allocator, alias: []const u8 };

fn unqualifyRecur(cx: Unqualify, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        const q = e.field;
        if (!q.dollar and q.parts.len == 2 and std.mem.eql(u8, q.parts[0], cx.alias))
            return mkExpr(cx.arena, .{ .field = try qual(cx.arena, q.parts[1..]) });
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, unqualifyRecur);
}

fn unqualify(arena: std.mem.Allocator, e: *ast.Expr, alias: []const u8) !*ast.Expr {
    return unqualifyRecur(.{ .arena = arena, .alias = alias }, e);
}

pub fn hoistThroughSelects(arena: std.mem.Allocator, stages: []const ast.Stage) !?[]const ast.Stage {
    if (stages.len < 3) return null;
    var list = std.array_list.Managed(ast.Stage).init(arena);
    try list.appendSlice(stages);
    var changed = false;
    var rounds: usize = 0;
    while (rounds < list.items.len) : (rounds += 1) {
        var moved_this_round = false;
        var i: usize = 1;
        while (i < list.items.len) : (i += 1) {
            if (list.items[i].node != .filter or list.items[i - 1].node != .select) continue;
            const f = list.items[i];
            const items = list.items[i - 1].node.select;
            var parts = std.array_list.Managed(*ast.Expr).init(arena);
            try splitAnd(f.node.filter, &parts);
            var below: ?*ast.Expr = null;
            var above: ?*ast.Expr = null;
            for (parts.items) |c| {
                if (ast.hasSearch(c)) {
                    above = try andWith(arena, above, c);
                } else if (try filterBelowSelect(arena, c, items)) |m| {
                    below = try andWith(arena, below, m);
                } else above = try andWith(arena, above, c);
            }
            const moved = below orelse continue;
            const sel = list.items[i - 1];
            list.items[i - 1] = .{ .node = .{ .filter = moved }, .hints = f.hints, .pos = f.pos };
            list.items[i] = sel;
            if (above) |rest| try list.insert(i + 1, .{ .node = .{ .filter = rest }, .hints = f.hints, .pos = f.pos });
            moved_this_round = true;
            changed = true;
        }
        if (!moved_this_round) break;
    }
    if (!changed) return null;
    return try list.toOwnedSlice();
}

fn splitAnd(e: *ast.Expr, out: *std.array_list.Managed(*ast.Expr)) !void {
    if (e.* == .binary and e.binary.op == .@"and") {
        try splitAnd(e.binary.l, out);
        try splitAnd(e.binary.r, out);
    } else try out.append(e);
}

fn andWith(arena: std.mem.Allocator, acc: ?*ast.Expr, e: *ast.Expr) !*ast.Expr {
    const l = acc orelse return e;
    return mkExpr(arena, .{ .binary = .{ .op = .@"and", .l = l, .r = e } });
}

/// The input column a projection outputs as `name`, or null when it computes it,
/// drops it, or outputs two columns of that name (a reference to it is ambiguous).
fn projectedFrom(arena: std.mem.Allocator, items: []const ast.SelectItem, name: []const u8) !?ast.QualName {
    var named: usize = 0;
    var all = false;
    for (items) |it| switch (it) {
        .field => |q| if (std.mem.eql(u8, q.last(), name)) {
            named += 1;
        },
        .computed => |c| if (std.mem.eql(u8, c.name, name)) {
            named += 1;
        },
        .star => all = true,
        .star_except => |names| all = all or for (names) |x| {
            if (std.mem.eql(u8, x, name)) break false;
        } else true,
        else => {},
    };
    if (named > 1 or (named == 1 and all)) return null;
    var star = false;
    for (items) |it| switch (it) {
        .star => star = true,
        .star_except => |names| {
            for (names) |x| if (std.mem.eql(u8, x, name)) return null;
            star = true;
        },
        .star_rename => |rs| {
            for (rs) |r| {
                if (std.mem.eql(u8, r.to, name)) return try qual(arena, &.{r.from});
                if (std.mem.eql(u8, r.from, name)) return null;
            }
            star = true;
        },
        .star_of => return null,
        .field => |q| if (std.mem.eql(u8, q.last(), name)) return q,
        .computed => |c| if (std.mem.eql(u8, c.name, name)) {
            if (c.expr.* == .field and !c.expr.field.dollar) return c.expr.field;
            return null;
        },
    };
    return if (star) try qual(arena, &.{name}) else null;
}

const ColSwap = struct {
    arena: std.mem.Allocator,
    map: *const std.StringHashMap(ast.QualName),
};

fn swapColsRecur(cx: ColSwap, e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
    if (e.* == .field) {
        if (!e.field.dollar and e.field.parts.len == 1)
            if (cx.map.get(e.field.parts[0])) |src| return mkExpr(cx.arena, .{ .field = src });
        return @constCast(e);
    }
    return ast.rebuildExpr(cx.arena, e, cx, swapColsRecur);
}

fn filterBelowSelect(arena: std.mem.Allocator, pred: *const ast.Expr, items: []const ast.SelectItem) !?*ast.Expr {
    for (items) |it| switch (it) {
        .field => |q| if (std.mem.indexOf(u8, q.last(), "${") != null) return null,
        .computed => |c| if (std.mem.indexOf(u8, c.name, "${") != null) return null,
        .star_except => |names| for (names) |n| {
            if (std.mem.indexOf(u8, n, "${") != null) return null;
        },
        else => {},
    };
    var refs = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, pred, &refs);
    var map = std.StringHashMap(ast.QualName).init(arena);
    for (refs.items) |q| {
        if (q.dollar) continue;
        if (q.parts.len != 1 or std.mem.indexOf(u8, q.parts[0], "${") != null) return null;
        const src = (try projectedFrom(arena, items, q.parts[0])) orelse return null;
        try map.put(q.parts[0], src);
    }
    return try swapColsRecur(.{ .arena = arena, .map = &map }, pred);
}

test "hoist: a filter on a renamed column moves below the projection, in the source's names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const items = [_]ast.SelectItem{
        .{ .computed = .{ .name = "num", .expr = try fld(a, "C5_NUM") } },
        .{ .computed = .{ .name = "dbl", .expr = try bin(a, .mul, try fld(a, "C5_VALOR"), try intLit(a, 2)) } },
    };
    const pred = try bin(a, .@"and", try bin(a, .gt, try fld(a, "dbl"), try intLit(a, 10)), try bin(a, .gt, try fld(a, "num"), try intLit(a, 5)));
    const stages = [_]ast.Stage{ readStage(), projStage(&items), filterStage(pred), writeStage() };

    const out = (try hoistThroughSelects(a, &stages)).?;
    try std.testing.expectEqual(@as(usize, 5), out.len);
    try std.testing.expect(out[1].node == .filter and out[2].node == .select and out[3].node == .filter);
    const where = (try serialWhere(a, .postgres, out[0 .. out.len - 1])).?;
    try std.testing.expectEqualStrings("(\"C5_NUM\" > 5)", where);
    try std.testing.expectEqualStrings("dbl", out[3].node.filter.binary.l.field.parts[0]);
}

test "hoist: a projection keeps a filter it computes, or names per row, and a qualifier it passes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const computed = [_]ast.SelectItem{.{ .computed = .{ .name = "y", .expr = try bin(a, .add, try fld(a, "x"), try intLit(a, 1)) } }};
    const s1 = [_]ast.Stage{ readStage(), projStage(&computed), filterStage(try bin(a, .eq, try fld(a, "y"), try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s1)) == null);
    const s2 = [_]ast.Stage{ readStage(), projStage(&computed), filterStage(try bin(a, .@"or", try bin(a, .eq, try fld(a, "y"), try intLit(a, 1)), try bin(a, .eq, try fld(a, "x"), try intLit(a, 2)))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s2)) == null);
    const dynamic = [_]ast.SelectItem{.{ .field = try qual(a, &.{"${col}"}) }};
    const s3 = [_]ast.Stage{ readStage(), projStage(&dynamic), filterStage(try bin(a, .eq, try fld(a, "k"), try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s3)) == null);
    const qualified = [_]ast.SelectItem{.{ .computed = .{ .name = "bx", .expr = try mkExpr(a, .{ .field = try qual(a, &.{ "b", "x" }) }) } }};
    const s4 = [_]ast.Stage{ readStage(), projStage(&qualified), filterStage(try bin(a, .eq, try fld(a, "bx"), try intLit(a, 1))), writeStage() };
    const out = (try hoistThroughSelects(a, &s4)).?;
    try std.testing.expectEqual(@as(usize, 2), out[1].node.filter.binary.l.field.parts.len);
    const star = [_]ast.SelectItem{ .star, .{ .computed = .{ .name = "y", .expr = try bin(a, .add, try fld(a, "x"), try intLit(a, 1)) } } };
    const s5 = [_]ast.Stage{ readStage(), projStage(&star), filterStage(try bin(a, .eq, try fld(a, "k"), try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughSelects(a, &s5)).?[1].node == .filter);
}

test "push: a filter on an inner join's right side by its alias becomes the join's, in the right side's names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const stages = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .@"and", try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0)), try bin(a, .gt, try fld(a, "x"), try intLit(a, 1)))), writeStage() };
    const out = (try pushIntoJoinSides(a, &stages)).?;
    const rf = out[1].node.join.right_filter.?;
    try std.testing.expectEqual(@as(usize, 1), rf.binary.l.field.parts.len);
    try std.testing.expectEqualStrings("v", rf.binary.l.field.parts[0]);
    try std.testing.expectEqualStrings("x", out[2].node.filter.binary.l.field.parts[0]);
    const whole = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0))), writeStage() };
    try std.testing.expectEqual(@as(usize, 3), (try pushIntoJoinSides(a, &whole)).?.len);
    const two = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try joinStage(a, .inner, "s", "k", "sk"), filterStage(try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0))), writeStage() };
    const o2 = (try pushIntoJoinSides(a, &two)).?;
    try std.testing.expect(o2[1].node.join.right_filter != null and o2[2].node.join.right_filter == null);
}

test "push: a filter stays after a LEFT or FULL join, across sides, on a bare name, or past a RIGHT join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0));
    for ([_]ast.JoinKind{ .left, .full }) |kind| {
        const st = [_]ast.Stage{ readStage(), try joinStage(a, kind, "r", "k", "rk"), filterStage(rv), writeStage() };
        try std.testing.expect((try pushIntoJoinSides(a, &st)) == null);
    }
    const across = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .@"or", rv, try bin(a, .eq, try fld(a, "x"), try intLit(a, 1)))), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &across)) == null);
    const bare = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .gt, try fld(a, "v"), try intLit(a, 0))), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &bare)) == null);
    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const past = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try joinStage(a, kind, "s", "k", "sk"), filterStage(rv), writeStage() };
        try std.testing.expect((try pushIntoJoinSides(a, &past)) == null);
    }
}

test "push: a filter on a RIGHT join's preserved side goes into it, and past a LEFT, semi or anti join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0));
    const right = [_]ast.Stage{ readStage(), try joinStage(a, .right, "r", "k", "rk"), filterStage(rv), writeStage() };
    const o1 = (try pushIntoJoinSides(a, &right)).?;
    try std.testing.expectEqual(@as(usize, 3), o1.len);
    try std.testing.expectEqualStrings("v", o1[1].node.join.right_filter.?.binary.l.field.parts[0]);
    for ([_]ast.JoinKind{ .left, .semi, .anti }) |kind| {
        const past = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try joinStage(a, kind, "s", "k", "sk"), filterStage(rv), writeStage() };
        const o2 = (try pushIntoJoinSides(a, &past)).?;
        try std.testing.expect(o2[1].node.join.right_filter != null and o2[2].node.join.right_filter == null);
    }
}

test "hoist: a WHERE splits at its ANDs; the left's conjuncts go below the join, the rest stay" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();
    const left = try bin(a, .gt, try fld(a, "v"), try intLit(a, 1));
    const right = try bin(a, .eq, try fld(a, "name"), try intLit(a, 2));
    const across = try bin(a, .@"or", try bin(a, .gt, try fld(a, "w"), try intLit(a, 3)), try bin(a, .eq, try fld(a, "rk"), try intLit(a, 4)));
    const pred = try bin(a, .@"and", try bin(a, .@"and", left, right), across);
    const stages = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(pred), writeStage() };
    const out = (try hoistThroughJoins(a, a, &stages, &binds)).?;
    try std.testing.expectEqual(@as(usize, 5), out.len);
    try std.testing.expect(out[1].node == .filter and out[2].node == .join and out[3].node == .filter);
    try std.testing.expectEqual(left, out[1].node.filter);
    try std.testing.expectEqual(right, out[3].node.filter.binary.l);
    try std.testing.expectEqual(across, out[3].node.filter.binary.r);
}

test "hoist: a conjunct that may fail on a row stays above the join, beside one that moves" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();
    const plain = try bin(a, .gt, try fld(a, "v"), try intLit(a, 1));
    const cast = try bin(a, .gt, try mkExpr(a, .{ .cast = .{ .e = try fld(a, "t"), .ty = .{ .kind = .int } } }), try intLit(a, 1));
    const div = try bin(a, .gt, try bin(a, .div, try intLit(a, 10), try fld(a, "v")), try intLit(a, 1));
    const pred = try bin(a, .@"and", try bin(a, .@"and", plain, cast), div);
    const stages = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(pred), writeStage() };
    const out = (try hoistThroughJoins(a, a, &stages, &binds)).?;
    try std.testing.expectEqual(plain, out[1].node.filter);
    try std.testing.expectEqual(cast, out[3].node.filter.binary.l);
    try std.testing.expectEqual(div, out[3].node.filter.binary.r);

    const rcast = try bin(a, .gt, try mkExpr(a, .{ .cast = .{ .e = try qfld(a, &.{ "r", "t" }), .ty = .{ .kind = .int } } }), try intLit(a, 1));
    const right = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(rcast), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &right)) == null);
    const lit = try bin(a, .gt, try fld(a, "d"), try mkExpr(a, .{ .cast = .{ .e = try mkExpr(a, .{ .str_lit = "2024-01-01" }), .ty = .{ .kind = .date } } }));
    try std.testing.expect(!mayRaise(lit));
}

test "hoist: a filter after a semi or anti join moves below it, whatever the right side's names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = std.StringHashMap(ast.Pipeline).init(a);
    defer binds.deinit();
    for ([_]ast.JoinKind{ .semi, .anti }) |kind| {
        const stages = [_]ast.Stage{ readStage(), try joinStage(a, kind, "unknown", "k", "rk"), try eqFilter(a, &.{"rk"}, 1), writeStage() };
        const out = (try hoistThroughJoins(a, a, &stages, &binds)).?;
        try std.testing.expect(out[1].node == .filter and out[2].node == .join);
    }
    const inner = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "unknown", "k", "rk"), try eqFilter(a, &.{"v"}, 1), writeStage() };
    try std.testing.expect((try hoistThroughJoins(a, a, &inner, &binds)) == null);
}

test "hoist: a name written with the FROM table's alias goes below though the right side has it; a bare one stays" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "fact.csv", .data = "k,name\n1,x\n" });
    const fact: ast.Stage = .{ .node = .{ .read = .{ .connector = "csv", .form = .{ .path = try tmp.dir.realpathAlloc(a, "fact.csv") } } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    var j = try joinStage(a, .inner, "r", "k", "rk");
    j.node.join.own_cols = &.{ try qual(a, &.{"name"}), try qual(a, &.{"zz"}) };
    const own = [_]ast.Stage{ fact, j, try eqFilter(a, &.{"name"}, 1), writeStage() };
    const out = (try hoistThroughJoins(a, a, &own, &binds)).?;
    try std.testing.expect(out[1].node == .filter and out[2].node == .join);

    const lacking = [_]ast.Stage{ fact, j, try eqFilter(a, &.{"zz"}, 1), writeStage() };
    try std.testing.expect((try hoistThroughJoins(a, a, &lacking, &binds)) == null);
    const unknown = [_]ast.Stage{ readStage(), j, try eqFilter(a, &.{"name"}, 1), writeStage() };
    try std.testing.expect((try hoistThroughJoins(a, a, &unknown, &binds)) == null);

    var bare_q = try qual(a, &.{"name"});
    bare_q.sides = &.{"r"};
    const bare_e = try mkExpr(a, .{ .field = bare_q });
    const bare = [_]ast.Stage{ fact, j, filterStage(try bin(a, .eq, bare_e, try intLit(a, 1))), writeStage() };
    try std.testing.expect((try hoistThroughJoins(a, a, &bare, &binds)) == null);
}

test "hoist: a right side read from a local file is asked its names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "dim.csv", .data = "rk,name\n1,x\n" });
    const path = try tmp.dir.realpathAlloc(a, "dim.csv");
    var binds = std.StringHashMap(ast.Pipeline).init(a);
    defer binds.deinit();
    const rs = try a.alloc(ast.Stage, 1);
    rs[0] = .{ .node = .{ .read = .{ .connector = "csv", .form = .{ .path = path } } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try binds.put("r", .{ .stages = rs, .pos = .{ .line = 0, .col = 0 } });

    const left = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try eqFilter(a, &.{"v"}, 1), writeStage() };
    try std.testing.expect((try hoistThroughJoins(a, a, &left, &binds)).?[1].node == .filter);
    const right = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try eqFilter(a, &.{"name"}, 1), writeStage() };
    try std.testing.expect((try hoistThroughJoins(a, a, &right, &binds)) == null);
}

test "hoist: a probe-only filter moves below an inner join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{"v"}, 1);
    stages[3] = writeStage();

    const out = (try hoistThroughJoins(a, a, stages, &binds)).?;
    try std.testing.expectEqual(@as(usize, 4), out.len);
    try std.testing.expect(out[1].node == .filter);
    try std.testing.expect(out[2].node == .join);

    const d: Dialect = .postgres;
    const where = (try serialWhere(a, d, out[0 .. out.len - 1])).?;
    try std.testing.expectEqualStrings("(\"v\" = 1)", where);
}

test "hoist: a filter naming a right-side column stays put" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    for ([_][]const []const u8{
        &.{"name"},
        &.{ "r", "name" },
        &.{"name_r"},
    }) |parts| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, .inner, "r", "k", "rk");
        stages[2] = try eqFilter(a, parts, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }
}

test "hoist: refused for the join kinds that null-extend the probe side" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{"v"}, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }
    for ([_]ast.JoinKind{ .inner, .left, .semi, .anti, .cross }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{"v"}, 1);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) != null);
    }
}

test "hoist: a binding with an open name set is left alone" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var binds = std.StringHashMap(ast.Pipeline).init(a);
    defer binds.deinit();
    const items = try a.alloc(ast.SelectItem, 1);
    items[0] = .star;
    const bs = try a.alloc(ast.Stage, 2);
    bs[0] = readStage();
    bs[1] = .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try binds.put("r", .{ .stages = bs, .pos = .{ .line = 0, .col = 0 } });

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{"v"}, 1);
    stages[3] = writeStage();
    try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
}

test "derive: a predicate on the join key gains a probe-side twin" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{ "r", "rk" }, 7);
    stages[3] = writeStage();

    const out = (try hoistThroughJoins(a, a, stages, &binds)).?;
    try std.testing.expectEqual(@as(usize, 5), out.len);
    try std.testing.expect(out[1].node == .filter);
    try std.testing.expect(out[2].node == .join);
    try std.testing.expect(out[3].node == .filter);

    const where = (try serialWhere(a, .postgres, out[0..2])).?;
    try std.testing.expectEqualStrings("(\"k\" = 7)", where);
}

test "derive: only for an inner join, and only when every ref is a right-side key written with its name" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var binds = try dimBindings(a);
    defer binds.deinit();

    for ([_]ast.JoinKind{ .left, .anti, .semi, .right, .full, .cross }) |kind| {
        const stages = try a.alloc(ast.Stage, 4);
        stages[0] = readStage();
        stages[1] = try joinStage(a, kind, "r", "k", "rk");
        stages[2] = try eqFilter(a, &.{ "r", "rk" }, 7);
        stages[3] = writeStage();
        try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);
    }

    const stages = try a.alloc(ast.Stage, 4);
    stages[0] = readStage();
    stages[1] = try joinStage(a, .inner, "r", "k", "rk");
    stages[2] = try eqFilter(a, &.{ "r", "name" }, 7);
    stages[3] = writeStage();
    try std.testing.expect((try hoistThroughJoins(a, a, stages, &binds)) == null);

    const j = (try joinStage(a, .inner, "r", "k", "rk")).node.join;
    try std.testing.expect((try deriveProbePredicate(a, a, (try eqFilter(a, &.{ "r", "rk" }, 7)).node.filter, j)) != null);
    for ([_][]const []const u8{ &.{ "l", "rk" }, &.{"rk"} }) |ref| {
        try std.testing.expect((try deriveProbePredicate(a, a, (try eqFilter(a, ref, 7)).node.filter, j)) == null);
    }
}
