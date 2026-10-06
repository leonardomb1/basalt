//! Filters moved toward the sources: below a join to the side whose columns they
//! name (with a twin on the other side's key), and below a renaming select.

const ast = @import("../../lang/ast.zig");
const qual = @import("../pushdown.zig").qual;
const std = @import("std");
pub const Dialect = @import("../pushdown.zig").Dialect;
pub const bin = @import("../pushdown.zig").bin;
const dimBindings = @import("testing_util.zig").dimBindings;
const eqFilter = @import("testing_util.zig").eqFilter;
const filterStage = @import("testing_util.zig").filterStage;
pub const fld = @import("../pushdown.zig").fld;
const intLit = @import("testing_util.zig").intLit;
const joinStage = @import("testing_util.zig").joinStage;
const projStage = @import("testing_util.zig").projStage;
pub const qfld = @import("../pushdown.zig").qfld;
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
    var i = pipe.stages.len;
    while (i > 0) {
        i -= 1;
        switch (pipe.stages[i].node) {
            .select => |items| {
                const out = try arena.alloc([]const u8, items.len);
                for (items, out) |it, *o| o.* = switch (it) {
                    .field => |q| q.last(),
                    .computed => |c| c.name,
                    else => return null,
                };
                return out;
            },
            .filter, .limit, .sort, .distinct => {},
            else => return null,
        }
    }
    return null;
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

fn refsOnlyProbe(
    arena: std.mem.Allocator,
    e: *const ast.Expr,
    right: []const []const u8,
    j: ast.Join,
) !bool {
    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    for (list.items) |q| {
        if (q.parts.len > 1 and (std.mem.eql(u8, q.parts[0], j.binding) or std.mem.eql(u8, q.parts[0], j.alias))) return false;
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

    var list = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &list);
    if (list.items.len == 0) return null;
    for (list.items) |q| {
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
            const right = (try bindingNames(arena, bindings, j.binding)) orelse continue;
            if (!try refsOnlyProbe(arena, list.items[i].node.filter, right, j)) continue;
            const tmp = list.items[i - 1];
            list.items[i - 1] = list.items[i];
            list.items[i] = tmp;
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
            const target = (try rightAliasOf(arena, c)) orelse {
                stay = try andWith(arena, stay, c);
                continue;
            };
            var k = i;
            const at: ?usize = while (k > 0) {
                k -= 1;
                switch (list.items[k].node) {
                    .filter => {},
                    .join => |j| {
                        if (j.kind != .inner and j.kind != .cross) break null;
                        if (std.mem.eql(u8, j.alias, target) or std.mem.eql(u8, j.binding, target)) break k;
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
                if (try filterBelowSelect(arena, c, items)) |m| {
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

fn projectedFrom(arena: std.mem.Allocator, items: []const ast.SelectItem, name: []const u8) !?ast.QualName {
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

test "push: a filter stays after a LEFT join, across sides, on a bare name, or past a LEFT join" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const rv = try bin(a, .gt, try qfld(a, &.{ "r", "v" }), try intLit(a, 0));
    const left = [_]ast.Stage{ readStage(), try joinStage(a, .left, "r", "k", "rk"), filterStage(rv), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &left)) == null);
    const across = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .@"or", rv, try bin(a, .eq, try fld(a, "x"), try intLit(a, 1)))), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &across)) == null);
    const bare = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), filterStage(try bin(a, .gt, try fld(a, "v"), try intLit(a, 0))), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &bare)) == null);
    const past = [_]ast.Stage{ readStage(), try joinStage(a, .inner, "r", "k", "rk"), try joinStage(a, .left, "s", "k", "sk"), filterStage(rv), writeStage() };
    try std.testing.expect((try pushIntoJoinSides(a, &past)) == null);
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

test "derive: only for an inner join, and only when every ref is a key" {
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
}
