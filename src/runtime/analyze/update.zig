//! What an `UPDATE` may be and, on a database, the SQL it sends. Its stages past the
//! table read are conditions on the table, the FROM source's join (an inner join
//! keyed on the WHERE's equalities between the source's columns and the table's,
//! conditions naming both sides after it), and IN / EXISTS subqueries as semi and
//! anti joins. On a database the conditions and the SET expressions are sent as
//! written (`pushdown/translate`, `need = .source`), the table's columns named bare
//! and the source's by their staged names (`staged`), so neither shadows the other.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const sql = @import("../../db/sql.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const translate = @import("../pushdown/translate.zig");
const print = @import("../../lang/print.zig");
const analyze = @import("../analyze.zig");

const ParamMap = std.StringHashMap(*const ast.Expr);

/// The prefix a FROM source's column takes in the run's staging table.
pub const staged_prefix = "_basalt_";

/// The parts of an UPDATE's stages: the FROM source's join, the conditions before
/// and after it, and the subqueries.
pub const Shape = struct {
    from: ?ast.Join = null,
    pre: []const *ast.Expr,
    post: []const *ast.Expr,
    members: []const ast.Join,
};

/// The stages between an UPDATE's table read and its write sorted into a `Shape`,
/// or why one statement cannot carry them.
pub fn shapeOf(arena: std.mem.Allocator, u: ast.Update, stages: []const ast.Stage, why: *[]const u8) !?Shape {
    var pre = std.array_list.Managed(*ast.Expr).init(arena);
    var post = std.array_list.Managed(*ast.Expr).init(arena);
    var members = std.array_list.Managed(ast.Join).init(arena);
    var from: ?ast.Join = null;
    for (stages) |st| switch (st.node) {
        .filter => |f| try (if (from == null) &pre else &post).append(f),
        .select => |items| for (items) |it| if (it != .star) {
            why.* = "`UPDATE` takes an IN whose left side is a column of the table, not an expression";
            return null;
        },
        .join => |j| {
            if (u.from != null and from == null and j.kind == .inner) {
                if (j.left_keys.len == 0 or j.left_keys.len != j.right_keys.len or j.deferred.len > 0) {
                    why.* = "`UPDATE ... FROM` matches the source's rows to the table's by equalities between their columns, `WHERE t.id = s.id`";
                    return null;
                }
                if (j.residual) |r| try post.append(r);
                from = j;
                continue;
            }
            if (j.kind != .semi and j.kind != .anti) {
                why.* = "`UPDATE` takes conditions on the table's columns, a FROM source and IN / EXISTS subqueries, not a scalar subquery compared in the WHERE";
                return null;
            }
            if (j.residual != null or j.deferred.len > 0 or j.left_keys.len == 0 or j.left_keys.len != j.right_keys.len or (j.null_aware and j.left_keys.len != 1)) {
                why.* = "`UPDATE` takes IN / EXISTS subqueries correlated by equalities between their columns and the table's, not by other conditions";
                return null;
            }
            try members.append(j);
        },
        else => {
            why.* = "`UPDATE` takes conditions on the table's columns, a FROM source and IN / EXISTS subqueries";
            return null;
        },
    };
    return .{ .from = from, .pre = pre.items, .post = post.items, .members = members.items };
}

/// The column of `schema` an UPDATE's SET names, matched without regard to case;
/// null when the table has none of that name.
pub fn column(schema: types.Schema, name: []const u8) ?[]const u8 {
    for (schema.fields) |f| if (std.ascii.eqlIgnoreCase(f.name, name)) return f.name;
    return null;
}

/// An UPDATE's SQL parts in dialect `d`: each `"col" = value`, the conditions, and
/// the FROM source's columns the statement reads (its staged ones), its keys first.
pub const Sql = struct {
    sets: []const []const u8,
    conds: []const []const u8,
    keys: []const []const u8,
    src_cols: []const []const u8,
};

pub const Built = union(enum) { sql: Sql, unsendable: *const ast.Expr, unbound: []const u8 };

/// The SQL of an UPDATE of `table` (alias `alias`) with shape `sh`, its `$` values
/// bound from `params`; or the first expression that cannot be sent as written, or a
/// `$` `params` does not bind. `offline` skips an expression reading a value the
/// analysis holds a NULL in place of.
pub fn build(arena: std.mem.Allocator, u: ast.Update, table: []const u8, sh: Shape, d: sql.Dialect, params: *const ParamMap, offline: bool) !Built {
    var src = std.array_list.Managed([]const u8).init(arena);
    var cx = Cx{ .arena = arena, .alias = u.alias, .table = table, .from = u.from, .src = &src };
    var keys = std.array_list.Managed([]const u8).init(arena);
    if (sh.from) |j| for (j.left_keys, j.right_keys) |lk, rk| {
        try cx.use(rk.last());
        try keys.append(try msgfmt.allocPrint(arena, "{s} = {s}", .{ try sql.quoteIdent(arena, d, lk.last()), try sql.quoteIdent(arena, d, try stagedName(arena, rk.last())) }));
    };
    var conds = std.array_list.Managed([]const u8).init(arena);
    try conds.appendSlice(keys.items);
    for ([_][]const *ast.Expr{ sh.pre, sh.post }) |list| for (list) |e| {
        switch (try one(&cx, e, d, params, offline, .pred)) {
            .text => |t| try conds.append(t),
            .skip => {},
            .unsendable => return .{ .unsendable = e },
            .unbound => |n| return .{ .unbound = n },
        }
    };
    var sets = std.array_list.Managed([]const u8).init(arena);
    for (u.sets) |s| switch (try one(&cx, s.expr, d, params, offline, .value)) {
        .text => |t| try sets.append(try msgfmt.allocPrint(arena, "{s} = {s}", .{ try sql.quoteIdent(arena, d, s.column), t })),
        .skip => {},
        .unsendable => return .{ .unsendable = s.expr },
        .unbound => |n| return .{ .unbound = n },
    };
    return .{ .sql = .{ .sets = sets.items, .conds = conds.items, .keys = keys.items, .src_cols = src.items } };
}

/// The name a FROM source's column `col` takes in the staging table.
pub fn stagedName(arena: std.mem.Allocator, col: []const u8) ![]const u8 {
    return std.mem.concat(arena, u8, &.{ staged_prefix, col });
}

/// The text of `e` as a message names it.
pub fn exprText(arena: std.mem.Allocator, e: *const ast.Expr) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    print.expr(&aw.writer, e) catch return error.OutOfMemory;
    return aw.written();
}

const One = union(enum) { text: []const u8, skip, unsendable, unbound: []const u8 };

fn one(cx: *Cx, e: *const ast.Expr, d: sql.Dialect, params: *const ParamMap, offline: bool, as: enum { pred, value }) !One {
    if (offline and try analyze.readsRunValue(cx.arena, e, params)) return .skip;
    const bound = try analyze.substExpr(cx.arena, e, params);
    const refs = try analyze.dollarRefs(cx.arena, bound);
    if (refs.len > 0) return if (offline) .skip else .{ .unbound = refs[0].last() };
    const bare = (try cx.rename(bound)) orelse return .unsendable;
    const text = switch (as) {
        .pred => try translate.translatePred(cx.arena, bare, d, .{ .need = .source }),
        .value => try translate.translateValue(cx.arena, bare, d, .{ .need = .source }),
    };
    return if (text) |t| .{ .text = t } else .unsendable;
}

/// Renames an expression's columns for the statement: the table's bare, the FROM
/// source's by their staged names, noting each it reads.
const Cx = struct {
    arena: std.mem.Allocator,
    alias: []const u8,
    table: []const u8,
    from: ?[]const u8,
    src: *std.array_list.Managed([]const u8),
    bad: bool = false,

    fn use(self: *Cx, col: []const u8) !void {
        for (self.src.items) |c| if (std.mem.eql(u8, c, col)) return;
        try self.src.append(col);
    }

    fn rename(self: *Cx, e: *const ast.Expr) !?*ast.Expr {
        self.bad = false;
        const out = try recur(self, e);
        return if (self.bad) null else out;
    }

    fn recur(self: *Cx, node: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
        if (node.* == .field and !node.field.dollar and node.field.parts.len == 2) {
            const q = node.field.parts[0];
            const col = node.field.parts[1];
            const own = (self.alias.len > 0 and std.ascii.eqlIgnoreCase(q, self.alias)) or std.ascii.eqlIgnoreCase(q, self.table);
            if (own) return ast.mk(self.arena, .{ .field = .{ .parts = node.field.parts[1..], .span = node.field.span } });
            if (self.from) |f| if (f.len > 0 and std.ascii.eqlIgnoreCase(q, f)) {
                try self.use(col);
                const name = try self.arena.alloc([]const u8, 1);
                name[0] = try stagedName(self.arena, col);
                return ast.mk(self.arena, .{ .field = .{ .parts = name, .span = node.field.span } });
            };
            self.bad = true;
        }
        return ast.rebuildExpr(self.arena, node, self, recur);
    }
};
