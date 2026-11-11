//! Vector similarity over LIST columns of numbers, as embeddings are stored:
//! `list_cosine_similarity`, `list_cosine_distance`, `list_distance` (Euclidean),
//! `list_inner_product` and `list_negative_inner_product`, named and behaving as
//! DuckDB's. Elements of any numeric type are taken as doubles; the result is a
//! DOUBLE. A NULL list gives NULL; a NULL element or two lists of different lengths
//! fail the row, naming the function. Two empty lists are 0 apart with a 0 inner
//! product. A cosine with an empty or all-zero side is NULL, not DuckDB's NaN,
//! which sorts above every number, so `ORDER BY s DESC NULLS LAST LIMIT k` keeps
//! it out of a search.
//!
//! One routine (`sums`) folds a pair of lists, in a fixed order of four-wide lanes,
//! for the row form and the kernel alike, so a row gives the same bits whichever
//! path computed it. The kernel takes either argument as a scalar, the query
//! vector of a similarity search.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;
const Column = column.Column;
const Type = @import("../../lang/types.zig").Type;
const Value = @import("../value.zig").Value;
const eval = @import("../eval.zig");
const EvalError = eval.EvalError;
const TypeCtx = eval.TypeCtx;
const TypeError = eval.TypeError;
const Vec = @import("vec.zig").Vec;
const VecError = @import("vec.zig").VecError;
const evalVec = @import("vec.zig").evalVec;
const evalRow = @import("row.zig").evalRow;
const support = @import("support.zig");
const eq = support.eq;

const Kind = enum { cosine_similarity, cosine_distance, distance, inner_product, negative_inner_product };

fn kindOf(name: []const u8) Kind {
    if (eq(name, "list_cosine_similarity")) return .cosine_similarity;
    if (eq(name, "list_cosine_distance")) return .cosine_distance;
    if (eq(name, "list_distance")) return .distance;
    if (eq(name, "list_inner_product")) return .inner_product;
    return .negative_inner_product;
}

pub fn typing(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
    if (c.args.len != 2) return self.err("`{s}` takes two lists of numbers", .{c.name});
    for (0..2) |i| {
        const t = try self.argType(c, i);
        if (t.unknown) continue;
        if (t.kind != .array or !column.childType(t).kind.isNumeric())
            return self.err("`{s}` takes two lists of numbers, got {s}", .{ c.name, t.name(self.arena) catch "?" });
    }
    return Type.init(.float).asNullable();
}

/// A list's elements: rows `start..end` of its child column, and the child's
/// doubles when it holds them plain and none is NULL (`dense`), read without a
/// check per element.
const Span = struct { child: Column, start: usize, end: usize, dense: ?[]const f64 = null };

fn spanOf(n: *const column.Nested, row: usize, dense: ?[]const f64) ?Span {
    if (n.body != .list) return null;
    const l = n.body.list;
    return .{ .child = l.child, .start = @intCast(l.offsets[row]), .end = @intCast(l.offsets[row + 1]), .dense = dense };
}

/// A list column's child as plain doubles with no NULL, checked once a batch.
fn denseOf(n: *const column.Nested) ?[]const f64 {
    if (n.body != .list) return null;
    const ch = n.body.list.child;
    if (ch.codes_only or ch.data != .f64 or !ch.validity.allSet(ch.len)) return null;
    return ch.data.f64;
}

/// The dot product and both sides' squared norms and squared difference.
const Sums = struct { dot: f64 = 0, aa: f64 = 0, bb: f64 = 0, dd: f64 = 0 };

const lanes = 4;

/// The four sums over a pair of equal-length spans, in lanes of four elements
/// folded in one fixed order.
fn sums(name: []const u8, a: Span, b: Span) EvalError!Sums {
    const n = a.end - a.start;
    if (b.end - b.start != n) return support.failWith(error.CastFailed, "{s}: the lists' lengths differ, {d} and {d}; they must be equal", .{ name, n, b.end - b.start });
    var dot: @Vector(lanes, f64) = @splat(0);
    var aa: @Vector(lanes, f64) = @splat(0);
    var bb: @Vector(lanes, f64) = @splat(0);
    var dd: @Vector(lanes, f64) = @splat(0);
    var i: usize = 0;
    while (i < n) : (i += lanes) {
        var x: @Vector(lanes, f64) = @splat(0);
        var y: @Vector(lanes, f64) = @splat(0);
        if (i + lanes <= n and a.dense != null and b.dense != null) {
            x = a.dense.?[a.start + i ..][0..lanes].*;
            y = b.dense.?[b.start + i ..][0..lanes].*;
        } else for (0..@min(lanes, n - i)) |k| {
            x[k] = try elem(name, a.child, a.start + i + k);
            y[k] = try elem(name, b.child, b.start + i + k);
        }
        dot += x * y;
        aa += x * x;
        bb += y * y;
        dd += (x - y) * (x - y);
    }
    return .{ .dot = fold(dot), .aa = fold(aa), .bb = fold(bb), .dd = fold(dd) };
}

fn fold(v: @Vector(lanes, f64)) f64 {
    return (v[0] + v[1]) + (v[2] + v[3]);
}

fn elem(name: []const u8, child: Column, i: usize) EvalError!f64 {
    if (!child.validity.get(i)) return support.failWith(error.CastFailed, "{s}: a list holds a NULL element; a vector's elements must all be numbers", .{name});
    if (!child.codes_only) switch (child.data) {
        .f64 => |d| return d[i],
        .i64 => |d| return @floatFromInt(d[i]),
        .i32 => |d| return @floatFromInt(d[i]),
        else => {},
    };
    return support.toF64(child.getValue(i));
}

/// The function's value over a pair of lists, null where it has none.
fn result(k: Kind, s: Sums) ?f64 {
    return switch (k) {
        .inner_product => s.dot,
        .negative_inner_product => 0 - s.dot,
        .distance => @sqrt(s.dd),
        .cosine_similarity, .cosine_distance => {
            if (s.aa == 0 or s.bb == 0) return null;
            const sim = s.dot / (@sqrt(s.aa) * @sqrt(s.bb));
            return if (k == .cosine_similarity) sim else 1 - sim;
        },
    };
}

pub fn rowFn(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
    const av = try evalRow(arena, c.args[0], batch, row);
    const bv = try evalRow(arena, c.args[1], batch, row);
    if (av != .nested or bv != .nested) return .null;
    const a = spanOf(av.nested.node, av.nested.row, null) orelse return .null;
    const b = spanOf(bv.nested.node, bv.nested.row, null) orelse return .null;
    const v = result(kindOf(c.name), try sums(c.name, a, b)) orelse return .null;
    return .{ .float = v };
}

/// One argument per row: a list column, or one list (or NULL) for every row.
const Arg = union(enum) {
    col: struct { col: Column, dense: ?[]const f64 },
    one: ?Span,

    fn of(v: Vec) VecError!Arg {
        return switch (v) {
            .col => |col| if (col.data == .nested and col.data.nested.body == .list) .{ .col = .{ .col = col, .dense = denseOf(col.data.nested) } } else error.Unsupported,
            .scalar => |s| if (s == .nested) .{ .one = spanOf(s.nested.node, s.nested.row, denseOf(s.nested.node)) orelse return error.Unsupported } else if (s.isNull()) .{ .one = null } else error.Unsupported,
        };
    }

    fn at(self: Arg, row: usize) ?Span {
        return switch (self) {
            .col => |c| if (c.col.validity.get(row)) spanOf(c.col.data.nested, row, c.dense) else null,
            .one => |s| s,
        };
    }
};

pub fn vecFn(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    const a = try Arg.of(try evalVec(arena, c.args[0], batch));
    const b = try Arg.of(try evalVec(arena, c.args[1], batch));
    const k = kindOf(c.name);
    const n = batch.len;
    const out = try arena.alloc(f64, n);
    var bm = try Bitmap.initFull(arena, n);
    for (out, 0..) |*o, row| {
        const v = if (a.at(row)) |x| (if (b.at(row)) |y| result(k, try sums(c.name, x, y)) else null) else null;
        o.* = v orelse 0;
        if (v == null) bm.setValid(row, false);
    }
    return .{ .col = Column.derived(Type.init(.float).asNullable(), n, bm, .{ .f64 = out }) };
}

test "a span read whole as doubles folds to the bits the element by element read gives" {
    var data: [11]f64 = undefined;
    var other: [11]f64 = undefined;
    for (&data, &other, 0..) |*x, *y, i| {
        x.* = @as(f64, @floatFromInt(i)) * 0.37 - 1.1;
        y.* = 2.3 / (@as(f64, @floatFromInt(i)) + 0.7);
    }
    const a = std.testing.allocator;
    const bm = try Bitmap.initFull(a, data.len);
    defer a.free(bm.bits);
    const ca = Column.derived(Type.init(.float), data.len, bm, .{ .f64 = &data });
    const cb = Column.derived(Type.init(.float), other.len, bm, .{ .f64 = &other });
    for ([_][2]usize{ .{ 0, 11 }, .{ 1, 10 }, .{ 3, 3 }, .{ 2, 9 } }) |r| {
        const slow = try sums("t", .{ .child = ca, .start = r[0], .end = r[1] }, .{ .child = cb, .start = r[0], .end = r[1] });
        const fast = try sums("t", .{ .child = ca, .start = r[0], .end = r[1], .dense = &data }, .{ .child = cb, .start = r[0], .end = r[1], .dense = &other });
        try std.testing.expectEqual(slow, fast);
    }
}

test "every vector function's kernel gives each row the bits its row form does, a column against a column or a constant" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const types = @import("../../lang/types.zig");
    var xs = column.Builder.init(a, Type.init(.string).asNullable());
    var ys = column.Builder.init(a, Type.init(.string).asNullable());
    const rows = [_][2]?[]const u8{
        .{ "[1.5,-2.25,3,0.125,9,4.75,1e-3]", "[0.5,0.5,-1,2,3.5,-6,7]" },
        .{ "[0.1,0.2,0.3,0.4,0.5]", "[0.5,0.4,0.3,0.2,0.1]" },
        .{ "[]", "[]" },
        .{ "[0,0,0]", "[1,2,3]" },
        .{ null, "[1,2]" },
        .{ "[3,4]", null },
        .{ "[1e300,2]", "[1e-300,3]" },
    };
    for (rows) |r| {
        try xs.append(if (r[0]) |s| .{ .string = s } else .null);
        try ys.append(if (r[1]) |s| .{ .string = s } else .null);
    }
    var cols = [_]Column{ try xs.finish(), try ys.finish() };
    const schema = types.Schema{ .fields = &.{ .{ .name = "x", .ty = cols[0].ty }, .{ .name = "y", .ty = cols[1].ty } } };
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = rows.len };
    const tu = @import("testing_util.zig");
    inline for (.{ "list_cosine_similarity", "list_cosine_distance", "list_distance", "list_inner_product", "list_negative_inner_product" }) |name| {
        try tu.expectVecMatchesRows(a, batch, &.{
            name ++ "(CAST(x AS LIST(DOUBLE)), CAST(y AS LIST(DOUBLE)))",
            name ++ "(CAST(x AS LIST(DOUBLE)), CAST(x AS LIST(DOUBLE)))",
            name ++ "(CAST(x AS LIST(INT)), CAST('[1,2]' AS LIST(DECIMAL(9,2))))",
        }, false);
    }
}
