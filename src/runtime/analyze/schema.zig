//! The schema each stage leaves: select lists, aggregates (and their result types),
//! windows, explodes and joins, with the errors a mistyped stage gets at check time.

const Diag = @import("../analyze.zig").Diag;
const eval = @import("../../exec/eval.zig");
const Error = @import("../analyze.zig").Error;
const ParamMap = std.StringHashMap(*const ast.Expr);
const aggregates = @import("../../lang/aggregates.zig");
const ast = @import("../../lang/ast.zig");
const exprType = @import("../analyze.zig").exprType;
const fail = @import("../analyze.zig").fail;
const failAt = @import("../analyze.zig").failAt;
const lastPart = @import("../analyze.zig").lastPart;
const mk = @import("../analyze.zig").mk;
const std = @import("std");
const substExpr = @import("../analyze.zig").substExpr;
const types = @import("../../lang/types.zig");
const tfld = @import("testing_util.zig").tfld;

pub const Col = struct {
    name: []const u8,
    ty: types.Type,
    source: union(enum) { passthrough: usize, expr: *const ast.Expr },
    rel: []const u8 = "",
    base: []const u8 = "",
};

/// `b.x` is called `x` in the output, as in SQL, unless `a.x` is already there.
pub fn selectCols(arena: std.mem.Allocator, in: types.Schema, items: []const ast.SelectItem, params: *const ParamMap, diag: *Diag) Error![]Col {
    var cols = std.array_list.Managed(Col).init(arena);
    for (items) |item| switch (item) {
        .star => for (in.fields, 0..) |f, idx| try cols.append(.{ .name = f.name, .ty = f.ty, .source = .{ .passthrough = idx }, .rel = f.rel, .base = f.base }),
        .star_except => |names| for (in.fields, 0..) |f, idx| {
            if (nameIn(names, f.name)) continue;
            try cols.append(.{ .name = f.name, .ty = f.ty, .source = .{ .passthrough = idx }, .rel = f.rel, .base = f.base });
        },
        .star_rename => |renames| {
            for (renames) |r| if (in.indexOf(r.from) == null)
                return fail(diag, "unknown rename field `{s}`", .{r.from});
            for (in.fields, 0..) |f, idx| {
                const nm = renameTo(renames, f.name) orelse f.name;
                for (in.fields[0..idx]) |g|
                    if (std.mem.eql(u8, nm, renameTo(renames, g.name) orelse g.name))
                        return fail(diag, "`* rename` produces duplicate column `{s}`", .{nm});
                try cols.append(.{ .name = nm, .ty = f.ty, .source = .{ .passthrough = idx }, .rel = f.rel, .base = f.base });
            }
        },
        .field => |q| {
            const idx = in.resolve(q.parts) orelse return failAt(diag, q.span, "unknown field `{s}`", .{lastPart(q)});
            var nm = lastPart(q);
            for (cols.items) |c| if (std.mem.eql(u8, c.name, nm)) {
                nm = in.fields[idx].name;
                break;
            };
            try cols.append(.{ .name = nm, .ty = in.fields[idx].ty, .source = .{ .passthrough = idx }, .rel = in.fields[idx].rel, .base = in.fields[idx].base });
        },
        .computed => |c| {
            const e = try substExpr(arena, c.expr, params);
            const ty = try exprType(arena, in, e, diag);
            try cols.append(.{ .name = c.name, .ty = ty, .source = .{ .expr = e } });
        },
    };
    return cols.toOwnedSlice();
}

pub fn schemaOfCols(arena: std.mem.Allocator, cols: []const Col) Error!types.Schema {
    const fields = try arena.alloc(types.Schema.Field, cols.len);
    for (cols, fields) |c, *f| f.* = .{ .name = c.name, .ty = c.ty, .rel = c.rel, .base = c.base };
    return .{ .fields = fields };
}

pub fn checkFilter(arena: std.mem.Allocator, in: types.Schema, pred0: *const ast.Expr, params: *const ParamMap, diag: *Diag) Error!*const ast.Expr {
    const pred = try substExpr(arena, pred0, params);
    const t = try exprType(arena, in, pred, diag);
    if (!(t.kind == .bool or t.unknown)) return fail(diag, "filter predicate must be bool", .{});
    return pred;
}

pub fn fieldIndices(arena: std.mem.Allocator, in: types.Schema, names: []const ast.QualName, diag: *Diag) Error![]usize {
    const idxs = try arena.alloc(usize, names.len);
    for (names, 0..) |q, i| idxs[i] = in.resolve(q.parts) orelse return failAt(diag, q.span, "unknown field `{s}`", .{lastPart(q)});
    return idxs;
}

pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, name: []const u8, distinct: bool = false };

pub const AggregatePlan = struct { by: []usize, aggs: []Agg, schema: types.Schema };

pub fn aggregatePlan(arena: std.mem.Allocator, in: types.Schema, ag: ast.Aggregate, params: *const ParamMap, diag: *Diag) Error!AggregatePlan {
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    const by = try arena.alloc(usize, ag.by.len);
    for (ag.by, 0..) |q, i| {
        const idx = in.resolve(q.parts) orelse return fail(diag, "unknown group field `{s}`", .{lastPart(q)});
        by[i] = idx;
        try fields.append(.{ .name = lastPart(q), .ty = in.fields[idx].ty, .rel = in.fields[idx].rel, .base = in.fields[idx].base });
    }
    const aggs = try arena.alloc(Agg, ag.aggs.len);
    for (ag.aggs, 0..) |item, i| {
        const arg: ?*const ast.Expr = if (item.arg) |a| try substExpr(arena, a, params) else null;
        const ty = try aggResultType(arena, item.func, arg, in, diag);
        aggs[i] = .{ .func = item.func, .arg = arg, .ty = ty, .name = item.name, .distinct = item.distinct };
        try fields.append(.{ .name = item.name, .ty = ty });
    }
    return .{ .by = by, .aggs = aggs, .schema = .{ .fields = try fields.toOwnedSlice() } };
}

/// Refuses `SUM('Kick-Off')`: single quotes make a string, which once summed to 0. A
/// decimal sum stays decimal; typed as int it once reported the unscaled integer.
pub fn aggResultType(arena: std.mem.Allocator, func: ast.AggFunc, arg: ?*const ast.Expr, in: types.Schema, diag: *Diag) Error!types.Type {
    const sp = aggregates.spec(func);
    if (sp.arg == .star_or_any) return types.Type.init(.int);
    const a = arg orelse return fail(diag, "this aggregate requires an argument", .{});
    if (sp.arg == .numeric and a.* == .str_lit)
        return fail(diag, "`{s}('{s}')` reads the text '{s}', not a column — a name with spaces or symbols takes double quotes: {s}(\"{s}\")", .{ sp.names[0], a.str_lit, a.str_lit, sp.names[0], a.str_lit });
    const at = try exprType(arena, in, a, diag);
    if (!sp.arg.accepts(at))
        return fail(diag, "`{s}` needs a {s} argument, got {s}", .{ sp.names[0], sp.arg.word(), try at.name(arena) });
    return switch (sp.result) {
        .count => types.Type.init(.int),
        .sum => switch (at.kind) {
            .float => types.Type.init(.float).withNull(true),
            .decimal => at.withNull(true),
            else => types.Type.init(.int).withNull(true),
        },
        .float => types.Type.init(.float).withNull(true),
        .same => at.withNull(true),
        .bool => types.Type.init(.bool).withNull(true),
        .int => types.Type.init(.int).withNull(true),
    };
}

/// Ranking is a non-null int; MIN/MAX, LAG and LEAD keep the column's type; AVG is a
/// float; SUM is an INT over ints, a DECIMAL of the column's scale over decimals, as the
/// SUM aggregate is, and a float otherwise. With an argument it is nullable (all-null
/// peers, first LAG).
pub fn windowFuncType(kind: ast.WinKind, src: types.Type) types.Type {
    return switch (kind) {
        .row_number, .rank, .dense_rank, .count => types.Type.init(.int),
        .min, .max, .lag, .lead => src.asNullable(),
        .avg => types.Type.init(.float).asNullable(),
        .sum => switch (src.kind) {
            .int => types.Type.init(.int).asNullable(),
            .decimal => src.asNullable(),
            else => types.Type.init(.float).asNullable(),
        },
    };
}

/// The input then one column per function, the planner's rule, so `EXPLAIN` shows
/// the schema past a window instead of `unresolved`.
pub fn windowSchema(arena: std.mem.Allocator, in: types.Schema, wd: ast.Window, diag: *Diag) Error!types.Schema {
    _ = try fieldIndices(arena, in, wd.partition_by, diag);
    const oqs = try arena.alloc(ast.QualName, wd.order_by.len);
    for (wd.order_by, oqs) |sk, *q| q.* = sk.field;
    _ = try fieldIndices(arena, in, oqs, diag);
    const fields = try arena.alloc(types.Schema.Field, in.fields.len + wd.funcs.len);
    @memcpy(fields[0..in.fields.len], in.fields);
    for (wd.funcs, 0..) |f, i| {
        var src = types.Type.init(.int);
        if (f.arg) |q| src = in.fields[(try fieldIndices(arena, in, &[_]ast.QualName{q}, diag))[0]].ty;
        if (f.default) |d| if (f.arg) |q| try checkLagDefault(arena, d, q, src, diag);
        fields[in.fields.len + i] = .{ .name = f.out, .ty = windowFuncType(f.kind, src) };
    }
    return .{ .fields = fields };
}

/// A literal LAG/LEAD default must fit its column, as the run requires; one over
/// `$params` is left to the run, which knows their values.
fn checkLagDefault(arena: std.mem.Allocator, d: *const ast.Expr, q: ast.QualName, ty: types.Type, diag: *Diag) Error!void {
    if (try exprHasDollar(arena, d)) return;
    const v = eval.constEval(arena, d, &.{}, &.{}) catch |e| {
        if (e == error.OutOfMemory) return error.OutOfMemory;
        return fail(diag, "LAG/LEAD default: {s}", .{@import("../env.zig").errLabel(e)});
    };
    if (v.isNull()) return;
    _ = eval.castValueTyped(arena, v, ty.asNullable()) catch
        return fail(diag, "LAG/LEAD default does not fit `{s}` ({s})", .{ q.last(), try ty.name(arena) });
}

fn exprHasDollar(arena: std.mem.Allocator, e: *const ast.Expr) Error!bool {
    var found = false;
    const Walk = struct {
        arena: std.mem.Allocator,
        found: *bool,
        fn recur(w: @This(), x: *const ast.Expr) Error!*ast.Expr {
            if (x.* == .field) {
                if (x.field.dollar) w.found.* = true;
                return @constCast(x);
            }
            return ast.rebuildExpr(w.arena, x, w, recur);
        }
    };
    _ = try Walk.recur(.{ .arena = arena, .found = &found }, e);
    return found;
}

pub const ExplodePlan = struct { idx: usize, schema: types.Schema };

pub fn explodePlan(arena: std.mem.Allocator, in: types.Schema, ex: ast.Explode, diag: *Diag) Error!ExplodePlan {
    const idx = in.indexOf(ex.field) orelse return fail(diag, "unknown field `{s}`", .{ex.field});
    const fty = in.fields[idx].ty;
    if (!(fty.kind == .string or fty.kind == .bytes))
        return fail(diag, "explode needs a string column (it splits a delimited value or a JSON array)", .{});
    const fields = try arena.alloc(types.Schema.Field, in.fields.len);
    for (in.fields, fields, 0..) |f, *out, i| {
        const ty = if (ex.json) types.Type.init(.string).asNullable() else types.Type.init(.string);
        out.* = if (i == idx) .{ .name = ex.as_name orelse f.name, .ty = ty } else f;
    }
    return .{ .idx = idx, .schema = .{ .fields = fields } };
}

pub const JoinPlan = struct {
    lks: []const usize,
    rks: []const usize,
    schema: types.Schema,
    emit_right: bool,
    right_nullable: bool,
    left_nullable: bool,
};

/// The parser orients by alias prefix, which unqualified names lack, so the side is
/// decided by where each name resolves. Both readings resolving to different columns
/// is ambiguous; a left key may carry an earlier join's alias.
fn joinPair(left: types.Schema, right: types.Schema, lq: ast.QualName, rq: ast.QualName, diag: *Diag) Error![2]usize {
    const ln = lastPart(lq);
    const rn = lastPart(rq);
    const l_in_l = left.resolve(lq.parts);
    const l_in_r = right.indexOf(ln);
    const r_in_l = left.resolve(rq.parts);
    const r_in_r = right.indexOf(rn);

    const as_written = l_in_l != null and r_in_r != null;
    const flipped = r_in_l != null and l_in_r != null;
    if (as_written and flipped and !std.mem.eql(u8, ln, rn))
        return fail(diag, "join key `{s}` is ambiguous — `{s}` and `{s}` both exist on both sides; qualify them", .{ ln, ln, rn });
    if (as_written) return .{ l_in_l.?, r_in_r.? };
    if (flipped) return .{ r_in_l.?, l_in_r.? };
    if (l_in_l == null and l_in_r == null) return fail(diag, "unknown left join key `{s}`", .{ln});
    if (r_in_r == null and r_in_l == null) return fail(diag, "unknown right join key `{s}`", .{rn});
    if (l_in_l != null and r_in_l != null) return fail(diag, "join key `{s}` is not a column of the joined side", .{rn});
    return fail(diag, "join key `{s}` is not a column of the joined side", .{ln});
}

/// The `_r` suffix keeps bumping until free: two output fields with one name make the
/// second unreachable, since every lookup goes through `Schema.indexOf`.
pub fn joinPlan(arena: std.mem.Allocator, left: types.Schema, right: types.Schema, j: ast.Join, diag: *Diag) Error!JoinPlan {
    if (j.left_keys.len != j.right_keys.len) return fail(diag, "join has mismatched key lists", .{});
    if (j.kind != .cross and j.left_keys.len == 0) return fail(diag, "join needs at least one `ON <column> = <column>` pair", .{});

    const lks = try arena.alloc(usize, j.left_keys.len);
    const rks = try arena.alloc(usize, j.right_keys.len);
    for (j.left_keys, j.right_keys, lks, rks) |lq, rq, *lo, *ro| {
        const pair = try joinPair(left, right, lq, rq, diag);
        lo.* = pair[0];
        ro.* = pair[1];
        const lt = left.fields[pair[0]].ty;
        const rt = right.fields[pair[1]].ty;
        if (types.Type.unify(lt, rt) == null)
            return fail(diag, "join keys `{s}` ({s}) and `{s}` ({s}) are not comparable — make them one type in the ON: `CAST(x AS string) = y`", .{ left.fields[pair[0]].name, @tagName(lt.kind), right.fields[pair[1]].name, @tagName(rt.kind) });
    }

    const emit_right = (j.kind != .semi and j.kind != .anti);
    const right_nullable = (j.kind == .left or j.kind == .full);
    const left_nullable = (j.kind == .right or j.kind == .full);

    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    for (left.fields) |f| try fields.append(.{ .name = f.name, .ty = if (left_nullable) f.ty.asNullable() else f.ty, .rel = f.rel, .base = f.base });
    if (emit_right) for (right.fields) |f| {
        var name = f.name;
        var n: usize = 0;
        while ((types.Schema{ .fields = fields.items }).indexOf(name) != null) : (n += 1) {
            name = if (n == 0)
                try std.fmt.allocPrint(arena, "{s}_r", .{f.name})
            else
                try std.fmt.allocPrint(arena, "{s}_r{d}", .{ f.name, n + 1 });
        }
        try fields.append(.{
            .name = name,
            .ty = if (right_nullable) f.ty.asNullable() else f.ty,
            .rel = if (j.alias.len != 0) j.alias else j.binding,
            .base = f.name,
        });
    };
    return .{
        .lks = lks,
        .rks = rks,
        .schema = .{ .fields = try fields.toOwnedSlice() },
        .emit_right = emit_right,
        .right_nullable = right_nullable,
        .left_nullable = left_nullable,
    };
}

fn nameIn(names: []const []const u8, n: []const u8) bool {
    for (names) |x| if (std.mem.eql(u8, x, n)) return true;
    return false;
}

fn renameTo(renames: []const ast.SelectItem.Rename, n: []const u8) ?[]const u8 {
    for (renames) |r| if (std.mem.eql(u8, r.from, n)) return r.to;
    return null;
}

/// Body-scoped variables (loop vars, statement-fn params) bound to typed
/// placeholders, so `$var` used as a value is lenient instead of an unknown field.
pub fn bindBodyVars(arena: std.mem.Allocator, stmts: []const ast.Stmt, map: *ParamMap) Error!void {
    for (stmts) |s| switch (s) {
        .for_each => |fe| {
            for (fe.var_names, 0..) |vn, i| {
                if (map.contains(vn)) continue;
                const ty: ?types.Type = if (i < fe.var_types.len) fe.var_types[i] else null;
                try map.put(vn, if (ty) |t| try typedZero(arena, t) else try mk(arena, .null_lit));
            }
            try bindBodyVars(arena, fe.body, map);
        },
        .func => |fd| if (fd.body == .stmts) {
            for (fd.params) |p| {
                if (map.contains(p.name)) continue;
                try map.put(p.name, if (p.ty) |t| try typedZero(arena, t) else try mk(arena, .null_lit));
            }
            try bindBodyVars(arena, fd.body.stmts, map);
        },
        .match => |m| for (m.arms) |arm| try bindBodyVars(arena, arm.body, map),
        else => {},
    };
}

/// A literal of the right type (value irrelevant) to stand in for a param during
/// type-flow when it has no declared default.
pub fn typedZero(arena: std.mem.Allocator, ty: types.Type) Error!*const ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = switch (ty.kind) {
        .int => .{ .int_lit = 0 },
        .float => .{ .float_lit = 0 },
        .string, .bytes => .{ .str_lit = "" },
        .bool => .{ .bool_lit = false },
        else => .null_lit,
    };
    return e;
}

test "joinPlan: collision suffix `_r`, left-nullability, semi/anti drop the right side" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    const left = types.Schema{ .fields = &.{ .{ .name = "id", .ty = I }, .{ .name = "code", .ty = S } } };
    const right = types.Schema{ .fields = &.{ .{ .name = "code", .ty = S }, .{ .name = "label", .ty = S } } };
    const key = ast.QualName{ .parts = &.{"code"} };
    var diag = Diag{};

    const keys: []const ast.QualName = &.{key};

    const inner = try joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expectEqual(@as(usize, 1), inner.lks[0]);
    try std.testing.expectEqual(@as(usize, 0), inner.rks[0]);
    try std.testing.expectEqual(@as(usize, 4), inner.schema.fields.len);
    try std.testing.expectEqualStrings("code_r", inner.schema.fields[2].name);
    try std.testing.expectEqualStrings("label", inner.schema.fields[3].name);
    try std.testing.expect(!inner.schema.fields[3].ty.nullable);

    const lj = try joinPlan(a, left, right, .{ .kind = .left, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(lj.right_nullable);
    try std.testing.expect(lj.schema.fields[2].ty.nullable and lj.schema.fields[3].ty.nullable);
    try std.testing.expect(!lj.schema.fields[0].ty.nullable);

    const semi = try joinPlan(a, left, right, .{ .kind = .semi, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(!semi.emit_right);
    try std.testing.expectEqual(@as(usize, 2), semi.schema.fields.len);

    const rj = try joinPlan(a, left, right, .{ .kind = .right, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(rj.left_nullable and !rj.right_nullable);
    try std.testing.expect(rj.schema.fields[0].ty.nullable);
    const fj = try joinPlan(a, left, right, .{ .kind = .full, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expect(fj.left_nullable and fj.right_nullable);
    const cj = try joinPlan(a, left, right, .{ .kind = .cross, .binding = "r", .left_keys = &.{}, .right_keys = &.{} }, &diag);
    try std.testing.expectEqual(@as(usize, 0), cj.lks.len);
    try std.testing.expectEqual(@as(usize, 4), cj.schema.fields.len);

    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_keys = &.{.{ .parts = &.{"nope"} }}, .right_keys = keys }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "unknown left join key") != null);
}

test "joinPlan: the `_r` suffix keeps bumping until the name is actually free" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    const left = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = I },
        .{ .name = "x", .ty = S },
        .{ .name = "x_r", .ty = S },
    } };
    const right = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = I },
        .{ .name = "x", .ty = S },
        .{ .name = "x_r", .ty = S },
    } };
    var diag = Diag{};
    const keys: []const ast.QualName = &.{.{ .parts = &.{"id"} }};
    const p = try joinPlan(a, left, right, .{ .kind = .inner, .binding = "r", .left_keys = keys, .right_keys = keys }, &diag);
    try std.testing.expectEqual(@as(usize, 6), p.schema.fields.len);
    try std.testing.expectEqualStrings("id_r", p.schema.fields[3].name);
    try std.testing.expectEqualStrings("x_r2", p.schema.fields[4].name);
    try std.testing.expectEqualStrings("x_r_r", p.schema.fields[5].name);
    for (p.schema.fields, 0..) |f, i| try std.testing.expectEqual(i, p.schema.indexOf(f.name).?);
}

test "joinPlan: pair orientation, ambiguity, per-pair comparability" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const S = types.Type.init(.string);
    const left = types.Schema{ .fields = &.{ .{ .name = "id", .ty = I }, .{ .name = "day", .ty = S }, .{ .name = "amount", .ty = I } } };
    const right = types.Schema{ .fields = &.{ .{ .name = "d", .ty = S }, .{ .name = "ref", .ty = I }, .{ .name = "note", .ty = S } } };
    const k_ref = ast.QualName{ .parts = &.{"ref"} };
    const k_id = ast.QualName{ .parts = &.{"id"} };
    const k_day = ast.QualName{ .parts = &.{"day"} };
    const k_d = ast.QualName{ .parts = &.{"d"} };
    const k_amount = ast.QualName{ .parts = &.{"amount"} };
    const k_a = ast.QualName{ .parts = &.{"a"} };
    const k_b = ast.QualName{ .parts = &.{"b"} };
    var diag = Diag{};

    const p = try joinPlan(a, left, right, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{ k_ref, k_day },
        .right_keys = &.{ k_id, k_d },
    }, &diag);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, p.lks);
    try std.testing.expectEqualSlices(usize, &.{ 1, 0 }, p.rks);

    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, left, right, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{k_amount},
        .right_keys = &.{k_d},
    }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "not comparable") != null);

    const both_l = types.Schema{ .fields = &.{ .{ .name = "a", .ty = I }, .{ .name = "b", .ty = I } } };
    const both_r = types.Schema{ .fields = &.{ .{ .name = "b", .ty = I }, .{ .name = "a", .ty = I } } };
    try std.testing.expectError(error.AnalyzeFailed, joinPlan(a, both_l, both_r, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{k_a},
        .right_keys = &.{k_b},
    }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "ambiguous") != null);

    const same = try joinPlan(a, both_l, both_r, .{
        .kind = .inner,
        .binding = "r",
        .left_keys = &.{k_a},
        .right_keys = &.{k_a},
    }, &diag);
    try std.testing.expectEqualSlices(usize, &.{0}, same.lks);
    try std.testing.expectEqualSlices(usize, &.{1}, same.rks);
}

test "aggregatePlan: result types per function and group-key passthrough" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const in = types.Schema{ .fields = &.{
        .{ .name = "g", .ty = types.Type.init(.string) },
        .{ .name = "v", .ty = types.Type.init(.int) },
    } };
    const by = try a.alloc(ast.QualName, 1);
    by[0] = .{ .parts = &.{"g"} };
    const aggs = try a.alloc(ast.AggItem, 4);
    aggs[0] = .{ .name = "n", .func = .count, .arg = null };
    aggs[1] = .{ .name = "s", .func = .sum, .arg = try tfld(a, "v") };
    aggs[2] = .{ .name = "m", .func = .avg, .arg = try tfld(a, "v") };
    aggs[3] = .{ .name = "lo", .func = .min, .arg = try tfld(a, "g") };
    var pm = std.StringHashMap(*const ast.Expr).init(a);
    var diag = Diag{};
    const plan = try aggregatePlan(a, in, .{ .aggs = aggs, .by = by }, &pm, &diag);

    try std.testing.expectEqual(@as(usize, 1), plan.by.len);
    try std.testing.expectEqual(@as(usize, 0), plan.by[0]);
    const f = plan.schema.fields;
    try std.testing.expectEqual(@as(usize, 5), f.len);
    try std.testing.expectEqual(types.TypeKind.string, f[0].ty.kind);
    try std.testing.expect(f[1].ty.kind == .int and !f[1].ty.nullable);
    try std.testing.expect(f[2].ty.kind == .int and f[2].ty.nullable);
    try std.testing.expect(f[3].ty.kind == .float and f[3].ty.nullable);
    try std.testing.expect(f[4].ty.kind == .string and f[4].ty.nullable);

    const bad = try a.alloc(ast.QualName, 1);
    bad[0] = .{ .parts = &.{"zzz"} };
    try std.testing.expectError(error.AnalyzeFailed, aggregatePlan(a, in, .{ .aggs = aggs, .by = bad }, &pm, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "zzz") != null);
}

test "selectCols: `* except` drops the named columns and keeps source order" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const I = types.Type.init(.int);
    const in = types.Schema{ .fields = &.{
        .{ .name = "a", .ty = I },
        .{ .name = "b", .ty = I },
        .{ .name = "c", .ty = I },
    } };
    var pm = std.StringHashMap(*const ast.Expr).init(a);
    var diag = Diag{};
    const items = [_]ast.SelectItem{.{ .star_except = &.{"b"} }};
    const cols = try selectCols(a, in, &items, &pm, &diag);
    try std.testing.expectEqual(@as(usize, 2), cols.len);
    try std.testing.expectEqualStrings("a", cols[0].name);
    try std.testing.expectEqualStrings("c", cols[1].name);
    try std.testing.expectEqual(@as(usize, 0), cols[0].source.passthrough);
    try std.testing.expectEqual(@as(usize, 2), cols[1].source.passthrough);
}

test "analyze: a numeric aggregate of a string literal says to double-quote a column name" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const f = [_]types.Schema.Field{.{ .name = "Kick-Off", .ty = types.Type.init(.int) }};
    var diag = Diag{};
    const lit = try a.create(ast.Expr);
    lit.* = .{ .str_lit = "Kick-Off" };
    try std.testing.expectError(error.AnalyzeFailed, aggResultType(a, .sum, lit, .{ .fields = &f }, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "sum(\"Kick-Off\")") != null);
}
