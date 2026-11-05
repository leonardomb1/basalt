//! The list lambdas: `list_filter`, `list_transform`, `list_any`, `list_all` and
//! `list_reduce`, over a LIST column's own elements, as the `json_*` lambdas are
//! over a JSON array's text.
//!
//! The elements are a batch of their own (`Elements`): the list's child rows as
//! one column named after the lambda's element parameter, a 1-based position under
//! its index parameter when it has one, and every outer column the body reads
//! repeated down its row's elements. The body is then one vectorized expression
//! over that batch, evaluated once for all of a batch's elements, not once per
//! element; `x.city`, `x[1]` and the other nested forms work on the parameter as
//! on any column, and a parameter shadows an outer column of its name. The result
//! is regrouped by the list's offsets: kept elements (`list_filter`), the body's
//! values (`list_transform`), or whether some or every element holds
//! (`list_any`, `list_all`; an empty list false or true). A body value that is not
//! true counts as false, as in the `json_*` lambdas. `list_reduce` folds each row's
//! elements in order from its initial value, each step's accumulator bound as a
//! value, so it alone runs element by element.

const std = @import("std");
const ast = @import("../../lang/ast.zig");
const types = @import("../../lang/types.zig");
const column = @import("../column.zig");
const Batch = @import("../batch.zig").Batch;
const Value = @import("../value.zig").Value;
const Type = types.Type;
const eval = @import("../eval.zig");
const EvalError = eval.EvalError;
const TypeCtx = eval.TypeCtx;
const TypeError = eval.TypeError;
const vec = @import("vec.zig");
const Vec = vec.Vec;
const VecError = vec.VecError;
const row_eval = @import("row.zig");
const eq = @import("support.zig").eq;

const Kind = enum { filter, transform, any, all, reduce };

fn kindOf(name: []const u8) Kind {
    if (eq(name, "list_filter")) return .filter;
    if (eq(name, "list_transform")) return .transform;
    if (eq(name, "list_any")) return .any;
    if (eq(name, "list_all")) return .all;
    return .reduce;
}

/// The positions of the lambda's parameters: the element's, the index's (when
/// it has one) and, for `list_reduce`, the accumulator's, which comes first.
const Params = struct { acc: ?usize, elem: usize, index: ?usize };

fn paramsOf(k: Kind, l: ast.Expr.Lambda) Params {
    if (k == .reduce) return .{ .acc = 0, .elem = 1, .index = if (l.params.len > 2) 2 else null };
    return .{ .acc = null, .elem = 0, .index = if (l.params.len > 1) 1 else null };
}

pub fn typing(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
    const k = kindOf(c.name);
    const at: usize = if (k == .reduce) 2 else 1;
    if (c.args.len != at + 1 or c.args[at].* != .lambda) return self.err("`{s}` takes {s}", .{ c.name, usage(k) });
    const l = c.args[at].lambda;
    const lo: usize = if (k == .reduce) 2 else 1;
    if (l.params.len < lo or l.params.len > lo + 1)
        return self.err("`{s}`'s lambda takes {s}, not {d} parameter{s}", .{ c.name, if (k == .reduce) "(acc, x) or (acc, x, i)" else "(x) or (x, i)", l.params.len, if (l.params.len == 1) "" else "s" });
    const lt = try self.argType(c, 0);
    if (lt.kind != .array) {
        if (lt.unknown) return Type.unknownNull();
        return self.err("`{s}` takes a LIST, got {s} (the `json_*` lambdas take a JSON array's text)", .{ c.name, lt.name(self.arena) catch "?" });
    }
    const elem = column.childType(lt);
    if (k == .reduce) {
        var acc = try self.typeOf(c.args[1]);
        for (0..3) |_| {
            const bt = try bodyType(self, k, l, elem, acc);
            var u = Type.unify(acc, bt) orelse
                return self.err("`list_reduce`: the lambda returns {s}, which an accumulator of {s} cannot hold", .{ bt.name(self.arena) catch "?", acc.name(self.arena) catch "?" });
            if (u.kind == .decimal) u.precision = 38;
            if (u.eql(acc) and u.unknown == acc.unknown) return u.asNullable();
            acc = u;
        }
        return self.err("`list_reduce`: the accumulator's type keeps changing — give the initial value the type the lambda returns", .{});
    }
    const bt = try bodyType(self, k, l, elem, null);
    if (k == .transform) {
        const e = try self.arena.create(Type);
        e.* = bt.asNullable();
        return .{ .kind = .array, .nullable = lt.nullable, .elem = e };
    }
    if (!(bt.kind == .bool or bt.unknown)) return self.err("`{s}`: the lambda must be a condition (BOOL), not {s}", .{ c.name, bt.name(self.arena) catch "?" });
    if (k == .filter) return lt;
    return Type.init(.bool).withNull(lt.nullable);
}

fn usage(k: Kind) []const u8 {
    return switch (k) {
        .filter, .any, .all => "(list, x -> condition)",
        .transform => "(list, x -> value)",
        .reduce => "(list, initial, (acc, x) -> value)",
    };
}

/// The body's type over the elements' schema (`elemSchema`), the accumulator typed
/// `acc` for `list_reduce`.
fn bodyType(self: *TypeCtx, k: Kind, l: ast.Expr.Lambda, elem: Type, acc: ?Type) TypeError!Type {
    const p = paramsOf(k, l);
    var fields = std.array_list.Managed(types.Schema.Field).init(self.arena);
    if (p.acc) |i| try fields.append(.{ .name = l.params[i], .ty = acc.? });
    try fields.append(.{ .name = l.params[p.elem], .ty = elem });
    if (p.index) |i| try fields.append(.{ .name = l.params[i], .ty = Type.init(.int) });
    for (self.schema.fields) |f| if (!isParam(l, f.name)) try fields.append(f);
    var sub = TypeCtx{ .schema = .{ .fields = fields.items }, .arena = self.arena };
    return sub.typeOf(try bodyOf(self.arena, l)) catch |e| {
        self.msg = sub.msg;
        self.span = sub.span;
        return e;
    };
}

fn isParam(l: ast.Expr.Lambda, name: []const u8) bool {
    for (l.params) |p| if (std.mem.eql(u8, p, name)) return true;
    return false;
}

/// The lambda's body with each parameter read as the column of its name.
fn bodyOf(arena: std.mem.Allocator, l: ast.Expr.Lambda) error{OutOfMemory}!*ast.Expr {
    const slots = try arena.alloc(*ast.Expr, l.params.len);
    for (slots, l.params) |*s, name| {
        s.* = try arena.create(ast.Expr);
        s.*.* = .{ .field = try ast.QualName.one(arena, name) };
    }
    return row_eval.bindParams(arena, l, slots);
}

/// A list column's elements as a batch: `child` its child rows in order, `starts`
/// where each list row's elements begin (`n + 1` of them, a null row none).
const Elements = struct { batch: Batch, starts: []const usize };

fn elements(arena: std.mem.Allocator, k: Kind, l: ast.Expr.Lambda, outer: Batch, list: column.Column) !Elements {
    const lst = list.data.nested.body.list;
    const starts = try arena.alloc(usize, list.len + 1);
    var total: usize = 0;
    for (0..list.len) |r| {
        starts[r] = total;
        if (list.validity.get(r)) total += @intCast(lst.offsets[r + 1] - lst.offsets[r]);
    }
    starts[list.len] = total;
    const parent = try arena.alloc(usize, total);
    const idx = try arena.alloc(usize, total);
    const pos = try arena.alloc(i64, total);
    for (0..list.len) |r| {
        const lo: usize = @intCast(lst.offsets[r]);
        for (starts[r]..starts[r + 1], 0..) |w, j| {
            parent[w] = r;
            idx[w] = lo + j;
            pos[w] = @intCast(j + 1);
        }
    }
    const p = paramsOf(k, l);
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    var cols = std.array_list.Managed(column.Column).init(arena);
    try fields.append(.{ .name = l.params[p.elem], .ty = lst.child.ty });
    try cols.append(try column.permute(arena, lst.child, idx));
    if (p.index) |i| {
        try fields.append(.{ .name = l.params[i], .ty = Type.init(.int) });
        try cols.append(column.Column.derived(Type.init(.int), total, try column.Bitmap.initFull(arena, total), .{ .i64 = pos }));
    }
    var used = std.StringHashMap(void).init(arena);
    const Names = struct {
        fn add(m: *std.StringHashMap(void), n: *const ast.Expr) ?bool {
            if (n.* != .field) return null;
            m.put(n.field.parts[0], {}) catch return true;
            return false;
        }
    };
    if (ast.anyExpr(l.body, &used, Names.add)) return error.OutOfMemory;
    for (outer.schema.fields, outer.columns) |f, c| {
        if (isParam(l, f.name) or !used.contains(f.name)) continue;
        try fields.append(f);
        try cols.append(try column.permute(arena, c, parent));
    }
    const schema = try arena.create(types.Schema);
    schema.* = .{ .fields = fields.items };
    return .{ .batch = .{ .schema = schema, .columns = cols.items, .len = total }, .starts = starts };
}

fn run(arena: std.mem.Allocator, c: ast.Expr.Call, outer: Batch, list: column.Column) EvalError!column.Column {
    const k = kindOf(c.name);
    const l = c.args[if (k == .reduce) 2 else 1].lambda;
    const els = try elements(arena, k, l, outer, list);
    if (k == .reduce) return reduce(arena, c, l, outer, list, els);
    const body = try bodyOf(arena, l);
    var tc = TypeCtx{ .schema = els.batch.schema.*, .arena = arena };
    const bt = tc.typeOf(body) catch return error.TypeMismatch;
    const got = try vec.evalColumn(arena, body, els.batch, bt);
    const bm = try column.Bitmap.initFull(arena, list.len);
    @memcpy(bm.bits, list.validity.bits[0..bm.bits.len]);
    switch (k) {
        .transform => {
            const offsets = try arena.alloc(i32, list.len + 1);
            for (offsets, els.starts) |*o, s| o.* = @intCast(s);
            const e = try arena.create(Type);
            e.* = bt.asNullable();
            const ty = Type{ .kind = .array, .nullable = true, .elem = e };
            return listColumn(arena, ty, list.len, bm, offsets, got);
        },
        .filter => {
            const keep = try arena.alloc(bool, got.len);
            var kept: usize = 0;
            for (keep, 0..) |*x, i| {
                x.* = holds(got, i);
                kept += @intFromBool(x.*);
            }
            const offsets = try arena.alloc(i32, list.len + 1);
            offsets[0] = 0;
            var w: usize = 0;
            for (0..list.len) |r| {
                for (keep[els.starts[r]..els.starts[r + 1]]) |x| w += @intFromBool(x);
                offsets[r + 1] = @intCast(w);
            }
            return listColumn(arena, list.ty, list.len, bm, offsets, try column.gather(arena, els.batch.columns[0], keep, kept));
        },
        .any, .all => {
            const out = try arena.alloc(bool, list.len);
            for (out, 0..) |*o, r| {
                o.* = k == .all;
                for (els.starts[r]..els.starts[r + 1]) |i| if (holds(got, i) != (k == .all)) {
                    o.* = k != .all;
                    break;
                };
            }
            return column.Column.derived(Type.init(.bool).asNullable(), list.len, bm, .{ .b = out });
        },
        .reduce => unreachable,
    }
}

fn holds(c: column.Column, i: usize) bool {
    const v = c.getValue(i);
    return v == .bool and v.bool;
}

fn listColumn(arena: std.mem.Allocator, ty: Type, n: usize, bm: column.Bitmap, offsets: []const i32, child: column.Column) !column.Column {
    const node = try arena.create(column.Nested);
    node.* = .{ .ty = ty, .body = .{ .list = .{ .offsets = offsets, .child = child } } };
    return column.Column.derived(ty, n, bm, .{ .nested = node });
}

/// Each row's elements folded from the row's initial value, in order; a null list
/// is null, an empty one its initial value.
fn reduce(arena: std.mem.Allocator, c: ast.Expr.Call, l: ast.Expr.Lambda, outer: Batch, list: column.Column, els: Elements) EvalError!column.Column {
    var tc = TypeCtx{ .schema = outer.schema.*, .arena = arena };
    const acc_ty = (typing(&tc, c) catch return error.TypeMismatch).asNullable();
    var b = try column.Builder.initCapacity(arena, acc_ty, list.len);
    const acc_slot = try arena.create(ast.Expr);
    acc_slot.* = .null_lit;
    const slots = try arena.alloc(*ast.Expr, l.params.len);
    for (slots, l.params, 0..) |*s, name, i| {
        if (i == 0) {
            s.* = acc_slot;
            continue;
        }
        s.* = try arena.create(ast.Expr);
        s.*.* = .{ .field = try ast.QualName.one(arena, name) };
    }
    const body = try row_eval.bindParams(arena, l, slots);
    for (0..list.len) |r| {
        if (!list.validity.get(r)) {
            try b.append(.null);
            continue;
        }
        var acc = try row_eval.evalRow(arena, c.args[1], outer, r);
        for (els.starts[r]..els.starts[r + 1]) |i| {
            acc_slot.* = try row_eval.accNode(arena, acc, acc_ty);
            acc = try row_eval.evalRow(arena, body, els.batch, i);
        }
        try b.append(acc);
    }
    return b.finish();
}

pub fn vecFn(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    const list = switch (try vec.evalVec(arena, c.args[0], batch)) {
        .col => |x| x,
        .scalar => return error.Unsupported,
    };
    if (list.data != .nested or list.data.nested.body != .list) return error.Unsupported;
    return .{ .col = try run(arena, c, batch, list) };
}

pub fn rowFn(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
    const v = try row_eval.evalRow(arena, c.args[0], batch, row);
    if (v != .nested) return .null;
    var lb = column.Builder.init(arena, v.nested.node.ty);
    try lb.append(v);
    const one = [_]usize{row};
    const cols = try arena.alloc(column.Column, batch.columns.len);
    for (cols, batch.columns) |*o, col| o.* = try column.permute(arena, col, &one);
    const got = try run(arena, c, .{ .schema = batch.schema, .columns = cols, .len = 1 }, try lb.finish());
    return got.getValue(0);
}
