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
const op = @import("../../exec/op.zig");
const std = @import("std");
const substExpr = @import("../analyze.zig").substExpr;
const collectQuals = @import("../pushdown/hoist.zig").collectQuals;
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
            for (renames) |r| if (in.indexOf(r.from) == null) {
                if (std.mem.startsWith(u8, r.to, "__uk"))
                    return fail(diag, "USING column `{s}` is not a column of the joined side", .{r.from});
                return fail(diag, "unknown rename field `{s}`", .{r.from});
            };
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

/// Ranking, NTILE and the counts are a non-null int, PERCENT_RANK and CUME_DIST a
/// non-null float; LAG, LEAD, the value functions, MIN and MAX keep the column's type;
/// SUM is an INT over ints, a DECIMAL of the column's scale over decimals, as the SUM
/// aggregate is, and a float otherwise; any other aggregate is typed as under GROUP
/// BY. With an argument it is nullable (an empty frame, the first LAG).
pub fn windowFuncType(f: ast.WinFn, src: types.Type) types.Type {
    return switch (f) {
        .win => |k| switch (k) {
            .row_number, .rank, .dense_rank, .ntile => types.Type.init(.int),
            .percent_rank, .cume_dist => types.Type.init(.float),
            .lag, .lead, .first_value, .last_value, .nth_value => src.asNullable(),
        },
        .agg => |a| switch (a) {
            .sum => switch (src.kind) {
                .int => types.Type.init(.int).asNullable(),
                .decimal => src.asNullable(),
                else => types.Type.init(.float).asNullable(),
            },
            else => switch (aggregates.spec(a).result) {
                .count => types.Type.init(.int),
                .float, .sum => types.Type.init(.float).asNullable(),
                .same => src.asNullable(),
                .bool => types.Type.init(.bool).asNullable(),
                .int => types.Type.init(.int).asNullable(),
            },
        },
    };
}

pub const WindowPlan = struct {
    part: []usize,
    ord: []usize,
    funcs: []op.Window.Func,
    range: op.Window.RangeConv,
    schema: types.Schema,
};

/// A window stage as the operator runs it: key columns, each function with its
/// frame in the order key's units, and the schema it leaves (the input then one
/// column per function). LAG/LEAD defaults are left to the caller, which knows the
/// parameters.
pub fn windowPlan(arena: std.mem.Allocator, in: types.Schema, wd: ast.Window, diag: *Diag) Error!WindowPlan {
    const part = try fieldIndices(arena, in, wd.partition_by, diag);
    const oqs = try arena.alloc(ast.QualName, wd.order_by.len);
    for (wd.order_by, oqs) |sk, *q| q.* = sk.field;
    const ord = try fieldIndices(arena, in, oqs, diag);
    var conv: op.Window.RangeConv = .{};
    var key_kind: types.TypeKind = .int;
    for (wd.funcs) |f| if (f.frame) |fr| if (fr.unit == .range and fr.hasOffset()) {
        if (ord.len != 1) return fail(diag, "RANGE with an offset needs exactly one ORDER BY key inside OVER (...), a number, date or timestamp", .{});
        const kt = in.fields[ord[0]].ty;
        key_kind = kt.kind;
        conv = try rangeConv(arena, kt, oqs[0], wd.funcs, diag);
        break;
    };
    const fields = try arena.alloc(types.Schema.Field, in.fields.len + wd.funcs.len);
    @memcpy(fields[0..in.fields.len], in.fields);
    const funcs = try arena.alloc(op.Window.Func, wd.funcs.len);
    for (wd.funcs, funcs, 0..) |f, *o, i| {
        var src = types.Type.init(.int);
        var arg: ?usize = null;
        if (f.arg) |q| {
            arg = (try fieldIndices(arena, in, &[_]ast.QualName{q}, diag))[0];
            src = in.fields[arg.?].ty;
        }
        if (f.func == .agg) {
            const sp = aggregates.spec(f.func.agg);
            if (sp.arg != .star_or_any and !sp.arg.accepts(src))
                return fail(diag, "`{s}` needs a {s} argument, got {s}", .{ sp.names[0], sp.arg.word(), try src.name(arena) });
        }
        fields[in.fields.len + i] = .{ .name = f.out, .ty = windowFuncType(f.func, src) };
        o.* = .{
            .func = f.func,
            .arg = arg,
            .offset = f.offset,
            .distinct = f.distinct,
            .ignore_nulls = f.ignore_nulls,
            .frame = if (f.frame) |fr| try opFrame(fr, conv, key_kind) else .{},
        };
    }
    return .{ .part = part, .ord = ord, .funcs = funcs, .range = conv, .schema = .{ .fields = fields } };
}

/// The input then one column per function, the planner's rule, so `EXPLAIN` shows
/// the schema past a window instead of `unresolved`.
pub fn windowSchema(arena: std.mem.Allocator, in: types.Schema, wd: ast.Window, diag: *Diag) Error!types.Schema {
    const wp = try windowPlan(arena, in, wd, diag);
    for (wd.funcs) |f| {
        if (f.default) |d| if (f.arg) |q| try checkLagDefault(arena, d, q, in.fields[(try fieldIndices(arena, in, &[_]ast.QualName{q}, diag))[0]].ty, diag);
    }
    return wp.schema;
}

const us_per_day: i128 = 86_400_000_000;

fn unitMicros(u: ast.IntervalUnit) i128 {
    return switch (u) {
        .day => us_per_day,
        .hour => 3_600_000_000,
        .minute => 60_000_000,
        .second => 1_000_000,
    };
}

/// Digits after the point in a frame offset's text, as its exact scale.
fn fracDigits(text: []const u8) u8 {
    if (std.mem.indexOfAny(u8, text, "eE") != null) return 6;
    const dot = std.mem.indexOfScalar(u8, text, '.') orelse return 0;
    return @intCast(@min(text.len - dot - 1, 18));
}

/// `text` as an integer at `scale` decimal places, exactly when it is plain digits.
fn scaledText(text: []const u8, scale: u8) i128 {
    const p = std.math.powi(i128, 10, scale) catch return std.math.maxInt(i64);
    if (std.mem.indexOfAny(u8, text, "eE") != null) {
        const f = std.fmt.parseFloat(f64, text) catch return 0;
        return @intFromFloat(@round(f * @as(f64, @floatFromInt(p))));
    }
    const dot = std.mem.indexOfScalar(u8, text, '.') orelse text.len;
    var v: i128 = std.fmt.parseInt(i128, text[0..dot], 10) catch std.math.maxInt(i64);
    v = std.math.mul(i128, v, p) catch std.math.maxInt(i64);
    if (dot < text.len) {
        const frac = text[dot + 1 ..];
        var place = p;
        for (frac) |c| {
            place = @divTrunc(place, 10);
            if (place == 0) break;
            v += @as(i128, c - '0') * place;
        }
    }
    return v;
}

/// How a stage's single ORDER BY key reads as a number for its RANGE offsets: a
/// float as itself, an int at the scale its offsets need, a decimal at its own or
/// theirs, a date or timestamp in microseconds. A plain number over a date or
/// timestamp is days; a TIME takes only an INTERVAL.
fn rangeConv(arena: std.mem.Allocator, kt: types.Type, key: ast.QualName, funcs: []const ast.WindowFunc, diag: *Diag) Error!op.Window.RangeConv {
    var digits: u8 = 0;
    var interval = false;
    var plain = false;
    for (funcs) |f| if (f.frame) |fr| if (fr.unit == .range) for ([_]ast.FrameBound{ fr.start, fr.end }) |b| switch (b) {
        .preceding, .following => |o| {
            if (o.unit != null) interval = true else plain = true;
            digits = @max(digits, fracDigits(o.text));
        },
        else => {},
    };
    if (kt.unknown) return .{};
    switch (kt.kind) {
        .int, .float, .decimal => if (interval)
            return fail(diag, "an INTERVAL frame offset needs a date or timestamp ORDER BY key; `{s}` is {s}", .{ lastPart(key), try kt.name(arena) }),
        .time => if (plain)
            return fail(diag, "a RANGE offset over the TIME key `{s}` is an INTERVAL, e.g. INTERVAL '5' MINUTE", .{lastPart(key)}),
        .date, .timestamp => {},
        else => return fail(diag, "RANGE with an offset needs a numeric, date or timestamp ORDER BY key; `{s}` is {s}", .{ lastPart(key), try kt.name(arena) }),
    }
    return switch (kt.kind) {
        .float => .{ .float = true },
        .int => .{ .mul = std.math.powi(i128, 10, digits) catch 1, .scale = digits },
        .decimal => .{ .scale = @max(kt.scale, digits) },
        .date => .{ .mul = us_per_day },
        else => .{},
    };
}

fn opOff(o: ast.FrameOffset, unit: ast.FrameUnit, conv: op.Window.RangeConv, key_kind: types.TypeKind) op.Window.Off {
    if (unit == .rows) return .{ .rows = std.fmt.parseInt(i64, o.text, 10) catch std.math.maxInt(i64) };
    if (conv.float) return .{ .float = std.fmt.parseFloat(f64, o.text) catch 0 };
    return switch (key_kind) {
        .date, .timestamp, .time => .{ .int = if (o.unit) |u|
            @intFromFloat(@round((std.fmt.parseFloat(f64, o.text) catch 0) * @as(f64, @floatFromInt(unitMicros(u)))))
        else
            @intFromFloat(@round((std.fmt.parseFloat(f64, o.text) catch 0) * @as(f64, @floatFromInt(us_per_day)))) },
        else => .{ .int = scaledText(o.text, conv.scale) },
    };
}

fn opBound(b: ast.FrameBound, unit: ast.FrameUnit, conv: op.Window.RangeConv, key_kind: types.TypeKind) op.Window.Bound {
    return switch (b) {
        .unbounded_preceding => .unbounded_preceding,
        .preceding => |o| .{ .preceding = opOff(o, unit, conv, key_kind) },
        .current_row => .current_row,
        .following => |o| .{ .following = opOff(o, unit, conv, key_kind) },
        .unbounded_following => .unbounded_following,
    };
}

fn opFrame(fr: ast.WinFrame, conv: op.Window.RangeConv, key_kind: types.TypeKind) Error!op.Window.Frame {
    return .{ .range = fr.unit == .range, .start = opBound(fr.start, fr.unit, conv, key_kind), .end = opBound(fr.end, fr.unit, conv, key_kind) };
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

/// A join with its deferred keys placed: `join` keys them like any other, and `left`
/// and `right` are the stages each side runs first, computing those keys (and
/// filtering by an `=` both of whose values are one side's).
pub const KeyPrep = struct {
    join: ast.Join,
    left: []const ast.Stage = &.{},
    right: []const ast.Stage = &.{},
};

const Fit = struct { left: bool = true, right: bool = true, unknown: ?[]const u8 = null };

/// Whether every column of `e` is the left side's, and whether every one is the
/// right side's; the first that is neither's, for the error.
fn fitOf(arena: std.mem.Allocator, e: *const ast.Expr, left: types.Schema, right: types.Schema) !Fit {
    var quals = std.array_list.Managed(ast.QualName).init(arena);
    try collectQuals(arena, e, &quals);
    var f = Fit{};
    for (quals.items) |q| {
        if (q.dollar) continue;
        const in_l = left.resolve(q.parts) != null;
        const in_r = q.parts.len == 1 and right.indexOf(q.parts[0]) != null;
        f.left = f.left and in_l;
        f.right = f.right and in_r;
        if (!in_l and !in_r and f.unknown == null) f.unknown = lastPart(q);
    }
    return f;
}

/// Places each deferred key by where its columns resolve, as `joinPair` does for a
/// plain one: a value of each side makes a computed key; two of the left side make
/// a filter of an inner join's left rows, two of the right side narrow the right.
/// Two of a side the join keeps unmatched (the left of a left join, the right of a
/// right one) cannot filter it, so they join the residual condition instead. With
/// no key left the join runs as a nested loop over the residual.
pub fn orientKeys(arena: std.mem.Allocator, left: types.Schema, right: types.Schema, j: ast.Join, diag: *Diag) Error!KeyPrep {
    if (j.deferred.len == 0) return .{ .join = j };
    var lk = std.array_list.Managed(ast.QualName).init(arena);
    var rk = std.array_list.Managed(ast.QualName).init(arena);
    try lk.appendSlice(j.left_keys);
    try rk.appendSlice(j.right_keys);
    var lcomp = std.array_list.Managed(ast.SelectItem).init(arena);
    var rcomp = std.array_list.Managed(ast.SelectItem).init(arena);
    var lfilt = std.array_list.Managed(ast.Stage).init(arena);
    var rfilt = std.array_list.Managed(ast.Stage).init(arena);
    const rname = if (j.alias.len > 0) j.alias else j.binding;
    var residual = j.residual;
    for (j.deferred) |d| {
        const fa = try fitOf(arena, d.a, left, right);
        const fb = try fitOf(arena, d.b, left, right);
        const as_written = fa.left and fb.right;
        const flipped = fb.left and fa.right;
        const eq = try arena.create(ast.Expr);
        eq.* = .{ .binary = .{ .op = .eq, .l = d.a, .r = d.b } };
        if (fa.unknown orelse fb.unknown) |u|
            return failPos(diag, d.pos, "unknown column `{s}` in the ON of the join with `{s}` — neither side has it", .{ u, rname });
        if (as_written and flipped)
            return failPos(diag, d.pos, "the ON of the join with `{s}` is ambiguous — the columns of each side of its `=` exist on both sides; qualify them (`{s}.col`)", .{ rname, rname });
        if (as_written or flipped) {
            const le = if (as_written) d.a else d.b;
            const re = if (as_written) d.b else d.a;
            try lcomp.append(.{ .computed = .{ .name = d.left_name, .expr = le } });
            try rcomp.append(.{ .computed = .{ .name = d.right_name, .expr = re } });
            try lk.append(try qualOne(arena, d.left_name));
            try rk.append(try qualOne(arena, d.right_name));
        } else if (fa.left and fb.left and j.kind == .inner) {
            try lfilt.append(.{ .node = .{ .filter = eq }, .hints = &.{}, .pos = d.pos });
        } else if (fa.right and fb.right and j.kind != .right and j.kind != .full) {
            try rfilt.append(.{ .node = .{ .filter = eq }, .hints = &.{}, .pos = d.pos });
        } else if ((fa.left and fb.left) or (fa.right and fb.right)) {
            residual = if (residual) |r| blk: {
                const both = try arena.create(ast.Expr);
                both.* = .{ .binary = .{ .op = .@"and", .l = r, .r = eq } };
                break :blk both;
            } else eq;
        } else return failPos(diag, d.pos, "each value of an `=` in the ON of the join with `{s}` must name one side's columns, not both", .{rname});
    }
    if (lcomp.items.len > 0) {
        try lcomp.insert(0, .star);
        try lfilt.append(.{ .node = .{ .select = try lcomp.toOwnedSlice() }, .hints = &.{}, .pos = j.deferred[0].pos });
    }
    if (rcomp.items.len > 0) {
        try rcomp.insert(0, .star);
        try rfilt.append(.{ .node = .{ .select = try rcomp.toOwnedSlice() }, .hints = &.{}, .pos = j.deferred[0].pos });
    }
    var out = j;
    out.left_keys = try lk.toOwnedSlice();
    out.right_keys = try rk.toOwnedSlice();
    out.deferred = &.{};
    out.residual = residual;
    return .{ .join = out, .left = try lfilt.toOwnedSlice(), .right = try rfilt.toOwnedSlice() };
}

fn failPos(diag: *Diag, pos: ast.Pos, comptime fmt: []const u8, args: anytype) error{AnalyzeFailed} {
    const e = fail(diag, fmt, args);
    diag.pos = pos;
    return e;
}

fn qualOne(arena: std.mem.Allocator, name: []const u8) !ast.QualName {
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    return .{ .parts = parts };
}

/// The schema `stages` (filters and selects, as `orientKeys` makes) leave of `in`.
pub fn prepSchema(arena: std.mem.Allocator, in: types.Schema, stages: []const ast.Stage, params: *const ParamMap, diag: *Diag) Error!types.Schema {
    var sch = in;
    for (stages) |st| switch (st.node) {
        .filter => |e| _ = try checkFilter(arena, sch, e, params, diag),
        .select => |items| sch = try schemaOfCols(arena, try selectCols(arena, sch, items, params, diag)),
        else => unreachable,
    };
    return sch;
}

/// A join's residual ON condition checked over both sides' columns side by side,
/// laid out as an inner join lays them out, with that schema; null without one.
pub fn residualPlan(arena: std.mem.Allocator, left: types.Schema, right: types.Schema, j: ast.Join, params: *const ParamMap, diag: *Diag) Error!?struct { pred: *const ast.Expr, schema: types.Schema } {
    const res = j.residual orelse return null;
    var pj = j;
    pj.kind = .inner;
    pj.residual = null;
    const pair = (try joinPlan(arena, left, right, pj, diag)).schema;
    return .{ .pred = try checkFilter(arena, pair, res, params, diag), .schema = pair };
}

/// The `_r` suffix keeps bumping until free: two output fields with one name make the
/// second unreachable, since every lookup goes through `Schema.indexOf`.
pub fn joinPlan(arena: std.mem.Allocator, left: types.Schema, right: types.Schema, j: ast.Join, diag: *Diag) Error!JoinPlan {
    if (j.left_keys.len != j.right_keys.len) return fail(diag, "join has mismatched key lists", .{});
    if (j.deferred.len > 0) return fail(diag, "internal error: a join's keys were not placed (orientKeys) before planning it", .{});

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

test "orientKeys: each value goes to the side its columns are on; one side's pair is a filter; ambiguity and strays are refused" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const int = types.Type.init(.int);
    const str = types.Type.init(.string);
    const left = types.Schema{ .fields = &.{ .{ .name = "cr", .ty = int }, .{ .name = "d", .ty = int } } };
    const right = types.Schema{ .fields = &.{ .{ .name = "code", .ty = str }, .{ .name = "e", .ty = int } } };
    const both = types.Schema{ .fields = &.{ .{ .name = "cr", .ty = int }, .{ .name = "code", .ty = str } } };
    const pos = ast.Pos{ .line = 1, .col = 1 };
    const fld = tfld;
    var diag = Diag{};

    const flipped = ast.Join{ .kind = .inner, .binding = "r", .left_keys = &.{}, .right_keys = &.{}, .deferred = &.{
        .{ .a = try fld(a, "code"), .b = try fld(a, "cr"), .left_name = "__l", .right_name = "__r", .pos = pos },
        .{ .a = try fld(a, "d"), .b = try fld(a, "cr"), .left_name = "__l2", .right_name = "__r2", .pos = pos },
    } };
    const p = try orientKeys(a, left, right, flipped, &diag);
    try std.testing.expectEqual(@as(usize, 1), p.join.left_keys.len);
    try std.testing.expectEqualStrings("__l", p.join.left_keys[0].parts[0]);
    try std.testing.expectEqualStrings("__r", p.join.right_keys[0].parts[0]);
    try std.testing.expect(p.left[0].node == .filter);
    try std.testing.expectEqualStrings("cr", p.left[1].node.select[1].computed.expr.field.parts[0]);
    try std.testing.expectEqualStrings("code", p.right[0].node.select[1].computed.expr.field.parts[0]);

    const stray = ast.Join{ .kind = .inner, .binding = "r", .left_keys = &.{}, .right_keys = &.{}, .deferred = &.{
        .{ .a = try fld(a, "cr"), .b = try fld(a, "nope"), .left_name = "__l", .right_name = "__r", .pos = pos },
    } };
    try std.testing.expectError(error.AnalyzeFailed, orientKeys(a, left, right, stray, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "unknown column `nope`") != null);

    const amb = ast.Join{ .kind = .inner, .binding = "r", .left_keys = &.{}, .right_keys = &.{}, .deferred = &.{
        .{ .a = try fld(a, "code"), .b = try fld(a, "cr"), .left_name = "__l", .right_name = "__r", .pos = pos },
    } };
    try std.testing.expectError(error.AnalyzeFailed, orientKeys(a, both, both, amb, &diag));
    try std.testing.expect(std.mem.indexOf(u8, diag.msg, "ambiguous") != null);

    const left_only = ast.Join{ .kind = .left, .binding = "r", .left_keys = &.{}, .right_keys = &.{}, .deferred = &.{
        .{ .a = try fld(a, "code"), .b = try fld(a, "cr"), .left_name = "__l", .right_name = "__r", .pos = pos },
        .{ .a = try fld(a, "cr"), .b = try fld(a, "d"), .left_name = "__l2", .right_name = "__r2", .pos = pos },
    } };
    const lp = try orientKeys(a, left, right, left_only, &diag);
    try std.testing.expectEqual(@as(usize, 1), lp.join.left_keys.len);
    try std.testing.expect(lp.join.residual.?.* == .binary);
    try std.testing.expectEqual(@as(usize, 1), lp.left.len);
}
