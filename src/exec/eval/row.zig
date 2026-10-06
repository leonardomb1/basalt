//! The row evaluator: an expression over one row, with SQL's NULL rules and the
//! numeric widening of `arith`/`decimalOp`, plus JSON paths and the lambda binding
//! the JSON array functions use.

const Batch = @import("../batch.zig").Batch;
const Decimal = @import("../value.zig").Decimal;
const EvalError = @import("../eval.zig").EvalError;
const Type = @import("../../lang/types.zig").Type;
const TypeCtx = @import("../eval.zig").TypeCtx;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const castFailure = @import("support.zig").castFailure;
const castValueTyped = @import("cast.zig").castValueTyped;
const compareValues = @import("support.zig").compareValues;
const fieldIndex = @import("support.zig").fieldIndex;
const forgetFailure = @import("support.zig").forgetFailure;
const isEmptyVal = @import("vec.zig").isEmptyVal;
const json = @import("../json.zig");
const lookupBuiltin = @import("functions.zig").lookupBuiltin;
const rescaleTo = @import("cast.zig").rescaleTo;
const std = @import("std");
const toF64 = @import("support.zig").toF64;
const types = @import("../../lang/types.zig");
const typing = @import("fn_typing.zig").typing;
const valueToString = @import("format.zig").valueToString;
const evalLit = @import("testing_util.zig").evalLit;

pub fn evalRow(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, row: usize) EvalError!Value {
    forgetFailure();
    switch (expr.*) {
        .null_lit => return .null,
        .bool_lit => |b| return .{ .bool = b },
        .int_lit => |i| return .{ .int = i },
        .float_lit => |f| return .{ .float = f },
        .str_lit => |s| return .{ .string = s },
        .field => |q| {
            const idx = fieldIndex(batch.schema.*, q) orelse return error.TypeMismatch;
            return batch.columns[idx].getValue(row);
        },
        .unary => |u| {
            const v = try evalRow(arena, u.e, batch, row);
            if (v.isNull()) return .null;
            return switch (u.op) {
                .neg => switch (v) {
                    .int => |x| .{ .int = std.math.negate(x) catch return error.IntOverflow },
                    .float => |x| .{ .float = -x },
                    .decimal => |d| .{ .decimal = .{ .unscaled = -d.unscaled, .scale = d.scale } },
                    else => error.TypeMismatch,
                },
                .not => .{ .bool = !v.bool },
                .bit_not => switch (v) {
                    .int => |x| .{ .int = ~x },
                    else => error.TypeMismatch,
                },
            };
        },
        .binary => |b| return evalBinary(arena, b, batch, row),
        .is_null => |n| {
            const v = try evalRow(arena, n.e, batch, row);
            const hit = switch (n.kind) {
                .is_null => v.isNull(),
                .is_empty => v.isNull() or isEmptyVal(v),
            };
            return .{ .bool = if (n.negated) !hit else hit };
        },
        .cond => |c| {
            const cv = try evalRow(arena, c.cond, batch, row);
            if (cv == .bool and cv.bool) return evalRow(arena, c.then, batch, row);
            return evalRow(arena, c.els, batch, row);
        },
        .cast => |c| {
            const v = try evalRow(arena, c.e, batch, row);
            if (v.isNull()) return .null;
            if (!c.safe) return castValueTyped(arena, v, c.ty) catch |e| switch (e) {
                error.CastFailed => return castFailure(arena, v, c.ty),
                else => return e,
            };
            const out: Value = castValueTyped(arena, v, c.ty) catch |e| {
                if (e == error.CastFailed) return .null;
                return e;
            };
            return out;
        },
        .match => |m| return evalMatch(arena, m, batch, row),
        .call => |c| return evalCall(arena, c, batch, row),
        .let_in, .lambda, .lambda_var => return error.TypeMismatch,
    }
}

fn evalBinary(arena: std.mem.Allocator, b: ast.Expr.Binary, batch: Batch, row: usize) EvalError!Value {
    switch (b.op) {
        .@"and" => {
            const l = try evalRow(arena, b.l, batch, row);
            if (l == .bool and l.bool == false) return .{ .bool = false };
            const r = try evalRow(arena, b.r, batch, row);
            if (r == .bool and r.bool == false) return .{ .bool = false };
            if (l.isNull() or r.isNull()) return .null;
            return .{ .bool = true };
        },
        .@"or" => {
            const l = try evalRow(arena, b.l, batch, row);
            if (l == .bool and l.bool == true) return .{ .bool = true };
            const r = try evalRow(arena, b.r, batch, row);
            if (r == .bool and r.bool == true) return .{ .bool = true };
            if (l.isNull() or r.isNull()) return .null;
            return .{ .bool = false };
        },
        else => {
            const l = try evalRow(arena, b.l, batch, row);
            const r = try evalRow(arena, b.r, batch, row);
            if (l.isNull() or r.isNull()) return .null;
            return switch (b.op) {
                .add, .sub, .mul, .div, .mod => arith(b.op, l, r),
                .bit_and, .bit_or, .bit_xor, .shl, .shr => bitwise(b.op, l, r),
                .eq, .ne, .lt, .le, .gt, .ge => blk: {
                    const ord = compareValues(l, r) orelse break :blk error.TypeMismatch;
                    break :blk Value{ .bool = cmpResult(b.op, ord) };
                },
                else => unreachable,
            };
        },
    }
}

/// `minInt(i64) / -1` is the one quotient i64 cannot hold and the hardware traps
/// on it (SIGFPE); it is IntOverflow here, and its remainder is 0.
pub fn intDiv(a: i64, b: i64) error{ DivByZero, IntOverflow }!i64 {
    if (b == 0) return error.DivByZero;
    if (b == -1) return std.math.negate(a) catch error.IntOverflow;
    return @divTrunc(a, b);
}

pub fn intRem(a: i64, b: i64) error{DivByZero}!i64 {
    if (b == 0) return error.DivByZero;
    if (b == -1) return 0;
    return @rem(a, b);
}

pub fn arith(op: ast.BinOp, l: Value, r: Value) EvalError!Value {
    if (l == .int and r == .int) {
        const a = l.int;
        const b = r.int;
        return switch (op) {
            .add => .{ .int = std.math.add(i64, a, b) catch return error.IntOverflow },
            .sub => .{ .int = std.math.sub(i64, a, b) catch return error.IntOverflow },
            .mul => .{ .int = std.math.mul(i64, a, b) catch return error.IntOverflow },
            .div => .{ .int = try intDiv(a, b) },
            .mod => .{ .int = try intRem(a, b) },
            else => unreachable,
        };
    }
    if (l == .decimal or r == .decimal) {
        if (asDecimal(l)) |a| if (asDecimal(r)) |b| if (decimalOp(op, a, b)) |d| return .{ .decimal = try d };
    }
    const a = toF64(l);
    const b = toF64(r);
    return switch (op) {
        .add => .{ .float = a + b },
        .sub => .{ .float = a - b },
        .mul => .{ .float = a * b },
        .div => .{ .float = a / b },
        .mod => .{ .float = @rem(a, b) },
        else => unreachable,
    };
}

/// The exact DECIMAL type of `op`, or null when it stays float. `+ - %` take the
/// wider scale, `*` the summed scale; an INT operand is a DECIMAL(19,0).
pub fn decimalArithType(op: ast.BinOp, lt: Type, rt: Type) ?Type {
    if (op != .add and op != .sub and op != .mul and op != .mod) return null;
    if (lt.kind == .float or rt.kind == .float) return null;
    if (lt.kind != .decimal and rt.kind != .decimal) return null;
    const lp: u16 = if (lt.kind != .decimal) 19 else if (lt.precision == 0) 38 else lt.precision;
    const rp: u16 = if (rt.kind != .decimal) 19 else if (rt.precision == 0) 38 else rt.precision;
    const ls: u16 = if (lt.kind == .decimal) lt.scale else 0;
    const rs: u16 = if (rt.kind == .decimal) rt.scale else 0;
    const scale: u16 = if (op == .mul) ls + rs else @max(ls, rs);
    const prec: u16 = switch (op) {
        .mul => lp + rp,
        .mod => @max(1, @min(lp -| ls, rp -| rs) + scale),
        else => @max(lp -| ls, rp -| rs) + scale + 1,
    };
    return Type.decimal(@intCast(@min(38, prec)), @intCast(@min(38, scale)));
}

pub fn asDecimal(v: Value) ?Decimal {
    return switch (v) {
        .decimal => |d| d,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        else => null,
    };
}

pub fn decimalOp(op: ast.BinOp, a: Decimal, b: Decimal) ?error{ IntOverflow, DivByZero }!Decimal {
    switch (op) {
        .mul => return .{
            .unscaled = std.math.mul(i128, a.unscaled, b.unscaled) catch return error.IntOverflow,
            .scale = a.scale + b.scale,
        },
        .add, .sub => {
            const s = @max(a.scale, b.scale);
            const x = rescaleTo(a, s) orelse return error.IntOverflow;
            const y = rescaleTo(b, s) orelse return error.IntOverflow;
            const u = if (op == .add) std.math.add(i128, x.unscaled, y.unscaled) else std.math.sub(i128, x.unscaled, y.unscaled);
            return .{ .unscaled = u catch return error.IntOverflow, .scale = s };
        },
        .mod => {
            const s = @max(a.scale, b.scale);
            const x = rescaleTo(a, s) orelse return error.IntOverflow;
            const y = rescaleTo(b, s) orelse return error.IntOverflow;
            if (y.unscaled == 0) return error.DivByZero;
            return .{ .unscaled = @rem(x.unscaled, y.unscaled), .scale = s };
        },
        else => return null,
    }
}

fn bitwise(op: ast.BinOp, l: Value, r: Value) EvalError!Value {
    if (l != .int or r != .int) return error.TypeMismatch;
    const a = l.int;
    const b = r.int;
    return switch (op) {
        .bit_and => .{ .int = a & b },
        .bit_or => .{ .int = a | b },
        .bit_xor => .{ .int = a ^ b },
        .shl => .{ .int = shiftLeft(a, b) },
        .shr => .{ .int = shiftRight(a, b) },
        else => unreachable,
    };
}

/// Shift counts outside 0..63 are defined, never UB: an over-wide `<<` is 0 and
/// `>>` is 0 or -1 by sign; a negative count is 0 either way.
fn shiftLeft(a: i64, n: i64) i64 {
    const s = std.math.cast(u6, n) orelse return 0;
    return @bitCast(@as(u64, @bitCast(a)) << s);
}

fn shiftRight(a: i64, n: i64) i64 {
    const s = std.math.cast(u6, n) orelse return if (n > 0 and a < 0) -1 else 0;
    return a >> s;
}

pub fn cmpResult(op: ast.BinOp, ord: std.math.Order) bool {
    return switch (op) {
        .eq => ord == .eq,
        .ne => ord != .eq,
        .lt => ord == .lt,
        .le => ord != .gt,
        .gt => ord == .gt,
        .ge => ord != .lt,
        else => false,
    };
}

fn evalMatch(arena: std.mem.Allocator, m: ast.Match, batch: Batch, row: usize) EvalError!Value {
    if (m.subject) |se| {
        const s = try evalRow(arena, se, batch, row);
        for (m.arms) |arm| {
            if (arm.is_default) return evalRow(arena, arm.value, batch, row);
            for (arm.pats) |p| {
                const pv = try evalRow(arena, p, batch, row);
                if (!s.isNull() and !pv.isNull()) {
                    if (compareValues(s, pv)) |ord| {
                        if (ord == .eq) return evalRow(arena, arm.value, batch, row);
                    }
                }
            }
        }
        return .null;
    }
    for (m.arms) |arm| {
        if (arm.is_default) return evalRow(arena, arm.value, batch, row);
        const g = try evalRow(arena, arm.guard.?, batch, row);
        if (g == .bool and g.bool) return evalRow(arena, arm.value, batch, row);
    }
    return .null;
}

fn evalCall(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
    const b = lookupBuiltin(c.name) orelse return error.TypeMismatch;
    return b.eval_fn(arena, c, batch, row);
}

pub fn parseJson(arena: std.mem.Allocator, text: []const u8) EvalError!std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch |e|
        return if (e == error.OutOfMemory) error.OutOfMemory else error.InvalidJson;
}

/// Walks `path` (`a.b`, `a[0].b` or `a.0.b`, optionally led by `$.`) through `v`;
/// null for a missing key, an index out of range, or a step onto a scalar.
pub fn jsonPath(v: std.json.Value, path: []const u8) ?std.json.Value {
    var p = path;
    if (std.mem.startsWith(u8, p, "$")) p = p[1..];
    var cur = v;
    var it = std.mem.tokenizeAny(u8, p, ".[]");
    while (it.next()) |step| {
        cur = switch (cur) {
            .object => |o| o.get(step) orelse return null,
            .array => |a| blk: {
                const i = std.fmt.parseInt(usize, step, 10) catch return null;
                break :blk if (i < a.items.len) a.items[i] else return null;
            },
            else => return null,
        };
    }
    return cur;
}

pub fn jsonToValue(arena: std.mem.Allocator, v: std.json.Value) EvalError!Value {
    return switch (v) {
        .null => .null,
        .string, .number_string => |s| .{ .string = s },
        .integer => |i| .{ .string = try std.fmt.allocPrint(arena, "{d}", .{i}) },
        .float => |f| .{ .string = try std.fmt.allocPrint(arena, "{d}", .{f}) },
        .bool => |b| .{ .string = if (b) "true" else "false" },
        .object, .array => .{ .string = std.json.Stringify.valueAlloc(arena, v, .{}) catch return error.OutOfMemory },
    };
}

/// A JSON array element as a lambda's parameter: numbers, booleans and strings as
/// themselves, so `x > 5` compares numbers, and objects or arrays as their JSON text.
pub fn jsonElementValue(arena: std.mem.Allocator, raw: []const u8) EvalError!Value {
    return switch (raw[0]) {
        '"' => .{ .string = try json.decodeString(arena, raw[1 .. raw.len - 1]) },
        't' => .{ .bool = true },
        'f' => .{ .bool = false },
        'n' => .null,
        '{', '[' => .{ .string = try json.compact(arena, raw) },
        else => switch (json.number(raw)) {
            .int => |i| .{ .int = i },
            .float => |f| .{ .float = f },
            .text => |t| .{ .string = t },
        },
    };
}

/// Text that is a JSON object or array goes in as that value, not a string.
/// NaN and infinity are written as `null`, as JSON.stringify does.
pub fn writeJsonValue(arena: std.mem.Allocator, v: Value, w: *std.Io.Writer) EvalError!void {
    switch (v) {
        .null => w.writeAll("null") catch return error.OutOfMemory,
        .bool => |b| w.writeAll(if (b) "true" else "false") catch return error.OutOfMemory,
        .int => |i| w.print("{d}", .{i}) catch return error.OutOfMemory,
        .float => |f| if (std.math.isFinite(f))
            w.print("{}", .{f}) catch return error.OutOfMemory
        else
            w.writeAll("null") catch return error.OutOfMemory,
        .decimal => w.writeAll(try valueToString(arena, v)) catch return error.OutOfMemory,
        .string => |s| {
            const t = std.mem.trim(u8, s, " \t\r\n");
            if (t.len > 0 and (t[0] == '{' or t[0] == '[')) {
                if (json.validate(arena, t)) |_| return json.compactInto(arena, t, w) else |_| {}
            }
            std.json.Stringify.encodeJsonString(s, .{}, w) catch return error.OutOfMemory;
        },
        else => std.json.Stringify.encodeJsonString(try valueToString(arena, v), .{}, w) catch return error.OutOfMemory,
    }
}

pub fn bindLambda(arena: std.mem.Allocator, body: *const ast.Expr, name: []const u8, v: Value) error{OutOfMemory}!*ast.Expr {
    const lit = try arena.create(ast.Expr);
    lit.* = try literalOf(arena, v);
    return bindLambdaTo(arena, body, name, lit);
}

pub fn literalOf(arena: std.mem.Allocator, v: Value) error{OutOfMemory}!ast.Expr {
    return switch (v) {
        .null => .null_lit,
        .bool => |x| .{ .bool_lit = x },
        .int => |x| .{ .int_lit = x },
        .float => |x| .{ .float_lit = x },
        .string => |x| .{ .str_lit = x },
        else => .{ .str_lit = try valueToString(arena, v) },
    };
}

/// `body` with parameter `name` replaced by the node `lit`, one node for every
/// mention, so a caller rebinds it by overwriting `lit`.
fn bindLambdaTo(arena: std.mem.Allocator, body: *const ast.Expr, name: []const u8, lit: *ast.Expr) error{OutOfMemory}!*ast.Expr {
    const Bind = struct {
        arena: std.mem.Allocator,
        name: []const u8,
        lit: *ast.Expr,
        fn recur(b: @This(), e: *const ast.Expr) error{OutOfMemory}!*ast.Expr {
            switch (e.*) {
                .lambda_var => |n| if (std.mem.eql(u8, n, b.name)) return b.lit,
                .lambda => |l| for (l.params) |pp| {
                    if (std.mem.eql(u8, pp, b.name)) return @constCast(e);
                },
                else => {},
            }
            return ast.rebuildExpr(b.arena, e, b, recur);
        }
    };
    return Bind.recur(.{ .arena = arena, .name = name, .lit = lit }, body);
}

pub fn jsonArrayArg(arena: std.mem.Allocator, e: *const ast.Expr, batch: Batch, row: usize) EvalError!?[]const u8 {
    const v = try evalRow(arena, e, batch, row);
    if (v.isNull()) return null;
    const doc = try valueToString(arena, v);
    try json.validate(arena, doc);
    if (json.rootKind(doc) != .array) return error.InvalidJson;
    return doc;
}

pub fn bindParams(arena: std.mem.Allocator, l: ast.Expr.Lambda, slots: []const *ast.Expr) error{OutOfMemory}!*ast.Expr {
    var body = l.body;
    for (l.params, slots[0..l.params.len]) |pp, slot| body = try bindLambdaTo(arena, body, pp, slot);
    return body;
}

pub fn lambdaSlots(arena: std.mem.Allocator, vals: []const ast.Expr) error{OutOfMemory}![]*ast.Expr {
    const slots = try arena.alloc(*ast.Expr, vals.len);
    for (slots, vals) |*sl, v| {
        sl.* = try arena.create(ast.Expr);
        sl.*.* = v;
    }
    return slots;
}

/// The node `json_reduce` binds `acc` to. A value with no literal of its own (a
/// DECIMAL, a date) is its text cast back, so a running DECIMAL total stays one.
pub fn accNode(arena: std.mem.Allocator, acc: Value, ty: Type) error{OutOfMemory}!ast.Expr {
    switch (acc) {
        .null, .bool, .int, .float, .string => return literalOf(arena, acc),
        else => {
            if (ty.unknown) return literalOf(arena, acc);
            const text = try arena.create(ast.Expr);
            text.* = .{ .str_lit = try valueToString(arena, acc) };
            return .{ .cast = .{ .e = text, .ty = ty } };
        },
    }
}

threadlocal var reduce_memo: struct { args: ?[*]const *ast.Expr = null, schema: ?*const types.Schema = null, ty: Type = undefined } = .{};

pub fn reduceTypeAt(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) EvalError!Type {
    const m = &reduce_memo;
    if (m.args == c.args.ptr and m.schema == batch.schema) return m.ty;
    var tc = TypeCtx{ .schema = batch.schema.*, .arena = arena };
    const ty = typing.reduceAcc(&tc, c) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.TypeError => return error.TypeMismatch,
    };
    m.* = .{ .args = c.args.ptr, .schema = batch.schema, .ty = ty };
    return ty;
}

test "integer arithmetic overflow is an error, not a silent wrap" {
    const big = Value{ .int = std.math.maxInt(i64) };
    const one = Value{ .int = 1 };
    try std.testing.expectError(error.IntOverflow, arith(.add, big, one));
    try std.testing.expectError(error.IntOverflow, arith(.mul, big, .{ .int = 2 }));
    try std.testing.expectError(error.IntOverflow, arith(.sub, .{ .int = std.math.minInt(i64) }, one));
    try std.testing.expectEqual(@as(i64, 5), (try arith(.add, .{ .int = 2 }, .{ .int = 3 })).int);

    const min = Value{ .int = std.math.minInt(i64) };
    try std.testing.expectError(error.IntOverflow, arith(.div, min, .{ .int = -1 }));
    try std.testing.expectEqual(@as(i64, 0), (try arith(.mod, min, .{ .int = -1 })).int);
    try std.testing.expectEqual(@as(i64, -7), (try arith(.div, .{ .int = 7 }, .{ .int = -1 })).int);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectError(error.IntOverflow, evalLit(ar.allocator(), "-(-9223372036854775807 - 1)"));
    try std.testing.expectError(error.IntOverflow, evalLit(ar.allocator(), "abs(-9223372036854775807 - 1)"));
}
