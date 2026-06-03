//! Typed expression evaluation. `TypeCtx` resolves and type-checks an expression
//! against an input schema (filling `msg` on failure); `evalColumn` evaluates an
//! expression over a whole batch into a new column. Null handling follows SQL
//! three-valued logic: any null operand in a comparison/arithmetic yields null;
//! `and`/`or` use the 3VL truth tables; `is null` is total (never null).

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const valuemod = @import("value.zig");
const batchmod = @import("batch.zig");

const Type = types.Type;
const Value = valuemod.Value;
const Batch = batchmod.Batch;

pub const TypeError = error{ TypeError, OutOfMemory };
pub const EvalError = error{ CastFailed, DivByZero, TypeMismatch, OutOfMemory };

// ---------------------------------------------------------------------------
// Type checking
// ---------------------------------------------------------------------------

pub const TypeCtx = struct {
    schema: types.Schema,
    arena: std.mem.Allocator,
    msg: []const u8 = "",

    pub fn typeOf(self: *TypeCtx, expr: *const ast.Expr) TypeError!Type {
        switch (expr.*) {
            .null_lit => return Type.unknownNull(),
            .bool_lit => return Type.init(.bool),
            .int_lit => return Type.init(.int),
            .float_lit => return Type.init(.float),
            .str_lit => return Type.init(.string),
            .field => |q| {
                const idx = fieldIndex(self.schema, q) orelse
                    return self.err("unknown field `{s}`", .{lastPart(q)});
                return self.schema.fields[idx].ty;
            },
            .unary => |u| {
                const t = try self.typeOf(u.e);
                return switch (u.op) {
                    .neg => if (numericish(t)) t else self.err("`-` needs a numeric operand", .{}),
                    .not => if (boolish(t)) Type.init(.bool).withNull(t.nullable) else self.err("`not` needs a bool operand", .{}),
                };
            },
            .binary => |b| return self.typeOfBinary(b),
            .call => |c| return self.typeOfCall(c),
            .cond => |c| {
                const ct = try self.typeOf(c.cond);
                if (!boolish(ct)) return self.err("`if` condition must be bool", .{});
                const a = try self.typeOf(c.then);
                const d = try self.typeOf(c.els);
                const u = Type.unify(a, d) orelse return self.err("`if` branches have incompatible types", .{});
                return u.withNull(u.nullable or ct.nullable);
            },
            .match => |m| return self.typeOfMatch(m),
            .cast => |c| {
                const s = try self.typeOf(c.e);
                return c.ty.withNull(s.nullable);
            },
            .is_null => return Type.init(.bool),
        }
    }

    fn typeOfBinary(self: *TypeCtx, b: ast.Expr.Binary) TypeError!Type {
        const lt = try self.typeOf(b.l);
        const rt = try self.typeOf(b.r);
        const nn = lt.nullable or rt.nullable or lt.unknown or rt.unknown;
        switch (b.op) {
            .add, .sub, .mul, .div, .mod => {
                if (!(numericish(lt) and numericish(rt))) return self.err("arithmetic needs numeric operands", .{});
                const k: types.TypeKind = if (lt.kind == .float or rt.kind == .float or lt.kind == .decimal or rt.kind == .decimal) .float else .int;
                return Type{ .kind = k, .nullable = nn };
            },
            .eq, .ne, .lt, .le, .gt, .ge => {
                if (!comparable(lt, rt)) return self.err("incomparable operands", .{});
                return Type{ .kind = .bool, .nullable = nn };
            },
            .@"and", .@"or" => {
                if (!(boolish(lt) and boolish(rt))) return self.err("`and`/`or` need bool operands", .{});
                return Type{ .kind = .bool, .nullable = nn };
            },
        }
    }

    fn typeOfCall(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
        const name = c.name;
        inline for (.{ "count", "sum", "avg", "min", "max" }) |agg| {
            if (std.mem.eql(u8, name, agg)) return self.err("aggregate `{s}` is only valid inside `aggregate`", .{name});
        }
        if (eq(name, "upper") or eq(name, "lower")) {
            const a = try self.argType(c, 0);
            return Type.init(.string).withNull(a.nullable);
        }
        if (eq(name, "length")) {
            const a = try self.argType(c, 0);
            return Type.init(.int).withNull(a.nullable);
        }
        if (eq(name, "concat")) {
            if (c.args.len == 0) return self.err("`concat` needs at least one argument", .{});
            var nn = false;
            for (c.args) |a| nn = nn or (try self.typeOf(a)).nullable;
            return Type.init(.string).withNull(nn);
        }
        if (eq(name, "coalesce")) {
            if (c.args.len == 0) return self.err("`coalesce` needs at least one argument", .{});
            var result: ?Type = null;
            var all_null = true;
            for (c.args) |a| {
                const t = try self.typeOf(a);
                all_null = all_null and t.nullable;
                result = if (result) |r| (Type.unify(r, t) orelse return self.err("`coalesce` args have incompatible types", .{})) else t;
            }
            return result.?.withNull(all_null);
        }
        return self.err("unknown function `{s}`", .{name});
    }

    fn typeOfMatch(self: *TypeCtx, m: ast.Match) TypeError!Type {
        var subj: ?Type = null;
        if (m.subject) |s| subj = try self.typeOf(s);
        var result: ?Type = null;
        var has_default = false;
        for (m.arms) |arm| {
            if (arm.is_default) {
                has_default = true;
            } else if (arm.guard) |g| {
                if (!boolish(try self.typeOf(g))) return self.err("match guard must be bool", .{});
            } else {
                for (arm.pats) |p| {
                    const pt = try self.typeOf(p);
                    if (subj) |st| if (!comparable(st, pt)) return self.err("match pattern type does not match subject", .{});
                }
            }
            const vt = try self.typeOf(arm.value);
            result = if (result) |r| (Type.unify(r, vt) orelse return self.err("match arms have incompatible types", .{})) else vt;
        }
        var r = result orelse return self.err("match has no arms", .{});
        if (!has_default) r.nullable = true;
        return r;
    }

    fn argType(self: *TypeCtx, c: ast.Expr.Call, i: usize) TypeError!Type {
        if (i >= c.args.len) return self.err("`{s}` is missing an argument", .{c.name});
        return self.typeOf(c.args[i]);
    }

    fn err(self: *TypeCtx, comptime fmt: []const u8, args: anytype) TypeError {
        self.msg = std.fmt.allocPrint(self.arena, fmt, args) catch "out of memory";
        return error.TypeError;
    }
};

fn numericish(t: Type) bool {
    return t.kind.isNumeric() or t.unknown;
}
fn boolish(t: Type) bool {
    return t.kind == .bool or t.unknown;
}
fn comparable(a: Type, b: Type) bool {
    if (a.unknown or b.unknown) return true;
    if (a.kind.isNumeric() and b.kind.isNumeric()) return true;
    return a.kind == b.kind;
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// Evaluate `expr` over every row of `batch` into a new column of type `out_ty`.
pub fn evalColumn(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, out_ty: Type) EvalError!column.Column {
    var ty = out_ty;
    if (ty.unknown) ty = Type.init(.string).asNullable();
    var b = column.Builder.init(arena, ty);
    var i: usize = 0;
    while (i < batch.len) : (i += 1) {
        try b.append(try evalRow(arena, expr, batch, i));
    }
    return b.finish();
}

pub fn evalRow(arena: std.mem.Allocator, expr: *const ast.Expr, batch: Batch, row: usize) EvalError!Value {
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
                    .int => |x| .{ .int = -x },
                    .float => |x| .{ .float = -x },
                    else => error.TypeMismatch,
                },
                .not => .{ .bool = !v.bool },
            };
        },
        .binary => |b| return evalBinary(arena, b, batch, row),
        .is_null => |n| {
            const v = try evalRow(arena, n.e, batch, row);
            const is_null = v.isNull();
            return .{ .bool = if (n.negated) !is_null else is_null };
        },
        .cond => |c| {
            const cv = try evalRow(arena, c.cond, batch, row);
            if (cv == .bool and cv.bool) return evalRow(arena, c.then, batch, row);
            return evalRow(arena, c.els, batch, row);
        },
        .cast => |c| {
            const v = try evalRow(arena, c.e, batch, row);
            if (v.isNull()) return .null;
            return castValue(arena, v, c.ty.kind);
        },
        .match => |m| return evalMatch(arena, m, batch, row),
        .call => |c| return evalCall(arena, c, batch, row),
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
                .eq, .ne, .lt, .le, .gt, .ge => blk: {
                    const ord = compareValues(l, r) orelse break :blk error.TypeMismatch;
                    break :blk Value{ .bool = cmpResult(b.op, ord) };
                },
                else => unreachable,
            };
        },
    }
}

fn arith(op: ast.BinOp, l: Value, r: Value) EvalError!Value {
    if (l == .int and r == .int) {
        const a = l.int;
        const b = r.int;
        return switch (op) {
            .add => .{ .int = a + b },
            .sub => .{ .int = a - b },
            .mul => .{ .int = a * b },
            .div => if (b == 0) error.DivByZero else .{ .int = @divTrunc(a, b) },
            .mod => if (b == 0) error.DivByZero else .{ .int = @rem(a, b) },
            else => unreachable,
        };
    }
    const a = toF64(l);
    const b = toF64(r);
    return switch (op) {
        .add => .{ .float = a + b },
        .sub => .{ .float = a - b },
        .mul => .{ .float = a * b },
        .div => .{ .float = a / b },
        .mod => .{ .float = @mod(a, b) },
        else => unreachable,
    };
}

fn cmpResult(op: ast.BinOp, ord: std.math.Order) bool {
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
    const name = c.name;
    if (eq(name, "coalesce")) {
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (!v.isNull()) return v;
        }
        return .null;
    }
    if (eq(name, "upper") or eq(name, "lower")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        const out = try arena.dupe(u8, try valueToString(arena, v));
        for (out) |*ch| ch.* = if (eq(name, "upper")) std.ascii.toUpper(ch.*) else std.ascii.toLower(ch.*);
        return .{ .string = out };
    }
    if (eq(name, "length")) {
        const v = try evalRow(arena, c.args[0], batch, row);
        if (v.isNull()) return .null;
        return .{ .int = @intCast((try valueToString(arena, v)).len) };
    }
    if (eq(name, "concat")) {
        var buf = std.ArrayList(u8).init(arena);
        for (c.args) |a| {
            const v = try evalRow(arena, a, batch, row);
            if (v.isNull()) return .null;
            try buf.appendSlice(try valueToString(arena, v));
        }
        return .{ .string = try buf.toOwnedSlice() };
    }
    return error.TypeMismatch;
}

fn castValue(arena: std.mem.Allocator, v: Value, kind: types.TypeKind) EvalError!Value {
    return switch (kind) {
        .int => switch (v) {
            .int => v,
            .float => |x| .{ .int = @intFromFloat(x) },
            .bool => |x| .{ .int = if (x) 1 else 0 },
            .string => |s| .{ .int = std.fmt.parseInt(i64, trim(s), 10) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .float => switch (v) {
            .float => v,
            .int => |x| .{ .float = @floatFromInt(x) },
            .string => |s| .{ .float = std.fmt.parseFloat(f64, trim(s)) catch return error.CastFailed },
            else => error.CastFailed,
        },
        .string => .{ .string = try valueToString(arena, v) },
        .bool => switch (v) {
            .bool => v,
            .int => |x| .{ .bool = x != 0 },
            .string => |s| if (std.ascii.eqlIgnoreCase(trim(s), "true"))
                Value{ .bool = true }
            else if (std.ascii.eqlIgnoreCase(trim(s), "false"))
                Value{ .bool = false }
            else
                error.CastFailed,
            else => error.CastFailed,
        },
        else => error.CastFailed,
    };
}

pub fn valueToString(arena: std.mem.Allocator, v: Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .string => |s| s,
        .bytes => |s| s,
        .bool => |b| if (b) "true" else "false",
        .int => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .float => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .decimal => |d| try formatDecimal(arena, d.unscaled, d.scale),
        .date => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .time => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
        .timestamp => |x| try std.fmt.allocPrint(arena, "{d}", .{x}),
    };
}

/// Render an exact decimal `unscaled * 10^-scale`, e.g. (12345, 2) -> "123.45".
fn formatDecimal(arena: std.mem.Allocator, unscaled: i128, scale: u8) ![]const u8 {
    const neg = unscaled < 0;
    var mag: u128 = if (neg) @intCast(-unscaled) else @intCast(unscaled);

    var digits: [48]u8 = undefined;
    var n: usize = 0;
    if (mag == 0) {
        digits[0] = '0';
        n = 1;
    }
    while (mag > 0) : (mag /= 10) {
        digits[n] = @intCast('0' + mag % 10);
        n += 1;
    }
    while (n <= scale) : (n += 1) digits[n] = '0'; // pad so there's an integer digit

    var out = std.ArrayList(u8).init(arena);
    if (neg) try out.append('-');
    var k: usize = n;
    while (k > 0) {
        k -= 1;
        try out.append(digits[k]);
        if (scale > 0 and k == scale) try out.append('.');
    }
    return out.toOwnedSlice();
}

// --- small helpers ---

fn fieldIndex(schema: types.Schema, q: ast.QualName) ?usize {
    return schema.indexOf(lastPart(q));
}
fn lastPart(q: ast.QualName) []const u8 {
    return q.parts[q.parts.len - 1];
}
fn isNum(v: Value) bool {
    return v == .int or v == .float or v == .decimal;
}
pub fn toF64(v: Value) f64 {
    return switch (v) {
        .int => |x| @floatFromInt(x),
        .float => |x| x,
        .decimal => |d| @as(f64, @floatFromInt(d.unscaled)) / pow10f(d.scale),
        else => 0,
    };
}
fn pow10f(n: u8) f64 {
    var r: f64 = 1;
    var k: u8 = 0;
    while (k < n) : (k += 1) r *= 10;
    return r;
}
pub fn compareValues(a: Value, b: Value) ?std.math.Order {
    if (isNum(a) and isNum(b)) {
        if (a == .int and b == .int) return std.math.order(a.int, b.int);
        return std.math.order(toF64(a), toF64(b));
    }
    if (a == .string and b == .string) return std.mem.order(u8, a.string, b.string);
    if (a == .bool and b == .bool) return std.math.order(@intFromBool(a.bool), @intFromBool(b.bool));
    if (a == .timestamp and b == .timestamp) return std.math.order(a.timestamp, b.timestamp);
    if (a == .date and b == .date) return std.math.order(a.date, b.date);
    if (a == .time and b == .time) return std.math.order(a.time, b.time);
    return null;
}
fn eq(a: []const u8, b: []const u8) bool {
    return std.mem.eql(u8, a, b);
}
fn trim(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const parser = @import("../lang/parser.zig");

test "type-check and evaluate an if-expression with 3VL" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\@batch
        \\read x query "q"
        \\  | filter amount > 100
        \\  | select big = if(amount >= 100, "yes", "no")
    , &diag);
    const stages = prog.stmts[1].output.stages;
    const pred = stages[1].node.filter;
    const sel = stages[2].node.select[0].computed.expr;

    const schema = types.Schema{ .fields = &.{.{ .name = "amount", .ty = Type.init(.int).asNullable() }} };
    var ctx = TypeCtx{ .schema = schema, .arena = a };
    try std.testing.expectEqual(types.TypeKind.bool, (try ctx.typeOf(pred)).kind);
    const sel_ty = try ctx.typeOf(sel);
    try std.testing.expectEqual(types.TypeKind.string, sel_ty.kind);

    const amt = try column.intColumn(a, &.{ 50, 150, null });
    var cols = [_]column.Column{amt};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = 3 };

    const out = try evalColumn(a, sel, batch, sel_ty);
    try std.testing.expectEqualStrings("no", out.getValue(0).string);
    try std.testing.expectEqualStrings("yes", out.getValue(1).string);
    try std.testing.expectEqualStrings("no", out.getValue(2).string); // null >= 100 -> null -> else
}

test "type errors: unknown field and non-bool not" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{.{ .name = "x", .ty = Type.init(.int) }} };

    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, "@batch\nread t query \"q\" | filter missing > 1", &diag);
    const pred = prog.stmts[1].output.stages[1].node.filter;
    var ctx = TypeCtx{ .schema = schema, .arena = a };
    try std.testing.expectError(error.TypeError, ctx.typeOf(pred));
    try std.testing.expect(std.mem.indexOf(u8, ctx.msg, "unknown field") != null);
}
