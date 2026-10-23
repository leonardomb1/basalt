//! The scalar function factory: `Scalar(F)` turns a function's math into both forms
//! the evaluator calls, the row form (`eval`) and the vector kernel (`vec`), so a
//! function is written once. `F.args` lists its arguments' kinds, `F.apply(cx, ...)`
//! takes them non-null and returns the result, an optional result meaning NULL.
//!
//! The result's kind follows `apply`'s type (text, `i64` INT, `f64` FLOAT, `bool`,
//! `i32` DATE, `Decimal` DECIMAL) unless `F.result` names another kind stored the
//! same way (BYTES for text, TIME or TIMESTAMP for `i64`). `F.resultType(call)`, when
//! present, gives the whole type from the call's literal arguments (a DECIMAL's
//! precision and scale); the typing rule should return the same, as both forms type
//! their result by it, and a decimal is brought to its scale in either form.
//!
//! Arguments may also be a DECIMAL (`dec`), a DATE as days (`day`) or a TIME or
//! TIMESTAMP as microseconds (`micros`). `F.null_first` hands a NULL first argument
//! to `apply` as null instead of ending the row, for an encoder that spells NULL. A
//! variadic `F.bytes_if_any` returns BYTES when any argument is BYTES. `typeRule(F)`
//! is the typing rule the arguments imply, `F.check` adding what literals must be.
//! Every argument propagates NULL, the first NULL ending the row in argument order
//! (`F.eval_all` evaluates every argument first, and it and `F.typed_last` read the
//! arguments as their kinds only once all are evaluated); `F.variadic` instead takes any
//! number of text arguments as one slice, `F.skip_null` leaving out a NULL past the
//! first instead of ending the row. `F.value_scale` keeps each DECIMAL result
//! at the scale `apply` gave it. `Overload` picks one of several declarations by an
//! argument's kind, and `Numeric` builds the one-number functions from their math.
//!
//! The row form reads an argument as SQL does for that kind: text through
//! `valueToString` (so `upper(123)` is '123'), an INT through `toI64`, a number
//! through `toF64`. The vector kernel takes only the columns it reads natively (text
//! and bytes, ints, floats); anything else, and any error `apply` raises, sends the
//! batch back to the row form, which gives the same answer or raises the error with
//! its message. A text result is written to `cx.buf` (`extend`, then `written`):
//! in a kernel that is the output column, so it is committed where it lies; a
//! result pointing anywhere else, an argument or the arena, is copied in.

const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;
const Filled = @import("vec.zig").Filled;
const EvalError = @import("../eval.zig").EvalError;
const Num = @import("vec.zig").Num;
const Str = @import("vec.zig").Str;
const Type = @import("../../lang/types.zig").Type;
const Value = @import("../value.zig").Value;
const Vec = @import("vec.zig").Vec;
const VecError = @import("vec.zig").VecError;
const asNum = @import("vec.zig").asNum;
const asStr = @import("vec.zig").asStr;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const evalRow = @import("row.zig").evalRow;
const evalVec = @import("vec.zig").evalVec;
const mkCol = @import("vec.zig").mkCol;
const numAt = @import("vec.zig").numAt;
const numValid = @import("vec.zig").numValid;
const std = @import("std");
const strAt = @import("vec.zig").strAt;
const toF64 = @import("support.zig").toF64;
const toI64 = @import("support.zig").toI64;
const valueToString = @import("format.zig").valueToString;
const Decimal = @import("../value.zig").Decimal;
const rescaleTo = @import("cast.zig").rescaleTo;
const decimalOverflow = @import("support.zig").decimalOverflow;
const types = @import("../../lang/types.zig");
const TypeCtx = @import("../eval.zig").TypeCtx;
const TypeError = @import("../eval.zig").TypeError;
const intish = @import("../eval.zig").intish;
const numericish = @import("../eval.zig").numericish;
const temporalish = @import("../eval.zig").temporalish;
const TimeUnit = @import("time.zig").TimeUnit;
const timeUnit = @import("time.zig").timeUnit;
const temporalMicros = @import("time.zig").temporalMicros;

pub const Arg = enum {
    str,
    int,
    exact_int,
    num,
    opt_str,
    opt_int,
    dec,
    day,
    micros,
    temporal,
    unit,

    fn Base(comptime a: Arg) type {
        return switch (a) {
            .str, .opt_str => []const u8,
            .int, .exact_int, .opt_int, .micros, .temporal => i64,
            .num => f64,
            .dec => Decimal,
            .day => i32,
            .unit => TimeUnit,
        };
    }

    fn typed(comptime a: Arg) bool {
        return a == .dec or a == .day or a == .micros;
    }

    fn T(comptime a: Arg) type {
        return if (a.optional()) ?a.Base() else a.Base();
    }

    fn optional(comptime a: Arg) bool {
        return a == .opt_str or a == .opt_int;
    }

    fn text(comptime a: Arg) bool {
        return a == .str or a == .opt_str;
    }
};

/// What `apply` sees besides its arguments: the arena, the buffer a text result is
/// written to (from `mark` on; in a kernel, the output column itself), whether the
/// first argument is BYTES (`length` counts bytes then), and the call.
pub const Cx = struct {
    arena: std.mem.Allocator,
    buf: *std.array_list.Managed(u8),
    call: ast.Expr.Call,
    mark: usize = 0,
    bytes: bool = false,

    /// What this row has written so far.
    pub fn written(cx: Cx) []const u8 {
        return cx.buf.items[cx.mark..];
    }

    /// `n` more bytes for this row's result, to fill.
    pub fn extend(cx: Cx, n: usize) ![]u8 {
        const at = cx.buf.items.len;
        try cx.buf.resize(at + n);
        return cx.buf.items[at..];
    }
};

fn argsOf(comptime F: type) [F.args.len]Arg {
    return F.args;
}

fn Args(comptime F: type) type {
    var fields: [F.args.len]type = undefined;
    for (argsOf(F), 0..) |a, i| fields[i] = if (i == 0 and nullFirst(F) and !a.optional()) ?a.T() else a.T();
    return std.meta.Tuple(&fields);
}

fn nullFirst(comptime F: type) bool {
    return @hasDecl(F, "null_first") and F.null_first;
}

fn Result(comptime F: type) type {
    const R = @typeInfo(@TypeOf(F.apply)).@"fn".return_type.?;
    return @typeInfo(R).error_union.payload;
}

fn Payload(comptime F: type) type {
    const R = Result(F);
    return if (@typeInfo(R) == .optional) @typeInfo(R).optional.child else R;
}

/// The kind of `F`'s result: `F.result`, else the one its payload stores.
fn kindOf(comptime F: type) types.TypeKind {
    if (@hasDecl(F, "result")) return F.result;
    return switch (Payload(F)) {
        []const u8 => .string,
        i64 => .int,
        f64 => .float,
        bool => .bool,
        i32 => .date,
        Decimal => .decimal,
        else => @compileError("unsupported scalar result"),
    };
}

/// The type of `F`'s result for this call. A `resultType(call, arg)` also reads the
/// type of the argument an overload picked its variant by.
pub fn resultType(comptime F: type, c: ast.Expr.Call, arg: ?Type) Type {
    if (@hasDecl(F, "resultType")) {
        if (comptime @typeInfo(@TypeOf(F.resultType)).@"fn".params.len == 2) return F.resultType(c, arg.?);
        return F.resultType(c);
    }
    return Type.init(kindOf(F));
}

fn flag(comptime F: type, comptime name: []const u8) bool {
    return @hasDecl(F, name) and @field(F, name);
}

fn toValue(comptime F: type, x: Payload(F), c: ast.Expr.Call) EvalError!Value {
    return switch (comptime kindOf(F)) {
        .string => .{ .string = x },
        .bytes => .{ .bytes = x },
        .int => .{ .int = x },
        .time => .{ .time = x },
        .timestamp => .{ .timestamp = x },
        .float => .{ .float = x },
        .bool => .{ .bool = x },
        .date => .{ .date = x },
        .decimal => .{ .decimal = if (comptime flag(F, "value_scale")) x else try atScale(x, resultType(F, c, null)) },
        else => @compileError("unsupported scalar result"),
    };
}

/// `d` at its column's scale, as every decimal of a column must be.
fn atScale(d: Decimal, ty: Type) EvalError!Decimal {
    if (d.scale == ty.scale) return d;
    return rescaleTo(d, ty.scale) orelse decimalOverflow();
}

fn rowArg(comptime a: Arg, arena: std.mem.Allocator, v: Value) EvalError!a.Base() {
    return switch (a) {
        .str, .opt_str => try valueToString(arena, v),
        .int, .opt_int => toI64(v),
        .exact_int => if (v == .int) v.int else error.TypeMismatch,
        .num => toF64(v),
        .dec => switch (v) {
            .decimal => |d| d,
            .int => |x| .{ .unscaled = x, .scale = 0 },
            else => error.TypeMismatch,
        },
        .day => if (v == .date) v.date else error.TypeMismatch,
        .micros => switch (v) {
            .time => |t| t,
            .timestamp => |t| t,
            else => error.TypeMismatch,
        },
        .temporal => temporalMicros(v) orelse error.TypeMismatch,
        .unit => if (v == .string) timeUnit(v.string) orelse error.TypeMismatch else error.TypeMismatch,
    };
}

/// A DECIMAL, DATE, TIME or TIMESTAMP argument across a batch: a column or a constant.
fn Typed(comptime E: type) type {
    return union(enum) {
        col: struct { d: []const E, v: Bitmap },
        scalar: E,
    };
}

/// A DATE or TIMESTAMP argument across a batch, read as microseconds row by row.
const Temporal = union(enum) {
    days: Typed(i32),
    micros: Typed(i64),
};

/// An argument across a batch as the kernel reads it, or null when absent.
fn Operand(comptime a: Arg) type {
    if (a == .unit) return ?TimeUnit;
    if (a == .temporal) return ?Temporal;
    return if (a.text()) ?Str else if (a.typed()) ?Typed(a.Base()) else ?Num;
}

fn vecArg(comptime a: Arg, arena: std.mem.Allocator, v: Vec, n: usize) VecError!Operand(a) {
    if (comptime a.text()) return asStr(v) orelse error.Unsupported;
    if (comptime a.typed()) return try typedArg(a, arena, v, n);
    if (comptime a == .unit) return if (v == .scalar and v.scalar == .string) timeUnit(v.scalar.string) orelse error.Unsupported else error.Unsupported;
    if (comptime a == .temporal) return switch (v) {
        .col => |c| switch (c.ty.kind) {
            .date => .{ .days = .{ .col = .{ .d = c.data.i32, .v = c.validity } } },
            .timestamp => .{ .micros = .{ .col = .{ .d = c.data.i64, .v = c.validity } } },
            else => error.Unsupported,
        },
        .scalar => |s| switch (s) {
            .date => |d| .{ .days = .{ .scalar = d } },
            .timestamp => |t| .{ .micros = .{ .scalar = t } },
            else => error.Unsupported,
        },
    };
    const x = (try asNum(arena, v, n)) orelse return error.Unsupported;
    const int = x == .icol or x == .iscalar;
    const float = x == .fcol or x == .fscalar;
    if (a == .num) {
        if (!int and !float) return error.Unsupported;
        const dec = switch (v) {
            .col => |col| col.ty.kind == .decimal,
            .scalar => |sc| sc == .decimal,
        };
        if (dec) return error.Unsupported;
    } else if (!int) return error.Unsupported;
    return x;
}

fn typedArg(comptime a: Arg, arena: std.mem.Allocator, v: Vec, n: usize) VecError!Typed(a.Base()) {
    const E = a.Base();
    switch (v) {
        .scalar => |s| return .{ .scalar = switch (a) {
            .dec => switch (s) {
                .decimal => |d| d,
                .int => |x| Decimal{ .unscaled = x, .scale = 0 },
                else => return error.Unsupported,
            },
            .day => if (s == .date) s.date else return error.Unsupported,
            .micros => switch (s) {
                .time => |t| t,
                .timestamp => |t| t,
                else => return error.Unsupported,
            },
            else => unreachable,
        } },
        .col => |c| switch (a) {
            .dec => switch (c.ty.kind) {
                .decimal => return .{ .col = .{ .d = c.data.dec, .v = c.validity } },
                .int => {
                    const out = try arena.alloc(E, n);
                    for (c.data.i64[0..n], out) |x, *o| o.* = .{ .unscaled = x, .scale = 0 };
                    return .{ .col = .{ .d = out, .v = c.validity } };
                },
                else => return error.Unsupported,
            },
            .day => return if (c.ty.kind == .date) .{ .col = .{ .d = c.data.i32, .v = c.validity } } else error.Unsupported,
            .micros => return if (c.ty.kind == .time or c.ty.kind == .timestamp) .{ .col = .{ .d = c.data.i64, .v = c.validity } } else error.Unsupported,
            else => unreachable,
        },
    }
}

fn isBytes(v: Vec) bool {
    return switch (v) {
        .col => |c| c.ty.kind == .bytes,
        .scalar => |s| s == .bytes,
    };
}

/// The typing rule `F`'s declaration implies: between its required and its total
/// argument count, each argument of its kind, and `resultType`'s type, nullable when an
/// argument is (a `null_first` one aside) or `F.may_null` says a value can come out
/// NULL. `F.signature` names the arguments in the arity message, and `F.check`, when
/// present, says what the call's literals must be.
pub fn typeRule(comptime F: type) fn (*TypeCtx, ast.Expr.Call) TypeError!Type {
    return struct {
        fn rule(self: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
            const args = comptime argsOf(F);
            const required = comptime blk: {
                var n = 0;
                for (args) |a| {
                    if (!a.optional()) n += 1;
                }
                break :blk n;
            };
            if (c.args.len < required or c.args.len > args.len) return self.err("`{s}` takes " ++ F.signature, .{c.name});
            var nullable = @hasDecl(F, "may_null") and F.may_null;
            inline for (args, 0..) |a, i| if (i < c.args.len) {
                const t = try self.argType(c, i);
                const ok = switch (a) {
                    .str, .opt_str => t.kind != .array and t.kind != .@"struct",
                    .int, .exact_int, .opt_int => intish(t),
                    .num => numericish(t),
                    .dec => t.kind == .decimal or intish(t),
                    .day => t.kind == .date,
                    .micros => t.kind == .time or t.kind == .timestamp,
                    .temporal => temporalish(t),
                    .unit => c.args[i].* == .str_lit and timeUnit(c.args[i].str_lit) != null,
                };
                if (!ok and !t.unknown) return self.err("`{s}` argument {d} must be {s}, got {s}", .{ c.name, i + 1, argWord(a), @tagName(t.kind) });
                if (t.nullable and !(i == 0 and nullFirst(F))) nullable = true;
            };
            if (@hasDecl(F, "check")) try F.check(self, c);
            return resultType(F, c, null).withNull(nullable);
        }
    }.rule;
}

fn argWord(comptime a: Arg) []const u8 {
    return switch (a) {
        .str, .opt_str => "text or bytes",
        .int, .exact_int, .opt_int => "an INT",
        .num => "a number",
        .dec => "a DECIMAL or INT",
        .day => "a DATE",
        .micros => "a TIME or TIMESTAMP",
        .temporal => "a DATE or TIMESTAMP",
        .unit => "a literal time unit",
    };
}

pub fn Scalar(comptime F: type) type {
    return struct {
        pub const eval = if (@hasDecl(F, "variadic")) evalMany else evalFixed;
        pub const vec = if (@hasDecl(F, "variadic")) vecMany else vecFixed;

        fn evalFixed(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
            return evalWith(arena, c, batch, row, &.{});
        }

        /// The row form, the arguments in `given` already evaluated (by an overload).
        pub fn evalWith(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize, given: []const ?Value) EvalError!Value {
            var buf = std.array_list.Managed(u8).init(arena);
            var cx = Cx{ .arena = arena, .buf = &buf, .call = c };
            const late = comptime flag(F, "eval_all") or flag(F, "typed_last");
            var vals: [F.args.len]Value = undefined;
            if (comptime flag(F, "eval_all")) {
                for (vals[0..@min(vals.len, c.args.len)], 0..) |*v, i| v.* = if (i < given.len and given[i] != null) given[i].? else try evalRow(arena, c.args[i], batch, row);
                for (vals[0..@min(vals.len, c.args.len)]) |v| if (v.isNull()) return .null;
            }
            var t: Args(F) = undefined;
            inline for (comptime argsOf(F), 0..) |a, i| {
                if (i >= c.args.len) {
                    if (comptime !a.optional()) return error.TypeMismatch;
                    t[i] = null;
                } else {
                    const v = if (comptime flag(F, "eval_all")) vals[i] else if (i < given.len and given[i] != null) given[i].? else try evalRow(arena, c.args[i], batch, row);
                    if (v.isNull()) {
                        if (comptime i == 0 and nullFirst(F)) t[i] = null else return .null;
                    } else {
                        if (i == 0) cx.bytes = v == .bytes;
                        if (late) vals[i] = v else t[i] = try rowArg(a, arena, v);
                    }
                }
            }
            if (late) inline for (comptime argsOf(F), 0..) |a, i| if (i < c.args.len) {
                t[i] = try rowArg(a, arena, vals[i]);
            };
            return wrap(try @call(.auto, F.apply, .{cx} ++ t), c);
        }

        fn evalMany(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
            var buf = std.array_list.Managed(u8).init(arena);
            const parts = try arena.alloc([]const u8, c.args.len);
            var any_bytes = false;
            var k: usize = 0;
            for (c.args, 0..) |e, i| {
                const v = try evalRow(arena, e, batch, row);
                if (v.isNull()) {
                    if (i > 0 and flag(F, "skip_null")) continue;
                    return .null;
                }
                any_bytes = any_bytes or v == .bytes;
                parts[k] = try valueToString(arena, v);
                k += 1;
            }
            const r = try F.apply(.{ .arena = arena, .buf = &buf, .call = c }, parts[0..k]);
            if (comptime @hasDecl(F, "bytes_if_any")) if (any_bytes) return .{ .bytes = r };
            return wrap(r, c);
        }

        fn wrap(r: Result(F), c: ast.Expr.Call) EvalError!Value {
            if (@typeInfo(Result(F)) == .optional) {
                return if (r) |x| toValue(F, x, c) else .null;
            }
            return toValue(F, r, c);
        }

        fn vecFixed(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
            return vecWith(arena, c, batch, &.{}, 0);
        }

        /// The kernel, the arguments in `given` already evaluated (by an overload,
        /// which picked its variant by argument `on`).
        pub fn vecWith(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, given: []const ?Vec, on: usize) VecError!Vec {
            const n = batch.len;
            var vecs: [F.args.len]?Vec = @splat(null);
            for (vecs[0..@min(vecs.len, c.args.len)], 0..) |*v, i| v.* = if (i < given.len and given[i] != null) given[i].? else try evalVec(arena, c.args[i], batch);
            var out = try Out(Payload(F), flag(F, "value_scale")).init(arena, n, resultType(F, c, if (on < vecs.len) if (vecs[on]) |v| vecType(v) else null else null));
            var cx = Cx{ .arena = arena, .buf = out.buf(), .call = c };
            var ops: OperandTuple(F) = undefined;
            inline for (comptime argsOf(F), 0..) |a, i| {
                if (vecs[i]) |v| {
                    if (i == 0) cx.bytes = isBytes(v);
                    ops[i] = try vecArg(a, arena, v, n);
                } else {
                    if (comptime !a.optional()) return error.Unsupported;
                    ops[i] = null;
                }
            }
            rows: for (0..n) |i| {
                cx.mark = out.begin();
                var t: Args(F) = undefined;
                inline for (comptime argsOf(F), 0..) |a, k| {
                    if (ops[k]) |op| {
                        if (at(a, op, i)) |x| {
                            t[k] = x;
                        } else if (comptime k == 0 and nullFirst(F)) {
                            t[k] = null;
                        } else {
                            try out.none(i, cx.mark);
                            continue :rows;
                        }
                    } else if (comptime a.optional()) {
                        t[k] = null;
                    } else unreachable;
                }
                const r = @call(.auto, F.apply, .{cx} ++ t) catch |e| return unsupported(e);
                if (@typeInfo(Result(F)) == .optional) {
                    if (r) |x| try out.put(i, cx.mark, x) else try out.none(i, cx.mark);
                } else try out.put(i, cx.mark, r);
            }
            return out.finish();
        }

        fn vecMany(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
            const n = batch.len;
            if (c.args.len == 0) return error.Unsupported;
            var out = try Out(Payload(F), false).init(arena, n, resultType(F, c, null));
            var cx = Cx{ .arena = arena, .buf = out.buf(), .call = c };
            const ops = try arena.alloc(Str, c.args.len);
            for (c.args, ops) |e, *o| {
                const v = try evalVec(arena, e, batch);
                if (comptime @hasDecl(F, "bytes_if_any")) if (isBytes(v)) {
                    out.ty = Type.init(.bytes);
                };
                o.* = asStr(v) orelse return error.Unsupported;
            }
            const parts = try arena.alloc([]const u8, ops.len);
            rows: for (0..n) |i| {
                cx.mark = out.begin();
                var k: usize = 0;
                for (ops, 0..) |o, j| {
                    parts[k] = strAt(o, i) orelse {
                        if (j > 0 and flag(F, "skip_null")) continue;
                        try out.none(i, cx.mark);
                        continue :rows;
                    };
                    k += 1;
                }
                try out.put(i, cx.mark, F.apply(cx, parts[0..k]) catch |e| return unsupported(e));
            }
            return out.finish();
        }

        inline fn at(comptime a: Arg, op: anytype, i: usize) ?a.Base() {
            if (comptime a.text()) return strAt(op, i);
            if (comptime a.typed()) return switch (op) {
                .scalar => |x| x,
                .col => |col| if (col.v.get(i)) col.d[i] else null,
            };
            if (comptime a == .unit) return op;
            if (comptime a == .temporal) return switch (op) {
                .days => |d| @as(i64, typedAt(i32, d, i) orelse return null) * 86_400_000_000,
                .micros => |us| typedAt(i64, us, i),
            };
            return if (numValid(op, i)) numAt(a.Base(), op, i) else null;
        }

        inline fn typedAt(comptime E: type, op: Typed(E), i: usize) ?E {
            return switch (op) {
                .scalar => |x| x,
                .col => |col| if (col.v.get(i)) col.d[i] else null,
            };
        }
    };
}

fn valueKind(v: Value) ?types.TypeKind {
    return switch (v) {
        .null => null,
        inline else => |_, tag| @field(types.TypeKind, @tagName(tag)),
    };
}

fn vecKind(v: Vec) ?types.TypeKind {
    return switch (v) {
        .col => |col| col.ty.kind,
        .scalar => |s| valueKind(s),
    };
}

/// The type of an evaluated argument: its column's, or a constant's own.
pub fn vecType(v: Vec) Type {
    return switch (v) {
        .col => |col| col.ty,
        .scalar => |s| switch (s) {
            .null => Type.unknownNull(),
            .decimal => |d| Type.decimal((ast.Expr.DecLit{ .unscaled = d.unscaled, .scale = d.scale }).precision(), d.scale),
            else => Type.init(valueKind(s).?),
        },
    };
}

/// One function over several kinds of argument: `O.variants` are `Scalar`
/// declarations with their arguments in the same places, and `O.pick(call, kind)`
/// names the one for the kind of argument `O.on` (the first by default), null when
/// none takes it: a TypeMismatch row by row, which the kernel leaves to the rows.
/// The arguments up to `O.on` are evaluated once, in order, a NULL ending the row
/// first (with `eval_all`, every argument is evaluated before NULL is looked at).
pub fn Overload(comptime O: type) type {
    const lead = O.variants[0];
    const on = if (@hasDecl(O, "on")) O.on else 0;
    return struct {
        pub fn eval(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
            var given: [lead.args.len]?Value = @splat(null);
            const upto = if (comptime flag(lead, "eval_all")) @min(given.len, c.args.len) else on + 1;
            for (given[0..upto], 0..) |*g, i| {
                const v = try evalRow(arena, c.args[i], batch, row);
                if (v.isNull() and !flag(lead, "eval_all")) return .null;
                g.* = v;
            }
            for (given[0..upto]) |g| if (g.?.isNull()) return .null;
            const k = O.pick(c, valueKind(given[on].?).?) orelse return error.TypeMismatch;
            inline for (O.variants, 0..) |F, i| if (i == k) return Scalar(F).evalWith(arena, c, batch, row, &given);
            unreachable;
        }

        pub fn vec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
            var given: [lead.args.len]?Vec = @splat(null);
            for (given[0..@min(given.len, c.args.len)], 0..) |*g, i| g.* = try evalVec(arena, c.args[i], batch);
            const kind = vecKind(given[on].?) orelse return .{ .scalar = .null };
            const k = O.pick(c, kind) orelse return error.Unsupported;
            inline for (O.variants, 0..) |F, i| if (i == k) return Scalar(F).vecWith(arena, c, batch, &given, on);
            unreachable;
        }
    };
}

/// A function of one number whose result has its argument's kind: `M.int`, `M.float`
/// and `M.dec` are its math for each, and `M.other`, when present, takes any other
/// value as `toF64` reads it (else such a value is a TypeMismatch). A DECIMAL
/// keeps each value's own scale, its column typed by `M.decType` from the
/// argument's (the same type by default).
pub fn Numeric(comptime M: type) type {
    const I = struct {
        pub const args = .{.exact_int};
        pub fn apply(_: Cx, x: i64) EvalError!i64 {
            return M.int(x);
        }
    };
    const Fl = struct {
        pub const args = .{.num};
        pub fn apply(_: Cx, x: f64) EvalError!f64 {
            return M.float(x);
        }
    };
    const D = struct {
        pub const args = .{.dec};
        pub const value_scale = true;
        pub fn resultType(_: ast.Expr.Call, arg: Type) Type {
            return if (@hasDecl(M, "decType")) M.decType(arg) else arg;
        }
        pub fn apply(_: Cx, d: Decimal) EvalError!Decimal {
            return M.dec(d);
        }
    };
    const Other = struct {
        pub const args = .{.num};
        pub fn apply(_: Cx, x: f64) EvalError!i64 {
            return M.other(x);
        }
    };
    return Overload(struct {
        pub const variants = if (@hasDecl(M, "other")) .{ I, Fl, D, Other } else .{ I, Fl, D };
        pub fn pick(_: ast.Expr.Call, k: types.TypeKind) ?usize {
            return switch (k) {
                .int => 0,
                .float => 1,
                .decimal => 2,
                else => if (@hasDecl(M, "other")) 3 else null,
            };
        }
    });
}

fn OperandTuple(comptime F: type) type {
    var fields: [F.args.len]type = undefined;
    for (argsOf(F), 0..) |a, i| fields[i] = Operand(a);
    return std.meta.Tuple(&fields);
}

fn unsupported(e: anyerror) VecError {
    return if (e == error.OutOfMemory) error.OutOfMemory else error.Unsupported;
}

/// The column a kernel fills, by its result type. A text column is also the buffer
/// `apply` writes to, so a result built there is committed where it lies.
fn Out(comptime P: type, comptime keep_scale: bool) type {
    return switch (P) {
        []const u8 => struct {
            app: column.BytesAppender,
            bm: Bitmap,
            ty: Type,
            any: bool = false,
            fn init(arena: std.mem.Allocator, n: usize, ty: Type) !@This() {
                return .{ .app = try column.BytesAppender.init(arena, n), .bm = try Bitmap.initFull(arena, n), .ty = ty };
            }
            fn buf(o: *@This()) *std.array_list.Managed(u8) {
                return &o.app.values;
            }
            fn begin(o: *@This()) usize {
                return o.app.values.items.len;
            }
            fn put(o: *@This(), _: usize, mark: usize, x: []const u8) !void {
                const vals = &o.app.values;
                const lo = @intFromPtr(vals.items.ptr) + mark;
                const p = @intFromPtr(x.ptr);
                if (x.len > 0 and p >= lo and p + x.len <= @intFromPtr(vals.items.ptr) + vals.items.len) {
                    const off = p - @intFromPtr(vals.items.ptr);
                    if (off != mark) std.mem.copyForwards(u8, vals.items[mark..], vals.items[off..][0..x.len]);
                    vals.shrinkRetainingCapacity(mark + x.len);
                } else {
                    vals.shrinkRetainingCapacity(mark);
                    try vals.appendSlice(x);
                }
                try o.app.endRow();
            }
            fn none(o: *@This(), i: usize, mark: usize) !void {
                o.app.values.shrinkRetainingCapacity(mark);
                try o.app.pushNull();
                o.bm.setValid(i, false);
                o.any = true;
            }
            fn finish(o: *@This()) !Vec {
                return mkCol(o.ty.withNull(o.any), o.bm.len, o.bm, .{ .bytes = try o.app.finish() });
            }
        },
        else => struct {
            f: Filled(P),
            scratch: std.array_list.Managed(u8),
            ty: Type,
            fn init(arena: std.mem.Allocator, n: usize, ty: Type) !@This() {
                return .{ .f = try .init(arena, n), .scratch = .init(arena), .ty = ty };
            }
            fn buf(o: *@This()) *std.array_list.Managed(u8) {
                return &o.scratch;
            }
            fn begin(o: *@This()) usize {
                o.scratch.clearRetainingCapacity();
                return 0;
            }
            fn put(o: *@This(), i: usize, _: usize, x: P) !void {
                o.f.d[i] = if (P == Decimal and !keep_scale) try atScale(x, o.ty) else x;
            }
            fn none(o: *@This(), i: usize, _: usize) !void {
                o.f.setNull(i);
            }
            fn finish(o: *@This()) !Vec {
                return o.f.finish(o.ty);
            }
        },
    };
}

const TestFns = struct {
    const Bytes = struct {
        pub const args = .{.str};
        pub const result = .bytes;
        pub fn apply(_: Cx, s: []const u8) EvalError![]const u8 {
            return s[0..@min(s.len, 2)];
        }
    };
    const Day = struct {
        pub const args = .{.str};
        pub fn apply(_: Cx, s: []const u8) EvalError!?i32 {
            if (s.len == 0) return null;
            return @intCast(s.len * 1000);
        }
    };
    const Stamp = struct {
        pub const args = .{.str};
        pub const result = .timestamp;
        pub fn apply(_: Cx, s: []const u8) EvalError!i64 {
            return @intCast(s.len * 1_000_000);
        }
    };
    const Clock = struct {
        pub const args = .{.str};
        pub const result = .time;
        pub fn apply(_: Cx, s: []const u8) EvalError!i64 {
            return @intCast(s.len * 1_000_000);
        }
    };
    const Money = struct {
        pub const args = .{ .str, .int };
        pub fn resultType(c: ast.Expr.Call) Type {
            return Type.decimal(18, @intCast(c.args[1].int_lit));
        }
        pub fn apply(_: Cx, s: []const u8, _: i64) EvalError!Decimal {
            return .{ .unscaled = @intCast(s.len), .scale = 0 };
        }
    };
};

fn expectFormsAgree(comptime F: type, a: std.mem.Allocator, call_src: []const u8, batch: Batch, want: Type) !void {
    const parser = @import("../../lang/sql_parser.zig");
    var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const e = try parser.parseExprStr(a, call_src, &diag);
    const v = try Scalar(F).vec(a, e.call, batch);
    const col = v.col;
    try std.testing.expectEqual(want.kind, col.ty.kind);
    try std.testing.expectEqual(want.scale, col.ty.scale);
    try std.testing.expectEqual(want.precision, col.ty.precision);
    for (0..batch.len) |i| {
        const row = try Scalar(F).eval(a, e.call, batch, i);
        const got = col.getValue(i);
        try std.testing.expect(std.meta.activeTag(row) == std.meta.activeTag(got));
        switch (row) {
            .null => {},
            .bytes => |b| try std.testing.expectEqualStrings(b, got.bytes),
            .date => |d| try std.testing.expectEqual(d, got.date),
            .time => |t| try std.testing.expectEqual(t, got.time),
            .timestamp => |t| try std.testing.expectEqual(t, got.timestamp),
            .decimal => |d| {
                try std.testing.expectEqual(want.scale, d.scale);
                try std.testing.expectEqual(d.unscaled, got.decimal.unscaled);
                try std.testing.expectEqual(d.scale, got.decimal.scale);
            },
            else => return error.TestUnexpectedResult,
        }
    }
}

test "results of every kind come out alike, row by row and vectorized, typed by the call" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const words = [_]?[]const u8{ "abc", "", null, "hello" };
    var app = try column.BytesAppender.init(a, words.len);
    var validity = try Bitmap.initFull(a, words.len);
    for (words, 0..) |w, i| {
        if (w) |s| {
            try app.values.appendSlice(s);
            try app.endRow();
        } else {
            try app.pushNull();
            validity.setValid(i, false);
        }
    }
    const scol = column.Column{ .ty = Type.init(.string).asNullable(), .len = words.len, .validity = validity, .data = .{ .bytes = try app.finish() } };
    const schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = Type.init(.string).asNullable() }} };
    var cols = [_]column.Column{scol};
    const batch = Batch{ .schema = &schema, .columns = &cols, .len = words.len };
    try expectFormsAgree(TestFns.Bytes, a, "f(s)", batch, Type.init(.bytes));
    try expectFormsAgree(TestFns.Day, a, "f(s)", batch, Type.init(.date));
    try expectFormsAgree(TestFns.Stamp, a, "f(s)", batch, Type.init(.timestamp));
    try expectFormsAgree(TestFns.Clock, a, "f(s)", batch, Type.init(.time));
    try expectFormsAgree(TestFns.Money, a, "f(s, 2)", batch, Type.decimal(18, 2));
}
