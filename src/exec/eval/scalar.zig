//! The scalar function factory: `Scalar(F)` turns a function's math into both forms
//! the evaluator calls, the row form (`eval`) and the vector kernel (`vec`), so a
//! function is written once. `F.args` lists its arguments' kinds, `F.apply(cx, ...)`
//! takes them non-null and returns the result, an optional result meaning NULL.
//! Every argument propagates NULL, the first NULL ending the row in argument order;
//! `F.variadic` instead takes any number of text arguments as one slice.
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
const std = @import("std");
const strAt = @import("vec.zig").strAt;
const toF64 = @import("support.zig").toF64;
const toI64 = @import("support.zig").toI64;
const valueToString = @import("format.zig").valueToString;

pub const Arg = enum {
    str,
    int,
    exact_int,
    num,
    opt_str,
    opt_int,

    fn Base(comptime a: Arg) type {
        return switch (a) {
            .str, .opt_str => []const u8,
            .int, .exact_int, .opt_int => i64,
            .num => f64,
        };
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
/// written to (from `mark` on; in a kernel, the output column itself), and whether
/// the first argument is BYTES (`length` counts bytes then).
pub const Cx = struct {
    arena: std.mem.Allocator,
    buf: *std.array_list.Managed(u8),
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
    for (argsOf(F), 0..) |a, i| fields[i] = a.T();
    return std.meta.Tuple(&fields);
}

fn Result(comptime F: type) type {
    const R = @typeInfo(@TypeOf(F.apply)).@"fn".return_type.?;
    return @typeInfo(R).error_union.payload;
}

fn Payload(comptime F: type) type {
    const R = Result(F);
    return if (@typeInfo(R) == .optional) @typeInfo(R).optional.child else R;
}

fn toValue(comptime P: type, x: P) Value {
    return switch (P) {
        []const u8 => .{ .string = x },
        i64 => .{ .int = x },
        f64 => .{ .float = x },
        bool => .{ .bool = x },
        else => @compileError("unsupported scalar result"),
    };
}

fn rowArg(comptime a: Arg, arena: std.mem.Allocator, v: Value) EvalError!a.Base() {
    return switch (a) {
        .str, .opt_str => try valueToString(arena, v),
        .int, .opt_int => toI64(v),
        .exact_int => if (v == .int) v.int else error.TypeMismatch,
        .num => toF64(v),
    };
}

/// An argument across a batch as the kernel reads it, or null when absent.
fn Operand(comptime a: Arg) type {
    return if (a.text()) ?Str else ?Num;
}

fn vecArg(comptime a: Arg, arena: std.mem.Allocator, v: Vec, n: usize) VecError!Operand(a) {
    if (comptime a.text()) return asStr(v) orelse error.Unsupported;
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

inline fn numAt(comptime a: Arg, x: Num, i: usize) ?a.Base() {
    return switch (x) {
        .icol => |c| if (c.v.get(i)) (if (a == .num) @as(f64, @floatFromInt(c.d[i])) else c.d[i]) else null,
        .fcol => |c| if (c.v.get(i)) (if (a == .num) c.d[i] else unreachable) else null,
        .iscalar => |s| if (a == .num) @as(f64, @floatFromInt(s)) else s,
        .fscalar => |s| if (a == .num) s else unreachable,
    };
}

fn isBytes(v: Vec) bool {
    return switch (v) {
        .col => |c| c.ty.kind == .bytes,
        .scalar => |s| s == .bytes,
    };
}

pub fn Scalar(comptime F: type) type {
    return struct {
        pub const eval = if (@hasDecl(F, "variadic")) evalMany else evalFixed;
        pub const vec = if (@hasDecl(F, "variadic")) vecMany else vecFixed;

        fn evalFixed(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
            var buf = std.array_list.Managed(u8).init(arena);
            var cx = Cx{ .arena = arena, .buf = &buf };
            var t: Args(F) = undefined;
            inline for (comptime argsOf(F), 0..) |a, i| {
                if (i >= c.args.len) {
                    if (comptime !a.optional()) return error.TypeMismatch;
                    t[i] = null;
                } else {
                    const v = try evalRow(arena, c.args[i], batch, row);
                    if (v.isNull()) return .null;
                    if (i == 0) cx.bytes = v == .bytes;
                    t[i] = try rowArg(a, arena, v);
                }
            }
            return wrap(try @call(.auto, F.apply, .{cx} ++ t));
        }

        fn evalMany(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
            var buf = std.array_list.Managed(u8).init(arena);
            const parts = try arena.alloc([]const u8, c.args.len);
            for (c.args, parts) |e, *p| {
                const v = try evalRow(arena, e, batch, row);
                if (v.isNull()) return .null;
                p.* = try valueToString(arena, v);
            }
            return wrap(try F.apply(.{ .arena = arena, .buf = &buf }, parts));
        }

        fn wrap(r: Result(F)) Value {
            if (@typeInfo(Result(F)) == .optional) {
                return if (r) |x| toValue(Payload(F), x) else .null;
            }
            return toValue(Payload(F), r);
        }

        fn vecFixed(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
            const n = batch.len;
            var out = try Out(Payload(F)).init(arena, n);
            var bm = try Bitmap.initFull(arena, n);
            var any = false;
            var cx = Cx{ .arena = arena, .buf = out.buf() };
            var ops: OperandTuple(F) = undefined;
            inline for (comptime argsOf(F), 0..) |a, i| {
                if (i >= c.args.len) {
                    if (comptime !a.optional()) return error.Unsupported;
                    ops[i] = null;
                } else {
                    const v = try evalVec(arena, c.args[i], batch);
                    if (i == 0) cx.bytes = isBytes(v);
                    ops[i] = try vecArg(a, arena, v, n);
                }
            }
            rows: for (0..n) |i| {
                cx.mark = out.begin();
                var t: Args(F) = undefined;
                inline for (comptime argsOf(F), 0..) |a, k| {
                    if (ops[k]) |op| {
                        t[k] = at(a, op, i) orelse {
                            try out.none(i, cx.mark, &bm);
                            any = true;
                            continue :rows;
                        };
                    } else if (comptime a.optional()) {
                        t[k] = null;
                    } else unreachable;
                }
                const r = @call(.auto, F.apply, .{cx} ++ t) catch |e| return unsupported(e);
                if (@typeInfo(Result(F)) == .optional) {
                    if (r) |x| try out.put(i, cx.mark, x) else {
                        try out.none(i, cx.mark, &bm);
                        any = true;
                    }
                } else try out.put(i, cx.mark, r);
            }
            return out.finish(n, bm, any);
        }

        fn vecMany(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
            const n = batch.len;
            if (c.args.len == 0) return error.Unsupported;
            var out = try Out(Payload(F)).init(arena, n);
            var bm = try Bitmap.initFull(arena, n);
            var any = false;
            var cx = Cx{ .arena = arena, .buf = out.buf() };
            const ops = try arena.alloc(Str, c.args.len);
            for (c.args, ops) |e, *o| o.* = asStr(try evalVec(arena, e, batch)) orelse return error.Unsupported;
            const parts = try arena.alloc([]const u8, ops.len);
            rows: for (0..n) |i| {
                cx.mark = out.begin();
                for (ops, parts) |o, *p| p.* = strAt(o, i) orelse {
                    try out.none(i, cx.mark, &bm);
                    any = true;
                    continue :rows;
                };
                try out.put(i, cx.mark, F.apply(cx, parts) catch |e| return unsupported(e));
            }
            return out.finish(n, bm, any);
        }

        inline fn at(comptime a: Arg, op: anytype, i: usize) ?a.Base() {
            if (comptime a.text()) return strAt(op, i);
            return numAt(a, op, i);
        }
    };
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
fn Out(comptime P: type) type {
    return switch (P) {
        []const u8 => struct {
            app: column.BytesAppender,
            fn init(arena: std.mem.Allocator, n: usize) !@This() {
                return .{ .app = try column.BytesAppender.init(arena, n) };
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
            fn none(o: *@This(), i: usize, mark: usize, bm: *Bitmap) !void {
                o.app.values.shrinkRetainingCapacity(mark);
                try o.app.pushNull();
                bm.setValid(i, false);
            }
            fn finish(o: *@This(), n: usize, bm: Bitmap, any: bool) !Vec {
                return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try o.app.finish() });
            }
        },
        else => struct {
            d: []P,
            scratch: std.array_list.Managed(u8),
            fn init(arena: std.mem.Allocator, n: usize) !@This() {
                return .{ .d = try arena.alloc(P, n), .scratch = .init(arena) };
            }
            fn buf(o: *@This()) *std.array_list.Managed(u8) {
                return &o.scratch;
            }
            fn begin(o: *@This()) usize {
                o.scratch.clearRetainingCapacity();
                return 0;
            }
            fn put(o: *@This(), i: usize, _: usize, x: P) !void {
                o.d[i] = x;
            }
            fn none(o: *@This(), i: usize, _: usize, bm: *Bitmap) !void {
                o.d[i] = std.mem.zeroes(P);
                bm.setValid(i, false);
            }
            fn finish(o: *@This(), n: usize, bm: Bitmap, any: bool) !Vec {
                return switch (P) {
                    i64 => mkCol(Type.init(.int).withNull(any), n, bm, .{ .i64 = o.d }),
                    f64 => mkCol(Type.init(.float).withNull(any), n, bm, .{ .f64 = o.d }),
                    bool => mkCol(Type.init(.bool).withNull(any), n, bm, .{ .b = o.d }),
                    else => @compileError("unsupported scalar result"),
                };
            }
        },
    };
}
