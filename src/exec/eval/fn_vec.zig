//! The vector kernels of the built-in functions that have one.

const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;
const Str = @import("vec.zig").Str;
const Type = @import("../../lang/types.zig").Type;
const Vec = @import("vec.zig").Vec;
const VecError = @import("vec.zig").VecError;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const mkCol = @import("vec.zig").mkCol;
const std = @import("std");
const civil = @import("../civil.zig");
const clockOf = @import("support.zig").clockOf;
const strArg = @import("vec.zig").strArg;
const strAt = @import("vec.zig").strAt;
const compareVecs = @import("vec.zig").compareVecs;
const evalVec = @import("vec.zig").evalVec;
const pickVec = @import("vec.zig").pickVec;
const vecType = @import("scalar.zig").vecType;
const TypeKind = @import("../../lang/types.zig").TypeKind;

pub const vectorized = struct {
    pub fn now(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        _ = arena;
        _ = batch;
        return .{ .scalar = .{ .timestamp = clockOf(c) } };
    }

    pub fn today(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        _ = arena;
        _ = batch;
        return .{ .scalar = .{ .date = @intCast(civil.dayOf(clockOf(c))) } };
    }

    pub fn coalesce(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const n = batch.len;
        if (c.args.len == 0) return error.Unsupported;
        const parts = try arena.alloc(Str, c.args.len);
        for (c.args, parts) |a, *sp| sp.* = try strArg(arena, a, batch);
        var out = try column.BytesAppender.init(arena, n);
        var bm = try Bitmap.initFull(arena, n);
        var any = false;
        var i: usize = 0;
        rows: while (i < n) : (i += 1) {
            for (parts) |sp| {
                if (strAt(sp, i)) |sv| {
                    try out.push(sv);
                    continue :rows;
                }
            }
            try out.pushNull();
            bm.setValid(i, false);
            any = true;
        }
        return mkCol(Type.init(.string).withNull(any), n, bm, .{ .bytes = try out.finish() });
    }

    /// NULL where `a = b`, else `a`, for operands of one kind (others run row by row).
    pub fn nullif(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
        const a = try evalVec(arena, c.args[0], batch);
        const b = try evalVec(arena, c.args[1], batch);
        const k = kindOf(a) orelse return error.Unsupported;
        if (kindOf(b) != k) return error.Unsupported;
        const take = try holds(arena, try compareVecs(arena, .eq, a, b, batch.len), batch.len);
        return typed(try pickVec(arena, take, .{ .scalar = .null }, a), vecType(a).asNullable());
    }

    /// The first of the greatest (or least) non-null arguments, for arguments of one
    /// kind (others run row by row).
    pub fn Extreme(comptime greatest: bool) type {
        return struct {
            pub fn vec(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
                const n = batch.len;
                var acc: ?Vec = null;
                var ty: ?Type = null;
                for (c.args) |e| {
                    const v = try evalVec(arena, e, batch);
                    if (v == .scalar and v.scalar == .null) continue;
                    const k = kindOf(v) orelse return error.Unsupported;
                    ty = if (ty) |t| Type.unify(t, vecType(v)) orelse return error.Unsupported else vecType(v);
                    const cur = acc orelse {
                        acc = v;
                        continue;
                    };
                    if (kindOf(cur) != k) return error.Unsupported;
                    const wins = try holds(arena, try compareVecs(arena, if (greatest) .gt else .lt, v, cur, n), n);
                    for (wins, 0..) |*w, i| w.* = validAt(v, i) and (w.* or !validAt(cur, i));
                    acc = try pickVec(arena, wins, v, cur);
                }
                return typed(acc orelse return .{ .scalar = .null }, ty.?.asNullable());
            }
        };
    }
};

/// `v` with its column typed `ty`, as the typing rule types it.
fn typed(v: Vec, ty: Type) Vec {
    var out = v;
    if (out == .col) out.col.ty = ty;
    return out;
}

/// The kind of an operand the comparison kernels order as `compareValues` does.
fn kindOf(v: Vec) ?TypeKind {
    const k: TypeKind = switch (v) {
        .col => |col| col.ty.kind,
        .scalar => |s| switch (s) {
            .int => .int,
            .float => .float,
            .decimal => .decimal,
            .string => .string,
            .date => .date,
            .timestamp => .timestamp,
            else => return null,
        },
    };
    return switch (k) {
        .int, .float, .decimal, .string, .date, .timestamp => k,
        else => null,
    };
}

fn validAt(v: Vec, i: usize) bool {
    return switch (v) {
        .col => |col| col.validity.get(i),
        .scalar => |s| !s.isNull(),
    };
}

/// The rows where a comparison holds: true, not NULL.
fn holds(arena: std.mem.Allocator, v: Vec, n: usize) VecError![]bool {
    const out = try arena.alloc(bool, n);
    switch (v) {
        .scalar => |s| @memset(out, s == .bool and s.bool),
        .col => |col| {
            if (col.ty.kind != .bool) return error.Unsupported;
            for (out, col.data.b[0..n], 0..) |*o, x, i| o.* = x and col.validity.get(i);
        },
    }
    return out;
}
