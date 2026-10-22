//! Expression builders shared by tests that assemble ASTs by hand.

const ast = @import("../lang/ast.zig");
const std = @import("std");

pub fn fld(arena: std.mem.Allocator, name: []const u8) !*ast.Expr {
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    const e = try arena.create(ast.Expr);
    e.* = .{ .field = .{ .parts = parts } };
    return e;
}

pub fn bin(arena: std.mem.Allocator, op: ast.BinOp, l: *ast.Expr, r: *ast.Expr) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .binary = .{ .op = op, .l = l, .r = r } };
    return e;
}
