//! Test helpers shared by the tests of pushdown.zig's parts.

pub const Dialect = @import("../pushdown.zig").Dialect;
const TopN = @import("topn.zig").TopN;
const ast = @import("../../lang/ast.zig");
pub const base_t = @import("../pushdown.zig").base_t;
pub const bin = @import("../pushdown.zig").bin;
const classifyTopN = @import("topn.zig").classifyTopN;
pub const fld = @import("../pushdown.zig").fld;
const planWholeAggWhy = @import("../pushdown.zig").planWholeAggWhy;
pub const qfld = @import("../pushdown.zig").qfld;
const qual = @import("../pushdown.zig").qual;
pub const schema4_fields = @import("../pushdown.zig").schema4_fields;
const std = @import("std");
pub const test_schema_fields = @import("../pushdown.zig").test_schema_fields;
const testing = std.testing;
pub const topNStagesOf = @import("../pushdown.zig").topNStagesOf;
const types = @import("../../lang/types.zig");
pub const wholeSchema = @import("../pushdown.zig").wholeSchema;
pub fn testSchema() types.Schema {
    return .{ .fields = &test_schema_fields };
}

pub fn fieldItem(arena: std.mem.Allocator, name: []const u8) !ast.SelectItem {
    const parts = try arena.alloc([]const u8, 1);
    parts[0] = name;
    return .{ .field = .{ .parts = parts } };
}

pub fn selectStage(arena: std.mem.Allocator, items: []const ast.SelectItem) ast.Stage {
    _ = arena;
    return .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn schema4() types.Schema {
    return .{ .fields = &schema4_fields };
}

pub fn intLit(arena: std.mem.Allocator, v: i64) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .int_lit = v };
    return e;
}

pub fn strLit(arena: std.mem.Allocator, s: []const u8) !*ast.Expr {
    const e = try arena.create(ast.Expr);
    e.* = .{ .str_lit = s };
    return e;
}

pub fn callExpr(arena: std.mem.Allocator, name: []const u8, args: []const *ast.Expr) !*ast.Expr {
    const owned = try arena.alloc(*ast.Expr, args.len);
    @memcpy(owned, args);
    const e = try arena.create(ast.Expr);
    e.* = .{ .call = .{ .name = name, .args = owned } };
    return e;
}

pub fn geFilter(arena: std.mem.Allocator, col: []const u8, v: i64) !ast.Stage {
    const lit = try arena.create(ast.Expr);
    lit.* = .{ .int_lit = v };
    return .{ .node = .{ .filter = try bin(arena, .ge, try fld(arena, col), lit) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn byList(arena: std.mem.Allocator, names: []const []const u8) ![]ast.QualName {
    const by = try arena.alloc(ast.QualName, names.len);
    for (names, by) |n, *q| {
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = n;
        q.* = .{ .parts = parts };
    }
    return by;
}

pub fn expectWholeAggRefused(a: std.mem.Allocator, d: Dialect, ag: ast.Aggregate, plan_schema: types.Schema, want: []const u8) !void {
    var why: []const u8 = "";
    try testing.expect((try planWholeAggWhy(a, d, base_t, wholeSchema(), &.{}, ag, plan_schema, null, &why)) == null);
    if (!std.mem.startsWith(u8, why, want)) {
        std.debug.print("expected a refusal starting `{s}`, got `{s}`\n", .{ want, why });
        return error.TestUnexpectedResult;
    }
}

pub fn eqFilter(arena: std.mem.Allocator, parts: []const []const u8, v: i64) !ast.Stage {
    const lit = try arena.create(ast.Expr);
    lit.* = .{ .int_lit = v };
    return .{ .node = .{ .filter = try bin(arena, .eq, try qfld(arena, parts), lit) }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn joinStage(arena: std.mem.Allocator, kind: ast.JoinKind, binding: []const u8, lk: []const u8, rk: []const u8) !ast.Stage {
    const lefts = try arena.alloc(ast.QualName, 1);
    lefts[0] = try qual(arena, &.{lk});
    const rights = try arena.alloc(ast.QualName, 1);
    rights[0] = try qual(arena, &.{rk});
    return .{ .node = .{ .join = .{ .kind = kind, .binding = binding, .left_keys = lefts, .right_keys = rights } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn dimBindings(arena: std.mem.Allocator) !std.StringHashMap(ast.Pipeline) {
    var m = std.StringHashMap(ast.Pipeline).init(arena);
    const items = try arena.alloc(ast.SelectItem, 2);
    items[0] = .{ .field = try qual(arena, &.{"rk"}) };
    items[1] = .{ .field = try qual(arena, &.{"name"}) };
    const stages = try arena.alloc(ast.Stage, 2);
    stages[0] = .{ .node = .{ .read = .{ .connector = "csv", .form = .{ .path = "dim.csv" } } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    stages[1] = .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    try m.put("r", .{ .stages = stages, .pos = .{ .line = 0, .col = 0 } });
    return m;
}

pub fn readStage() ast.Stage {
    return .{ .node = .{ .read = .{ .connector = "mysql", .form = .{ .table = .{ .parts = &.{"t"} } } } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn writeStage() ast.Stage {
    return .{ .node = .{ .write = .{ .connector = "stdout", .form = null, .target = "", .mode = .default } }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn projStage(items: []const ast.SelectItem) ast.Stage {
    return .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn filterStage(e: *ast.Expr) ast.Stage {
    return .{ .node = .{ .filter = e }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
}

pub fn topNOf(arena: std.mem.Allocator, sql: []const u8, why: *[]const u8) !?TopN {
    return classifyTopN(arena, try topNStagesOf(arena, sql), why);
}
