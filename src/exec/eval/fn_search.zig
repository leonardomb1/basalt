//! `search(cols, 'query'[, unaccent])`: whether a row holds the query's words
//! (`search.zig`) in the given columns, ignoring accents as well as case when
//! `unaccent` is true. The parser hands the columns over as string items of a
//! `search_cols` call — `*` every column, `*@t` the columns of `t` (`*@` the
//! FROM table's own, which carry no relation), `-name` one left out by `EXCEPT`,
//! `=col` / `=t.col` a listed one — and they are resolved here against the rows'
//! own schema. The query is text evaluated once per batch; a value is matched as
//! `valueToString` prints it.

const Batch = @import("../batch.zig").Batch;
const Bitmap = @import("vec.zig").Bitmap;
const EvalError = @import("../eval.zig").EvalError;
const Type = @import("../../lang/types.zig").Type;
const TypeCtx = @import("../eval.zig").TypeCtx;
const TypeError = @import("../eval.zig").TypeError;
const Value = @import("../value.zig").Value;
const Vec = @import("vec.zig").Vec;
const VecError = @import("vec.zig").VecError;
const ast = @import("../../lang/ast.zig");
const evalRow = @import("row.zig").evalRow;
const mkCol = @import("vec.zig").mkCol;
const search = @import("../search.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const valueToString = @import("format.zig").valueToString;

const Cols = struct { idx: []usize, names: [][]const u8 };

const Unresolved = union(enum) { unknown: []const u8, no_rel: []const u8, not_searched: []const u8 };

/// The searched columns of `schema`, in schema order for stars and as listed
/// otherwise; on failure, what could not be resolved.
fn resolve(arena: std.mem.Allocator, spec: ast.Expr.Call, schema: types.Schema, why: *Unresolved) error{ OutOfMemory, Unresolved }!Cols {
    var picked = std.array_list.Managed(usize).init(arena);
    for (spec.args) |a| {
        const item = a.str_lit;
        if (std.mem.eql(u8, item, "*")) {
            for (0..schema.fields.len) |i| try pick(&picked, i);
        } else if (std.mem.startsWith(u8, item, "*@")) {
            const rel = item[2..];
            var any = false;
            for (schema.fields, 0..) |f, i| if (std.mem.eql(u8, f.rel, rel)) {
                try pick(&picked, i);
                any = true;
            };
            if (!any) {
                why.* = .{ .no_rel = rel };
                return error.Unresolved;
            }
        } else if (std.mem.startsWith(u8, item, "-")) {
            const name = item[1..];
            var removed = false;
            var k: usize = 0;
            while (k < picked.items.len) {
                if (std.mem.eql(u8, schema.fields[picked.items[k]].name, name)) {
                    _ = picked.orderedRemove(k);
                    removed = true;
                } else k += 1;
            }
            if (!removed) {
                why.* = .{ .not_searched = name };
                return error.Unresolved;
            }
        } else if (std.mem.startsWith(u8, item, "=")) {
            var parts = std.array_list.Managed([]const u8).init(arena);
            var it = std.mem.splitScalar(u8, item[1..], '.');
            while (it.next()) |p| try parts.append(p);
            const i = schema.resolve(parts.items) orelse {
                why.* = .{ .unknown = item[1..] };
                return error.Unresolved;
            };
            try pick(&picked, i);
        }
    }
    const names = try arena.alloc([]const u8, picked.items.len);
    for (picked.items, names) |i, *n| n.* = schema.fields[i].name;
    return .{ .idx = picked.items, .names = names };
}

fn pick(list: *std.array_list.Managed(usize), i: usize) !void {
    for (list.items) |x| if (x == i) return;
    try list.append(i);
}

fn specOf(c: ast.Expr.Call) ?ast.Expr.Call {
    if (c.args.len < 2 or c.args.len > 3 or c.args[0].* != .call) return null;
    const s = c.args[0].call;
    if (!std.mem.eql(u8, s.name, "search_cols")) return null;
    for (s.args) |a| if (a.* != .str_lit) return null;
    return s;
}

/// The fold `unaccent` asks for at `row`: accents too when it is true, case alone
/// when it is false, null or left out.
fn foldAt(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!search.Fold {
    if (c.args.len < 3) return .case;
    return switch (try evalRow(arena, c.args[2], batch, row)) {
        .bool => |b| if (b) .accents else .case,
        else => .case,
    };
}

pub fn typeSearch(ctx: *TypeCtx, c: ast.Expr.Call) TypeError!Type {
    const spec = specOf(c) orelse return ctx.err("`search` takes the columns, then the text: `search(*, 'sp 2026')`", .{});
    if (c.args.len == 3) {
        const u = try ctx.typeOf(c.args[2]);
        if (!(u.kind == .bool or u.unknown)) return ctx.err("`search`'s third argument is a bool: true ignores accents as well as case", .{});
    }
    var why: Unresolved = undefined;
    _ = resolve(ctx.arena, spec, ctx.schema, &why) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.Unresolved => return switch (why) {
            .unknown => |n| ctx.err("`search`: unknown column `{s}`", .{n}),
            .no_rel => |r| ctx.err("`search`: no columns of `{s}` here", .{r}),
            .not_searched => |n| ctx.err("`search`: `{s}` in EXCEPT is not among the searched columns", .{n}),
        },
    };
    const q = try ctx.typeOf(c.args[1]);
    if (!(q.kind == .string or q.unknown)) return ctx.err("`search`'s text must be a string, got {s}", .{@tagName(q.kind)});
    return Type.init(.bool);
}

const Prepared = struct { cols: Cols, terms: []const search.Bound };

/// The columns and the bound query for `batch`, or null when the query is NULL.
fn prepare(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, query: ?[]const u8, fold: search.Fold) EvalError!?Prepared {
    const text = query orelse return null;
    const spec = specOf(c) orelse return error.TypeMismatch;
    var why: Unresolved = undefined;
    const cols = resolve(arena, spec, batch.schema.*, &why) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.Unresolved => return error.TypeMismatch,
    };
    var q = try search.Query.parse(arena, text);
    q.fold = fold;
    return .{ .cols = cols, .terms = try q.bind(arena, cols.names) };
}

const RowCtx = struct {
    arena: std.mem.Allocator,
    batch: Batch,
    cols: []const usize,
    row: usize,

    fn at(cx: @This(), k: usize) ?[]const u8 {
        const v = cx.batch.columns[cx.cols[k]].getValue(cx.row);
        if (v == .null) return null;
        return valueToString(cx.arena, v) catch null;
    }
};

pub fn rowSearch(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch, row: usize) EvalError!Value {
    const qv = try evalRow(arena, c.args[1], batch, row);
    const text: ?[]const u8 = switch (qv) {
        .null => null,
        .string => |s| s,
        else => try valueToString(arena, qv),
    };
    const p = try prepare(arena, c, batch, text, try foldAt(arena, c, batch, row)) orelse return .{ .bool = false };
    return .{ .bool = search.matches(p.terms, p.cols.idx.len, RowCtx{ .arena = arena, .batch = batch, .cols = p.cols.idx, .row = row }, RowCtx.at) };
}

/// One query for the whole batch: a text or an `unaccent` that varies by row goes
/// row by row.
pub fn vecSearch(arena: std.mem.Allocator, c: ast.Expr.Call, batch: Batch) VecError!Vec {
    if (c.args.len < 2 or !(c.args[1].* == .str_lit or c.args[1].* == .null_lit)) return error.Unsupported;
    const fold: search.Fold = if (c.args.len < 3) .case else switch (c.args[2].*) {
        .bool_lit => |b| if (b) .accents else .case,
        .null_lit => .case,
        else => return error.Unsupported,
    };
    const text: ?[]const u8 = if (c.args[1].* == .str_lit) c.args[1].str_lit else null;
    const n = batch.len;
    const out = try arena.alloc(bool, n);
    const p = try prepare(arena, c, batch, text, fold) orelse {
        @memset(out, false);
        return mkCol(Type.init(.bool), n, try Bitmap.initFull(arena, n), .{ .b = out });
    };
    var scratch = std.heap.ArenaAllocator.init(arena);
    defer scratch.deinit();
    for (out, 0..) |*o, r| {
        o.* = search.matches(p.terms, p.cols.idx.len, RowCtx{ .arena = scratch.allocator(), .batch = batch, .cols = p.cols.idx, .row = r }, RowCtx.at);
        if (r % 256 == 255) _ = scratch.reset(.retain_capacity);
    }
    return mkCol(Type.init(.bool), n, try Bitmap.initFull(arena, n), .{ .b = out });
}
