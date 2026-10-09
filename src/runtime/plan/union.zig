//! UNION, UNION BY NAME and the set operations: each branch planned, its columns
//! reconciled to one canon schema, tags and null fills added.

const Env = @import("../env.zig").Env;
const PipeRes = @import("../env.zig").PipeRes;
const ast = @import("../../lang/ast.zig");
const buildPipeline = @import("../plan.zig").buildPipeline;
const buildProject = @import("../plan.zig").buildProject;
const buildStage = @import("../plan.zig").buildStage;
const discoverRows = @import("discover.zig").discoverRows;
const discoverRowsPipeline = @import("discover.zig").discoverRowsPipeline;
const dupeSchema = @import("../connect.zig").dupeSchema;
const exceptColumns = @import("../connect.zig").exceptColumns;
const forHintIdent = @import("../env.zig").forHintIdent;
const forHintName = @import("../env.zig").forHintName;
const mk = @import("../env.zig").mk;
const obs = @import("../obs.zig");
const op = @import("../../exec/op.zig");
const openSource = @import("../connect.zig").openSource;
const planErr = @import("../env.zig").planErr;
const readName = @import("../plan.zig").readName;
const sourceLabel = @import("../connect.zig").sourceLabel;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// The per-branch reconcile projection: an optional tag literal, then every canon
/// column cast to its type from the source field, else NULL.
pub fn synthReconcile(arena: std.mem.Allocator, src: types.Schema, canon: types.Schema, tag_col: ?[]const u8, tag_val: ?[]const u8) ![]const ast.SelectItem {
    var items = std.array_list.Managed(ast.SelectItem).init(arena);
    if (tag_col) |tc|
        try items.append(.{ .computed = .{ .name = tc, .expr = try mk(arena, .{ .str_lit = tag_val orelse "" }) } });
    for (canon.fields) |cf| {
        var present = false;
        for (src.fields) |sf| {
            if (std.mem.eql(u8, sf.name, cf.name)) {
                present = true;
                break;
            }
        }
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = cf.name;
        const inner = if (present) try mk(arena, .{ .field = .{ .parts = parts } }) else try mk(arena, .null_lit);
        const e = try mk(arena, .{ .cast = .{ .e = inner, .ty = cf.ty } });
        try items.append(.{ .computed = .{ .name = cf.name, .expr = e } });
    }
    return items.toOwnedSlice();
}

const UnionSpec = struct { read: ast.Read, tag: ?[]const u8, name: []const u8, pipeline: ?ast.Pipeline = null };

/// A union's branch list: explicit branches, or tables discovered via a
/// `(table_name, tag)` query.
pub fn unionSpecs(env: *Env, u: ast.Union, hints: []const ast.Hint) ![]UnionSpec {
    const arena = env.arena;
    var specs = std.array_list.Managed(UnionSpec).init(arena);
    const where = forHintName(hints, "where") orelse "";
    if (u.discover_json.len > 0) {
        const table_key = forHintName(hints, "table_field");
        const tag_key = forHintName(hints, "tag_field");
        const tag_substr = forHintName(hints, "tag_substr");
        const parsed = std.json.parseFromSliceLeaky(std.json.Value, arena, u.discover_json, .{}) catch
            return planErr(env.diag, try std.fmt.allocPrint(arena, "union json: invalid JSON: {s}", .{u.discover_json}));
        const items = switch (parsed) {
            .array => |a| a.items,
            else => return planErr(env.diag, "union json: expected a JSON array"),
        };
        for (items) |elem| {
            var tbl: []const u8 = undefined;
            var tag: ?[]const u8 = null;
            switch (elem) {
                .string => |s| tbl = s,
                .object => |o| {
                    tbl = (if (table_key) |k| jsonStrField(o, k) else null) orelse
                        jsonStrField(o, "table") orelse jsonStrField(o, "name") orelse
                        return planErr(env.diag, "union json: element has no table name");
                    tag = (if (tag_key) |k| jsonStrField(o, k) else null) orelse
                        jsonStrField(o, "tag") orelse jsonStrField(o, "emp");
                },
                else => return planErr(env.diag, "union json: each element must be a string or object"),
            }
            if (tag == null) if (tag_substr) |spec| {
                tag = deriveSubstr(tbl, spec);
            };
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = tbl;
            try specs.append(.{
                .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } }, .where = where },
                .tag = tag,
                .name = tbl,
            });
        }
    } else if (u.discover_pipeline) |pipe| {
        for (try discoverRowsPipeline(env, pipe, 2)) |row| {
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = row[0];
            try specs.append(.{ .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } }, .where = where }, .tag = row[1], .name = row[0] });
        }
    } else if (u.discover_query.len > 0) {
        const disc = ast.Read{ .connector = u.discover_conn, .form = .{ .query = u.discover_query } };
        for (try discoverRows(env, disc, 2)) |row| {
            const parts = try arena.alloc([]const u8, 1);
            parts[0] = row[0];
            try specs.append(.{ .read = .{ .connector = u.discover_conn, .form = .{ .table = .{ .parts = parts } }, .where = where }, .tag = row[1], .name = row[0] });
        }
    } else for (u.branches) |b| {
        var rd = b.read;
        if (where.len > 0) rd.where = where;
        try specs.append(.{
            .read = rd,
            .tag = b.tag,
            .name = if (b.pipeline != null) "query" else readName(b.read),
            .pipeline = b.pipeline,
        });
    }
    return specs.toOwnedSlice();
}

fn jsonStrField(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    return switch (obj.get(key) orelse return null) {
        .string => |s| s,
        else => null,
    };
}

/// A substring of `s` from a `"start,len"` spec, 1-based like `substr`; null on a
/// malformed or out-of-range spec.
fn deriveSubstr(s: []const u8, spec: []const u8) ?[]const u8 {
    const comma = std.mem.indexOfScalar(u8, spec, ',') orelse return null;
    const start = std.fmt.parseInt(usize, std.mem.trim(u8, spec[0..comma], " "), 10) catch return null;
    const len = std.fmt.parseInt(usize, std.mem.trim(u8, spec[comma + 1 ..], " "), 10) catch return null;
    if (start == 0 or start > s.len) return null;
    const a = start - 1;
    return s[a..@min(a + len, s.len)];
}

pub fn unionCanon(env: *Env, specs: []const UnionSpec, schemas: []const types.Schema, canon_opt: ?[]const u8, except: []const []const u8) !types.Schema {
    if (canon_opt) |c| if (!std.mem.eql(u8, c, "first")) {
        for (specs, schemas) |s, sch| if (std.mem.eql(u8, s.name, c)) return dropExcept(env.arena, sch, except);
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "union canon `{s}` is not one of the source tables", .{c}));
    };
    const canon = @constCast((try dropExcept(env.arena, schemas[0], except)).fields);
    for (schemas[1..]) |sch| {
        for (canon) |*f| {
            const other = sch.indexOf(f.name) orelse continue;
            const ot = sch.fields[other].ty;
            f.ty = types.Type.unify(f.ty, ot) orelse return planErr(env.diag, try std.fmt.allocPrint(
                env.arena,
                "union: column `{s}` is {s} in one branch and {s} in another, with no common type",
                .{ f.name, @tagName(f.ty.kind), @tagName(ot.kind) },
            ));
        }
    }
    return .{ .fields = canon };
}

/// A positional `UNION`: the first branch's names, each column widened across
/// branches. Column counts must agree; padding would misalign later columns.
fn positionalCanon(env: *Env, schemas: []const types.Schema, set: ast.SetOp) !types.Schema {
    const op_name = setOpName(set);
    const hint: []const u8 = if (set == .union_all) " (`UNION ALL BY NAME` matches them by name)" else "";
    const canon = try dupeSchema(env.arena, schemas[0]);
    const fields = @constCast(canon.fields);
    for (schemas[1..], 2..) |sch, n| {
        if (sch.fields.len != fields.len) return planErr(env.diag, try std.fmt.allocPrint(
            env.arena,
            "{s}: branch {d} has {d} columns and the first has {d}; {s} lines columns up by position{s}",
            .{ op_name, n, sch.fields.len, fields.len, op_name, hint },
        ));
        for (fields, sch.fields, 1..) |*f, of, col| {
            f.ty = types.Type.unify(f.ty, of.ty) orelse return planErr(env.diag, try std.fmt.allocPrint(
                env.arena,
                "{s}: column {d} (`{s}`) is {s} in the first branch and {s} in branch {d}, with no common type",
                .{ op_name, col, f.name, @tagName(f.ty.kind), @tagName(of.ty.kind), n },
            ));
        }
    }
    return canon;
}

pub fn setOpName(set: ast.SetOp) []const u8 {
    return switch (set) {
        .union_all => "UNION",
        .intersect => "INTERSECT",
        .except => "EXCEPT",
    };
}

fn positionalReconcile(arena: std.mem.Allocator, src: types.Schema, canon: types.Schema) ![]const ast.SelectItem {
    const items = try arena.alloc(ast.SelectItem, canon.fields.len);
    for (items, src.fields, canon.fields) |*it, sf, cf| {
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = sf.name;
        const e = try mk(arena, .{ .cast = .{ .e = try mk(arena, .{ .field = .{ .parts = parts } }), .ty = cf.ty } });
        it.* = .{ .computed = .{ .name = cf.name, .expr = e } };
    }
    return items;
}

fn dropExcept(arena: std.mem.Allocator, schema: types.Schema, except: []const []const u8) !types.Schema {
    var kept = std.array_list.Managed(types.Schema.Field).init(arena);
    for (schema.fields) |f| {
        var out = false;
        for (except) |x| if (std.ascii.eqlIgnoreCase(x, f.name)) {
            out = true;
        };
        if (!out) try kept.append(f);
    }
    return .{ .fields = try kept.toOwnedSlice() };
}

pub fn unionDownstreamMapOnly(stages: []const ast.Stage) bool {
    for (stages) |s| switch (s.node) {
        .filter, .select, .explode => {},
        else => return false,
    };
    return true;
}

/// `... WHERE rn <= k` over a `ROW_NUMBER()` binding: rebuild its window with
/// `top_k`. Sorting every row took 2 GB and 6 s over 10M rows; null if inapplicable.
pub fn windowTopK(arena: std.mem.Allocator, bstages: []const ast.Stage, after: []const ast.Stage) !?[]const ast.Stage {
    if (after.len == 0 or after[0].node != .filter) return null;
    var wi = bstages.len;
    while (wi > 0) {
        wi -= 1;
        if (bstages[wi].node == .window) break;
    } else return null;
    if (bstages[wi].node != .window) return null;
    const wd = bstages[wi].node.window;
    if (wd.funcs.len != 1 or wd.funcs[0].func != .win or wd.funcs[0].func.win != .row_number) return null;
    const rn = wd.funcs[0].out;
    for (bstages[wi + 1 ..]) |st| {
        if (st.node != .select) return null;
        var kept = false;
        for (st.node.select) |it| switch (it) {
            .star => kept = true,
            .field => |q| {
                if (std.mem.eql(u8, q.last(), rn)) kept = true;
            },
            else => {},
        };
        if (!kept) return null;
    }
    const k = rankBound(after[0].node.filter, rn) orelse return null;
    const out = try arena.dupe(ast.Stage, bstages);
    var w = wd;
    w.top_k = k;
    out[wi].node = .{ .window = w };
    return out;
}

/// The highest rank a filter keeps, from an AND-conjunct `rn <= c`, `rn < c`,
/// `rn = c` or the same written the other way round.
fn rankBound(e: *const ast.Expr, rn: []const u8) ?u64 {
    if (e.* != .binary) return null;
    const b = e.binary;
    if (b.op == .@"and") {
        const l = rankBound(b.l, rn);
        const r = rankBound(b.r, rn);
        if (l != null and r != null) return @min(l.?, r.?);
        return l orelse r;
    }
    const field_left = b.l.* == .field and b.l.field.parts.len == 1 and std.mem.eql(u8, b.l.field.last(), rn);
    const field_right = b.r.* == .field and b.r.field.parts.len == 1 and std.mem.eql(u8, b.r.field.last(), rn);
    const lit = if (field_left) b.r else if (field_right) b.l else return null;
    if (lit.* != .int_lit) return null;
    const c = lit.int_lit;
    const cmp: ast.BinOp = if (field_left) b.op else switch (b.op) {
        .ge => .le,
        .gt => .lt,
        .le => .ge,
        .lt => .gt,
        else => b.op,
    };
    return switch (cmp) {
        .le, .eq => if (c < 0) 0 else @intCast(c),
        .lt => if (c <= 0) 0 else @intCast(c - 1),
        else => null,
    };
}

pub fn unionExceptNames(after: []const ast.Stage) []const []const u8 {
    if (after.len == 0 or after[0].node != .select) return &.{};
    for (after[0].node.select) |it| {
        if (it == .star_except) return it.star_except;
    }
    return &.{};
}

/// The serial union: every branch opened, reconciled to the canon and drained in
/// order; used when splitting does not apply. Positional unions leave EXCEPT downstream.
pub fn buildUnion(env: *Env, u: ast.Union, hints: []const ast.Hint, except_names: []const []const u8) anyerror!PipeRes {
    const arena = env.arena;
    const except: []const []const u8 = if (u.positional) &.{} else except_names;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const children = try arena.alloc(op.Op, specs.len);
    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs) |*s| if (except.len > 0 and s.pipeline == null) {
        if (try exceptColumns(env, s.read, hints, except)) |cols| s.read.cols = cols;
    };
    for (specs, 0..) |s, i| {
        if (s.pipeline) |p| {
            const r = try buildPipeline(env, p.stages);
            children[i] = r.op;
            schemas[i] = r.schema;
            continue;
        }
        const raw = try openSource(env, s.read, hints);
        const cs = try arena.create(obs.CountingSource);
        cs.* = .{ .inner = raw, .count = env.rows_read };
        const src = cs.source();
        try env.sources.append(src);
        if (env.src_name.len == 0) env.src_name = sourceLabel(env, s.read, hints);
        const scan = try arena.create(op.Scan);
        scan.* = .{ .src = src };
        children[i] = .{ .scan = scan };
        schemas[i] = src.schema();
    }
    const canon = if (u.positional)
        try positionalCanon(env, schemas, u.set)
    else
        try dupeSchema(arena, try unionCanon(env, specs, schemas, canon_opt, except));

    var out_schema: types.Schema = undefined;
    for (specs, 0..) |s, i| {
        var items = if (u.positional)
            try positionalReconcile(arena, schemas[i], canon)
        else
            try synthReconcile(arena, schemas[i], canon, tag_col, s.tag);
        if (u.set != .union_all) items = try withSideMarks(arena, items, i);
        const proj = try buildProject(env, items, schemas[i], children[i]);
        children[i] = proj.op;
        out_schema = proj.schema;
    }
    const un = try arena.create(op.Union);
    un.* = .{ .children = children };
    if (u.set == .union_all) return .{ .op = .{ .union_ = un }, .schema = out_schema };
    return buildSetOp(env, u.set, canon, .{ .union_ = un }, out_schema);
}

const side_cols = [2][]const u8{ "__set_left", "__set_right" };

fn withSideMarks(arena: std.mem.Allocator, items: []const ast.SelectItem, branch: usize) ![]const ast.SelectItem {
    const out = try arena.alloc(ast.SelectItem, items.len + 2);
    @memcpy(out[0..items.len], items);
    for (side_cols, 0..) |name, side| {
        const v: i64 = if (side == branch) 1 else 0;
        out[items.len + side] = .{ .computed = .{ .name = name, .expr = try mk(arena, .{ .int_lit = v }) } };
    }
    return out;
}

fn buildSetOp(env: *Env, set: ast.SetOp, canon: types.Schema, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    const by = try arena.alloc(ast.QualName, canon.fields.len);
    const keep = try arena.alloc(ast.SelectItem, canon.fields.len);
    for (canon.fields, by, keep) |f, *q, *k| {
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = f.name;
        q.* = .{ .parts = parts };
        k.* = .{ .field = q.* };
    }
    const aggs = try arena.alloc(ast.AggItem, 2);
    var counted: [2]*ast.Expr = undefined;
    for (side_cols, aggs, &counted) |name, *a, *c| {
        const parts = try arena.alloc([]const u8, 1);
        parts[0] = name;
        a.* = .{ .name = name, .func = .sum, .arg = try mk(arena, .{ .field = .{ .parts = parts } }) };
        c.* = try mk(arena, .{ .field = .{ .parts = parts } });
    }
    const zero = try mk(arena, .{ .int_lit = 0 });
    const left_has = try mk(arena, .{ .binary = .{ .op = .gt, .l = counted[0], .r = zero } });
    const right_has = try mk(arena, .{ .binary = .{ .op = if (set == .intersect) .gt else .eq, .l = counted[1], .r = zero } });
    const pred = try mk(arena, .{ .binary = .{ .op = .@"and", .l = left_has, .r = right_has } });

    const at: ast.Pos = .{ .line = 0, .col = 0 };
    var r = try buildStage(env, .{ .node = .{ .aggregate = .{ .aggs = aggs, .by = by } }, .hints = &.{}, .pos = at }, child, schema);
    r = try buildStage(env, .{ .node = .{ .filter = pred }, .hints = &.{}, .pos = at }, r.op, r.schema);
    return buildStage(env, .{ .node = .{ .select = keep }, .hints = &.{}, .pos = at }, r.op, r.schema);
}

test "windowTopK: a whole-item ROW_NUMBER, filtered by its alias, gets top_k; one inside an expression does not" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../../lang/sql_parser.zig");
    const Case = struct { src: []const u8, want: ?u64 };
    const cases = [_]Case{
        .{ .src = "WITH r AS (SELECT k, v, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v DESC) AS rn FROM 'x.csv') SELECT * FROM r WHERE rn <= 2;", .want = 2 },
        .{ .src = "WITH r AS (SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) + 0 AS rn FROM 'x.csv') SELECT * FROM r WHERE rn <= 2;", .want = null },
        .{ .src = "WITH r AS (SELECT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY v) AS rn, SUM(v) OVER (ORDER BY v) AS s FROM 'x.csv') SELECT * FROM r WHERE rn <= 2;", .want = null },
    };
    for (cases) |c| {
        var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        const prog = try parser.parseSource(a, c.src, &diag);
        var bstages: []const ast.Stage = &.{};
        var after: []const ast.Stage = &.{};
        for (prog.stmts) |st| switch (st) {
            .binding => |b| bstages = b.pipeline.stages,
            .output => |o| after = o.stages[1..],
            else => {},
        };
        const got = try windowTopK(a, bstages, after);
        if (c.want) |k| {
            var top: ?u64 = null;
            for (got.?) |st| {
                if (st.node == .window) top = st.node.window.top_k;
            }
            try std.testing.expectEqual(k, top.?);
        } else try std.testing.expect(got == null);
    }
}
