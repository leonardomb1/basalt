//! Plan building: turns analyzed stages into the operator tree (projections,
//! filters, joins, aggregates, windows, unions and set operations), the
//! per-stage schema derivations, and row discovery (a pipeline drained for the
//! rows a `for`/`union` expands over).
//!
//! Parallel plans rebuild the map-only `filter`/`select` prefix per worker on a
//! thread arena from the shared AST stages. Resolution is pure and plan-time
//! cheap, so no mutable operator state is shared across threads; the prefix
//! schema is validated once serially first so a worker cannot hit an analyze error.
//!
//! Source-side optimisations are only allowed to lose speed, never rows:
//! `projectedColumns` and `filterBounds` under-report rather than guess (a missed
//! column fails loudly, a missed bound only skips less), and the Parquet footer
//! shortcut for `COUNT(*)`/`MIN`/`MAX` applies only to an unfiltered, ungrouped
//! read of the file itself. A field qualified by a joined right side is that
//! side's column and is never asked of the read.
//!
//! Union branches are reconciled to a canon schema by a synthesized `select` of
//! casts, widened column by column across every branch with SQL's UNION type
//! resolution: taking branch 0 verbatim once truncated a later float branch's 2.7
//! to 2. A `SELECT * EXCEPT` right after a union is taken out of the canon before
//! reconciling, so an incompatible column is never cast. INTERSECT and EXCEPT
//! group the marked union on every column (which, unlike a join key, puts NULLs
//! together) and keep groups seen on both sides or the left only.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const obs = @import("obs.zig");
const Threshold = @import("../exec/value.zig").Threshold;
const Value = @import("../exec/value.zig").Value;

const aborting = @import("env.zig").aborting;
const Env = @import("env.zig").Env;
const MemSource = @import("env.zig").MemSource;
const forHintIdent = @import("env.zig").forHintIdent;
const forHintName = @import("env.zig").forHintName;
const mk = @import("env.zig").mk;
const PipeRes = @import("env.zig").PipeRes;
const planErr = @import("env.zig").planErr;
const planErrT = @import("env.zig").planErrT;
const Row = @import("env.zig").Row;
const schemaPtr = @import("env.zig").schemaPtr;

const ConstSource = @import("connect.zig").ConstSource;
const dupeSchema = @import("connect.zig").dupeSchema;
const OneBatch = @import("connect.zig").OneBatch;
const openSource = @import("connect.zig").openSource;
const openSourceProjected = @import("connect.zig").openSourceProjected;
const projectSqlRead = @import("connect.zig").projectSqlRead;
const factsIfWanted = @import("connect.zig").factsIfWanted;
const sqlConnInfo = @import("connect.zig").sqlConnInfo;
const exceptColumns = @import("connect.zig").exceptColumns;
const sourceLabel = @import("connect.zig").sourceLabel;

/// Rebuild a map-only `filter`/`select` chain against the projected source schema
/// for `parallel.run`, keeping column indices in step; null if anything does not fit.
pub fn rebuildMapStages(env: *Env, middle: []const ast.Stage, proj_schema: *const types.Schema) ?[]const op.Stage {
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    const ob = env.arena.create(OneBatch) catch return null;
    ob.* = .{ .b = null, .sch = proj_schema.* };
    const scan = env.arena.create(op.Scan) catch return null;
    scan.* = .{ .src = ob.source() };
    const chain = buildMapChain(env.arena, env.params_expr, env.errctx, middle, scan, proj_schema) catch return null;
    const lin = (op.linearize(env.arena, chain) catch return null) orelse return null;
    return lin.stages;
}

pub fn buildMapChain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), errctx: ?*op.ErrCtx, prefix: []const ast.Stage, scan: *op.Scan, csv_schema: *const types.Schema) !op.Op {
    return buildChainFrom(ta, params, errctx, prefix, .{ .scan = scan }, csv_schema.*);
}

/// `buildMapChain` rooted at any operator, e.g. the stages after a join. `errctx`
/// says where a row failed; without it a lane's error was a bare `cast failed`.
pub fn buildChainFrom(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), errctx: ?*op.ErrCtx, stages: []const ast.Stage, start: op.Op, in_schema: types.Schema) !op.Op {
    var cur: op.Op = start;
    var sch = in_schema;
    for (stages) |st| switch (st.node) {
        .filter => |pred0| {
            var ad = analyze.Diag{};
            const pred = try analyze.checkFilter(ta, sch, pred0, params, &ad);
            const f = try ta.create(op.Filter);
            f.* = .{ .child = cur, .pred = pred, .err = errctx, .back = ta };
            cur = .{ .filter = f };
        },
        .select => |items| {
            var ad = analyze.Diag{};
            const rcols = try analyze.selectCols(ta, sch, items, params, &ad);
            const cols = try ta.alloc(op.Project.Col, rcols.len);
            for (rcols, cols) |rc, *c| c.* = .{ .source = switch (rc.source) {
                .passthrough => |x| .{ .passthrough = x },
                .expr => |e| .{ .expr = e },
            }, .ty = rc.ty };
            const out = try ta.create(types.Schema);
            out.* = try analyze.schemaOfCols(ta, rcols);
            const p = try ta.create(op.Project);
            p.* = .{ .child = cur, .cols = cols, .out_schema = out, .err = errctx };
            cur = .{ .project = p };
            sch = out.*;
        },
        else => unreachable,
    };
    return cur;
}

pub fn mapChainSchema(env: *Env, prefix: []const ast.Stage, csv_schema: types.Schema) !types.Schema {
    var sch = csv_schema;
    for (prefix) |st| {
        errdefer env.diag.stamp(st.pos);
        switch (st.node) {
            .filter => |pred0| {
                var ad = analyze.Diag{};
                _ = analyze.checkFilter(env.arena, sch, pred0, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
            },
            .select => |items| {
                var ad = analyze.Diag{};
                const rcols = analyze.selectCols(env.arena, sch, items, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
                sch = analyze.schemaOfCols(env.arena, rcols) catch |e| return aErr(env, &ad, e);
            },
            else => unreachable,
        }
    }
    return sch;
}

/// The sink's schema after a parallel breaker's tail. A tail may hold a projection,
/// aggregate or window (HAVING, `COUNT(*) FROM (SELECT DISTINCT …)`), which change it.
pub fn tailSchema(env: *Env, tail: []const ast.Stage, in: types.Schema) !types.Schema {
    var sch = in;
    for (tail) |st| switch (st.node) {
        .select => {
            const one = [_]ast.Stage{st};
            sch = try mapChainSchema(env, &one, sch);
        },
        .aggregate => |ag| {
            var ad = analyze.Diag{};
            sch = (analyze.aggregatePlan(env.arena, sch, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e)).schema;
        },
        .window => |wd| {
            var ad = analyze.Diag{};
            sch = analyze.windowSchema(env.arena, sch, wd, &ad) catch |e| return aErr(env, &ad, e);
        },
        else => {},
    };
    return sch;
}

/// Source columns the stages after a read need, or null when unprovable. A join
/// adds its keys and both sides' names; abandoning it once decoded 17 columns, not 4.
pub fn projectedColumns(env: *Env, stages: []const ast.Stage) !?[][]const u8 {
    var set = std.StringHashMap(void).init(env.arena);
    var right = std.array_list.Managed([]const u8).init(env.arena);
    var defines_output = false;
    for (stages) |st| {
        switch (st.node) {
            .filter => |e| try exprFields(env, e, &set, right.items),
            .sort => |so| for (so.keys) |k| try putField(&set, k.field, right.items),
            .distinct => |d| {
                const on = d.on orelse return null;
                for (on) |q| try putField(&set, q, right.items);
            },
            .aggregate => |ag| {
                for (ag.by) |q| try putField(&set, q, right.items);
                for (ag.aggs) |a| if (a.arg) |e| try exprFields(env, e, &set, right.items);
                defines_output = true;
                break;
            },
            .select => |items| {
                for (items) |it| switch (it) {
                    .star, .star_except, .star_rename => return null,
                    .field => |q| try putField(&set, q, right.items),
                    .computed => |c| try exprFields(env, c.expr, &set, right.items),
                };
                defines_output = true;
                break;
            },
            .limit => {},
            .window => |w| {
                for (w.partition_by) |q| try putField(&set, q, right.items);
                for (w.order_by) |k| try putField(&set, k.field, right.items);
                for (w.funcs) |f| if (f.arg) |q| try putField(&set, q, right.items);
            },
            .join => |j| {
                for (j.left_keys) |q| try set.put(q.parts[q.parts.len - 1], {});
                try right.append(if (j.alias.len > 0) j.alias else j.binding);
            },
            else => return null,
        }
    }
    if (!defines_output) return null;
    var out = std.array_list.Managed([]const u8).init(env.arena);
    var it = set.keyIterator();
    while (it.next()) |k| try out.append(k.*);
    return try out.toOwnedSlice();
}

fn putField(set: *std.StringHashMap(void), q: ast.QualName, right: []const []const u8) !void {
    if (q.parts.len > 1) for (right) |r| {
        if (std.ascii.eqlIgnoreCase(r, q.parts[0])) return;
    };
    try set.put(q.parts[0], {});
}

fn exprFields(env: *Env, e: *const ast.Expr, set: *std.StringHashMap(void), right: []const []const u8) !void {
    if (right.len == 0) return pushdown.collectFields(e, set);
    var quals = std.array_list.Managed(ast.QualName).init(env.arena);
    try pushdown.collectQuals(env.arena, e, &quals);
    for (quals.items) |q| try putField(set, q, right);
}

/// `column <op> literal` conjuncts of a filter, AND-joined only, usable to skip
/// row groups from their statistics.
pub fn filterBounds(env: *Env, stages: []const ast.Stage) ![]pqdecode.Bound {
    var out = std.array_list.Managed(pqdecode.Bound).init(env.arena);
    for (stages) |st| {
        switch (st.node) {
            .filter => |e| try collectBounds(e, &out),
            .select, .aggregate, .join, .explode, .union_ => break,
            else => {},
        }
    }
    return out.toOwnedSlice();
}

fn collectBounds(e: *const ast.Expr, out: *std.array_list.Managed(pqdecode.Bound)) !void {
    const b = switch (e.*) {
        .binary => |x| x,
        else => return,
    };
    if (b.op == .@"and") {
        try collectBounds(b.l, out);
        try collectBounds(b.r, out);
        return;
    }
    const name = switch (b.l.*) {
        .field => |q| if (q.parts.len == 1) q.parts[0] else return,
        else => return,
    };
    const v: Value = switch (b.r.*) {
        .int_lit => |x| .{ .int = x },
        .float_lit => |x| .{ .float = x },
        .str_lit => |x| .{ .string = x },
        else => return,
    };
    const bop: pqdecode.Bound.Op = switch (b.op) {
        .lt => .lt,
        .le => .le,
        .gt => .gt,
        .ge => .ge,
        .eq => .eq,
        else => return,
    };
    try out.append(.{ .column = name, .op = bop, .value = v });
}

fn metaShortcut(env: *Env, stages: []const ast.Stage) anyerror!?PipeRes {
    if (stages.len != 2) return null;
    if (stages[0].node != .read or stages[1].node != .aggregate) return null;
    const rd = stages[0].node.read;
    if (!std.mem.eql(u8, rd.connector, "csv")) return null;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (!pqdecode.Reader.isPath(path) or csv.CsvReader.isUrl(path)) return null;
    const ag = stages[1].node.aggregate;
    if (ag.by.len != 0 or ag.aggs.len == 0) return null;

    const rdr = pqdecode.Reader.open(env.arena, path) catch return null;
    var ad = analyze.Diag{};
    const ap = analyze.aggregatePlan(env.arena, rdr.schema, ag, env.params_expr, &ad) catch return null;

    const vals = try env.arena.alloc(Value, ap.aggs.len);
    for (ap.aggs, ag.aggs, vals) |ra, item, *out| {
        if (item.distinct) return null;
        switch (ra.func) {
            .count => {
                if (ra.arg != null) return null;
                out.* = .{ .int = rdr.md.num_rows };
            },
            .min, .max => {
                const arg = ra.arg orelse return null;
                if (arg.* != .field) return null;
                const mm = pqdecode.fileMinMax(rdr, arg.field.last()) orelse return null;
                out.* = try op.dupeValue(env.arena, if (ra.func == .min) mm.min else mm.max);
            },
            else => return null,
        }
    }

    const out = try schemaPtr(env.arena, ap.schema);
    const cols = try env.arena.alloc(column.Column, ap.aggs.len);
    for (ap.aggs, vals, cols) |ra, v, *c| {
        var bd = column.Builder.init(env.arena, ra.ty);
        try bd.append(v);
        c.* = try bd.finish();
    }

    const cs = try env.arena.create(ConstSource);
    cs.* = .{ .batch = .{ .schema = out, .columns = cols, .len = 1 }, .out = out };
    const scan = try env.arena.create(op.Scan);
    scan.* = .{ .src = .{ .ptr = cs, .vtable = &ConstSource.vtable } };
    if (env.src_name.len == 0) env.src_name = "parquet";
    return .{ .op = .{ .scan = scan }, .schema = out.* };
}

pub fn buildPipeline(env: *Env, stages_in: []const ast.Stage) anyerror!PipeRes {
    const stages = try projectSqlRead(env, stages_in);
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    if (try metaShortcut(env, stages)) |r| return r;

    var current: op.Op = undefined;
    var schema: types.Schema = undefined;

    switch (stages[0].node) {
        .read => |rd| {
            const raw = try openSourceProjected(env, rd, stages[0].hints, try projectedColumns(env, stages[1..]), try filterBounds(env, stages[1..]));
            const cs = try env.arena.create(obs.CountingSource);
            cs.* = .{ .inner = raw, .count = env.rows_read };
            const src = cs.source();
            try env.sources.append(src);
            if (env.src_name.len == 0) env.src_name = sourceLabel(env, rd, stages[0].hints);
            const scan = try env.arena.create(op.Scan);
            scan.* = .{ .src = src };
            current = .{ .scan = scan };
            schema = src.schema();
        },
        .ref => |name| {
            if (env.materialized.get(name)) |m| {
                const ms = try env.arena.create(MemSource);
                ms.* = .{ .m = m };
                const scan = try env.arena.create(op.Scan);
                scan.* = .{ .src = ms.source() };
                current = .{ .scan = scan };
                schema = m.schema;
            } else {
                const b = env.bindings.get(name) orelse
                    return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown binding `{s}`", .{name}));
                const r = if (b.stages.len == 1 and b.stages[0].node == .union_)
                    try buildUnion(env, b.stages[0].node.union_, b.stages[0].hints, unionExceptNames(stages[1..]))
                else
                    try buildPipeline(env, (try windowTopK(env.arena, b.stages, stages[1..])) orelse b.stages);
                current = r.op;
                schema = r.schema;
            }
        },
        .union_ => |u| {
            const r = try buildUnion(env, u, stages[0].hints, unionExceptNames(stages[1..]));
            current = r.op;
            schema = r.schema;
        },
        else => return planErr(env.diag, "a pipeline must start with `read`, `union`, or a binding reference"),
    }

    var si: usize = 1;
    while (si < stages.len) : (si += 1) {
        const stage = stages[si];
        if (stage.node == .sort and si + 1 < stages.len and stages[si + 1].node == .limit and op.TopN.fits(stages[si + 1].node.limit)) {
            const r = try buildTopN(env, stage.node.sort, stages[si + 1].node.limit, current, schema);
            current = r.op;
            schema = r.schema;
            si += 1;
            continue;
        }
        const r = try buildStage(env, stage, current, schema);
        current = r.op;
        schema = r.schema;
    }
    return .{ .op = current, .schema = schema };
}

/// Pushes the running K-th-best bound into a single parquet source with one sort
/// key, so it can skip row groups its statistics rule out.
pub fn buildTopN(env: *Env, s: ast.Sort, lim: ast.Limit, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    const qs = try arena.alloc(ast.QualName, s.keys.len);
    for (s.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, schema, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, s.keys.len);
    for (s.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };
    const o = try arena.create(op.TopN);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks, .count = lim.count, .offset = lim.offset, .state = arena, .gpa = env.gpa };

    if (env.pq_readers == 1 and s.keys.len == 1 and s.keys[0].field.parts.len == 1) {
        if (env.pq_reader != null or env.pq_folder != null) {
            const t = try arena.create(Threshold);
            t.* = .{ .column = s.keys[0].field.last(), .desc = s.keys[0].desc };
            if (env.pq_reader) |pr| pr.threshold = t;
            if (env.pq_folder) |f| f.threshold = t;
            o.threshold = t;
        }
    }
    return .{ .op = .{ .top_n = o }, .schema = schema };
}

fn readName(rd: ast.Read) []const u8 {
    return switch (rd.form) {
        .table => |q| q.last(),
        else => "",
    };
}

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
fn windowTopK(arena: std.mem.Allocator, bstages: []const ast.Stage, after: []const ast.Stage) !?[]const ast.Stage {
    if (after.len == 0 or after[0].node != .filter) return null;
    var wi = bstages.len;
    while (wi > 0) {
        wi -= 1;
        if (bstages[wi].node == .window) break;
    } else return null;
    if (bstages[wi].node != .window) return null;
    const wd = bstages[wi].node.window;
    if (wd.funcs.len != 1 or wd.funcs[0].kind != .row_number) return null;
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
fn buildUnion(env: *Env, u: ast.Union, hints: []const ast.Hint, except_names: []const []const u8) anyerror!PipeRes {
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

pub fn aErr(env: *Env, ad: *analyze.Diag, e: analyze.Error) anyerror {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.AnalyzeFailed => blk: {
            const err = planErr(env.diag, ad.msg);
            env.diag.pos = ad.pos;
            env.diag.end = ad.end;
            break :blk err;
        },
    };
}

pub fn buildStage(env: *Env, stage: ast.Stage, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    errdefer env.diag.stamp(stage.pos);
    const arena = env.arena;
    switch (stage.node) {
        .filter => |pred0| {
            var ad = analyze.Diag{};
            const pred = analyze.checkFilter(arena, schema, pred0, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
            const f = try arena.create(op.Filter);
            f.* = .{ .child = child, .pred = pred, .err = env.errctx, .back = arena };
            return .{ .op = .{ .filter = f }, .schema = schema };
        },
        .select => |items| return buildProject(env, items, schema, child),
        .limit => |lim| {
            const l = try arena.create(op.Limit);
            l.* = .{ .child = child, .remaining = lim.count, .to_skip = lim.offset };
            return .{ .op = .{ .limit = l }, .schema = schema };
        },
        .distinct => |d| {
            var keys: ?[]const usize = null;
            if (d.on) |fields| {
                var ad = analyze.Diag{};
                keys = analyze.fieldIndices(arena, schema, fields, &ad) catch |e| return aErr(env, &ad, e);
            }
            const o = try arena.create(op.Distinct);
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = keys, .state = arena, .gpa = env.gpa };
            return .{ .op = .{ .distinct = o }, .schema = schema };
        },
        .sort => |s| {
            const qs = try arena.alloc(ast.QualName, s.keys.len);
            for (s.keys, qs) |sk, *q| q.* = sk.field;
            var ad = analyze.Diag{};
            const idxs = analyze.fieldIndices(arena, schema, qs, &ad) catch |e| return aErr(env, &ad, e);
            const ks = try arena.alloc(op.Sort.Key, s.keys.len);
            for (s.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };
            const o = try arena.create(op.Sort);
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks, .threads = env.sort_threads };
            return .{ .op = .{ .sort = o }, .schema = schema };
        },
        .window => |wd| {
            var ad = analyze.Diag{};
            const pidx = analyze.fieldIndices(arena, schema, wd.partition_by, &ad) catch |e| return aErr(env, &ad, e);
            const oqs = try arena.alloc(ast.QualName, wd.order_by.len);
            for (wd.order_by, oqs) |sk, *q| q.* = sk.field;
            const oidx = analyze.fieldIndices(arena, schema, oqs, &ad) catch |e| return aErr(env, &ad, e);

            const pk = try arena.alloc(op.Sort.Key, pidx.len);
            for (pidx, pk) |idx, *k| k.* = .{ .idx = idx, .desc = false };
            const ok = try arena.alloc(op.Sort.Key, oidx.len);
            for (wd.order_by, oidx, ok) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };

            const kinds = try arena.alloc(op.Window.Func, wd.funcs.len);
            const fields = try arena.alloc(types.Schema.Field, schema.fields.len + wd.funcs.len);
            @memcpy(fields[0..schema.fields.len], schema.fields);
            for (wd.funcs, kinds, 0..) |f, *out, i| {
                var ty = types.Type.init(.int);
                var arg: ?usize = null;
                switch (f.kind) {
                    .row_number, .rank, .dense_rank => {},
                    .count => {
                        if (f.arg) |q| {
                            const ai = analyze.fieldIndices(arena, schema, &[_]ast.QualName{q}, &ad) catch |e| return aErr(env, &ad, e);
                            arg = ai[0];
                        }
                    },
                    .sum, .min, .max, .avg => {
                        const q = f.arg orelse return planErr(env.diag, "this window function needs a column argument");
                        const ai = analyze.fieldIndices(arena, schema, &[_]ast.QualName{q}, &ad) catch |e| return aErr(env, &ad, e);
                        arg = ai[0];
                        ty = analyze.windowFuncType(f.kind, schema.fields[ai[0]].ty);
                    },
                    .lag, .lead => {
                        const q = f.arg orelse return planErr(env.diag, "LAG/LEAD needs a column argument");
                        const ai = analyze.fieldIndices(arena, schema, &[_]ast.QualName{q}, &ad) catch |e| return aErr(env, &ad, e);
                        arg = ai[0];
                        ty = schema.fields[ai[0]].ty.asNullable();
                    },
                }
                out.* = .{
                    .kind = switch (f.kind) {
                        .row_number => .row_number,
                        .rank => .rank,
                        .dense_rank => .dense_rank,
                        .lag => .lag,
                        .lead => .lead,
                        .sum => .sum,
                        .count => .count,
                        .min => .min,
                        .max => .max,
                        .avg => .avg,
                    },
                    .arg = arg,
                    .offset = f.offset,
                    .frame = .{ .rows = f.frame.rows, .unbounded = f.frame.unbounded, .preceding = f.frame.preceding },
                };
                fields[schema.fields.len + i] = .{ .name = f.out, .ty = ty };
            }
            const out: types.Schema = .{ .fields = fields };
            const o = try arena.create(op.Window);
            o.* = .{
                .child = child,
                .in_schema = try schemaPtr(arena, schema),
                .out_schema = try schemaPtr(arena, out),
                .part = pk,
                .ord = ok,
                .funcs = kinds,
                .err = env.errctx,
                .top_k = wd.top_k,
                .gpa = env.gpa,
                .threads = env.sort_threads,
            };
            return .{ .op = .{ .window = o }, .schema = out };
        },
        .aggregate => |ag| return buildAggregate(env, ag, schema, child),
        .join => |j| return buildJoin(env, j, stage.hints, schema, child),
        .explode => |ex| {
            var ad = analyze.Diag{};
            const ep = analyze.explodePlan(arena, schema, ex, &ad) catch |e| return aErr(env, &ad, e);
            const out = try schemaPtr(arena, ep.schema);
            const o = try arena.create(op.Explode);
            o.* = .{ .child = child, .field_idx = ep.idx, .delim = ex.delim orelse ",", .json = ex.json, .out_schema = out };
            return .{ .op = .{ .explode = o }, .schema = out.* };
        },
        .read, .ref, .write, .union_ => return planErr(env.diag, "unexpected operator in the middle of a pipeline"),
    }
}

fn buildProject(env: *Env, items: []const ast.SelectItem, in_schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    var ad = analyze.Diag{};
    const rcols = analyze.selectCols(arena, in_schema, items, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);

    const cols = try arena.alloc(op.Project.Col, rcols.len);
    for (rcols, cols) |rc, *c| c.* = .{
        .source = switch (rc.source) {
            .passthrough => |idx| .{ .passthrough = idx },
            .expr => |e| .{ .expr = e },
        },
        .ty = rc.ty,
    };
    const out = try arena.create(types.Schema);
    out.* = try analyze.schemaOfCols(arena, rcols);
    const p = try arena.create(op.Project);
    p.* = .{ .child = child, .cols = cols, .out_schema = out, .err = env.errctx };
    return .{ .op = .{ .project = p }, .schema = out.* };
}

fn buildAggregate(env: *Env, ag: ast.Aggregate, schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    var ad = analyze.Diag{};
    const ap = analyze.aggregatePlan(arena, schema, ag, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const aggs = try arena.alloc(op.Aggregate.Agg, ap.aggs.len);
    for (ap.aggs, aggs) |ra, *a| a.* = .{ .func = ra.func, .arg = ra.arg, .ty = ra.ty, .distinct = ra.distinct };
    const out = try schemaPtr(arena, ap.schema);
    const o = try arena.create(op.Aggregate);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .by = ap.by, .aggs = aggs, .out_schema = out, .err = env.errctx, .state = arena, .gpa = env.gpa };
    return .{ .op = .{ .aggregate = o }, .schema = out.* };
}

fn parseByteSizeText(txt: []const u8) ?usize {
    const t = std.mem.trim(u8, txt, " \t");
    var n: usize = 0;
    while (n < t.len and std.ascii.isDigit(t[n])) n += 1;
    if (n == 0) return null;
    const v = std.fmt.parseInt(usize, t[0..n], 10) catch return null;
    const unit = std.mem.trim(u8, t[n..], " \t");
    if (unit.len == 0 or std.ascii.eqlIgnoreCase(unit, "b")) return v;
    if (std.ascii.eqlIgnoreCase(unit, "kb")) return v << 10;
    if (std.ascii.eqlIgnoreCase(unit, "mb")) return v << 20;
    if (std.ascii.eqlIgnoreCase(unit, "gb")) return v << 30;
    return null;
}

pub fn joinBuildCap(env: *Env, hints: []const ast.Hint) !usize {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "max_build")) continue;
        const txt: []const u8 = switch (h.value) {
            .str => |v| v,
            .ident => |v| v,
            .int => |v| return if (v > 0) @intCast(v) else planErr(env.diag, "max_build must be positive"),
            .flag => "",
        };
        return parseByteSizeText(txt) orelse
            planErr(env.diag, try std.fmt.allocPrint(env.arena, "bad max_build `{s}` — use e.g. '512MB' or '8GB'", .{txt}));
    }
    return op.join_build_byte_cap;
}

/// Replace the binding chain at the head of `stages` with the stages it stands for,
/// except a materialized binding or one holding a window (`windowTopK` needs it apart).
pub fn inlineHeadBindings(env: *Env, stages_in: []const ast.Stage) ![]const ast.Stage {
    var stages = stages_in;
    var n: usize = 0;
    while (stages[0].node == .ref and n < 16) : (n += 1) {
        const name = stages[0].node.ref;
        if (env.materialized.contains(name)) break;
        const b = env.bindings.get(name) orelse break;
        if (b.stages.len == 0 or b.stages[0].node != .read) break;
        for (b.stages) |st| {
            if (st.node == .window) return stages;
        }
        const joined = try env.arena.alloc(ast.Stage, b.stages.len + stages.len - 1);
        @memcpy(joined[0..b.stages.len], b.stages);
        @memcpy(joined[b.stages.len..], stages[1..]);
        stages = joined;
    }
    return stages;
}

/// The implicit pushdown: the contiguous WHERE right after a SQL read, translated
/// into that read's own query.
pub fn descendLeadingWhere(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    if (stages[0].node != .read) return stages;
    const arena = env.arena;
    const rd = stages[0].node.read;
    if (rd.form != .table and rd.form != .query) return stages;
    const conn = env.connections.get(rd.connector) orelse return stages;
    const d = (sqlConnInfo(conn) orelse return stages).dialect;
    const facts = try factsIfWanted(env, rd, d, stages[1..], .{ .fields = &.{} }, false, .superset);
    const extra = (try pushdown.serialWhereWith(arena, d, stages, facts, null)) orelse return stages;
    const out = try arena.dupe(ast.Stage, stages);
    var nrd = rd;
    nrd.where = if (rd.where.len > 0)
        try std.fmt.allocPrint(arena, "({s}) AND ({s})", .{ rd.where, extra })
    else
        extra;
    out[0].node = .{ .read = nrd };
    return out;
}

/// A join's right side readied like a query's head pipeline, with filters moved
/// and the SQL read narrowed, so a joined CTE no longer reads its whole table.
pub fn prepareJoinSide(env: *Env, stages_in: []const ast.Stage) ![]const ast.Stage {
    if (stages_in.len == 0) return stages_in;
    var stages = try inlineHeadBindings(env, stages_in);
    stages = try analyze.substFilterParams(env.arena, stages, env.params_expr);
    if (try pushdown.hoistFilters(env.arena, env.gpa, stages, env.bindings)) |h| stages = h;
    stages = try projectSqlRead(env, stages);
    return descendLeadingWhere(env, stages);
}

fn buildJoin(env: *Env, j: ast.Join, hints: []const ast.Hint, left_schema: types.Schema, probe: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    if (env.bindings.get(j.binding) == null)
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const build = try buildPipeline(env, try prepareJoinSide(env, try j.rightStages(env.arena, env.bindings.get(j.binding).?.stages)));

    var ad = analyze.Diag{};
    const jp = analyze.joinPlan(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    const o = try arena.create(op.Join);
    o.* = .{
        .probe = probe,
        .build = build.op,
        .index = null,
        .left_keys = jp.lks,
        .right_keys = jp.rks,
        .left_schema = try schemaPtr(arena, left_schema),
        .right_schema = try schemaPtr(arena, build.schema),
        .out_schema = out,
        .kind = j.kind,
        .null_aware = j.null_aware,
        .state = arena,
        .err = env.errctx,
        .build_cap = try joinBuildCap(env, hints),
    };
    return .{ .op = .{ .join = o }, .schema = out.* };
}

/// Append a discovery batch's first `ncols` columns as text (null → ""), shared by
/// every discovery form so all agree on coercion and the column-count error.
fn appendDiscoveryRows(env: *Env, rows: *std.array_list.Managed(Row), b: Batch, ncols: usize) !void {
    if (b.columns.len == 0) return;
    if (b.columns.len < ncols)
        return planErr(env.diag, "for-each: the discovery query returns fewer columns than loop variables");
    for (0..b.len) |r| {
        const row = try env.arena.alloc([]const u8, ncols);
        for (0..ncols) |j| {
            row[j] = switch (b.columns[j].getValue(r)) {
                .null => "",
                .string, .bytes => |s| try env.arena.dupe(u8, s),
                .int => |x| try std.fmt.allocPrint(env.arena, "{d}", .{x}),
                else => return planErr(env.diag, "for-each values must be string or int"),
            };
        }
        try rows.append(row);
    }
}

/// The discovery source's first `ncols` columns as text rows, fully materialized
/// (a table catalog is small). Keeps `openSource`'s own error message.
pub fn discoverRows(env: *Env, src_read: ast.Read, ncols: usize) ![]const Row {
    const src = openSource(env, src_read, &.{}) catch |e| {
        const why = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{why}));
    };
    defer src.close();
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        _ = da.reset(.retain_capacity);
        const b = (try src.next(da.allocator())) orelse break;
        try appendDiscoveryRows(env, &rows, b, ncols);
    }
    return rows.toOwnedSlice();
}

/// Discovery from a full query, planned through `buildPipeline` (pushdown free).
/// Its sources are closed here and per-pipeline scratch fields restored after.
pub fn discoverRowsPipeline(env: *Env, pipe: ast.Pipeline, ncols: usize) anyerror![]const Row {
    if (pipe.stages.len == 0) return planErr(env.diag, "for-each: empty discovery query");
    const src_base = env.sources.items.len;
    const saved_src_name = env.src_name;
    const saved_sql_desc = env.sql_desc;
    const saved_pq_readers = env.pq_readers;
    const saved_pq_reader = env.pq_reader;
    const saved_pq_folder = env.pq_folder;
    defer {
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.src_name = saved_src_name;
        env.sql_desc = saved_sql_desc;
        env.pq_readers = saved_pq_readers;
        env.pq_reader = saved_pq_reader;
        env.pq_folder = saved_pq_folder;
    }

    const res = buildPipeline(env, pipe.stages) catch |e| {
        const why = if (env.diag.msg.len > 0) env.diag.msg else @errorName(e);
        return planErrT(env.diag, e, try std.fmt.allocPrint(env.arena, "for-each discovery failed: {s}", .{why}));
    };
    var rows = std.array_list.Managed(Row).init(env.arena);
    var da = std.heap.ArenaAllocator.init(env.gpa);
    defer da.deinit();
    while (true) {
        if (aborting()) return error.Aborted;
        _ = da.reset(.retain_capacity);
        const b = (try res.op.next(da.allocator())) orelse break;
        try appendDiscoveryRows(env, &rows, b, ncols);
    }
    return rows.toOwnedSlice();
}

/// For-each rows from a JSON array param, each loop variable bound to the
/// like-named field of each element as text.
pub fn discoverRowsJson(env: *Env, path: ast.QualName, var_names: []const []const u8) ![]const Row {
    const head = path.parts[0];
    var cur = env.json_params.get(head) orelse
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: `{s}` is not a JSON param", .{head}));
    for (path.parts[1..], 0..) |key, j| {
        const seg_safe = j < path.safe.len and path.safe[j];
        cur = switch (cur) {
            .object => |o| o.get(key) orelse {
                if (seg_safe) return &.{};
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "for-each: json key `{s}` not found", .{key}));
            },
            else => {
                if (seg_safe) return &.{};
                return planErr(env.diag, "for-each: json path is not an object");
            },
        };
    }
    const arr = switch (cur) {
        .array => |a| a,
        else => return planErr(env.diag, "for-each: json source is not an array"),
    };
    var rows = std.array_list.Managed(Row).init(env.arena);
    for (arr.items) |elem| {
        const row = try env.arena.alloc([]const u8, var_names.len);
        for (var_names, 0..) |vn, i| {
            row[i] = switch (elem) {
                .object => |o| if (o.get(vn)) |fv| try jsonToStr(env.arena, fv) else "",
                else => "",
            };
        }
        try rows.append(row);
    }
    return rows.toOwnedSlice();
}

fn jsonToStr(arena: std.mem.Allocator, v: std.json.Value) ![]const u8 {
    return switch (v) {
        .null => "",
        .bool => |b| if (b) "true" else "false",
        .integer => |i| try std.fmt.allocPrint(arena, "{d}", .{i}),
        .float => |f| try std.fmt.allocPrint(arena, "{d}", .{f}),
        .number_string, .string => |s| s,
        .array, .object => try std.json.Stringify.valueAlloc(arena, v, .{}),
    };
}

test "prepareJoinSide: a CTE joined in reads its table with its own WHERE, its columns narrowed" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../lang/sql_parser.zig");
    const env_mod = @import("env.zig");

    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\WITH c AS (SELECT cid, name AS nm FROM pg.public.customers WHERE active = 1)
        \\SELECT o.id, c.nm FROM 'o.csv' o JOIN c ON o.id = c.cid;
    , &pd);
    var params = std.StringHashMap(Value).init(a);
    var bindings = std.StringHashMap(ast.Pipeline).init(a);
    var connections = std.StringHashMap(ast.Connection).init(a);
    for (prog.stmts) |st| switch (st) {
        .connection => |c| try connections.put(c.name, c),
        .binding => |b| try bindings.put(b.name, b.pipeline),
        else => {},
    };
    var sources = std.array_list.Managed(@import("../connect/driver.zig").Source).init(a);
    var diag = env_mod.Diag{};
    var log = obs.Logger.init(0, .text, .err);
    var params_expr = std.StringHashMap(*const ast.Expr).init(a);
    var errctx = op.ErrCtx{};
    var rows = obs.RowCounter.init(0);
    var json = std.StringHashMap(std.json.Value).init(a);
    const fns = std.StringHashMap(ast.FnDecl).init(a);
    var env = Env{ .arena = a, .gpa = a, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = null, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns };

    const side = [_]ast.Stage{.{ .node = .{ .ref = "c" }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } }};
    const out = try prepareJoinSide(&env, &side);
    const rd = out[0].node.read;
    try std.testing.expectEqualStrings("(\"active\" = 1)", rd.where);
    try std.testing.expectEqual(@as(usize, 3), rd.cols.len);
    try std.testing.expectEqualStrings("active", rd.cols[0]);
    try std.testing.expectEqualStrings("name", rd.cols[1]);
    try std.testing.expectEqualStrings("cid", rd.cols[2]);
}
