//! Plan building: turns analyzed stages into the operator tree — projections,
//! filters, joins, aggregates, unions — the per-stage schema derivations, and
//! row discovery (a pipeline drained for the rows a `for`/`union` expands over).

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const Batch = @import("../exec/batch.zig").Batch;
const column = @import("../exec/column.zig");
const eval = @import("../exec/eval.zig");
const csv = @import("../connect/csv.zig");
const pqdecode = @import("../connect/pqdecode.zig");
const parallel = @import("parallel.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const obs = @import("obs.zig");
const Threshold = @import("../exec/value.zig").Threshold;
const Value = @import("../exec/value.zig").Value;

const aborting = @import("env.zig").aborting;
const Env = @import("env.zig").Env;
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
const sourceLabel = @import("connect.zig").sourceLabel;

/// Rebuild a map-only `filter`/`select` chain against the projected source schema and
/// return its linearized stages for `parallel.run` — so projecting fewer columns at the
/// source keeps each `project` op's column indices in step. Returns null (caller keeps
/// the full chain) if anything doesn't fit: a non-filter/select stage, or an analyze
/// hitch. The throwaway scan is only a chain anchor; `linearize` drops it and the stages
/// apply statelessly, so it's never read.
pub fn rebuildMapStages(env: *Env, middle: []const ast.Stage, proj_schema: *const types.Schema) ?[]const op.Stage {
    for (middle) |st| switch (st.node) {
        .filter, .select => {},
        else => return null,
    };
    const ob = env.arena.create(OneBatch) catch return null;
    ob.* = .{ .b = null, .sch = proj_schema.* };
    const scan = env.arena.create(op.Scan) catch return null;
    scan.* = .{ .src = ob.source() };
    const chain = buildMapChain(env.arena, env.params_expr, middle, scan, proj_schema) catch return null;
    const lin = (op.linearize(env.arena, chain) catch return null) orelse return null;
    return lin.stages;
}

/// Build the map-only prefix (`filter`/`select`) onto `scan` using `ta` (a thread
/// arena). `checkFilter`/`selectCols` are pure (arena + read-only schema/params), so
/// each worker rebuilds its own prefix chain safely from the shared AST stages — the
/// resolution is plan-time-cheap and avoids sharing mutable op state across threads.
pub fn buildMapChain(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), prefix: []const ast.Stage, scan: *op.Scan, csv_schema: *const types.Schema) !op.Op {
    return buildChainFrom(ta, params, prefix, .{ .scan = scan }, csv_schema.*);
}

/// The body of `buildMapChain`, rooted at an arbitrary operator instead of a scan —
/// the join path reuses it for the stages that sit *after* the join, where the input
/// is the join's output schema rather than the source's.
pub fn buildChainFrom(ta: std.mem.Allocator, params: *std.StringHashMap(*const ast.Expr), stages: []const ast.Stage, start: op.Op, in_schema: types.Schema) !op.Op {
    var cur: op.Op = start;
    var sch = in_schema;
    for (stages) |st| switch (st.node) {
        .filter => |pred0| {
            var ad = analyze.Diag{};
            const pred = try analyze.checkFilter(ta, sch, pred0, params, &ad);
            const f = try ta.create(op.Filter);
            f.* = .{ .child = cur, .pred = pred, .err = null, .back = ta };
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
            p.* = .{ .child = cur, .cols = cols, .out_schema = out, .err = null };
            cur = .{ .project = p };
            sch = out.*;
        },
        else => unreachable,
    };
    return cur;
}

/// The schema after the map-only prefix — validated once serially (so worker rebuilds
/// can't hit an analyze error) and used as the aggregate's input schema.
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

/// Write a parallel breaker's merged batch to the sink, first running the small
/// post-breaker `sort`/`limit` tail (which preserves `schema`) serially over it.
/// The schema the sink is opened with: the aggregate's output as the tail leaves it.
/// Sort and limit do not change it; a projection does, and one lands in the tail
/// whenever the SELECT list interleaves grouping keys and aggregates.
pub fn tailSchema(env: *Env, tail: []const ast.Stage, in: types.Schema) !types.Schema {
    var sch = in;
    for (tail) |st| switch (st.node) {
        .select => {
            const one = [_]ast.Stage{st};
            sch = try mapChainSchema(env, &one, sch);
        },
        // A lane tail may now hold an aggregate or a window (HAVING and
        // `COUNT(*) FROM (SELECT DISTINCT …)` shapes); the sink must be
        // opened with the columns they produce, not the breaker's rows.
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

/// Source columns the stages after a read actually need, or null when that set
/// cannot be proven — in which case every column is read.
///
/// Only used to skip decoding work: if this under-reports, a later stage fails
/// to resolve a field and the query errors loudly. It can never silently return
/// wrong rows, which is what makes the optimisation safe to attempt.
pub fn projectedColumns(env: *Env, stages: []const ast.Stage) !?[][]const u8 {
    var set = std.StringHashMap(void).init(env.arena);
    // Projection is only sound once some stage *defines* the output columns.
    // With no such stage the read's own columns are the result — an empty
    // reference set then means "everything", not "nothing".
    var defines_output = false;
    for (stages) |st| {
        switch (st.node) {
            .filter => |e| try pushdown.collectFields(e, &set),
            .sort => |so| for (so.keys) |k| try set.put(k.field.parts[0], {}),
            .distinct => |d| {
                // `distinct` with no key list looks at every column
                const on = d.on orelse return null;
                for (on) |q| try set.put(q.parts[0], {});
            },
            .aggregate => |ag| {
                for (ag.by) |q| try set.put(q.parts[0], {});
                for (ag.aggs) |a| if (a.arg) |e| try pushdown.collectFields(e, &set);
                defines_output = true;
                break;
            },
            .select => |items| {
                for (items) |it| switch (it) {
                    // a star keeps every column, and after it names are no
                    // longer the source's, so nothing further can be proven
                    .star, .star_except, .star_rename => return null,
                    .field => |q| try set.put(q.parts[0], {}),
                    .computed => |c| try pushdown.collectFields(c.expr, &set),
                };
                // downstream stages refer to this select's outputs, not the
                // source's columns, so the set is complete here
                defines_output = true;
                break;
            },
            .limit => {},
            // A join needs its own probe-side keys, and the stages after it name
            // columns from both sides. Adding all of them is safe: `openProjected`
            // walks the file's own leaves and keeps the ones asked for, so a
            // right-side name simply never matches. Without this case a join fell to
            // the `else` below and the projection was abandoned entirely — a joined
            // query decoded all 17 columns of TPC-H lineitem instead of the 4 it
            // read, measured at 1174ms of scan against 263ms.
            .join => |j| for (j.left_keys) |q| try set.put(q.parts[q.parts.len - 1], {}),
            // anything else may reference columns in ways not modelled here
            else => return null,
        }
    }
    if (!defines_output) return null;
    var out = std.array_list.Managed([]const u8).init(env.arena);
    var it = set.keyIterator();
    while (it.next()) |k| try out.append(k.*);
    return try out.toOwnedSlice();
}

/// Simple `column <op> literal` conjuncts of a filter, usable to skip whole
/// row groups from their statistics.
///
/// Only `AND`-joined comparisons are collected. Anything else contributes no
/// bound, which loses an optimisation but can never exclude a matching row.
pub fn filterBounds(env: *Env, stages: []const ast.Stage) ![]pqdecode.Bound {
    var out = std.array_list.Managed(pqdecode.Bound).init(env.arena);
    for (stages) |st| {
        switch (st.node) {
            .filter => |e| try collectBounds(e, &out),
            // stop at the first stage that redefines the columns
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
    // only `field <op> literal` in that order; the mirrored form is left alone
    // A qualified name is a join's right-side column, never the scanned file's.
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

/// Answer `COUNT(*)`, `MIN(col)` and `MAX(col)` over a whole Parquet file from
/// the footer instead of scanning it. Deliberately narrow: an unfiltered,
/// ungrouped aggregate reading the file directly. Anything else — a filter, a
/// GROUP BY, a URL, a missing statistic — falls through to the real pipeline,
/// so the shortcut can only ever be as correct as the scan it replaces.
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
                if (ra.arg != null) return null; // COUNT(col) needs null counts
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

pub fn buildPipeline(env: *Env, stages: []const ast.Stage) anyerror!PipeRes {
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
            const b = env.bindings.get(name) orelse
                return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown binding `{s}`", .{name}));
            // `WITH u AS (<union>) SELECT * EXCEPT (x) FROM u`: the EXCEPT reaches the
            // union through the binding the same as if it stood right after it.
            const r = if (b.stages.len == 1 and b.stages[0].node == .union_)
                try buildUnion(env, b.stages[0].node.union_, b.stages[0].hints, unionExceptNames(stages[1..]))
            else
                try buildPipeline(env, b.stages);
            current = r.op;
            schema = r.schema;
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
        if (stage.node == .sort and si + 1 < stages.len and stages[si + 1].node == .limit) {
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

fn buildTopN(env: *Env, s: ast.Sort, lim: ast.Limit, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    const arena = env.arena;
    const qs = try arena.alloc(ast.QualName, s.keys.len);
    for (s.keys, qs) |sk, *q| q.* = sk.field;
    var ad = analyze.Diag{};
    const idxs = analyze.fieldIndices(arena, schema, qs, &ad) catch |e| return aErr(env, &ad, e);
    const ks = try arena.alloc(op.Sort.Key, s.keys.len);
    for (s.keys, idxs, ks) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc };
    const o = try arena.create(op.TopN);
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks, .count = lim.count, .offset = lim.offset, .state = arena, .gpa = env.gpa };

    // Push the running K-th-best bound into a single parquet source so it can
    // skip row groups its statistics rule out. Requires exactly one parquet
    // reader and one sort key, so the bound is unambiguous.
    if (env.pq_readers == 1 and s.keys.len == 1 and s.keys[0].field.parts.len == 1) {
        if (env.pq_reader) |pr| {
            const t = try arena.create(Threshold);
            t.* = .{ .column = s.keys[0].field.last(), .desc = s.keys[0].desc };
            pr.threshold = t;
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

/// Synthesize the per-branch "reconcile to canon" projection as a `select`: an
/// optional tag literal, then every canon column cast to its canon type — taking
/// the source field when present, else NULL. (Extra source columns aren't listed,
/// so they're dropped.) Reusing `select` gets us the vectorized cast/eval for free.
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

/// Resolve a union's branch list — explicit branches, or tables discovered via a
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

/// A string-valued field of a JSON object, or null if absent / not a string.
fn jsonStrField(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    return switch (obj.get(key) orelse return null) {
        .string => |s| s,
        else => null,
    };
}

/// Derive a substring of `s` from a `"start,len"` spec (1-based start, matching the
/// `substr` builtin). Returns null on a malformed/out-of-range spec.
fn deriveSubstr(s: []const u8, spec: []const u8) ?[]const u8 {
    const comma = std.mem.indexOfScalar(u8, spec, ',') orelse return null;
    const start = std.fmt.parseInt(usize, std.mem.trim(u8, spec[0..comma], " "), 10) catch return null;
    const len = std.fmt.parseInt(usize, std.mem.trim(u8, spec[comma + 1 ..], " "), 10) catch return null;
    if (start == 0 or start > s.len) return null;
    const a = start - 1;
    return s[a..@min(a + len, s.len)];
}

/// Pick the canon schema among the branch schemas: a named source table, or the
/// first branch.
pub fn unionCanon(env: *Env, specs: []const UnionSpec, schemas: []const types.Schema, canon_opt: ?[]const u8, except: []const []const u8) !types.Schema {
    if (canon_opt) |c| if (!std.mem.eql(u8, c, "first")) {
        for (specs, schemas) |s, sch| if (std.mem.eql(u8, s.name, c)) return dropExcept(env.arena, sch, except);
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "union canon `{s}` is not one of the source tables", .{c}));
    };
    // Widen field-by-field across every branch instead of taking branch 0
    // verbatim: `synthReconcile` CASTs each branch to this schema, so an int
    // first branch silently truncated a later float branch's 2.7 to 2 — and
    // swapping the branches changed the answer. `unify` is SQL's UNION column
    // type resolution; a pair that cannot unify is an error, not a guess.
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

/// `schema` without the columns named in `except` (SQL sources are case-insensitive
/// about names, so the match is too).
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

/// Build the serial union op: open every branch (kept open, drained in order by
/// op.Union), reconcile each to the canon, and concatenate. Used when split isn't
/// applicable (threads=1, a breaker downstream, or non-splittable branches).
/// The columns a `SELECT * EXCEPT (...)` right after a union leaves out. They are
/// taken out of the canon before the branches are reconciled to it, so a column
/// that one branch carries with an incompatible type — the reason to except it —
/// is never cast at all. Applied downstream alone, the EXCEPT would come too late.
pub fn unionExceptNames(after: []const ast.Stage) []const []const u8 {
    if (after.len == 0 or after[0].node != .select) return &.{};
    for (after[0].node.select) |it| {
        if (it == .star_except) return it.star_except;
    }
    return &.{};
}

fn buildUnion(env: *Env, u: ast.Union, hints: []const ast.Hint, except: []const []const u8) anyerror!PipeRes {
    const arena = env.arena;
    const tag_col = forHintIdent(hints, "tag");
    const canon_opt = forHintIdent(hints, "canon");
    const specs = try unionSpecs(env, u, hints);
    if (specs.len == 0) return planErr(env.diag, "union has no source tables");

    const children = try arena.alloc(op.Op, specs.len);
    const schemas = try arena.alloc(types.Schema, specs.len);
    for (specs, 0..) |s, i| {
        // An arm that carries a pipeline is a general query — a file, a projection, an
        // aggregate — rather than the bare table the reconciliation case uses. Build it
        // like any other pipeline; the by-name alignment below works off the arm's
        // schema either way and does not care which it was.
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
    const canon = try dupeSchema(arena, try unionCanon(env, specs, schemas, canon_opt, except));

    var out_schema: types.Schema = undefined;
    for (specs, 0..) |s, i| {
        const items = try synthReconcile(arena, schemas[i], canon, tag_col, s.tag);
        const proj = try buildProject(env, items, schemas[i], children[i]);
        children[i] = proj.op;
        out_schema = proj.schema;
    }
    const un = try arena.create(op.Union);
    un.* = .{ .children = children };
    return .{ .op = .{ .union_ = un }, .schema = out_schema };
}

/// Bridge an analyze-layer error (which writes `ad.msg`) into a plan error.
pub fn aErr(env: *Env, ad: *analyze.Diag, e: analyze.Error) anyerror {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.AnalyzeFailed => blk: {
            const err = planErr(env.diag, ad.msg);
            env.diag.pos = ad.pos;
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
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks };
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

            // The ranking columns are appended, so the input schema is a prefix of the
            // output and every column reference below stays valid.
            const kinds = try arena.alloc(op.Window.Func, wd.funcs.len);
            const fields = try arena.alloc(types.Schema.Field, schema.fields.len + wd.funcs.len);
            @memcpy(fields[0..schema.fields.len], schema.fields);
            for (wd.funcs, kinds, 0..) |f, *out, i| {
                // A ranking function counts rows, so it is a non-null int. `lag`/`lead`
                // carry their source column's type and are always nullable — the first
                // row of a partition has nothing behind it.
                var ty = types.Type.init(.int);
                var arg: ?usize = null;
                switch (f.kind) {
                    .row_number, .rank, .dense_rank => {},
                    // COUNT answers an int; SUM keeps its column's family (int stays
                    // int, anything else widens to float) and is nullable, because a
                    // peer group of nothing but nulls sums to null.
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
                .frame = .{ .rows = wd.frame.rows, .unbounded = wd.frame.unbounded, .preceding = wd.frame.preceding },
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

/// Parse '512MB' / '8GB' / '1024' (bytes) — the value of a join's `max_build` hint.
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

/// The join's build-side byte cap: `WITH (max_build = '8GB')` on the join
/// clause, else the process default.
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

fn buildJoin(env: *Env, j: ast.Join, hints: []const ast.Hint, left_schema: types.Schema, probe: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    if (env.bindings.get(j.binding) == null)
        return planErr(env.diag, try std.fmt.allocPrint(arena, "unknown binding `{s}` in join", .{j.binding}));
    const build = try buildPipeline(env, env.bindings.get(j.binding).?.stages);

    var ad = analyze.Diag{};
    const jp = analyze.joinPlan(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    const o = try arena.create(op.Join);
    o.* = .{
        .probe = probe,
        // Serial plan: the index is built from this pipeline on the first pull.
        .build = build.op,
        .index = null,
        .left_keys = jp.lks,
        .right_keys = jp.rks,
        .left_schema = try schemaPtr(arena, left_schema),
        .right_schema = try schemaPtr(arena, build.schema),
        .out_schema = out,
        .kind = j.kind,
        .state = arena,
        .err = env.errctx,
        .build_cap = try joinBuildCap(env, hints),
    };
    return .{ .op = .{ .join = o }, .schema = out.* };
}

/// Append one discovery batch's first `ncols` columns to `rows` as text
/// (strings/ints; null → ""). Shared by every discovery form so they all agree
/// on the coercion rules and the column-count error.
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

/// Run the discovery source once and collect its first `ncols` columns as rows of
/// text (strings/ints; null → ""). The list is small — a table catalog — so it is
/// fully materialized into the plan arena.
pub fn discoverRows(env: *Env, src_read: ast.Read, ncols: usize) ![]const Row {
    const src = openSource(env, src_read, &.{}) catch |e| {
        // `openSource` already recorded why it failed; reporting only the error
        // name would replace "sqlserver connect failed: …" with "PlanFailed"
        // and throw the cause away.
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

/// Discovery from a full basalt query (`FOR EACH ROW OF (SELECT ...)` /
/// `EACH TABLE OF (SELECT ...)`): plan and execute the sub-pipeline through the
/// normal machinery, then collect it into the same rows-of-text shape as
/// `discoverRows`. Predicate/projection pushdown for SQL sources therefore comes
/// free from `buildPipeline`.
///
/// The sub-pipeline's sources are opened into `env.sources` like any other, so
/// they are closed here rather than left for the enclosing statement; the
/// per-pipeline scratch fields are restored so discovery cannot influence how the
/// body pipelines are planned.
pub fn discoverRowsPipeline(env: *Env, pipe: ast.Pipeline, ncols: usize) anyerror![]const Row {
    if (pipe.stages.len == 0) return planErr(env.diag, "for-each: empty discovery query");
    const src_base = env.sources.items.len;
    const saved_src_name = env.src_name;
    const saved_sql_desc = env.sql_desc;
    const saved_pq_readers = env.pq_readers;
    const saved_pq_reader = env.pq_reader;
    defer {
        for (env.sources.items[src_base..]) |sc| sc.close();
        env.sources.shrinkRetainingCapacity(src_base);
        env.src_name = saved_src_name;
        env.sql_desc = saved_sql_desc;
        env.pq_readers = saved_pq_readers;
        env.pq_reader = saved_pq_reader;
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

/// Discover for-each rows from a JSON array param (`for a, b in job.tables`):
/// navigate to the array, then bind each loop variable to the like-named field of
/// each object element (coerced to text). Mirrors `discoverRows` for reads.
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
