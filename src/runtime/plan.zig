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
//!
//! Unions and set operations are planned in `plan/union.zig`, FOR EACH discovery
//! rows in `plan/discover.zig`.

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
const keypush = @import("keypush.zig");
const joinorder = @import("joinorder.zig");
const obs = @import("obs.zig");
const Threshold = @import("../exec/value.zig").Threshold;
const Value = @import("../exec/value.zig").Value;

const aborting = @import("env.zig").aborting;
const Env = @import("env.zig").Env;
const MemSource = @import("env.zig").MemSource;
const forHintIdent = @import("env.zig").forHintIdent;
const forHintName = @import("env.zig").forHintName;
const mk = @import("env.zig").mk;
const parseByteSize = @import("env.zig").parseByteSize;
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
const projectJoinedSqlRead = @import("connect/sql_read.zig").projectJoinedSqlRead;
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
/// A key whose side the plan places by schema (`deferred`) adds every name either
/// value of its `=` uses, so the placement sees the same columns. A select of `*`
/// plus computed columns (a computed join key's) passes every column through and
/// asks only for what its expressions read; a lone `* EXCEPT` (the drop of those
/// keys after the join) passes the rest through. A `search` reads whole rows, so
/// it proves nothing.
pub fn projectedColumns(env: *Env, stages: []const ast.Stage) !?[][]const u8 {
    var w = Needs.init(env, null);
    return w.walk(stages);
}

/// The names a join and the stages after it (`after`) may take from its right side:
/// its keys and every name its ON uses, then what the rest uses that is the side's
/// (qualified by its alias) or could be (unqualified, a clash's `_r` suffix also read
/// as the bare name). Null when the rest keeps every column or cannot be read.
pub fn rightNeeds(env: *Env, j: ast.Join, after: []const ast.Stage) !?[][]const u8 {
    var w = Needs.init(env, if (j.alias.len > 0) j.alias else j.binding);
    for (j.right_keys) |q| try w.key(q);
    for (j.deferred) |d| {
        try w.expr(d.a);
        try w.expr(d.b);
    }
    if (j.residual) |e| try w.expr(e);
    return w.walk(after);
}

/// What `projectedColumns` and `rightNeeds` collect. `own`, when set, is the alias
/// whose qualified names are the read's own; names a join or a pass-through select
/// made (`made`) are no source's.
const Needs = struct {
    env: *Env,
    own: ?[]const u8,
    set: std.StringHashMap(void),
    right: std.array_list.Managed([]const u8),
    made: std.StringHashMap(void),
    joined: bool = false,
    whole_row: bool = false,

    fn init(env: *Env, own: ?[]const u8) Needs {
        return .{
            .env = env,
            .own = own,
            .set = std.StringHashMap(void).init(env.arena),
            .right = std.array_list.Managed([]const u8).init(env.arena),
            .made = std.StringHashMap(void).init(env.arena),
            .joined = own != null,
        };
    }

    fn walk(self: *Needs, stages: []const ast.Stage) !?[][]const u8 {
        var defines_output = false;
        for (stages) |st| {
            switch (st.node) {
                .filter => |e| try self.expr(e),
                .sort => |so| for (so.keys) |k| try self.field(k.field),
                .distinct => |d| {
                    const on = d.on orelse return null;
                    for (on) |q| try self.field(q);
                },
                .aggregate => |ag| {
                    for (ag.by) |q| try self.field(q);
                    for (ag.aggs) |a| if (a.arg) |e| try self.expr(e);
                    defines_output = true;
                    break;
                },
                .select => |items| {
                    if (passThrough(items)) {
                        for (items[1..]) |it| try self.expr(it.computed.expr);
                        for (items[1..]) |it| try self.made.put(it.computed.name, {});
                        continue;
                    }
                    if (items.len == 1 and items[0] == .star_except) continue;
                    for (items) |it| switch (it) {
                        .star, .star_except, .star_rename => return null,
                        .field => |q| try self.field(q),
                        .computed => |c| try self.expr(c.expr),
                    };
                    defines_output = true;
                    break;
                },
                .limit => {},
                .window => |wd| {
                    for (wd.partition_by) |q| try self.field(q);
                    for (wd.order_by) |k| try self.field(k.field);
                    for (wd.funcs) |f| if (f.arg) |q| try self.field(q);
                },
                .join => |j| {
                    for (j.left_keys) |q| try self.key(q);
                    for (j.deferred) |d| {
                        try self.expr(d.a);
                        try self.expr(d.b);
                        try self.made.put(d.left_name, {});
                        try self.made.put(d.right_name, {});
                    }
                    try self.right.append(if (j.alias.len > 0) j.alias else j.binding);
                    self.joined = true;
                    if (j.residual) |e| try self.expr(e);
                },
                else => return null,
            }
        }
        if (!defines_output or self.whole_row) return null;
        var out = std.array_list.Managed([]const u8).init(self.env.arena);
        var it = self.set.keyIterator();
        while (it.next()) |k| try out.append(k.*);
        return try out.toOwnedSlice();
    }

    /// `*` followed only by computed columns: every input column passes through.
    fn passThrough(items: []const ast.SelectItem) bool {
        if (items.len < 2 or items[0] != .star) return false;
        for (items[1..]) |it| if (it != .computed) return false;
        return true;
    }

    fn name(self: *Needs, n: []const u8) !void {
        if (self.made.contains(n)) return;
        try self.set.put(n, {});
        if (self.joined) if (clashBase(n)) |b| try self.set.put(b, {});
    }

    fn field(self: *Needs, q: ast.QualName) !void {
        if (q.parts.len > 1) {
            if (self.own) |o| if (std.ascii.eqlIgnoreCase(o, q.parts[0])) return self.name(q.parts[1]);
            for (self.right.items) |r| if (std.ascii.eqlIgnoreCase(r, q.parts[0])) return;
        }
        try self.name(q.parts[0]);
    }

    fn key(self: *Needs, q: ast.QualName) !void {
        if (q.parts.len > 1) if (self.own) |o| if (std.ascii.eqlIgnoreCase(o, q.parts[0])) return self.name(q.parts[1]);
        try self.name(q.parts[q.parts.len - 1]);
    }

    fn expr(self: *Needs, e: *const ast.Expr) !void {
        if (ast.hasSearch(e)) self.whole_row = true;
        if (!self.joined) {
            var names = std.StringHashMap(void).init(self.env.arena);
            try pushdown.collectFields(e, &names);
            var it = names.keyIterator();
            while (it.next()) |k| try self.name(k.*);
            return;
        }
        var quals = std.array_list.Managed(ast.QualName).init(self.env.arena);
        try pushdown.collectQuals(self.env.arena, e, &quals);
        for (quals.items) |q| try self.field(q);
    }
};

/// `x` for a right column renamed `x_r` or `x_r2` on clashing with a left one.
fn clashBase(n: []const u8) ?[]const u8 {
    const at = std.mem.lastIndexOf(u8, n, "_r") orelse return null;
    if (at == 0) return null;
    for (n[at + 2 ..]) |c| if (!std.ascii.isDigit(c)) return null;
    return n[0..at];
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
    return buildPipelineWith(env, stages_in, null);
}

/// A join's request that the pipeline's read take its keys: a SQL read opens late,
/// a Parquet one keeps its reader.
const LateReq = struct { want: keypush.Want, out: *?keypush.Taker };

/// The pipeline's own read taking the keys of the first join after it, which hands
/// it the right side's keys; `prefix` is what lies between them.
pub const ProbeTake = struct { late: keypush.Taker, prefix: []const ast.Stage };

/// The first join after `stages[0]` whose right side's keys narrow this SQL or
/// Parquet read: only filters and selects between, a kind that drops unmatched left
/// rows, and a right side that is not itself taking the left side's keys.
fn probeTaker(env: *Env, stages: []const ast.Stage) !?struct { idx: usize, want: keypush.Want } {
    for (stages[1..], 1..) |st, i| switch (st.node) {
        .filter, .select => {},
        .join => |j| {
            if (keypush.disabled(st.hints) or !keypush.leftMayNarrow(j)) return null;
            const want = (try keypush.takerFor(env, stages[0..i])) orelse return null;
            if (try rightTakes(env, j, st.hints)) return null;
            if (want == .parquet and try rightTakesParquet(env, j, st.hints)) return null;
            return .{ .idx = i, .want = want };
        },
        else => return null,
    };
    return null;
}

/// Whether the join's right side is a SQL read that takes the left side's keys.
pub fn rightTakes(env: *Env, j: ast.Join, hints: []const ast.Hint) !bool {
    if (keypush.disabled(hints) or !keypush.rightMayNarrow(j)) return false;
    const b = env.bindings.get(j.binding) orelse return false;
    return keypush.sqlDialect(env, try inlineHeadBindings(env, try j.rightStages(env.arena, b.stages))) != null;
}

fn rightTakesParquet(env: *Env, j: ast.Join, hints: []const ast.Hint) !bool {
    if (keypush.disabled(hints) or !keypush.rightMayNarrow(j)) return false;
    const b = env.bindings.get(j.binding) orelse return false;
    return keypush.parquetSide(env, try inlineHeadBindings(env, try j.rightStages(env.arena, b.stages)));
}

const Taken = struct { src: @import("../connect/driver.zig").Source, taker: ?keypush.Taker };

/// The pipeline's read opened to take a join's keys: a SQL read late, a Parquet one
/// as usual with its reader or folder kept. No taker when it did not open as Parquet.
fn openTaker(env: *Env, stages: []const ast.Stage, want: keypush.Want) !Taken {
    const rd = stages[0].node.read;
    switch (want) {
        .sql => |d| {
            const l = try keypush.LateSql.open(env, rd, stages[0].hints, d);
            return .{ .src = l.source(), .taker = .{ .sql = l } };
        },
        .parquet => {
            const r0 = env.pq_reader;
            const f0 = env.pq_folder;
            const src = try openSourceProjected(env, rd, stages[0].hints, try projectedColumns(env, stages[1..]), try filterBounds(env, stages[1..]));
            const pk = try env.arena.create(keypush.ParquetKeys);
            pk.* = .{ .arena = env.arena };
            if (env.pq_reader != r0) pk.reader = env.pq_reader;
            if (env.pq_folder != f0) pk.folder = env.pq_folder;
            if (pk.reader == null and pk.folder == null) return .{ .src = src, .taker = null };
            return .{ .src = src, .taker = .{ .parquet = pk } };
        },
    }
}

fn buildPipelineWith(env: *Env, stages_in: []const ast.Stage, late_req: ?LateReq) anyerror!PipeRes {
    const stages = try projectSqlRead(env, stages_in);
    if (stages.len == 0) return planErr(env.diag, "empty pipeline");
    if (try metaShortcut(env, stages)) |r| return r;

    var current: op.Op = undefined;
    var schema: types.Schema = undefined;
    var take: ?ProbeTake = null;
    var take_at: usize = 0;

    switch (stages[0].node) {
        .read => |rd| {
            const raw = if (late_req) |lr| blk: {
                const t = try openTaker(env, stages, lr.want);
                lr.out.* = t.taker;
                break :blk t.src;
            } else if (try probeTaker(env, stages)) |pt| blk: {
                const t = try openTaker(env, stages, pt.want);
                if (t.taker) |tk| {
                    take = .{ .late = tk, .prefix = stages[1..pt.idx] };
                    take_at = pt.idx;
                }
                break :blk t.src;
            } else try openSourceProjected(env, rd, stages[0].hints, try projectedColumns(env, stages[1..]), try filterBounds(env, stages[1..]));
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
        if (stage.node == .join) {
            const j = try joinorder.buildAt(env, stages, si, current, schema, if (take != null and si == take_at) take else null);
            current = j.res.op;
            schema = j.res.schema;
            si = j.next - 1;
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

pub fn readName(rd: ast.Read) []const u8 {
    return switch (rd.form) {
        .table => |q| q.last(),
        else => "",
    };
}

pub const synthReconcile = @import("plan/union.zig").synthReconcile;
pub const unionSpecs = @import("plan/union.zig").unionSpecs;
pub const unionCanon = @import("plan/union.zig").unionCanon;
pub const setOpName = @import("plan/union.zig").setOpName;
pub const unionDownstreamMapOnly = @import("plan/union.zig").unionDownstreamMapOnly;
const windowTopK = @import("plan/union.zig").windowTopK;
pub const unionExceptNames = @import("plan/union.zig").unionExceptNames;
const buildUnion = @import("plan/union.zig").buildUnion;
pub const discoverRows = @import("plan/discover.zig").discoverRows;
pub const discoverRowsPipeline = @import("plan/discover.zig").discoverRowsPipeline;
pub const discoverRowsJson = @import("plan/discover.zig").discoverRowsJson;

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
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = keys, .state = arena, .gpa = env.gpa, .err = env.errctx, .space = env.space, .spill_at = env.op_memory };
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
            o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .keys = ks, .threads = env.sort_threads, .space = env.space, .spill_at = env.op_memory, .gpa = env.gpa };
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
                var default: Value = .null;
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
                        if (f.default) |d| {
                            const v = eval.constEval(arena, try analyze.substExpr(arena, d, env.params_expr), &.{}, &.{}) catch |e|
                                return planErr(env.diag, try std.fmt.allocPrint(arena, "LAG/LEAD default: {s}", .{op.errLabel(e)}));
                            default = if (v.isNull()) .null else eval.castValueTyped(arena, v, ty) catch
                                return planErr(env.diag, try std.fmt.allocPrint(arena, "LAG/LEAD default does not fit `{s}` ({s})", .{ q.last(), try ty.name(arena) }));
                        }
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
                    .default = default,
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
        .join => |j| return buildJoin(env, j, stage.hints, schema, child, null),
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

pub fn buildProject(env: *Env, items: []const ast.SelectItem, in_schema: types.Schema, child: op.Op) anyerror!PipeRes {
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
    o.* = .{ .child = child, .in_schema = try schemaPtr(arena, schema), .by = ap.by, .aggs = aggs, .out_schema = out, .err = env.errctx, .state = arena, .gpa = env.gpa, .space = env.space, .spill_at = env.op_memory };
    return .{ .op = .{ .aggregate = o }, .schema = out.* };
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
        if (parseByteSize(txt)) |v| if (std.math.cast(usize, v)) |cap| return cap;
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "bad max_build `{s}` — use e.g. '512MB' or '8GB'", .{txt}));
    }
    return op.join_build_byte_cap;
}

/// Replace the binding chain at the head of `stages` with the stages it stands for —
/// a binding over a read, or over another binding — except a materialized binding
/// or one holding a window (`windowTopK` needs it apart).
pub fn inlineHeadBindings(env: *Env, stages_in: []const ast.Stage) ![]const ast.Stage {
    var stages = stages_in;
    var n: usize = 0;
    while (stages[0].node == .ref and n < 16) : (n += 1) {
        const name = stages[0].node.ref;
        if (env.materialized.contains(name)) break;
        const b = env.bindings.get(name) orelse break;
        if (b.stages.len == 0 or (b.stages[0].node != .read and b.stages[0].node != .ref)) break;
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

/// The build-side bytes past which a join spills: its `max_build` when given,
/// else the run's per-operator memory.
pub fn joinSpillAt(env: *Env, hints: []const ast.Hint) !usize {
    for (hints) |h| {
        if (std.mem.eql(u8, h.key, "max_build")) return joinBuildCap(env, hints);
    }
    return env.op_memory;
}

/// Whether the serial join could spill where a lane's in-memory index cannot:
/// there is a scratch space and the join is neither NOT IN nor CROSS.
pub fn joinMaySpill(env: *Env, j: ast.Join) bool {
    return env.space != null and !j.null_aware and j.kind != .cross;
}

/// The cap a lane's shared index is built under: the spill threshold where the
/// serial join could spill instead, so a larger build side falls back to it.
pub fn laneJoinCap(env: *Env, j: ast.Join, hints: []const ast.Hint) !usize {
    return if (joinMaySpill(env, j)) joinSpillAt(env, hints) else joinBuildCap(env, hints);
}

fn buildJoin(env: *Env, j: ast.Join, hints: []const ast.Hint, left_schema: types.Schema, probe: op.Op, take: ?ProbeTake) anyerror!PipeRes {
    const a = try assembleJoin(env, j, hints, try joinSide(env, j, hints, null), left_schema, probe, take);
    if (a.nl) |r| return r;
    return .{ .op = .{ .join = a.o }, .schema = a.schema };
}

pub const JoinSide = struct { rstages: []const ast.Stage, build: PipeRes, right_late: ?keypush.Taker };

/// A join's right side, planned, with its SQL or Parquet read when that read takes
/// the left side's keys. `after` is what follows the join in its pipeline, when
/// known: a SQL table read then asks only for the columns those stages may take.
pub fn joinSide(env: *Env, j: ast.Join, hints: []const ast.Hint, after: ?[]const ast.Stage) anyerror!JoinSide {
    if (env.bindings.get(j.binding) == null)
        return planErr(env.diag, try std.fmt.allocPrint(env.arena, "unknown binding `{s}` in join", .{j.binding}));
    var rstages = try prepareJoinSide(env, try j.rightStages(env.arena, env.bindings.get(j.binding).?.stages));
    if (after) |rest| if (try rightNeeds(env, j, rest)) |needs| {
        rstages = try projectJoinedSqlRead(env, rstages, needs);
    };
    var right_late: ?keypush.Taker = null;
    const a_want: ?keypush.Want = if (keypush.disabled(hints) or !keypush.rightMayNarrow(j))
        null
    else
        try keypush.takerFor(env, rstages);
    const build = if (a_want) |w|
        try buildPipelineWith(env, rstages, .{ .want = w, .out = &right_late })
    else
        try buildPipeline(env, rstages);
    return .{ .rstages = rstages, .build = build, .right_late = right_late };
}

/// A hash join (`o`), or for a join with no key the nested-loop plan (`nl`).
pub const Assembled = struct { o: *op.Join = undefined, schema: types.Schema, nl: ?PipeRes = null };

/// The join as written over a planned right side: `probe` streams, the right side
/// is indexed, and the output is the left columns then the right.
pub fn assembleJoin(env: *Env, j: ast.Join, hints: []const ast.Hint, side: JoinSide, left_schema: types.Schema, probe: op.Op, take: ?ProbeTake) anyerror!Assembled {
    const arena = env.arena;
    const rstages = side.rstages;
    const build = side.build;
    const right_late = side.right_late;

    var ad = analyze.Diag{};
    const prep = analyze.orientKeys(arena, left_schema, build.schema, j, &ad) catch |e| return aErr(env, &ad, e);
    const lsch = try mapChainSchema(env, prep.left, left_schema);
    const rsch = try mapChainSchema(env, prep.right, build.schema);
    const jp = analyze.joinPlan(arena, lsch, rsch, prep.join, &ad) catch |e| return aErr(env, &ad, e);
    const out = try schemaPtr(arena, jp.schema);
    if (prep.join.keyless()) return .{ .schema = out.*, .nl = try buildNLJoin(env, prep, hints, lsch, rsch, out, try buildChainFrom(arena, env.params_expr, env.errctx, prep.left, probe, left_schema), try buildChainFrom(arena, env.params_expr, env.errctx, prep.right, build.op, build.schema)) };
    const o = try arena.create(op.Join);
    o.* = .{
        .probe = try buildChainFrom(arena, env.params_expr, env.errctx, prep.left, probe, left_schema),
        .build = try buildChainFrom(arena, env.params_expr, env.errctx, prep.right, build.op, build.schema),
        .index = null,
        .left_keys = jp.lks,
        .right_keys = jp.rks,
        .left_schema = try schemaPtr(arena, lsch),
        .right_schema = try schemaPtr(arena, rsch),
        .out_schema = out,
        .kind = j.kind,
        .null_aware = j.null_aware,
        .state = arena,
        .err = env.errctx,
        .build_cap = try joinBuildCap(env, hints),
        .space = env.space,
        .spill_at = try joinSpillAt(env, hints),
    };
    if (analyze.residualPlan(arena, lsch, rsch, prep.join, env.params_expr, &ad) catch |e| return aErr(env, &ad, e)) |rp| {
        o.residual = rp.pred;
        o.pair_schema = try schemaPtr(arena, rp.schema);
    }
    const right_takes = if (right_late) |rl| rl == .sql or take == null else false;
    if (right_takes) {
        if (try keyExprs(arena, try std.mem.concat(arena, ast.Stage, &.{ rstages[1..], prep.right }), prep.join.right_keys)) |ex|
            o.push_build = try right_late.?.bind(ex);
    } else if (take) |t| {
        if (try keyExprs(arena, try std.mem.concat(arena, ast.Stage, &.{ t.prefix, prep.left }), prep.join.left_keys)) |ex|
            o.push_probe = try t.late.bind(ex);
    }
    return .{ .o = o, .schema = out.* };
}

/// A join with no key: a nested loop over its ON, with the range path when the ON
/// bounds a left value by right ones. It takes no key pushdown and never spills.
fn buildNLJoin(env: *Env, prep: analyze.KeyPrep, hints: []const ast.Hint, lsch: types.Schema, rsch: types.Schema, out: *const types.Schema, probe: op.Op, build: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    var ad = analyze.Diag{};
    const o = try arena.create(op.NLJoin);
    o.* = .{
        .probe = probe,
        .build = build,
        .left_schema = try schemaPtr(arena, lsch),
        .right_schema = try schemaPtr(arena, rsch),
        .out_schema = out,
        .kind = prep.join.kind,
        .state = arena,
        .err = env.errctx,
        .build_cap = try joinBuildCap(env, hints),
    };
    if (analyze.residualPlan(arena, lsch, rsch, prep.join, env.params_expr, &ad) catch |e| return aErr(env, &ad, e)) |rp| {
        o.cond = rp.pred;
        o.pair_schema = try schemaPtr(arena, rp.schema);
        o.range = try op.nlRangeOf(arena, rp.pred, rp.schema, lsch.fields.len);
    }
    return .{ .op = .{ .nl_join = o }, .schema = out.* };
}

/// Whether a pipeline over a local file small enough to read ahead runs serially,
/// so a join's SQL right side can take its keys: the lanes read the right side in
/// full. A larger file would pass the read-ahead cap and gain nothing.
pub fn keysPreferSerial(env: *Env, stages: []const ast.Stage) !bool {
    if (stages.len == 0 or stages[0].node != .read) return false;
    const rd = stages[0].node.read;
    if (rd.form != .path or csv.CsvReader.isUrl(rd.form.path)) return false;
    const st = std.fs.cwd().statFile(rd.form.path) catch return false;
    if (st.kind != .file or st.size > op.default_prefetch_bytes) return false;
    for (stages[1..]) |s| if (s.node == .join and try rightTakes(env, s.node.join, s.hints)) return true;
    return false;
}

/// Each join key traced to its read's columns, or null when none of them is.
pub fn keyExprs(arena: std.mem.Allocator, stages: []const ast.Stage, keys: []const ast.QualName) !?[]const ?*ast.Expr {
    const out = try arena.alloc(?*ast.Expr, keys.len);
    var any = false;
    for (keys, out) |k, *e| {
        e.* = try keypush.traceKey(arena, stages, k.last());
        if (e.* != null) any = true;
    }
    return if (any) out else null;
}

pub fn jsonToStr(arena: std.mem.Allocator, v: std.json.Value) ![]const u8 {
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

fn sortedNames(names: ?[][]const u8) ![]const []const u8 {
    const n = names orelse return error.TestUnexpectedResult;
    std.mem.sort([]const u8, n, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.lt);
    return n;
}

test "projectedColumns and rightNeeds: computed and deferred join keys narrow both sides to what they read" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../lang/sql_parser.zig");
    const env_mod = @import("env.zig");
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\SELECT s.qty, c.name FROM 's.csv' s JOIN 'c.csv' c ON trim(c.code) = CAST(s.cr AS varchar);
        \\SELECT qty, nm_r FROM 's.csv' JOIN 'c.csv' ON trim(code) = CAST(cr AS varchar);
        \\SELECT * FROM 's.csv' s JOIN 'c.csv' c ON trim(c.code) = CAST(s.cr AS varchar);
    , &pd);
    var params = std.StringHashMap(Value).init(a);
    var bindings = std.StringHashMap(ast.Pipeline).init(a);
    var connections = std.StringHashMap(ast.Connection).init(a);
    var sources = std.array_list.Managed(@import("../connect/driver.zig").Source).init(a);
    var diag = env_mod.Diag{};
    var log = obs.Logger.init(0, .text, .err);
    var params_expr = std.StringHashMap(*const ast.Expr).init(a);
    var errctx = op.ErrCtx{};
    var rows = obs.RowCounter.init(0);
    var json = std.StringHashMap(std.json.Value).init(a);
    const fns = std.StringHashMap(ast.FnDecl).init(a);
    var env = Env{ .arena = a, .gpa = a, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .request_body = null, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns };

    var outs = std.array_list.Managed([]const ast.Stage).init(a);
    for (prog.stmts) |st| if (st == .output) try outs.append(st.output.stages);
    const want = [_][]const []const u8{
        &.{ "cr", "qty" },
        &.{ "code", "cr", "nm", "nm_r", "qty" },
    };
    const want_right = [_][]const []const u8{
        &.{ "__jk", "name", "qty" },
        &.{ "code", "cr", "nm", "nm_r", "qty" },
    };
    for (outs.items[0..2], want, want_right) |stages, w, wr| {
        const got = try sortedNames(try projectedColumns(&env, stages[1..]));
        try std.testing.expectEqual(w.len, got.len);
        for (w, got) |x, y| try std.testing.expectEqualStrings(x, y);
        for (stages[1..], 1..) |st, i| if (st.node == .join) {
            const r = try sortedNames(try rightNeeds(&env, st.node.join, stages[i + 1 ..]));
            try std.testing.expectEqual(wr.len, r.len);
            for (wr, r) |x, y| try std.testing.expectEqualStrings(if (std.mem.eql(u8, x, "__jk")) st.node.join.right_keys[0].last() else x, y);
        };
    }
    try std.testing.expect((try projectedColumns(&env, outs.items[2][1..])) == null);
}

test {
    _ = @import("estimate.zig");
    _ = @import("joinorder.zig");
    _ = @import("plan/discover.zig");
    _ = @import("plan/union.zig");
}
