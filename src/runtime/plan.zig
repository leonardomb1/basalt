//! Plan building: turns analyzed stages into the operator tree (projections,
//! filters, joins, aggregates, windows, unions and set operations), the
//! per-stage schema derivations, and row discovery (a pipeline drained for the
//! rows a `for`/`union` expands over).
//!
//! Each stage is typed once (`analyze.resolve`) into the record its operator is
//! built from. Parallel plans build their map chains per worker on a thread arena
//! from the shared records (`chainOps`), so no mutable operator state is shared
//! across threads and a worker never types a stage.
//!
//! Source-side optimisations are only allowed to lose speed, never rows:
//! `projectedColumns` and `filterBounds` under-report rather than guess (a missed
//! column fails loudly, a missed bound only skips less), and the Parquet footer
//! shortcut for `COUNT(*)`/`MIN`/`MAX` applies only to an unfiltered, ungrouped
//! read of the file itself. A field qualified by a joined right side is that
//! side's column and is never asked of the read. A CTE, derived table or joined side
//! is planned with its filters moved toward its read and the select columns its
//! reader leaves unused dropped (`pruneSelects`), so neither boundary widens the read.
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
const msgfmt = @import("../format/msgfmt.zig");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const op = @import("../exec/op.zig");
const profile = @import("../exec/profile.zig");
const column = @import("../exec/column.zig");
const csv = @import("../format/csv.zig");
const pqdecode = @import("../format/parquet/read.zig");
const analyze = @import("analyze.zig");
const pushdown = @import("pushdown.zig");
const keypush = @import("plan/keypush.zig");
const joinorder = @import("plan/joinorder.zig");
const obs = @import("obs.zig");
const Threshold = @import("../exec/value.zig").Threshold;
const Value = @import("../exec/value.zig").Value;

const Env = @import("env.zig").Env;
const MemSource = @import("env.zig").MemSource;
const options = @import("analyze/options.zig");
const PipeRes = @import("env.zig").PipeRes;
const aErr = @import("env.zig").aErr;
const planErr = @import("env.zig").planErr;
const schemaPtr = @import("env.zig").schemaPtr;

const ConstSource = @import("connect.zig").ConstSource;
const OneBatch = @import("connect.zig").OneBatch;
const openSourceProjected = @import("connect.zig").openSourceProjected;
const projectSqlRead = @import("connect.zig").projectSqlRead;
const projectJoinedSqlRead = @import("connect/sql_read.zig").projectJoinedSqlRead;
const sql_read = @import("connect/sql_read.zig");
const factsIfWanted = @import("connect.zig").factsIfWanted;
const sqlConnInfo = @import("connect.zig").sqlConnInfo;
const sourceLabel = @import("connect.zig").sourceLabel;

/// Rebuild a map-only `filter`/`select` chain against the projected source schema
/// for `parallel.run`, keeping column indices in step; null if anything does not fit.
pub fn rebuildMapStages(env: *Env, middle: []const ast.Stage, proj_schema: *const types.Schema) ?[]const op.Stage {
    for (middle) |st| if (!analyze.isMap(st.node, false)) return null;
    var ad = analyze.Diag{};
    const c = analyze.resolveChain(env.arena, proj_schema.*, middle, env.params_expr, &ad) catch return null;
    const ob = env.arena.create(OneBatch) catch return null;
    ob.* = .{ .b = null, .sch = proj_schema.* };
    const scan = env.arena.create(op.Scan) catch return null;
    scan.* = .{ .src = ob.source() };
    const chain = chainOps(env.arena, env.errctx, c.recs, .{ .scan = scan }) catch return null;
    const lin = (op.linearize(env.arena, chain) catch return null) orelse return null;
    return lin.stages;
}

/// `analyze.resolve` for the run, its error the run's.
pub fn resolveAt(env: *Env, in: types.Schema, st: ast.Stage) !analyze.Resolved {
    var ad = analyze.Diag{};
    return analyze.resolve(env.arena, in, st, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
}

/// `analyze.resolveChain` for the run, its error the run's, placed at its stage.
pub fn chainAt(env: *Env, in: types.Schema, stages: []const ast.Stage) !analyze.Chain {
    var ad = analyze.Diag{};
    return analyze.resolveChain(env.arena, in, stages, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
}

/// The streaming operator map record `r` stands for over `child`, made on `ta` (a
/// lane's arena) from the shared record. `errctx` says where a row failed; without it
/// a lane's error was a bare `cast failed`.
pub fn mapOp(ta: std.mem.Allocator, errctx: ?*op.ErrCtx, r: analyze.Resolved, child: op.Op) !op.Op {
    switch (r.plan) {
        .filter => |pred| {
            const f = try ta.create(op.Filter);
            f.* = .{ .child = child, .pred = pred, .err = errctx, .back = ta };
            return .{ .filter = f };
        },
        .project => |cols| {
            const p = try ta.create(op.Project);
            p.* = .{ .child = child, .cols = cols, .out_schema = r.out, .err = errctx };
            return .{ .project = p };
        },
        .explode => |idx| {
            const ex = r.stage.node.explode;
            const o = try ta.create(op.Explode);
            o.* = .{ .child = child, .field_idx = idx, .delim = ex.delim orelse ",", .json = ex.json, .out_schema = r.out };
            return .{ .explode = o };
        },
        else => unreachable,
    }
}

/// The map records `recs` as operators over `start`, each keyed to its stage.
pub fn chainOps(ta: std.mem.Allocator, errctx: ?*op.ErrCtx, recs: []const analyze.Resolved, start: op.Op) !op.Op {
    var cur = start;
    for (recs) |r| {
        cur = try mapOp(ta, errctx, r, cur);
        cur.stats().at = profile.keyOf(r.stage.pos);
    }
    return cur;
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
                        .star, .star_except, .star_rename, .star_of => return null,
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

/// `stages` with each select's columns that no later stage reads dropped, so a CTE
/// or derived table never reads a column its reader leaves unused. A select stays
/// whole when what follows is unprovable (a `*`, a plain DISTINCT, whose rows a column
/// sets apart, a `search`, or no later select or aggregate fixing the output) or when
/// two of its items share a name, which a drop would rename. A computed column that
/// may fail on a row is kept, so a run that failed on it still fails.
pub fn pruneSelects(arena: std.mem.Allocator, stages_in: []const ast.Stage) ![]const ast.Stage {
    var out: ?[]ast.Stage = null;
    var i = stages_in.len;
    outer: while (i > 0) {
        i -= 1;
        const cur: []const ast.Stage = out orelse stages_in;
        const items = switch (cur[i].node) {
            .select => |s| s,
            else => continue,
        };
        const used = (try laterNames(arena, cur[i + 1 ..])) orelse continue;
        var names = std.StringHashMap(void).init(arena);
        var keep = std.array_list.Managed(ast.SelectItem).init(arena);
        for (items) |it| {
            const name = switch (it) {
                .field => |q| q.last(),
                .computed => |c| c.name,
                else => continue :outer,
            };
            if (std.mem.indexOf(u8, name, "${") != null) continue :outer;
            if ((try names.getOrPut(name)).found_existing) continue :outer;
            const raises = it == .computed and pushdown.mayRaise(it.computed.expr);
            if (raises or used.contains(name)) try keep.append(it);
        }
        if (keep.items.len == items.len) continue;
        if (keep.items.len == 0) try keep.append(items[0]);
        const o = out orelse try arena.dupe(ast.Stage, stages_in);
        o[i].node = .{ .select = try keep.toOwnedSlice() };
        out = o;
    }
    const o = out orelse return stages_in;
    for (o, 0..) |*st, k| {
        if (st.node != .join or st.node.join.own_cols.len == 0) continue;
        const refs = (try allNames(arena, o[k + 1 ..])) orelse return stages_in;
        var own = std.array_list.Managed(ast.QualName).init(arena);
        for (st.node.join.own_cols) |q| if (refs.contains(q.last())) try own.append(q);
        st.node.join.own_cols = try own.toOwnedSlice();
    }
    return o;
}

/// Every name any of `stages` reads, null when one holds a `search`.
fn allNames(arena: std.mem.Allocator, stages: []const ast.Stage) !?std.StringHashMap(void) {
    var set = std.StringHashMap(void).init(arena);
    for (stages) |st| switch (st.node) {
        .write => {},
        .explode => |ex| try set.put(ex.field, {}),
        .distinct => |d| if (d.on) |on| for (on) |q| try qualNames(q, &set),
        .select => |items| for (items) |it| switch (it) {
            .field => |q| try qualNames(q, &set),
            .computed => |c| if (!try exprNames(arena, c.expr, &set)) return null,
            else => {},
        },
        else => if (try stageReads(arena, st, &set) == .opaque_) return null,
    };
    return set;
}

/// Every name the stages up to the next select or aggregate may read, each part of a
/// qualified one and a clash-renamed `x_r`'s `x` too; null when unprovable.
fn laterNames(arena: std.mem.Allocator, stages: []const ast.Stage) !?std.StringHashMap(void) {
    var set = std.StringHashMap(void).init(arena);
    for (stages) |st| switch (try stageReads(arena, st, &set)) {
        .opaque_ => return null,
        .defines => return set,
        .passes => {},
    };
    return null;
}

const Reads = enum { passes, defines, opaque_ };

/// Adds the names `st` reads to `set`, and says whether its output is its input's
/// rows and columns (`passes`), a new set of columns (`defines`), or unprovable.
fn stageReads(arena: std.mem.Allocator, st: ast.Stage, set: *std.StringHashMap(void)) !Reads {
    switch (st.node) {
        .filter => |e| if (!try exprNames(arena, e, set)) return .opaque_,
        .limit => {},
        .sort => |so| for (so.keys) |k| try qualNames(k.field, set),
        .distinct => |d| for (d.on orelse return .opaque_) |q| try qualNames(q, set),
        .window => |wd| {
            for (wd.partition_by) |q| try qualNames(q, set);
            for (wd.order_by) |k| try qualNames(k.field, set);
            for (wd.funcs) |f| {
                if (f.arg) |q| try qualNames(q, set);
                if (f.offset_expr) |e| if (!try exprNames(arena, e, set)) return .opaque_;
                if (f.default) |e| if (!try exprNames(arena, e, set)) return .opaque_;
            }
        },
        .join => |j| {
            for (j.left_keys) |q| try qualNames(q, set);
            for (j.own_cols) |q| try qualNames(q, set);
            for (j.deferred) |d| {
                if (!try exprNames(arena, d.a, set)) return .opaque_;
                if (!try exprNames(arena, d.b, set)) return .opaque_;
            }
            if (j.residual) |e| if (!try exprNames(arena, e, set)) return .opaque_;
        },
        .aggregate => |ag| {
            for (ag.by) |q| try qualNames(q, set);
            for (ag.aggs) |a| if (a.arg) |e| if (!try exprNames(arena, e, set)) return .opaque_;
            return .defines;
        },
        .select => |items| {
            const through = Needs.passThrough(items) or (items.len == 1 and items[0] == .star_except);
            for (items) |it| switch (it) {
                .star, .star_except => if (!through) return .opaque_,
                .star_rename, .star_of => return .opaque_,
                .field => |q| try qualNames(q, set),
                .computed => |c| if (!try exprNames(arena, c.expr, set)) return .opaque_,
            };
            if (!through) return .defines;
        },
        else => return .opaque_,
    }
    return .passes;
}

fn exprNames(arena: std.mem.Allocator, e: *const ast.Expr, set: *std.StringHashMap(void)) !bool {
    if (ast.hasSearch(e)) return false;
    var quals = std.array_list.Managed(ast.QualName).init(arena);
    try pushdown.collectQuals(arena, e, &quals);
    for (quals.items) |q| try qualNames(q, set);
    return true;
}

fn qualNames(q: ast.QualName, set: *std.StringHashMap(void)) !void {
    if (q.dollar) return;
    for (q.parts) |p| try set.put(p, {});
    if (clashBase(q.last())) |b| try set.put(b, {});
}

/// `stages` with the select columns `after`, the stages reading their output, leave
/// unused dropped.
fn pruneFor(arena: std.mem.Allocator, stages: []const ast.Stage, after: []const ast.Stage) ![]const ast.Stage {
    if (stages.len == 0 or after.len == 0) return stages;
    const whole = try std.mem.concat(arena, ast.Stage, &.{ stages, after });
    return (try pruneSelects(arena, whole))[0..stages.len];
}

/// `pruneSelects` where every source `stages` reads has columns known offline
/// (`pushdown.sourcesKnown`); elsewhere unchanged, so a column a database lacks still
/// reaches its read and fails there.
pub fn pruneKnown(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    if (!pushdown.sourcesKnown(env.arena, env.bindings, stages, env.tables)) return stages;
    return pruneSelects(env.arena, stages);
}

/// A select of `needs`, standing for what reads a joined side (its `rightNeeds`).
fn needsTail(arena: std.mem.Allocator, needs: []const []const u8) ![]const ast.Stage {
    const items = try arena.alloc(ast.SelectItem, needs.len);
    for (needs, items) |n, *it| it.* = .{ .field = .{ .parts = try arena.dupe([]const u8, &.{n}) } };
    const tail = try arena.alloc(ast.Stage, 1);
    tail[0] = .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } };
    return tail;
}

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
        .dec_lit => |x| .{ .decimal = .{ .unscaled = x.unscaled, .scale = x.scale } },
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
    const r = analyze.resolve(env.arena, rdr.schema, stages[1], env.params_expr, &ad) catch return null;
    const aggs = r.plan.aggregate.aggs;

    const vals = try env.arena.alloc(Value, aggs.len);
    for (aggs, vals) |ra, *out| {
        if (ra.distinct) return null;
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

    const out = r.out;
    const cols = try env.arena.alloc(column.Column, aggs.len);
    for (aggs, vals, cols) |ra, v, *c| {
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
    var ld = analyze.Diag{};
    const bound = analyze.bindLimits(env.arena, stages_in, env.params_expr, &ld) catch |e| return aErr(env, &ld, e);
    const stages = try projectSqlRead(env, bound);
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
                const b = (try stampedBinding(env, name)) orelse
                    return planErr(env.diag, try msgfmt.allocPrint(env.arena, "unknown binding `{s}`", .{name}));
                const r = if (b.stages.len == 1 and b.stages[0].node == .union_)
                    try buildUnion(env, b.stages[0].node.union_, b.stages[0].hints, unionExceptNames(stages[1..]))
                else
                    try buildPipeline(env, try prepareSide(env, (try windowTopK(env.arena, b.stages, stages[1..])) orelse b.stages, stages[1..]));
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
    current.stampNew(profile.keyOf(stages[0].pos));

    var si: usize = 1;
    while (si < stages.len) : (si += 1) {
        const stage = stages[si];
        if (stage.node == .sort and si + 1 < stages.len and stages[si + 1].node == .limit and op.TopN.fits(stages[si + 1].node.limit)) {
            const r = try buildTopN(env, try resolveAt(env, schema, stage), stages[si + 1].node.limit, current);
            current = r.op;
            schema = r.schema;
            current.stampNew(profile.keyOf(stage.pos));
            si += 1;
            continue;
        }
        if (stage.node == .join) {
            const j = try joinorder.buildAt(env, stages, si, current, schema, if (take != null and si == take_at) take else null);
            current = j.res.op;
            schema = j.res.schema;
            current.stampNew(profile.keyOf(stage.pos));
            si = j.next - 1;
            continue;
        }
        const r = try buildStage(env, stage, current, schema);
        current = r.op;
        schema = r.schema;
        current.stampNew(profile.keyOf(stage.pos));
    }
    return .{ .op = current, .schema = schema };
}

/// Pushes the running K-th-best bound into a single parquet source with one sort
/// key, so it can skip row groups its statistics rule out.
/// `srt` is the sort's record.
pub fn buildTopN(env: *Env, srt: analyze.Resolved, lim: ast.Limit, child: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    const s = srt.stage.node.sort;
    const o = try arena.create(op.TopN);
    o.* = .{ .child = child, .in_schema = srt.in, .keys = srt.plan.sort, .count = lim.count, .offset = lim.offset, .state = arena, .gpa = env.gpa };

    if (env.pq_readers == 1 and s.keys.len == 1 and s.keys[0].field.parts.len == 1) {
        if (env.pq_reader != null or env.pq_folder != null) {
            const t = try arena.create(Threshold);
            t.* = .{ .column = s.keys[0].field.last(), .desc = s.keys[0].desc, .nulls_first = s.keys[0].nulls_first };
            if (env.pq_reader) |pr| pr.threshold = t;
            if (env.pq_folder) |f| f.threshold = t;
            o.threshold = t;
        }
    }
    return .{ .op = .{ .top_n = o }, .schema = srt.in.* };
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
const windowTopK = @import("plan/union.zig").windowTopK;
pub const unionExceptNames = @import("plan/union.zig").unionExceptNames;
const buildUnion = @import("plan/union.zig").buildUnion;
pub const discoverRows = @import("plan/discover.zig").discoverRows;
pub const discoverRowsPipeline = @import("plan/discover.zig").discoverRowsPipeline;
pub const discoverRowsJson = @import("plan/discover.zig").discoverRowsJson;

pub fn buildStage(env: *Env, stage: ast.Stage, child: op.Op, schema: types.Schema) anyerror!PipeRes {
    errdefer env.diag.stamp(stage.pos);
    if (stage.node == .join) return buildJoin(env, stage.node.join, stage.hints, schema, child, null);
    return buildOp(env, try resolveAt(env, schema, stage), child);
}

/// The operator record `r` stands for over `child`. A window's `$param` offsets and
/// LAG/LEAD defaults are folded here, from the run's values.
pub fn buildOp(env: *Env, r: analyze.Resolved, child: op.Op) anyerror!PipeRes {
    errdefer env.diag.stamp(r.stage.pos);
    const arena = env.arena;
    const o: op.Op = switch (r.plan) {
        .filter, .project, .explode => try mapOp(arena, env.errctx, r, child),
        .limit => blk: {
            const lim = r.stage.node.limit;
            const l = try arena.create(op.Limit);
            l.* = .{ .child = child, .remaining = lim.count, .to_skip = lim.offset };
            break :blk .{ .limit = l };
        },
        .distinct => |keys| blk: {
            const d = try arena.create(op.Distinct);
            const st = try ownState(env);
            d.* = .{ .child = child, .in_schema = r.in, .keys = keys, .state = st.allocator(), .own_state = st, .gpa = env.gpa, .err = env.errctx, .space = env.space, .spill_at = env.op_memory };
            break :blk .{ .distinct = d };
        },
        .sort => |ks| blk: {
            const so = try arena.create(op.Sort);
            so.* = .{ .child = child, .in_schema = r.in, .keys = ks, .threads = env.sort_threads, .space = env.space, .spill_at = env.op_memory, .gpa = env.gpa };
            break :blk .{ .sort = so };
        },
        .window => |wp| blk: {
            const wd = r.stage.node.window;
            const pk = try arena.alloc(op.Sort.Key, wp.part.len);
            for (wp.part, pk) |idx, *k| k.* = .{ .idx = idx, .desc = false, .nulls_first = false };
            const ok = try arena.alloc(op.Sort.Key, wp.ord.len);
            for (wd.order_by, wp.ord, ok) |sk, idx, *k| k.* = .{ .idx = idx, .desc = sk.desc, .nulls_first = sk.nulls_first };
            const funcs = try arena.dupe(op.Window.Func, wp.funcs);
            for (wd.funcs, funcs, 0..) |f, *out, i| {
                if (f.offset_expr) |e| {
                    var od = analyze.Diag{};
                    out.offset = analyze.windowOffset(arena, f, try analyze.substExpr(arena, e, env.params_expr), &od) catch |err| return aErr(env, &od, err);
                }
                const d = f.default orelse continue;
                const q = f.arg orelse continue;
                const ty = r.out.fields[r.in.fields.len + i].ty;
                out.default = switch (try analyze.lagDefault(arena, try analyze.substExpr(arena, d, env.params_expr), q, ty)) {
                    .value => |v| v,
                    .problem => |why| return planErr(env.diag, why),
                };
            }
            const w = try arena.create(op.Window);
            w.* = .{
                .child = child,
                .in_schema = r.in,
                .out_schema = r.out,
                .part = pk,
                .ord = ok,
                .funcs = funcs,
                .range = wp.range,
                .err = env.errctx,
                .top_k = wd.top_k,
                .gpa = env.gpa,
                .threads = env.sort_threads,
            };
            break :blk .{ .window = w };
        },
        .aggregate => |ag| blk: {
            const a = try arena.create(op.Aggregate);
            const st = try ownState(env);
            a.* = .{ .child = child, .in_schema = r.in, .by = ag.by, .aggs = ag.aggs, .out_schema = r.out, .err = env.errctx, .state = st.allocator(), .own_state = st, .gpa = env.gpa, .space = env.space, .spill_at = env.op_memory };
            break :blk .{ .aggregate = a };
        },
    };
    return .{ .op = o, .schema = r.out.* };
}

pub fn buildProject(env: *Env, items: []const ast.SelectItem, in_schema: types.Schema, child: op.Op) anyerror!PipeRes {
    const r = try resolveAt(env, in_schema, .{ .node = .{ .select = items }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } });
    return .{ .op = try mapOp(env.arena, env.errctx, r, child), .schema = r.out.* };
}

/// A grouping operator's own state arena on the run's gpa, freed by the operator's
/// `release` when its statement or loop row is done: on the plan arena, every
/// row of a sequential FOR EACH kept its groups until the run ended.
fn ownState(env: *Env) !*std.heap.ArenaAllocator {
    const st = try env.arena.create(std.heap.ArenaAllocator);
    st.* = std.heap.ArenaAllocator.init(env.gpa);
    return st;
}

pub fn joinBuildCap(env: *Env, hints: []const ast.Hint) !usize {
    for (hints) |h| {
        if (!std.mem.eql(u8, h.key, "max_build")) continue;
        return switch (try options.maxBuild(env.arena, h)) {
            .bytes => |cap| cap,
            .problem => |why| planErr(env.diag, why),
        };
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
        const b = (try stampedBinding(env, name)) orelse break;
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

/// `stages` as a statement of their own, run without `renderBound` (a query LET, a
/// FOR EACH discovery): `env.now_us` set to a fresh instant and stamped on them.
pub fn ownClock(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    const at = std.time.microTimestamp();
    env.now_us = at;
    return ast.stampClock(env.arena, stages, at);
}

/// The binding `name` as the statement planning it runs it: its `now()` and
/// `today()` on the statement's instant, not the one the binding was rendered at.
pub fn stampedBinding(env: *Env, name: []const u8) !?ast.Pipeline {
    var b = env.bindings.get(name) orelse return null;
    if (env.now_us) |at| b.stages = try ast.stampClock(env.arena, b.stages, at);
    return b;
}

/// The implicit pushdown: the contiguous WHERE right after a SQL read, translated
/// into that read's own query.
pub fn descendLeadingWhere(env: *Env, stages: []const ast.Stage) ![]const ast.Stage {
    if (stages[0].node != .read) return stages;
    const arena = env.arena;
    const rd = stages[0].node.read;
    const conn = env.connections.get(rd.connector) orelse return stages;
    const d = (sqlConnInfo(conn) orelse return stages).dialect;
    if (!sql_read.pushableRead(d, rd)) return stages;
    const facts = try factsIfWanted(env, rd, d, stages[1..], .{ .fields = &.{} }, false, .superset);
    const extra = (try pushdown.serialWhereWith(arena, d, stages, facts, null)) orelse return stages;
    const out = try arena.dupe(ast.Stage, stages);
    var nrd = rd;
    nrd.where = if (rd.where.len > 0)
        try msgfmt.allocPrint(arena, "({s}) AND ({s})", .{ rd.where, extra })
    else
        extra;
    out[0].node = .{ .read = nrd };
    return out;
}

/// A join's right side readied like a query's head pipeline, with filters moved
/// and the SQL read narrowed, so a joined CTE no longer reads its whole table.
pub fn prepareJoinSide(env: *Env, stages_in: []const ast.Stage) ![]const ast.Stage {
    return prepareSide(env, stages_in, &.{});
}

/// `prepareJoinSide`, the select columns `after` (the stages reading the side's
/// output) leaves unused dropped before the read is narrowed to what remains.
fn prepareSide(env: *Env, stages_in: []const ast.Stage, after: []const ast.Stage) ![]const ast.Stage {
    if (stages_in.len == 0) return stages_in;
    var stages = try inlineHeadBindings(env, stages_in);
    stages = try analyze.substFilterParams(env.arena, stages, env.params_expr);
    if (try pushdown.hoistFilters(env.arena, env.gpa, stages, env.bindings, env.tables)) |h| stages = h;
    if (pushdown.sourcesKnown(env.arena, env.bindings, stages, env.tables)) stages = try pruneFor(env.arena, stages, after);
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
    return if (joinMaySpill(env, j)) joinSpillAt(env, hints) else hashJoinCap(env, j, hints);
}

/// A hash join's in-memory cap: NOT IN, which never spills, fails past the spill
/// threshold; every other join holds up to `max_build` or the 4 GiB default.
fn hashJoinCap(env: *Env, j: ast.Join, hints: []const ast.Hint) !usize {
    return if (j.null_aware) joinSpillAt(env, hints) else joinBuildCap(env, hints);
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
        return planErr(env.diag, try msgfmt.allocPrint(env.arena, "unknown binding `{s}` in join", .{j.binding}));
    const needs = if (after) |rest| try rightNeeds(env, j, rest) else null;
    const tail: []const ast.Stage = if (needs) |n| try needsTail(env.arena, n) else &.{};
    var rstages = try prepareSide(env, try j.rightStages(env.arena, (try stampedBinding(env, j.binding)).?.stages), tail);
    if (needs) |n| rstages = try projectJoinedSqlRead(env, rstages, n);
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
    const jr = analyze.resolveJoin(arena, left_schema, build.schema, j, env.params_expr, &ad) catch |e| return aErr(env, &ad, e);
    const prep = jr.keys;
    const out = try schemaPtr(arena, jr.plan.schema);
    const probe_chain = try chainOps(arena, env.errctx, jr.left.recs, probe);
    const build_chain = try chainOps(arena, env.errctx, jr.right.recs, build.op);
    if (prep.join.keyless()) return .{ .schema = out.*, .nl = try buildNLJoin(env, jr, hints, out, probe_chain, build_chain) };
    const o = try arena.create(op.Join);
    o.* = .{
        .probe = probe_chain,
        .build = build_chain,
        .index = null,
        .left_keys = jr.plan.lks,
        .right_keys = jr.plan.rks,
        .left_schema = jr.left.out,
        .right_schema = jr.right.out,
        .out_schema = out,
        .kind = j.kind,
        .null_aware = j.null_aware,
        .state = arena,
        .err = env.errctx,
        .build_cap = try hashJoinCap(env, j, hints),
        .threads = env.sort_threads,
        .space = env.space,
        .spill_at = try joinSpillAt(env, hints),
    };
    if (jr.residual) |rp| {
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
fn buildNLJoin(env: *Env, jr: analyze.JoinResolved, hints: []const ast.Hint, out: *const types.Schema, probe: op.Op, build: op.Op) anyerror!PipeRes {
    const arena = env.arena;
    const o = try arena.create(op.NLJoin);
    o.* = .{
        .probe = probe,
        .build = build,
        .left_schema = jr.left.out,
        .right_schema = jr.right.out,
        .out_schema = out,
        .kind = jr.keys.join.kind,
        .state = arena,
        .err = env.errctx,
        .build_cap = try joinBuildCap(env, hints),
    };
    if (jr.residual) |rp| {
        o.cond = rp.pred;
        o.pair_schema = try schemaPtr(arena, rp.schema);
        o.range = try op.nlRangeOf(arena, rp.pred, rp.schema, jr.left.out.fields.len);
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
        .integer => |i| try msgfmt.allocPrint(arena, "{d}", .{i}),
        .float => |f| try msgfmt.allocPrint(arena, "{d}", .{f}),
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
    var env = Env{ .arena = a, .gpa = a, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns };

    const side = [_]ast.Stage{.{ .node = .{ .ref = "c" }, .hints = &.{}, .pos = .{ .line = 0, .col = 0 } }};
    const out = try prepareJoinSide(&env, &side);
    const rd = out[0].node.read;
    try std.testing.expectEqualStrings("(\"active\" = 1)", rd.where);
    try std.testing.expectEqual(@as(usize, 3), rd.cols.len);
    try std.testing.expectEqualStrings("active", rd.cols[0]);
    try std.testing.expectEqualStrings("name", rd.cols[1]);
    try std.testing.expectEqualStrings("cid", rd.cols[2]);
}

test "a WHERE over a join of SQL reads: the FROM table's conjunct reaches its query, the joined CTE's its side, whose read still asks for every column the CTE names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../lang/sql_parser.zig");
    const env_mod = @import("env.zig");

    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a,
        \\CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'h', database = 'd');
        \\WITH d AS (SELECT k, nm, extra FROM pg.public.dim)
        \\SELECT o.id, d.nm FROM pg.public.orders o JOIN d ON o.k = d.k WHERE o.status = 1 AND d.nm = 'x';
    , &pd);
    var params = std.StringHashMap(Value).init(a);
    var bindings = std.StringHashMap(ast.Pipeline).init(a);
    var connections = std.StringHashMap(ast.Connection).init(a);
    var stages: []const ast.Stage = &.{};
    for (prog.stmts) |st| switch (st) {
        .connection => |c| try connections.put(c.name, c),
        .binding => |b| try bindings.put(b.name, b.pipeline),
        .output => |o| stages = o.stages,
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
    var env = Env{ .arena = a, .gpa = a, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns };

    const hoisted = (try pushdown.hoistFilters(a, a, stages, &bindings, null)).?;
    const out = try descendLeadingWhere(&env, hoisted);
    try std.testing.expect(std.mem.indexOf(u8, out[0].node.read.where, "\"status\" = 1") != null);
    try std.testing.expect(out[1].node == .filter and out[2].node == .join);
    const j = out[2].node.join;
    try std.testing.expect(j.right_filter != null);

    const needs = (try rightNeeds(&env, j, out[3 .. out.len - 1])).?;
    const side = try prepareSide(&env, try j.rightStages(a, bindings.get("d").?.stages), try needsTail(a, needs));
    const cols = side[0].node.read.cols;
    try std.testing.expectEqual(@as(usize, 3), cols.len);
    for (cols) |c| try std.testing.expect(std.mem.eql(u8, c, "k") or std.mem.eql(u8, c, "nm") or std.mem.eql(u8, c, "extra"));
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
    var env = Env{ .arena = a, .gpa = a, .params = &params, .bindings = &bindings, .connections = &connections, .sources = &sources, .diag = &diag, .log = &log, .params_expr = &params_expr, .errctx = &errctx, .rows_read = &rows, .json_params = &json, .fns = &fns };

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

fn cteAndReader(a: std.mem.Allocator, src: []const u8) ![]const ast.Stage {
    const parser = @import("../lang/sql_parser.zig");
    var pd: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = try parser.parseSource(a, src, &pd);
    var cte: []const ast.Stage = &.{};
    for (prog.stmts) |st| switch (st) {
        .binding => |b| if (std.mem.eql(u8, b.name, "c")) {
            cte = b.pipeline.stages;
        },
        .output => |o| return std.mem.concat(a, ast.Stage, &.{ cte, o.stages[1..] }),
        else => {},
    };
    return error.TestUnexpectedResult;
}

fn selectNames(a: std.mem.Allocator, st: ast.Stage) ![]const u8 {
    var out = std.array_list.Managed(u8).init(a);
    for (st.node.select) |it| {
        if (out.items.len > 0) try out.append(',');
        try out.appendSlice(switch (it) {
            .field => |q| q.last(),
            .computed => |c| c.name,
            else => "*",
        });
    }
    return out.toOwnedSlice();
}

test "pruneSelects: a CTE's columns its reader leaves unused are dropped, through a window" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const whole = try cteAndReader(a,
        \\WITH c AS (SELECT f.ev, p.cat, f.amt, f.region,
        \\  ROW_NUMBER() OVER (PARTITION BY p.cat ORDER BY f.amt DESC) AS rn
        \\  FROM 'f.parquet' f JOIN 'p.parquet' p ON f.pid = p.pid WHERE f.ev = 'x')
        \\SELECT cat, SUM(amt) AS total FROM c WHERE rn <= 10 GROUP BY cat;
    );
    const out = try pruneSelects(a, whole);
    var sels = std.array_list.Managed([]const u8).init(a);
    for (out) |st| if (st.node == .select) try sels.append(try selectNames(a, st));
    try std.testing.expectEqual(@as(usize, 2), sels.items.len);
    try std.testing.expect(std.mem.indexOf(u8, sels.items[0], "region") == null and std.mem.indexOf(u8, sels.items[0], "ev") == null);
    try std.testing.expect(std.mem.indexOf(u8, sels.items[0], "amt") != null);
    try std.testing.expectEqualStrings("cat,amt,rn", sels.items[1]);
    for (out) |st| if (st.node == .join) for (st.node.join.own_cols) |q| {
        try std.testing.expect(!std.mem.eql(u8, q.last(), "region"));
    };
}

test "pruneSelects: a select stays whole before a plain DISTINCT, a `*`, a search or the output, or keeps what may fail" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_][]const u8{
        "WITH c AS (SELECT DISTINCT a, b FROM 't.csv') SELECT a FROM c;",
        "WITH c AS (SELECT a, b FROM 't.csv') SELECT * FROM c;",
        "WITH c AS (SELECT a, b FROM 't.csv') SELECT a FROM c WHERE search(*, 'x');",
        "WITH c AS (SELECT a, b FROM 't.csv') SELECT a, b FROM c;",
        "WITH c AS (SELECT a, CAST(b AS INT) AS bi FROM 't.csv') SELECT a FROM c;",
    }) |src| {
        const whole = try cteAndReader(a, src);
        const out = try pruneSelects(a, whole);
        for (whole, out) |w, o| if (w.node == .select) try std.testing.expectEqual(w.node.select.len, o.node.select.len);
    }
    const counted = try pruneSelects(a, try cteAndReader(a, "WITH c AS (SELECT a, b FROM 't.csv') SELECT COUNT(*) AS n FROM c;"));
    try std.testing.expectEqual(@as(usize, 1), counted[1].node.select.len);
}

test "pruneFor: a joined CTE keeps only the columns the join and what follows may take" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const side = try cteAndReader(a, "WITH c AS (SELECT k, w AS ww, s, category FROM 'r.csv' WHERE w > 1) SELECT * FROM c;");
    const out = try pruneFor(a, side[0..3], try needsTail(a, &.{ "k", "category", "id" }));
    try std.testing.expectEqualStrings("k,category", try selectNames(a, out[2]));
    try std.testing.expect(out[1].node == .filter);
}

test {
    _ = @import("plan/estimate.zig");
    _ = @import("plan/joinorder.zig");
    _ = @import("plan/discover.zig");
    _ = @import("plan/union.zig");
}
