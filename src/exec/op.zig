//! Streaming pull operators. Each `next(arena)` returns the next batch or null.
//! Operators form a closed set (a tagged union), dispatched once per batch — the
//! cold boundary; the per-row work happens in the columnar kernels in `eval.zig`.
//! The scan operator reads through the abstract `Source` driver seam.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const batchmod = @import("batch.zig");
const eval = @import("eval.zig");
const valuemod = @import("value.zig");
const driver = @import("../connect/driver.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;

pub const Op = union(enum) {
    scan: *Scan,
    filter: *Filter,
    project: *Project,
    limit: *Limit,
    distinct: *Distinct,
    sort: *Sort,
    aggregate: *Aggregate,
    join: *Join,
    explode: *Explode,

    pub fn next(self: Op, arena: std.mem.Allocator) anyerror!?Batch {
        return switch (self) {
            .scan => |s| s.next(arena),
            .filter => |f| f.next(arena),
            .project => |p| p.next(arena),
            .limit => |l| l.next(arena),
            .distinct => |d| d.next(arena),
            .sort => |s| s.next(arena),
            .aggregate => |a| a.next(arena),
            .join => |j| j.next(arena),
            .explode => |e| e.next(arena),
        };
    }
};

/// Streaming 1→N: split a delimited string column, emitting one row per element
/// (other columns repeated). Null/missing cells produce zero rows.
pub const Explode = struct {
    child: Op,
    field_idx: usize,
    delim: []const u8,
    out_schema: *const types.Schema,

    pub fn next(self: *Explode, arena: std.mem.Allocator) anyerror!?Batch {
        while (try self.child.next(arena)) |b| {
            const out = try self.explodeBatch(arena, b);
            if (out.len > 0) return out;
        }
        return null;
    }

    fn explodeBatch(self: *Explode, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        const ncols = b.columns.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders, self.out_schema.fields) |*bd, f| bd.* = column.Builder.init(arena, f.ty);

        var n: usize = 0;
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const fv = b.columns[self.field_idx].getValue(r);
            const s = switch (fv) {
                .string => |x| x,
                .bytes => |x| x,
                else => continue, // null/non-string -> no rows
            };
            var it = std.mem.splitSequence(u8, s, self.delim);
            while (it.next()) |elem| {
                for (b.columns, 0..) |*c, ci| {
                    if (ci == self.field_idx) {
                        try builders[ci].append(.{ .string = elem });
                    } else {
                        try builders[ci].append(c.getValue(r));
                    }
                }
                n += 1;
            }
        }

        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*bd, i| cols[i] = try bd.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

pub const Scan = struct {
    src: driver.Source,

    pub fn next(self: *Scan, arena: std.mem.Allocator) anyerror!?Batch {
        return self.src.next(arena);
    }
};

pub const Filter = struct {
    child: Op,
    pred: *const ast.Expr,

    pub fn next(self: *Filter, arena: std.mem.Allocator) anyerror!?Batch {
        while (try self.child.next(arena)) |b| {
            const out = try applyFilter(arena, b, self.pred);
            if (out.len > 0) return out;
        }
        return null;
    }
};

pub const Project = struct {
    child: Op,
    cols: []const Col,
    out_schema: *const types.Schema,

    /// A projected output column: either a passthrough of an input column index,
    /// or a computed expression with its resolved output type.
    pub const Col = struct {
        source: union(enum) { passthrough: usize, expr: *const ast.Expr },
        ty: types.Type,
    };

    pub fn next(self: *Project, arena: std.mem.Allocator) anyerror!?Batch {
        const b = (try self.child.next(arena)) orelse return null;
        const outcols = try arena.alloc(column.Column, self.cols.len);
        for (self.cols, 0..) |c, i| {
            outcols[i] = switch (c.source) {
                .passthrough => |idx| b.columns[idx],
                .expr => |e| try eval.evalColumn(arena, e, b, c.ty),
            };
        }
        return Batch{ .schema = self.out_schema, .columns = outcols, .len = b.len };
    }
};

pub const Limit = struct {
    child: Op,
    remaining: u64,
    to_skip: u64,

    pub fn next(self: *Limit, arena: std.mem.Allocator) anyerror!?Batch {
        while (true) {
            if (self.remaining == 0) return null;
            const b = (try self.child.next(arena)) orelse return null;

            var start: usize = 0;
            if (self.to_skip > 0) {
                if (self.to_skip >= b.len) {
                    self.to_skip -= b.len;
                    continue;
                }
                start = @intCast(self.to_skip);
                self.to_skip = 0;
            }
            var take = b.len - start;
            if (take > self.remaining) take = @intCast(self.remaining);
            self.remaining -= take;
            if (start == 0 and take == b.len) return b;
            return try sliceBatch(arena, b, start, take);
        }
    }
};

// --- helpers ---

fn applyFilter(arena: std.mem.Allocator, b: Batch, pred: *const ast.Expr) anyerror!Batch {
    const keep = try arena.alloc(bool, b.len);
    var kept: usize = 0;
    var i: usize = 0;
    while (i < b.len) : (i += 1) {
        const v = try eval.evalRow(arena, pred, b, i);
        keep[i] = (v == .bool and v.bool); // 3VL: null and false both drop
        if (keep[i]) kept += 1;
    }
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, 0..) |*col, ci| {
        var bld = column.Builder.init(arena, col.ty);
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            if (keep[r]) try bld.append(col.getValue(r));
        }
        outcols[ci] = try bld.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
}

fn sliceBatch(arena: std.mem.Allocator, b: Batch, start: usize, take: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, 0..) |*col, ci| {
        var bld = column.Builder.init(arena, col.ty);
        var r: usize = start;
        while (r < start + take) : (r += 1) try bld.append(col.getValue(r));
        outcols[ci] = try bld.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = take };
}

// ===========================================================================
// Breakers — each drains its child fully (inside the first `next`), materializes
// the whole input as one batch, computes its result, then is done.
// ===========================================================================

/// Drain `child` and concatenate every row into one in-memory batch (or null if
/// the input is empty). Memory is O(dataset) — the defining cost of a breaker.
fn materializeAll(arena: std.mem.Allocator, child: Op, schema: *const types.Schema) anyerror!?Batch {
    const ncols = schema.fields.len;
    const builders = try arena.alloc(column.Builder, ncols);
    for (builders, schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);

    var total: usize = 0;
    while (try child.next(arena)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (b.columns, 0..) |*col, ci| try builders[ci].append(col.getValue(r));
        }
        total += b.len;
    }
    if (total == 0) return null;

    const cols = try arena.alloc(column.Column, ncols);
    for (builders, 0..) |*bd, i| cols[i] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// Build a batch selecting the rows where `keep[r]` is true.
fn gather(arena: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, 0..) |*col, ci| {
        var bld = column.Builder.init(arena, col.ty);
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            if (keep[r]) try bld.append(col.getValue(r));
        }
        outcols[ci] = try bld.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
}

/// Serialize the key columns of one row into a comparable byte string (with a
/// null marker and field separator), for hash-grouping/dedup.
fn rowKey(arena: std.mem.Allocator, b: Batch, idxs: []const usize, row: usize) anyerror![]const u8 {
    var buf = std.ArrayList(u8).init(arena);
    for (idxs) |ci| {
        const v = b.columns[ci].getValue(row);
        if (v.isNull()) {
            try buf.appendSlice(&.{ 0, 'N' });
        } else {
            try buf.append(1);
            try buf.appendSlice(try eval.valueToString(arena, v));
        }
        try buf.append(0);
    }
    return buf.toOwnedSlice();
}

pub const Distinct = struct {
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize, // null = all columns
    done: bool = false,

    pub fn next(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;

        var key_idx: []const usize = undefined;
        if (self.keys) |k| {
            key_idx = k;
        } else {
            const idxs = try arena.alloc(usize, all.columns.len);
            for (idxs, 0..) |*x, i| x.* = i;
            key_idx = idxs;
        }

        var seen = std.StringHashMap(void).init(arena);
        const keep = try arena.alloc(bool, all.len);
        var kept: usize = 0;
        var r: usize = 0;
        while (r < all.len) : (r += 1) {
            const k = try rowKey(arena, all, key_idx, r);
            if (seen.contains(k)) {
                keep[r] = false;
            } else {
                try seen.put(k, {});
                keep[r] = true;
                kept += 1;
            }
        }
        return try gather(arena, all, keep, kept);
    }
};

pub const Sort = struct {
    child: Op,
    in_schema: *const types.Schema,
    keys: []const Key,
    done: bool = false,

    pub const Key = struct { idx: usize, desc: bool };

    pub fn next(self: *Sort, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;

        const idx = try arena.alloc(usize, all.len);
        for (idx, 0..) |*x, i| x.* = i;
        std.mem.sort(usize, idx, SortCtx{ .b = all, .keys = self.keys }, SortCtx.lessThan);

        const outcols = try arena.alloc(column.Column, all.columns.len);
        for (all.columns, 0..) |*col, ci| {
            var bld = column.Builder.init(arena, col.ty);
            for (idx) |r| try bld.append(col.getValue(r));
            outcols[ci] = try bld.finish();
        }
        return Batch{ .schema = all.schema, .columns = outcols, .len = all.len };
    }
};

const SortCtx = struct {
    b: Batch,
    keys: []const Sort.Key,

    fn lessThan(self: SortCtx, a: usize, c: usize) bool {
        for (self.keys) |k| {
            const va = self.b.columns[k.idx].getValue(a);
            const vc = self.b.columns[k.idx].getValue(c);
            const an = va.isNull();
            const cn = vc.isNull();
            if (an or cn) {
                if (an and cn) continue; // equal on this key
                return !an; // nulls last: non-null sorts before null
            }
            const ord = eval.compareValues(va, vc) orelse continue;
            if (ord == .eq) continue;
            const less = (ord == .lt);
            return if (k.desc) !less else less;
        }
        return false;
    }
};

pub const Aggregate = struct {
    child: Op,
    in_schema: *const types.Schema,
    by: []const usize,
    aggs: []const Agg,
    out_schema: *const types.Schema,
    done: bool = false,

    pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type };

    const Acc = struct {
        n: i64 = 0,
        sum_i: i64 = 0,
        sum_f: f64 = 0,
        ext: Value = .null,
        has_ext: bool = false,
    };

    const Group = struct { key_vals: []Value, accs: []Acc };

    pub fn next(self: *Aggregate, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse {
            if (self.by.len == 0) return try self.emit(arena, &.{}); // global aggregate over empty input
            return null;
        };

        var groups = std.ArrayList(Group).init(arena);
        var map = std.StringHashMap(usize).init(arena);
        var r: usize = 0;
        while (r < all.len) : (r += 1) {
            const key = try rowKey(arena, all, self.by, r);
            const gop = try map.getOrPut(key);
            if (!gop.found_existing) {
                gop.value_ptr.* = groups.items.len;
                const kv = try arena.alloc(Value, self.by.len);
                for (self.by, 0..) |ci, j| kv[j] = all.columns[ci].getValue(r);
                const accs = try arena.alloc(Acc, self.aggs.len);
                for (accs) |*a| a.* = .{};
                try groups.append(.{ .key_vals = kv, .accs = accs });
            }
            const g = &groups.items[gop.value_ptr.*];
            for (self.aggs, 0..) |agg, j| {
                var v: Value = .null;
                if (agg.arg) |e| v = try eval.evalRow(arena, e, all, r);
                updateAcc(&g.accs[j], agg, v, agg.arg != null);
            }
        }
        return try self.emit(arena, groups.items);
    }

    fn emit(self: *Aggregate, arena: std.mem.Allocator, groups: []const Group) anyerror!Batch {
        const nfields = self.out_schema.fields.len;
        const builders = try arena.alloc(column.Builder, nfields);
        for (builders, self.out_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);

        if (groups.len == 0 and self.by.len == 0) {
            // empty global aggregate: one row of finalized fresh accumulators
            for (self.aggs, 0..) |agg, j| try builders[j].append(finalizeAcc(.{}, agg));
        } else {
            for (groups) |g| {
                var col: usize = 0;
                for (g.key_vals) |kv| {
                    try builders[col].append(kv);
                    col += 1;
                }
                for (self.aggs, 0..) |agg, j| {
                    try builders[col].append(finalizeAcc(g.accs[j], agg));
                    col += 1;
                }
            }
        }

        const cols = try arena.alloc(column.Column, nfields);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        const n: usize = if (groups.len == 0 and self.by.len == 0) 1 else groups.len;
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }

    fn updateAcc(acc: *Acc, agg: Agg, v: Value, has_arg: bool) void {
        switch (agg.func) {
            .count => {
                if (!has_arg or !v.isNull()) acc.n += 1;
            },
            .sum => if (!v.isNull()) {
                if (agg.ty.kind == .float) acc.sum_f += eval.toF64(v) else acc.sum_i += v.int;
                acc.n += 1;
            },
            .avg => if (!v.isNull()) {
                acc.sum_f += eval.toF64(v);
                acc.n += 1;
            },
            .min => if (!v.isNull()) {
                if (!acc.has_ext or lessV(v, acc.ext)) {
                    acc.ext = v;
                    acc.has_ext = true;
                }
            },
            .max => if (!v.isNull()) {
                if (!acc.has_ext or lessV(acc.ext, v)) {
                    acc.ext = v;
                    acc.has_ext = true;
                }
            },
        }
    }

    fn finalizeAcc(acc: Acc, agg: Agg) Value {
        return switch (agg.func) {
            .count => .{ .int = acc.n },
            .sum => if (acc.n == 0) .null else if (agg.ty.kind == .float) Value{ .float = acc.sum_f } else Value{ .int = acc.sum_i },
            .avg => if (acc.n == 0) .null else Value{ .float = acc.sum_f / @as(f64, @floatFromInt(acc.n)) },
            .min, .max => if (acc.has_ext) acc.ext else .null,
        };
    }
};

fn lessV(a: Value, b: Value) bool {
    return (eval.compareValues(a, b) orelse .eq) == .lt;
}

/// Like materializeAll but always returns a (possibly empty) batch with the
/// schema's columns present — so a join can emit right-side nulls even when the
/// build side is empty.
fn materializeFull(arena: std.mem.Allocator, child: Op, schema: *const types.Schema) anyerror!Batch {
    const ncols = schema.fields.len;
    const builders = try arena.alloc(column.Builder, ncols);
    for (builders, schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
    var total: usize = 0;
    while (try child.next(arena)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (b.columns, 0..) |*col, ci| try builders[ci].append(col.getValue(r));
        }
        total += b.len;
    }
    const cols = try arena.alloc(column.Column, ncols);
    for (builders, 0..) |*bd, i| cols[i] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// A join key string for one value, or null if the value is null (null keys
/// never match, per SQL).
fn valueKey(arena: std.mem.Allocator, v: Value) anyerror!?[]const u8 {
    if (v.isNull()) return null;
    return try arena.dupe(u8, try eval.valueToString(arena, v));
}

/// Hash equi-join. The build (right) side is materialized into a hash index on
/// the first `next`; the probe (left) side then streams through. Supports
/// inner / left / semi / anti.
pub const Join = struct {
    probe: Op,
    build: Op,
    left_key: usize,
    right_key: usize,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,

    built: bool = false,
    build_batch: Batch = undefined,
    index: std.StringHashMap(std.ArrayList(usize)) = undefined,

    const empty_match: []const usize = &.{};

    pub fn next(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        if (!self.built) {
            self.built = true;
            self.build_batch = try materializeFull(arena, self.build, self.right_schema);
            self.index = std.StringHashMap(std.ArrayList(usize)).init(arena);
            var r: usize = 0;
            while (r < self.build_batch.len) : (r += 1) {
                const k = (try valueKey(arena, self.build_batch.columns[self.right_key].getValue(r))) orelse continue;
                const gop = try self.index.getOrPut(k);
                if (!gop.found_existing) gop.value_ptr.* = std.ArrayList(usize).init(arena);
                try gop.value_ptr.append(r);
            }
        }
        while (try self.probe.next(arena)) |lb| {
            const out = try self.joinBatch(arena, lb);
            if (out.len > 0) return out;
        }
        return null;
    }

    fn joinBatch(self: *Join, arena: std.mem.Allocator, lb: Batch) anyerror!Batch {
        const emit_right = (self.kind == .inner or self.kind == .left);
        const nout = self.out_schema.fields.len;
        const builders = try arena.alloc(column.Builder, nout);
        for (builders, self.out_schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);

        var n: usize = 0;
        var r: usize = 0;
        while (r < lb.len) : (r += 1) {
            const key = try valueKey(arena, lb.columns[self.left_key].getValue(r));
            const matches: []const usize = if (key) |k|
                (if (self.index.get(k)) |list| list.items else empty_match)
            else
                empty_match;

            switch (self.kind) {
                .inner => for (matches) |bri| {
                    try self.emitRow(builders, lb, r, bri, emit_right, false);
                    n += 1;
                },
                .left => if (matches.len == 0) {
                    try self.emitRow(builders, lb, r, 0, emit_right, true);
                    n += 1;
                } else for (matches) |bri| {
                    try self.emitRow(builders, lb, r, bri, emit_right, false);
                    n += 1;
                },
                .semi => if (matches.len > 0) {
                    try self.emitRow(builders, lb, r, 0, false, false);
                    n += 1;
                },
                .anti => if (matches.len == 0) {
                    try self.emitRow(builders, lb, r, 0, false, false);
                    n += 1;
                },
                else => unreachable,
            }
        }

        const cols = try arena.alloc(column.Column, nout);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }

    fn emitRow(self: *Join, builders: []column.Builder, lb: Batch, lr: usize, bri: usize, emit_right: bool, right_null: bool) anyerror!void {
        var col: usize = 0;
        for (lb.columns) |*c| {
            try builders[col].append(c.getValue(lr));
            col += 1;
        }
        if (emit_right) {
            for (self.build_batch.columns) |*c| {
                try builders[col].append(if (right_null) .null else c.getValue(bri));
                col += 1;
            }
        }
    }
};
