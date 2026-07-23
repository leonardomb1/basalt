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
const simd = @import("simd.zig");
const valuemod = @import("value.zig");
const keyhash = @import("keyhash.zig");
const driver = @import("../connect/driver.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;

/// Captures context for a runtime expression error (which stage/column), turning
/// a bare `CastFailed` into something actionable. Inline buffer so it outlives the
/// per-batch arena; mutex + first-wins so concurrent lanes report deterministically.
pub const ErrCtx = struct {
    buf: [256]u8 = undefined,
    msg: []const u8 = "",
    mutex: std.Thread.Mutex = .{},

    pub fn set(self: *ErrCtx, comptime fmt: []const u8, args: anytype) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.msg.len > 0) return; // first error wins
        self.msg = std.fmt.bufPrint(&self.buf, fmt, args) catch return;
    }
};

/// Human label for an evaluation error.
pub fn errLabel(e: anyerror) []const u8 {
    return switch (e) {
        error.CastFailed => "cast failed",
        error.DivByZero => "division by zero",
        error.TypeMismatch => "type mismatch",
        else => @errorName(e),
    };
}

pub const Op = union(enum) {
    scan: *Scan,
    filter: *Filter,
    project: *Project,
    limit: *Limit,
    distinct: *Distinct,
    sort: *Sort,
    aggregate: *Aggregate,
    top_n: *TopN,
    join: *Join,
    explode: *Explode,
    union_: *Union,

    pub fn next(self: Op, arena: std.mem.Allocator) anyerror!?Batch {
        return switch (self) {
            .scan => |s| s.next(arena),
            .filter => |f| f.next(arena),
            .project => |p| p.next(arena),
            .limit => |l| l.next(arena),
            .distinct => |d| d.next(arena),
            .sort => |s| s.next(arena),
            .aggregate => |a| a.next(arena),
            .top_n => |t| t.next(arena),
            .join => |j| j.next(arena),
            .explode => |e| e.next(arena),
            .union_ => |u| u.next(arena),
        };
    }
};

/// Concatenate (UNION ALL) several child ops: drain child 0 fully, then child 1,
/// … Each child is expected to already emit the unified output schema (e.g. a
/// reconcile-projection over its source), so this op just forwards their batches.
pub const Union = struct {
    children: []const Op,
    idx: usize = 0,

    pub fn next(self: *Union, arena: std.mem.Allocator) anyerror!?Batch {
        while (self.idx < self.children.len) {
            if (try self.children[self.idx].next(arena)) |b| return b;
            self.idx += 1;
        }
        return null;
    }
};

/// A stateless per-batch transform (it does NOT pull from a child) — the building
/// block of a parallelizable "map" pipeline. Only filter/project/explode qualify;
/// breakers and limit are order/state sensitive and stay on the serial driver.
pub const Stage = union(enum) {
    filter: *Filter,
    project: *Project,
    explode: *Explode,

    pub fn apply(self: Stage, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return switch (self) {
            .filter => |f| f.transform(arena, b),
            .project => |p| p.transform(arena, b),
            .explode => |e| e.transform(arena, b),
        };
    }
};

pub const Linear = struct { src: driver.Source, stages: []const Stage };

/// If `top` is a map-only pipeline (scan → filter/project/explode chain, no
/// breakers or limit), decompose it into a source + ordered stage list the
/// parallel driver can fan out across threads. Returns null otherwise.
pub fn linearize(arena: std.mem.Allocator, top: Op) !?Linear {
    var rev = std.array_list.Managed(Stage).init(arena);
    var cur = top;
    while (true) {
        switch (cur) {
            .scan => |s| {
                const stages = try arena.alloc(Stage, rev.items.len);
                // rev is in sink→source order; reverse into source→sink order.
                for (rev.items, 0..) |st, i| stages[rev.items.len - 1 - i] = st;
                return Linear{ .src = s.src, .stages = stages };
            },
            .filter => |f| {
                try rev.append(.{ .filter = f });
                cur = f.child;
            },
            .project => |p| {
                try rev.append(.{ .project = p });
                cur = p.child;
            },
            .explode => |e| {
                try rev.append(.{ .explode = e });
                cur = e.child;
            },
            else => return null,
        }
    }
}

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

    /// Stateless transform of one input batch (for the parallel driver).
    pub fn transform(self: *Explode, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return self.explodeBatch(arena, b);
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
    err: ?*ErrCtx = null,

    pub fn next(self: *Filter, arena: std.mem.Allocator) anyerror!?Batch {
        while (try self.child.next(arena)) |b| {
            const out = try self.transform(arena, b);
            if (out.len > 0) return out;
        }
        return null;
    }

    /// Stateless transform of one input batch (for the parallel driver).
    pub fn transform(self: *Filter, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return applyFilter(arena, b, self.pred) catch |e| {
            if (self.err) |ec| ec.set("{s}: in filter predicate", .{errLabel(e)});
            return e;
        };
    }
};

pub const Project = struct {
    child: Op,
    cols: []const Col,
    out_schema: *const types.Schema,
    err: ?*ErrCtx = null,

    /// A projected output column: either a passthrough of an input column index,
    /// or a computed expression with its resolved output type.
    pub const Col = struct {
        source: union(enum) { passthrough: usize, expr: *const ast.Expr },
        ty: types.Type,
    };

    pub fn next(self: *Project, arena: std.mem.Allocator) anyerror!?Batch {
        const b = (try self.child.next(arena)) orelse return null;
        return try self.transform(arena, b);
    }

    /// Stateless transform of one input batch (for the parallel driver).
    pub fn transform(self: *Project, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        const outcols = try arena.alloc(column.Column, self.cols.len);
        for (self.cols, 0..) |c, i| {
            outcols[i] = switch (c.source) {
                .passthrough => |idx| b.columns[idx],
                .expr => |e| eval.evalColumn(arena, e, b, c.ty) catch |err| {
                    if (self.err) |ec| ec.set("{s}: computing column `{s}` in select", .{ errLabel(err), self.out_schema.fields[i].name });
                    return err;
                },
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
    // The predicate evaluates to a bool column through the vectorized kernels
    // (falling back to row-at-a-time internally for unsupported nodes); a null
    // result drops the row (3VL: only a known-true keeps it).
    const mask = try eval.evalColumn(arena, pred, b, types.Type.init(.bool));
    // Reuse the mask's own bool storage as the keep array (it's a fresh arena
    // column we own), folding nulls in (3VL: a null result drops the row). Avoids
    // a second n-sized buffer; the no-null case skips the per-row validity AND.
    const keep = mask.data.b;
    var kept: usize = 0;
    if (mask.validity.allSet(b.len)) {
        for (keep) |k| {
            if (k) kept += 1;
        }
    } else {
        for (keep, 0..) |*k, i| {
            if (!mask.validity.get(i)) k.* = false;
            if (k.*) kept += 1;
        }
    }
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, 0..) |*col, ci| outcols[ci] = try column.gather(arena, col.*, keep, kept);
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
/// All chunks live in this single `next()`'s arena (no reset happens mid-call),
/// so the typed buffers are concatenated directly: no per-row `Value` boxing
/// and no re-duping of string bytes.
fn materializeAll(arena: std.mem.Allocator, child: Op, schema: *const types.Schema) anyerror!?Batch {
    var chunks = std.array_list.Managed(Batch).init(arena);
    var total: usize = 0;
    while (try child.next(arena)) |b| {
        if (b.len == 0) continue;
        try chunks.append(b);
        total += b.len;
    }
    if (total == 0) return null;

    const ncols = schema.fields.len;
    const cols = try arena.alloc(column.Column, ncols);
    const per = try arena.alloc(column.Column, chunks.items.len); // one column's chunks
    for (cols, 0..) |*out, ci| {
        for (chunks.items, 0..) |b, k| per[k] = b.columns[ci];
        out.* = try column.concat(arena, per, total);
    }
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// Streaming dedup: batches flow through one at a time, filtered against a
/// seen-set of key strings — O(distinct keys) memory, not O(dataset). The
/// seen-set (and its key copies) live in `state` (the plan arena), because the
/// per-pull batch arena is reset between pulls.
pub const Distinct = struct {
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize, // null = all columns
    state: std.mem.Allocator,
    gpa: std.mem.Allocator, // backs the per-batch scratch arena
    seen: ?Seen = null,

    const Seen = std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);

    pub fn next(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.seen == null) self.seen = Seen.init(self.state);
        const seen = &self.seen.?;

        // The seen-set keys live in `state`, so each child batch (and its transient
        // row keys) can be freed once scanned. Pull into a scratch arena reset per
        // batch — otherwise a long run of all-duplicate batches (common once the
        // distinct set is saturated) accumulates the whole remaining input here in
        // one `next` call.
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        while (try self.child.next(pull)) |b| {
            var key_idx: []const usize = undefined;
            if (self.keys) |k| {
                key_idx = k;
            } else {
                const idxs = try pull.alloc(usize, b.columns.len);
                for (idxs, 0..) |*x, i| x.* = i;
                key_idx = idxs;
            }

            const keep = try pull.alloc(bool, b.len);
            const probe = try pull.alloc(Value, key_idx.len); // reused per row (scratch)
            var kept: usize = 0;
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                for (key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r); // aliases batch; lookup only
                const gop = try seen.getOrPut(probe);
                if (gop.found_existing) {
                    keep[r] = false;
                } else {
                    // First sight: store a copy of the key (the probe aliases batch memory).
                    const kv = try self.state.alloc(Value, key_idx.len);
                    for (key_idx, 0..) |ci, j| kv[j] = try dupeValue(self.state, b.columns[ci].getValue(r));
                    gop.key_ptr.* = kv;
                    keep[r] = true;
                    kept += 1;
                }
            }
            if (kept == 0) {
                _ = scratch.reset(.retain_capacity);
                continue;
            }
            // The returned batch must outlive `scratch`, so deep-copy the kept rows
            // into the caller's arena (column.gather only copies string *pointers*,
            // which would dangle once scratch is freed).
            return try gatherDeep(arena, b, keep, kept);
        }
        return null;
    }
};

/// Deep-copy the `keep`-marked rows of `b` into `arena` via column builders, which
/// dupe string/bytes payloads (unlike `column.gather`, which aliases them). Used when the
/// source batch lives in a scratch arena that is about to be freed.
fn gatherDeep(arena: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, b.schema.fields, 0..) |*col, f, ci| {
        var bd = column.Builder.init(arena, f.ty);
        var r: usize = 0;
        while (r < b.len) : (r += 1) if (keep[r]) try bd.append(col.getValue(r));
        outcols[ci] = try bd.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
}

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
        for (all.columns, 0..) |*col, ci| outcols[ci] = try column.permute(arena, col.*, idx);
        return Batch{ .schema = all.schema, .columns = outcols, .len = all.len };
    }
};

const SortCtx = struct {
    b: Batch,
    keys: []const Sort.Key,

    fn lessThan(self: SortCtx, a: usize, c: usize) bool {
        for (self.keys) |k| {
            const o = keyOrder(self.b.columns[k.idx].getValue(a), self.b.columns[k.idx].getValue(c), k.desc);
            if (o != .eq) return o == .lt;
        }
        return false;
    }
};

/// Effective order of two sort-key values: `.lt` means `va` sorts before `vb`.
/// Nulls always sort last (independent of `desc`); `desc` flips non-null order.
/// Shared by `SortCtx.lessThan` and Top-N's heap and final sort.
fn keyOrder(va: Value, vb: Value, desc: bool) std.math.Order {
    const an = va.isNull();
    const bn = vb.isNull();
    if (an or bn) {
        if (an and bn) return .eq;
        return if (an) .gt else .lt; // null is "greater" → sorts last
    }
    const ord = eval.compareValues(va, vb) orelse return .eq;
    if (ord == .eq) return .eq;
    return if (desc) (if (ord == .lt) std.math.Order.gt else std.math.Order.lt) else ord;
}

/// `sort … | limit N [offset M]` fused into a bounded Top-(M+N) heap: O(n log K)
/// time and O(K) memory instead of materializing + sorting the whole input. The K
/// kept rows are deep-copied into `gpa` (strings freed on eviction), so memory is
/// bounded regardless of input size; only the final K rows are emitted into the
/// caller arena. A plain `sort` (no following `limit`) still uses the full Sort op.
pub const TopN = struct {
    child: Op,
    in_schema: *const types.Schema,
    keys: []const Sort.Key,
    count: u64,
    offset: u64,
    state: std.mem.Allocator, // output batch
    gpa: std.mem.Allocator, // heap row storage (per-entry alloc/free)
    done: bool = false,

    const Entry = []Value; // one materialized row: a value per input column
    const Heap = std.PriorityQueue(Entry, []const Sort.Key, entryWorstFirst);

    pub fn next(self: *TopN, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        if (self.count == 0) return null;
        const cap = self.offset + self.count; // keep this many best rows

        // Priority queue keyed worst-first (root = the worst-ranked kept row),
        // entries owned by `gpa` and freed on eviction → O(cap) memory. Child
        // pulled through a scratch arena reset per batch (the batches themselves
        // are never retained).
        var heap = Heap.init(self.gpa, self.keys);
        defer {
            for (heap.items) |e| self.freeEntry(e);
            heap.deinit();
        }
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        while (try self.child.next(pull)) |b| {
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                if (heap.count() < cap) {
                    try heap.add(try self.cloneRow(b, r));
                } else if (self.rowLess(b, r, heap.items[0])) {
                    // r ranks before the current worst kept → evict the root
                    self.freeEntry(heap.remove());
                    try heap.add(try self.cloneRow(b, r));
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        if (heap.items.len == 0) return null;

        // Order the kept rows, then drop `offset` and take `count`.
        std.mem.sort(Entry, heap.items, self.keys, entryLessCtx);
        const start = @min(self.offset, heap.items.len);
        const end = @min(self.offset + self.count, heap.items.len);
        if (start >= end) return null;
        return try self.emit(arena, heap.items[start..end]);
    }

    fn cloneRow(self: *TopN, b: Batch, r: usize) !Entry {
        const vals = try self.gpa.alloc(Value, b.columns.len);
        for (b.columns, vals) |*col, *out| out.* = try dupeValueGpa(self.gpa, col.getValue(r));
        return vals;
    }

    fn freeEntry(self: *TopN, e: Entry) void {
        for (e) |v| switch (v) {
            .string, .bytes => |s| self.gpa.free(s),
            else => {},
        };
        self.gpa.free(e);
    }

    /// Does row `r` of `b` rank before stored entry `e` (i.e. belongs above it)?
    fn rowLess(self: *TopN, b: Batch, r: usize, e: Entry) bool {
        for (self.keys) |k| {
            const o = keyOrder(b.columns[k.idx].getValue(r), e[k.idx], k.desc);
            if (o != .eq) return o == .lt;
        }
        return false;
    }

    fn emit(self: *TopN, arena: std.mem.Allocator, entries: []const Entry) !Batch {
        const cols = try arena.alloc(column.Column, self.in_schema.fields.len);
        for (self.in_schema.fields, 0..) |f, ci| {
            var bd = column.Builder.init(arena, f.ty);
            for (entries) |e| try bd.append(e[ci]);
            cols[ci] = try bd.finish();
        }
        return Batch{ .schema = self.in_schema, .columns = cols, .len = entries.len };
    }
};

fn entryLess(a: TopN.Entry, b: TopN.Entry, keys: []const Sort.Key) bool {
    for (keys) |k| {
        const o = keyOrder(a[k.idx], b[k.idx], k.desc);
        if (o != .eq) return o == .lt;
    }
    return false;
}

fn entryLessCtx(keys: []const Sort.Key, a: TopN.Entry, b: TopN.Entry) bool {
    return entryLess(a, b, keys);
}

/// `std.PriorityQueue` comparator: ranks the *worst* row (greatest under
/// `entryLess`) as highest priority, so `peek`/`remove` yield the eviction
/// candidate — the max-heap TopN needs, expressed against a min-heap API.
fn entryWorstFirst(keys: []const Sort.Key, a: TopN.Entry, b: TopN.Entry) std.math.Order {
    for (keys) |k| {
        const o = keyOrder(a[k.idx], b[k.idx], k.desc);
        if (o != .eq) return o.invert();
    }
    return .eq;
}

fn dupeValueGpa(gpa: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try gpa.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try gpa.dupe(u8, s) },
        else => v,
    };
}

/// Streaming hash aggregation: batches are consumed one at a time, folding into
/// per-group accumulators — O(groups) memory, not O(dataset). Group state (keys,
/// key values, accumulators) lives in `state` (the plan arena), with string key
/// values deep-copied there because batch memory dies between pulls.
pub const Aggregate = struct {
    child: Op,
    in_schema: *const types.Schema,
    by: []const usize,
    aggs: []const Agg,
    out_schema: *const types.Schema,
    err: ?*ErrCtx = null,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator, // backs the per-batch scratch arena
    done: bool = false,

    pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type };

    pub const Acc = struct {
        n: i64 = 0,
        sum_i: i64 = 0,
        sum_f: f64 = 0,
        ext: Value = .null,
        has_ext: bool = false,
    };

    pub const Group = struct { key_vals: []Value, accs: []Acc };

    /// Group-key hash map (value-keyed). Used by `drainGroups` and by `mergeGroups`
    /// when combining partial group sets from parallel workers.
    ///
    /// A type-returning fn, NOT a `const` type decl: `std.testing.refAllDeclsRecursive`
    /// (main.zig's test root) recurses into every container-level *type* declaration,
    /// and diving through a `std.HashMap` instantiation here produces a binary that
    /// segfaults at startup under Zig 0.15.2. Wrapping in a fn keeps the type reachable
    /// across modules while hiding it from that recursion. (Matches the codebase's other
    /// HashMaps, which stay function-local for the same reason.)
    pub fn GroupMap() type {
        return std.HashMap([]const Value, usize, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    }

    /// One agg's vectorized reduction of a single batch, merged into the running
    /// accumulator by `mergePartial`.
    const Partial = struct {
        nvalid: usize,
        sum_i: i64 = 0,
        sum_f: f64 = 0,
        ext: ?Value = null,
    };

    pub fn next(self: *Aggregate, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const groups = try self.drainGroups();
        // Grouped + empty input → no rows; a no-GROUP-BY still emits one row.
        if (self.by.len != 0 and groups.len == 0) return null;
        return try self.emit(arena, groups);
    }

    /// Fold the entire child into raw per-group accumulators (kept in `state`). This
    /// is the parallelizable half of aggregation: a worker drains its slice of the
    /// input into a partial group set, and `mergeGroups` combines partials across
    /// workers by recombining the *raw* accumulators (so AVG etc. stay correct);
    /// `emit` finalizes once at the end. No-GROUP-BY returns exactly one group.
    pub fn drainGroups(self: *Aggregate) anyerror![]Group {
        // Group state lives in `state`, so each child batch can be freed once folded.
        // Pull into a scratch arena reset per batch — this drains the WHOLE input in
        // one call, so reusing a never-reset arena would retain every parsed batch
        // (O(dataset) memory instead of O(groups)).
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        if (self.by.len == 0) {
            const accs = try self.state.alloc(Acc, self.aggs.len);
            for (accs) |*a| a.* = .{};
            while (try self.child.next(pull)) |b| {
                if (b.len != 0 and !(try self.foldVectorized(pull, b, accs))) try self.foldRowwise(pull, b, accs);
                _ = scratch.reset(.retain_capacity);
            }
            const one = try self.state.alloc(Group, 1);
            one[0] = .{ .key_vals = &.{}, .accs = accs };
            return one;
        }

        var groups = std.array_list.Managed(Group).init(self.state);
        var map = GroupMap().init(self.state);
        while (try self.child.next(pull)) |b| {
            const probe = try pull.alloc(Value, self.by.len); // reused per row (scratch)
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                for (self.by, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r); // aliases batch; lookup only
                const gop = try map.getOrPut(probe);
                if (!gop.found_existing) {
                    const kv = try self.state.alloc(Value, self.by.len);
                    for (self.by, 0..) |ci, j| kv[j] = try dupeValue(self.state, b.columns[ci].getValue(r));
                    gop.key_ptr.* = kv;
                    gop.value_ptr.* = groups.items.len;
                    const accs = try self.state.alloc(Acc, self.aggs.len);
                    for (accs) |*a| a.* = .{};
                    try groups.append(.{ .key_vals = kv, .accs = accs });
                }
                const g = &groups.items[gop.value_ptr.*];
                for (self.aggs, 0..) |agg, j| {
                    const v = try self.argValue(pull, agg, b, r);
                    try updateAcc(self.state, &g.accs[j], agg, v, agg.arg != null);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        return groups.toOwnedSlice();
    }

    /// Combine a partial accumulator `src` into `dst` for one agg — the dual of
    /// `updateAcc`, but folding two partials instead of a row. `dst_alloc` owns any
    /// min/max string carried over (the source partial's memory may be freed).
    pub fn mergeAcc(dst_alloc: std.mem.Allocator, dst: *Acc, src: Acc, agg: Agg) !void {
        switch (agg.func) {
            .count => dst.n += src.n,
            .sum => {
                if (agg.ty.kind == .float) dst.sum_f += src.sum_f else dst.sum_i += src.sum_i;
                dst.n += src.n;
            },
            .avg => {
                dst.sum_f += src.sum_f;
                dst.n += src.n;
            },
            .min => if (src.has_ext and (!dst.has_ext or lessV(src.ext, dst.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
                dst.has_ext = true;
            },
            .max => if (src.has_ext and (!dst.has_ext or lessV(dst.ext, src.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
                dst.has_ext = true;
            },
        }
    }

    /// Merge a worker's partial `src_groups` into a combined (`map`, `groups`) set,
    /// deep-copying keys and min/max values into `dst_alloc` so they survive the
    /// worker's arena being freed. Call under a lock when workers share the combiner.
    pub fn mergeGroups(map: *GroupMap(), groups: *std.array_list.Managed(Group), dst_alloc: std.mem.Allocator, src_groups: []const Group, aggs: []const Agg) !void {
        for (src_groups) |g| {
            const gop = try map.getOrPut(g.key_vals); // probe aliases worker arena; ok for lookup
            if (!gop.found_existing) {
                const kv = try dst_alloc.alloc(Value, g.key_vals.len);
                for (g.key_vals, kv) |v, *o| o.* = try dupeValue(dst_alloc, v);
                gop.key_ptr.* = kv;
                gop.value_ptr.* = groups.items.len;
                const accs = try dst_alloc.alloc(Acc, aggs.len);
                for (g.accs, accs, aggs) |src, *dst, agg| {
                    dst.* = src;
                    if ((agg.func == .min or agg.func == .max) and src.has_ext) dst.ext = try dupeValue(dst_alloc, src.ext);
                }
                try groups.append(.{ .key_vals = kv, .accs = accs });
            } else {
                const cg = &groups.items[gop.value_ptr.*];
                for (g.accs, aggs, 0..) |src, agg, j| try mergeAcc(dst_alloc, &cg.accs[j], src, agg);
            }
        }
    }

    /// Try the vectorized path for one batch: every agg's argument evaluated as
    /// a column once and SIMD-reduced to a `Partial`. Returns false (touching
    /// nothing) if any agg isn't covered, so the caller folds the batch row-wise.
    /// The int/float-only constraint depends on the (fixed) schema, so the same
    /// path is taken for every batch of a run.
    fn foldVectorized(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!bool {
        const partials = try arena.alloc(Partial, self.aggs.len);
        for (self.aggs, partials) |agg, *p| {
            p.* = (try self.reduceBatch(arena, agg, b)) orelse return false;
        }
        for (self.aggs, partials, accs) |agg, p, *acc| mergePartial(acc, agg, p);
        return true;
    }

    fn foldRowwise(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!void {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (self.aggs, 0..) |agg, j| {
                const v = try self.argValue(arena, agg, b, r);
                try updateAcc(self.state, &accs[j], agg, v, agg.arg != null);
            }
        }
    }

    /// One agg argument for one row, coerced to the planned type. The parallel
    /// CSV lanes carry raw string columns (the planner types sum/avg numeric and
    /// expects runtime coercion — the vectorized path gets it via `evalColumn`);
    /// without this a string cell reaching `sum` is a union-access crash, and
    /// unparseable text is a clean CastFailed instead.
    fn argValue(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch, r: usize) anyerror!Value {
        const e = agg.arg orelse return .null;
        var v = eval.evalRow(arena, e, b, r) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{errLabel(err)});
            return err;
        };
        if (v == .string and (agg.func == .sum or agg.func == .avg)) {
            v = eval.castValue(arena, v, agg.ty.kind) catch |err| {
                if (self.err) |ec| ec.set("{s}: in aggregate", .{errLabel(err)});
                return err;
            };
        }
        return v;
    }

    /// Vectorized reduce of one agg over one batch. `null` means "not covered,
    /// fold row-wise" (non-numeric arg); the constraint is schema-dependent.
    fn reduceBatch(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch) anyerror!?Partial {
        if (agg.func == .count and agg.arg == null) return Partial{ .nvalid = b.len };
        const e = agg.arg orelse return null;
        const col = eval.evalColumn(arena, e, b, agg.ty) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{errLabel(err)});
            return err;
        };
        if (col.ty.kind != .int and col.ty.kind != .float) return null; // row-wise handles the rest
        const n = b.len;
        const nvalid = simd.popcountValid(col.validity.bits, n);
        var p = Partial{ .nvalid = nvalid };
        if (nvalid == 0) return p;
        switch (agg.func) {
            .count => {},
            .sum, .avg => switch (col.ty.kind) {
                .float => p.sum_f = simd.sumF(col.data.f64[0..n]),
                .int => {
                    p.sum_i = sumIntCol(col.data.i64[0..n]); // null lanes are 0
                    p.sum_f = @floatFromInt(p.sum_i);
                },
                else => unreachable,
            },
            .min, .max => p.ext = reduceExtreme(col, agg.func, n),
        }
        return p;
    }

    /// Fold one batch's `Partial` into the running accumulator. Mirrors the
    /// row-wise `updateAcc` semantics (null-skipping, agg.ty-driven sum kind).
    fn mergePartial(acc: *Acc, agg: Agg, p: Partial) void {
        switch (agg.func) {
            .count => acc.n += @intCast(p.nvalid),
            .sum => if (p.nvalid > 0) {
                if (agg.ty.kind == .float) acc.sum_f += p.sum_f else acc.sum_i += p.sum_i;
                acc.n += @intCast(p.nvalid);
            },
            .avg => if (p.nvalid > 0) {
                acc.sum_f += p.sum_f;
                acc.n += @intCast(p.nvalid);
            },
            .min => if (p.ext) |v| {
                if (!acc.has_ext or lessV(v, acc.ext)) {
                    acc.ext = v; // numeric scalar: no batch memory to outlive
                    acc.has_ext = true;
                }
            },
            .max => if (p.ext) |v| {
                if (!acc.has_ext or lessV(acc.ext, v)) {
                    acc.ext = v;
                    acc.has_ext = true;
                }
            },
        }
    }

    pub fn emit(self: *Aggregate, arena: std.mem.Allocator, groups: []const Group) anyerror!Batch {
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

    /// `state` owns any string extremum copied into the accumulator: the value
    /// must outlive the batch it came from (the per-pull arena is reset).
    fn updateAcc(state: std.mem.Allocator, acc: *Acc, agg: Agg, v: Value, has_arg: bool) !void {
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
                    acc.ext = try dupeValue(state, v);
                    acc.has_ext = true;
                }
            },
            .max => if (!v.isNull()) {
                if (!acc.has_ext or lessV(acc.ext, v)) {
                    acc.ext = try dupeValue(state, v);
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

/// Deep-copy a value into `state` so it survives the batch it was read from.
/// Only string/bytes carry pointers into batch memory; scalars copy by value.
pub fn dupeValue(state: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try state.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try state.dupe(u8, s) },
        else => v,
    };
}

fn sumIntCol(d: []const i64) i64 {
    var s: i64 = 0;
    for (d) |x| s +%= x; // LLVM auto-vectorizes integer reduction; null lanes hold 0
    return s;
}

/// MIN/MAX over an int/float column, honoring nulls. SIMD on the all-valid fast
/// path (null lanes' 0 default would corrupt the extreme), else a scalar skip.
fn reduceExtreme(col: column.Column, func: ast.AggFunc, n: usize) Value {
    const is_min = func == .min;
    if (col.ty.kind == .float) {
        const d = col.data.f64[0..n];
        if (col.validity.allSet(n)) {
            return .{ .float = if (is_min) simd.minF(d) else simd.maxF(d) };
        }
        var m: ?f64 = null;
        for (d, 0..) |x, i| {
            if (!col.validity.get(i)) continue;
            m = if (m) |cur| (if (is_min) @min(cur, x) else @max(cur, x)) else x;
        }
        return if (m) |x| Value{ .float = x } else .null;
    }
    var m: ?i64 = null;
    for (col.data.i64[0..n], 0..) |x, i| {
        if (!col.validity.get(i)) continue;
        m = if (m) |cur| (if (is_min) @min(cur, x) else @max(cur, x)) else x;
    }
    return if (m) |x| Value{ .int = x } else .null;
}

/// Like materializeAll but always returns a (possibly empty) batch with the
/// schema's columns present — so a join can emit right-side nulls even when the
/// build side is empty. The result is built in `state` (which must outlive the
/// per-pull batch arena: the join probes it across many pulls), while the child
/// is pulled with the transient `pull` arena.
fn materializeFull(state: std.mem.Allocator, pull: std.mem.Allocator, child: Op, schema: *const types.Schema) anyerror!Batch {
    const ncols = schema.fields.len;
    const builders = try state.alloc(column.Builder, ncols);
    for (builders, schema.fields) |*b, f| b.* = column.Builder.init(state, f.ty);
    var total: usize = 0;
    while (try child.next(pull)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (b.columns, 0..) |*col, ci| try builders[ci].append(col.getValue(r));
        }
        total += b.len;
    }
    const cols = try state.alloc(column.Column, ncols);
    for (builders, 0..) |*bd, i| cols[i] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// Hash equi-join. The build (right) side is materialized into a hash index on
/// the first `next`; the probe (left) side then streams through. Supports
/// inner / left / semi / anti.
///
/// The build batch and index live across pulls, so they MUST NOT go into the
/// per-pull batch arena (the driver resets it before every `next`). They are
/// allocated in `state` — the plan arena, freed when the run ends.
pub const Join = struct {
    probe: Op,
    build: Op,
    left_key: usize,
    right_key: usize,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    state: std.mem.Allocator,

    built: bool = false,
    build_batch: Batch = undefined,
    index: Index = undefined,

    // Value-keyed hash index (no per-row string serialization). Build keys live in
    // `build_batch` (in state), so they need no separate copy. Null keys never match.
    const Index = std.HashMap(Value, std.array_list.Managed(usize), keyhash.SingleKeyCtx, std.hash_map.default_max_load_percentage);
    const empty_match: []const usize = &.{};

    pub fn next(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        if (!self.built) {
            self.built = true;
            self.build_batch = try materializeFull(self.state, arena, self.build, self.right_schema);
            self.index = Index.init(self.state);
            var r: usize = 0;
            while (r < self.build_batch.len) : (r += 1) {
                const k = self.build_batch.columns[self.right_key].getValue(r);
                if (k.isNull()) continue; // SQL: null keys never match
                const gop = try self.index.getOrPut(k);
                if (!gop.found_existing) gop.value_ptr.* = std.array_list.Managed(usize).init(self.state);
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
            const key = lb.columns[self.left_key].getValue(r);
            const matches: []const usize = if (key.isNull())
                empty_match
            else if (self.index.get(key)) |list|
                list.items
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// Test-only in-memory source handing out prebuilt batches. The batches live in
/// the test arena (outliving any pull arena), so operators that deep-copy for
/// cross-pull survival are still exercised safely.
const TestSource = struct {
    schema_: types.Schema,
    batches: []const Batch,
    idx: usize = 0,

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    fn schemaFn(p: *anyopaque) types.Schema {
        return @as(*TestSource, @ptrCast(@alignCast(p))).schema_;
    }
    fn nextFn(p: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
        const self: *TestSource = @ptrCast(@alignCast(p));
        if (self.idx >= self.batches.len) return null;
        defer self.idx += 1;
        return self.batches[self.idx];
    }
    fn closeFn(_: *anyopaque) void {}

    fn src(self: *TestSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

/// One nullable-int column batch.
fn intBatch(a: std.mem.Allocator, schema: *const types.Schema, vals: []const ?i64) !Batch {
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try column.intColumn(a, vals);
    return Batch{ .schema = schema, .columns = cols, .len = vals.len };
}

/// One nullable-string column batch.
fn strBatch(a: std.mem.Allocator, schema: *const types.Schema, vals: []const ?[]const u8) !Batch {
    var bd = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (vals) |v| try bd.append(if (v) |s| Value{ .string = s } else .null);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = vals.len };
}

/// Two-column (nullable int, nullable string) batch; slices must be equal length.
fn kvBatch(a: std.mem.Allocator, schema: *const types.Schema, ints: []const ?i64, strs: []const ?[]const u8) !Batch {
    const cols = try a.alloc(column.Column, 2);
    cols[0] = try column.intColumn(a, ints);
    var bd = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (strs) |v| try bd.append(if (v) |s| Value{ .string = s } else .null);
    cols[1] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = ints.len };
}

/// Drain `top` and collect column 0 as optional ints.
fn drainInts(a: std.mem.Allocator, top: Op) ![]const ?i64 {
    var got = std.array_list.Managed(?i64).init(a);
    while (try top.next(a)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            try got.append(if (v.isNull()) null else v.int);
        }
    }
    return got.toOwnedSlice();
}

const int_schema = types.Schema{ .fields = &.{
    .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
} };

test "limit skips offset rows across batch boundaries and stops at count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 1, 2, 3 }),
        try intBatch(a, &int_schema, &.{ 4, 5, 6 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    // offset 4 swallows batch 1 entirely and one row of batch 2; count 3 > what's left.
    var lim = Limit{ .child = .{ .scan = &scan }, .remaining = 3, .to_skip = 4 };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 5, 6 }), try drainInts(a, .{ .limit = &lim }));

    // count cutting a batch mid-way: skip 1, take 3 of 6.
    var ts2 = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan2 = Scan{ .src = ts2.src() };
    var lim2 = Limit{ .child = .{ .scan = &scan2 }, .remaining = 3, .to_skip = 1 };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 2, 3, 4 }), try drainInts(a, .{ .limit = &lim2 }));
}

test "filter keeps only known-true rows: null predicate drops the row (3VL)" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{try intBatch(a, &int_schema, &.{ 1, null, 5, 3, 2 })};
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var two = ast.Expr{ .int_lit = 2 };
    var pred = ast.Expr{ .binary = .{ .op = .gt, .l = &fx, .r = &two } };
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 5, 3 }), try drainInts(a, .{ .filter = &flt }));
}

test "filter surfaces eval errors through ErrCtx; first error wins" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{try intBatch(a, &int_schema, &.{1})};
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var zero = ast.Expr{ .int_lit = 0 };
    var one = ast.Expr{ .int_lit = 1 };
    var div = ast.Expr{ .binary = .{ .op = .div, .l = &fx, .r = &zero } };
    var pred = ast.Expr{ .binary = .{ .op = .gt, .l = &div, .r = &one } };

    var ec = ErrCtx{};
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred, .err = &ec };
    const top = Op{ .filter = &flt };
    try testing.expectError(error.DivByZero, top.next(a));
    try testing.expectEqualStrings("division by zero: in filter predicate", ec.msg);
    ec.set("later error", .{});
    try testing.expectEqualStrings("division by zero: in filter predicate", ec.msg);
}

test "project passes columns through and computes expressions with null propagation" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{try intBatch(a, &int_schema, &.{ 10, null })};
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var one = ast.Expr{ .int_lit = 1 };
    var plus = ast.Expr{ .binary = .{ .op = .add, .l = &fx, .r = &one } };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "y", .ty = types.Type.init(.int).asNullable() },
    } };
    const pcols = [_]Project.Col{
        .{ .source = .{ .passthrough = 0 }, .ty = types.Type.init(.int).asNullable() },
        .{ .source = .{ .expr = &plus }, .ty = types.Type.init(.int).asNullable() },
    };
    var proj = Project{ .child = .{ .scan = &scan }, .cols = &pcols, .out_schema = &out_schema };

    const b = (try proj.next(a)).?;
    try testing.expectEqual(@as(usize, 2), b.len);
    try testing.expectEqual(@as(i64, 10), b.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 11), b.columns[1].getValue(0).int);
    try testing.expect(b.columns[0].getValue(1).isNull());
    try testing.expect(b.columns[1].getValue(1).isNull());
}

test "distinct dedups across batches, groups nulls as one key, deep-copies strings" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try strBatch(a, &schema, &.{ "a", "b", null }),
        try strBatch(a, &schema, &.{ "b", null, "c", "a" }),
    };
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &schema, .keys = null, .state = a, .gpa = testing.allocator };

    var got = std.array_list.Managed(?[]const u8).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            try got.append(if (v.isNull()) null else v.string);
        }
    }
    const want = [_]?[]const u8{ "a", "b", null, "c" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| {
        if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
    }
}

test "sort: descending order with nulls always last" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 3, null }),
        try intBatch(a, &int_schema, &.{ 1, 2 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var srt = Sort{ .child = .{ .scan = &scan }, .in_schema = &int_schema, .keys = &[_]Sort.Key{.{ .idx = 0, .desc = true }} };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 3, 2, 1, null }), try drainInts(a, .{ .sort = &srt }));
}

test "top_n keeps best rows across batches, honors offset, matches full sort" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try kvBatch(a, &schema, &.{ 5, 1, 4 }, &.{ "e", "a", "d" }),
        try kvBatch(a, &schema, &.{ 2, 8, 3 }, &.{ "b", "z", "c" }),
    };
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    // sorted asc: 1,2,3,4,5,8; offset 1, count 2 -> rows 2,3. Heap cap 3 forces evictions.
    var tn = TopN{
        .child = .{ .scan = &scan },
        .in_schema = &schema,
        .keys = &[_]Sort.Key{.{ .idx = 0, .desc = false }},
        .count = 2,
        .offset = 1,
        .state = a,
        .gpa = testing.allocator, // leak-checks cloned row strings
    };
    const b = (try tn.next(a)).?;
    try testing.expectEqual(@as(usize, 2), b.len);
    try testing.expectEqual(@as(i64, 2), b.columns[0].getValue(0).int);
    try testing.expectEqualStrings("b", b.columns[1].getValue(0).string);
    try testing.expectEqual(@as(i64, 3), b.columns[0].getValue(1).int);
    try testing.expectEqualStrings("c", b.columns[1].getValue(1).string);
    try testing.expect((try tn.next(a)) == null); // done after one emission
}

test "aggregate: grouped count/sum/avg/min/max skip nulls per group" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // kvBatch puts ints in column 0, strings in column 1 -> key column is 1.
    const in_schema = types.Schema{ .fields = &.{
        .{ .name = "v", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "k", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try kvBatch(a, &in_schema, &.{ 1, 10 }, &.{ "a", "b" }),
        try kvBatch(a, &in_schema, &.{ null, 3, 2 }, &.{ "a", "a", "b" }),
    };
    var ts = TestSource{ .schema_ = in_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };

    const fv = ast.Expr{ .field = .{ .parts = &[_][]const u8{"v"} } };
    const aggs = [_]Aggregate.Agg{
        .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
        .{ .func = .sum, .arg = &fv, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .avg, .arg = &fv, .ty = types.Type.init(.float).asNullable() },
        .{ .func = .min, .arg = &fv, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .max, .arg = &fv, .ty = types.Type.init(.int).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "k", .ty = types.Type.init(.string) },
        .{ .name = "c", .ty = types.Type.init(.int) },
        .{ .name = "s", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "av", .ty = types.Type.init(.float).asNullable() },
        .{ .name = "mn", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "mx", .ty = types.Type.init(.int).asNullable() },
    } };
    var agg = Aggregate{
        .child = .{ .scan = &scan },
        .in_schema = &in_schema,
        .by = &.{1},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    const b = (try agg.next(a)).?;
    try testing.expectEqual(@as(usize, 2), b.len);
    // Groups emit in first-seen order: "a" (rows 1, null, 3), then "b" (10, 2).
    try testing.expectEqualStrings("a", b.columns[0].getValue(0).string);
    try testing.expectEqual(@as(i64, 3), b.columns[1].getValue(0).int); // count(*) counts the null row
    try testing.expectEqual(@as(i64, 4), b.columns[2].getValue(0).int); // sum skips the null
    try testing.expectEqual(@as(f64, 2.0), b.columns[3].getValue(0).float); // avg over 2 non-null rows
    try testing.expectEqual(@as(i64, 1), b.columns[4].getValue(0).int);
    try testing.expectEqual(@as(i64, 3), b.columns[5].getValue(0).int);
    try testing.expectEqualStrings("b", b.columns[0].getValue(1).string);
    try testing.expectEqual(@as(i64, 2), b.columns[1].getValue(1).int);
    try testing.expectEqual(@as(i64, 12), b.columns[2].getValue(1).int);
    try testing.expectEqual(@as(f64, 6.0), b.columns[3].getValue(1).float);
    try testing.expectEqual(@as(i64, 2), b.columns[4].getValue(1).int);
    try testing.expectEqual(@as(i64, 10), b.columns[5].getValue(1).int);
}

test "aggregate: sum/avg coerce raw string cells (parallel CSV lane shape); garbage text errors" {
    // The mapped-CSV lanes feed all-string columns; the planner types sum(int-ish)
    // as int and expects runtime coercion. Regression: this used to be a
    // union-access crash in the row-wise fold.
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const s_schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.string).asNullable() },
    } };
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const aggs = [_]Aggregate.Agg{
        .{ .func = .sum, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .avg, .arg = &fx, .ty = types.Type.init(.float).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "av", .ty = types.Type.init(.float).asNullable() },
    } };

    {
        const batches = [_]Batch{try strBatch(a, &s_schema, &.{ "4", null, "2" })};
        var ts = TestSource{ .schema_ = s_schema, .batches = &batches };
        var scan = Scan{ .src = ts.src() };
        var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &s_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
        const b = (try agg.next(a)).?;
        try testing.expectEqual(@as(i64, 6), b.columns[0].getValue(0).int);
        try testing.expectEqual(@as(f64, 3.0), b.columns[1].getValue(0).float);
    }
    {
        const batches = [_]Batch{try strBatch(a, &s_schema, &.{ "4", "oops" })};
        var ts = TestSource{ .schema_ = s_schema, .batches = &batches };
        var scan = Scan{ .src = ts.src() };
        var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &s_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
        try testing.expectError(error.CastFailed, agg.next(a));
    }
}

test "aggregate: global vectorized reductions honor nulls; empty input edge cases" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const aggs = [_]Aggregate.Agg{
        .{ .func = .count, .arg = null, .ty = types.Type.init(.int) },
        .{ .func = .count, .arg = &fx, .ty = types.Type.init(.int) },
        .{ .func = .sum, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .avg, .arg = &fx, .ty = types.Type.init(.float).asNullable() },
        .{ .func = .min, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
        .{ .func = .max, .arg = &fx, .ty = types.Type.init(.int).asNullable() },
    };
    const out_schema = types.Schema{ .fields = &.{
        .{ .name = "c", .ty = types.Type.init(.int) },
        .{ .name = "cv", .ty = types.Type.init(.int) },
        .{ .name = "s", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "av", .ty = types.Type.init(.float).asNullable() },
        .{ .name = "mn", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "mx", .ty = types.Type.init(.int).asNullable() },
    } };

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 4, null }),
        try intBatch(a, &int_schema, &.{ 2, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var agg = Aggregate{
        .child = .{ .scan = &scan },
        .in_schema = &int_schema,
        .by = &.{},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    const b = (try agg.next(a)).?;
    try testing.expectEqual(@as(usize, 1), b.len);
    try testing.expectEqual(@as(i64, 4), b.columns[0].getValue(0).int); // count(*) includes null row
    try testing.expectEqual(@as(i64, 3), b.columns[1].getValue(0).int); // count(x) does not
    try testing.expectEqual(@as(i64, 15), b.columns[2].getValue(0).int);
    try testing.expectEqual(@as(f64, 5.0), b.columns[3].getValue(0).float);
    try testing.expectEqual(@as(i64, 2), b.columns[4].getValue(0).int);
    try testing.expectEqual(@as(i64, 9), b.columns[5].getValue(0).int);

    // Empty input: a global aggregate still emits one row (count 0, sum/min null)...
    var ets = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var escan = Scan{ .src = ets.src() };
    var eagg = Aggregate{
        .child = .{ .scan = &escan },
        .in_schema = &int_schema,
        .by = &.{},
        .aggs = &aggs,
        .out_schema = &out_schema,
        .state = a,
        .gpa = testing.allocator,
    };
    const eb = (try eagg.next(a)).?;
    try testing.expectEqual(@as(usize, 1), eb.len);
    try testing.expectEqual(@as(i64, 0), eb.columns[0].getValue(0).int);
    try testing.expect(eb.columns[2].getValue(0).isNull());
    try testing.expect(eb.columns[4].getValue(0).isNull());

    // ...while a grouped aggregate over empty input emits no rows at all.
    var gts = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var gscan = Scan{ .src = gts.src() };
    var gagg = Aggregate{
        .child = .{ .scan = &gscan },
        .in_schema = &int_schema,
        .by = &.{0},
        .aggs = &aggs,
        .out_schema = &out_schema, // shape unused: no rows come out
        .state = a,
        .gpa = testing.allocator,
    };
    try testing.expect((try gagg.next(a)) == null);
}

test "join: inner/left/semi/anti; null keys never match on either side" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const left_schema = types.Schema{ .fields = &.{
        .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
    } };
    const right_schema = types.Schema{ .fields = &.{
        .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
    } };
    const both_schema = types.Schema{ .fields = &.{
        .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
        .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
    } };

    const Case = struct { kind: ast.JoinKind, keys: []const ?i64, rvs: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .kind = .inner, .keys = &.{ 1, 1 }, .rvs = &.{ "x", "y" } },
        .{ .kind = .left, .keys = &.{ 1, 1, 2, null, 3 }, .rvs = &.{ "x", "y", null, null, null } },
        .{ .kind = .semi, .keys = &.{1}, .rvs = &.{} },
        .{ .kind = .anti, .keys = &.{ 2, null, 3 }, .rvs = &.{} },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rb = [_]Batch{try kvBatch(a, &right_schema, &.{ 1, 1, 4, null }, &.{ "x", "y", "z", "m" })};
        var lts = TestSource{ .schema_ = left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        const emit_right = case.kind == .inner or case.kind == .left;
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_key = 0,
            .right_key = 0,
            .left_schema = &left_schema,
            .right_schema = &right_schema,
            .out_schema = if (emit_right) &both_schema else &left_schema,
            .kind = case.kind,
            .state = a,
        };
        var keys = std.array_list.Managed(?i64).init(a);
        var rvs = std.array_list.Managed(?[]const u8).init(a);
        const top = Op{ .join = &jn };
        while (try top.next(a)) |b| {
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                const kv = b.columns[0].getValue(r);
                try keys.append(if (kv.isNull()) null else kv.int);
                if (emit_right) {
                    const rv = b.columns[3].getValue(r);
                    try rvs.append(if (rv.isNull()) null else rv.string);
                }
            }
        }
        try testing.expectEqualDeep(case.keys, @as([]const ?i64, keys.items));
        try testing.expectEqual(case.rvs.len, rvs.items.len);
        for (case.rvs, rvs.items) |w, g| {
            if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
        }
    }
}

test "union drains children in order, skipping empty ones" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const b1 = [_]Batch{try intBatch(a, &int_schema, &.{ 1, 2 })};
    const b3 = [_]Batch{try intBatch(a, &int_schema, &.{3})};
    var ts1 = TestSource{ .schema_ = int_schema, .batches = &b1 };
    var ts2 = TestSource{ .schema_ = int_schema, .batches = &.{} }; // empty middle child
    var ts3 = TestSource{ .schema_ = int_schema, .batches = &b3 };
    var s1 = Scan{ .src = ts1.src() };
    var s2 = Scan{ .src = ts2.src() };
    var s3 = Scan{ .src = ts3.src() };
    const children = [_]Op{ .{ .scan = &s1 }, .{ .scan = &s2 }, .{ .scan = &s3 } };
    var un = Union{ .children = &children };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 1, 2, 3 }), try drainInts(a, .{ .union_ = &un }));
}

test "explode splits delimited strings, repeats other columns, drops null cells" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "tags", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{try kvBatch(a, &schema, &.{ 1, 2, 3, 4 }, &.{ "a,b", null, "c", "" })};
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var ex = Explode{ .child = .{ .scan = &scan }, .field_idx = 1, .delim = ",", .out_schema = &schema };

    const b = (try (Op{ .explode = &ex }).next(a)).?;
    try testing.expectEqual(@as(usize, 4), b.len);
    const want_ids = [_]i64{ 1, 1, 3, 4 };
    const want_tags = [_][]const u8{ "a", "b", "c", "" }; // empty string -> one empty element
    for (want_ids, want_tags, 0..) |wi, wt, r| {
        try testing.expectEqual(wi, b.columns[0].getValue(r).int);
        try testing.expectEqualStrings(wt, b.columns[1].getValue(r).string);
    }
}

test "linearize decomposes map-only pipelines source-to-sink; breakers refuse" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var ts = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var scan = Scan{ .src = ts.src() };
    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var zero = ast.Expr{ .int_lit = 0 };
    var pred = ast.Expr{ .binary = .{ .op = .gt, .l = &fx, .r = &zero } };
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred };
    const pcols = [_]Project.Col{.{ .source = .{ .passthrough = 0 }, .ty = types.Type.init(.int).asNullable() }};
    var proj = Project{ .child = .{ .filter = &flt }, .cols = &pcols, .out_schema = &int_schema };

    const lin = (try linearize(a, .{ .project = &proj })).?;
    try testing.expectEqual(@as(usize, 2), lin.stages.len);
    try testing.expect(lin.stages[0] == .filter); // source-side stage first
    try testing.expect(lin.stages[1] == .project);
    try testing.expectEqual(@as(*anyopaque, &ts), lin.src.ptr);

    // A breaker anywhere in the chain disqualifies the whole pipeline.
    var srt = Sort{ .child = .{ .project = &proj }, .in_schema = &int_schema, .keys = &.{} };
    try testing.expect((try linearize(a, .{ .sort = &srt })) == null);
}
