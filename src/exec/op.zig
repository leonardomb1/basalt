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

/// Build a batch selecting the rows where `keep[r]` is true.
fn gather(arena: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, 0..) |*col, ci| outcols[ci] = try column.gather(arena, col.*, keep, kept);
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
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
/// dupe string/bytes payloads (unlike `gather`, which aliases them). Used when the
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

/// Effective order of two sort-key values: `.lt` means `va` sorts before `vb`.
/// Nulls always sort last (independent of `desc`); `desc` flips non-null order.
/// Shared by Top-N's heap and final sort (mirrors `SortCtx.lessThan`).
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

    pub fn next(self: *TopN, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        if (self.count == 0) return null;
        const cap = self.offset + self.count; // keep this many best rows

        // Max-heap by `entryLess` (root = the worst-ranked kept row), entries owned
        // by `gpa` and freed on eviction → O(cap) memory. Child pulled through a
        // scratch arena reset per batch (the batches themselves are never retained).
        var heap = std.array_list.Managed(Entry).init(self.gpa);
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
                if (heap.items.len < cap) {
                    try heap.append(try self.cloneRow(b, r));
                    siftUp(heap.items, heap.items.len - 1, self.keys);
                } else if (self.rowLess(b, r, heap.items[0])) {
                    // r ranks before the current worst kept → replace the root
                    self.freeEntry(heap.items[0]);
                    heap.items[0] = try self.cloneRow(b, r);
                    siftDown(heap.items, 0, self.keys);
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

/// Binary max-heap (root = greatest under `entryLess`) sift helpers over a slice.
fn siftUp(h: []TopN.Entry, start: usize, keys: []const Sort.Key) void {
    var i = start;
    while (i > 0) {
        const parent = (i - 1) / 2;
        if (!entryLess(h[parent], h[i], keys)) break; // parent already >= child
        std.mem.swap(TopN.Entry, &h[parent], &h[i]);
        i = parent;
    }
}

fn siftDown(h: []TopN.Entry, start: usize, keys: []const Sort.Key) void {
    var i = start;
    const n = h.len;
    while (true) {
        const l = 2 * i + 1;
        const r = 2 * i + 2;
        var largest = i;
        if (l < n and entryLess(h[largest], h[l], keys)) largest = l;
        if (r < n and entryLess(h[largest], h[r], keys)) largest = r;
        if (largest == i) break;
        std.mem.swap(TopN.Entry, &h[i], &h[largest]);
        i = largest;
    }
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

    const Acc = struct {
        n: i64 = 0,
        sum_i: i64 = 0,
        sum_f: f64 = 0,
        ext: Value = .null,
        has_ext: bool = false,
    };

    const Group = struct { key_vals: []Value, accs: []Acc };

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

        // Group state (accumulators, keys, key values) lives in `state`, so each
        // child batch can be freed once folded. Pull into a scratch arena reset per
        // batch — this drain runs the WHOLE input in one call, so reusing the
        // caller's arena (never reset mid-call) would retain every parsed batch,
        // making aggregation O(dataset) memory instead of O(groups).
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        // No GROUP BY: one accumulator set; each batch folds in via the
        // vectorized partial reduce (SIMD per batch) or row-wise fallback.
        if (self.by.len == 0) {
            const accs = try self.state.alloc(Acc, self.aggs.len);
            for (accs) |*a| a.* = .{};
            while (try self.child.next(pull)) |b| {
                if (b.len != 0 and !(try self.foldVectorized(pull, b, accs))) try self.foldRowwise(pull, b, accs);
                _ = scratch.reset(.retain_capacity);
            }
            // Empty input still emits one row of finalized fresh accumulators.
            return try self.emit(arena, &.{.{ .key_vals = &.{}, .accs = accs }});
        }

        var groups = std.array_list.Managed(Group).init(self.state);
        const Map = std.HashMap([]const Value, usize, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
        var map = Map.init(self.state);
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
                    var v: Value = .null;
                    if (agg.arg) |e| v = eval.evalRow(pull, e, b, r) catch |err| {
                        if (self.err) |ec| ec.set("{s}: in aggregate", .{errLabel(err)});
                        return err;
                    };
                    try updateAcc(self.state, &g.accs[j], agg, v, agg.arg != null);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        if (groups.items.len == 0) return null;
        return try self.emit(arena, groups.items);
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
                var v: Value = .null;
                if (agg.arg) |e| v = eval.evalRow(arena, e, b, r) catch |err| {
                    if (self.err) |ec| ec.set("{s}: in aggregate", .{errLabel(err)});
                    return err;
                };
                try updateAcc(self.state, &accs[j], agg, v, agg.arg != null);
            }
        }
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
fn dupeValue(state: std.mem.Allocator, v: Value) !Value {
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
