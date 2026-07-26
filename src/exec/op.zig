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
        if (self.msg.len > 0) return;
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

/// Per-operator execution counters, filled in as the pipeline is pulled so a
/// plan can be printed back with actuals beside its estimates.
pub const Stats = struct {
    ns: u64 = 0,
    rows: u64 = 0,
    calls: u64 = 0,
};

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

    /// Accumulated counters for this operator. `inline else` reaches the
    /// payload pointer without naming all eleven variants.
    pub fn stats(self: Op) *Stats {
        return switch (self) {
            inline else => |o| &o.stats,
        };
    }

    /// This operator's inputs, written into `buf` (at most two, plus a union's
    /// branches). Used to turn inclusive timings into exclusive ones.
    pub fn inputs(self: Op, buf: *std.array_list.Managed(Op)) !void {
        switch (self) {
            .scan => {},
            .join => |j| {
                try buf.append(j.probe);
                try buf.append(j.build);
            },
            .union_ => |u| try buf.appendSlice(u.children),
            inline else => |o| try buf.append(o.child),
        }
    }

    pub fn next(self: Op, arena: std.mem.Allocator) anyerror!?Batch {
        const st = self.stats();
        const t0 = std.time.Instant.now() catch {
            return self.nextInner(arena);
        };
        const r = try self.nextInner(arena);
        const t1 = std.time.Instant.now() catch return r;
        st.ns += t1.since(t0);
        st.calls += 1;
        if (r) |b| st.rows += b.len;
        return r;
    }

    fn nextInner(self: Op, arena: std.mem.Allocator) anyerror!?Batch {
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
    stats: Stats = .{},
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
    stats: Stats = .{},
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
                else => continue,
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
    stats: Stats = .{},
    src: driver.Source,

    pub fn next(self: *Scan, arena: std.mem.Allocator) anyerror!?Batch {
        return self.src.next(arena);
    }
};

pub const Filter = struct {
    stats: Stats = .{},
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
    stats: Stats = .{},
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
    stats: Stats = .{},
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

fn applyFilter(arena: std.mem.Allocator, b: Batch, pred: *const ast.Expr) anyerror!Batch {
    const mask = try eval.evalColumn(arena, pred, b, types.Type.init(.bool));
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
    const per = try arena.alloc(column.Column, chunks.items.len);
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
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    seen: ?Seen = null,

    const Seen = std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);

    pub fn next(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.seen == null) self.seen = Seen.init(self.state);
        const seen = &self.seen.?;

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
            const probe = try pull.alloc(Value, key_idx.len);
            var kept: usize = 0;
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                for (key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                const gop = try seen.getOrPut(probe);
                if (gop.found_existing) {
                    keep[r] = false;
                } else {
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
    stats: Stats = .{},
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
        // lift each key column into a flat typed array once, then sort on that
        const arrs = try arena.alloc(KeyArr, self.keys.len);
        for (self.keys, arrs) |k, *a| a.* = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        std.mem.sort(usize, idx, SortCtx{ .arrs = arrs }, SortCtx.lessThan);

        const outcols = try arena.alloc(column.Column, all.columns.len);
        for (all.columns, 0..) |*col, ci| outcols[ci] = try column.permute(arena, col.*, idx);
        return Batch{ .schema = all.schema, .columns = outcols, .len = all.len };
    }
};

/// One sort key lifted out of its column into a comparable typed array.
///
/// The comparator runs O(n log n) times; `getValue` boxes a ~32-byte tagged
/// union and re-switches on the column type on every call, twice per comparison.
/// Extracting each key once, up front, turns that into a plain typed compare.
const KeyArr = struct {
    desc: bool,
    valid: column.Bitmap,
    data: Data,

    const Data = union(enum) {
        ints: []i64,
        floats: []f64,
        decs: []i128,
        strs: [][]const u8,
        /// Types with no cheap flat form fall back to boxing.
        boxed: column.Column,
    };

    fn prepare(arena: std.mem.Allocator, col: column.Column, desc: bool) !KeyArr {
        const n = col.len;
        const data: Data = switch (col.ty.kind) {
            .int, .date, .time, .timestamp, .bool => blk: {
                const out = try arena.alloc(i64, n);
                for (out, 0..) |*o, i| o.* = switch (col.getValue(i)) {
                    .int => |x| x,
                    .date => |x| x,
                    .time => |x| x,
                    .timestamp => |x| x,
                    .bool => |x| @intFromBool(x),
                    else => 0,
                };
                break :blk .{ .ints = out };
            },
            .float => blk: {
                const out = try arena.alloc(f64, n);
                for (out, 0..) |*o, i| o.* = switch (col.getValue(i)) {
                    .float => |x| x,
                    else => 0,
                };
                break :blk .{ .floats = out };
            },
            .decimal => blk: {
                const out = try arena.alloc(i128, n);
                for (out, 0..) |*o, i| o.* = switch (col.getValue(i)) {
                    .decimal => |d| d.unscaled,
                    else => 0,
                };
                break :blk .{ .decs = out };
            },
            .string, .bytes => blk: {
                const out = try arena.alloc([]const u8, n);
                for (out, 0..) |*o, i| o.* = switch (col.getValue(i)) {
                    .string => |x| x,
                    .bytes => |x| x,
                    else => "",
                };
                break :blk .{ .strs = out };
            },
            else => .{ .boxed = col },
        };
        return .{ .desc = desc, .valid = col.validity, .data = data };
    }

    /// Nulls sort last regardless of direction, matching `keyOrder`.
    fn order(self: KeyArr, a: usize, c: usize) std.math.Order {
        const an = !self.valid.get(a);
        const bn = !self.valid.get(c);
        if (an or bn) {
            if (an and bn) return .eq;
            return if (an) .gt else .lt;
        }
        const ord: std.math.Order = switch (self.data) {
            .ints => |v| std.math.order(v[a], v[c]),
            .floats => |v| std.math.order(v[a], v[c]),
            .decs => |v| std.math.order(v[a], v[c]),
            .strs => |v| std.mem.order(u8, v[a], v[c]),
            .boxed => |col| return keyOrder(col.getValue(a), col.getValue(c), self.desc),
        };
        if (ord == .eq) return .eq;
        return if (self.desc) (if (ord == .lt) std.math.Order.gt else std.math.Order.lt) else ord;
    }
};

const SortCtx = struct {
    arrs: []const KeyArr,

    fn lessThan(self: SortCtx, a: usize, c: usize) bool {
        for (self.arrs) |k| {
            const o = k.order(a, c);
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
        return if (an) .gt else .lt;
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
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: []const Sort.Key,
    count: u64,
    offset: u64,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    done: bool = false,
    /// When set, the K-th best key is published here so a source can skip
    /// row groups that cannot beat it.
    threshold: ?*valuemod.Threshold = null,

    const Entry = []Value;
    const Heap = std.PriorityQueue(Entry, []const Sort.Key, entryWorstFirst);

    pub fn next(self: *TopN, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        if (self.count == 0) return null;
        const cap = self.offset + self.count;

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
                    if (heap.count() >= cap) self.publish(heap.items[0]);
                } else if (self.rowLess(b, r, heap.items[0])) {
                    self.freeEntry(heap.remove());
                    try heap.add(try self.cloneRow(b, r));
                    self.publish(heap.items[0]);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        if (heap.items.len == 0) return null;

        std.mem.sort(Entry, heap.items, self.keys, entryLessCtx);
        const start = @min(self.offset, heap.items.len);
        const end = @min(self.offset + self.count, heap.items.len);
        if (start >= end) return null;
        return try self.emit(arena, heap.items[start..end]);
    }

    /// Publishes the worst kept entry's first key. Only a single sort key is
    /// pushed down; with several, the leading key still bounds the rest.
    fn publish(self: *TopN, worst: Entry) void {
        const t = self.threshold orelse return;
        if (self.keys.len == 0) return;
        const v = worst[self.keys[0].idx];
        // a null bound would skip nothing useful, and nulls sort last anyway
        if (v == .null or v == .string or v == .bytes) return;
        t.value = v;
        t.full = true;
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
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    by: []const usize,
    aggs: []const Agg,
    out_schema: *const types.Schema,
    err: ?*ErrCtx = null,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    done: bool = false,

    pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, distinct: bool = false };

    pub const Acc = struct {
        n: i64 = 0,
        sum_i: i64 = 0,
        sum_f: f64 = 0,
        ext: Value = .null,
        has_ext: bool = false,
        /// Values already counted by a `COUNT(DISTINCT x)`, per group. Only
        /// allocated for distinct aggs, so ordinary aggregation keeps its
        /// scalar accumulator.
        seen: ?*DistinctSet() = null,
    };

    /// Wrapped in a fn for the same reason as `GroupMap` — see its comment.
    pub fn DistinctSet() type {
        return std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    }

    fn noteDistinct(alloc: std.mem.Allocator, acc: *Acc, v: Value) !void {
        const set = acc.seen orelse blk: {
            const p = try alloc.create(DistinctSet());
            p.* = DistinctSet().init(alloc);
            acc.seen = p;
            break :blk p;
        };
        const key = try alloc.alloc(Value, 1);
        key[0] = try dupeValue(alloc, v);
        const gop = try set.getOrPut(key);
        if (!gop.found_existing) gop.key_ptr.* = key;
        acc.n = @intCast(set.count());
    }

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
        if (self.by.len != 0 and groups.len == 0) return null;
        return try self.emit(arena, groups);
    }

    /// Fold the entire child into raw per-group accumulators (kept in `state`). This
    /// is the parallelizable half of aggregation: a worker drains its slice of the
    /// input into a partial group set, and `mergeGroups` combines partials across
    /// workers by recombining the *raw* accumulators (so AVG etc. stay correct);
    /// `emit` finalizes once at the end. No-GROUP-BY returns exactly one group.
    pub fn drainGroups(self: *Aggregate) anyerror![]Group {
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
        var ghashes = std.array_list.Managed(u64).init(self.state);
        // One table per radix partition. A single table for millions of groups
        // misses cache on every probe; sixty-four smaller ones stay resident,
        // the shape DuckDB and ClickHouse both arrived at. The partition comes
        // from the top hash bits and the bucket from the bottom, so the two
        // never interfere.
        const nparts = 64;
        const part_shift = 58;
        const tables = try self.state.alloc(GroupTable, nparts);
        for (tables) |*t| t.* = try GroupTable.init(self.state, 256);
        const counts = try self.state.alloc(u32, nparts + 1);
        var store = GroupStore{ .alloc = self.state, .nkeys = self.by.len, .naggs = self.aggs.len };
        const hctx = keyhash.MultiKeyCtx{};
        while (try self.child.next(pull)) |b| {
            const probe = try pull.alloc(Value, self.by.len);
            const hashes = try pull.alloc(u64, b.len);

            // Hash the whole batch first, then walk it again to probe: by the
            // time a row's bucket is read the prefetch has had the rest of the
            // batch to land.
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                for (self.by, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                hashes[r] = hctx.hash(probe);
            }

            // Evaluate each aggregate's argument once for the whole batch.
            // Per row it re-walked the expression tree for every aggregate.
            const argcols = try pull.alloc(?column.Column, self.aggs.len);
            for (self.aggs, argcols) |agg, *c| {
                c.* = if (agg.arg) |e| try self.argColumn(pull, agg, e, b) else null;
            }

            // Counting-sort the batch's rows by partition, then walk one
            // partition at a time so consecutive probes land in the same small
            // table instead of scattering across a huge one.
            @memset(counts, 0);
            for (hashes[0..b.len]) |h| counts[(h >> part_shift) + 1] += 1;
            for (1..nparts + 1) |ci| counts[ci] += counts[ci - 1];
            const order = try pull.alloc(u32, b.len);
            for (hashes[0..b.len], 0..) |h, ri| {
                const pi = h >> part_shift;
                order[counts[pi]] = @intCast(ri);
                counts[pi] += 1;
            }

            for (order) |ri| {
                r = ri;
                for (self.by, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                const at: u32 = @intCast(groups.items.len);
                const table = &tables[hashes[r] >> part_shift];
                const f = try table.getOrPut(hashes[r], probe, groups.items, ghashes.items, at);
                if (!f.found) {
                    const slot = try store.next();
                    for (probe, slot.keys) |v, *o| o.* = try dupeValue(self.state, v);
                    try groups.append(.{ .key_vals = slot.keys, .accs = slot.accs });
                    try ghashes.append(hashes[r]);
                }
                const g = &groups.items[f.slot];
                for (self.aggs, 0..) |agg, j| {
                    const v = if (argcols[j]) |col| col.getValue(r) else Value.null;
                    try updateAcc(self.state, &g.accs[j], agg, v, agg.arg != null);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        return groups.toOwnedSlice();
    }

    /// Open-addressed group index built for the one access pattern aggregation
    /// has: hash a batch of keys, then probe them all. Two things it does that a
    /// general map cannot — it stores each key's hash, so growing never re-hashes
    /// a key, and it exposes the bucket up front so a batch can prefetch its
    /// buckets before probing. At high cardinality the probe is a cache miss, and
    /// hiding that miss is the whole game.
    /// Open-addressed group index sized for cache, not for generality.
    ///
    /// Each slot is a single `u32`: a few salt bits from the key's hash plus the
    /// group's index. Storing a salt rather than the whole hash is what makes
    /// the entry small enough that sixteen share a cache line, and at high
    /// cardinality the number of lines a probe touches *is* the cost — an
    /// earlier version kept the full 8-byte hash in one array and the index in
    /// another, so every probe missed twice. The full hash lives beside the
    /// group instead, so growing never re-hashes a key (the same trick DuckDB's
    /// aggregate table uses).
    const GroupTable = struct {
        const salt_bits = 6;
        const idx_bits = 32 - salt_bits;
        const max_groups = (1 << idx_bits) - 1;

        entries: []u32,
        len: usize = 0,
        mask: u64,
        alloc: std.mem.Allocator,

        fn init(alloc: std.mem.Allocator, cap_pow2: usize) !GroupTable {
            const e = try alloc.alloc(u32, cap_pow2);
            @memset(e, 0);
            return .{ .entries = e, .mask = cap_pow2 - 1, .alloc = alloc };
        }

        /// Salt comes from bits the bucket index does not use, so the two stay
        /// independent as the table grows.
        fn saltOf(h: u64) u32 {
            return @intCast((h >> 32) & ((1 << salt_bits) - 1));
        }

        fn pack(h: u64, idx: usize) u32 {
            return (saltOf(h) << idx_bits) | @as(u32, @intCast(idx + 1));
        }

        fn prefetch(self: *const GroupTable, h: u64) void {
            @prefetch(&self.entries[h & self.mask], .{ .rw = .read, .locality = 3 });
        }

        /// Rebuild from this table's own entries, taking each group's hash from
        /// `hashes` — so a table owning one radix partition rehomes only the
        /// groups it holds.
        fn grow(self: *GroupTable, hashes: []const u64) !void {
            const cap = self.entries.len * 2;
            const ne = try self.alloc.alloc(u32, cap);
            @memset(ne, 0);
            const nmask = cap - 1;
            for (self.entries) |e| {
                if (e == 0) continue;
                const h = hashes[(e & max_groups) - 1];
                var i = h & nmask;
                while (ne[i] != 0) i = (i + 1) & nmask;
                ne[i] = e;
            }
            self.entries = ne;
            self.mask = nmask;
        }

        const Found = struct { slot: u32, found: bool };

        fn getOrPut(
            self: *GroupTable,
            h: u64,
            key: []const Value,
            groups: []const Group,
            hashes: []const u64,
            new_slot: u32,
        ) !Found {
            if (new_slot >= max_groups) return error.TooManyGroups;
            if ((self.len + 1) * 10 >= self.entries.len * 7) try self.grow(hashes);
            const want = saltOf(h) << idx_bits;
            var i = h & self.mask;
            while (true) : (i = (i + 1) & self.mask) {
                const e = self.entries[i];
                if (e == 0) {
                    self.entries[i] = pack(h, new_slot);
                    self.len += 1;
                    return .{ .slot = new_slot, .found = false };
                }
                if ((e >> idx_bits) << idx_bits != want) continue;
                const idx = (e & max_groups) - 1;
                if (keyhash.MultiKeyCtx.eql(.{}, key, groups[idx].key_vals)) return .{ .slot = idx, .found = true };
            }
        }
    };

    /// Block allocator for group keys and accumulators. One allocation per
    /// `block` groups instead of two per group, and the accumulators of nearby
    /// groups land next to each other. Blocks are never resized, so the slices
    /// handed out stay valid.
    const GroupStore = struct {
        const block = 8192;

        alloc: std.mem.Allocator,
        nkeys: usize,
        naggs: usize,
        keys: []Value = &.{},
        accs: []Acc = &.{},
        used: usize = block,

        const Slot = struct { keys: []Value, accs: []Acc };

        fn next(self: *GroupStore) !Slot {
            if (self.used == block) {
                self.keys = try self.alloc.alloc(Value, block * self.nkeys);
                self.accs = try self.alloc.alloc(Acc, block * self.naggs);
                self.used = 0;
            }
            const i = self.used;
            self.used += 1;
            const a = self.accs[i * self.naggs ..][0..self.naggs];
            for (a) |*x| x.* = .{};
            return .{ .keys = self.keys[i * self.nkeys ..][0..self.nkeys], .accs = a };
        }
    };

    /// Combine a partial accumulator `src` into `dst` for one agg — the dual of
    /// `updateAcc`, but folding two partials instead of a row. `dst_alloc` owns any
    /// min/max string carried over (the source partial's memory may be freed).
    pub fn mergeAcc(dst_alloc: std.mem.Allocator, dst: *Acc, src: Acc, agg: Agg) !void {
        switch (agg.func) {
            .count => if (agg.distinct) {
                if (src.seen) |s| {
                    var it = s.keyIterator();
                    while (it.next()) |k| try noteDistinct(dst_alloc, dst, k.*[0]);
                }
            } else {
                dst.n += src.n;
            },
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
        return mergeGroupsPart(map, groups, dst_alloc, src_groups, aggs, 0, 1);
    }

    /// Merge for the parallel path, where the source groups live in lane arenas
    /// that outlive the merge: a key seen for the first time is *adopted* — its
    /// key and accumulators are reused in place rather than copied. The
    /// high-cardinality case is then one hash and one pointer store per group
    /// instead of two allocations, which is what makes the merge cheap enough
    /// for the fold to be worth parallelising at all. Only a repeated key pays
    /// for an accumulator fold.
    pub fn adoptGroups(
        map: *GroupMap(),
        groups: *std.array_list.Managed(Group),
        dst_alloc: std.mem.Allocator,
        src_groups: []const Group,
        aggs: []const Agg,
    ) !void {
        var any_distinct = false;
        for (aggs) |a| {
            if (a.distinct) any_distinct = true;
        }
        for (src_groups) |g| {
            const gop = try map.getOrPut(g.key_vals);
            if (!gop.found_existing) {
                gop.key_ptr.* = g.key_vals;
                gop.value_ptr.* = groups.items.len;
                var adopted = g;
                if (any_distinct) {
                    const accs = try dst_alloc.alloc(Acc, aggs.len);
                    for (g.accs, accs, aggs) |src, *dst, agg| {
                        dst.* = src;
                        if (!agg.distinct) continue;
                        dst.seen = null;
                        dst.n = 0;
                        if (src.seen) |ss| {
                            var it = ss.keyIterator();
                            while (it.next()) |k| try noteDistinct(dst_alloc, dst, k.*[0]);
                        }
                    }
                    adopted.accs = accs;
                }
                try groups.append(adopted);
            } else {
                const cg = &groups.items[gop.value_ptr.*];
                for (g.accs, aggs, 0..) |src, agg, j| try mergeAcc(dst_alloc, &cg.accs[j], src, agg);
            }
        }
    }

    /// `mergeGroups` restricted to the groups whose key hashes into `part` of
    /// `nparts`. Partitioning the merge by hash lets one task own each partition
    /// outright: the key sets are disjoint, so the tasks need no lock between
    /// them — which is what keeps a high-cardinality merge from serializing.
    pub fn mergeGroupsPart(
        map: *GroupMap(),
        groups: *std.array_list.Managed(Group),
        dst_alloc: std.mem.Allocator,
        src_groups: []const Group,
        aggs: []const Agg,
        part: usize,
        nparts: usize,
    ) !void {
        const ctx = keyhash.MultiKeyCtx{};
        for (src_groups) |g| {
            if (nparts > 1 and (ctx.hash(g.key_vals) >> 32) % nparts != part) continue;
            const gop = try map.getOrPut(g.key_vals);
            if (!gop.found_existing) {
                const kv = try dst_alloc.alloc(Value, g.key_vals.len);
                for (g.key_vals, kv) |v, *o| o.* = try dupeValue(dst_alloc, v);
                gop.key_ptr.* = kv;
                gop.value_ptr.* = groups.items.len;
                const accs = try dst_alloc.alloc(Acc, aggs.len);
                for (g.accs, accs, aggs) |src, *dst, agg| {
                    dst.* = src;
                    if ((agg.func == .min or agg.func == .max) and src.has_ext) dst.ext = try dupeValue(dst_alloc, src.ext);
                    if (agg.distinct) {
                        dst.seen = null;
                        dst.n = 0;
                        if (src.seen) |ss| {
                            var it = ss.keyIterator();
                            while (it.next()) |k| try noteDistinct(dst_alloc, dst, k.*[0]);
                        }
                    }
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
        for (self.aggs) |agg| if (agg.distinct) return false;
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
    /// One aggregate's argument evaluated across a whole batch, typed as the
    /// aggregate expects — which also applies the string-to-number coercion
    /// `argValue` did per row for `sum`/`avg`.
    fn argColumn(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, e: *const ast.Expr, b: Batch) anyerror!column.Column {
        // `sum(x)` and friends name a column the batch already holds; hand it
        // over instead of materialising a copy. Only when the stored type feeds
        // the accumulator directly — a text column under a numeric aggregate
        // still has to go through the coercing path.
        if (e.* == .field) {
            if (b.schema.indexOf(e.field.last())) |ci| {
                const ck = b.columns[ci].ty.kind;
                const ak = agg.ty.kind;
                if (ck == ak or (ck.isNumeric() and ak.isNumeric())) return b.columns[ci];
            }
        }
        return eval.evalColumn(arena, e, b, agg.ty) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{errLabel(err)});
            return err;
        };
    }

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
        if (col.ty.kind != .int and col.ty.kind != .float) return null;
        const n = b.len;
        const nvalid = simd.popcountValid(col.validity.bits, n);
        var p = Partial{ .nvalid = nvalid };
        if (nvalid == 0) return p;
        switch (agg.func) {
            .count => {},
            .sum, .avg => switch (col.ty.kind) {
                .float => p.sum_f = simd.sumF(col.data.f64[0..n]),
                .int => {
                    p.sum_i = sumIntCol(col.data.i64[0..n]);
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
                    acc.ext = v;
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
                if (agg.distinct) {
                    if (!v.isNull()) try noteDistinct(state, acc, v);
                } else if (!has_arg or !v.isNull()) acc.n += 1;
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

    pub fn finalizeAcc(acc: Acc, agg: Agg) Value {
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
    for (d) |x| s +%= x;
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
    stats: Stats = .{},
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
                if (k.isNull()) continue;
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
        @setEvalBranchQuota(2000);
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
    var lim = Limit{ .child = .{ .scan = &scan }, .remaining = 3, .to_skip = 4 };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 5, 6 }), try drainInts(a, .{ .limit = &lim }));

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
    var tn = TopN{
        .child = .{ .scan = &scan },
        .in_schema = &schema,
        .keys = &[_]Sort.Key{.{ .idx = 0, .desc = false }},
        .count = 2,
        .offset = 1,
        .state = a,
        .gpa = testing.allocator,
    };
    const b = (try tn.next(a)).?;
    try testing.expectEqual(@as(usize, 2), b.len);
    try testing.expectEqual(@as(i64, 2), b.columns[0].getValue(0).int);
    try testing.expectEqualStrings("b", b.columns[1].getValue(0).string);
    try testing.expectEqual(@as(i64, 3), b.columns[0].getValue(1).int);
    try testing.expectEqualStrings("c", b.columns[1].getValue(1).string);
    try testing.expect((try tn.next(a)) == null);
}

test "aggregate: grouped count/sum/avg/min/max skip nulls per group" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

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
    try testing.expectEqualStrings("a", b.columns[0].getValue(0).string);
    try testing.expectEqual(@as(i64, 3), b.columns[1].getValue(0).int);
    try testing.expectEqual(@as(i64, 4), b.columns[2].getValue(0).int);
    try testing.expectEqual(@as(f64, 2.0), b.columns[3].getValue(0).float);
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
    try testing.expectEqual(@as(i64, 4), b.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 3), b.columns[1].getValue(0).int);
    try testing.expectEqual(@as(i64, 15), b.columns[2].getValue(0).int);
    try testing.expectEqual(@as(f64, 5.0), b.columns[3].getValue(0).float);
    try testing.expectEqual(@as(i64, 2), b.columns[4].getValue(0).int);
    try testing.expectEqual(@as(i64, 9), b.columns[5].getValue(0).int);

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

    var gts = TestSource{ .schema_ = int_schema, .batches = &.{} };
    var gscan = Scan{ .src = gts.src() };
    var gagg = Aggregate{
        .child = .{ .scan = &gscan },
        .in_schema = &int_schema,
        .by = &.{0},
        .aggs = &aggs,
        .out_schema = &out_schema,
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
    var ts2 = TestSource{ .schema_ = int_schema, .batches = &.{} };
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
    const want_tags = [_][]const u8{ "a", "b", "c", "" };
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
    try testing.expect(lin.stages[0] == .filter);
    try testing.expect(lin.stages[1] == .project);
    try testing.expectEqual(@as(*anyopaque, &ts), lin.src.ptr);

    var srt = Sort{ .child = .{ .project = &proj }, .in_schema = &int_schema, .keys = &.{} };
    try testing.expect((try linearize(a, .{ .sort = &srt })) == null);
}
