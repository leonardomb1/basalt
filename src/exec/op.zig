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
        error.JoinBuildTooLarge => "join build side exceeds its cap — raise it with WITH (max_build = '8GB') on the join, filter the CTE, or flip the join",
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
                // A pre-built (shared) index has no build pipeline to charge.
                if (j.build) |b| try buf.append(b);
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
    /// Input batches are pulled into this arena, not the caller's, and it is
    /// reset per batch. A selective predicate can otherwise drain the entire
    /// source inside one `next` call — the caller only resets between calls —
    /// so a filter that matches nothing used to hold every batch it rejected at
    /// once. `gather` copies the surviving rows into the caller's arena, so
    /// nothing returned points into here.
    scratch: ?std.heap.ArenaAllocator = null,

    pub fn next(self: *Filter, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.scratch == null) self.scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        while (true) {
            _ = self.scratch.?.reset(.retain_capacity);
            const b = (try self.child.next(self.scratch.?.allocator())) orelse return null;
            const out = try self.filterInto(arena, self.scratch.?.allocator(), b);
            if (out.len > 0) return out;
        }
    }

    /// Stateless transform of one input batch (for the parallel driver).
    pub fn transform(self: *Filter, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return self.filterInto(arena, arena, b);
    }

    fn filterInto(self: *Filter, out: std.mem.Allocator, scratch: std.mem.Allocator, b: Batch) anyerror!Batch {
        return applyFilter(out, scratch, b, self.pred) catch |e| {
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

fn applyFilter(arena: std.mem.Allocator, scratch: std.mem.Allocator, b: Batch, pred: *const ast.Expr) anyerror!Batch {
    const mask = try eval.evalColumn(scratch, pred, b, types.Type.init(.bool));
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
    pub const Drained = struct { groups: []Group, hashes: []u64 };

    /// `drainGroups`, but keeping the hash it computed for each group. The
    /// parallel path partitions and merges by hash afterwards; recomputing it
    /// there means hashing every key three times instead of once.
    pub fn drainGroupsHashed(self: *Aggregate) anyerror!Drained {
        return self.drainImpl();
    }

    pub fn drainGroups(self: *Aggregate) anyerror![]Group {
        return (try self.drainImpl()).groups;
    }

    /// Fold with raw fixed-width keys. Same shape as `drainImpl`, but the probe
    /// key is a run of `i64` rather than boxed `Value`s, so the record is smaller
    /// and the hash is over plain words. Groups are boxed back into `Value` once,
    /// at the end, so everything downstream is unchanged.
    fn drainFixed(self: *Aggregate, kinds: []const KeyKind, comptime counts_only: bool) anyerror!Drained {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        var ghashes = std.array_list.Managed(u64).init(self.state);
        const nparts = 64;
        const part_shift = 58;
        const tables = try self.state.alloc(GroupTable, nparts);
        for (tables) |*t| t.* = try GroupTable.init(self.state, 256);
        const counts = try self.state.alloc(u32, nparts + 1);
        var store = if (counts_only) CountStore.init(self.state, self.by.len, self.aggs.len) else FixedStore.init(self.state, self.by.len, self.aggs.len);
        const nk = self.by.len;

        while (try self.child.next(pull)) |b| {
            const keys = try pull.alloc(i64, b.len * nk);
            const masks = try pull.alloc(u64, b.len);
            const hashes = try pull.alloc(u64, b.len);
            @memset(masks, 0);

            for (self.by, kinds, 0..) |ci, kk, j| {
                const col = b.columns[ci];
                const all_valid = col.validity.allSet(b.len);
                var r: usize = 0;
                while (r < b.len) : (r += 1) {
                    if (!all_valid and !col.validity.get(r)) {
                        masks[r] |= @as(u64, 1) << @intCast(j);
                        keys[r * nk + j] = 0;
                        continue;
                    }
                    keys[r * nk + j] = switch (kk) {
                        .i64k => col.data.i64[r],
                        .i32k => col.data.i32[r],
                        .boolk => @intFromBool(col.data.b[r]),
                        .f64k => @bitCast(col.data.f64[r]),
                    };
                }
            }
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                var hh = std.hash.Wyhash.init(masks[r]);
                hh.update(std.mem.sliceAsBytes(keys[r * nk ..][0..nk]));
                hashes[r] = hh.final();
            }

            const argcols = try pull.alloc(?column.Column, self.aggs.len);
            for (self.aggs, argcols) |agg, *c| {
                c.* = if (agg.arg) |e| try self.argColumn(pull, agg, e, b) else null;
            }

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
                const key = FixedKey{ .vals = keys[ri * nk ..][0..nk], .mask = masks[ri] };
                const at: u32 = @intCast(store.len);
                const table = &tables[hashes[ri] >> part_shift];
                const f = try table.getOrPut(hashes[ri], key, &store, ghashes.items, at);
                const rec = if (f.found) store.at(f.slot) else blk: {
                    const nr = try store.push();
                    @memcpy(nr.keys, key.vals);
                    nr.mask.* = key.mask;
                    try ghashes.append(hashes[ri]);
                    break :blk nr;
                };
                if (counts_only) {
                    for (rec.counts) |*c| c.* += 1;
                } else {
                    for (self.aggs, 0..) |agg, j| {
                        const v = if (argcols[j]) |col| col.getValue(ri) else Value.null;
                        try updateAcc(self.state, &rec.accs[j], agg, v, agg.arg != null);
                    }
                }
            }
            _ = scratch.reset(.retain_capacity);
        }

        const out = try self.state.alloc(Group, store.len);
        const kv_all = try self.state.alloc(Value, store.len * nk);
        for (out, 0..) |*g, i| {
            const rec = store.at(i);
            const kv = kv_all[i * nk ..][0..nk];
            for (kv, rec.keys, kinds, 0..) |*o, raw, kk, j| {
                if (rec.mask.* & (@as(u64, 1) << @intCast(j)) != 0) {
                    o.* = .null;
                    continue;
                }
                o.* = switch (self.in_schema.fields[self.by[j]].ty.kind) {
                    .int => .{ .int = raw },
                    .time => .{ .time = raw },
                    .timestamp => .{ .timestamp = raw },
                    .date => .{ .date = @intCast(raw) },
                    .bool => .{ .bool = raw != 0 },
                    .float => .{ .float = @bitCast(raw) },
                    else => unreachable,
                };
                _ = kk;
            }
            if (counts_only) {
                const accs = try self.state.alloc(Acc, self.aggs.len);
                for (accs, rec.counts) |*a, c| a.* = .{ .n = c };
                g.* = .{ .key_vals = kv, .accs = accs };
            } else {
                g.* = .{ .key_vals = kv, .accs = rec.accs };
            }
        }
        return .{ .groups = out, .hashes = ghashes.items };
    }

    fn drainImpl(self: *Aggregate) anyerror!Drained {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        if (self.by.len != 0) fixed: {
            const kinds = try self.state.alloc(KeyKind, self.by.len);
            for (self.by, kinds) |ci, *k| k.* = keyKindOf(self.in_schema.fields[ci].ty.kind) orelse break :fixed;
            var counts_only = true;
            for (self.aggs) |a| {
                if (a.func != .count or a.arg != null or a.distinct) counts_only = false;
            }
            return if (counts_only) self.drainFixed(kinds, true) else self.drainFixed(kinds, false);
        }

        if (self.by.len == 0) {
            const accs = try self.state.alloc(Acc, self.aggs.len);
            for (accs) |*a| a.* = .{};
            while (try self.child.next(pull)) |b| {
                if (b.len != 0 and !(try self.foldVectorized(pull, b, accs))) try self.foldRowwise(pull, b, accs);
                _ = scratch.reset(.retain_capacity);
            }
            const one = try self.state.alloc(Group, 1);
            one[0] = .{ .key_vals = &.{}, .accs = accs };
            return .{ .groups = one, .hashes = &.{} };
        }

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
        var store = GroupStore.init(self.state, self.by.len, self.aggs.len);
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
                const at: u32 = @intCast(store.len);
                const table = &tables[hashes[r] >> part_shift];
                const f = try table.getOrPut(hashes[r], probe, &store, ghashes.items, at);
                const rec = if (f.found) store.at(f.slot) else blk: {
                    const nr = try store.push();
                    for (probe, nr.keys) |v, *o| o.* = try dupeValue(self.state, v);
                    try ghashes.append(hashes[r]);
                    break :blk nr;
                };
                for (self.aggs, 0..) |agg, j| {
                    const v = if (argcols[j]) |col| col.getValue(r) else Value.null;
                    try updateAcc(self.state, &rec.accs[j], agg, v, agg.arg != null);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        const out = try self.state.alloc(Group, store.len);
        for (out, 0..) |*g, i| {
            const rec = store.at(i);
            g.* = .{ .key_vals = rec.keys, .accs = rec.accs };
        }
        return .{ .groups = out, .hashes = ghashes.items };
    }

    /// Open-addressed group index built for the one access pattern aggregation
    /// has: hash a batch of keys, then probe them all. Two things it does that a
    /// general map cannot — it stores each key's hash, so growing never re-hashes
    /// a key, and it exposes the bucket up front so a batch can prefetch its
    /// buckets before probing. At high cardinality the probe is a cache miss, and
    /// hiding that miss is the whole game.
    /// How a fixed-width key column is read into a raw `i64`.
    const KeyKind = enum { i64k, i32k, boolk, f64k };

    fn keyKindOf(kind: types.TypeKind) ?KeyKind {
        return switch (kind) {
            .int, .time, .timestamp => .i64k,
            .date => .i32k,
            .bool => .boolk,
            .float => .f64k,
            else => null,
        };
    }

    /// Group storage for keys that are all fixed-width. A boxed `Value` costs 32
    /// bytes to say what eight bytes of `i64` already says, and hashing one walks
    /// a tagged union per key; here the keys are raw words with a null mask
    /// beside them, so both the record and the hash get cheaper.
    const FixedStore = struct {
        const block_shift = 13;
        const block = 1 << block_shift;
        const block_mask = block - 1;

        alloc: std.mem.Allocator,
        nkeys: usize,
        naggs: usize,
        blocks: std.array_list.Managed([]u8),
        len: usize = 0,

        const Rec = struct { keys: []i64, mask: *u64, accs: []Acc };

        fn init(alloc: std.mem.Allocator, nkeys: usize, naggs: usize) FixedStore {
            return .{ .alloc = alloc, .nkeys = nkeys, .naggs = naggs, .blocks = std.array_list.Managed([]u8).init(alloc) };
        }

        fn recSize(self: FixedStore) usize {
            return self.nkeys * @sizeOf(i64) + @sizeOf(u64) + self.naggs * @sizeOf(Acc);
        }

        fn at(self: FixedStore, i: usize) Rec {
            const rec = self.recSize();
            const base = self.blocks.items[i >> block_shift].ptr + (i & block_mask) * rec;
            const ksz = self.nkeys * @sizeOf(i64);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(i64, base[0..ksz])),
                .mask = @alignCast(@ptrCast(base + ksz)),
                .accs = @alignCast(std.mem.bytesAsSlice(Acc, (base + ksz + @sizeOf(u64))[0 .. self.naggs * @sizeOf(Acc)])),
            };
        }

        fn eqlAt(self: *const FixedStore, i: usize, key: FixedKey) bool {
            const r = self.at(i);
            if (r.mask.* != key.mask) return false;
            return std.mem.eql(i64, r.keys, key.vals);
        }

        fn push(self: *FixedStore) !Rec {
            if (self.len & block_mask == 0 and self.len >> block_shift == self.blocks.items.len) {
                try self.blocks.append(try self.alloc.alignedAlloc(u8, .of(Acc), block * self.recSize()));
            }
            const rec = self.at(self.len);
            self.len += 1;
            for (rec.accs) |*a| a.* = .{};
            return rec;
        }
    };

    /// `COUNT(*)` needs eight bytes of state, but `Acc` is sixty-four — enough
    /// for a min/max `Value` this group will never hold. Counting-only folds get
    /// a record of keys, mask and counters, which for two keys and one count is
    /// 32 bytes: two groups per cache line instead of one straddling two.
    const CountStore = struct {
        const block_shift = 13;
        const block = 1 << block_shift;
        const block_mask = block - 1;

        alloc: std.mem.Allocator,
        nkeys: usize,
        naggs: usize,
        blocks: std.array_list.Managed([]u8),
        len: usize = 0,

        const Rec = struct { keys: []i64, mask: *u64, counts: []i64 };

        fn init(alloc: std.mem.Allocator, nkeys: usize, naggs: usize) CountStore {
            return .{ .alloc = alloc, .nkeys = nkeys, .naggs = naggs, .blocks = std.array_list.Managed([]u8).init(alloc) };
        }

        fn recSize(self: CountStore) usize {
            return (self.nkeys + 1 + self.naggs) * @sizeOf(i64);
        }

        fn at(self: CountStore, i: usize) Rec {
            const rec = self.recSize();
            const base = self.blocks.items[i >> block_shift].ptr + (i & block_mask) * rec;
            const ksz = self.nkeys * @sizeOf(i64);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(i64, base[0..ksz])),
                .mask = @alignCast(@ptrCast(base + ksz)),
                .counts = @alignCast(std.mem.bytesAsSlice(i64, (base + ksz + @sizeOf(u64))[0 .. self.naggs * @sizeOf(i64)])),
            };
        }

        fn eqlAt(self: *const CountStore, i: usize, key: FixedKey) bool {
            const r = self.at(i);
            if (r.mask.* != key.mask) return false;
            return std.mem.eql(i64, r.keys, key.vals);
        }

        fn push(self: *CountStore) !Rec {
            if (self.len & block_mask == 0 and self.len >> block_shift == self.blocks.items.len) {
                try self.blocks.append(try self.alloc.alignedAlloc(u8, .of(i64), block * self.recSize()));
            }
            const rec = self.at(self.len);
            self.len += 1;
            @memset(rec.counts, 0);
            return rec;
        }
    };

    const FixedKey = struct { vals: []const i64, mask: u64 };

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
            // Doubling is right while the table is small, but every rehash at
            // size re-scatters the whole table, and a high-cardinality fold pays
            // that eight or nine times over. Past the point where a resize is
            // the expensive part, jump further ahead instead.
            const cap = self.entries.len * @as(usize, if (self.entries.len >= 1 << 14) 8 else 2);
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
            key: anytype,
            store: anytype,
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
                if (store.eqlAt(idx, key)) return .{ .slot = idx, .found = true };
            }
        }
    };

    /// Block allocator for group keys and accumulators. One allocation per
    /// `block` groups instead of two per group, and the accumulators of nearby
    /// groups land next to each other. Blocks are never resized, so the slices
    /// handed out stay valid.
    /// Group records in fixed blocks, addressed by index. A record holds its
    /// keys immediately followed by its accumulators, so the probe that finds a
    /// group and the update that follows touch the same run of bytes. Blocks are
    /// never resized, so an index stays valid for the life of the fold.
    const GroupStore = struct {
        const block_shift = 13;
        const block = 1 << block_shift;
        const block_mask = block - 1;

        alloc: std.mem.Allocator,
        nkeys: usize,
        naggs: usize,
        blocks: std.array_list.Managed([]u8),
        len: usize = 0,

        const Rec = struct { keys: []Value, accs: []Acc };

        fn init(alloc: std.mem.Allocator, nkeys: usize, naggs: usize) GroupStore {
            return .{
                .alloc = alloc,
                .nkeys = nkeys,
                .naggs = naggs,
                .blocks = std.array_list.Managed([]u8).init(alloc),
            };
        }

        fn recSize(self: GroupStore) usize {
            return self.nkeys * @sizeOf(Value) + self.naggs * @sizeOf(Acc);
        }

        fn at(self: GroupStore, i: usize) Rec {
            const rec = self.recSize();
            const base = self.blocks.items[i >> block_shift].ptr + (i & block_mask) * rec;
            const ksz = self.nkeys * @sizeOf(Value);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(Value, base[0..ksz])),
                .accs = @alignCast(std.mem.bytesAsSlice(Acc, base[ksz..][0 .. self.naggs * @sizeOf(Acc)])),
            };
        }

        fn eqlAt(self: *const GroupStore, i: usize, key: []const Value) bool {
            return keyhash.MultiKeyCtx.eql(.{}, key, self.at(i).keys);
        }

        fn push(self: *GroupStore) !Rec {
            if (self.len & block_mask == 0 and self.len >> block_shift == self.blocks.items.len) {
                try self.blocks.append(try self.alloc.alignedAlloc(u8, .of(Value), block * self.recSize()));
            }
            const rec = self.at(self.len);
            self.len += 1;
            for (rec.accs) |*a| a.* = .{};
            return rec;
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
            .min => if (!src.ext.isNull() and (dst.ext.isNull() or lessV(src.ext, dst.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
            },
            .max => if (!src.ext.isNull() and (dst.ext.isNull() or lessV(dst.ext, src.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
            },
        }
    }

    /// Merge a worker's partial `src_groups` into a combined (`map`, `groups`) set,
    /// deep-copying keys and min/max values into `dst_alloc` so they survive the
    /// worker's arena being freed. Call under a lock when workers share the combiner.
    pub fn mergeGroups(map: *GroupMap(), groups: *std.array_list.Managed(Group), dst_alloc: std.mem.Allocator, src_groups: []const Group, aggs: []const Agg) !void {
        return mergeGroupsPart(map, groups, dst_alloc, src_groups, aggs, 0, 1);
    }

    /// Open-addressed index for the merge, keyed on a hash the caller already
    /// holds. `adoptGroups` went through a general map, which re-hashed every
    /// key it was handed — the third full hashing pass over the same data.
    /// Take a lane's group as-is, rebuilding only what cannot be shared — the
    /// distinct set, whose arena belongs to the producing lane.
    pub fn adoptOne(dst_alloc: std.mem.Allocator, g: Group, aggs: []const Agg, any_distinct: bool) !Group {
        if (!any_distinct) return g;
        var out = g;
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
        out.accs = accs;
        return out;
    }

    pub const MergeTable = struct {
        entries: []u32,
        len: usize = 0,
        mask: u64,
        alloc: std.mem.Allocator,

        const salt_bits = 6;
        const idx_bits = 32 - salt_bits;
        const max_groups = (1 << idx_bits) - 1;

        pub fn init(alloc: std.mem.Allocator, cap_pow2: usize) !MergeTable {
            const e = try alloc.alloc(u32, cap_pow2);
            @memset(e, 0);
            return .{ .entries = e, .mask = cap_pow2 - 1, .alloc = alloc };
        }

        fn saltOf(h: u64) u32 {
            return @intCast((h >> 32) & ((1 << salt_bits) - 1));
        }

        fn grow(self: *MergeTable, hashes: []const u64) !void {
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

        /// Index of the group matching `key`/`h`, or null after recording
        /// `new_idx` as its slot.
        pub fn find(self: *MergeTable, h: u64, key: []const Value, groups: []const Group, hashes: []const u64, new_idx: usize) !?usize {
            if ((self.len + 1) * 10 >= self.entries.len * 7) try self.grow(hashes);
            const want = saltOf(h) << idx_bits;
            var i = h & self.mask;
            while (true) : (i = (i + 1) & self.mask) {
                const e = self.entries[i];
                if (e == 0) {
                    self.entries[i] = want | @as(u32, @intCast(new_idx + 1));
                    self.len += 1;
                    return null;
                }
                if ((e >> idx_bits) << idx_bits != want) continue;
                const idx = (e & max_groups) - 1;
                if (keyhash.MultiKeyCtx.eql(.{}, key, groups[idx].key_vals)) return idx;
            }
        }
    };

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
                    if ((agg.func == .min or agg.func == .max) and !src.ext.isNull()) dst.ext = try dupeValue(dst_alloc, src.ext);
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
                if (acc.ext.isNull() or lessV(v, acc.ext)) {
                    acc.ext = v;
                }
            },
            .max => if (p.ext) |v| {
                if (acc.ext.isNull() or lessV(acc.ext, v)) {
                    acc.ext = v;
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
                switch (agg.ty.kind) {
                    .float => acc.sum_f += eval.toF64(v),
                    // A value's scale is whatever the source sent, which need not
                    // be the column's declared scale (postgres NUMERIC carries a
                    // per-value dscale), so every addend is normalized to the
                    // output scale that `finalizeAcc` will stamp back on. Adding
                    // raw unscaled integers instead multiplied the sum by
                    // 10^(declared - actual).
                    .decimal => {
                        const d: valuemod.Decimal = if (v == .decimal)
                            v.decimal
                        else
                            .{ .unscaled = v.int, .scale = 0 };
                        const r = eval.rescaleTo(d, agg.ty.scale) orelse return error.CastFailed;
                        const addend = std.math.cast(i64, r.unscaled) orelse return error.CastFailed;
                        acc.sum_i = std.math.add(i64, acc.sum_i, addend) catch return error.CastFailed;
                    },
                    else => acc.sum_i += v.int,
                }
                acc.n += 1;
            },
            .avg => if (!v.isNull()) {
                acc.sum_f += eval.toF64(v);
                acc.n += 1;
            },
            .min => if (!v.isNull()) {
                if (acc.ext.isNull() or lessV(v, acc.ext)) {
                    acc.ext = try dupeValue(state, v);
                }
            },
            .max => if (!v.isNull()) {
                if (acc.ext.isNull() or lessV(acc.ext, v)) {
                    acc.ext = try dupeValue(state, v);
                }
            },
        }
    }

    pub fn finalizeAcc(acc: Acc, agg: Agg) Value {
        return switch (agg.func) {
            .count => .{ .int = acc.n },
            .sum => if (acc.n == 0) .null else switch (agg.ty.kind) {
                .float => Value{ .float = acc.sum_f },
                .decimal => Value{ .decimal = .{ .unscaled = acc.sum_i, .scale = agg.ty.scale } },
                else => Value{ .int = acc.sum_i },
            },
            .avg => if (acc.n == 0) .null else Value{ .float = acc.sum_f / @as(f64, @floatFromInt(acc.n)) },
            .min, .max => acc.ext,
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
///
/// Fails with `JoinBuildTooLarge` as soon as the drained bytes cross `cap`,
/// so an oversized build side is a diagnostic rather than an OOM.
fn materializeFull(state: std.mem.Allocator, pull: std.mem.Allocator, child: Op, schema: *const types.Schema, bytes_out: *usize, cap: usize) anyerror!Batch {
    const ncols = schema.fields.len;
    const builders = try state.alloc(column.Builder, ncols);
    for (builders, schema.fields) |*b, f| b.* = column.Builder.init(state, f.ty);
    var total: usize = 0;
    var bytes: usize = 0;
    while (try child.next(pull)) |b| {
        for (b.columns) |*col| bytes += columnBytes(col);
        if (bytes > cap) return error.JoinBuildTooLarge;
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            for (b.columns, 0..) |*col, ci| try builders[ci].append(col.getValue(r));
        }
        total += b.len;
    }
    const cols = try state.alloc(column.Column, ncols);
    for (builders, 0..) |*bd, i| cols[i] = try bd.finish();
    bytes_out.* = bytes;
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// Heap a column occupies: its typed store plus the validity bitmap. Close
/// enough to bound a build side; exact accounting would have to reach into the
/// allocator.
fn columnBytes(c: *const column.Column) usize {
    const payload: usize = switch (c.data) {
        .b => |s| s.len,
        .i32 => |s| s.len * @sizeOf(i32),
        .i64 => |s| s.len * @sizeOf(i64),
        .f64 => |s| s.len * @sizeOf(f64),
        .dec => |s| s.len * @sizeOf(valuemod.Decimal),
        .bytes => |b| b.values.len + b.offsets.len * @sizeOf(i32),
    };
    return payload + c.validity.bits.len;
}

// ponytail: a cap, no spill. If a capped run ever needs the data anyway, the
// upgrade is partitioning the build side and spilling cold partitions to disk.
/// Default ceiling on a materialized join build side (fully resident — that is
/// what makes probing O(1)). Without one a mis-written CTE takes the process
/// down instead of reporting. Per-join override: `WITH (max_build = '16GB')`
/// on the join clause. A `var` so tests can lower it; nothing else writes it.
pub var join_build_byte_cap: usize = 4 << 30;

/// How key columns compare for one key position. Deciding once per batch keeps
/// the per-row comparison a direct typed load instead of two boxed `Value`s.
pub const KeyClass = enum { i64s, bytes, boxed };

fn classOf(a: column.Column, b: column.Column) KeyClass {
    // Same logical kind is required: `int` and `timestamp` share the i64 store
    // but are never equal, and `valueEq` (the boxed path) says so.
    if (a.ty.kind != b.ty.kind) return .boxed;
    return switch (a.data) {
        .i64 => if (std.meta.activeTag(b.data) == .i64) .i64s else .boxed,
        .bytes => if (std.meta.activeTag(b.data) == .bytes) .bytes else .boxed,
        else => .boxed,
    };
}

fn cellEq(cls: KeyClass, a: *const column.Column, ar: usize, b: *const column.Column, br: usize) bool {
    return switch (cls) {
        .i64s => a.data.i64[ar] == b.data.i64[br],
        .bytes => std.mem.eql(u8, a.data.bytes.at(ar), b.data.bytes.at(br)),
        .boxed => keyhash.valueEq(a.getValue(ar), b.getValue(br)),
    };
}

/// Composite key hash for one row. Folds the same way `keyhash.MultiKeyCtx`
/// does (Wyhash, type tag + payload per value, in key order), so the build and
/// probe sides agree.
fn hashRowKeys(cols: []const column.Column, keys: []const usize, row: usize) u64 {
    var h = std.hash.Wyhash.init(0);
    for (keys) |k| keyhash.hashValue(&h, cols[k].getValue(row));
    return h.final();
}

/// SQL: a null key joins to nothing, on either side.
fn anyNullKey(cols: []const column.Column, keys: []const usize, row: usize) bool {
    for (keys) |k| {
        if (!cols[k].validity.get(row)) return true;
    }
    return false;
}

/// The build side of a hash join: the rows, materialized columnar, plus a flat
/// chained index over them.
///
/// Layout — three arrays, no per-key allocation and no boxed key stored:
///   * `heads[slot]`  bucket → build row + 1 (0 = empty). Linear probing, and a
///     slot is claimed by one *distinct* key.
///   * `next[row]`    build row → the next build row carrying the same key + 1
///     (0 ends the chain). Duplicate keys are a chain, not a heap list.
///   * `hashes[row]`  that row's key hash, so a bucket collision costs one u64
///     compare rather than a column-wise key comparison.
/// `heads` is sized from the build row count (which bounds the distinct keys)
/// and never grows, so probing needs no synchronisation.
///
/// After `create` the whole structure is read-only: several probe lanes may
/// share one index. Anything a probe has to *write* (outer-join match tracking)
/// lives on the `Join`, not here.
pub const JoinIndex = struct {
    build_batch: Batch,
    keys: []const usize,
    /// All three are const: they are filled through the local slices `create`
    /// allocates and never written again, which is what makes a shared probe safe.
    heads: []const u32,
    next: []const u32,
    hashes: []const u64,
    mask: u64,

    /// Runs `build` to completion, materializes it columnar, and indexes
    /// `right_keys`. `state` must outlive every probe (plan arena); `pull` is
    /// the transient arena the build child is drained with.
    pub fn create(
        state: std.mem.Allocator,
        pull: std.mem.Allocator,
        build: Op,
        right_schema: *const types.Schema,
        right_keys: []const usize,
        cap: usize,
    ) anyerror!*JoinIndex {
        var bytes: usize = 0;
        const batch = try materializeFull(state, pull, build, right_schema, &bytes, cap);
        const n = batch.len;
        // Rows are addressed as `row + 1` in u32 slots.
        if (n >= std.math.maxInt(u32)) return error.JoinBuildTooLarge;

        const self = try state.create(JoinIndex);
        self.* = .{
            .build_batch = batch,
            .keys = right_keys,
            .heads = &.{},
            .next = &.{},
            .hashes = &.{},
            .mask = 0,
        };
        // A cross join has no keys and is never looked up.
        if (right_keys.len == 0) return self;

        var cap_slots: usize = 16;
        while (cap_slots < n * 2) cap_slots *= 2;
        bytes += cap_slots * @sizeOf(u32) + n * (@sizeOf(u32) + @sizeOf(u64));
        if (bytes > cap) return error.JoinBuildTooLarge;

        const heads = try state.alloc(u32, cap_slots);
        @memset(heads, 0);
        const chain = try state.alloc(u32, n);
        const hashes = try state.alloc(u64, n);
        self.heads = heads;
        self.next = chain;
        self.hashes = hashes;
        self.mask = cap_slots - 1;

        const classes = try pull.alloc(KeyClass, right_keys.len);
        for (right_keys, classes) |k, *c| c.* = classOf(batch.columns[k], batch.columns[k]);

        @memset(chain, 0);
        // Insert in reverse row order: prepending then yields chains in build
        // order, so duplicate-key fan-out preserves the build side's row order
        // (the pre-rewrite behavior scripts may rely on).
        var ri: usize = n;
        while (ri > 0) {
            ri -= 1;
            const r = ri;
            if (anyNullKey(batch.columns, right_keys, r)) continue;
            const h = hashRowKeys(batch.columns, right_keys, r);
            hashes[r] = h;
            var slot = h & self.mask;
            while (heads[slot] != 0) : (slot = (slot + 1) & self.mask) {
                const hr: usize = heads[slot] - 1;
                if (hashes[hr] == h and self.rowsEq(classes, hr, r)) break;
            }
            // Either an empty slot (a key seen for the first time) or the slot
            // this key already owns; both prepend, so a chain only ever holds
            // rows with equal keys.
            chain[r] = heads[slot];
            heads[slot] = @intCast(r + 1);
        }
        return self;
    }

    pub fn rows(self: *const JoinIndex) usize {
        return self.build_batch.len;
    }

    fn rowsEq(self: *const JoinIndex, classes: []const KeyClass, a_row: usize, b_row: usize) bool {
        for (self.keys, classes) |k, cls| {
            const c = &self.build_batch.columns[k];
            if (!cellEq(cls, c, a_row, c, b_row)) return false;
        }
        return true;
    }

    fn probeEq(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, prow: usize, brow: usize) bool {
        for (self.keys, probe_keys, classes) |bk, pk, cls| {
            if (!cellEq(cls, &self.build_batch.columns[bk], brow, &probe.columns[pk], prow)) return false;
        }
        return true;
    }

    /// First build row whose keys equal probe row `row`'s, or null. Walk the
    /// rest with `chainNext`. Read-only, so concurrent probes are safe.
    /// `classes` comes from `classesFor` and is positional with `probe_keys`.
    pub fn find(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, row: usize) ?usize {
        if (self.keys.len == 0 or self.build_batch.len == 0) return null;
        if (anyNullKey(probe.columns, probe_keys, row)) return null;
        const h = hashRowKeys(probe.columns, probe_keys, row);
        var slot = h & self.mask;
        while (self.heads[slot] != 0) : (slot = (slot + 1) & self.mask) {
            const hr: usize = self.heads[slot] - 1;
            if (self.hashes[hr] == h and self.probeEq(probe, probe_keys, classes, row, hr)) return hr;
        }
        return null;
    }

    /// Next build row carrying the same key, or null at the end of the chain.
    pub fn chainNext(self: *const JoinIndex, row: usize) ?usize {
        const nx = self.next[row];
        return if (nx == 0) null else nx - 1;
    }

    /// How each key position compares for this probe batch. Once per batch.
    pub fn classesFor(self: *const JoinIndex, arena: std.mem.Allocator, probe: Batch, probe_keys: []const usize) ![]KeyClass {
        const classes = try arena.alloc(KeyClass, probe_keys.len);
        for (self.keys, probe_keys, classes) |bk, pk, *c|
            c.* = classOf(probe.columns[pk], self.build_batch.columns[bk]);
        return classes;
    }
};

/// Hash join. The build (right) side is drained into a `JoinIndex`; the probe
/// (left) side then streams through it. Supports inner / left / semi / anti /
/// right / full / cross.
///
/// Serial plans set `build` and the index is created on the first `next`;
/// parallel plans create it up front and hand the same one to every lane
/// through `index`. The index and its batch live across pulls, so they MUST NOT
/// go into the per-pull batch arena (the driver resets it before every `next`):
/// they are allocated in `state`, the plan arena.
pub const Join = struct {
    stats: Stats = .{},
    probe: Op,
    /// Serial path: the build pipeline, indexed lazily on first `next`.
    build: ?Op,
    /// Parallel path: an index built once and shared across lanes.
    index: ?*JoinIndex = null,
    left_keys: []const usize,
    right_keys: []const usize,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    state: std.mem.Allocator,
    err: ?*ErrCtx = null,
    /// Per-join build-side byte cap (`WITH (max_build = '8GB')`); null = the
    /// process default `join_build_byte_cap`.
    build_cap: ?usize = null,

    /// right/full: build rows some probe row matched. Lives here rather than in
    /// `JoinIndex` precisely because it is written on the probe path.
    matched: ?[]bool = null,
    drain_pos: usize = 0,
    probe_done: bool = false,

    /// Rows per drain batch, so a large unmatched build side arrives in pieces.
    const drain_chunk = 4096;

    pub fn next(self: *Join, arena: std.mem.Allocator) anyerror!?Batch {
        const ix = try self.ensureIndex(arena);
        while (!self.probe_done) {
            if (try self.probe.next(arena)) |lb| {
                const out = try self.joinBatch(arena, ix, lb);
                if (out.len > 0) return out;
            } else {
                self.probe_done = true;
            }
        }
        return self.drain(arena, ix);
    }

    fn ensureIndex(self: *Join, arena: std.mem.Allocator) anyerror!*JoinIndex {
        const ix = self.index orelse blk: {
            const build = self.build orelse return error.JoinHasNoBuildSide;
            const made = JoinIndex.create(self.state, arena, build, self.right_schema, self.right_keys, self.build_cap orelse join_build_byte_cap) catch |e| {
                if (self.err) |ec| ec.set("{s}", .{errLabel(e)});
                return e;
            };
            self.index = made;
            break :blk made;
        };
        if ((self.kind == .right or self.kind == .full) and self.matched == null) {
            const m = try self.state.alloc(bool, ix.build_batch.len);
            @memset(m, false);
            self.matched = m;
        }
        return ix;
    }

    /// One probe batch → one output batch. Row pairs are collected as index
    /// lists first, then every output column is filled by a single gather.
    fn joinBatch(self: *Join, arena: std.mem.Allocator, ix: *JoinIndex, lb: Batch) anyerror!Batch {
        var lidx = std.array_list.Managed(usize).init(arena);
        var ridx = std.array_list.Managed(usize).init(arena);
        var rnull = std.array_list.Managed(bool).init(arena);
        try lidx.ensureTotalCapacity(lb.len);

        if (self.kind == .cross) {
            var r: usize = 0;
            while (r < lb.len) : (r += 1) {
                var b: usize = 0;
                while (b < ix.build_batch.len) : (b += 1) {
                    try lidx.append(r);
                    try ridx.append(b);
                }
            }
            return self.gatherOut(arena, ix, lb, lidx.items, ridx.items, &.{}, lidx.items.len);
        }

        // Only left/full fill a missing right side; for the others an unmatched
        // probe row either vanishes or carries no right columns at all.
        const fill_right = (self.kind == .left or self.kind == .full);
        const classes = try ix.classesFor(arena, lb, self.left_keys);

        var r: usize = 0;
        while (r < lb.len) : (r += 1) {
            const first = ix.find(lb, self.left_keys, classes, r);
            switch (self.kind) {
                .semi => {
                    if (first != null) try lidx.append(r);
                },
                .anti => {
                    if (first == null) try lidx.append(r);
                },
                else => {
                    if (first == null) {
                        if (fill_right) {
                            try lidx.append(r);
                            try ridx.append(0);
                            try rnull.append(true);
                        }
                        continue;
                    }
                    var cur = first;
                    while (cur) |br| {
                        try lidx.append(r);
                        try ridx.append(br);
                        if (fill_right) try rnull.append(false);
                        if (self.matched) |m| m[br] = true;
                        cur = ix.chainNext(br);
                    }
                },
            }
        }
        return self.gatherOut(arena, ix, lb, lidx.items, ridx.items, rnull.items, lidx.items.len);
    }

    /// right/full: once the probe stream ends, every build row nothing matched
    /// is emitted with the left columns null. Chunked so a large build side does
    /// not become one enormous batch.
    fn drain(self: *Join, arena: std.mem.Allocator, ix: *JoinIndex) anyerror!?Batch {
        if (self.kind != .right and self.kind != .full) return null;
        var ridx = std.array_list.Managed(usize).init(arena);
        const matched = self.matched orelse return null;
        while (self.drain_pos < ix.build_batch.len and ridx.items.len < drain_chunk) : (self.drain_pos += 1) {
            if (!matched[self.drain_pos]) try ridx.append(self.drain_pos);
        }
        if (ridx.items.len == 0) return null;
        return try self.gatherOut(arena, ix, null, &.{}, ridx.items, &.{}, ridx.items.len);
    }

    /// Assemble `n` output rows from the index lists: one gather per column,
    /// never a per-cell boxed append. `lb == null` marks the outer drain, where
    /// the whole left side is null.
    fn gatherOut(
        self: *Join,
        arena: std.mem.Allocator,
        ix: *JoinIndex,
        lb: ?Batch,
        lidx: []const usize,
        ridx: []const usize,
        rnull: []const bool,
        n: usize,
    ) anyerror!Batch {
        const emit_right = (self.kind != .semi and self.kind != .anti);
        const nleft = if (lb) |b| b.columns.len else self.left_schema.fields.len;
        const nout = nleft + (if (emit_right) ix.build_batch.columns.len else @as(usize, 0));
        const cols = try arena.alloc(column.Column, nout);

        var i: usize = 0;
        while (i < nleft) : (i += 1) {
            cols[i] = if (lb) |b|
                try takeCol(arena, b.columns[i], lidx, &.{})
            else
                try nullColumn(arena, self.left_schema.fields[i].ty, n);
        }
        if (emit_right) {
            var k: usize = 0;
            while (k < ix.build_batch.columns.len) : (k += 1) {
                cols[nleft + k] = try takeCol(arena, ix.build_batch.columns[k], ridx, rnull);
            }
        }
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

/// Gather rows `idx` out of `c`, then null out the positions flagged in
/// `null_mask` (the outer-join fill, which gathers a placeholder row and then
/// discards it via validity). `permute` covers every physical store, so there
/// is no per-kind fallback; only a source with no rows at all has to be built
/// as an all-null column instead.
fn takeCol(arena: std.mem.Allocator, c: column.Column, idx: []const usize, null_mask: []const bool) !column.Column {
    if (c.len == 0) return nullColumn(arena, c.ty, idx.len);
    var out = try column.permute(arena, c, idx);
    if (null_mask.len == idx.len) {
        out.ty = out.ty.asNullable();
        for (null_mask, 0..) |is_null, i| {
            if (is_null) out.validity.setValid(i, false);
        }
    }
    return out;
}

/// An all-null column of `n` rows.
fn nullColumn(arena: std.mem.Allocator, ty: types.Type, n: usize) !column.Column {
    var b = try column.Builder.initCapacity(arena, ty.asNullable(), n);
    var i: usize = 0;
    while (i < n) : (i += 1) try b.append(.null);
    return b.finish();
}

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

test "aggregate: a decimal sum normalizes each value's own scale" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // A bare postgres `numeric` has no typmod, so the column is typed
    // decimal(38,6) while each value arrives at whatever dscale it was stored
    // with. Summing the raw unscaled integers scaled the answer by
    // 10^(declared - actual); every addend has to be normalized first.
    const col_ty = types.Type.decimal(38, 6).asNullable();
    const in_schema = types.Schema{ .fields = &.{.{ .name = "n", .ty = col_ty }} };

    var bld = column.Builder.init(a, col_ty);
    try bld.append(.{ .decimal = .{ .unscaled = 15, .scale = 1 } }); // 1.5
    try bld.append(.{ .decimal = .{ .unscaled = 150, .scale = 2 } }); // 1.50
    try bld.append(.{ .decimal = .{ .unscaled = 1, .scale = 3 } }); // 0.001
    try bld.append(.null);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try bld.finish();
    const batches = [_]Batch{.{ .schema = &in_schema, .columns = cols, .len = 4 }};

    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"n"} } };
    const aggs = [_]Aggregate.Agg{.{ .func = .sum, .arg = &fx, .ty = col_ty }};
    const out_schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = col_ty }} };

    var ts = TestSource{ .schema_ = in_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &in_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
    const out = (try agg.next(a)).?;
    const d = out.columns[0].getValue(0).decimal;
    // 1.5 + 1.50 + 0.001 = 3.001, carried at the output scale.
    try testing.expectEqual(@as(u8, 6), d.scale);
    try testing.expectEqual(@as(i128, 3_001_000), d.unscaled);
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

const join_left_schema = types.Schema{ .fields = &.{
    .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
} };
const join_right_schema = types.Schema{ .fields = &.{
    .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
} };
const join_both_schema = types.Schema{ .fields = &.{
    .{ .name = "lk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "lv", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "rk", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "rv", .ty = types.Type.init(.string).asNullable() },
} };

/// Drain a join into (column 0 as ?i64, column `rvc` as ?string) pairs. `rvc`
/// of null collects only the key column (semi/anti, which emit no right side).
const JoinRows = struct {
    keys: std.array_list.Managed(?i64),
    rvs: std.array_list.Managed(?[]const u8),

    fn collect(a: std.mem.Allocator, top: Op, rvc: ?usize) !JoinRows {
        var out = JoinRows{
            .keys = std.array_list.Managed(?i64).init(a),
            .rvs = std.array_list.Managed(?[]const u8).init(a),
        };
        while (try top.next(a)) |b| {
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                const kv = b.columns[0].getValue(r);
                try out.keys.append(if (kv.isNull()) null else kv.int);
                if (rvc) |c| {
                    const rv = b.columns[c].getValue(r);
                    try out.rvs.append(if (rv.isNull()) null else rv.string);
                }
            }
        }
        return out;
    }

    fn expect(self: JoinRows, keys: []const ?i64, rvs: []const ?[]const u8) !void {
        try testing.expectEqualDeep(keys, @as([]const ?i64, self.keys.items));
        try testing.expectEqual(rvs.len, self.rvs.items.len);
        for (rvs, self.rvs.items) |w, g| {
            if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
        }
    }
};

test "join: inner/left/semi/anti; null keys never match, duplicate build keys fan out" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { kind: ast.JoinKind, keys: []const ?i64, rvs: []const ?[]const u8 };
    const cases = [_]Case{
        .{ .kind = .inner, .keys = &.{ 1, 1 }, .rvs = &.{ "x", "y" } },
        .{ .kind = .left, .keys = &.{ 1, 1, 2, null, 3 }, .rvs = &.{ "x", "y", null, null, null } },
        .{ .kind = .semi, .keys = &.{1}, .rvs = &.{} },
        .{ .kind = .anti, .keys = &.{ 2, null, 3 }, .rvs = &.{} },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 1, 4, null }, &.{ "x", "y", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        const emit_right = case.kind == .inner or case.kind == .left;
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = if (emit_right) &join_both_schema else &join_left_schema,
            .kind = case.kind,
            .state = a,
        };
        // Reverse-order insertion keeps duplicate chains in build order.
        const got = try JoinRows.collect(a, .{ .join = &jn }, if (emit_right) @as(?usize, 3) else null);
        try got.expect(case.keys, case.rvs);
    }
}

test "join: right and full drain unmatched build rows with a null left side" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    for ([_]ast.JoinKind{ .right, .full }) |kind| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null }, &.{ "a", "b", "n" })};
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 4, null }, &.{ "x", "z", "m" })};
        var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
        var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
        var lscan = Scan{ .src = lts.src() };
        var rscan = Scan{ .src = rts.src() };
        var jn = Join{
            .probe = .{ .scan = &lscan },
            .build = .{ .scan = &rscan },
            .left_keys = &.{0},
            .right_keys = &.{0},
            .left_schema = &join_left_schema,
            .right_schema = &join_right_schema,
            .out_schema = &join_both_schema,
            .kind = kind,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
        // The drain carries build rows 4 and null (nothing matched them), with
        // the left key column null. `full` additionally keeps left rows 2/null.
        if (kind == .right) {
            try got.expect(&.{ 1, null, null }, &.{ "x", "z", "m" });
        } else {
            try got.expect(&.{ 1, 2, null, null, null }, &.{ "x", null, null, "z", "m" });
        }
    }
}

test "join: multi-key ON (int + string) pairs only fully equal keys" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 1, 2, 3 }, &.{ "a", "b", "a", null })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 2, 1 }, &.{ "a", "z", "a" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{ 0, 1 },
        .right_keys = &.{ 0, 1 },
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .inner,
        .state = a,
    };
    // Only (1,"a") matches, and it matches both build rows carrying that key.
    const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
    try got.expect(&.{ 1, 1 }, &.{ "a", "a" });
}

test "join: cross pairs every probe row with every build row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2 }, &.{ "a", "b" })};
    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 7, 8, 9 }, &.{ "x", "y", "z" })};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{},
        .right_keys = &.{},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .cross,
        .state = a,
    };
    const got = try JoinRows.collect(a, .{ .join = &jn }, 3);
    try got.expect(&.{ 1, 1, 1, 2, 2, 2 }, &.{ "x", "y", "z", "x", "y", "z" });
}

test "join: the build-size guard reports instead of exhausting memory" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const rb = [_]Batch{try kvBatch(a, &join_right_schema, &.{ 1, 2, 3 }, &.{ "x", "y", "z" })};
    const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{1}, &.{"a"})};
    var lts = TestSource{ .schema_ = join_left_schema, .batches = &lb };
    var rts = TestSource{ .schema_ = join_right_schema, .batches = &rb };
    var lscan = Scan{ .src = lts.src() };
    var rscan = Scan{ .src = rts.src() };
    var ec = ErrCtx{};
    var jn = Join{
        .probe = .{ .scan = &lscan },
        .build = .{ .scan = &rscan },
        .left_keys = &.{0},
        .right_keys = &.{0},
        .left_schema = &join_left_schema,
        .right_schema = &join_right_schema,
        .out_schema = &join_both_schema,
        .kind = .inner,
        .state = a,
        .err = &ec,
    };
    const saved = join_build_byte_cap;
    join_build_byte_cap = 8;
    defer join_build_byte_cap = saved;
    const top = Op{ .join = &jn };
    try testing.expectError(error.JoinBuildTooLarge, top.next(a));
    try testing.expect(std.mem.indexOf(u8, ec.msg, "exceeds its cap") != null);
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
