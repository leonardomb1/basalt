//! Streaming pull operators. Each `next(arena)` returns the next batch or null.
//! Operators form a closed set (a tagged union) dispatched once per batch, the cold
//! boundary; per-row work happens in the columnar kernels in `eval.zig`. The scan
//! reads through the abstract `Source` driver seam. Filter, project and explode are
//! also stateless `Stage`s that `linearize` hands to the parallel driver; breakers
//! and limit are order- or state-sensitive and stay serial. Each op's `Stats` are
//! filled as the pipeline is pulled, so a plan prints with actuals beside estimates.
//!
//! Memory: the driver resets the per-pull arena before every `next`, never during
//! one. Anything that lives across pulls (seen-sets, group tables, join indexes,
//! copied string keys) goes in `state`, the plan arena, or in `gpa`. A filter pulls
//! into its own scratch, reset per batch, because a selective predicate can drain
//! the whole source inside one call; that scratch is backed by `back`, the plan
//! arena, since nothing destroys a `Filter` and a page-allocator scratch would
//! orphan its pages (a `FOR EACH` mints a plan per row). Breakers (sort, window,
//! materialize) hold their whole input.
//!
//! `ErrCtx` gives a runtime expression error its stage and column. It keeps an
//! inline buffer so the message outlives the batch arena, is first-wins under a
//! mutex so concurrent lanes report deterministically, and cuts a long message
//! short rather than dropping it.
//!
//! Distinct dedups against a seen-set, O(distinct keys). One fixed-width key dedups
//! on its raw 64-bit word (not floats, which have two zeros). With `track_ords` it
//! reports each surviving row's input ordinal, so parallel lanes that dedup per
//! chunk merge to the same row on every run.
//!
//! Sort lifts each key once into a typed `KeyArr` and packs every row's
//! order-preserving u64 words with its row number into one record, which an MSD
//! byte radix orders, stably, skipping the bytes a bucket shares. Ints and floats are one word, decimals two (scaled to the widest scale
//! present, since postgres NUMERIC carries a per-value scale and raw unscaled ints
//! sorted 0.5 after 0.10), a string up to 31 bytes is its zero-padded bytes with
//! the length in the last byte, and a longer one is a 24-byte prefix whose ties the
//! comparator settles. Nulls sort first or last as each key says (by default as
//! larger than every value: last ascending, first DESC), NaN sorts last and -0.0
//! equals 0.0, as `eval.orderF64`. A comparator sort of 10M strings took 22s. The
//! parallel sort splits rows into ranges on the first key's leading bits, a few
//! per thread handed out as threads free up (`par`), and gives exactly the serial
//! result.
//!
//! TopN fuses `sort | limit N [offset M]` into the top-k core (`topk.zig`) keeping
//! M+N rows, up to `max_rows` (past it a radix sort is faster). Ties rank by input
//! position (`seq_base`, and the row group `item` under a stealing source), so the
//! rows kept are a stable sort's whatever lanes produced them; lanes' rows combine
//! through one more core, at their positions. A row not ranking before the kept
//! bound is rejected off its batch's key column before any copy, and that bound can
//! be published as a `threshold` so a source skips row groups.
//!
//! Window is a breaker: one sort over partition ++ order keys, then one pass per
//! function. Nulls in a partition key group together, as in GROUP BY. The default
//! frame is the peer-based RANGE frame (ties share a value); any ROWS or RANGE frame
//! resolves to two forward-moving edges per row, and an aggregate slides over them,
//! adding and taking back, with a monotonic deque for MIN/MAX (re-walking the frame
//! was O(partition^2)); MEDIAN and BIT_AND/OR recompute a frame that does not only
//! grow. A sliding float frame can drift by rounding; a segment tree is the exact
//! upgrade. Under `WHERE rn <= k` a lone ROW_NUMBER (`top_k`, k up to `max_top_k`)
//! keeps only each partition's best k rows, through the same top-k core. Window SUM skips non-numeric kinds like nulls, and sums
//! as the SUM aggregate does, range-checking only the frame's total.
//!
//! Aggregate is streaming hash aggregation, O(groups). Groups are typed records in
//! blocks: `FixedStore` (raw i64 words plus a null mask) when every key is
//! fixed-width, `GroupStore` (boxed values) otherwise. Keys are followed by each
//! aggregate's state as laid out by `Layout`, so a COUNT pays eight bytes and a SUM
//! its own, and the hash leads the record, so a table grows without rehashing. `GroupTable` slots are one u32 (hash salt plus index), sixteen to a
//! cache line, since at high cardinality the lines a probe touches are the cost;
//! a batch is hashed first and probed `prefetch_ahead` rows behind a prefetch.
//! Groups split over `fold_parts` radix partitions by the hash's top bits (the
//! bucket uses the bottom bits, the salt bits 32-37). A single small-range int key
//! skips hashing through a `Direct` array. String keys are interned ids in a
//! `StrTable` shared by the lanes, but placement hashes the string itself so output
//! order does not follow thread timing. Parallel lanes fold partial `GroupSet`s that
//! `GroupMerge` combines by their raw accumulators; `emitSets` finalizes once.
//!
//! Each aggregate is its math alone (`aggregate/funcs.zig`: a state and its add,
//! merge and finish), and `aggregate/kernel.zig` generates every loop around it —
//! a batch with no nulls, one with, a batch's rows into their groups, a lane's
//! states into another's — once per physical type, chosen per aggregate at plan
//! time. Sums are exact, i128 for ints and i256 for DECIMAL, range-checked once at
//! the end, so the answer cannot depend on how rows split across batches and lanes
//! or on their order (it once came out 5 at -j 1 and an overflow at -j 8). DECIMAL
//! addends are normalized to the output scale. Variances merge with Chan's formula
//! in fixed lane order, reproducible for a given `-j`, like a float SUM. Text
//! arguments are cast before the state (`argCast`); without it SUM panicked on a
//! text cell and AVG returned 0.
//!
//! Join is a hash join: the build (right) side is drained into a `JoinIndex` and
//! the probe (left) side streams through it. The index is three flat arrays (bucket
//! heads, per-row duplicate chains, per-row hashes), read-only after `create`, so
//! parallel lanes share one; the outer-join `matched` flags are written on the
//! probe path and live on the `Join`. Serial plans build it on the first `next`,
//! parallel plans up front. With the run's scratch space, a build side past
//! `--op-memory` (or the join's `max_build`) spills: both sides split by key hash
//! into partition files joined one at a time (`join.zig`). Without one, or under
//! `NOT IN`, it is capped at `join_build_byte_cap` so an oversized one is
//! `JoinBuildTooLarge` rather than an OOM. Sort and GROUP BY spill the same way
//! (`spill.zig` holds the file format).
//! A null key joins nothing, and a null-aware (`NOT IN`) anti join keeps nothing
//! against a build side holding a null. Duplicate keys fan out in build order.
//! A join whose ON has no key is an `NLJoin` (`nljoin.zig`): the build side held
//! in memory under the same cap, never spilled, each probe row paired with its
//! rows and the ON evaluated over the pairs.
//!
//! The operators with more than a page of logic each live in op/: `aggregate.zig`,
//! `join.zig`, `nljoin.zig`, `sort.zig`, `topk.zig`, `topn.zig`, `window.zig`, `distinct.zig` and
//! `explode.zig`. This file keeps the `Op` union and its dispatch, the simple
//! operators (scan, filter, project, limit, union) and the error labels. Each part
//! carries the tests of its own code; `testing_util.zig` holds the shared helpers.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const aggregates = @import("../lang/aggregates.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
pub const Batch = @import("batch.zig").Batch;
const eval = @import("eval.zig");
pub const Value = @import("value.zig").Value;
const driver = @import("../connect/driver.zig");
const TestSource = @import("op/testing_util.zig").TestSource;
const drainInts = @import("op/testing_util.zig").drainInts;
const intBatch = @import("op/testing_util.zig").intBatch;
const int_schema = @import("op/testing_util.zig").int_schema;

pub const ErrCtx = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    mutex: std.Thread.Mutex = .{},

    pub fn set(self: *ErrCtx, comptime fmt: []const u8, args: anytype) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.msg.len > 0) return;
        self.msg = std.fmt.bufPrint(&self.buf, fmt, args) catch blk: {
            @memcpy(self.buf[self.buf.len - 3 ..], "...");
            break :blk self.buf[0..];
        };
    }
};

/// The label for an evaluation error, or the builtin's own account of it when it left
/// one (`eval.takeFailure`), e.g. `strptime: '31/02/2026' is not a date in '%d/%m/%Y'`.
pub fn failLabel(e: anyerror) []const u8 {
    return eval.takeFailure(e) orelse errLabel(e);
}

pub fn errLabel(e: anyerror) []const u8 {
    return switch (e) {
        error.CastFailed => "cast failed",
        error.DivByZero => "division by zero",
        error.TypeMismatch => "type mismatch",
        error.IntOverflow => "integer overflow — the value does not fit a 64-bit integer",
        error.JsonNotArray => "JSON_EACH needs a JSON array (got an object or a scalar)",
        error.InvalidJson => "invalid JSON — json_get and JSON_EACH need a JSON document, and the json_* array functions a JSON array",
        error.PatternTooComplex => "regular expression gave up: too much backtracking, or a group repeated more than about 500,000 times in one match — replace a nested quantifier like `(a+)+` with a flat one, and a repeated group with a class or a single quantified character (`(?:a|b)+` as `[ab]+`)",
        error.CsvHeaderMismatch => "a file in the folder has another header than the first — a folder read takes CSVs of one layout",
        error.InterpFailed => "a `${...}` interpolation failed",
        error.JoinBuildTooLarge => "join build side exceeds its cap — raise it with WITH (max_build = '8GB') on the join, filter the CTE, or flip the join",
        error.SpillCapExceeded => "spilling to disk passed the run's cap — raise it with --spill-cap, give operators more memory with --op-memory, or filter earlier",
        error.CorruptSpill => "a spill file read back corrupt — the scratch directory was changed or the disk failed during the run",
        else => @errorName(e),
    };
}

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
    window: *Window,
    aggregate: *Aggregate,
    top_n: *TopN,
    join: *Join,
    nl_join: *NLJoin,
    explode: *Explode,
    union_: *Union,

    pub fn stats(self: Op) *Stats {
        return switch (self) {
            inline else => |o| &o.stats,
        };
    }

    /// This operator's inputs, written into `buf`, used to turn inclusive timings into
    /// exclusive ones.
    pub fn inputs(self: Op, buf: *std.array_list.Managed(Op)) !void {
        switch (self) {
            .scan => {},
            .join => |j| {
                try buf.append(j.probe);
                if (j.build) |b| try buf.append(b);
            },
            .nl_join => |j| {
                try buf.append(j.probe);
                if (j.build) |b| try buf.append(b);
            },
            .union_ => |u| try buf.appendSlice(u.children),
            inline else => |o| try buf.append(o.child),
        }
    }

    /// Frees what spilling operators in this tree still hold (files, their mappings,
    /// their disk charge and memory) once the consumer is done pulling, whether the
    /// tree ran to its end or not. Idempotent.
    pub fn release(self: Op) void {
        switch (self) {
            .scan => {},
            .sort => |s| {
                s.release();
                s.child.release();
            },
            .join => |j| {
                j.release();
                j.probe.release();
                if (j.build) |b| b.release();
            },
            .nl_join => |j| {
                j.probe.release();
                if (j.build) |b| b.release();
            },
            .union_ => |u| for (u.children) |c| c.release(),
            .distinct => |d| {
                d.release();
                d.child.release();
            },
            .aggregate => |g| {
                g.release();
                g.child.release();
            },
            inline else => |o| o.child.release(),
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
            .window => |w| w.next(arena),
            .aggregate => |a| a.next(arena),
            .top_n => |t| t.next(arena),
            .join => |j| j.next(arena),
            .nl_join => |j| j.next(arena),
            .explode => |e| e.next(arena),
            .union_ => |u| u.next(arena),
        };
    }
};

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

/// Decompose a map-only pipeline (scan, then filter/project/explode) into a source
/// and ordered stages for the parallel driver; null for anything else.
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

pub const Explode = @import("op/explode.zig").Explode;
pub const Window = @import("op/window.zig").Window;
pub const Distinct = @import("op/distinct.zig").Distinct;
pub const Sort = @import("op/sort.zig").Sort;
const KeyArr = @import("op/sort.zig").KeyArr;
pub const TopN = @import("op/topn.zig").TopN;
pub const keyOrder = @import("op/sort.zig").keyOrder;
pub const Aggregate = @import("op/aggregate.zig").Aggregate;
pub const dupeValue = @import("op/aggregate.zig").dupeValue;
pub var join_build_byte_cap: usize = 4 << 30;
pub const KeyClass = @import("op/join.zig").KeyClass;
pub const JoinIndex = @import("op/join.zig").JoinIndex;
pub const Join = @import("op/join.zig").Join;
pub const notInTooLarge = @import("op/join.zig").notInTooLarge;
pub const NLJoin = @import("op/nljoin.zig").NLJoin;
pub const nlRangeOf = @import("op/nljoin.zig").rangeOf;
pub const KeyValues = @import("op/keyset.zig").KeyValues;
pub const KeyPush = @import("op/keyset.zig").KeyPush;
pub const collectKeys = @import("op/keyset.zig").collect;
pub const default_prefetch_rows = @import("op/join.zig").default_prefetch_rows;
pub const default_prefetch_bytes = @import("op/join.zig").default_prefetch_bytes;
pub const default_push_cap = @import("op/join.zig").default_push_cap;

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
    back: std.mem.Allocator,
    scratch: ?std.heap.ArenaAllocator = null,

    pub fn next(self: *Filter, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.scratch == null) self.scratch = std.heap.ArenaAllocator.init(self.back);
        while (true) {
            _ = self.scratch.?.reset(.retain_capacity);
            const b = (try self.child.next(self.scratch.?.allocator())) orelse return null;
            const out = try self.filterInto(arena, self.scratch.?.allocator(), b);
            if (out.len > 0) return out;
        }
    }

    pub fn transform(self: *Filter, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return self.filterInto(arena, arena, b);
    }

    fn filterInto(self: *Filter, out: std.mem.Allocator, scratch: std.mem.Allocator, b: Batch) anyerror!Batch {
        return applyFilter(out, scratch, b, self.pred) catch |e| {
            if (self.err) |ec| ec.set("{s}: in filter predicate", .{failLabel(e)});
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

    pub const Col = struct {
        source: union(enum) { passthrough: usize, expr: *const ast.Expr },
        ty: types.Type,
    };

    pub fn next(self: *Project, arena: std.mem.Allocator) anyerror!?Batch {
        const b = (try self.child.next(arena)) orelse return null;
        return try self.transform(arena, b);
    }

    pub fn transform(self: *Project, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        const outcols = try arena.alloc(column.Column, self.cols.len);
        for (self.cols, 0..) |c, i| {
            outcols[i] = switch (c.source) {
                .passthrough => |idx| b.columns[idx],
                .expr => |e| eval.evalColumn(arena, e, b, c.ty) catch |err| {
                    if (self.err) |ec| ec.set("{s}: computing column `{s}` in select", .{ failLabel(err), self.out_schema.fields[i].name });
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

pub fn sliceBatch(arena: std.mem.Allocator, b: Batch, start: usize, take: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, 0..) |*col, ci| {
        var bld = column.Builder.init(arena, col.ty);
        var r: usize = start;
        while (r < start + take) : (r += 1) try bld.append(col.getValue(r));
        outcols[ci] = try bld.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = take };
}

/// Drain `child` into one batch, or null for empty input. All chunks live in this
/// one `next`'s arena, so typed buffers are concatenated without boxing or re-duping.
pub fn materializeAll(arena: std.mem.Allocator, child: Op, schema: *const types.Schema) anyerror!?Batch {
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

const testing = std.testing;

test "limit skips offset rows across batch boundaries and stops at count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 1, 2, 3 }),
        try intBatch(a, &int_schema, &.{ 4, 5, 6 }),
        try intBatch(a, &int_schema, &.{ 7, 8, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var lim = Limit{ .child = .{ .scan = &scan }, .remaining = 3, .to_skip = 4 };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 5, 6, 7 }), try drainInts(a, .{ .limit = &lim }));
    try testing.expectEqual(@as(usize, 3), ts.idx);

    var ts2 = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan2 = Scan{ .src = ts2.src() };
    var lim2 = Limit{ .child = .{ .scan = &scan2 }, .remaining = 3, .to_skip = 1 };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 2, 3, 4 }), try drainInts(a, .{ .limit = &lim2 }));
    try testing.expectEqual(@as(usize, 2), ts2.idx);
    try testing.expect((try lim2.next(a)) == null);
    try testing.expectEqual(@as(usize, 2), ts2.idx);
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
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred, .back = a };
    try testing.expectEqualDeep(@as([]const ?i64, &.{ 5, 3 }), try drainInts(a, .{ .filter = &flt }));
}

test "filter scratch is backed by the plan arena, so it dies with the plan" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{try intBatch(a, &int_schema, &.{ 1, 5, 3 })};
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };

    var fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    var two = ast.Expr{ .int_lit = 2 };
    var pred = ast.Expr{ .binary = .{ .op = .gt, .l = &fx, .r = &two } };
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred, .back = a };
    _ = try drainInts(a, .{ .filter = &flt });

    const child = flt.scratch.?.child_allocator;
    try testing.expectEqual(a.ptr, child.ptr);
    try testing.expectEqual(a.vtable, child.vtable);
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
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred, .err = &ec, .back = a };
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

test {
    _ = @import("op/aggregate.zig");
    _ = @import("op/distinct.zig");
    _ = @import("op/explode.zig");
    _ = @import("op/join.zig");
    _ = @import("op/keyset.zig");
    _ = @import("op/nljoin.zig");
    _ = @import("op/sort.zig");
    _ = @import("op/topk.zig");
    _ = @import("op/topn.zig");
    _ = @import("op/window.zig");
    _ = @import("op/testing_util.zig");
    _ = @import("spill.zig");
    _ = @import("space.zig");
}
