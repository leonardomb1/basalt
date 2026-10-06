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
//! Sort lifts each key once into a typed `KeyArr`, then LSD-radix sorts the rows,
//! stably, on order-preserving u64 words in 11-bit digits over only the range the
//! keys use. Ints and floats are one word, decimals two (scaled to the widest scale
//! present, since postgres NUMERIC carries a per-value scale and raw unscaled ints
//! sorted 0.5 after 0.10), a string up to 31 bytes is its zero-padded bytes with
//! the length in the last byte, and a longer one is a 24-byte prefix whose ties the
//! comparator settles. Nulls sort last in either direction, NaN sorts last and -0.0
//! equals 0.0, as `eval.orderF64`. A comparator sort of 10M strings took 22s. The
//! parallel sort splits rows into ranges on the first key's leading bits, one range
//! per thread, and gives exactly the serial result.
//!
//! TopN fuses `sort | limit N [offset M]` into a bounded heap of M+N rows copied
//! into `gpa`, up to `max_rows` (past it a radix sort is faster). Ties rank by input
//! position (`seq_base`, and the row group `item` under a stealing source), so the
//! rows kept are a stable sort's whatever lanes produced them. A row strictly worse
//! than the worst kept on its first key is rejected off the typed column without
//! boxing, and that bound can be published as a `threshold` so a source skips row
//! groups.
//!
//! Window is a breaker: one sort over partition ++ order keys, then one numbering
//! pass. Nulls in a partition key group together, as in GROUP BY. The default frame
//! is the peer-based RANGE frame (ties share a value); a ROWS frame slides over
//! positions, adding and subtracting, with a monotonic deque for MIN/MAX (re-walking
//! the frame was O(partition^2)). A bounded float ROWS frame can drift by rounding;
//! a segment tree is the exact upgrade. Under `WHERE rn <= k` a lone ROW_NUMBER
//! (`top_k`) keeps only each partition's best k rows. Window SUM skips non-numeric
//! kinds like nulls, and sums in i128 with a range check, as it once wrapped.
//!
//! Aggregate is streaming hash aggregation, O(groups). Groups are typed records in
//! blocks: `FixedStore` (raw i64 words plus a null mask) when every key is
//! fixed-width, `GroupStore` (boxed values) otherwise. Keys are followed by each
//! aggregate's `Slot` as laid out by `Layout`, so a SUM pays its own bytes rather
//! than an 80-byte `Acc`, and the hash leads the record, so a table grows without
//! rehashing. `GroupTable` slots are one u32 (hash salt plus index), sixteen to a
//! cache line, since at high cardinality the lines a probe touches are the cost;
//! a batch is hashed first and probed `prefetch_ahead` rows behind a prefetch.
//! Groups split over `fold_parts` radix partitions by the hash's top bits (the
//! bucket uses the bottom bits, the salt bits 32-37). A single small-range int key
//! skips hashing through a `Direct` array. String keys are interned ids in a
//! `StrTable` shared by the lanes, but placement hashes the string itself so output
//! order does not follow thread timing. Parallel lanes fold partial `GroupSet`s that
//! `GroupMerge` combines by their raw accumulators; `emitSets` finalizes once.
//!
//! `Acc` fields are shared between aggregates: `n` counts rows, `sum_i`/`sum_f`
//! hold sums, `ext` the extreme. Bitwise aggregates keep their bits in `sum_i`,
//! with `n == 0` meaning none yet since zero is not AND's identity; variances keep
//! Welford's mean in `sum_f` and squared deviations in `ext`, keeping `Acc` at 80
//! bytes. Sums are exact i128, range-checked once in `finalizeAcc`, so the answer
//! cannot depend on how rows split across batches and lanes (it once came out 5 at
//! -j 1 and an overflow at -j 8). DECIMAL addends are normalized to the output
//! scale. Variances merge with Chan's formula in fixed lane order, reproducible for
//! a given `-j`, like a float SUM. Text arguments are cast before the accumulator
//! (`argCast`); without it SUM panicked on a text cell and AVG returned 0.
//!
//! Join is a hash join: the build (right) side is drained into a `JoinIndex` and
//! the probe (left) side streams through it. The index is three flat arrays (bucket
//! heads, per-row duplicate chains, per-row hashes), read-only after `create`, so
//! parallel lanes share one; the outer-join `matched` flags are written on the
//! probe path and live on the `Join`. Serial plans build it on the first `next`,
//! parallel plans up front. The build side is fully resident and capped at
//! `join_build_byte_cap` (overridable per join with `WITH (max_build = '16GB')`) so
//! an oversized one is `JoinBuildTooLarge` rather than an OOM; there is no spill.
//! A null key joins nothing, and a null-aware (`NOT IN`) anti join keeps nothing
//! against a build side holding a null. Duplicate keys fan out in build order.

const std = @import("std");
const ast = @import("../lang/ast.zig");
const aggregates = @import("../lang/aggregates.zig");
const json = @import("json.zig");
const types = @import("../lang/types.zig");
const column = @import("column.zig");
const Batch = @import("batch.zig").Batch;
const eval = @import("eval.zig");
const simd = @import("simd.zig");
const Decimal = @import("value.zig").Decimal;
const Threshold = @import("value.zig").Threshold;
const Value = @import("value.zig").Value;
const keyhash = @import("keyhash.zig");
const driver = @import("../connect/driver.zig");

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
        error.PatternTooComplex => "regular expression gave up: too much backtracking — anchor it, or replace a nested quantifier like `(a+)+` with a flat one",
        error.CsvHeaderMismatch => "a file in the folder has another header than the first — a folder read takes CSVs of one layout",
        error.JoinBuildTooLarge => "join build side exceeds its cap — raise it with WITH (max_build = '8GB') on the join, filter the CTE, or flip the join",
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
            .window => |w| w.next(arena),
            .aggregate => |a| a.next(arena),
            .top_n => |t| t.next(arena),
            .join => |j| j.next(arena),
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

pub const Explode = struct {
    stats: Stats = .{},
    child: Op,
    field_idx: usize,
    delim: []const u8,
    json: bool = false,
    out_schema: *const types.Schema,

    pub fn next(self: *Explode, arena: std.mem.Allocator) anyerror!?Batch {
        while (try self.child.next(arena)) |b| {
            const out = try self.explodeBatch(arena, b);
            if (out.len > 0) return out;
        }
        return null;
    }

    pub fn transform(self: *Explode, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return self.explodeBatch(arena, b);
    }

    /// Split a delimited string column, or with `json` a JSON array, into one row per
    /// element, other columns repeated. Null or missing cells produce zero rows.
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
            if (self.json) {
                for (try jsonElems(arena, s)) |elem| {
                    for (b.columns, 0..) |*c, ci| {
                        try builders[ci].append(if (ci == self.field_idx) elem else c.getValue(r));
                    }
                    n += 1;
                }
                continue;
            }
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

/// The elements of the JSON array `text` as cells; a JSON null is no elements.
fn jsonElems(arena: std.mem.Allocator, text: []const u8) ![]const Value {
    try json.validate(arena, text);
    switch (json.rootKind(text)) {
        .array => {},
        .null => return &.{},
        .other => return error.JsonNotArray,
    }
    var out = std.array_list.Managed(Value).init(arena);
    var it = json.Elements.root(text);
    while (it.next()) |raw| try out.append(switch (try json.cell(arena, raw)) {
        .null => .null,
        .text => |t| .{ .string = t },
    });
    return out.items;
}

fn asF64Opt(v: Value) ?f64 {
    return switch (v) {
        .int => |x| @floatFromInt(x),
        .float => |x| x,
        .decimal => |d| blk: {
            var scale: f64 = 1;
            var i: u8 = 0;
            while (i < d.scale) : (i += 1) scale *= 10;
            break :blk @as(f64, @floatFromInt(d.unscaled)) / scale;
        },
        .bool => |b| if (b) 1 else 0,
        else => null,
    };
}

pub const Window = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    out_schema: *const types.Schema,
    part: []const Sort.Key,
    ord: []const Sort.Key,
    funcs: []const Func,
    done: bool = false,
    err: ?*ErrCtx = null,
    top_k: ?u64 = null,
    gpa: ?std.mem.Allocator = null,
    threads: usize = 1,

    pub const Frame = struct { rows: bool = false, unbounded: bool = false, preceding: i64 = 0 };

    pub const Kind = enum { row_number, rank, dense_rank, lag, lead, sum, count, min, max, avg };
    pub const Func = struct { kind: Kind, arg: ?usize = null, offset: i64 = 1, frame: Frame = .{} };

    fn sameOn(arrs: []const KeyArr, a: usize, b: usize) bool {
        for (arrs) |k| {
            if (k.order(a, b) != .eq) return false;
        }
        return true;
    }

    const Kept = std.ArrayListUnmanaged([]Value);
    const Parts = std.HashMap([]const Value, *Kept, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);

    fn ranksBefore(self: *const Window, a: []const Value, b: []const Value) bool {
        for (self.ord) |k| {
            const o = keyOrder(a[k.idx], b[k.idx], k.desc);
            if (o != .eq) return o == .lt;
        }
        return false;
    }

    /// ROW_NUMBER up to rank `k`: per partition, the best `k` rows so far, best first.
    /// A newcomer goes after every row it does not rank before, so ties keep input
    /// order as the stable sort would, and one landing past `k` is never copied.
    fn nextTopK(self: *Window, arena: std.mem.Allocator, k: u64, gpa: std.mem.Allocator) anyerror!?Batch {
        var parts = Parts.init(gpa);
        defer {
            var it = parts.iterator();
            while (it.next()) |e| {
                freeRowGpa(gpa, @constCast(e.key_ptr.*));
                for (e.value_ptr.*.items) |r| freeRowGpa(gpa, r);
                e.value_ptr.*.deinit(gpa);
                gpa.destroy(e.value_ptr.*);
            }
            parts.deinit();
        }
        var pull = std.heap.ArenaAllocator.init(gpa);
        defer pull.deinit();
        const ncols = self.in_schema.fields.len;
        const row = try arena.alloc(Value, ncols);
        const key = try arena.alloc(Value, self.part.len);

        if (k > 0) while (try self.child.next(pull.allocator())) |b| {
            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                for (row, b.columns) |*v, c| v.* = c.getValue(r);
                for (key, self.part) |*v, pk| v.* = row[pk.idx];
                const gop = try parts.getOrPut(key);
                if (!gop.found_existing) {
                    gop.key_ptr.* = try dupeRowGpa(gpa, key);
                    gop.value_ptr.* = try gpa.create(Kept);
                    gop.value_ptr.*.* = .{};
                }
                const kept = gop.value_ptr.*;
                var lo: usize = 0;
                var hi: usize = kept.items.len;
                while (lo < hi) {
                    const mid = (lo + hi) / 2;
                    if (self.ranksBefore(row, kept.items[mid])) hi = mid else lo = mid + 1;
                }
                if (lo >= k) continue;
                try kept.insert(gpa, lo, try dupeRowGpa(gpa, row));
                if (kept.items.len > k) freeRowGpa(gpa, kept.pop().?);
            }
            _ = pull.reset(.retain_capacity);
        };

        const order = try arena.alloc(Parts.Entry, parts.count());
        var it = parts.iterator();
        var n: usize = 0;
        var total: usize = 0;
        while (it.next()) |e| : (n += 1) {
            order[n] = e;
            total += e.value_ptr.*.items.len;
        }
        if (total == 0) return null;
        std.mem.sort(Parts.Entry, order, {}, struct {
            fn lt(_: void, a: Parts.Entry, b: Parts.Entry) bool {
                for (a.key_ptr.*, b.key_ptr.*) |x, y| {
                    const o = keyOrder(x, y, false);
                    if (o != .eq) return o == .lt;
                }
                return false;
            }
        }.lt);

        const builders = try arena.alloc(column.Builder, self.out_schema.fields.len);
        for (builders, self.out_schema.fields) |*bd, f| bd.* = try column.Builder.initCapacity(arena, f.ty, total);
        for (order) |e| {
            for (e.value_ptr.*.items, 1..) |kr, rn| {
                for (kr, builders[0..ncols]) |v, *bd| try bd.append(v);
                try builders[ncols].append(.{ .int = @intCast(rn) });
            }
        }
        const cols = try arena.alloc(column.Column, builders.len);
        for (builders, cols) |*bd, *c| c.* = try bd.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = total };
    }

    fn intSum(self: *Window, acc: i128) error{IntOverflow}!Value {
        const v = std.math.cast(i64, acc) orelse {
            if (self.err) |ec| ec.set("{s}: in window SUM", .{errLabel(error.IntOverflow)});
            return error.IntOverflow;
        };
        return .{ .int = v };
    }

    pub fn next(self: *Window, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        if (self.top_k) |k| return self.nextTopK(arena, k, self.gpa.?);
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;

        const idx = try arena.alloc(usize, all.len);
        for (idx, 0..) |*x, i| x.* = i;

        const arrs = try arena.alloc(KeyArr, self.part.len + self.ord.len);
        for (self.part, 0..) |k, i| arrs[i] = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        for (self.ord, 0..) |k, i| arrs[self.part.len + i] = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        try sortIdxThreads(arena, idx, arrs, self.threads);
        const parts = arrs[0..self.part.len];
        const ords = arrs[self.part.len..];

        var bounds_needed = false;
        for (self.funcs) |f| switch (f.kind) {
            .row_number, .rank, .dense_rank => {},
            else => bounds_needed = true,
        };
        const nb = if (bounds_needed) all.len else 0;
        const pstart = try arena.alloc(u32, nb);
        const pend = try arena.alloc(u32, nb);
        if (bounds_needed) {
            var i: usize = 0;
            while (i < idx.len) {
                var j = i + 1;
                while (j < idx.len and sameOn(parts, idx[j - 1], idx[j])) j += 1;
                var k = i;
                while (k < j) : (k += 1) {
                    pstart[k] = @intCast(i);
                    pend[k] = @intCast(j);
                }
                i = j;
            }
        }

        const ncols = all.columns.len;
        const builders = try arena.alloc(column.Builder, self.funcs.len);
        for (builders, self.out_schema.fields[ncols..]) |*bd, f| bd.* = try column.Builder.initCapacity(arena, f.ty, all.len);

        const vals = try arena.alloc([]Value, self.funcs.len);
        for (self.funcs, vals, builders) |f, *out, *bd| {
            const ranking = switch (f.kind) {
                .row_number, .rank, .dense_rank => true,
                else => false,
            };
            out.* = if (ranking) &.{} else try arena.alloc(Value, all.len);
            switch (f.kind) {
                .row_number, .rank, .dense_rank => {
                    var rn: i64 = 0;
                    var rk: i64 = 0;
                    var dr: i64 = 0;
                    for (idx, 0..) |row, k| {
                        if (k == 0 or !sameOn(parts, idx[k - 1], row)) {
                            rn = 1;
                            rk = 1;
                            dr = 1;
                        } else {
                            rn += 1;
                            if (!sameOn(ords, idx[k - 1], row)) {
                                rk = rn;
                                dr += 1;
                            }
                        }
                        try bd.appendInt(switch (f.kind) {
                            .row_number => rn,
                            .rank => rk,
                            else => dr,
                        });
                    }
                },
                .lag, .lead => {
                    const dir: i64 = if (f.kind == .lag) -f.offset else f.offset;
                    for (idx, 0..) |_, k| {
                        const target = @as(i64, @intCast(k)) + dir;
                        if (target < @as(i64, @intCast(pstart[k])) or target >= @as(i64, @intCast(pend[k]))) {
                            out.*[k] = .null;
                        } else {
                            out.*[k] = all.columns[f.arg.?].getValue(idx[@intCast(target)]);
                        }
                    }
                },
                .sum, .count, .min, .max, .avg => if (f.frame.rows) {
                    const vs = try arena.alloc(Value, idx.len);
                    for (idx, vs) |row, *v| v.* = if (f.arg) |ai| all.columns[ai].getValue(row) else .null;
                    var dq = std.array_list.Managed(usize).init(arena);
                    var dq_head: usize = 0;
                    var lo: usize = 0;
                    var acc_i: i128 = 0;
                    var acc_f: f64 = 0;
                    var n: i64 = 0;
                    var seen_float = false;
                    for (idx, 0..) |_, k| {
                        if (k == pstart[k]) {
                            lo = k;
                            acc_i = 0;
                            acc_f = 0;
                            n = 0;
                            seen_float = false;
                            dq.clearRetainingCapacity();
                            dq_head = 0;
                        }
                        const start = if (f.frame.unbounded)
                            pstart[k]
                        else blk: {
                            const back = @as(i64, @intCast(k)) - f.frame.preceding;
                            const floor = @as(i64, @intCast(pstart[k]));
                            break :blk @as(usize, @intCast(@max(back, floor)));
                        };
                        while (lo < start) : (lo += 1) {
                            if (f.arg == null) {
                                n -= 1;
                                continue;
                            }
                            const v = vs[lo];
                            if (v.isNull()) continue;
                            n -= 1;
                            switch (v) {
                                .int => |x| {
                                    acc_i -= x;
                                    acc_f -= @floatFromInt(x);
                                },
                                else => if (asF64Opt(v)) |x| {
                                    acc_f -= x;
                                },
                            }
                        }
                        while (dq_head < dq.items.len and dq.items[dq_head] < start) dq_head += 1;
                        if (f.arg == null) {
                            n += 1;
                        } else if (!vs[k].isNull()) {
                            const v = vs[k];
                            n += 1;
                            switch (v) {
                                .int => |x| {
                                    acc_i += x;
                                    acc_f += @floatFromInt(x);
                                },
                                else => if (asF64Opt(v)) |x| {
                                    acc_f += x;
                                    seen_float = true;
                                },
                            }
                            if (f.kind == .min or f.kind == .max) {
                                while (dq.items.len > dq_head) {
                                    const back = vs[dq.items[dq.items.len - 1]];
                                    const beaten = if (f.kind == .max) !lessV(v, back) else !lessV(back, v);
                                    if (!beaten) break;
                                    dq.items.len -= 1;
                                }
                                try dq.append(k);
                            }
                        }
                        out.*[k] = switch (f.kind) {
                            .count => .{ .int = n },
                            .min, .max => if (dq_head < dq.items.len) vs[dq.items[dq_head]] else .null,
                            .avg => if (n == 0) .null else .{ .float = acc_f / @as(f64, @floatFromInt(n)) },
                            else => if (n == 0) .null else if (seen_float) .{ .float = acc_f } else try self.intSum(acc_i),
                        };
                    }
                } else {
                    var i: usize = 0;
                    while (i < idx.len) {
                        const stop = pend[i];
                        var acc_i: i128 = 0;
                        var acc_f: f64 = 0;
                        var n: i64 = 0;
                        var seen_float = false;
                        var ext: Value = .null;
                        var g = i;
                        while (g < stop) {
                            var e = g + 1;
                            while (e < stop and sameOn(ords, idx[e - 1], idx[e])) e += 1;
                            var m = g;
                            while (m < e) : (m += 1) {
                                if (f.arg) |ai| {
                                    const v = all.columns[ai].getValue(idx[m]);
                                    if (v.isNull()) continue;
                                    n += 1;
                                    if (ext.isNull() or (if (f.kind == .max) lessV(ext, v) else lessV(v, ext))) ext = v;
                                    switch (v) {
                                        .int => |x| {
                                            acc_i += x;
                                            acc_f += @floatFromInt(x);
                                        },
                                        else => if (asF64Opt(v)) |x| {
                                            acc_f += x;
                                            seen_float = true;
                                        },
                                    }
                                } else {
                                    n += 1;
                                }
                            }
                            var k = g;
                            while (k < e) : (k += 1) {
                                out.*[k] = switch (f.kind) {
                                    .count => .{ .int = n },
                                    .min, .max => ext,
                                    .avg => if (n == 0) .null else .{ .float = acc_f / @as(f64, @floatFromInt(n)) },
                                    else => if (n == 0) .null else if (seen_float) .{ .float = acc_f } else try self.intSum(acc_i),
                                };
                            }
                            g = e;
                        }
                        i = stop;
                    }
                },
            }
        }

        const cols = try arena.alloc(column.Column, ncols + self.funcs.len);
        for (all.columns, 0..) |*col, ci| cols[ci] = try column.permute(arena, col.*, idx);
        for (builders, vals, ncols..) |*bd, v, ci| {
            for (v) |x| try bd.append(x);
            cols[ci] = try bd.finish();
        }
        return Batch{ .schema = self.out_schema, .columns = cols, .len = all.len };
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

pub const Distinct = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    seen: ?Seen = null,
    track_ords: bool = false,
    ords: []const u64 = &.{},
    seen_rows: u64 = 0,
    seen_words: ?SeenWords = null,
    seen_null: bool = false,

    const Seen = std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    const SeenWords = std.AutoHashMap(u64, void);

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
            if (key_idx.len == 1 and fixedWord(b.columns[key_idx[0]]) != null) {
                if (self.seen_words == null) self.seen_words = SeenWords.init(self.state);
                const words = &self.seen_words.?;
                const col = b.columns[key_idx[0]];
                while (r < b.len) : (r += 1) {
                    if (!col.validity.get(r)) {
                        keep[r] = !self.seen_null;
                        self.seen_null = true;
                    } else {
                        const gop = try words.getOrPut(fixedWord(col).?[r]);
                        keep[r] = !gop.found_existing;
                    }
                    if (keep[r]) kept += 1;
                }
            } else while (r < b.len) : (r += 1) {
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
            const base = self.seen_rows;
            self.seen_rows += b.len;
            if (kept == 0) {
                _ = scratch.reset(.retain_capacity);
                continue;
            }
            if (self.track_ords) {
                const ords = try arena.alloc(u64, kept);
                var oi: usize = 0;
                r = 0;
                while (r < b.len) : (r += 1) if (keep[r]) {
                    ords[oi] = base + r;
                    oi += 1;
                };
                self.ords = ords;
            }
            return try gatherDeep(arena, b, keep, kept);
        }
        return null;
    }
};

/// The raw 64-bit words of an int-family column, or null for a kind whose bits
/// are not its identity (floats have two zeros).
fn fixedWord(col: column.Column) ?[]const u64 {
    return switch (col.ty.kind) {
        .int, .time, .timestamp => @ptrCast(col.data.i64),
        else => null,
    };
}

/// Deep-copy the `keep`-marked rows of `b` into `arena`, duping string payloads
/// (unlike `column.gather`), for a source batch whose scratch is about to be freed.
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
    threads: usize = 1,

    pub const Key = struct { idx: usize, desc: bool };

    pub fn next(self: *Sort, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const all = (try materializeAll(arena, self.child, self.in_schema)) orelse return null;

        const idx = try arena.alloc(usize, all.len);
        for (idx, 0..) |*x, i| x.* = i;
        const arrs = try arena.alloc(KeyArr, self.keys.len);
        for (self.keys, arrs) |k, *a| a.* = try KeyArr.prepare(arena, all.columns[k.idx], k.desc);
        try sortIdxThreads(arena, idx, arrs, self.threads);

        const outcols = try arena.alloc(column.Column, all.columns.len);
        for (all.columns, 0..) |*col, ci| outcols[ci] = try column.permute(arena, col.*, idx);
        return Batch{ .schema = all.schema, .columns = outcols, .len = all.len };
    }
};

const KeyArr = struct {
    desc: bool,
    valid: column.Bitmap,
    data: Data,

    const Data = union(enum) {
        ints: []i64,
        floats: []f64,
        decs: []i128,
        strs: [][]const u8,
        boxed: column.Column,
    };

    fn prepare(arena: std.mem.Allocator, col: column.Column, desc: bool) !KeyArr {
        const n = col.len;
        const data: Data = switch (col.ty.kind) {
            .int, .time, .timestamp => .{ .ints = col.data.i64 },
            .date => blk: {
                const out = try arena.alloc(i64, n);
                for (out, col.data.i32[0..n]) |*o, x| o.* = x;
                break :blk .{ .ints = out };
            },
            .bool => blk: {
                const out = try arena.alloc(i64, n);
                for (out, col.data.b[0..n]) |*o, x| o.* = @intFromBool(x);
                break :blk .{ .ints = out };
            },
            .float => .{ .floats = col.data.f64 },
            .decimal => blk: {
                var scale: u8 = 0;
                for (col.data.dec[0..n], 0..) |d, i| {
                    if (col.validity.get(i)) scale = @max(scale, d.scale);
                }
                const out = try arena.alloc(i128, n);
                for (out, col.data.dec[0..n], 0..) |*o, d, i| {
                    o.* = if (!col.validity.get(i)) 0 else (eval.rescaleTo(d, scale) orelse break :blk Data{ .boxed = col }).unscaled;
                }
                break :blk .{ .decs = out };
            },
            .string, .bytes => blk: {
                const out = try arena.alloc([]const u8, n);
                for (out, 0..) |*o, i| o.* = col.data.bytes.at(i);
                break :blk .{ .strs = out };
            },
            else => .{ .boxed = col },
        };
        return .{ .desc = desc, .valid = col.validity, .data = data };
    }

    fn order(self: KeyArr, a: usize, c: usize) std.math.Order {
        const an = !self.valid.get(a);
        const bn = !self.valid.get(c);
        if (an or bn) {
            if (an and bn) return .eq;
            return if (an) .gt else .lt;
        }
        const ord: std.math.Order = switch (self.data) {
            .ints => |v| std.math.order(v[a], v[c]),
            .floats => |v| eval.orderF64(v[a], v[c]),
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

/// Sort `idx` (pre-filled 0..n) by `arrs`, first key most significant, stably,
/// with the radix scheme the module header describes.
fn sortIdx(out_arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr) !void {
    return sortIdxThreads(out_arena, idx, arrs, 1);
}

fn sortIdxThreads(out_arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, threads: usize) !void {
    _ = out_arena;
    const n = idx.len;
    if (n == 0 or arrs.len == 0) return;
    var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer scratch.deinit();
    const arena = scratch.allocator();
    const plans = try arena.alloc(KeyPlan, arrs.len);
    var exact = true;
    for (arrs, plans) |k, *p| {
        p.* = KeyPlan.of(k, n) orelse {
            std.mem.sort(usize, idx, SortCtx{ .arrs = arrs }, SortCtx.lessThan);
            return;
        };
        if (!p.exact) exact = false;
    }
    if (n > std.math.maxInt(u32)) {
        std.mem.sort(usize, idx, SortCtx{ .arrs = arrs }, SortCtx.lessThan);
        return;
    }
    if (try sortIdxParallel(arena, idx, arrs, plans, exact, threads)) return;
    try sortRows(arena, idx, arrs, plans, exact, true);
}

/// A stable LSD radix sort of `idx`. With `all` (every row, once) each word is
/// encoded once in row order and gathered; encoding via the permutation read
/// strings at random.
fn sortRows(arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan, exact: bool, all: bool) !void {
    const n = idx.len;
    const pairs = try arena.alloc(RadixPair, n);
    const tmp = try arena.alloc(RadixPair, n);
    const counts = try arena.alloc(u32, 1 << 11);
    const words: []u64 = if (all) try arena.alloc(u64, n) else &.{};
    var j = arrs.len;
    while (j > 0) {
        j -= 1;
        const k = arrs[j];
        const p = plans[j];
        var w = p.words;
        while (w > 0) {
            w -= 1;
            if (all) {
                for (words, 0..) |*x, i| x.* = encWord(k, p, w, i);
                for (pairs, idx) |*pr, i| pr.* = .{ .k = words[i], .i = @intCast(i) };
            } else {
                for (pairs, idx) |*pr, i| pr.* = .{ .k = encWord(k, p, w, i), .i = @intCast(i) };
            }
            radixSortPairs(pairs, tmp, counts);
            for (pairs, idx) |pr, *x| x.* = pr.i;
        }
        if (p.nulls) {
            for (pairs, idx) |*pr, i| pr.* = .{ .k = @intFromBool(!k.valid.get(i)), .i = @intCast(i) };
            radixSortPairs(pairs, tmp, counts);
            for (pairs, idx) |pr, *x| x.* = pr.i;
        }
    }
    if (!exact) fixTies(idx, arrs, plans);
}

/// Deal rows, in input order, into ranges of the first key split where a histogram
/// puts about one thread's share (nulls in a range of their own), and sort each on
/// its own thread. False when not worth it or the first key cannot split.
fn sortIdxParallel(arena: std.mem.Allocator, idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan, exact: bool, threads: usize) !bool {
    const n = idx.len;
    if (threads < 2 or n < 1 << 17) return false;
    const k0 = arrs[0];
    const p0 = plans[0];
    const isnull = struct {
        fn f(k: KeyArr, p: KeyPlan, row: usize) bool {
            return p.nulls and !k.valid.get(row);
        }
    }.f;
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (0..n) |row| {
        if (isnull(k0, p0, row)) continue;
        const w = encWord(k0, p0, 0, row);
        lo = @min(lo, w);
        hi = @max(hi, w);
    }
    if (lo >= hi) return false;
    const bits: u32 = 64 - @clz(hi - lo);
    const shift: u6 = @intCast(if (bits > 11) bits - 11 else 0);
    var hist = [_]u32{0} ** (1 << 11);
    var nonnull: usize = 0;
    for (0..n) |row| {
        if (isnull(k0, p0, row)) continue;
        hist[@intCast((encWord(k0, p0, 0, row) - lo) >> shift)] += 1;
        nonnull += 1;
    }
    const nthreads = @min(threads, 64);
    var of_digit: [1 << 11]u8 = undefined;
    var cur: usize = 0;
    var acc: usize = 0;
    for (hist, &of_digit) |h, *d| {
        d.* = @intCast(cur);
        acc += h;
        if (cur + 1 < nthreads and acc * nthreads >= nonnull * (cur + 1)) cur += 1;
    }
    const null_range = cur + 1;
    const nranges = null_range + 1;
    var size = [_]usize{0} ** 66;
    var range_of = try arena.alloc(u8, n);
    for (0..n) |row| {
        const r: u8 = if (isnull(k0, p0, row)) @intCast(null_range) else of_digit[@intCast((encWord(k0, p0, 0, row) - lo) >> shift)];
        range_of[row] = r;
        size[r] += 1;
    }
    var start = [_]usize{0} ** 66;
    for (1..nranges) |r| start[r] = start[r - 1] + size[r - 1];
    var fill = start;
    for (0..n) |row| {
        const r = range_of[row];
        idx[fill[r]] = row;
        fill[r] += 1;
    }

    const Job = struct {
        rows: []usize,
        arrs: []const KeyArr,
        plans: []const KeyPlan,
        exact: bool,
        err: ?anyerror = null,
        fn run(job: *@This()) void {
            var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer ar.deinit();
            sortRows(ar.allocator(), job.rows, job.arrs, job.plans, job.exact, false) catch |e| {
                job.err = e;
            };
        }
    };
    var jobs: [66]Job = undefined;
    var handles: [66]?std.Thread = @splat(null);
    for (0..nranges) |r| {
        jobs[r] = .{ .rows = idx[start[r]..][0..size[r]], .arrs = arrs, .plans = plans, .exact = exact };
        if (size[r] < 2) continue;
        handles[r] = std.Thread.spawn(.{}, Job.run, .{&jobs[r]}) catch blk: {
            jobs[r].run();
            break :blk null;
        };
    }
    for (handles[0..nranges]) |h| if (h) |t| t.join();
    for (jobs[0..nranges]) |jb| if (jb.err) |e| return e;
    return true;
}

const KeyPlan = struct {
    words: u8,
    exact: bool,
    nulls: bool,

    const max_exact_str = 31;
    const prefix_words = 3;

    fn of(k: KeyArr, n: usize) ?KeyPlan {
        const nulls = !k.valid.allSet(n);
        return switch (k.data) {
            .ints, .floats => .{ .words = 1, .exact = true, .nulls = nulls },
            .decs => .{ .words = 2, .exact = true, .nulls = nulls },
            .strs => |v| blk: {
                var longest: usize = 0;
                for (v[0..n], 0..) |x, i| {
                    if (k.valid.get(i)) longest = @max(longest, x.len);
                }
                if (longest > max_exact_str) break :blk .{ .words = prefix_words, .exact = false, .nulls = nulls };
                break :blk .{ .words = @intCast(longest / 8 + 1), .exact = true, .nulls = nulls };
            },
            .boxed => null,
        };
    }
};

/// Word `w` (0 most significant) of row `i`'s key. A null row's words are 0: the
/// flag word places it, and among nulls the key is all equal.
fn encWord(k: KeyArr, p: KeyPlan, w: usize, i: usize) u64 {
    if (p.nulls and !k.valid.get(i)) return 0;
    const word: u64 = switch (k.data) {
        .ints, .floats => return orderedWord(k, i),
        .decs => |v| blk: {
            const u: u128 = @as(u128, @bitCast(v[i])) ^ (@as(u128, 1) << 127);
            break :blk if (w == 0) @intCast(u >> 64) else @truncate(u);
        },
        .strs => |v| blk: {
            const str = v[i];
            var b: [8]u8 = @splat(0);
            const lo = w * 8;
            if (lo < str.len) {
                const take = @min(8, str.len - lo);
                @memcpy(b[0..take], str[lo..][0..take]);
            }
            if (p.exact and w + 1 == p.words) b[7] = @intCast(str.len);
            break :blk std.mem.readInt(u64, &b, .big);
        },
        .boxed => unreachable,
    };
    return if (k.desc) ~word else word;
}

/// Re-sort, with the comparator, each run the string prefixes could not order. A
/// run is equal on every key's words up to the first inexact key only, so a later
/// key cannot decide between rows that still differ in it.
fn fixTies(idx: []usize, arrs: []const KeyArr, plans: []const KeyPlan) void {
    var m: usize = 0;
    while (plans[m].exact) m += 1;
    var start: usize = 0;
    while (start < idx.len) {
        var end = start + 1;
        while (end < idx.len and sameWords(arrs[0 .. m + 1], plans[0 .. m + 1], idx[start], idx[end])) end += 1;
        if (end - start > 1) std.mem.sort(usize, idx[start..end], SortCtx{ .arrs = arrs }, SortCtx.lessThan);
        start = end;
    }
}

fn sameWords(arrs: []const KeyArr, plans: []const KeyPlan, a: usize, b: usize) bool {
    for (arrs, plans) |k, p| {
        if (k.valid.get(a) != k.valid.get(b)) return false;
        var w: usize = 0;
        while (w < p.words) : (w += 1) {
            if (encWord(k, p, w, a) != encWord(k, p, w, b)) return false;
        }
    }
    return true;
}

const RadixPair = struct { k: u64, i: u32 };

/// Stable LSD radix sort on `k` less its minimum, in 11-bit digits over only the
/// digits the range needs. 2048 buckets keep the scatter in cache; 65536 did not.
fn radixSortPairs(pairs: []RadixPair, tmp: []RadixPair, counts: []u32) void {
    if (pairs.len < 2) return;
    var lo: u64 = std.math.maxInt(u64);
    var hi: u64 = 0;
    for (pairs) |p| {
        lo = @min(lo, p.k);
        hi = @max(hi, p.k);
    }
    if (lo == hi) return;
    const bits: u32 = 64 - @clz(hi - lo);
    const digit = 11;
    const nb = 1 << digit;
    const cs = counts[0..nb];
    var src = pairs;
    var dst = tmp;
    var shift: u32 = 0;
    while (shift < bits) : (shift += digit) {
        const sh: u6 = @intCast(shift);
        @memset(cs, 0);
        for (src) |p| cs[@intCast(((p.k - lo) >> sh) & (nb - 1))] += 1;
        var sum: u32 = 0;
        for (cs) |*c| {
            const v = c.*;
            c.* = sum;
            sum += v;
        }
        for (src) |p| {
            const d: usize = @intCast(((p.k - lo) >> sh) & (nb - 1));
            dst[cs[d]] = p;
            cs[d] += 1;
        }
        const t = src;
        src = dst;
        dst = t;
    }
    if (src.ptr != pairs.ptr) @memcpy(pairs, src);
}

/// The u64 whose unsigned order is the key's: ints flip the sign bit, floats use the
/// IEEE trick with NaN canonicalized last and -0.0 folded into 0.0; `desc` complements.
fn orderedWord(k: KeyArr, i: usize) u64 {
    const w: u64 = switch (k.data) {
        .ints => |v| @as(u64, @bitCast(v[i])) ^ (1 << 63),
        .floats => |v| blk: {
            const x = if (v[i] == 0) 0.0 else v[i];
            const b: u64 = if (std.math.isNan(x)) 0x7ff8000000000000 else @bitCast(x);
            break :blk if (b >> 63 != 0) ~b else b | (1 << 63);
        },
        else => unreachable,
    };
    return if (k.desc) ~w else w;
}

/// Order of two sort-key values: nulls always last, `desc` flips non-null order.
/// Shared by `SortCtx.lessThan` and Top-N.
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

pub const TopN = struct {
    pub const max_rows: u64 = 1 << 16;

    pub fn fits(lim: anytype) bool {
        return lim.count +| lim.offset <= max_rows;
    }

    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: []const Sort.Key,
    count: u64,
    offset: u64,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    done: bool = false,
    threshold: ?*Threshold = null,
    seq_base: u64 = 0,
    seen: u64 = 0,
    item: ?*const usize = null,
    last_item: usize = std.math.maxInt(usize),

    pub const Entry = []Value;
    const Heap = std.PriorityQueue(Entry, []const Sort.Key, entryWorstFirst);

    pub fn next(self: *TopN, arena: std.mem.Allocator) anyerror!?Batch {
        const kept = (try self.nextEntries(arena)) orelse return null;
        const start = @min(self.offset, kept.len);
        const end = @min(self.offset + self.count, kept.len);
        if (start >= end) return null;
        return try self.emit(arena, kept[start..end]);
    }

    /// Every row kept, best first, positions included, copied into `arena`, for a
    /// combine across lanes that ranks them with `entryLess`.
    pub fn nextEntries(self: *TopN, arena: std.mem.Allocator) anyerror!?[]Entry {
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
            if (self.item) |it| if (it.* != self.last_item) {
                self.last_item = it.*;
                self.seq_base = @as(u64, it.*) << 40;
                self.seen = 0;
            };
            const k0 = if (self.keys.len > 0) self.keys[0] else Sort.Key{ .idx = 0, .desc = false };
            const kc = &b.columns[k0.idx];
            const typed: enum { none, int, float } = if (self.keys.len == 0) .none else switch (kc.ty.kind) {
                .int, .time, .timestamp => .int,
                .float => .float,
                else => .none,
            };
            const base = self.seen;
            var r: usize = 0;
            while (r < b.len and heap.count() < cap) : (r += 1) {
                self.seen = base + r;
                try heap.add(try self.cloneRow(b, r));
                if (heap.count() >= cap) self.publish(heap.items[0]);
            }
            if (r < b.len) {
                const cand = try pull.alloc(u32, b.len - r);
                var nc: usize = 0;
                const w = heap.items[0][k0.idx];
                while (r < b.len) : (r += 1) {
                    if (typed != .none and !w.isNull()) {
                        const worse = if (!kc.validity.get(r)) true else switch (typed) {
                            .int => switch (w) {
                                .int, .time, .timestamp => |y| if (k0.desc) kc.data.i64[r] < y else kc.data.i64[r] > y,
                                else => false,
                            },
                            .float => switch (w) {
                                .float => |y| if (k0.desc) kc.data.f64[r] < y else kc.data.f64[r] > y,
                                else => false,
                            },
                            .none => false,
                        };
                        if (worse) continue;
                    }
                    cand[nc] = @intCast(r);
                    nc += 1;
                }
                const Ctx = struct {
                    top: *TopN,
                    b: Batch,
                    kc: *const column.Column,
                    typed: @TypeOf(typed),
                    fn lt(c: @This(), x: u32, y: u32) bool {
                        var rest = c.top.keys;
                        if (c.typed != .none) {
                            const k = c.top.keys[0];
                            const xn = !c.kc.validity.get(x);
                            const yn = !c.kc.validity.get(y);
                            if (xn != yn) return yn;
                            const nan = c.typed == .float and (std.math.isNan(c.kc.data.f64[x]) or std.math.isNan(c.kc.data.f64[y]));
                            if (!xn and !nan) {
                                const o = switch (c.typed) {
                                    .int => std.math.order(c.kc.data.i64[x], c.kc.data.i64[y]),
                                    .float => std.math.order(c.kc.data.f64[x], c.kc.data.f64[y]),
                                    .none => unreachable,
                                };
                                if (o != .eq) return if (k.desc) o == .gt else o == .lt;
                            }
                            if (!nan) rest = rest[1..];
                        }
                        for (rest) |k| {
                            const o = keyOrder(c.b.columns[k.idx].getValue(x), c.b.columns[k.idx].getValue(y), k.desc);
                            if (o != .eq) return o == .lt;
                        }
                        return x < y;
                    }
                };
                std.mem.sort(u32, cand[0..nc], Ctx{ .top = self, .b = b, .kc = kc, .typed = typed }, Ctx.lt);
                for (cand[0..nc]) |ri| {
                    self.seen = base + ri;
                    if (!self.rowLess(b, ri, heap.items[0])) break;
                    self.freeEntry(heap.remove());
                    try heap.add(try self.cloneRow(b, ri));
                    self.publish(heap.items[0]);
                }
            }
            self.seen = base + b.len;
            _ = scratch.reset(.retain_capacity);
        }
        if (heap.items.len == 0) return null;

        std.mem.sort(Entry, heap.items, self.keys, entryLessCtx);
        const out = try arena.alloc(Entry, heap.items.len);
        for (heap.items, out) |e, *o| o.* = try dupeRowArena(arena, e);
        return out;
    }

    /// Publishes the worst kept entry's first key; with several sort keys the leading
    /// one still bounds the rest. Null, string and bytes bounds are not published.
    fn publish(self: *TopN, worst: Entry) void {
        const t = self.threshold orelse return;
        if (self.keys.len == 0) return;
        const v = worst[self.keys[0].idx];
        if (v == .null or v == .string or v == .bytes) return;
        t.value = v;
        t.full = true;
    }

    fn cloneRow(self: *TopN, b: Batch, r: usize) !Entry {
        const vals = try self.gpa.alloc(Value, b.columns.len + 1);
        for (b.columns, vals[0..b.columns.len]) |*col, *out| out.* = try dupeValueGpa(self.gpa, col.getValue(r));
        vals[b.columns.len] = .{ .int = @bitCast(self.seq_base + self.seen) };
        return vals;
    }

    fn freeEntry(self: *TopN, e: Entry) void {
        for (e) |v| switch (v) {
            .string, .bytes => |s| self.gpa.free(s),
            else => {},
        };
        self.gpa.free(e);
    }

    /// Whether row `r` of `b` ranks before entry `e`. Equal keys rank by position, since
    /// a lane reading row groups out of order meets rows earlier than some it keeps.
    fn rowLess(self: *TopN, b: Batch, r: usize, e: Entry) bool {
        for (self.keys) |k| {
            const o = keyOrder(b.columns[k.idx].getValue(r), e[k.idx], k.desc);
            if (o != .eq) return o == .lt;
        }
        return self.seq_base + self.seen < entrySeq(e);
    }

    pub fn emit(self: *TopN, arena: std.mem.Allocator, entries: []const Entry) !Batch {
        const cols = try arena.alloc(column.Column, self.in_schema.fields.len);
        for (self.in_schema.fields, 0..) |f, ci| {
            var bd = column.Builder.init(arena, f.ty);
            for (entries) |e| try bd.append(e[ci]);
            cols[ci] = try bd.finish();
        }
        return Batch{ .schema = self.in_schema, .columns = cols, .len = entries.len };
    }
};

pub fn entryLess(a: TopN.Entry, b: TopN.Entry, keys: []const Sort.Key) bool {
    for (keys) |k| {
        const o = keyOrder(a[k.idx], b[k.idx], k.desc);
        if (o != .eq) return o == .lt;
    }
    return entrySeq(a) < entrySeq(b);
}

fn entrySeq(e: TopN.Entry) u64 {
    return @bitCast(e[e.len - 1].int);
}

fn dupeRowArena(arena: std.mem.Allocator, row: []const Value) ![]Value {
    const out = try arena.alloc(Value, row.len);
    for (out, row) |*o, v| o.* = try dupeValue(arena, v);
    return out;
}

fn entryLessCtx(keys: []const Sort.Key, a: TopN.Entry, b: TopN.Entry) bool {
    return entryLess(a, b, keys);
}

/// `std.PriorityQueue` comparator ranking the worst row highest, so the min-heap API
/// yields the eviction candidate. Of two equal rows the later is worse.
fn entryWorstFirst(keys: []const Sort.Key, a: TopN.Entry, b: TopN.Entry) std.math.Order {
    for (keys) |k| {
        const o = keyOrder(a[k.idx], b[k.idx], k.desc);
        if (o != .eq) return o.invert();
    }
    if (entrySeq(a) != entrySeq(b)) return std.math.order(entrySeq(b), entrySeq(a));
    return .eq;
}

fn dupeRowGpa(gpa: std.mem.Allocator, row: []const Value) ![]Value {
    const out = try gpa.alloc(Value, row.len);
    for (out, row) |*o, v| o.* = try dupeValueGpa(gpa, v);
    return out;
}

fn freeRowGpa(gpa: std.mem.Allocator, row: []Value) void {
    for (row) |v| switch (v) {
        .string, .bytes => |x| gpa.free(x),
        else => {},
    };
    gpa.free(row);
}

fn dupeValueGpa(gpa: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try gpa.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try gpa.dupe(u8, s) },
        else => v,
    };
}

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
    part_state: ?[]const std.mem.Allocator = null,
    table_gpa: ?std.mem.Allocator = null,
    strs: ?*StrTable = null,
    str_cache: std.StringHashMapUnmanaged(StrId) = .empty,
    done: bool = false,

    const StrId = struct { id: u32, h: u64 };

    pub const StrTable = struct {
        mtx: std.Thread.Mutex = .{},
        arena: std.heap.ArenaAllocator,
        map: std.StringHashMapUnmanaged(u32) = .empty,
        strs: std.ArrayListUnmanaged([]const u8) = .empty,

        pub fn init(child: std.mem.Allocator) StrTable {
            return .{ .arena = std.heap.ArenaAllocator.init(child) };
        }

        pub fn deinit(self: *StrTable) void {
            self.arena.deinit();
        }

        fn intern(self: *StrTable, s: []const u8) !u32 {
            self.mtx.lock();
            defer self.mtx.unlock();
            const a = self.arena.allocator();
            const gop = try self.map.getOrPut(a, s);
            if (!gop.found_existing) {
                const own = try a.dupe(u8, s);
                gop.key_ptr.* = own;
                gop.value_ptr.* = @intCast(self.strs.items.len);
                try self.strs.append(a, own);
            }
            return gop.value_ptr.*;
        }

        pub fn at(self: *const StrTable, id: u32) []const u8 {
            return self.strs.items[id];
        }
    };

    fn strId(self: *Aggregate, table: *StrTable, s: []const u8) !StrId {
        const gop = try self.str_cache.getOrPut(self.state, s);
        if (!gop.found_existing) {
            gop.key_ptr.* = try self.state.dupe(u8, s);
            gop.value_ptr.* = .{ .id = try table.intern(s), .h = std.hash.Wyhash.hash(0x5bd1e995, s) };
        }
        return gop.value_ptr.*;
    }

    fn strTable(self: *Aggregate) !*StrTable {
        if (self.strs) |t| return t;
        const t = try self.state.create(StrTable);
        t.* = StrTable.init(self.state);
        self.strs = t;
        return t;
    }

    pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, distinct: bool = false };

    pub const Acc = struct {
        n: i64 = 0,
        sum_i: i128 align(8) = 0,
        sum_f: f64 = 0,
        ext: Value = .null,
        seen: ?*DistinctSet() = null,
        vals: ?*std.array_list.Managed(f64) = null,
    };

    /// A type-returning fn for the same reason as `GroupMap`.
    pub fn DistinctSet() type {
        return std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    }

    fn noteMedian(alloc: std.mem.Allocator, acc: *Acc, x: f64) !void {
        const l = acc.vals orelse blk: {
            const p = try alloc.create(std.array_list.Managed(f64));
            p.* = std.array_list.Managed(f64).init(alloc);
            acc.vals = p;
            break :blk p;
        };
        try l.append(x);
    }

    /// Fold another lane's distinct values into `acc`, sizing the destination first:
    /// inserting a hash-ordered run into a smaller table built ever-longer probe runs
    /// (a global COUNT(DISTINCT) took 292s at -j 8, 8s at -j 1).
    fn mergeDistinct(alloc: std.mem.Allocator, acc: *Acc, src: *const DistinctSet()) !void {
        const set = acc.seen orelse blk: {
            const p = try alloc.create(DistinctSet());
            p.* = DistinctSet().init(alloc);
            acc.seen = p;
            break :blk p;
        };
        try set.ensureTotalCapacity(std.math.cast(u32, set.count() + src.count()) orelse return error.OutOfMemory);
        var it = src.keyIterator();
        while (it.next()) |k| try noteDistinct(alloc, acc, k.*[0]);
    }

    /// Probes with a stack key and copies only on a miss. Copying first grew an arena
    /// per row: COUNT(DISTINCT) over 200M rows held ~7.6 GB for 200 values.
    fn noteDistinct(alloc: std.mem.Allocator, acc: *Acc, v: Value) !void {
        const set = acc.seen orelse blk: {
            const p = try alloc.create(DistinctSet());
            p.* = DistinctSet().init(alloc);
            acc.seen = p;
            break :blk p;
        };
        var probe = [_]Value{v};
        const gop = try set.getOrPut(probe[0..]);
        if (!gop.found_existing) {
            const key = try alloc.alloc(Value, 1);
            key[0] = try dupeValue(alloc, v);
            gop.key_ptr.* = key;
        }
        acc.n = @intCast(set.count());
    }

    pub const GroupSet = struct {
        store: Store,
        len: usize,
        key_kinds: []const types.TypeKind,
        table: ?*GroupTable = null,
        strs: ?*const StrTable = null,

        pub fn freeTable(self: *const GroupSet) void {
            if (self.table) |t| t.deinit();
        }

        pub const Store = union(enum) {
            fixed: *FixedStore,
            boxed: *GroupStore,
            single: []Acc,
        };

        pub fn keyValue(self: *const GroupSet, i: usize, j: usize) Value {
            switch (self.store) {
                .boxed => |st| return st.at(i).keys[j],
                .single => unreachable,
                .fixed => |st| {
                    const rec = st.at(i);
                    if (rec.mask.* & (@as(u64, 1) << @intCast(j)) != 0) return .null;
                    const raw = rec.keys[j];
                    return switch (self.key_kinds[j]) {
                        .int => .{ .int = raw },
                        .time => .{ .time = raw },
                        .timestamp => .{ .timestamp = raw },
                        .date => .{ .date = @intCast(raw) },
                        .bool => .{ .bool = raw != 0 },
                        .float => .{ .float = @bitCast(raw) },
                        .string => .{ .string = self.strs.?.at(@intCast(raw)) },
                        else => unreachable,
                    };
                },
            }
        }

        pub fn acc(self: *const GroupSet, i: usize, j: usize) Acc {
            return switch (self.store) {
                .fixed => |st| st.layout.acc(st.at(i).tail, j),
                .boxed => |st| st.layout.acc(st.at(i).tail, j),
                .single => |accs| accs[j],
            };
        }
    };

    pub const GroupMerge = struct {
        alloc: std.mem.Allocator,
        aggs: []const Agg,
        any_distinct: bool,
        own: bool = false,
        set: GroupSet,
        table: GroupTable,

        fn anyDistinct(aggs: []const Agg) bool {
            for (aggs) |a| {
                if (a.distinct) return true;
            }
            return false;
        }

        /// Merge into `set` itself, its store and index, instead of copying it. The index
        /// moves here (`deinit` frees it); null for a set without one of its own.
        pub fn adopt(set: *const GroupSet, aggs: []const Agg) ?GroupMerge {
            const table = set.table orelse return null;
            const alloc = switch (set.store) {
                .fixed => |st| st.recs.alloc,
                .boxed => |st| st.recs.alloc,
                .single => return null,
            };
            defer table.entries = &.{};
            return .{
                .alloc = alloc,
                .aggs = aggs,
                .any_distinct = anyDistinct(aggs),
                .set = set.*,
                .table = table.*,
            };
        }

        pub fn deinit(self: *GroupMerge) void {
            self.table.deinit();
        }

        pub fn init(alloc: std.mem.Allocator, like: *const GroupSet, aggs: []const Agg) !GroupMerge {
            const any_distinct = anyDistinct(aggs);
            const store: GroupSet.Store = switch (like.store) {
                .fixed => |st| blk: {
                    const n = try alloc.create(FixedStore);
                    n.* = FixedStore.init(alloc, st.nkeys, st.layout);
                    break :blk .{ .fixed = n };
                },
                .boxed => |st| blk: {
                    const n = try alloc.create(GroupStore);
                    n.* = GroupStore.init(alloc, st.nkeys, st.layout);
                    break :blk .{ .boxed = n };
                },
                .single => blk: {
                    const accs = try alloc.alloc(Acc, aggs.len);
                    @memset(accs, .{});
                    break :blk .{ .single = accs };
                },
            };
            return .{
                .alloc = alloc,
                .aggs = aggs,
                .any_distinct = any_distinct,
                .set = .{ .store = store, .len = if (store == .single) 1 else 0, .key_kinds = like.key_kinds, .strs = like.strs },
                .table = try GroupTable.init(alloc, 256),
            };
        }

        pub fn add(self: *GroupMerge, src: *const GroupSet, i: usize) !void {
            switch (self.set.store) {
                .single => |dst| for (dst, self.aggs, 0..) |*d, agg, j| try mergeAcc(self.alloc, d, src.acc(i, j), agg),
                .fixed => |dst| {
                    const sr = src.store.fixed.at(i);
                    const h = src.store.fixed.hashAt(i);
                    const rec = try self.find(dst, h, FixedKey{ .vals = sr.keys, .mask = sr.mask.* }) orelse {
                        const nr = try dst.push(h);
                        @memcpy(nr.keys, sr.keys);
                        nr.mask.* = sr.mask.*;
                        try self.adoptTail(dst.layout, nr.tail, sr.tail);
                        return;
                    };
                    for (self.aggs, 0..) |agg, j| try dst.layout.merge(self.alloc, rec.tail, sr.tail, j, agg);
                },
                .boxed => |dst| {
                    const sr = src.store.boxed.at(i);
                    const h = src.store.boxed.hashAt(i);
                    const rec = try self.find(dst, h, @as([]const Value, sr.keys)) orelse {
                        const nr = try dst.push(h);
                        if (self.own) {
                            for (nr.keys, sr.keys) |*o, v| o.* = try dupeValue(self.alloc, v);
                        } else @memcpy(nr.keys, sr.keys);
                        try self.adoptTail(dst.layout, nr.tail, sr.tail);
                        return;
                    };
                    for (self.aggs, 0..) |agg, j| try dst.layout.merge(self.alloc, rec.tail, sr.tail, j, agg);
                },
            }
        }

        /// The record for `key`, or null after reserving a slot the caller then fills.
        fn find(self: *GroupMerge, dst: anytype, h: u64, key: anytype) !?@TypeOf(dst.at(0)) {
            const f = try self.table.getOrPut(h, key, dst, @intCast(dst.len));
            if (f.found) return dst.at(f.slot);
            self.set.len += 1;
            return null;
        }

        /// A new group's aggregate state, copied, except a DISTINCT set, which belongs to
        /// the producing fold's arena and is rebuilt here, and with `own` anything else
        /// pointing into the source, so the source set can be freed once merged.
        fn adoptTail(self: *GroupMerge, layout: *const Layout, dst: [*]u8, src: [*]u8) !void {
            @memcpy(dst[0..layout.size], src[0..layout.size]);
            if (!self.any_distinct and !self.own) return;
            for (self.aggs, layout.slots, 0..) |agg, sl, j| {
                if (sl != .full) continue;
                const d = layout.ptr(Acc, dst, j);
                if (agg.distinct) {
                    d.seen = null;
                    d.n = 0;
                    if (layout.ptr(Acc, src, j).seen) |ss| try mergeDistinct(self.alloc, d, ss);
                } else if (self.own) switch (agg.func) {
                    .min, .max => d.ext = try dupeValue(self.alloc, d.ext),
                    .median => if (d.vals) |sv| {
                        d.vals = null;
                        const l = try self.alloc.create(std.array_list.Managed(f64));
                        l.* = try .initCapacity(self.alloc, sv.items.len);
                        l.appendSliceAssumeCapacity(sv.items);
                        d.vals = l;
                    },
                    else => {},
                };
            }
        }

        pub fn result(self: *GroupMerge) GroupSet {
            var set = self.set;
            set.table = null;
            return set;
        }
    };

    /// A type-returning fn, not a `const` type: `std.testing.refAllDeclsRecursive` dives
    /// into container-level types, and diving through this `std.HashMap` built a binary
    /// that segfaulted at startup under Zig 0.15.2.
    pub fn GroupMap() type {
        return std.HashMap([]const Value, usize, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    }

    const Partial = struct {
        nvalid: usize,
        sum_i: i128 = 0,
        sum_f: f64 = 0,
        ext: ?Value = null,
    };

    pub fn next(self: *Aggregate, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        const set = try self.drainSet();
        if (self.by.len != 0 and set.len == 0) return null;
        return try self.emitSets(arena, &.{set}, null);
    }

    /// Fold the whole child into raw per-group accumulators: the parallelizable half of
    /// aggregation, merged across workers by `GroupMerge` and finalized by `emitSets`.
    pub fn drainSet(self: *Aggregate) anyerror!GroupSet {
        std.debug.assert(self.part_state == null);
        return (try self.drainImpl())[0];
    }

    /// As `drainSet`, but one set per radix partition in its own `part_state`
    /// allocator, so the merge takes partition `p` from each lane and frees it once folded.
    pub fn drainParts(self: *Aggregate) anyerror![]GroupSet {
        std.debug.assert(self.part_state.?.len == fold_parts);
        return self.drainImpl();
    }

    pub const fold_parts = 64;
    const part_shift = 58;

    pub fn partOf(h: u64) usize {
        return @intCast(h >> part_shift);
    }

    /// One partition's table and the store it indexes. A serial fold shares one store,
    /// so groups keep first-seen order; a lane's partitions each own theirs.
    fn FoldPart(comptime Store: type) type {
        return struct {
            table: GroupTable,
            store: *Store,
            alloc: std.mem.Allocator,
        };
    }

    /// The tables live in a real allocator, not the arena, so a growing table frees
    /// what it outgrew.
    fn foldParts(self: *Aggregate, comptime Store: type, layout: *const Layout) ![]FoldPart(Store) {
        const parts = try self.state.alloc(FoldPart(Store), fold_parts);
        const tg = self.table_gpa orelse self.gpa;
        var made: usize = 0;
        errdefer for (parts[0..made]) |*p| p.table.deinit();
        if (self.part_state) |ps| {
            for (parts, ps) |*p, a| {
                p.* = .{ .table = try GroupTable.init(tg, 16), .store = try newIn(a, Store.init(a, self.by.len, layout)), .alloc = a };
                made += 1;
            }
        } else {
            const a = self.state;
            const st = try newIn(a, Store.init(a, self.by.len, layout));
            for (parts) |*p| {
                p.* = .{ .table = try GroupTable.init(tg, 16), .store = st, .alloc = a };
                made += 1;
            }
        }
        return parts;
    }

    fn newIn(a: std.mem.Allocator, v: anytype) !*@TypeOf(v) {
        const p = try a.create(@TypeOf(v));
        p.* = v;
        return p;
    }

    fn foldSets(self: *Aggregate, comptime tag: std.meta.Tag(GroupSet.Store), parts: anytype) ![]GroupSet {
        const kinds = try self.keyKinds();
        const own = self.part_state != null;
        const out = try self.state.alloc(GroupSet, if (own) fold_parts else 1);
        for (out, parts[0..out.len]) |*o, *p| o.* = .{
            .store = @unionInit(GroupSet.Store, @tagName(tag), p.store),
            .len = p.store.len,
            .key_kinds = kinds,
            .table = if (own) &p.table else null,
            .strs = self.strs,
        };
        if (!own) for (parts) |*p| p.table.deinit();
        return out;
    }

    fn keyKinds(self: *Aggregate) ![]types.TypeKind {
        const kinds = try self.state.alloc(types.TypeKind, self.by.len);
        for (self.by, kinds) |ci, *k| k.* = self.in_schema.fields[ci].ty.kind;
        return kinds;
    }

    /// Fold with raw fixed-width keys. Floats group by number, not bits, as everywhere
    /// else (-0.0 split from 0.0 before). Below `few_groups` rows go in arrival order;
    /// past it a batch is walked a partition at a time so probes share a table.
    fn drainFixed(self: *Aggregate, kinds: []const KeyKind, comptime counts_only: bool) anyerror![]GroupSet {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        const nparts = fold_parts;
        const counts = try self.state.alloc(u32, nparts + 1);
        const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
        const parts = try self.foldParts(FixedStore, layout);
        errdefer for (parts) |*p| p.table.deinit();
        const nk = self.by.len;

        const direct: ?Direct = if (nk == 1 and kinds[0] != .f64k and kinds[0] != .boolk) try Direct.init(self.gpa) else null;
        var has_str = false;
        for (kinds) |k| if (k == .strk) {
            has_str = true;
        };
        defer if (direct) |d| d.deinit(self.gpa);
        var direct_base: ?i64 = null;

        while (try self.child.next(pull)) |b| {
            const keys = try pull.alloc(i64, b.len * nk);
            const hkeys = if (has_str) try pull.alloc(i64, b.len * nk) else keys;
            const masks = try pull.alloc(u64, b.len);
            const hashes = try pull.alloc(u64, b.len);
            const tails = try pull.alloc(?[*]u8, b.len);
            const pidx = try pull.alloc(u8, b.len);
            @memset(masks, 0);
            @memset(tails, null);

            for (self.by, kinds, 0..) |ci, kk, j| {
                const col = b.columns[ci];
                switch (kk) {
                    .i64k => for (0..b.len) |r| {
                        keys[r * nk + j] = col.data.i64[r];
                    },
                    .i32k => for (0..b.len) |r| {
                        keys[r * nk + j] = col.data.i32[r];
                    },
                    .boolk => for (0..b.len) |r| {
                        keys[r * nk + j] = @intFromBool(col.data.b[r]);
                    },
                    .f64k => for (0..b.len) |r| {
                        keys[r * nk + j] = @bitCast(keyhash.canonF64(col.data.f64[r]));
                    },
                    .strk => {
                        const table = try self.strTable();
                        if (col.dict) |d| {
                            const ents = try pull.alloc(StrId, d.values.len);
                            for (ents, d.values) |*e, v| e.* = try self.strId(table, v);
                            for (0..b.len) |r| {
                                if (!col.validity.get(r)) continue;
                                const e = ents[d.codes[r]];
                                keys[r * nk + j] = e.id;
                                hkeys[r * nk + j] = @bitCast(e.h);
                            }
                        } else for (0..b.len) |r| {
                            if (!col.validity.get(r)) continue;
                            const e = try self.strId(table, col.data.bytes.at(r));
                            keys[r * nk + j] = e.id;
                            hkeys[r * nk + j] = @bitCast(e.h);
                        }
                    },
                }
                if (!col.validity.allSet(b.len)) for (0..b.len) |r| {
                    if (col.validity.get(r)) continue;
                    masks[r] |= @as(u64, 1) << @intCast(j);
                    keys[r * nk + j] = 0;
                    if (has_str) hkeys[r * nk + j] = 0;
                };
                if (has_str and kk != .strk) for (0..b.len) |r| {
                    hkeys[r * nk + j] = keys[r * nk + j];
                };
            }
            if (direct) |d| {
                if (direct_base == null) direct_base = Direct.baseFor(keys, masks);
                const base = direct_base.?;
                for (keys, masks, tails, pidx) |k, m, *t, *pi| {
                    if (m != 0) continue;
                    const off = k -% base;
                    if (off < 0 or off >= Direct.len) continue;
                    const e = d.recs[@intCast(off)] orelse continue;
                    t.* = e;
                    pi.* = d.parts[@intCast(off)];
                }
            }
            var r: usize = 0;
            while (r < b.len) : (r += 1) hashes[r] = if (tails[r] != null) 0 else fixedHash(hkeys[r * nk ..][0..nk], masks[r]);

            const argcols = try pull.alloc(?column.Column, self.aggs.len);
            const fast = try pull.alloc(Fast, self.aggs.len);
            for (self.aggs, argcols, fast, layout.slots) |agg, *c, *f, sl| {
                c.* = if (agg.arg) |e| try self.argColumn(pull, agg, e, b) else null;
                f.* = Fast.of(sl, agg, c.*, b.len);
            }

            var groups: usize = 0;
            if (self.part_state == null) groups = parts[0].store.len else for (parts) |*pp| {
                groups += pp.store.len;
            }
            const order = try pull.alloc(u32, b.len);
            if (groups < few_groups) {
                for (order, 0..) |*o, ri| o.* = @intCast(ri);
            } else {
                @memset(counts, 0);
                for (hashes[0..b.len]) |h| counts[(h >> part_shift) + 1] += 1;
                for (1..nparts + 1) |ci| counts[ci] += counts[ci - 1];
                for (hashes[0..b.len], 0..) |h, ri| {
                    const pi = h >> part_shift;
                    order[counts[pi]] = @intCast(ri);
                    counts[pi] += 1;
                }
            }

            for (order, 0..) |ri, oi| {
                if (tails[ri] != null) continue;
                if (oi + prefetch_ahead < order.len) {
                    const pr = order[oi + prefetch_ahead];
                    parts[hashes[pr] >> part_shift].table.prefetch(hashes[pr]);
                }
                const key = FixedKey{ .vals = keys[ri * nk ..][0..nk], .mask = masks[ri] };
                const pi = hashes[ri] >> part_shift;
                const part = &parts[pi];
                const at: u32 = @intCast(part.store.len);
                const f = try part.table.getOrPut(hashes[ri], key, part.store, at);
                const rec = if (f.found) part.store.at(f.slot) else blk: {
                    const nr = try part.store.push(hashes[ri]);
                    @memcpy(nr.keys, key.vals);
                    nr.mask.* = key.mask;
                    break :blk nr;
                };
                tails[ri] = rec.tail;
                pidx[ri] = @intCast(pi);
                if (direct) |d| if (key.mask == 0) {
                    const off = key.vals[0] -% direct_base.?;
                    if (off >= 0 and off < Direct.len) {
                        d.recs[@intCast(off)] = rec.tail;
                        d.parts[@intCast(off)] = @intCast(pi);
                    }
                };
            }

            if (counts_only) {
                for (tails[0..b.len]) |t| {
                    const cs: [*]i64 = @ptrCast(@alignCast(t.?));
                    for (cs[0..self.aggs.len]) |*c| c.* += 1;
                }
            } else for (self.aggs, fast, 0..) |agg, fk, j| switch (fk) {
                .count_star, .count_all_valid => for (tails[0..b.len]) |t| {
                    layout.ptr(i64, t.?, j).* += 1;
                },
                .sum_i64 => {
                    const col = argcols[j].?.data.i64;
                    for (tails[0..b.len], col[0..b.len]) |t, v| {
                        const x = layout.ptr(SumI, t.?, j);
                        x.s += v;
                        x.n += 1;
                    }
                },
                .sum_f64 => {
                    const col = argcols[j].?.data.f64;
                    for (tails[0..b.len], col[0..b.len]) |t, v| {
                        const x = layout.ptr(SumF, t.?, j);
                        x.s += v;
                        x.n += 1;
                    }
                },
                .sum_f_of_i64 => {
                    const col = argcols[j].?.data.i64;
                    for (tails[0..b.len], col[0..b.len]) |t, v| {
                        const x = layout.ptr(SumF, t.?, j);
                        x.s += @floatFromInt(v);
                        x.n += 1;
                    }
                },
                .generic => for (tails[0..b.len], pidx[0..b.len], 0..) |t, pi, ri| {
                    const v = if (argcols[j]) |col| col.getValue(ri) else Value.null;
                    try layout.update(parts[pi].alloc, t.?, j, agg, v);
                },
            };
            _ = scratch.reset(.retain_capacity);
        }
        return self.foldSets(.fixed, parts);
    }

    /// Fold with boxed keys: each row's keys boxed once, the batch hashed then probed
    /// behind a prefetch, each argument evaluated once per batch, rows walked by partition.
    fn drainImpl(self: *Aggregate) anyerror![]GroupSet {
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
            const one = try self.state.alloc(GroupSet, 1);
            one[0] = .{ .store = .{ .single = accs }, .len = 1, .key_kinds = &.{} };
            return one;
        }

        const nparts = fold_parts;
        const counts = try self.state.alloc(u32, nparts + 1);
        const layout = try newIn(self.state, try Layout.init(self.state, self.aggs));
        const parts = try self.foldParts(GroupStore, layout);
        errdefer for (parts) |*p| p.table.deinit();
        const hctx = keyhash.MultiKeyCtx{};
        while (try self.child.next(pull)) |b| {
            const nk = self.by.len;
            const probes = try pull.alloc(Value, b.len * nk);
            const hashes = try pull.alloc(u64, b.len);

            var r: usize = 0;
            while (r < b.len) : (r += 1) {
                const probe = probes[r * nk ..][0..nk];
                for (self.by, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                hashes[r] = hctx.hash(probe);
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

            for (order, 0..) |ri, oi| {
                if (oi + prefetch_ahead < order.len) {
                    const pr = order[oi + prefetch_ahead];
                    parts[hashes[pr] >> part_shift].table.prefetch(hashes[pr]);
                }
                r = ri;
                const probe = probes[r * nk ..][0..nk];
                const part = &parts[hashes[r] >> part_shift];
                const at: u32 = @intCast(part.store.len);
                const f = try part.table.getOrPut(hashes[r], probe, part.store, at);
                const rec = if (f.found) part.store.at(f.slot) else blk: {
                    const nr = try part.store.push(hashes[r]);
                    for (probe, nr.keys) |v, *o| o.* = try dupeValue(part.alloc, v);
                    break :blk nr;
                };
                for (self.aggs, 0..) |agg, j| {
                    const v = if (argcols[j]) |col| col.getValue(r) else Value.null;
                    try layout.update(part.alloc, rec.tail, j, agg, v);
                }
            }
            _ = scratch.reset(.retain_capacity);
        }
        return self.foldSets(.boxed, parts);
    }

    const prefetch_ahead = 8;

    const Direct = struct {
        recs: []?[*]u8,
        parts: []u8,

        const len = 1 << 14;

        fn init(gpa: std.mem.Allocator) !Direct {
            const recs = try gpa.alloc(?[*]u8, len);
            @memset(recs, null);
            const parts = try gpa.alloc(u8, len);
            return .{ .recs = recs, .parts = parts };
        }

        fn deinit(self: Direct, gpa: std.mem.Allocator) void {
            gpa.free(self.recs);
            gpa.free(self.parts);
        }

        /// The smallest non-null key, so a range starting at 0 or 1 sits wholly inside.
        fn baseFor(keys: []const i64, masks: []const u64) i64 {
            var lo: i64 = std.math.maxInt(i64);
            for (keys, masks) |k, m| if (m == 0 and k < lo) {
                lo = k;
            };
            return if (lo == std.math.maxInt(i64)) 0 else lo;
        }
    };

    const few_groups = 1 << 12;

    /// Each word through murmur3's 64-bit finalizer. Wyhash's streaming form was a fifth
    /// of a 100-group GROUP BY.
    fn fixedHash(vals: []const i64, mask: u64) u64 {
        var h: u64 = 0x9E3779B97F4A7C15 ^ mask;
        for (vals) |v| h = fmix64(h ^ (@as(u64, @bitCast(v)) *% 0xC2B2AE3D27D4EB4F));
        return h;
    }

    fn fmix64(x0: u64) u64 {
        var x = x0;
        x ^= x >> 33;
        x *%= 0xff51afd7ed558ccd;
        x ^= x >> 33;
        x *%= 0xc4ceb9fe1a85ec53;
        x ^= x >> 33;
        return x;
    }

    const Fast = enum {
        count_star,
        count_all_valid,
        sum_i64,
        sum_f64,
        sum_f_of_i64,
        generic,

        fn of(slot: Slot, agg: Agg, col: ?column.Column, n: usize) Fast {
            const c = col orelse return if (slot == .count) .count_star else .generic;
            if (!c.validity.allSet(n)) return .generic;
            return switch (slot) {
                .count => .count_all_valid,
                .sum_i => if (agg.ty.kind != .decimal and c.ty.kind == .int) .sum_i64 else .generic,
                .sum_f => switch (c.ty.kind) {
                    .float => .sum_f64,
                    .int => .sum_f_of_i64,
                    else => .generic,
                },
                .full => .generic,
            };
        }
    };

    const KeyKind = enum { i64k, i32k, boolk, f64k, strk };

    fn keyKindOf(kind: types.TypeKind) ?KeyKind {
        return switch (kind) {
            .int, .time, .timestamp => .i64k,
            .date => .i32k,
            .bool => .boolk,
            .float => .f64k,
            .string => .strk,
            else => null,
        };
    }

    const RecBlocks = struct {
        const first_shift = 4;
        const block_shift = 13;
        const block = 1 << block_shift;
        const small = block_shift - first_shift + 1;

        alloc: std.mem.Allocator,
        rec_size: usize,
        blocks: std.array_list.Managed([]u8),

        fn init(alloc: std.mem.Allocator, rec_size: usize) RecBlocks {
            return .{ .alloc = alloc, .rec_size = rec_size, .blocks = .init(alloc) };
        }

        const Loc = struct { b: usize, off: usize };

        fn locate(i: usize) Loc {
            if (i >= block) return .{ .b = small - 1 + (i >> block_shift), .off = i & (block - 1) };
            if (i < 1 << first_shift) return .{ .b = 0, .off = i };
            const k = std.math.log2_int(usize, i >> first_shift);
            return .{ .b = k + 1, .off = i - (@as(usize, 1) << first_shift << k) };
        }

        fn capOf(b: usize) usize {
            if (b >= small) return block;
            return @as(usize, 1) << @intCast(first_shift + @max(b, 1) - 1);
        }

        fn base(self: RecBlocks, i: usize) [*]u8 {
            const l = locate(i);
            return self.blocks.items[l.b].ptr + l.off * self.rec_size;
        }

        fn reserve(self: *RecBlocks, i: usize) !void {
            const l = locate(i);
            if (l.b < self.blocks.items.len) return;
            try self.blocks.append(try self.alloc.alignedAlloc(u8, .of(Value), capOf(l.b) * self.rec_size));
        }
    };

    const FixedStore = struct {
        nkeys: usize,
        layout: *const Layout,
        recs: RecBlocks,
        len: usize = 0,

        const Rec = struct { keys: []i64, mask: *u64, tail: [*]u8 };

        fn init(alloc: std.mem.Allocator, nkeys: usize, layout: *const Layout) FixedStore {
            return .{ .nkeys = nkeys, .layout = layout, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(i64) + @sizeOf(u64) + layout.size) };
        }

        fn hashAt(self: *const FixedStore, i: usize) u64 {
            return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
        }

        fn at(self: FixedStore, i: usize) Rec {
            const base = self.recs.base(i) + @sizeOf(u64);
            const ksz = self.nkeys * @sizeOf(i64);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(i64, base[0..ksz])),
                .mask = @ptrCast(@alignCast(base + ksz)),
                .tail = base + ksz + @sizeOf(u64),
            };
        }

        /// One address computation for hash, mask and keys; going through `hashAt` and `at`
        /// located the record twice per probe.
        fn eqlAt(self: *const FixedStore, i: usize, h: u64, key: FixedKey) bool {
            const base = self.recs.base(i);
            const words: [*]const u64 = @ptrCast(@alignCast(base));
            if (words[0] != h) return false;
            const vals: [*]const i64 = @ptrCast(@alignCast(base + @sizeOf(u64)));
            for (key.vals, 0..) |v, j| if (vals[j] != v) return false;
            return words[1 + key.vals.len] == key.mask;
        }

        fn push(self: *FixedStore, h: u64) !Rec {
            try self.recs.reserve(self.len);
            @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
            const rec = self.at(self.len);
            self.len += 1;
            self.layout.clear(rec.tail);
            return rec;
        }
    };

    const FixedKey = struct { vals: []const i64, mask: u64 };

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

        /// Salt comes from bits the bucket index does not use, so the two stay independent.
        fn saltOf(h: u64) u32 {
            return @intCast((h >> 32) & ((1 << salt_bits) - 1));
        }

        fn pack(h: u64, idx: usize) u32 {
            return (saltOf(h) << idx_bits) | @as(u32, @intCast(idx + 1));
        }

        fn prefetch(self: *const GroupTable, h: u64) void {
            @prefetch(&self.entries[h & self.mask], .{ .rw = .read, .locality = 3 });
        }

        /// Free the entries. Safe to repeat: a table handed on by `GroupMerge.adopt` is left
        /// empty and its owner's cleanup still runs.
        fn deinit(self: *GroupTable) void {
            self.alloc.free(self.entries);
            self.entries = &.{};
        }

        /// Rebuild from this table's own entries, taking each hash from its record, and
        /// free the outgrown entries (an arena kept every outgrown table).
        fn grow(self: *GroupTable, store: anytype) !void {
            const cap = self.entries.len * growth;
            const ne = try self.alloc.alloc(u32, cap);
            @memset(ne, 0);
            const nmask = cap - 1;
            for (self.entries) |e| {
                if (e == 0) continue;
                const h = store.hashAt((e & max_groups) - 1);
                var i = h & nmask;
                while (ne[i] != 0) i = (i + 1) & nmask;
                ne[i] = e;
            }
            self.alloc.free(self.entries);
            self.entries = ne;
            self.mask = nmask;
        }

        const growth = 2;

        const Found = struct { slot: u32, found: bool };

        inline fn getOrPut(
            self: *GroupTable,
            h: u64,
            key: anytype,
            store: anytype,
            new_slot: u32,
        ) !Found {
            if (new_slot >= max_groups) return error.TooManyGroups;
            if ((self.len + 1) * 10 >= self.entries.len * 7) try self.grow(store);
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
                if (store.eqlAt(idx, h, key)) return .{ .slot = idx, .found = true };
            }
        }
    };

    const GroupStore = struct {
        nkeys: usize,
        layout: *const Layout,
        recs: RecBlocks,
        len: usize = 0,

        const Rec = struct { keys: []Value, tail: [*]u8 };

        fn init(alloc: std.mem.Allocator, nkeys: usize, layout: *const Layout) GroupStore {
            return .{ .nkeys = nkeys, .layout = layout, .recs = .init(alloc, @sizeOf(u64) + nkeys * @sizeOf(Value) + layout.size) };
        }

        fn hashAt(self: *const GroupStore, i: usize) u64 {
            return @as(*const u64, @ptrCast(@alignCast(self.recs.base(i)))).*;
        }

        fn at(self: GroupStore, i: usize) Rec {
            const base = self.recs.base(i) + @sizeOf(u64);
            const ksz = self.nkeys * @sizeOf(Value);
            return .{
                .keys = @alignCast(std.mem.bytesAsSlice(Value, base[0..ksz])),
                .tail = base + ksz,
            };
        }

        fn eqlAt(self: *const GroupStore, i: usize, h: u64, key: []const Value) bool {
            if (self.hashAt(i) != h) return false;
            return keyhash.MultiKeyCtx.eql(.{}, key, self.at(i).keys);
        }

        fn push(self: *GroupStore, h: u64) !Rec {
            try self.recs.reserve(self.len);
            @as(*u64, @ptrCast(@alignCast(self.recs.base(self.len)))).* = h;
            const rec = self.at(self.len);
            self.len += 1;
            self.layout.clear(rec.tail);
            return rec;
        }
    };

    /// Combine partial `src` into `dst` for one agg, the dual of `updateAcc`. `dst_alloc`
    /// owns any MIN/MAX string carried over.
    pub fn mergeAcc(dst_alloc: std.mem.Allocator, dst: *Acc, src: Acc, agg: Agg) !void {
        switch (agg.func) {
            .count => if (agg.distinct) {
                if (src.seen) |s| try mergeDistinct(dst_alloc, dst, s);
            } else {
                dst.n += src.n;
            },
            .sum => {
                if (agg.ty.kind == .float)
                    dst.sum_f += src.sum_f
                else
                    dst.sum_i = std.math.add(i128, dst.sum_i, src.sum_i) catch return error.IntOverflow;
                dst.n += src.n;
            },
            .avg => {
                dst.sum_f += src.sum_f;
                dst.n += src.n;
            },
            .median => if (src.vals) |s| {
                for (s.items) |x| try noteMedian(dst_alloc, dst, x);
            },
            .count_if => dst.n += src.n,
            .bool_and, .bool_or => {
                dst.n += src.n;
                dst.sum_i += src.sum_i;
            },
            .bit_and, .bit_or, .bit_xor => if (src.n > 0) {
                dst.sum_i = if (dst.n == 0) src.sum_i else bitFold(agg.func, dst.sum_i, src.sum_i);
                dst.n += src.n;
            },
            .var_samp, .var_pop, .stddev_samp, .stddev_pop => mergeMoments(dst, src),
            .min => if (!src.ext.isNull() and (dst.ext.isNull() or lessV(src.ext, dst.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
            },
            .max => if (!src.ext.isNull() and (dst.ext.isNull() or lessV(dst.ext, src.ext))) {
                dst.ext = try dupeValue(dst_alloc, src.ext);
            },
        }
    }

    /// Evaluate every agg's argument as a column once and SIMD-reduce it. False,
    /// touching nothing, if any agg is not covered; that depends only on the schema.
    fn foldVectorized(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!bool {
        for (self.aggs) |agg| if (agg.distinct) return false;
        const partials = try arena.alloc(Partial, self.aggs.len);
        for (self.aggs, partials) |agg, *p| {
            p.* = (try self.reduceBatch(arena, agg, b)) orelse return false;
        }
        for (self.aggs, partials, accs) |agg, p, *acc| try mergePartial(acc, agg, p);
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

    /// What a text argument is cast to before it reaches the accumulator, or null for
    /// COUNT, MIN and MAX. Parallel CSV lanes carry raw text, so the cast happens here.
    fn argCast(agg: Agg) ?types.Type {
        return switch (aggregates.spec(agg.func).arg) {
            .star_or_any, .any => null,
            .numeric => agg.ty.asNullable(),
            .int => types.Type.init(.int).asNullable(),
            .bool => types.Type.init(.bool).asNullable(),
        };
    }

    /// One agg's argument across a batch, a text column cast per `argCast`. A stored
    /// column is handed over uncopied when its type feeds the accumulator; others are
    /// built as the type it reads (`count_if` folds BOOLs into an INT).
    fn argColumn(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, e: *const ast.Expr, b: Batch) anyerror!column.Column {
        const want = argCast(agg) orelse agg.ty;
        if (e.* == .field) {
            if (b.schema.resolve(e.field.parts)) |ci| {
                const ck = b.columns[ci].ty.kind;
                const ak = want.kind;
                if (ck == ak or (ck.isNumeric() and ak.isNumeric())) return b.columns[ci];
            }
        }
        const col = eval.evalColumn(arena, e, b, want) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
        if (col.ty.kind != .string) return col;
        const to = argCast(agg) orelse return col;
        if (to.kind == .string) return col;
        var out = try column.Builder.initCapacity(arena, to, b.len);
        for (0..b.len) |r| try out.append(try self.castArg(arena, to, col.getValue(r)));
        return out.finish();
    }

    fn argValue(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch, r: usize) anyerror!Value {
        const e = agg.arg orelse return .null;
        const v = eval.evalRow(arena, e, b, r) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
        if (v != .string) return v;
        const to = argCast(agg) orelse return v;
        if (to.kind == .string) return v;
        return self.castArg(arena, to, v);
    }

    fn castArg(self: *Aggregate, arena: std.mem.Allocator, to: types.Type, v: Value) anyerror!Value {
        if (v.isNull()) return v;
        return eval.castValueTyped(arena, v, to) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
    }

    /// Vectorized reduce of one agg over one batch; null means fold row-wise. A null
    /// slot holds whatever its producer left, so only an all-valid batch is summed blind.
    fn reduceBatch(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch) anyerror!?Partial {
        if (agg.func == .count and agg.arg == null) return Partial{ .nvalid = b.len };
        if (foldsRowwise(agg.func)) return null;
        const e = agg.arg orelse return null;
        const col = eval.evalColumn(arena, e, b, agg.ty) catch |err| {
            if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
            return err;
        };
        if (col.ty.kind != .int and col.ty.kind != .float) return null;
        const n = b.len;
        const nvalid = simd.popcountValid(col.validity.bits, n);
        var p = Partial{ .nvalid = nvalid };
        if (nvalid == 0) return p;
        switch (agg.func) {
            .count => {},
            .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => unreachable,
            .sum, .avg => switch (col.ty.kind) {
                .float => p.sum_f = if (nvalid == n) simd.sumF(col.data.f64[0..n]) else validSumF(col, n),
                .int => {
                    p.sum_i = if (nvalid == n) sumIntCol(col.data.i64[0..n]) else validSumI(col, n);
                    p.sum_f = @floatFromInt(p.sum_i);
                },
                else => unreachable,
            },
            .min, .max => p.ext = reduceExtreme(col, agg.func, n),
        }
        return p;
    }

    /// Whether `reduceBatch` leaves an aggregate to the row-wise fold, decided before
    /// evaluating a column it would only discard.
    fn foldsRowwise(func: ast.AggFunc) bool {
        return switch (func) {
            .count, .sum, .avg, .min, .max => false,
            .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => true,
        };
    }

    /// Fold one batch's `Partial` into the running accumulator, with `updateAcc`'s
    /// semantics.
    fn mergePartial(acc: *Acc, agg: Agg, p: Partial) error{IntOverflow}!void {
        switch (agg.func) {
            .count => acc.n += @intCast(p.nvalid),
            .sum => if (p.nvalid > 0) {
                if (agg.ty.kind == .float)
                    acc.sum_f += p.sum_f
                else
                    acc.sum_i = std.math.add(i128, acc.sum_i, p.sum_i) catch return error.IntOverflow;
                acc.n += @intCast(p.nvalid);
            },
            .avg => if (p.nvalid > 0) {
                acc.sum_f += p.sum_f;
                acc.n += @intCast(p.nvalid);
            },
            .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => unreachable,
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

    /// One output row per group of `sets`, or with `sel` only the groups `sel[i]` lists
    /// for set `i` (a top-N's survivors).
    pub fn emitSets(self: *Aggregate, arena: std.mem.Allocator, sets: []const GroupSet, sel: ?[]const []const u32) anyerror!Batch {
        const nfields = self.out_schema.fields.len;
        var n: usize = 0;
        for (sets, 0..) |*st, si| n += if (sel) |sl| sl[si].len else st.len;
        const builders = try arena.alloc(column.Builder, nfields);
        for (builders, self.out_schema.fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, @max(n, 1));

        if (n == 0 and self.by.len == 0) {
            for (self.aggs, 0..) |agg, j| try builders[j].append(try finalizeAcc(.{}, agg));
            n = 1;
        } else {
            for (sets, 0..) |*st, si| {
                if (sel) |sl| {
                    for (sl[si]) |gi| try self.emitGroup(builders, st, gi);
                } else {
                    for (0..st.len) |gi| try self.emitGroup(builders, st, gi);
                }
            }
        }

        const cols = try arena.alloc(column.Column, nfields);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }

    fn emitGroup(self: *Aggregate, builders: []column.Builder, st: *const GroupSet, gi: usize) !void {
        const nk = self.by.len;
        for (builders[0..nk], 0..) |*b, j| try b.append(st.keyValue(gi, j));
        for (self.aggs, builders[nk..], 0..) |agg, *b, j| {
            try b.append(finalizeAcc(st.acc(gi, j), agg) catch |err| {
                if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
                return err;
            });
        }
    }

    /// `state` owns any string extremum copied into the accumulator, since the value
    /// must outlive the per-pull arena.
    fn updateAcc(state: std.mem.Allocator, acc: *Acc, agg: Agg, v: Value, has_arg: bool) !void {
        switch (agg.func) {
            .count => {
                if (agg.distinct) {
                    if (!v.isNull()) try noteDistinct(state, acc, v);
                } else if (!has_arg or !v.isNull()) acc.n += 1;
            },
            .sum => if (!v.isNull()) {
                if (agg.ty.kind == .float) acc.sum_f += eval.toF64(v) else try addExact(&acc.sum_i, agg, v);
                acc.n += 1;
            },
            .avg => if (!v.isNull()) {
                acc.sum_f += eval.toF64(v);
                acc.n += 1;
            },
            .median => if (!v.isNull()) try noteMedian(state, acc, eval.toF64(v)),
            .count_if => if (v == .bool and v.bool) {
                acc.n += 1;
            },
            .bool_and, .bool_or => if (!v.isNull()) {
                acc.n += 1;
                if (v == .bool and v.bool) acc.sum_i += 1;
            },
            .bit_and, .bit_or, .bit_xor => if (!v.isNull()) {
                if (v != .int) return error.TypeMismatch;
                acc.sum_i = if (acc.n == 0) v.int else bitFold(agg.func, acc.sum_i, v.int);
                acc.n += 1;
            },
            .var_samp, .var_pop, .stddev_samp, .stddev_pop => if (!v.isNull()) noteMoment(acc, eval.toF64(v)),
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

    /// Add a non-null value to an int or DECIMAL SUM, normalized to the output scale.
    /// Adding raw unscaled values multiplied the sum by 10^(declared - actual).
    fn addExact(sum: *align(8) i128, agg: Agg, v: Value) !void {
        if (agg.ty.kind == .decimal) {
            const d: Decimal = if (v == .decimal) v.decimal else .{ .unscaled = v.int, .scale = 0 };
            const r = eval.rescaleTo(d, agg.ty.scale) orelse return error.CastFailed;
            sum.* = std.math.add(i128, sum.*, r.unscaled) catch return error.CastFailed;
        } else sum.* = std.math.add(i128, sum.*, v.int) catch return error.IntOverflow;
    }

    pub const Slot = enum {
        count,
        sum_i,
        sum_f,
        full,

        fn of(agg: Agg) Slot {
            if (agg.distinct) return .full;
            return switch (agg.func) {
                .count => .count,
                .sum => if (agg.ty.kind == .float) .sum_f else .sum_i,
                .avg => .sum_f,
                else => .full,
            };
        }

        fn size(self: Slot) usize {
            return switch (self) {
                .count => @sizeOf(i64),
                .sum_i => @sizeOf(SumI),
                .sum_f => @sizeOf(SumF),
                .full => @sizeOf(Acc),
            };
        }
    };
    const SumI = struct { n: i64, s: i128 align(8) };
    const SumF = struct { n: i64, s: f64 };

    pub const Layout = struct {
        slots: []const Slot,
        offs: []const usize,
        size: usize,
        all_count: bool,

        fn init(a: std.mem.Allocator, aggs: []const Agg) !Layout {
            const slots = try a.alloc(Slot, aggs.len);
            const offs = try a.alloc(usize, aggs.len);
            var at: usize = 0;
            var all_count = true;
            for (aggs, slots, offs) |agg, *sl, *o| {
                sl.* = Slot.of(agg);
                if (sl.* != .count or agg.arg != null) all_count = false;
                o.* = at;
                at += sl.size();
            }
            return .{ .slots = slots, .offs = offs, .size = at, .all_count = all_count };
        }

        fn ptr(self: Layout, comptime T: type, tail: [*]u8, j: usize) *T {
            return @ptrCast(@alignCast(tail + self.offs[j]));
        }

        fn clear(self: Layout, tail: [*]u8) void {
            for (self.slots, 0..) |sl, j| switch (sl) {
                .count => self.ptr(i64, tail, j).* = 0,
                .sum_i => self.ptr(SumI, tail, j).* = .{ .n = 0, .s = 0 },
                .sum_f => self.ptr(SumF, tail, j).* = .{ .n = 0, .s = 0 },
                .full => self.ptr(Acc, tail, j).* = .{},
            };
        }

        fn acc(self: Layout, tail: [*]u8, j: usize) Acc {
            return switch (self.slots[j]) {
                .count => .{ .n = self.ptr(i64, tail, j).* },
                .sum_i => blk: {
                    const x = self.ptr(SumI, tail, j).*;
                    break :blk .{ .n = x.n, .sum_i = x.s };
                },
                .sum_f => blk: {
                    const x = self.ptr(SumF, tail, j).*;
                    break :blk .{ .n = x.n, .sum_f = x.s };
                },
                .full => self.ptr(Acc, tail, j).*,
            };
        }

        fn update(self: Layout, state: std.mem.Allocator, tail: [*]u8, j: usize, agg: Agg, v: Value) !void {
            switch (self.slots[j]) {
                .count => if (agg.arg == null or !v.isNull()) {
                    self.ptr(i64, tail, j).* += 1;
                },
                .sum_i => if (!v.isNull()) {
                    const x = self.ptr(SumI, tail, j);
                    try addExact(&x.s, agg, v);
                    x.n += 1;
                },
                .sum_f => if (!v.isNull()) {
                    const x = self.ptr(SumF, tail, j);
                    x.s += eval.toF64(v);
                    x.n += 1;
                },
                .full => try updateAcc(state, self.ptr(Acc, tail, j), agg, v, agg.arg != null),
            }
        }

        fn merge(self: Layout, alloc: std.mem.Allocator, dst: [*]u8, src: [*]u8, j: usize, agg: Agg) !void {
            switch (self.slots[j]) {
                .count => self.ptr(i64, dst, j).* += self.ptr(i64, src, j).*,
                .sum_i => {
                    const d = self.ptr(SumI, dst, j);
                    const x = self.ptr(SumI, src, j).*;
                    d.s = std.math.add(i128, d.s, x.s) catch return error.IntOverflow;
                    d.n += x.n;
                },
                .sum_f => {
                    const d = self.ptr(SumF, dst, j);
                    const x = self.ptr(SumF, src, j).*;
                    d.s += x.s;
                    d.n += x.n;
                },
                .full => try mergeAcc(alloc, self.ptr(Acc, dst, j), self.ptr(Acc, src, j).*, agg),
            }
        }
    };

    /// The aggregate's final value. MEDIAN uses selection, and with an even count the
    /// mean of the two middle values, as Postgres's percentile_cont(0.5) does.
    pub fn finalizeAcc(acc: Acc, agg: Agg) error{IntOverflow}!Value {
        return switch (agg.func) {
            .count => .{ .int = acc.n },
            .sum => if (acc.n == 0) .null else switch (agg.ty.kind) {
                .float => Value{ .float = acc.sum_f },
                .decimal => Value{ .decimal = .{ .unscaled = acc.sum_i, .scale = agg.ty.scale } },
                else => Value{ .int = std.math.cast(i64, acc.sum_i) orelse return error.IntOverflow },
            },
            .avg => if (acc.n == 0) .null else Value{ .float = acc.sum_f / @as(f64, @floatFromInt(acc.n)) },
            .median => blk: {
                const l = acc.vals orelse break :blk Value.null;
                const xs = l.items;
                if (xs.len == 0) break :blk Value.null;
                const mid = xs.len / 2;
                const hi = selectNth(xs, mid);
                if (xs.len % 2 == 1) break :blk Value{ .float = hi };
                var lo = xs[0];
                for (xs[1..mid]) |x| lo = @max(lo, x);
                break :blk Value{ .float = (lo + hi) / 2 };
            },
            .min, .max => acc.ext,
            .count_if => .{ .int = acc.n },
            .bool_and => if (acc.n == 0) .null else Value{ .bool = acc.sum_i == acc.n },
            .bool_or => if (acc.n == 0) .null else Value{ .bool = acc.sum_i > 0 },
            .bit_and, .bit_or, .bit_xor => if (acc.n == 0) .null else Value{ .int = @intCast(acc.sum_i) },
            .var_samp, .stddev_samp => if (acc.n < 2) .null else spread(agg.func, sqDev(acc) / @as(f64, @floatFromInt(acc.n - 1))),
            .var_pop, .stddev_pop => if (acc.n == 0) .null else spread(agg.func, sqDev(acc) / @as(f64, @floatFromInt(acc.n))),
        };
    }

    fn bitFold(func: ast.AggFunc, a: i128, b: i128) i128 {
        return switch (func) {
            .bit_and => a & b,
            .bit_or => a | b,
            .bit_xor => a ^ b,
            else => unreachable,
        };
    }

    fn sqDev(acc: Acc) f64 {
        return if (acc.ext == .float) acc.ext.float else 0;
    }

    /// Welford's update. `ext` is read before it is assigned: assigning the union may
    /// set its tag first and read back a garbage float.
    fn noteMoment(acc: *Acc, x: f64) void {
        const m2 = sqDev(acc.*);
        acc.n += 1;
        const delta = x - acc.sum_f;
        acc.sum_f += delta / @as(f64, @floatFromInt(acc.n));
        acc.ext = .{ .float = m2 + delta * (x - acc.sum_f) };
    }

    /// Chan et al.'s pairwise combination of two Welford states.
    fn mergeMoments(dst: *Acc, src: Acc) void {
        if (src.n == 0) return;
        if (dst.n == 0) {
            dst.n = src.n;
            dst.sum_f = src.sum_f;
            dst.ext = .{ .float = sqDev(src) };
            return;
        }
        const na: f64 = @floatFromInt(dst.n);
        const nb: f64 = @floatFromInt(src.n);
        const n = na + nb;
        const delta = src.sum_f - dst.sum_f;
        const m2 = sqDev(dst.*) + sqDev(src) + delta * delta * na * nb / n;
        dst.ext = .{ .float = m2 };
        dst.sum_f += delta * nb / n;
        dst.n += src.n;
    }

    fn spread(func: ast.AggFunc, variance: f64) Value {
        return .{ .float = switch (func) {
            .stddev_samp, .stddev_pop => @sqrt(variance),
            else => variance,
        } };
    }
};

fn lessV(a: Value, b: Value) bool {
    return (eval.compareValues(a, b) orelse .eq) == .lt;
}

/// Quickselect: `xs[k]` becomes the k-th smallest, smaller before, larger after.
/// Median-of-three pivot, Hoare partition; expected O(n).
fn selectNth(xs: []f64, k: usize) f64 {
    var lo: usize = 0;
    var hi: usize = xs.len - 1;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (xs[mid] < xs[lo]) std.mem.swap(f64, &xs[mid], &xs[lo]);
        if (xs[hi] < xs[lo]) std.mem.swap(f64, &xs[hi], &xs[lo]);
        if (xs[hi] < xs[mid]) std.mem.swap(f64, &xs[hi], &xs[mid]);
        const p = xs[mid];
        var i = lo;
        var j = hi;
        while (i <= j) {
            while (xs[i] < p) i += 1;
            while (p < xs[j]) j -= 1;
            if (i <= j) {
                std.mem.swap(f64, &xs[i], &xs[j]);
                i += 1;
                if (j == 0) break;
                j -= 1;
            }
        }
        if (k <= j) {
            hi = j;
        } else if (k >= i) {
            lo = i;
        } else {
            return xs[k];
        }
    }
    return xs[k];
}

pub fn dupeValue(state: std.mem.Allocator, v: Value) !Value {
    return switch (v) {
        .string => |s| .{ .string = try state.dupe(u8, s) },
        .bytes => |s| .{ .bytes = try state.dupe(u8, s) },
        else => v,
    };
}

/// Sum an i64 column exactly in i128, branch-free, range-checked in `finalizeAcc`.
/// This was `+%`, which returned a plausible wrong total.
fn sumIntCol(d: []const i64) i128 {
    var s: i128 = 0;
    for (d) |x| s += x;
    return s;
}

fn validSumI(col: column.Column, n: usize) i128 {
    var s: i128 = 0;
    for (col.data.i64[0..n], 0..) |x, i| {
        if (col.validity.get(i)) s += x;
    }
    return s;
}

fn validSumF(col: column.Column, n: usize) f64 {
    var s: f64 = 0;
    for (col.data.f64[0..n], 0..) |x, i| {
        if (col.validity.get(i)) s += x;
    }
    return s;
}

/// MIN/MAX over an int or float column: SIMD when all valid (a null lane's 0 would
/// corrupt the extreme), else a scalar skip.
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

/// Drain the build side into a batch in `state` that is never null, so a join can
/// emit right-side nulls for an empty build. Fails with `JoinBuildTooLarge` past `cap`.
fn materializeFull(state: std.mem.Allocator, pull: std.mem.Allocator, child: Op, schema: *const types.Schema, bytes_out: *usize, cap: usize) anyerror!Batch {
    _ = pull;
    const ncols = schema.fields.len;
    var chunks = std.array_list.Managed(Batch).init(state);
    var total: usize = 0;
    var bytes: usize = 0;
    while (try child.next(state)) |b| {
        for (b.columns) |*col| bytes += columnBytes(col);
        if (bytes > cap) return error.JoinBuildTooLarge;
        if (b.len == 0) continue;
        try chunks.append(b);
        total += b.len;
    }
    const cols = try state.alloc(column.Column, ncols);
    if (total == 0) {
        for (cols, schema.fields) |*c, f| {
            var bd = column.Builder.init(state, f.ty);
            c.* = try bd.finish();
        }
    } else {
        const per = try state.alloc(column.Column, chunks.items.len);
        for (cols, 0..) |*out, ci| {
            for (chunks.items, 0..) |b, k| per[k] = b.columns[ci];
            out.* = try column.concat(state, per, total);
        }
    }
    bytes_out.* = bytes;
    return Batch{ .schema = schema, .columns = cols, .len = total };
}

/// A column's heap: typed store plus validity bitmap, close enough to bound a build side.
fn columnBytes(c: *const column.Column) usize {
    const payload: usize = switch (c.data) {
        .b => |s| s.len,
        .i32 => |s| s.len * @sizeOf(i32),
        .i64 => |s| s.len * @sizeOf(i64),
        .f64 => |s| s.len * @sizeOf(f64),
        .dec => |s| s.len * @sizeOf(Decimal),
        .bytes => |b| b.values.len + b.offsets.len * @sizeOf(i32),
    };
    return payload + c.validity.bits.len;
}

pub var join_build_byte_cap: usize = 4 << 30;

pub const KeyClass = enum { i64s, bytes, boxed };

/// How a key position compares. Same kind required: `int` and `timestamp` share the
/// i64 store but are never equal.
fn classOf(a: column.Column, b: column.Column) KeyClass {
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

/// Composite key hash for one row, folded from the typed store exactly as
/// `keyhash.MultiKeyCtx` folds boxed values, so build and probe sides agree.
fn hashRowKeys(cols: []const column.Column, keys: []const usize, classes: []const KeyClass, row: usize) u64 {
    var h = std.hash.Wyhash.init(0);
    for (keys, classes) |k, cls| {
        const c = &cols[k];
        switch (cls) {
            .i64s => switch (c.ty.kind) {
                .int => keyhash.hashInt(&h, c.data.i64[row]),
                .time => keyhash.hashTagged(&h, .time, std.mem.asBytes(&c.data.i64[row])),
                .timestamp => keyhash.hashTagged(&h, .timestamp, std.mem.asBytes(&c.data.i64[row])),
                else => keyhash.hashValue(&h, c.getValue(row)),
            },
            .bytes => keyhash.hashTagged(&h, if (c.ty.kind == .string) .string else .bytes, c.data.bytes.at(row)),
            .boxed => keyhash.hashValue(&h, c.getValue(row)),
        }
    }
    return h.final();
}

fn anyNullKey(cols: []const column.Column, keys: []const usize, row: usize) bool {
    for (keys) |k| {
        if (!cols[k].validity.get(row)) return true;
    }
    return false;
}

pub const JoinIndex = struct {
    build_batch: Batch,
    keys: []const usize,
    heads: []const u32,
    next: []const u32,
    hashes: []const u64,
    mask: u64,
    has_null_key: bool = false,

    /// Drain `build`, materialize it and index `right_keys`. Rows are inserted in
    /// reverse so prepending leaves duplicate chains in build order.
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
        var ri: usize = n;
        while (ri > 0) {
            ri -= 1;
            const r = ri;
            if (anyNullKey(batch.columns, right_keys, r)) {
                self.has_null_key = true;
                continue;
            }
            const h = hashRowKeys(batch.columns, right_keys, classes, r);
            hashes[r] = h;
            var slot = h & self.mask;
            while (heads[slot] != 0) : (slot = (slot + 1) & self.mask) {
                const hr: usize = heads[slot] - 1;
                if (hashes[hr] == h and self.rowsEq(classes, hr, r)) break;
            }
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

    /// First build row whose keys equal probe row `row`'s, or null; walk the rest with
    /// `chainNext`. `classes` comes from `classesFor`.
    pub fn find(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, row: usize) ?usize {
        if (self.keys.len == 0 or self.build_batch.len == 0) return null;
        if (anyNullKey(probe.columns, probe_keys, row)) return null;
        return self.findHashed(probe, probe_keys, classes, row, hashRowKeys(probe.columns, probe_keys, classes, row));
    }

    pub fn prefetch(self: *const JoinIndex, h: u64) void {
        @prefetch(&self.heads[h & self.mask], .{ .rw = .read, .locality = 3 });
    }

    pub fn findHashed(self: *const JoinIndex, probe: Batch, probe_keys: []const usize, classes: []const KeyClass, row: usize, h: u64) ?usize {
        var slot = h & self.mask;
        while (self.heads[slot] != 0) : (slot = (slot + 1) & self.mask) {
            const hr: usize = self.heads[slot] - 1;
            if (self.hashes[hr] == h and self.probeEq(probe, probe_keys, classes, row, hr)) return hr;
        }
        return null;
    }

    pub fn chainNext(self: *const JoinIndex, row: usize) ?usize {
        const nx = self.next[row];
        return if (nx == 0) null else nx - 1;
    }

    pub fn classesFor(self: *const JoinIndex, arena: std.mem.Allocator, probe: Batch, probe_keys: []const usize) ![]KeyClass {
        const classes = try arena.alloc(KeyClass, probe_keys.len);
        for (self.keys, probe_keys, classes) |bk, pk, *c|
            c.* = classOf(probe.columns[pk], self.build_batch.columns[bk]);
        return classes;
    }
};

pub const Join = struct {
    stats: Stats = .{},
    probe: Op,
    build: ?Op,
    index: ?*JoinIndex = null,
    left_keys: []const usize,
    right_keys: []const usize,
    left_schema: *const types.Schema,
    right_schema: *const types.Schema,
    out_schema: *const types.Schema,
    kind: ast.JoinKind,
    null_aware: bool = false,
    state: std.mem.Allocator,
    err: ?*ErrCtx = null,
    build_cap: ?usize = null,

    matched: ?[]bool = null,
    drain_pos: usize = 0,
    probe_done: bool = false,

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
                if (self.err) |ec| ec.set("{s}", .{failLabel(e)});
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

    /// One probe batch to one output batch, gathered from index lists. Under NOT IN a
    /// null on either side against a non-empty build is unknown and drops the row.
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

        const fill_right = (self.kind == .left or self.kind == .full);
        const classes = try ix.classesFor(arena, lb, self.left_keys);
        try ridx.ensureTotalCapacity(lb.len);
        if (fill_right) try rnull.ensureTotalCapacity(lb.len);

        const empty = ix.keys.len == 0 or ix.build_batch.len == 0;
        const hs = try arena.alloc(u64, lb.len);
        const nulls = try arena.alloc(bool, lb.len);
        for (hs, nulls, 0..) |*h, *nl, i| {
            nl.* = empty or anyNullKey(lb.columns, self.left_keys, i);
            h.* = if (nl.*) 0 else hashRowKeys(lb.columns, self.left_keys, classes, i);
        }
        const ahead = 8;

        var r: usize = 0;
        while (r < lb.len) : (r += 1) {
            if (r + ahead < lb.len and !nulls[r + ahead]) ix.prefetch(hs[r + ahead]);
            const first = if (nulls[r]) null else ix.findHashed(lb, self.left_keys, classes, r, hs[r]);
            switch (self.kind) {
                .semi => {
                    if (first != null) try lidx.append(r);
                },
                .anti => {
                    if (self.null_aware and !empty and (ix.has_null_key or nulls[r])) continue;
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

    /// right/full: after the probe ends, every unmatched build row with the left
    /// columns null, in chunks.
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

    /// Assemble `n` output rows with one gather per column. `lb == null` marks the
    /// outer drain, where the whole left side is null.
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

/// Gather rows `idx` of `c`, then null out the `null_mask` positions (the outer-join
/// fill). Only a source with no rows is built as an all-null column instead.
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

fn nullColumn(arena: std.mem.Allocator, ty: types.Type, n: usize) !column.Column {
    var b = try column.Builder.initCapacity(arena, ty.asNullable(), n);
    var i: usize = 0;
    while (i < n) : (i += 1) try b.append(.null);
    return b.finish();
}

const testing = std.testing;

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

fn intBatch(a: std.mem.Allocator, schema: *const types.Schema, vals: []const ?i64) !Batch {
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try column.intColumn(a, vals);
    return Batch{ .schema = schema, .columns = cols, .len = vals.len };
}

fn strBatch(a: std.mem.Allocator, schema: *const types.Schema, vals: []const ?[]const u8) !Batch {
    var bd = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (vals) |v| try bd.append(if (v) |s| Value{ .string = s } else .null);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = vals.len };
}

fn kvBatch(a: std.mem.Allocator, schema: *const types.Schema, ints: []const ?i64, strs: []const ?[]const u8) !Batch {
    const cols = try a.alloc(column.Column, 2);
    cols[0] = try column.intColumn(a, ints);
    var bd = column.Builder.init(a, types.Type.init(.string).asNullable());
    for (strs) |v| try bd.append(if (v) |s| Value{ .string = s } else .null);
    cols[1] = try bd.finish();
    return Batch{ .schema = schema, .columns = cols, .len = ints.len };
}

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

test "distinct reports the input ordinal of every surviving row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 7, 7, 4 }),
        try intBatch(a, &int_schema, &.{ 4, 7, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &int_schema, .keys = null, .state = a, .gpa = testing.allocator, .track_ords = true };

    var ords = std.array_list.Managed(u64).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        try testing.expectEqual(b.len, dst.ords.len);
        try ords.appendSlice(dst.ords);
    }
    try testing.expectEqualSlices(u64, &.{ 0, 2, 5 }, ords.items);
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

test "aggregate: variance folded in two halves and merged matches one fold, and holds up far from zero" {
    const A = Aggregate;
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const xs = [_]f64{ 2, 4, 4, 4, 5, 5, 7, 9, 1.5, 12.25 };
    inline for (.{ ast.AggFunc.var_samp, .var_pop, .stddev_samp, .stddev_pop }) |func| {
        const agg = A.Agg{ .func = func, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
        var whole = A.Acc{};
        for (xs) |x| try A.updateAcc(testing.allocator, &whole, agg, .{ .float = x }, true);
        for (0..xs.len + 1) |cut| {
            var lo = A.Acc{};
            var hi = A.Acc{};
            for (xs[0..cut]) |x| try A.updateAcc(testing.allocator, &lo, agg, .{ .float = x }, true);
            for (xs[cut..]) |x| try A.updateAcc(testing.allocator, &hi, agg, .{ .float = x }, true);
            try A.mergeAcc(testing.allocator, &lo, hi, agg);
            try testing.expectApproxEqAbs((try A.finalizeAcc(whole, agg)).float, (try A.finalizeAcc(lo, agg)).float, 1e-9);
        }
    }
    const pop = A.Agg{ .func = .var_pop, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
    const samp = A.Agg{ .func = .var_samp, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
    var p = A.Acc{};
    var q = A.Acc{};
    for (xs[0..8]) |x| {
        try A.updateAcc(testing.allocator, &p, pop, .{ .float = x + 1e9 }, true);
        try A.updateAcc(testing.allocator, &q, samp, .{ .int = @intFromFloat(x) }, true);
    }
    try testing.expectApproxEqAbs(@as(f64, 4), (try A.finalizeAcc(p, pop)).float, 1e-6);
    try testing.expectApproxEqAbs(@as(f64, 32.0 / 7.0), (try A.finalizeAcc(q, samp)).float, 1e-12);
    var one = A.Acc{};
    try A.updateAcc(testing.allocator, &one, samp, .{ .float = 3 }, true);
    try testing.expect((try A.finalizeAcc(one, samp)) == .null);
    try testing.expectEqual(@as(f64, 0), (try A.finalizeAcc(one, pop)).float);
    try testing.expect((try A.finalizeAcc(.{}, pop)) == .null);
}

test "aggregate: bit_and starts from its first value, not zero, through a merge too" {
    const A = Aggregate;
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const agg = A.Agg{ .func = .bit_and, .arg = &fx, .ty = types.Type.init(.int).asNullable() };
    var empty = A.Acc{};
    var some = A.Acc{};
    for ([_]i64{ 0b1110, 0b0111, -1 }) |x| try A.updateAcc(testing.allocator, &some, agg, .{ .int = x }, true);
    try A.updateAcc(testing.allocator, &some, agg, .null, true);
    try A.mergeAcc(testing.allocator, &empty, some, agg);
    try testing.expectEqual(@as(i64, 0b0110), (try A.finalizeAcc(empty, agg)).int);
    var neg = A.Acc{};
    try A.updateAcc(testing.allocator, &neg, agg, .{ .int = std.math.minInt(i64) }, true);
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), (try A.finalizeAcc(neg, agg)).int);
    try testing.expect((try A.finalizeAcc(.{}, agg)) == .null);
}

test "aggregate: a decimal sum normalizes each value's own scale" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const col_ty = types.Type.decimal(38, 6).asNullable();
    const in_schema = types.Schema{ .fields = &.{.{ .name = "n", .ty = col_ty }} };

    var bld = column.Builder.init(a, col_ty);
    try bld.append(.{ .decimal = .{ .unscaled = 15, .scale = 1 } });
    try bld.append(.{ .decimal = .{ .unscaled = 150, .scale = 2 } });
    try bld.append(.{ .decimal = .{ .unscaled = 1, .scale = 3 } });
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
        const got = try JoinRows.collect(a, .{ .join = &jn }, if (emit_right) @as(?usize, 3) else null);
        try got.expect(case.keys, case.rvs);
    }
}

test "join: a null-aware anti join is NOT IN — a NULL on either side is unknown" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Case = struct { build: []const ?i64, keys: []const ?i64 };
    const cases = [_]Case{
        .{ .build = &.{ 1, 4, null }, .keys = &.{} },
        .{ .build = &.{ 1, 4 }, .keys = &.{ 2, 3 } },
        .{ .build = &.{}, .keys = &.{ 1, 2, null, 3 } },
    };
    for (cases) |case| {
        const lb = [_]Batch{try kvBatch(a, &join_left_schema, &.{ 1, 2, null, 3 }, &.{ "a", "b", "n", "c" })};
        const rvs = try a.alloc(?[]const u8, case.build.len);
        @memset(rvs, "r");
        const rb = [_]Batch{try kvBatch(a, &join_right_schema, case.build, rvs)};
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
            .out_schema = &join_left_schema,
            .kind = .anti,
            .null_aware = true,
            .state = a,
        };
        const got = try JoinRows.collect(a, .{ .join = &jn }, null);
        try got.expect(case.keys, &.{});
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
    var flt = Filter{ .child = .{ .scan = &scan }, .pred = &pred, .back = a };
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

test "integer SUM: exact across batches, an error only when the total leaves i64" {
    const big: i64 = 6148914691236517205;
    const agg = Aggregate.Agg{ .func = .sum, .arg = null, .ty = types.Type.init(.int) };
    try testing.expectError(error.IntOverflow, Aggregate.finalizeAcc(.{ .n = 3, .sum_i = sumIntCol(&[_]i64{ big, big, big }) }, agg));
    try testing.expectError(error.IntOverflow, Aggregate.finalizeAcc(.{ .n = 2, .sum_i = sumIntCol(&[_]i64{ std.math.minInt(i64), -1 }) }, agg));
    try testing.expectEqual(@as(i128, 6), sumIntCol(&[_]i64{ 1, 2, 3 }));

    const max = std.math.maxInt(i64);
    var acc = Aggregate.Acc{ .n = 5 };
    for ([_][]const i64{ &.{ max, max }, &.{ -max, -max }, &.{5} }) |part| acc.sum_i += sumIntCol(part);
    try testing.expectEqual(Value{ .int = 5 }, try Aggregate.finalizeAcc(acc, agg));
}

test "sortIdx: the radix words order rows exactly as the comparator does" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xba5a17);
    const rnd = prng.random();

    const long_base = "a-long-prefix-shared-by-many-";
    for (0..60) |round| {
        const n = 1 + rnd.uintLessThan(usize, 400);
        const nkeys = 1 + rnd.uintLessThan(usize, 3);
        const arrs = try a.alloc(KeyArr, nkeys);
        for (arrs) |*k| {
            const kind = rnd.uintLessThan(u8, 4);
            const ty: types.Type = switch (kind) {
                0 => types.Type.init(.int),
                1 => types.Type.init(.float),
                2 => types.Type.decimal(18, 3),
                else => types.Type.init(.string),
            };
            var b = column.Builder.init(a, ty.asNullable());
            for (0..n) |_| {
                if (rnd.uintLessThan(u8, 8) == 0) {
                    try b.append(.null);
                    continue;
                }
                try b.append(switch (kind) {
                    0 => Value{ .int = rnd.intRangeAtMost(i64, -5, 5) * @as(i64, if (rnd.boolean()) 1 else std.math.maxInt(i64) / 7) },
                    1 => Value{ .float = switch (rnd.uintLessThan(u8, 6)) {
                        0 => std.math.nan(f64),
                        1 => -0.0,
                        2 => 0.0,
                        else => @as(f64, @floatFromInt(rnd.intRangeAtMost(i64, -3, 3))) / 2,
                    } },
                    2 => Value{ .decimal = .{ .unscaled = rnd.intRangeAtMost(i128, -30, 30), .scale = rnd.uintLessThan(u8, 3) } },
                    else => Value{ .string = blk: {
                        const len = rnd.uintLessThan(usize, 4);
                        const tail = try a.alloc(u8, len);
                        for (tail) |*c| c.* = "ab\x00"[rnd.uintLessThan(usize, 3)];
                        break :blk if (rnd.boolean()) try std.mem.concat(a, u8, &.{ long_base, tail }) else tail;
                    } },
                });
            }
            k.* = try KeyArr.prepare(a, try b.finish(), rnd.boolean());
        }
        const want = try a.alloc(usize, n);
        const got = try a.alloc(usize, n);
        for (want, got, 0..) |*x, *y, i| {
            x.* = i;
            y.* = i;
        }
        std.mem.sort(usize, want, SortCtx{ .arrs = arrs }, SortCtx.lessThan);
        try sortIdx(a, got, arrs);
        testing.expectEqualSlices(usize, want, got) catch |e| {
            std.debug.print("sortIdx mismatch in round {d}\n", .{round});
            return e;
        };
    }
}

test "top_n: equal keys rank by input position, also when a lane reads items out of order" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const Src = struct {
        batches: []const Batch,
        items: []const usize,
        i: usize = 0,
        cur: usize = 0,
        sch: types.Schema,
        fn schemaFn(p: *anyopaque) types.Schema {
            return @as(*@This(), @ptrCast(@alignCast(p))).sch;
        }
        fn nextFn(p: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
            const self: *@This() = @ptrCast(@alignCast(p));
            if (self.i == self.batches.len) return null;
            defer self.i += 1;
            self.cur = self.items[self.i];
            return self.batches[self.i];
        }
        fn closeFn(_: *anyopaque) void {}
        const vt = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
    };
    const batches = [_]Batch{
        try kvBatch(a, &schema, &.{ 7, 7, 7 }, &.{ "five-a", "five-b", "five-c" }),
        try kvBatch(a, &schema, &.{ 7, 7, 7 }, &.{ "two-a", "two-b", "two-c" }),
    };
    var src = Src{ .batches = &batches, .items = &.{ 5, 2 }, .sch = schema };
    var scan = Scan{ .src = .{ .ptr = &src, .vtable = &Src.vt } };
    var tn = TopN{
        .child = .{ .scan = &scan },
        .in_schema = &schema,
        .keys = &[_]Sort.Key{.{ .idx = 0, .desc = true }},
        .count = 2,
        .offset = 0,
        .state = a,
        .gpa = testing.allocator,
        .item = &src.cur,
    };
    const b = (try tn.next(a)).?;
    try testing.expectEqualStrings("two-a", b.columns[1].getValue(0).string);
    try testing.expectEqualStrings("two-b", b.columns[1].getValue(1).string);
}
