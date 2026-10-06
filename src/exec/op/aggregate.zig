//! `GROUP BY` and global aggregates: grouped hash tables, per-lane partials and
//! their merge, and the vectorized reductions of the global path.
//!
//! `Aggregate` keeps its fields and core types here; its group tables are in
//! `aggregate/table.zig`, a lane's group sets and their merge in `groups.zig`, the
//! input loop in `drain.zig`, and the accumulators in `accumulate.zig`, each aliased
//! back into the struct.

const Batch = @import("../batch.zig").Batch;
const Decimal = @import("../value.zig").Decimal;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const aggregates = @import("../../lang/aggregates.zig");
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const eval = @import("../eval.zig");
const failLabel = @import("../op.zig").failLabel;
const keyhash = @import("../keyhash.zig");
const simd = @import("../simd.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const intBatch = @import("testing_util.zig").intBatch;
const int_schema = @import("testing_util.zig").int_schema;
const kvBatch = @import("testing_util.zig").kvBatch;
const strBatch = @import("testing_util.zig").strBatch;
const testing = std.testing;

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

    pub const StrId = @import("aggregate/groups.zig").StrId;
    pub const StrTable = @import("aggregate/groups.zig").StrTable;
    pub const strId = @import("aggregate/groups.zig").strId;
    pub const strTable = @import("aggregate/groups.zig").strTable;
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

    pub fn noteMedian(alloc: std.mem.Allocator, acc: *Acc, x: f64) !void {
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
    pub fn mergeDistinct(alloc: std.mem.Allocator, acc: *Acc, src: *const DistinctSet()) !void {
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
    pub fn noteDistinct(alloc: std.mem.Allocator, acc: *Acc, v: Value) !void {
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

    pub const GroupSet = @import("aggregate/groups.zig").GroupSet;
    pub const GroupMerge = @import("aggregate/groups.zig").GroupMerge;
    pub const GroupMap = @import("aggregate/groups.zig").GroupMap;
    pub const Partial = @import("aggregate/groups.zig").Partial;
    pub const next = @import("aggregate/drain.zig").next;
    pub const drainSet = @import("aggregate/drain.zig").drainSet;
    pub const drainParts = @import("aggregate/drain.zig").drainParts;
    pub const fold_parts = @import("aggregate/drain.zig").fold_parts;
    pub const part_shift = @import("aggregate/drain.zig").part_shift;
    pub const partOf = @import("aggregate/drain.zig").partOf;
    pub const FoldPart = @import("aggregate/drain.zig").FoldPart;
    pub const foldParts = @import("aggregate/drain.zig").foldParts;
    pub const newIn = @import("aggregate/drain.zig").newIn;
    pub const foldSets = @import("aggregate/drain.zig").foldSets;
    pub const keyKinds = @import("aggregate/drain.zig").keyKinds;
    pub const drainFixed = @import("aggregate/drain.zig").drainFixed;
    pub const drainImpl = @import("aggregate/drain.zig").drainImpl;
    pub const prefetch_ahead = @import("aggregate/table.zig").prefetch_ahead;
    pub const Direct = @import("aggregate/table.zig").Direct;
    pub const few_groups = @import("aggregate/table.zig").few_groups;
    pub const fixedHash = @import("aggregate/table.zig").fixedHash;
    pub const fmix64 = @import("aggregate/table.zig").fmix64;
    pub const Fast = @import("aggregate/table.zig").Fast;
    pub const KeyKind = @import("aggregate/table.zig").KeyKind;
    pub const keyKindOf = @import("aggregate/table.zig").keyKindOf;
    pub const RecBlocks = @import("aggregate/table.zig").RecBlocks;
    pub const FixedStore = @import("aggregate/table.zig").FixedStore;
    pub const FixedKey = @import("aggregate/table.zig").FixedKey;
    pub const GroupTable = @import("aggregate/table.zig").GroupTable;
    pub const GroupStore = @import("aggregate/table.zig").GroupStore;
    pub const mergeAcc = @import("aggregate/accumulate.zig").mergeAcc;
    pub const foldVectorized = @import("aggregate/accumulate.zig").foldVectorized;
    pub const foldRowwise = @import("aggregate/accumulate.zig").foldRowwise;
    pub const argCast = @import("aggregate/accumulate.zig").argCast;
    pub const argColumn = @import("aggregate/accumulate.zig").argColumn;
    pub const argValue = @import("aggregate/accumulate.zig").argValue;
    pub const castArg = @import("aggregate/accumulate.zig").castArg;
    pub const reduceBatch = @import("aggregate/accumulate.zig").reduceBatch;
    pub const foldsRowwise = @import("aggregate/accumulate.zig").foldsRowwise;
    pub const mergePartial = @import("aggregate/accumulate.zig").mergePartial;
    pub const emitSets = @import("aggregate/accumulate.zig").emitSets;
    pub const emitGroup = @import("aggregate/accumulate.zig").emitGroup;
    pub const updateAcc = @import("aggregate/accumulate.zig").updateAcc;
    pub const addExact = @import("aggregate/accumulate.zig").addExact;
    pub const Slot = @import("aggregate/accumulate.zig").Slot;
    pub const SumI = @import("aggregate/accumulate.zig").SumI;
    pub const SumF = @import("aggregate/accumulate.zig").SumF;
    pub const Layout = @import("aggregate/accumulate.zig").Layout;
    pub const finalizeAcc = @import("aggregate/accumulate.zig").finalizeAcc;
    pub const bitFold = @import("aggregate/accumulate.zig").bitFold;
    pub const sqDev = @import("aggregate/accumulate.zig").sqDev;
    pub const noteMoment = @import("aggregate/accumulate.zig").noteMoment;
    pub const mergeMoments = @import("aggregate/accumulate.zig").mergeMoments;
    pub const spread = @import("aggregate/accumulate.zig").spread;
};

pub fn lessV(a: Value, b: Value) bool {
    return (eval.compareValues(a, b) orelse .eq) == .lt;
}

/// Quickselect: `xs[k]` becomes the k-th smallest, smaller before, larger after.
/// Median-of-three pivot, Hoare partition; expected O(n).
pub fn selectNth(xs: []f64, k: usize) f64 {
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
pub fn sumIntCol(d: []const i64) i128 {
    var s: i128 = 0;
    for (d) |x| s += x;
    return s;
}

pub fn validSumI(col: column.Column, n: usize) i128 {
    var s: i128 = 0;
    for (col.data.i64[0..n], 0..) |x, i| {
        if (col.validity.get(i)) s += x;
    }
    return s;
}

pub fn validSumF(col: column.Column, n: usize) f64 {
    var s: f64 = 0;
    for (col.data.f64[0..n], 0..) |x, i| {
        if (col.validity.get(i)) s += x;
    }
    return s;
}

/// MIN/MAX over an int or float column: SIMD when all valid (a null lane's 0 would
/// corrupt the extreme), else a scalar skip.
pub fn reduceExtreme(col: column.Column, func: ast.AggFunc, n: usize) Value {
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
