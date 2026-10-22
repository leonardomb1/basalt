//! `GROUP BY` and global aggregates: grouped hash tables, per-lane partials and
//! their merge.
//!
//! `Aggregate` keeps its fields and core types here; its group tables are in
//! `../keytable.zig`, a lane's group sets and their merge in `groups.zig`, the
//! input loop in `drain.zig`, arguments and output in `accumulate.zig`, each
//! aliased back into the struct. Every aggregate is its math in `funcs.zig` run by
//! the loops `kernel.zig` generates for it.
//!
//! With a `space`, a grouped serial aggregate spills to disk once what it holds
//! passes `spill_at` (`aggregate/spill.zig`); without one, or on a lane, it never does.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const ast = @import("../../lang/ast.zig");
const column = @import("../column.zig");
const eval = @import("../eval.zig");
const keytable = @import("../keytable.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const Space = @import("../space.zig").Space;
const spill_parts = @import("spill_parts.zig");
const kernel = @import("aggregate/kernel.zig");
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
    /// The arena `state` comes from when this aggregate owns it, freed by
    /// `release`, so a statement's groups go with the statement, not the run.
    own_state: ?*std.heap.ArenaAllocator = null,
    gpa: std.mem.Allocator,
    part_state: ?[]const std.mem.Allocator = null,
    table_gpa: ?std.mem.Allocator = null,
    strs: ?*StrTable = null,
    str_cache: std.StringHashMapUnmanaged(StrId) = .empty,
    done: bool = false,
    space: ?Space = null,
    spill_at: usize = std.math.maxInt(usize),
    max_spill_depth: u8 = spill_parts.default_max_depth,
    spill_depth: u8 = 0,
    spill: ?*Spill = null,
    /// Levels that wrote spill files, this one and its replays' deeper ones, held
    /// by the top aggregate (`spilled_to` points a replay at it).
    spilled: u8 = 0,
    spilled_to: ?*u8 = null,
    pairs: []Pairs = &.{},

    pub const Spill = @import("aggregate/spill.zig").Spill;
    pub const spillNext = @import("aggregate/spill.zig").spillNext;
    pub const replayChild = @import("aggregate/spill.zig").replayChild;
    pub const release = @import("aggregate/spill.zig").release;
    pub const overBudget = @import("aggregate/spill.zig").overBudget;
    pub const spillRows = @import("aggregate/spill.zig").spillRows;
    pub const destOf = @import("aggregate/spill.zig").destOf;
    pub const Pairs = @import("aggregate/pairs.zig").Pairs;
    pub const pairsOf = @import("aggregate/drain.zig").pairsOf;
    pub const StrId = keytable.StrId;
    pub const StrTable = keytable.StrTable;

    /// String keys' ids, through the table the lanes share.
    pub fn interner(self: *Aggregate) keytable.Interner {
        return .{ .table = &self.strs, .cache = &self.str_cache, .alloc = self.state };
    }
    pub const Agg = struct { func: ast.AggFunc, arg: ?*const ast.Expr, ty: types.Type, distinct: bool = false, arg_ty: ?types.Type = null };

    pub const GroupSet = @import("aggregate/groups.zig").GroupSet;
    pub const GroupMerge = @import("aggregate/groups.zig").GroupMerge;
    pub const GroupMap = @import("aggregate/groups.zig").GroupMap;
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
    pub const prefetch_ahead = keytable.prefetch_ahead;
    pub const Direct = keytable.Direct;
    pub const few_groups = keytable.few_groups;
    pub const fixedHash = keytable.fixedHash;
    pub const KeyKind = @import("../keyhash.zig").KeyKind;
    pub const keyKindOf = @import("../keyhash.zig").keyKindOf;
    pub const RecBlocks = keytable.RecBlocks;
    pub const FixedStore = keytable.FixedStore(Layout);
    pub const FixedKey = keytable.FixedKey;
    pub const GroupTable = keytable.GroupTable;
    pub const GroupStore = keytable.GroupStore(Layout);
    pub const argCast = @import("aggregate/accumulate.zig").argCast;
    pub const argColumn = @import("aggregate/accumulate.zig").argColumn;
    pub const emitSets = @import("aggregate/accumulate.zig").emitSets;
    pub const Layout = @import("aggregate/kernel.zig").Layout;
    pub const foldSingle = @import("aggregate/accumulate.zig").foldSingle;
};

pub const selectNth = @import("aggregate/funcs.zig").selectNth;
pub const dupeValue = @import("aggregate/funcs.zig").dupeValue;

test {
    _ = @import("aggregate/spill.zig");
    _ = @import("aggregate/funcs.zig");
    _ = @import("aggregate/kernel.zig");
    _ = @import("aggregate/pairs.zig");
    _ = @import("spill_parts.zig");
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

/// One aggregate's state outside an operator, for testing its math through its kernel.
const Probe = struct {
    k: *const kernel.Kernel,
    cx: kernel.Ctx,
    buf: [64]u8 align(16) = undefined,

    fn init(agg: Aggregate.Agg) Probe {
        const kf = kernel.kernelFor(agg);
        var p = Probe{ .k = kf.k, .cx = .{ .out = agg.ty, .ty = kf.ty, .alloc = testing.allocator, .scratch = testing.allocator } };
        p.k.init(&p.buf);
        return p;
    }
    fn add(p: *Probe, v: Value) !void {
        try p.k.add(&p.buf, v, p.cx);
    }
    fn merge(p: *Probe, o: *const Probe) !void {
        try p.k.merge(&p.buf, &o.buf, p.cx);
    }
    fn finish(p: *const Probe) !Value {
        return p.k.finish(&p.buf, p.cx);
    }
};

test "aggregate: variance folded in two halves and merged matches one fold, and holds up far from zero" {
    const A = Aggregate;
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const xs = [_]f64{ 2, 4, 4, 4, 5, 5, 7, 9, 1.5, 12.25 };
    inline for (.{ ast.AggFunc.var_samp, .var_pop, .stddev_samp, .stddev_pop }) |func| {
        const agg = A.Agg{ .func = func, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
        var whole = Probe.init(agg);
        for (xs) |x| try whole.add(.{ .float = x });
        for (0..xs.len + 1) |cut| {
            var lo = Probe.init(agg);
            var hi = Probe.init(agg);
            for (xs[0..cut]) |x| try lo.add(.{ .float = x });
            for (xs[cut..]) |x| try hi.add(.{ .float = x });
            try lo.merge(&hi);
            try testing.expectApproxEqAbs((try whole.finish()).float, (try lo.finish()).float, 1e-9);
        }
    }
    const pop = A.Agg{ .func = .var_pop, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
    const samp = A.Agg{ .func = .var_samp, .arg = &fx, .ty = types.Type.init(.float).asNullable() };
    var p = Probe.init(pop);
    var q = Probe.init(samp);
    for (xs[0..8]) |x| {
        try p.add(.{ .float = x + 1e9 });
        try q.add(.{ .int = @intFromFloat(x) });
    }
    try testing.expectApproxEqAbs(@as(f64, 4), (try p.finish()).float, 1e-6);
    try testing.expectApproxEqAbs(@as(f64, 32.0 / 7.0), (try q.finish()).float, 1e-12);
    var one = Probe.init(samp);
    try one.add(.{ .float = 3 });
    try testing.expect((try one.finish()) == .null);
    var one_pop = Probe.init(pop);
    try one_pop.add(.{ .float = 3 });
    try testing.expectEqual(@as(f64, 0), (try one_pop.finish()).float);
    try testing.expect((try Probe.init(pop).finish()) == .null);
}

test "aggregate: bit_and starts from its first value, not zero, through a merge too" {
    const A = Aggregate;
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"x"} } };
    const agg = A.Agg{ .func = .bit_and, .arg = &fx, .ty = types.Type.init(.int).asNullable() };
    var empty = Probe.init(agg);
    var some = Probe.init(agg);
    for ([_]i64{ 0b1110, 0b0111, -1 }) |x| try some.add(.{ .int = x });
    try some.add(.null);
    try empty.merge(&some);
    try testing.expectEqual(@as(i64, 0b0110), (try empty.finish()).int);
    var neg = Probe.init(agg);
    try neg.add(.{ .int = std.math.minInt(i64) });
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), (try neg.finish()).int);
    try testing.expect((try Probe.init(agg).finish()) == .null);
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
    try bld.append(.{ .decimal = .{ .unscaled = 15, .scale = 7 } });
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try bld.finish();
    const batches = [_]Batch{.{ .schema = &in_schema, .columns = cols, .len = 5 }};

    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"n"} } };
    const aggs = [_]Aggregate.Agg{.{ .func = .sum, .arg = &fx, .ty = col_ty }};
    const out_schema = types.Schema{ .fields = &.{.{ .name = "s", .ty = col_ty }} };

    var ts = TestSource{ .schema_ = in_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &in_schema, .by = &.{}, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
    const out = (try agg.next(a)).?;
    const d = out.columns[0].getValue(0).decimal;
    try testing.expectEqual(@as(u8, 6), d.scale);
    try testing.expectEqual(@as(i128, 3_001_002), d.unscaled);

    const top: i128 = std.math.powi(i128, 10, 38) catch unreachable;
    var big = column.Builder.init(a, types.Type.decimal(38, 0));
    try big.append(.{ .decimal = .{ .unscaled = top - 1, .scale = 0 } });
    try big.append(.{ .decimal = .{ .unscaled = top - 1, .scale = 0 } });
    const big_cols = try a.alloc(column.Column, 1);
    big_cols[0] = try big.finish();
    const big_aggs = [_]Aggregate.Agg{.{ .func = .sum, .arg = &fx, .ty = types.Type.decimal(38, 0) }};
    var over = Probe.init(big_aggs[0]);
    try over.k.fold(&over.buf, big_cols[0], 2, over.cx);
    try testing.expectError(error.IntOverflow, over.finish());
}

test "aggregate: a decimal sum checks 38 digits on the total, whatever the batches or row order" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const dty = types.Type.decimal(38, 0).asNullable();
    const in_schema = types.Schema{ .fields = &.{ .{ .name = "k", .ty = types.Type.init(.int) }, .{ .name = "n", .ty = dty } } };
    const big: i128 = 9 * (std.math.powi(i128, 10, 37) catch unreachable);
    const fx = ast.Expr{ .field = .{ .parts = &[_][]const u8{"n"} } };
    const aggs = [_]Aggregate.Agg{.{ .func = .sum, .arg = &fx, .ty = dty }};
    const out_schema = types.Schema{ .fields = &.{ .{ .name = "k", .ty = types.Type.init(.int) }, .{ .name = "s", .ty = dty } } };
    const orders = [_][3]i128{ .{ -big, big, big }, .{ big, big, -big }, .{ big, -big, big } };
    for (orders) |vals| for ([_][]const usize{ &.{}, &.{1}, &.{2}, &.{ 1, 2 } }) |cuts| for ([_][]const usize{ &.{}, &.{0} }) |by| {
        var batches: [3]Batch = undefined;
        var nb: usize = 0;
        var lo: usize = 0;
        for (0..cuts.len + 1) |ci| {
            const hi = if (ci < cuts.len) cuts[ci] else vals.len;
            var kb = column.Builder.init(a, types.Type.init(.int));
            var db = column.Builder.init(a, dty);
            for (vals[lo..hi]) |v| {
                try kb.append(.{ .int = 1 });
                try db.append(.{ .decimal = .{ .unscaled = v, .scale = 0 } });
            }
            const cols = try a.alloc(column.Column, 2);
            cols[0] = try kb.finish();
            cols[1] = try db.finish();
            batches[nb] = .{ .schema = &in_schema, .columns = cols, .len = hi - lo };
            nb += 1;
            lo = hi;
        }
        var ts = TestSource{ .schema_ = in_schema, .batches = batches[0..nb] };
        var scan = Scan{ .src = ts.src() };
        var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &in_schema, .by = by, .aggs = &aggs, .out_schema = if (by.len == 0) &.{ .fields = out_schema.fields[1..] } else &out_schema, .state = a, .gpa = testing.allocator };
        const out = (try agg.next(a)).?;
        try testing.expectEqual(big, out.columns[by.len].getValue(0).decimal.unscaled);
    };
}

test "aggregate: a grouped decimal SUM of arithmetic folded from its operands matches the row path per group" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const parser = @import("../../lang/sql_parser.zig");

    const dty = types.Type.decimal(38, 2).asNullable();
    const ity = types.Type.init(.int);
    const in_schema = types.Schema{ .fields = &.{
        .{ .name = "k1", .ty = ity },
        .{ .name = "k2", .ty = ity },
        .{ .name = "x", .ty = ity.asNullable() },
        .{ .name = "d", .ty = dty },
        .{ .name = "e", .ty = dty },
    } };
    var prng = std.Random.DefaultPrng.init(0xdec5);
    const rnd = prng.random();
    const exprs = [_][]const u8{ "d * e", "d + x", "x * 0.5 - d", "e - 1.255" };
    for ([_]usize{ 1025, 2049, 4097 }) |ngroups| {
        var batches: [2]Batch = undefined;
        for (&batches, 0..) |*bt, bi| {
            const n = ngroups * 2 + bi * 7;
            var cb: [5]column.Builder = undefined;
            for (&cb, in_schema.fields) |*b, f| b.* = column.Builder.init(a, f.ty);
            for (0..n) |i| {
                try cb[0].append(.{ .int = @intCast(i % ngroups) });
                try cb[1].append(.{ .int = @intCast((i / 3) % 2) });
                try cb[2].append(if (rnd.uintLessThan(u8, 9) == 0) .null else .{ .int = rnd.intRangeAtMost(i64, -50_000, 50_000) });
                for (cb[3..]) |*b| {
                    const pick = rnd.uintLessThan(u8, 10);
                    if (pick == 0) {
                        try b.append(.null);
                        continue;
                    }
                    const scale: u8 = switch (pick) {
                        1 => 0,
                        2 => 4,
                        3 => 3,
                        else => 2,
                    };
                    try b.append(.{ .decimal = .{ .unscaled = rnd.intRangeAtMost(i128, -9_999_999, 9_999_999), .scale = scale } });
                }
            }
            const cols = try a.alloc(column.Column, 5);
            for (cols, &cb) |*c, *b| c.* = try b.finish();
            bt.* = .{ .schema = &in_schema, .columns = cols, .len = n };
        }
        for (exprs) |src| {
            var diag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
            const e = try parser.parseExprStr(a, src, &diag);
            var tcx = eval.TypeCtx{ .schema = in_schema, .arena = a };
            const sty = types.Type.decimal(38, (try tcx.typeOf(e)).scale).asNullable();
            for ([_][]const usize{ &.{}, &.{0}, &.{ 0, 1 } }) |by| {
                var want = std.AutoHashMap([2]i64, struct { s: i256 = 0, n: usize = 0 }).init(a);
                for (batches) |bt| for (0..bt.len) |r| {
                    const v = try eval.evalRow(a, e, bt, r);
                    var key = [2]i64{ 0, 0 };
                    for (by, 0..) |ci, j| key[j] = bt.columns[ci].getValue(r).int;
                    const g = try want.getOrPut(key);
                    if (!g.found_existing) g.value_ptr.* = .{};
                    if (v.isNull()) continue;
                    g.value_ptr.s += eval.rescaleTo(v.decimal, sty.scale).?.unscaled;
                    g.value_ptr.n += 1;
                };
                const aggs = [_]Aggregate.Agg{
                    .{ .func = .count, .arg = null, .ty = ity },
                    .{ .func = .sum, .arg = e, .ty = sty },
                };
                const fields = [_]types.Schema.Field{ .{ .name = "k1", .ty = ity }, .{ .name = "k2", .ty = ity }, .{ .name = "c", .ty = ity }, .{ .name = "s", .ty = sty } };
                var of: [4]types.Schema.Field = undefined;
                @memcpy(of[0..by.len], fields[0..by.len]);
                @memcpy(of[by.len..][0..2], fields[2..4]);
                const out_schema = types.Schema{ .fields = of[0 .. by.len + 2] };
                var ts = TestSource{ .schema_ = in_schema, .batches = &batches };
                var scan = Scan{ .src = ts.src() };
                var agg = Aggregate{ .child = .{ .scan = &scan }, .in_schema = &in_schema, .by = by, .aggs = &aggs, .out_schema = &out_schema, .state = a, .gpa = testing.allocator };
                const out = (try agg.next(a)).?;
                try testing.expectEqual(want.count(), out.len);
                for (0..out.len) |r| {
                    var key = [2]i64{ 0, 0 };
                    for (0..by.len) |j| key[j] = out.columns[j].getValue(r).int;
                    const w = want.get(key).?;
                    const got = out.columns[by.len + 1].getValue(r);
                    if (w.n == 0) {
                        try testing.expect(got.isNull());
                        continue;
                    }
                    try testing.expectEqual(sty.scale, got.decimal.scale);
                    try testing.expectEqual(w.s, got.decimal.unscaled);
                }
            }
        }
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

test "integer SUM: exact across batches, an error only when the total leaves i64" {
    const big: i64 = 6148914691236517205;
    const agg = Aggregate.Agg{ .func = .sum, .arg = null, .ty = types.Type.init(.int) };
    const max = std.math.maxInt(i64);
    const cases = [_]struct { parts: []const []const i64, want: ?i64 }{
        .{ .parts = &.{&.{ big, big, big }}, .want = null },
        .{ .parts = &.{&.{ std.math.minInt(i64), -1 }}, .want = null },
        .{ .parts = &.{&.{ 1, 2, 3 }}, .want = 6 },
        .{ .parts = &.{ &.{ max, max }, &.{ -max, -max }, &.{5} }, .want = 5 },
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    for (cases) |c| {
        var p = Probe.init(agg);
        for (c.parts) |part| {
            var b = column.Builder.init(ar.allocator(), types.Type.init(.int));
            for (part) |x| try b.append(.{ .int = x });
            try p.k.fold(&p.buf, try b.finish(), part.len, p.cx);
        }
        if (c.want) |w| try testing.expectEqual(Value{ .int = w }, try p.finish()) else try testing.expectError(error.IntOverflow, p.finish());
    }
}

test "aggregate: selectNth orders NaN above every number" {
    const nan = std.math.nan(f64);
    var xs = [_]f64{ 0, -0.0, nan, nan, 1.5 };
    try std.testing.expectEqual(@as(f64, 1.5), selectNth(&xs, 2));
    var ys = [_]f64{ nan, 3, 1, nan, 2 };
    try std.testing.expectEqual(@as(f64, 3), selectNth(&ys, 2));
    try std.testing.expect(std.math.isNan(selectNth(&ys, 3)));
    var zs = [_]f64{ nan, nan };
    try std.testing.expect(std.math.isNan(selectNth(&zs, 0)));
}
