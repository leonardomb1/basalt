//! Accumulators: how each aggregate updates, merges across lanes and finalizes, the
//! slot layout they share, and the moments behind variance and standard deviation.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Acc = Aggregate.Acc;
const Agg = Aggregate.Agg;
const Batch = @import("../../batch.zig").Batch;
const Decimal = @import("../../value.zig").Decimal;
const GroupSet = Aggregate.GroupSet;
const Partial = Aggregate.Partial;
const Value = @import("../../value.zig").Value;
const aggregates = @import("../../../lang/aggregates.zig");
const ast = @import("../../../lang/ast.zig");
const column = @import("../../column.zig");
const dupeValue = @import("../aggregate.zig").dupeValue;
const eval = @import("../../eval.zig");
const failLabel = @import("../../op.zig").failLabel;
const lessV = @import("../aggregate.zig").lessV;
const mergeDistinct = Aggregate.mergeDistinct;
const noteDistinct = Aggregate.noteDistinct;
const noteMedian = Aggregate.noteMedian;
const reduceExtreme = @import("../aggregate.zig").reduceExtreme;
const selectNth = @import("../aggregate.zig").selectNth;
const simd = @import("../../simd.zig");
const std = @import("std");
const sumIntCol = @import("../aggregate.zig").sumIntCol;
const types = @import("../../../lang/types.zig");
const validSumF = @import("../aggregate.zig").validSumF;
const validSumI = @import("../aggregate.zig").validSumI;

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
pub fn foldVectorized(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!bool {
    for (self.aggs) |agg| if (agg.distinct) return false;
    const partials = try arena.alloc(Partial, self.aggs.len);
    for (self.aggs, partials) |agg, *p| {
        p.* = (try self.reduceBatch(arena, agg, b)) orelse return false;
    }
    for (self.aggs, partials, accs) |agg, p, *acc| try mergePartial(acc, agg, p);
    return true;
}

pub fn foldRowwise(self: *Aggregate, arena: std.mem.Allocator, b: Batch, accs: []Acc) anyerror!void {
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
pub fn argCast(agg: Agg) ?types.Type {
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
pub fn argColumn(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, e: *const ast.Expr, b: Batch) anyerror!column.Column {
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

pub fn argValue(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch, r: usize) anyerror!Value {
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

pub fn castArg(self: *Aggregate, arena: std.mem.Allocator, to: types.Type, v: Value) anyerror!Value {
    if (v.isNull()) return v;
    return eval.castValueTyped(arena, v, to) catch |err| {
        if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
        return err;
    };
}

/// Vectorized reduce of one agg over one batch; null means fold row-wise. A null
/// slot holds whatever its producer left, so only an all-valid batch is summed blind.
pub fn reduceBatch(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, b: Batch) anyerror!?Partial {
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
pub fn foldsRowwise(func: ast.AggFunc) bool {
    return switch (func) {
        .count, .sum, .avg, .min, .max => false,
        .median, .count_if, .bool_and, .bool_or, .bit_and, .bit_or, .bit_xor, .var_samp, .var_pop, .stddev_samp, .stddev_pop => true,
    };
}

/// Fold one batch's `Partial` into the running accumulator, with `updateAcc`'s
/// semantics.
pub fn mergePartial(acc: *Acc, agg: Agg, p: Partial) error{IntOverflow}!void {
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

pub fn emitGroup(self: *Aggregate, builders: []column.Builder, st: *const GroupSet, gi: usize) !void {
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
pub fn updateAcc(state: std.mem.Allocator, acc: *Acc, agg: Agg, v: Value, has_arg: bool) !void {
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
pub fn addExact(sum: *align(8) i128, agg: Agg, v: Value) !void {
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

    pub fn of(agg: Agg) Slot {
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

pub const SumI = struct { n: i64, s: i128 align(8) };

pub const SumF = struct { n: i64, s: f64 };

pub const Layout = struct {
    slots: []const Slot,
    offs: []const usize,
    size: usize,
    all_count: bool,

    pub fn init(a: std.mem.Allocator, aggs: []const Agg) !Layout {
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

    pub fn ptr(self: Layout, comptime T: type, tail: [*]u8, j: usize) *T {
        return @ptrCast(@alignCast(tail + self.offs[j]));
    }

    pub fn clear(self: Layout, tail: [*]u8) void {
        for (self.slots, 0..) |sl, j| switch (sl) {
            .count => self.ptr(i64, tail, j).* = 0,
            .sum_i => self.ptr(SumI, tail, j).* = .{ .n = 0, .s = 0 },
            .sum_f => self.ptr(SumF, tail, j).* = .{ .n = 0, .s = 0 },
            .full => self.ptr(Acc, tail, j).* = .{},
        };
    }

    pub fn acc(self: Layout, tail: [*]u8, j: usize) Acc {
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

    pub fn update(self: Layout, state: std.mem.Allocator, tail: [*]u8, j: usize, agg: Agg, v: Value) !void {
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

    pub fn merge(self: Layout, alloc: std.mem.Allocator, dst: [*]u8, src: [*]u8, j: usize, agg: Agg) !void {
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

pub fn bitFold(func: ast.AggFunc, a: i128, b: i128) i128 {
    return switch (func) {
        .bit_and => a & b,
        .bit_or => a | b,
        .bit_xor => a ^ b,
        else => unreachable,
    };
}

pub fn sqDev(acc: Acc) f64 {
    return if (acc.ext == .float) acc.ext.float else 0;
}

/// Welford's update. `ext` is read before it is assigned: assigning the union may
/// set its tag first and read back a garbage float.
pub fn noteMoment(acc: *Acc, x: f64) void {
    const m2 = sqDev(acc.*);
    acc.n += 1;
    const delta = x - acc.sum_f;
    acc.sum_f += delta / @as(f64, @floatFromInt(acc.n));
    acc.ext = .{ .float = m2 + delta * (x - acc.sum_f) };
}

/// Chan et al.'s pairwise combination of two Welford states.
pub fn mergeMoments(dst: *Acc, src: Acc) void {
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

pub fn spread(func: ast.AggFunc, variance: f64) Value {
    return .{ .float = switch (func) {
        .stddev_samp, .stddev_pop => @sqrt(variance),
        else => variance,
    } };
}
