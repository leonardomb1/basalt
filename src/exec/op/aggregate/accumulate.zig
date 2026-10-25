//! An aggregate's arguments as columns, the global aggregate's fold of one batch,
//! and the output rows. The folding itself is `kernel.zig`'s; the math, `funcs.zig`.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Agg = Aggregate.Agg;
const Batch = @import("../../batch.zig").Batch;
const Decimal = @import("../../value.zig").Decimal;
const GroupSet = Aggregate.GroupSet;
const Layout = @import("kernel.zig").Layout;
const Value = @import("../../value.zig").Value;
const ast = @import("../../../lang/ast.zig");
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const failLabel = @import("../../op.zig").failLabel;
const keytable = @import("../../keytable.zig");
const read = @import("read.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

/// One batch into the global aggregate's single record, each argument evaluated
/// once as a column, or straight from its operands where the kernel has `foldExpr`.
pub fn foldSingle(self: *Aggregate, arena: std.mem.Allocator, b: Batch, layout: *const Layout, tail: [*]u8) anyerror!void {
    for (self.aggs, 0..) |agg, j| {
        const p = tail + layout.offs[j];
        var cx = layout.ctx(j, self.state);
        cx.scratch = arena;
        const e = agg.arg orelse {
            try layout.kernels[j].fold(p, null, b.len, cx);
            continue;
        };
        if (agg.distinct) {
            try self.pairsOf(j).add(arena, try self.argColumn(arena, agg, e, b), b.len, null, self.interner());
            continue;
        }
        if (layout.kernels[j].foldExpr) |fx| if (fx(p, e, b, cx) catch |err| return inAggregate(self, err)) continue;
        try layout.kernels[j].fold(p, try self.argColumn(arena, agg, e, b), b.len, cx);
    }
}

pub fn argCast(agg: Agg) ?types.Type {
    return read.castTo(agg.func, agg.ty);
}

/// One agg's argument across a batch, a text column cast per `argCast`. A stored
/// column is handed over uncopied when its type feeds the accumulator, or always
/// for COUNT, which reads only presence and values as they are; others are built as
/// the type they read (`count_if` folds BOOLs into an INT).
pub fn argColumn(self: *Aggregate, arena: std.mem.Allocator, agg: Agg, e: *const ast.Expr, b: Batch) anyerror!column.Column {
    var want = argCast(agg) orelse agg.ty;
    if (agg.func == .count) want.unknown = true;
    if (e.* == .field) {
        if (b.schema.resolve(e.field.parts)) |ci| {
            const ck = b.columns[ci].ty.kind;
            const ak = want.kind;
            if (agg.func == .count or ck == ak or (ck.isNumeric() and ak.isNumeric())) return b.columns[ci];
        }
    }
    const col = eval.evalColumn(arena, e, b, want) catch |err| return inAggregate(self, err);
    return read.castText(arena, agg.func, agg.ty, col) catch |err| inAggregate(self, err);
}

/// `err`, its message noted as raised in an aggregate.
pub fn inAggregate(self: *Aggregate, err: anyerror) anyerror {
    if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
    return err;
}

/// One output row per group of `sets`, or with `sel` only the groups `sel[i]` lists
/// for set `i` (a top-N's survivors), a column at a time over chunks of groups small
/// enough that their records stay cached across the columns: each group's record is
/// found once, fixed-width keys are written from their words, and each aggregate's
/// results come from one loop of its kernel. A global aggregate over no set still
/// answers one row, each aggregate finished from its empty state.
pub fn emitSets(self: *Aggregate, arena: std.mem.Allocator, sets: []const GroupSet, sel: ?[]const []const u32) anyerror!Batch {
    const nfields = self.out_schema.fields.len;
    const nk = self.by.len;
    var n: usize = 0;
    for (sets, 0..) |*st, si| n += if (sel) |sl| sl[si].len else st.len;
    const cols = try arena.alloc(column.Column, nfields);

    if (n == 0 and nk == 0) {
        const layout = try Layout.init(arena, self.aggs);
        const tail = try arena.alignedAlloc(u8, .of(Value), @max(layout.size, 1));
        layout.clear(tail.ptr);
        var outs = try Outs.init(self, arena, 1, false);
        try outs.chunk(self, layout, &.{tail.ptr}, &.{}, &.{});
        try outs.finish(cols);
        return Batch{ .schema = self.out_schema, .columns = cols, .len = 1 };
    }

    var fixed = sets.len > 0 and sets[0].store == .fixed;
    if (fixed) for (self.out_schema.fields[0..nk], sets[0].key_kinds) |f, kk| {
        if (column.storageOf(f.ty.kind) != column.storageOf(kk)) fixed = false;
    };
    var outs = try Outs.init(self, arena, n, fixed);
    if (fixed) outs.strs = sets[0].strs;
    const layout = if (n > 0) sets[0].layoutOf().* else undefined;
    var tails: [emit_chunk][*]const u8 = undefined;
    var keys: [emit_chunk][*]const i64 = undefined;
    var masks: [emit_chunk]u64 = undefined;
    var c: usize = 0;
    for (sets, 0..) |*st, si| {
        const rows: ?[]const u32 = if (sel) |sl| sl[si] else null;
        const cnt = if (rows) |r| r.len else st.len;
        for (0..cnt) |x| {
            const gi = if (rows) |r| r[x] else x;
            if (fixed) {
                const rec = st.store.fixed.at(gi);
                tails[c] = rec.tail;
                keys[c] = rec.keys.ptr;
                masks[c] = rec.mask.*;
            } else {
                tails[c] = st.tail(gi);
                for (outs.boxed, 0..) |*b, j| try b.append(st.keyValue(gi, j));
            }
            c += 1;
            if (c == emit_chunk) {
                try outs.chunk(self, layout, tails[0..c], keys[0..c], masks[0..c]);
                c = 0;
            }
        }
    }
    if (c > 0) try outs.chunk(self, layout, tails[0..c], keys[0..c], masks[0..c]);
    try outs.finish(cols);
    return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
}

const emit_chunk = 2048;

/// The output columns `emitSets` fills a chunk of groups at a time: fixed-width keys
/// written from their words into `data` (strings through the table that interned
/// them), boxed keys appended as values, and each aggregate by its kernel.
const Outs = struct {
    tys: []const types.Type,
    nk: usize,
    at: usize = 0,
    data: []column.Column.Data,
    bms: []column.Bitmap,
    strs_out: []column.BytesAppender,
    boxed: []column.Builder,
    aggs: []column.Builder,
    strs: ?*const keytable.StrTable = null,

    fn init(self: *Aggregate, arena: std.mem.Allocator, n: usize, fixed: bool) !Outs {
        const fields = self.out_schema.fields;
        const nk = self.by.len;
        const tys = try arena.alloc(types.Type, fields.len);
        for (tys, fields) |*t, f| t.* = f.ty;
        const o = Outs{
            .tys = tys,
            .nk = nk,
            .data = try arena.alloc(column.Column.Data, if (fixed) nk else 0),
            .bms = try arena.alloc(column.Bitmap, if (fixed) nk else 0),
            .strs_out = try arena.alloc(column.BytesAppender, if (fixed) nk else 0),
            .boxed = try arena.alloc(column.Builder, if (fixed) 0 else nk),
            .aggs = try arena.alloc(column.Builder, fields.len - nk),
        };
        for (o.data, o.bms, o.strs_out, tys[0..o.data.len]) |*d, *bm, *sa, ty| {
            bm.* = try column.Bitmap.initFull(arena, n);
            switch (column.storageOf(ty.kind)) {
                .bytes => {
                    sa.* = try column.BytesAppender.init(arena, n);
                    d.* = .{ .bytes = undefined };
                },
                inline else => |t| d.* = column.dataOf(t, try arena.alloc(column.Elem(t), n)),
            }
        }
        for (o.boxed, tys[0..o.boxed.len]) |*b, ty| b.* = try column.Builder.initCapacity(arena, ty, @max(n, 1));
        for (o.aggs, tys[nk..]) |*b, ty| b.* = try column.Builder.initCapacity(arena, ty, @max(n, 1));
        return o;
    }

    fn chunk(o: *Outs, self: *Aggregate, layout: Layout, tails: []const [*]const u8, keys: []const [*]const i64, masks: []const u64) !void {
        for (o.data, o.bms, o.strs_out, 0..) |*d, *bm, *sa, j| {
            const bit = @as(u64, 1) << @intCast(j);
            switch (d.*) {
                .bytes => for (keys, masks, o.at..) |k, m, r| {
                    if (m & bit != 0) {
                        bm.setValid(r, false);
                        try sa.pushNull();
                    } else try sa.push(o.strs.?.at(@intCast(k[j])));
                },
                inline else => |_, tag| {
                    const arr = column.fixedOf(tag, d.*);
                    const T = column.Elem(tag);
                    for (keys, masks, arr[o.at..][0..keys.len], o.at..) |k, m, *x, r| {
                        if (m & bit != 0) {
                            bm.setValid(r, false);
                            x.* = if (T == Decimal) .{ .unscaled = 0, .scale = 0 } else std.mem.zeroes(T);
                            continue;
                        }
                        const w = k[j];
                        x.* = switch (T) {
                            i64 => w,
                            i32 => @intCast(w),
                            bool => w != 0,
                            f64 => @bitCast(w),
                            else => unreachable,
                        };
                    }
                },
            }
        }
        for (o.aggs, 0..) |*b, j| layout.finishAll(tails, j, b) catch |err| return inAggregate(self, err);
        o.at += tails.len;
    }

    fn finish(o: *Outs, cols: []column.Column) !void {
        for (o.data, o.bms, o.strs_out, cols[0..o.data.len], o.tys[0..o.data.len]) |*d, bm, *sa, *c, ty| {
            if (d.* == .bytes) d.* = .{ .bytes = try sa.finish() };
            c.* = column.Column.derived(ty, o.at, bm, d.*);
        }
        for (o.boxed, cols[0..o.boxed.len]) |*b, *c| c.* = try b.finish();
        for (o.aggs, cols[o.nk..]) |*b, *c| c.* = try b.finish();
    }
};
