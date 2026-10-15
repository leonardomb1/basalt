//! An aggregate's arguments as columns, the global aggregate's fold of one batch,
//! and the output rows. The folding itself is `kernel.zig`'s; the math, `funcs.zig`.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Agg = Aggregate.Agg;
const Batch = @import("../../batch.zig").Batch;
const GroupSet = Aggregate.GroupSet;
const Layout = @import("kernel.zig").Layout;
const Value = @import("../../value.zig").Value;
const aggregates = @import("../../../lang/aggregates.zig");
const ast = @import("../../../lang/ast.zig");
const column = @import("../../column.zig");
const eval = @import("../../eval.zig");
const failLabel = @import("../../op.zig").failLabel;
const funcs = @import("funcs.zig");
const read = @import("read.zig");
const std = @import("std");
const types = @import("../../../lang/types.zig");

/// One batch into the global aggregate's single record, each argument evaluated
/// once as a column. A DECIMAL SUM of arithmetic is summed fused, the column unbuilt.
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
        if (agg.func == .sum and agg.ty.kind == .decimal) {
            const fused = eval.decSum(arena, e, b, agg.ty.scale) catch |err| return inAggregate(self, err);
            if (fused) |f| {
                const s: *funcs.SumDecimal.State = @ptrCast(@alignCast(p));
                s.s = std.math.add(i256, s.s, f.sum) catch return eval.decimalOverflow();
                s.n += @intCast(f.nvalid);
                continue;
            }
        }
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
fn inAggregate(self: *Aggregate, err: anyerror) anyerror {
    if (self.err) |ec| ec.set("{s}: in aggregate", .{failLabel(err)});
    return err;
}

/// One output row per group of `sets`, or with `sel` only the groups `sel[i]` lists
/// for set `i` (a top-N's survivors). A global aggregate over no set still answers
/// one row, each aggregate finished from its empty state.
pub fn emitSets(self: *Aggregate, arena: std.mem.Allocator, sets: []const GroupSet, sel: ?[]const []const u32) anyerror!Batch {
    const nfields = self.out_schema.fields.len;
    var n: usize = 0;
    for (sets, 0..) |*st, si| n += if (sel) |sl| sl[si].len else st.len;
    const builders = try arena.alloc(column.Builder, nfields);
    for (builders, self.out_schema.fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, @max(n, 1));

    if (n == 0 and self.by.len == 0) {
        const layout = try Layout.init(arena, self.aggs);
        const tail = try arena.alignedAlloc(u8, .of(Value), @max(layout.size, 1));
        layout.clear(tail.ptr);
        for (0..self.aggs.len) |j| try builders[j].append(layout.finish(tail.ptr, j) catch |err| return inAggregate(self, err));
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
    for (builders[nk..], 0..) |*b, j| {
        try b.append(st.finish(gi, j) catch |err| return inAggregate(self, err));
    }
}
