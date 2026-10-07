//! Window functions: rank, lag/lead and framed aggregates over sorted partitions.
//! A breaker: a partition is complete before any of its rows is numbered.

const Batch = @import("../batch.zig").Batch;
const ErrCtx = @import("../op.zig").ErrCtx;
const KeyArr = @import("sort.zig").KeyArr;
const Op = @import("../op.zig").Op;
const Sort = @import("sort.zig").Sort;
const Stats = @import("../op.zig").Stats;
const Decimal = @import("../value.zig").Decimal;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const dupeRowGpa = @import("topn.zig").dupeRowGpa;
const errLabel = @import("../op.zig").errLabel;
const rescaleTo = @import("../eval.zig").rescaleTo;
const freeRowGpa = @import("topn.zig").freeRowGpa;
const keyOrder = @import("sort.zig").keyOrder;
const keyhash = @import("../keyhash.zig");
const lessV = @import("aggregate.zig").lessV;
const materializeAll = @import("../op.zig").materializeAll;
const sortIdxThreads = @import("sort.zig").sortIdxThreads;
const std = @import("std");
const types = @import("../../lang/types.zig");

/// Adds (or, leaving a ROWS frame, takes away) a value from a DECIMAL sum, exactly, at
/// the sum's scale, as the SUM aggregate does; a float total would read 0.1 + 0.2 as
/// 0.30000000000000004.
fn decStep(acc: *i128, v: Value, scale: u8, sub: bool) !void {
    const d: Decimal = switch (v) {
        .decimal => |x| x,
        .int => |x| .{ .unscaled = x, .scale = 0 },
        else => return error.TypeMismatch,
    };
    const u = (rescaleTo(d, scale) orelse return error.CastFailed).unscaled;
    acc.* = (if (sub) std.math.sub(i128, acc.*, u) else std.math.add(i128, acc.*, u)) catch return error.CastFailed;
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
    pub const Func = struct { kind: Kind, arg: ?usize = null, offset: i64 = 1, default: Value = .null, frame: Frame = .{} };

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
                            out.*[k] = f.default;
                        } else {
                            out.*[k] = all.columns[f.arg.?].getValue(idx[@intCast(target)]);
                        }
                    }
                },
                .sum, .count, .min, .max, .avg => if (f.frame.rows) {
                    const dec: ?u8 = if (f.kind == .sum and bd.ty.kind == .decimal) bd.ty.scale else null;
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
                            if (dec) |s| try decStep(&acc_i, v, s, true) else switch (v) {
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
                            if (dec) |s| try decStep(&acc_i, v, s, false) else switch (v) {
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
                            else => if (n == 0) .null else if (dec) |s| .{ .decimal = .{ .unscaled = acc_i, .scale = s } } else if (seen_float) .{ .float = acc_f } else try self.intSum(acc_i),
                        };
                    }
                } else {
                    const dec: ?u8 = if (f.kind == .sum and bd.ty.kind == .decimal) bd.ty.scale else null;
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
                                    if (dec) |s| try decStep(&acc_i, v, s, false) else switch (v) {
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
                                    else => if (n == 0) .null else if (dec) |s| .{ .decimal = .{ .unscaled = acc_i, .scale = s } } else if (seen_float) .{ .float = acc_f } else try self.intSum(acc_i),
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
