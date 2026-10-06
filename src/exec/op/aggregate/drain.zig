//! The aggregate's input loop: batches folded into the group tables, partitioned
//! into parts for a parallel combine, and the group sets drained in the end.

const Aggregate = @import("../aggregate.zig").Aggregate;
const Acc = Aggregate.Acc;
const Batch = @import("../../batch.zig").Batch;
const Direct = Aggregate.Direct;
const Fast = Aggregate.Fast;
const FixedKey = Aggregate.FixedKey;
const FixedStore = Aggregate.FixedStore;
const GroupSet = Aggregate.GroupSet;
const GroupStore = Aggregate.GroupStore;
const GroupTable = Aggregate.GroupTable;
const KeyKind = Aggregate.KeyKind;
const Layout = Aggregate.Layout;
const StrId = Aggregate.StrId;
const SumF = Aggregate.SumF;
const SumI = Aggregate.SumI;
const Value = @import("../../value.zig").Value;
const column = @import("../../column.zig");
const dupeValue = @import("../aggregate.zig").dupeValue;
const few_groups = Aggregate.few_groups;
const fixedHash = Aggregate.fixedHash;
const keyKindOf = Aggregate.keyKindOf;
const keyhash = @import("../../keyhash.zig");
const prefetch_ahead = Aggregate.prefetch_ahead;
const std = @import("std");
const types = @import("../../../lang/types.zig");

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

pub const part_shift = 58;

pub fn partOf(h: u64) usize {
    return @intCast(h >> part_shift);
}

/// One partition's table and the store it indexes. A serial fold shares one store,
/// so groups keep first-seen order; a lane's partitions each own theirs.
pub fn FoldPart(comptime Store: type) type {
    return struct {
        table: GroupTable,
        store: *Store,
        alloc: std.mem.Allocator,
    };
}

/// The tables live in a real allocator, not the arena, so a growing table frees
/// what it outgrew.
pub fn foldParts(self: *Aggregate, comptime Store: type, layout: *const Layout) ![]FoldPart(Store) {
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

pub fn newIn(a: std.mem.Allocator, v: anytype) !*@TypeOf(v) {
    const p = try a.create(@TypeOf(v));
    p.* = v;
    return p;
}

pub fn foldSets(self: *Aggregate, comptime tag: std.meta.Tag(GroupSet.Store), parts: anytype) ![]GroupSet {
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

pub fn keyKinds(self: *Aggregate) ![]types.TypeKind {
    const kinds = try self.state.alloc(types.TypeKind, self.by.len);
    for (self.by, kinds) |ci, *k| k.* = self.in_schema.fields[ci].ty.kind;
    return kinds;
}

/// Fold with raw fixed-width keys. Floats group by number, not bits, as everywhere
/// else (-0.0 split from 0.0 before). Below `few_groups` rows go in arrival order;
/// past it a batch is walked a partition at a time so probes share a table.
pub fn drainFixed(self: *Aggregate, kinds: []const KeyKind, comptime counts_only: bool) anyerror![]GroupSet {
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
pub fn drainImpl(self: *Aggregate) anyerror![]GroupSet {
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
