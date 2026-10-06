//! `ORDER BY … LIMIT n`: a bounded heap of the best n rows, never a full sort.

const Batch = @import("../batch.zig").Batch;
const Op = @import("../op.zig").Op;
const Sort = @import("sort.zig").Sort;
const Stats = @import("../op.zig").Stats;
const Threshold = @import("../value.zig").Threshold;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const dupeValue = @import("aggregate.zig").dupeValue;
const keyOrder = @import("sort.zig").keyOrder;
const std = @import("std");
const types = @import("../../lang/types.zig");

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

pub fn dupeRowGpa(gpa: std.mem.Allocator, row: []const Value) ![]Value {
    const out = try gpa.alloc(Value, row.len);
    for (out, row) |*o, v| o.* = try dupeValueGpa(gpa, v);
    return out;
}

pub fn freeRowGpa(gpa: std.mem.Allocator, row: []Value) void {
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
