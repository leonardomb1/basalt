//! A runtime filter: directly above a scan, the rows a later hash join is sure to
//! drop are dropped as they are read, before the joins and stages between pay for
//! them. Each slot is one such join (`runtime/plan/runtime_filters.zig` places
//! them): the scan columns its probe keys are, and the join's index, read once it
//! is built. A row passes a slot when no key is null, its key can be encoded as the
//! build side's, and the index's Bloom filter may hold its hash, hashed by the
//! index's own `probe`, so the filter agrees with the join by construction. A slot
//! whose index is not built (a join that spilled) passes every row.
//!
//! Once the filters standing for one join have checked `min_checked` rows between
//! them and dropped under one in `min_drop_ratio`, they turn off: the hash per row
//! costs more than it saves. The count is the join index's, shared by every lane
//! and morsel, so a run cut into many morsels does not pay the trial once each.
//! When they stop follows the lanes' timing; the rows that come out, and their
//! order, are the same either way. A batch whose rows all drop comes out empty rather than pulled past, so
//! the caller's arena is reset between batches however selective the filter is.

const Batch = @import("../batch.zig").Batch;
const Bloom = @import("bloom.zig").Bloom;
const JoinIndex = @import("join.zig").JoinIndex;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const column = @import("../column.zig");
const keytable = @import("../keytable.zig");
const std = @import("std");

pub const min_checked: u64 = 1 << 16;
pub const min_drop_ratio: u64 = 8;

pub const RuntimeFilter = struct {
    stats: Stats = .{},
    child: Op,
    back: std.mem.Allocator,
    slots: std.ArrayListUnmanaged(Slot) = .empty,
    scratch: ?std.heap.ArenaAllocator = null,
    /// The joins the slots stand for, as `EXPLAIN ANALYZE` prints them.
    note: []const u8 = "",

    pub const Slot = struct {
        index: *const ?*JoinIndex,
        cols: []const usize,
        /// The join it stands for, as `EXPLAIN ANALYZE` names it.
        label: []const u8,
        ready: bool = false,
        enc: ?keytable.Encoder = null,
        bloom: *const Bloom = undefined,
        checked: u64 = 0,
        dropped: u64 = 0,
        off: bool = false,
    };

    pub fn next(self: *RuntimeFilter, arena: std.mem.Allocator) anyerror!?Batch {
        const b = (try self.child.next(arena)) orelse return null;
        if (b.len == 0) return b;
        if (self.scratch == null) self.scratch = std.heap.ArenaAllocator.init(self.back);
        defer _ = self.scratch.?.reset(.retain_capacity);
        return try self.narrow(arena, self.scratch.?.allocator(), b);
    }

    fn narrow(self: *RuntimeFilter, arena: std.mem.Allocator, scratch: std.mem.Allocator, b: Batch) !Batch {
        var keep: ?[]bool = null;
        var kept = b.len;
        for (self.slots.items) |*s| {
            if (s.off) continue;
            const ix = s.index.* orelse continue;
            if (ix.filter_off.load(.monotonic)) {
                s.off = true;
                continue;
            }
            if (!s.ready) {
                s.enc = try ix.probeEncoder(self.back, s.cols);
                s.bloom = try ix.filter();
                s.ready = true;
            }
            var dropped: usize = 0;
            if (keep != null and kept * 2 <= b.len) {
                const k = keep.?;
                const p = try ix.probe(scratch, try liveKeys(scratch, b, k, kept, s.cols), s.cols, s.enc);
                var j: usize = 0;
                for (k) |*live| {
                    if (!live.*) continue;
                    if (p.miss[j] or !s.bloom.mayContain(p.hashes[j])) {
                        live.* = false;
                        dropped += 1;
                    }
                    j += 1;
                }
            } else {
                const p = try ix.probe(scratch, b, s.cols, s.enc);
                const k = keep orelse blk: {
                    const m = try scratch.alloc(bool, b.len);
                    @memset(m, true);
                    keep = m;
                    break :blk m;
                };
                for (k, p.miss, p.hashes) |*live, miss, h| {
                    if (!live.*) continue;
                    if (miss or !s.bloom.mayContain(h)) {
                        live.* = false;
                        dropped += 1;
                    }
                }
            }
            s.checked += kept;
            s.dropped += dropped;
            const all_checked = ix.filter_checked.fetchAdd(kept, .monotonic) + kept;
            const all_dropped = ix.filter_dropped.fetchAdd(dropped, .monotonic) + dropped;
            kept -= dropped;
            if (all_checked >= min_checked and all_dropped * min_drop_ratio < all_checked) {
                ix.filter_off.store(true, .monotonic);
                s.off = true;
            }
            if (kept == 0) break;
        }
        const k = keep orelse return b;
        if (kept == b.len) return b;
        const cols = try arena.alloc(column.Column, b.columns.len);
        try column.gatherColumns(arena, b.columns, cols, k, kept);
        return .{ .schema = b.schema, .columns = cols, .len = kept };
    }

    /// `b` with only the key columns `cols`, and only its `kept` live rows: what a
    /// later slot probes once earlier ones dropped most of the batch, rather than
    /// hashing rows already gone.
    fn liveKeys(scratch: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize, cols: []const usize) !Batch {
        const sel = try column.Selection.init(scratch, keep, kept);
        const out = try scratch.dupe(column.Column, b.columns);
        for (cols) |c| out[c] = try column.select(scratch, b.columns[c], sel);
        return .{ .schema = b.schema, .columns = out, .len = kept };
    }

    /// The rows the slots checked and dropped, and the slots turned off, summed.
    pub fn tally(self: *const RuntimeFilter) struct { checked: u64, dropped: u64, off: u64 } {
        var c: u64 = 0;
        var d: u64 = 0;
        var o: u64 = 0;
        for (self.slots.items) |s| {
            c += s.checked;
            d += s.dropped;
            o += @intFromBool(s.off);
        }
        return .{ .checked = c, .dropped = d, .off = o };
    }
};
