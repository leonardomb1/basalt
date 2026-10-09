//! Hash partitioning to disk for `GROUP BY` and `DISTINCT` past `--op-memory`.
//!
//! The scheme (Grace-style, without serializing aggregate states): an operator
//! meters what its `state` allocator hands out (`Meter`); once that, plus its hash
//! table, passes `spill_at`, the table is frozen. A row whose key is already in the
//! table is still folded in memory; a row whose key is not goes, whole, to one of
//! `fanout` spill files picked by `partOf` from the key's hash (`Parts`). Every row
//! of a key therefore lands in exactly one place, memory or one file, and a file
//! keeps input order, so each group sees its rows in the order it would have
//! without spilling and every result is exact. When the input ends the operator
//! emits what it holds, then, one file per call, replays it (`RunSource`) through
//! a fresh copy of itself on its own arena (`Replay`), drained and freed within
//! that call. A copy that overflows partitions again one level deeper with
//! another salt; past the operator's depth limit it fails with
//! `error.SpillTooDeep`. Output order without ORDER BY differs from an unspilled
//! run.

const Batch = @import("../batch.zig").Batch;
const Scan = @import("../op.zig").Scan;
const Space = @import("../space.zig").Space;
const column = @import("../column.zig");
const driver = @import("../../connect/driver.zig");
const spill = @import("../spill.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");

pub const fanout = 16;
pub const default_max_depth: u8 = 8;
pub const keep: u8 = 0xFF;

/// The spill file a key's hash goes to at `depth`. Each depth salts the hash
/// differently, so the keys of one file spread over all files of the next level.
pub fn partOf(h: u64, depth: u8) u8 {
    var x = h ^ (0x9E3779B97F4A7C15 *% (@as(u64, depth) + 1));
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    x *%= 0xc4ceb9fe1a85ec53;
    x ^= x >> 33;
    return @intCast(x >> 60);
}

/// An allocator that counts the bytes it hands out, never what is freed: over an
/// arena that is closer to what is held than counting frees would be.
pub const Meter = struct {
    child: std.mem.Allocator,
    bytes: usize = 0,

    const vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };

    pub fn allocator(self: *Meter) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        const p = self.child.rawAlloc(len, alignment, ra) orelse return null;
        self.bytes += len;
        return p;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) bool {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(memory, alignment, new_len, ra)) return false;
        if (new_len > memory.len) self.bytes += new_len - memory.len;
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        const p = self.child.rawRemap(memory, alignment, new_len, ra) orelse return null;
        if (new_len > memory.len) self.bytes += new_len - memory.len;
        return p;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *Meter = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ra);
    }
};

/// The `fanout` spill files of one operator at one depth, each made on its first row.
pub const Parts = struct {
    space: Space,
    alloc: std.mem.Allocator,
    schema: *const types.Schema,
    tag: []const u8,
    writers: [fanout]?spill.Writer = @splat(null),

    /// Appends each row `r` of `b` with `dest[r] != keep` to file `dest[r]`.
    pub fn route(self: *Parts, scratch: std.mem.Allocator, b: Batch, dest: []const u8) !void {
        const mask = try scratch.alloc(bool, b.len);
        for (0..fanout) |p| {
            var n: usize = 0;
            for (dest[0..b.len], mask) |d, *m| {
                m.* = d == p;
                if (m.*) n += 1;
            }
            if (n == 0) continue;
            const cols = try scratch.alloc(column.Column, b.columns.len);
            for (cols, b.columns) |*o, c| o.* = try column.gather(scratch, c, mask, n);
            if (self.writers[p] == null) self.writers[p] = try spill.Writer.init(self.space, self.alloc, self.schema, self.tag);
            try self.writers[p].?.write(.{ .schema = self.schema, .columns = cols, .len = n });
        }
    }

    /// Closes every file; the runs that hold rows, in partition order.
    pub fn finish(self: *Parts) ![]spill.Run {
        var runs = std.array_list.Managed(spill.Run).init(self.alloc);
        for (&self.writers) |*w| if (w.*) |*wr| {
            const run = try wr.finish();
            if (run.rows == 0) {
                spill.discard(run);
            } else try runs.append(run);
            w.* = null;
        };
        return runs.toOwnedSlice();
    }

    pub fn abort(self: *Parts) void {
        for (&self.writers) |*w| if (w.*) |*wr| {
            wr.abort();
            w.* = null;
        };
    }
};

/// A spill file read back as a source, deleted once read to the end. A batch stays
/// valid until the next pull.
pub const RunSource = struct {
    run: spill.Run,
    reader: ?spill.Reader = null,
    done: bool = false,

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    pub fn src(self: *RunSource) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn schemaFn(p: *anyopaque) types.Schema {
        const self: *RunSource = @ptrCast(@alignCast(p));
        return self.run.schema.*;
    }

    fn nextFn(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *RunSource = @ptrCast(@alignCast(p));
        if (self.done) return null;
        if (self.reader == null) self.reader = try spill.Reader.open(self.run);
        if (try self.reader.?.next(arena)) |b| return b;
        self.end();
        return null;
    }

    fn closeFn(p: *anyopaque) void {
        const self: *RunSource = @ptrCast(@alignCast(p));
        self.end();
    }

    pub fn end(self: *RunSource) void {
        if (self.done) return;
        if (self.reader) |*r| r.close();
        self.reader = null;
        spill.discard(self.run);
        self.done = true;
    }
};

/// Replays spill runs one per call through a fresh `T` made by
/// `parent.replayChild(child_op, state)`, returning all it outputs for that run
/// as one batch. The copy lives in its own arena over `gpa` and is drained and
/// freed, with its file, within the call: a consumer that stops pulling early
/// (a LIMIT) leaves nothing behind.
pub fn Replay(comptime T: type) type {
    return struct {
        const Self = @This();

        runs: []const spill.Run,
        idx: usize = 0,

        pub fn next(self: *Self, parent: *T, gpa: std.mem.Allocator, arena: std.mem.Allocator) anyerror!?Batch {
            while (self.idx < self.runs.len) {
                const run = self.runs[self.idx];
                self.idx += 1;
                var ar = std.heap.ArenaAllocator.init(gpa);
                defer ar.deinit();
                const a = ar.allocator();
                var rs = RunSource{ .run = run };
                defer rs.end();
                var scan = Scan{ .src = rs.src() };
                var op = parent.replayChild(.{ .scan = &scan }, a);
                var outs = std.array_list.Managed(Batch).init(a);
                var total: usize = 0;
                while (try op.next(arena)) |b| if (b.len > 0) {
                    try outs.append(b);
                    total += b.len;
                };
                if (outs.items.len == 0) continue;
                if (outs.items.len == 1) return outs.items[0];
                const first = outs.items[0];
                const cols = try arena.alloc(column.Column, first.columns.len);
                const chunks = try a.alloc(column.Column, outs.items.len);
                for (cols, 0..) |*c, ci| {
                    for (chunks, outs.items) |*ch, b| ch.* = b.columns[ci];
                    c.* = try column.concat(arena, chunks, total);
                }
                return .{ .schema = first.schema, .columns = cols, .len = total };
            }
            return null;
        }
    };
}
