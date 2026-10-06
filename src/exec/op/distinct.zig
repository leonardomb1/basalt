//! `DISTINCT` and `DISTINCT ON`: the first row per key, in input order.

const Batch = @import("../batch.zig").Batch;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const dupeValue = @import("aggregate.zig").dupeValue;
const keyhash = @import("../keyhash.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const intBatch = @import("testing_util.zig").intBatch;
const int_schema = @import("testing_util.zig").int_schema;
const strBatch = @import("testing_util.zig").strBatch;
const testing = std.testing;

pub const Distinct = struct {
    stats: Stats = .{},
    child: Op,
    in_schema: *const types.Schema,
    keys: ?[]const usize,
    state: std.mem.Allocator,
    gpa: std.mem.Allocator,
    seen: ?Seen = null,
    track_ords: bool = false,
    ords: []const u64 = &.{},
    seen_rows: u64 = 0,
    seen_words: ?SeenWords = null,
    seen_null: bool = false,

    const Seen = std.HashMap([]const Value, void, keyhash.MultiKeyCtx, std.hash_map.default_max_load_percentage);
    const SeenWords = std.AutoHashMap(u64, void);

    pub fn next(self: *Distinct, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.seen == null) self.seen = Seen.init(self.state);
        const seen = &self.seen.?;

        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();

        while (try self.child.next(pull)) |b| {
            var key_idx: []const usize = undefined;
            if (self.keys) |k| {
                key_idx = k;
            } else {
                const idxs = try pull.alloc(usize, b.columns.len);
                for (idxs, 0..) |*x, i| x.* = i;
                key_idx = idxs;
            }

            const keep = try pull.alloc(bool, b.len);
            const probe = try pull.alloc(Value, key_idx.len);
            var kept: usize = 0;
            var r: usize = 0;
            if (key_idx.len == 1 and fixedWord(b.columns[key_idx[0]]) != null) {
                if (self.seen_words == null) self.seen_words = SeenWords.init(self.state);
                const words = &self.seen_words.?;
                const col = b.columns[key_idx[0]];
                while (r < b.len) : (r += 1) {
                    if (!col.validity.get(r)) {
                        keep[r] = !self.seen_null;
                        self.seen_null = true;
                    } else {
                        const gop = try words.getOrPut(fixedWord(col).?[r]);
                        keep[r] = !gop.found_existing;
                    }
                    if (keep[r]) kept += 1;
                }
            } else while (r < b.len) : (r += 1) {
                for (key_idx, 0..) |ci, j| probe[j] = b.columns[ci].getValue(r);
                const gop = try seen.getOrPut(probe);
                if (gop.found_existing) {
                    keep[r] = false;
                } else {
                    const kv = try self.state.alloc(Value, key_idx.len);
                    for (key_idx, 0..) |ci, j| kv[j] = try dupeValue(self.state, b.columns[ci].getValue(r));
                    gop.key_ptr.* = kv;
                    keep[r] = true;
                    kept += 1;
                }
            }
            const base = self.seen_rows;
            self.seen_rows += b.len;
            if (kept == 0) {
                _ = scratch.reset(.retain_capacity);
                continue;
            }
            if (self.track_ords) {
                const ords = try arena.alloc(u64, kept);
                var oi: usize = 0;
                r = 0;
                while (r < b.len) : (r += 1) if (keep[r]) {
                    ords[oi] = base + r;
                    oi += 1;
                };
                self.ords = ords;
            }
            return try gatherDeep(arena, b, keep, kept);
        }
        return null;
    }
};

/// The raw 64-bit words of an int-family column, or null for a kind whose bits
/// are not its identity (floats have two zeros).
fn fixedWord(col: column.Column) ?[]const u64 {
    return switch (col.ty.kind) {
        .int, .time, .timestamp => @ptrCast(col.data.i64),
        else => null,
    };
}

/// Deep-copy the `keep`-marked rows of `b` into `arena`, duping string payloads
/// (unlike `column.gather`), for a source batch whose scratch is about to be freed.
fn gatherDeep(arena: std.mem.Allocator, b: Batch, keep: []const bool, kept: usize) anyerror!Batch {
    const outcols = try arena.alloc(column.Column, b.columns.len);
    for (b.columns, b.schema.fields, 0..) |*col, f, ci| {
        var bd = column.Builder.init(arena, f.ty);
        var r: usize = 0;
        while (r < b.len) : (r += 1) if (keep[r]) try bd.append(col.getValue(r));
        outcols[ci] = try bd.finish();
    }
    return Batch{ .schema = b.schema, .columns = outcols, .len = kept };
}

test "distinct reports the input ordinal of every surviving row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const batches = [_]Batch{
        try intBatch(a, &int_schema, &.{ 7, 7, 4 }),
        try intBatch(a, &int_schema, &.{ 4, 7, 9 }),
    };
    var ts = TestSource{ .schema_ = int_schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &int_schema, .keys = null, .state = a, .gpa = testing.allocator, .track_ords = true };

    var ords = std.array_list.Managed(u64).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        try testing.expectEqual(b.len, dst.ords.len);
        try ords.appendSlice(dst.ords);
    }
    try testing.expectEqualSlices(u64, &.{ 0, 2, 5 }, ords.items);
}

test "distinct dedups across batches, groups nulls as one key, deep-copies strings" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try strBatch(a, &schema, &.{ "a", "b", null }),
        try strBatch(a, &schema, &.{ "b", null, "c", "a" }),
    };
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var dst = Distinct{ .child = .{ .scan = &scan }, .in_schema = &schema, .keys = null, .state = a, .gpa = testing.allocator };

    var got = std.array_list.Managed(?[]const u8).init(a);
    const top = Op{ .distinct = &dst };
    while (try top.next(a)) |b| {
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const v = b.columns[0].getValue(r);
            try got.append(if (v.isNull()) null else v.string);
        }
        for (batches[0..ts.idx]) |consumed| @memset(consumed.columns[0].data.bytes.values, '#');
    }
    const want = [_]?[]const u8{ "a", "b", null, "c" };
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| {
        if (w) |s| try testing.expectEqualStrings(s, g.?) else try testing.expect(g == null);
    }
}
