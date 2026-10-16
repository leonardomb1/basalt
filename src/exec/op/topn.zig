//! `ORDER BY … LIMIT n [OFFSET m]`: the best m+n rows through the top-k core
//! (`topk.zig`), never a full sort.

const Batch = @import("../batch.zig").Batch;
const Entry = @import("topk.zig").Entry;
const Op = @import("../op.zig").Op;
const Sort = @import("sort.zig").Sort;
const Stats = @import("../op.zig").Stats;
const Threshold = @import("../value.zig").Threshold;
const TopK = @import("topk.zig").TopK;
const column = @import("../column.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Limit = @import("../op.zig").Limit;
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const driver = @import("../../connect/driver.zig");
const kvBatch = @import("testing_util.zig").kvBatch;
const testing = std.testing;

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

    pub const Kept = struct { rows: Batch, seqs: []const u64 };

    pub fn next(self: *TopN, arena: std.mem.Allocator) anyerror!?Batch {
        if (self.done) return null;
        self.done = true;
        if (self.count == 0) return null;
        var core = TopK.init(self.gpa, self.in_schema, self.keys, &.{}, @intCast(self.offset + self.count));
        core.threshold = self.threshold;
        defer core.deinit();
        try self.feed(&core);
        return try window(&core, arena, self.in_schema, self.offset);
    }

    /// Every row kept, best first, copied into `arena` with its input position.
    pub fn nextKept(self: *TopN, arena: std.mem.Allocator) anyerror!?Kept {
        if (self.done) return null;
        self.done = true;
        if (self.count == 0) return null;
        var core = TopK.init(self.gpa, self.in_schema, self.keys, &.{}, @intCast(self.offset + self.count));
        core.threshold = self.threshold;
        defer core.deinit();
        try self.feed(&core);
        const groups = try core.finish(arena);
        if (groups.len == 0 or groups[0].len == 0) return null;
        const kept = groups[0];
        const seqs = try arena.alloc(u64, kept.len);
        for (seqs, kept) |*s, e| s.* = e.seq;
        return .{ .rows = try rowsOf(&core, arena, self.in_schema, kept), .seqs = seqs };
    }

    /// Rows `offset` to `offset + count` of what lanes kept, ranked as one `TopN` over
    /// all their input would rank them.
    pub fn combine(gpa: std.mem.Allocator, arena: std.mem.Allocator, schema: *const types.Schema, keys: []const Sort.Key, count: u64, offset: u64, lanes: []const Kept) !?Batch {
        if (count == 0) return null;
        var core = TopK.init(gpa, schema, keys, &.{}, @intCast(offset + count));
        defer core.deinit();
        var scratch = std.heap.ArenaAllocator.init(gpa);
        defer scratch.deinit();
        for (lanes) |l| {
            try core.add(scratch.allocator(), l.rows, 0, l.seqs);
            _ = scratch.reset(.retain_capacity);
        }
        return try window(&core, arena, schema, offset);
    }

    /// Each child batch offered at its input position: `seq_base` plus the rows met
    /// before it, counted afresh per row group `item` under a stealing source.
    fn feed(self: *TopN, core: *TopK) !void {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const pull = scratch.allocator();
        while (try self.child.next(pull)) |b| {
            if (self.item) |it| if (it.* != self.last_item) {
                self.last_item = it.*;
                self.seq_base = @as(u64, it.*) << 40;
                self.seen = 0;
            };
            try core.add(pull, b, self.seq_base + self.seen, null);
            self.seen += b.len;
            _ = scratch.reset(.retain_capacity);
        }
    }

    fn window(core: *TopK, arena: std.mem.Allocator, schema: *const types.Schema, offset: u64) !?Batch {
        const groups = try core.finish(arena);
        if (groups.len == 0) return null;
        const kept = groups[0];
        if (offset >= kept.len) return null;
        return try rowsOf(core, arena, schema, kept[@intCast(offset)..]);
    }

    fn rowsOf(core: *const TopK, arena: std.mem.Allocator, schema: *const types.Schema, entries: []const Entry) !Batch {
        const cols = try arena.alloc(column.Column, schema.fields.len);
        try core.columns(arena, entries, schema.fields, cols);
        return .{ .schema = schema, .columns = cols, .len = entries.len };
    }
};

test "top_n keeps best rows across batches, honors offset, matches full sort" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{
        try kvBatch(a, &schema, &.{ 5, 1, 4 }, &.{ "e", "a", "d" }),
        try kvBatch(a, &schema, &.{ 2, 8, 3 }, &.{ "b", "z", "c" }),
        try kvBatch(a, &schema, &.{ null, 0, 9 }, &.{ "n", "y", "q" }),
    };
    const Row = struct { x: ?i64, s: []const u8 };
    const drain = struct {
        fn f(al: std.mem.Allocator, top: Op) ![]const Row {
            var rows = std.array_list.Managed(Row).init(al);
            while (try top.next(al)) |b| {
                var r: usize = 0;
                while (r < b.len) : (r += 1) {
                    const v = b.columns[0].getValue(r);
                    try rows.append(.{ .x = if (v.isNull()) null else v.int, .s = b.columns[1].getValue(r).string });
                }
            }
            return rows.toOwnedSlice();
        }
    }.f;

    const cases = [_]struct { count: u64, offset: u64, desc: bool }{
        .{ .count = 2, .offset = 1, .desc = false },
        .{ .count = 3, .offset = 0, .desc = true },
        .{ .count = 4, .offset = 7, .desc = false },
        .{ .count = 5, .offset = 6, .desc = true },
    };
    for (cases, 0..) |tc, ci| {
        const keys = try a.dupe(Sort.Key, &.{.{ .idx = 0, .desc = tc.desc, .nulls_first = false }});

        var ts = TestSource{ .schema_ = schema, .batches = &batches };
        var scan = Scan{ .src = ts.src() };
        var tn = TopN{
            .child = .{ .scan = &scan },
            .in_schema = &schema,
            .keys = keys,
            .count = tc.count,
            .offset = tc.offset,
            .state = a,
            .gpa = testing.allocator,
        };
        const got = try drain(a, .{ .top_n = &tn });

        var ts_ref = TestSource{ .schema_ = schema, .batches = &batches };
        var scan_ref = Scan{ .src = ts_ref.src() };
        var srt = Sort{ .child = .{ .scan = &scan_ref }, .in_schema = &schema, .keys = keys };
        var lim = Limit{ .child = .{ .sort = &srt }, .remaining = tc.count, .to_skip = tc.offset };
        const want = try drain(a, .{ .limit = &lim });

        try testing.expectEqual(want.len, got.len);
        for (want, got) |w, g| {
            try testing.expectEqual(w.x, g.x);
            try testing.expectEqualStrings(w.s, g.s);
        }

        if (ci == 0) {
            try testing.expectEqual(@as(usize, 2), got.len);
            try testing.expectEqual(@as(?i64, 1), got[0].x);
            try testing.expectEqualStrings("a", got[0].s);
            try testing.expectEqual(@as(?i64, 2), got[1].x);
            try testing.expectEqualStrings("b", got[1].s);
        }
    }
}

test "top_n: equal keys rank by input position, also when a lane reads items out of order" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = types.Schema{ .fields = &.{
        .{ .name = "x", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    } };
    const Src = struct {
        batches: []const Batch,
        items: []const usize,
        i: usize = 0,
        cur: usize = 0,
        sch: types.Schema,
        fn schemaFn(p: *anyopaque) types.Schema {
            return @as(*@This(), @ptrCast(@alignCast(p))).sch;
        }
        fn nextFn(p: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
            const self: *@This() = @ptrCast(@alignCast(p));
            if (self.i == self.batches.len) return null;
            defer self.i += 1;
            self.cur = self.items[self.i];
            return self.batches[self.i];
        }
        fn closeFn(_: *anyopaque) void {}
        const vt = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
    };
    const batches = [_]Batch{
        try kvBatch(a, &schema, &.{ 7, 7, 7 }, &.{ "five-a", "five-b", "five-c" }),
        try kvBatch(a, &schema, &.{ 7, 7, 7 }, &.{ "two-a", "two-b", "two-c" }),
    };
    var src = Src{ .batches = &batches, .items = &.{ 5, 2 }, .sch = schema };
    var scan = Scan{ .src = .{ .ptr = &src, .vtable = &Src.vt } };
    var tn = TopN{
        .child = .{ .scan = &scan },
        .in_schema = &schema,
        .keys = &[_]Sort.Key{.{ .idx = 0, .desc = true, .nulls_first = false }},
        .count = 2,
        .offset = 0,
        .state = a,
        .gpa = testing.allocator,
        .item = &src.cur,
    };
    const b = (try tn.next(a)).?;
    try testing.expectEqualStrings("two-a", b.columns[1].getValue(0).string);
    try testing.expectEqualStrings("two-b", b.columns[1].getValue(1).string);
}
