//! A `Batch` is the universal currency between operators: a slice of columns
//! (struct-of-arrays) sharing one schema and row count, ~4096 rows at a time.
//! Column buffers are owned by the producing operator's per-batch arena. A lane
//! source that decodes more rows at once (a Parquet row group, ~120K) hands them on
//! through a `Slicer`, in `max_rows` views, so what follows the scan works on
//! cache-sized columns: at -j 8 it cut TPC-H Q1's CPU cycles by about 15%.

const std = @import("std");
const types = @import("../lang/types.zig");
const Column = @import("column.zig").Column;
const column_mod = @import("column.zig");
const intColumn = @import("column.zig").intColumn;

pub const Batch = struct {
    /// The most rows a sliced source hands on at once; a multiple of 8, so each
    /// slice starts on a validity byte.
    pub const max_rows: usize = 8192;

    schema: *const types.Schema,
    columns: []Column,
    len: usize,

    pub fn column(self: Batch, name: []const u8) ?*Column {
        const idx = self.schema.indexOf(name) orelse return null;
        return &self.columns[idx];
    }

    /// Every column copied into `a`, for a batch that must outlive the per-batch
    /// arena it was produced in.
    pub fn deepCopy(self: Batch, a: std.mem.Allocator) !Batch {
        const cols = try a.alloc(Column, self.columns.len);
        for (cols, self.columns) |*o, c| o.* = try column_mod.copy(a, c);
        return .{ .schema = self.schema, .columns = cols, .len = self.len };
    }

    /// Rows `start..start + n` as views on this batch's buffers (`column_mod.slice`);
    /// only the column list is allocated, in `a`.
    pub fn slice(self: Batch, a: std.mem.Allocator, start: usize, n: usize) !Batch {
        const cols = try a.alloc(Column, self.columns.len);
        for (cols, self.columns) |*o, c| o.* = try column_mod.slice(a, c, start, n);
        return .{ .schema = self.schema, .columns = cols, .len = n };
    }

    /// The batch's heap, typed stores plus validity bitmaps: what holding it costs.
    /// A byte column counts only its rows' span, as it may view a larger buffer.
    pub fn heapBytes(self: Batch) usize {
        var n: usize = 0;
        for (self.columns) |c| {
            n += c.validity.bits.len + switch (c.data) {
                .bytes => |b| b.span(c.len) + b.offsets.len * @sizeOf(i32),
                inline else => |_, t| std.mem.sliceAsBytes(column_mod.fixedOf(t, c.data)).len,
            };
        }
        return n;
    }
};

/// A source's batches handed on at most `Batch.max_rows` rows at a time, as views on
/// the batch it read. The source reads into `rows`, owned here and reset only when
/// the next batch is read, because the caller resets the arena it pulls into on every
/// pull, so a remainder kept there would be overwritten. A slice is therefore valid
/// until the caller's next pull, which every lane consumer honours (none holds a batch
/// across pulls). `rows` sits on `back`, never on a lane's arena, so a reset that
/// outgrows one buffer and `deinit` give the decoded rows back.
pub const Slicer = struct {
    back: std.mem.Allocator = std.heap.page_allocator,
    rows: ?std.heap.ArenaAllocator = null,
    pend: ?Batch = null,
    at: usize = 0,

    /// The next slice from `src` (anything with `next(arena) !?Batch`): of the batch
    /// in hand, else of the next one it reads; null when it is spent.
    pub fn read(self: *Slicer, arena: std.mem.Allocator, src: anytype) !?Batch {
        if (try self.take(arena)) |s| return s;
        const b = (try src.next(self.into())) orelse return null;
        if (b.len <= Batch.max_rows) return b;
        self.pend = b;
        self.at = 0;
        return try self.take(arena);
    }

    fn take(self: *Slicer, arena: std.mem.Allocator) !?Batch {
        const b = self.pend orelse return null;
        const n = @min(Batch.max_rows, b.len - self.at);
        const out = try b.slice(arena, self.at, n);
        self.at += n;
        if (self.at == b.len) self.pend = null;
        return out;
    }

    fn into(self: *Slicer) std.mem.Allocator {
        if (self.rows == null) self.rows = .init(self.back);
        _ = self.rows.?.reset(.retain_capacity);
        return self.rows.?.allocator();
    }

    pub fn deinit(self: *Slicer) void {
        if (self.rows) |*r| r.deinit();
        self.* = .{ .back = self.back };
    }
};

test "batch column lookup by name" {
    const alloc = std.testing.allocator;

    const id_col = try intColumn(alloc, &.{ 10, 20 });
    defer {
        alloc.free(id_col.validity.bits);
        alloc.free(id_col.data.i64);
    }

    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int) },
    } };
    var cols = [_]Column{id_col};
    const b = Batch{ .schema = &schema, .columns = &cols, .len = 2 };

    const got = b.column("id").?;
    try std.testing.expectEqual(@as(i64, 10), got.getValue(0).int);
    try std.testing.expect(b.column("missing") == null);
}

test "slices of a batch together weigh what the batch does" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var sb = column_mod.Builder.init(a, types.Type.init(.string));
    var ib = column_mod.Builder.init(a, types.Type.init(.int));
    for (0..48) |i| {
        try sb.append(.{ .string = "abcdefgh"[0 .. i % 8] });
        try ib.append(.{ .int = @intCast(i) });
    }
    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.string) },
        .{ .name = "i", .ty = types.Type.init(.int) },
    } };
    var cols = [_]Column{ try sb.finish(), try ib.finish() };
    const whole = Batch{ .schema = &schema, .columns = &cols, .len = 48 };
    var parts: usize = 0;
    var at: usize = 0;
    while (at < whole.len) : (at += 16) parts += (try whole.slice(a, at, 16)).heapBytes();
    const extra_offsets = 2 * @sizeOf(i32);
    try std.testing.expectEqual(whole.heapBytes() + extra_offsets, parts);
}

test "a slicer hands each batch on in max_rows views that outlive the caller's per-pull reset, and gives its rows back" {
    var pull = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer pull.deinit();
    const schema = types.Schema{ .fields = &.{.{ .name = "i", .ty = types.Type.init(.int).asNullable() }} };
    const Src = struct {
        schema: *const types.Schema,
        sizes: []const usize,
        k: usize = 0,
        fn next(self: *@This(), into: std.mem.Allocator) !?Batch {
            if (self.k == self.sizes.len) return null;
            const rows = self.sizes[self.k];
            self.k += 1;
            var b = column_mod.Builder.init(into, types.Type.init(.int).asNullable());
            for (0..rows) |i| try b.append(if (i % 11 == 0) .null else .{ .int = @intCast(i) });
            const cols = try into.alloc(Column, 1);
            cols[0] = try b.finish();
            return .{ .schema = self.schema, .columns = cols, .len = rows };
        }
    };
    const sizes = [_]usize{ 7, 2 * Batch.max_rows + 100, 3 * Batch.max_rows + 5, 7 };
    var src = Src{ .schema = &schema, .sizes = &sizes };
    var sl = Slicer{ .back = std.testing.allocator };
    defer sl.deinit();
    for (sizes) |rows| {
        var at: usize = 0;
        while (at < rows) {
            _ = pull.reset(.retain_capacity);
            const got = (try sl.read(pull.allocator(), &src)).?;
            try std.testing.expectEqual(@min(Batch.max_rows, rows - at), got.len);
            for (0..got.len) |r| {
                const v = got.columns[0].getValue(r);
                if ((at + r) % 11 == 0) try std.testing.expect(v == .null) else try std.testing.expectEqual(@as(i64, @intCast(at + r)), v.int);
            }
            at += got.len;
        }
    }
    try std.testing.expect((try sl.read(pull.allocator(), &src)) == null);
}
