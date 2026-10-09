//! Spill files: batches a blocking operator (join, sort, aggregate) cannot keep in
//! memory, dumped raw to its run's scratch `Space` and read back as zero-copy views
//! into a memory mapping. A file is only read by the process that wrote it, so the
//! buffers are native-endian and nothing is versioned.
//!
//! Layout, every piece starting on a 16-byte boundary (enough for `Decimal`):
//!
//!   magic       "BSPILL\x00\x01", padded to 16 bytes
//!   per batch   header: rows u64, column count u64, then per column the `Data`
//!               tag u64 and the byte length of its values buffer u64 (0 unless
//!               bytes); then per column its validity bits (`(rows + 7) / 8`
//!               bytes), its data buffer (`rows` elements, or `rows + 1` offsets
//!               for bytes rebased to 0) and, for bytes, the values buffer
//!
//! The schema is not stored: the reader takes it from the `Run` and each column's
//! `ty` from its field. A column's `dict` is an index over bytes already present,
//! so it is dropped. Every batch is charged to the space at its exact size on disk
//! before any of it is written, and the magic is charged when the file is made,
//! so a run's charges add up to its file size. The mapping is read-only: columns
//! read back must not be written in place.

const std = @import("std");
const types = @import("../lang/types.zig");
const value = @import("value.zig");
const column = @import("column.zig");
const space_mod = @import("space.zig");

const Space = space_mod.Space;
const Batch = @import("batch.zig").Batch;
const Column = column.Column;
const Bitmap = column.Bitmap;
const Tag = std.meta.Tag(Column.Data);

const ALIGN = 16;
const MAGIC = "BSPILL\x00\x01";
const BUF_SIZE = 64 * 1024;

comptime {
    std.debug.assert(ALIGN >= @alignOf(value.Decimal));
}

fn pad(n: u64) u64 {
    return std.mem.alignForward(u64, n, ALIGN);
}

fn elemSize(tag: Tag) u64 {
    return switch (tag) {
        .b => @sizeOf(bool),
        .i32 => @sizeOf(i32),
        .i64 => @sizeOf(i64),
        .f64 => @sizeOf(f64),
        .dec => @sizeOf(value.Decimal),
        .bytes => @sizeOf(i32),
    };
}

fn dataLen(tag: Tag, rows: u64) u64 {
    return elemSize(tag) * (if (tag == .bytes) rows + 1 else rows);
}

fn headerLen(ncols: usize) u64 {
    return 16 + 16 * @as(u64, ncols);
}

/// The byte span of a bytes column's first `rows` rows, which need not start at 0.
fn bytesSpan(b: column.Bytes, rows: usize) struct { lo: usize, hi: usize } {
    if (b.offsets.len == 0) return .{ .lo = 0, .hi = 0 };
    return .{ .lo = @intCast(b.offsets[0]), .hi = @intCast(b.offsets[rows]) };
}

pub const Run = struct { path: []const u8, schema: *const types.Schema, rows: u64, bytes: u64, space: ?Space = null };

/// Deletes a finished run's file and gives its bytes back to the space it was
/// charged to.
pub fn discard(run: Run) void {
    std.fs.cwd().deleteFile(run.path) catch {};
    if (run.space) |s| s.release(run.bytes);
}

pub const Writer = struct {
    rows: u64 = 0,
    bytes: u64 = 0,
    space: Space,
    schema: *const types.Schema,
    file: std.fs.File,
    path: []const u8,
    fw: std.fs.File.Writer,
    state: enum { open, finished, aborted } = .open,

    /// Creates a new file in `space` (named after `tag`) for batches of `schema`.
    pub fn init(space: Space, arena: std.mem.Allocator, schema: *const types.Schema, tag: []const u8) !Writer {
        const buf = try arena.alloc(u8, BUF_SIZE);
        const f = try space.create(arena, tag);
        var self = Writer{
            .space = space,
            .schema = schema,
            .file = f.file,
            .path = f.path,
            .fw = f.file.writer(buf),
        };
        errdefer self.abort();
        try space.charge(ALIGN);
        try self.put(MAGIC);
        try self.zeros(ALIGN - MAGIC.len);
        self.bytes = ALIGN;
        return self;
    }

    fn put(self: *Writer, data: []const u8) !void {
        self.fw.interface.writeAll(data) catch return self.fw.err orelse error.WriteFailed;
    }

    fn zeros(self: *Writer, n: u64) !void {
        self.fw.interface.splatByteAll(0, @intCast(n)) catch return self.fw.err orelse error.WriteFailed;
    }

    fn putU64(self: *Writer, x: u64) !void {
        try self.put(std.mem.asBytes(&x));
    }

    fn putPadded(self: *Writer, data: []const u8) !void {
        try self.put(data);
        try self.zeros(pad(data.len) - data.len);
    }

    fn valuesLen(c: Column, rows: usize) u64 {
        if (c.data != .bytes) return 0;
        const s = bytesSpan(c.data.bytes, rows);
        return s.hi - s.lo;
    }

    fn batchLen(b: Batch) u64 {
        var n = headerLen(b.columns.len);
        for (b.columns) |c| {
            n += pad((b.len + 7) / 8) + pad(dataLen(c.data, b.len)) + pad(valuesLen(c, b.len));
        }
        return n;
    }

    /// Appends one batch (any length, 0 allowed), charging its bytes to the space first.
    pub fn write(self: *Writer, b: Batch) !void {
        std.debug.assert(self.state == .open);
        if (b.columns.len != self.schema.fields.len) return error.SpillSchemaMismatch;
        const total = batchLen(b);
        try self.space.charge(total);
        const rows = b.len;
        try self.putU64(rows);
        try self.putU64(b.columns.len);
        for (b.columns) |c| {
            try self.putU64(@intFromEnum(@as(Tag, c.data)));
            try self.putU64(valuesLen(c, rows));
        }
        for (b.columns) |c| {
            try self.putPadded(c.validity.bits[0 .. (rows + 7) / 8]);
            switch (c.data) {
                .bytes => |by| try self.putBytes(by, rows),
                inline else => |s| try self.putPadded(std.mem.sliceAsBytes(s[0..rows])),
            }
        }
        self.rows += rows;
        self.bytes += total;
    }

    fn putBytes(self: *Writer, by: column.Bytes, rows: usize) !void {
        const s = bytesSpan(by, rows);
        if (by.offsets.len == 0) {
            const zero: i32 = 0;
            try self.putPadded(std.mem.asBytes(&zero));
        } else if (s.lo == 0) {
            try self.putPadded(std.mem.sliceAsBytes(by.offsets[0 .. rows + 1]));
        } else {
            for (by.offsets[0 .. rows + 1]) |o| {
                const r: i32 = o - @as(i32, @intCast(s.lo));
                try self.put(std.mem.asBytes(&r));
            }
            const n = (rows + 1) * @sizeOf(i32);
            try self.zeros(pad(n) - n);
        }
        try self.putPadded(by.values[s.lo..s.hi]);
    }

    /// Flushes and closes the file; the Run describes it for reading back.
    pub fn finish(self: *Writer) !Run {
        std.debug.assert(self.state == .open);
        self.fw.interface.flush() catch return self.fw.err orelse error.WriteFailed;
        self.file.close();
        self.state = .finished;
        return .{ .path = self.path, .schema = self.schema, .rows = self.rows, .bytes = self.bytes, .space = self.space };
    }

    /// Closes and deletes the file without producing a Run (error paths). A no-op
    /// once `finish` has handed the file over or the writer was already aborted.
    pub fn abort(self: *Writer) void {
        if (self.state != .open) return;
        self.file.close();
        std.fs.cwd().deleteFile(self.path) catch {};
        self.space.release(self.bytes);
        self.state = .aborted;
    }
};

pub const Reader = struct {
    map: []align(std.heap.page_size_min) const u8,
    schema: *const types.Schema,
    pos: usize = ALIGN,

    /// Memory-maps the run's file.
    pub fn open(run: Run) !Reader {
        const file = try std.fs.cwd().openFile(run.path, .{});
        defer file.close();
        const size = (try file.stat()).size;
        if (size != run.bytes) return error.CorruptSpill;
        if (size < ALIGN) return error.CorruptSpill;
        const map = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(map);
        if (!std.mem.eql(u8, map[0..MAGIC.len], MAGIC)) return error.CorruptSpill;
        return .{ .map = map, .schema = run.schema };
    }

    fn take(self: *Reader, n: u64) ![]u8 {
        const p = pad(n);
        if (p > self.map.len - self.pos) return error.CorruptSpill;
        const s = @constCast(self.map[self.pos..][0..@intCast(n)]);
        self.pos += @intCast(p);
        return s;
    }

    fn takeU64(self: *Reader) !u64 {
        if (self.map.len - self.pos < 8) return error.CorruptSpill;
        const x = std.mem.bytesToValue(u64, self.map[self.pos..][0..8]);
        self.pos += 8;
        return x;
    }

    fn takeSlice(self: *Reader, comptime T: type, count: u64) ![]T {
        const raw = try self.take(count * @sizeOf(T));
        const ptr: [*]T = @ptrCast(@alignCast(raw.ptr));
        return ptr[0..@intCast(count)];
    }

    /// The batches in the order written. Columns point into the mapping and stay
    /// valid until `close`; `arena` holds only the small per-batch structs.
    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        if (self.pos >= self.map.len) return null;
        const rows = try self.takeU64();
        const ncols = try self.takeU64();
        const fields = self.schema.fields;
        if (ncols != fields.len) return error.CorruptSpill;
        const tags = try arena.alloc(Tag, fields.len);
        const vlens = try arena.alloc(u64, fields.len);
        for (tags, vlens) |*t, *v| {
            t.* = std.meta.intToEnum(Tag, try self.takeU64()) catch return error.CorruptSpill;
            v.* = try self.takeU64();
        }
        const cols = try arena.alloc(Column, fields.len);
        for (cols, fields, tags, vlens) |*c, f, t, vlen| {
            const bits = try self.take((rows + 7) / 8);
            const data: Column.Data = switch (t) {
                .b => .{ .b = try self.takeSlice(bool, rows) },
                .i32 => .{ .i32 = try self.takeSlice(i32, rows) },
                .i64 => .{ .i64 = try self.takeSlice(i64, rows) },
                .f64 => .{ .f64 = try self.takeSlice(f64, rows) },
                .dec => .{ .dec = try self.takeSlice(value.Decimal, rows) },
                .bytes => .{ .bytes = .{
                    .offsets = try self.takeSlice(i32, rows + 1),
                    .values = try self.take(vlen),
                } },
            };
            c.* = .{
                .ty = f.ty,
                .len = @intCast(rows),
                .validity = .{ .bits = bits, .len = @intCast(rows) },
                .data = data,
            };
        }
        return .{ .schema = self.schema, .columns = cols, .len = @intCast(rows) };
    }

    pub fn close(self: *Reader) void {
        if (self.map.len > 0) std.posix.munmap(self.map);
        self.map = self.map[0..0];
    }
};

const testing = std.testing;

const TestDir = struct {
    tmp: testing.TmpDir,
    path: []const u8,
    ds: space_mod.DirSpace,

    fn init(cap: u64) !*TestDir {
        const self = try testing.allocator.create(TestDir);
        errdefer testing.allocator.destroy(self);
        self.tmp = testing.tmpDir(.{ .iterate = true });
        errdefer self.tmp.cleanup();
        self.path = try self.tmp.dir.realpathAlloc(testing.allocator, ".");
        self.ds = .{ .dir = self.path, .cap = cap };
        return self;
    }

    fn deinit(self: *TestDir) void {
        testing.allocator.free(self.path);
        self.tmp.cleanup();
        testing.allocator.destroy(self);
    }

    fn count(self: *TestDir) !usize {
        var it = self.tmp.dir.iterate();
        var n: usize = 0;
        while (try it.next()) |_| n += 1;
        return n;
    }
};

fn buildColumn(a: std.mem.Allocator, ty: types.Type, vals: []const value.Value) !Column {
    var b = column.Builder.init(a, ty);
    for (vals) |v| try b.append(v);
    return b.finish();
}

fn expectSameBatch(want: Batch, got: Batch) !void {
    try testing.expectEqual(want.len, got.len);
    try testing.expectEqual(want.columns.len, got.columns.len);
    for (want.columns, got.columns, want.schema.fields) |w, g, f| {
        try testing.expect(g.ty.eql(f.ty));
        try testing.expectEqual(want.len, g.validity.len);
        try testing.expectEqual((want.len + 7) / 8, g.validity.bits.len);
        try testing.expect(g.dict == null);
        for (0..want.len) |i| try testing.expectEqualDeep(w.getValue(i), g.getValue(i));
    }
}

const all_schema = types.Schema{ .fields = &.{
    .{ .name = "b", .ty = types.Type.init(.bool).asNullable() },
    .{ .name = "i", .ty = types.Type.init(.int).asNullable() },
    .{ .name = "f", .ty = types.Type.init(.float).asNullable() },
    .{ .name = "d", .ty = types.Type.decimal(38, 4).asNullable() },
    .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
    .{ .name = "y", .ty = types.Type.init(.bytes).asNullable() },
    .{ .name = "dt", .ty = types.Type.init(.date).asNullable() },
    .{ .name = "t", .ty = types.Type.init(.time).asNullable() },
    .{ .name = "ts", .ty = types.Type.init(.timestamp).asNullable() },
    .{ .name = "n", .ty = types.Type.init(.int).asNullable() },
} };

/// A batch of every column kind, `rows` long, with nulls where `(i + seed) % 3 == 0`
/// and an all-null last column; decimals vary in scale and pass beyond i64.
fn allKindsBatch(a: std.mem.Allocator, rows: usize, seed: usize) !Batch {
    const fields = all_schema.fields;
    var builders: [fields.len]column.Builder = undefined;
    for (&builders, fields) |*b, f| b.* = column.Builder.init(a, f.ty);
    const scales = [_]u8{ 0, 2, 4, 9, 18 };
    for (0..rows) |r| {
        const k = r + seed;
        const ki: i64 = @intCast(k);
        const nul = k % 3 == 0;
        const big: i128 = @as(i128, std.math.maxInt(i64)) * 1000 + @as(i128, ki);
        const unscaled: i128 = if (k % 2 == 0) -big else ki * 7 - 50;
        const str = try std.fmt.allocPrint(a, "row-{d}", .{k});
        const raw = try a.dupe(u8, &.{ 0, @truncate(k), 0xFF });
        const row = [fields.len]value.Value{
            if (nul) .null else .{ .bool = k % 2 == 1 },
            if (nul) .null else .{ .int = ki * -1_000_003 },
            if (nul) .null else .{ .float = @as(f64, @floatFromInt(ki)) / 3.0 },
            if (nul) .null else .{ .decimal = .{ .unscaled = unscaled, .scale = scales[k % scales.len] } },
            if (nul) .null else .{ .string = if (k % 4 == 1) "" else str },
            if (nul) .null else .{ .bytes = if (k % 5 == 2) "" else raw },
            if (nul) .null else .{ .date = @as(i32, @intCast(k)) - 20000 },
            if (nul) .null else .{ .time = ki * 1_000_000 },
            if (nul) .null else .{ .timestamp = ki * 86_400_000_000 - 5 },
            .null,
        };
        for (&builders, row) |*b, v| try b.append(v);
    }
    const cols = try a.alloc(Column, fields.len);
    for (cols, &builders) |*c, *b| c.* = try b.finish();
    return .{ .schema = &all_schema, .columns = cols, .len = rows };
}

test "spill round-trips every column kind across batches of different lengths" {
    var td = try TestDir.init(std.math.maxInt(u64));
    defer td.deinit();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const lens = [_]usize{ 0, 1, 7, 8, 9, 13, 0, 64, 100 };
    var batches: [lens.len]Batch = undefined;
    for (&batches, lens, 0..) |*b, n, i| b.* = try allKindsBatch(a, n, i * 5);

    var w = try Writer.init(td.ds.space(), a, &all_schema, "all");
    errdefer w.abort();
    for (batches) |b| try w.write(b);
    try testing.expectEqual(@as(u64, 202), w.rows);
    const run = try w.finish();
    try testing.expectEqual(@as(u64, 202), run.rows);
    try testing.expectEqual(run.bytes, td.ds.used.load(.monotonic));
    try testing.expectEqual(run.bytes, (try std.fs.cwd().statFile(run.path)).size);

    var r = try Reader.open(run);
    defer r.close();
    var i: usize = 0;
    while (try r.next(a)) |got| : (i += 1) {
        try testing.expect(got.schema == &all_schema);
        try expectSameBatch(batches[i], got);
    }
    try testing.expectEqual(lens.len, i);
    try testing.expect((try r.next(a)) == null);
}

test "spill drops a dictionary and keeps a string column sliced off a larger buffer" {
    var td = try TestDir.init(1 << 20);
    defer td.deinit();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "s", .ty = types.Type.init(.string).asNullable() },
        .{ .name = "t", .ty = types.Type.init(.string) },
    } };
    const entries = [_][]const u8{ "red", "", "blue" };
    const codes = [_]u32{ 2, 0, 1, 2, 2, 0, 1, 0, 2, 1 };
    var db = column.Builder.init(a, schema.fields[0].ty);
    for (codes) |c| try db.appendStr(entries[c]);
    try db.noteDict(0, &entries, &codes, null, 0);
    const dcol = try db.finish();
    try testing.expect(dcol.dict != null);

    const whole = try buildColumn(a, schema.fields[1].ty, &.{
        .{ .string = "skip" },  .{ .string = "me" }, .{ .string = "alpha" }, .{ .string = "" },
        .{ .string = "gamma" }, .{ .string = "d" },  .{ .string = "e" },     .{ .string = "ff" },
        .{ .string = "g" },     .{ .string = "h" },  .{ .string = "i" },     .{ .string = "j" },
    });
    var sliced = whole;
    sliced.len = codes.len;
    sliced.data.bytes.offsets = whole.data.bytes.offsets[2..];
    sliced.validity = try Bitmap.initFull(a, codes.len);

    var cols = [_]Column{ dcol, sliced };
    const b = Batch{ .schema = &schema, .columns = &cols, .len = codes.len };

    var w = try Writer.init(td.ds.space(), a, &schema, "dict");
    errdefer w.abort();
    try w.write(b);
    const run = try w.finish();

    var r = try Reader.open(run);
    defer r.close();
    const got = (try r.next(a)).?;
    try expectSameBatch(b, got);
    try testing.expectEqual(@as(i32, 0), got.columns[1].data.bytes.offsets[0]);
    try testing.expectEqualStrings("alpha", got.columns[1].data.bytes.at(0));
    try testing.expect((try r.next(a)) == null);
}

test "spill of no batches reads back empty" {
    var td = try TestDir.init(1 << 20);
    defer td.deinit();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var w = try Writer.init(td.ds.space(), a, &all_schema, "empty");
    const run = try w.finish();
    try testing.expectEqual(@as(u64, 0), run.rows);
    var r = try Reader.open(run);
    defer r.close();
    try testing.expect((try r.next(a)) == null);
}

test "spill write past the cap fails with SpillCapExceeded" {
    var td = try TestDir.init(256);
    defer td.deinit();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var w = try Writer.init(td.ds.space(), a, &all_schema, "cap");
    defer w.abort();
    const b = try allKindsBatch(a, 50, 0);
    try testing.expectError(error.SpillCapExceeded, w.write(b));
}

test "spill abort closes and deletes the file" {
    var td = try TestDir.init(1 << 20);
    defer td.deinit();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var w = try Writer.init(td.ds.space(), a, &all_schema, "gone");
    try w.write(try allKindsBatch(a, 10, 1));
    try testing.expectEqual(@as(usize, 1), try td.count());
    w.abort();
    try testing.expectEqual(@as(usize, 0), try td.count());
    try testing.expectError(error.FileNotFound, std.fs.cwd().access(w.path, .{}));
    w.abort();
}

test "spill reader rejects a file without the magic" {
    var td = try TestDir.init(1 << 20);
    defer td.deinit();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const f = try td.ds.space().create(a, "bad");
    try f.file.writeAll("not a spill file");
    f.file.close();
    try testing.expectError(error.CorruptSpill, Reader.open(.{ .path = f.path, .schema = &all_schema, .rows = 0, .bytes = 16 }));
}
