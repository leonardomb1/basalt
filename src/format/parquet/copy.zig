//! Parquet to Parquet without decoding: the column chunks a query keeps are copied
//! byte for byte and the footer is rewritten around them. It applies only where the
//! re-encoding writer would write the same column (`write.writesAsIs`): its physical
//! type, repetition and meaning. The pages keep the source's encodings and codec.
//! The chunks are fetched as a decode fetches them (`Reader.chunks`), and each is
//! walked header by header (`footer.pageAt`) until its data pages hold its value
//! count: that is the structural check, and the extent copied, as writers disagree
//! on whether `total_compressed_size` counts headers. A page that does not frame
//! fails the copy; a page's payload is never decompressed.
//!
//! The footer is transcoded field by field from the source's own bytes, so what
//! basalt does not model (encoding stats, size statistics, fields of later spec
//! versions) carries over. Dropped columns lose their schema elements, chunks and
//! column orders, and kept ones take their new names. Each chunk's page offsets
//! move by where it now starts. Its page index and bloom filter offsets are
//! dropped, as those bytes are not copied. A row group's sizes are summed over its
//! kept chunks, and its sorting columns renumbered, cut at the first dropped one.
//! `ARROW:schema` names the source's columns, so it is kept only when every column
//! is, in order, under its own name. `created_by` stays the source's, as readers
//! judge a file's statistics and sizes by the writer that made them. An encrypted
//! file, a chunk kept in another file, an empty chunk, or a nested or repeated
//! column is not copied.

const std = @import("std");
const chunkEnd = @import("folder.zig").chunkEnd;
const parquet = @import("footer.zig");
const thrift = @import("thrift.zig");
const Leaf = @import("read.zig").Leaf;
const Reader = @import("read.zig").Reader;
const writesAsIs = @import("write.zig").writesAsIs;

const List = std.array_list.Managed;

pub const Error = error{ CorruptParquetChunk, NotCopyable } || thrift.Error;

/// One output column: the reader's top-level column `field`, written as `to`.
pub const Col = struct { field: usize, to: []const u8 };

pub const Plan = struct {
    /// The distinct leaves the columns read, fetched once a row group.
    fetch: []const Leaf,
    /// Per output column, its leaf's place in `fetch`.
    slot: []const usize,
    names: []const []const u8,
    identity: bool,
};

/// Where a chunk was and is: `len` bytes from `from` in the source, at `to` in the
/// copy.
const Moved = struct { from: i64, len: i64, to: i64 };

/// The copy of `cols` out of `rd`, or null where one of them cannot be copied as it
/// stands.
pub fn plan(arena: std.mem.Allocator, rd: *const Reader, cols: []const Col) !?Plan {
    if (cols.len == 0) return null;
    const md = rd.md;
    var fetch = List(Leaf).init(arena);
    const slot = try arena.alloc(usize, cols.len);
    const names = try arena.alloc([]const u8, cols.len);
    var identity = cols.len == rd.outputs.len;
    for (cols, slot, names, 0..) |c, *s, *n, i| {
        if (c.field >= rd.outputs.len) return null;
        const lf = switch (rd.outputs[c.field]) {
            .leaf => |k| rd.leaves[k],
            .nested => return null,
        };
        if (lf.isRepeated() or !writesAsIs(md.schema[lf.schema_idx])) return null;
        s.* = for (fetch.items, 0..) |f, k| {
            if (f.schema_idx == lf.schema_idx) break k;
        } else blk: {
            try fetch.append(lf);
            break :blk fetch.items.len - 1;
        };
        n.* = c.to;
        identity = identity and c.field == i and std.mem.eql(u8, c.to, rd.schema.fields[i].name);
    }
    for (rd.outputs) |o| identity = identity and o == .leaf;

    const moved = try arena.alloc([]Moved, md.row_groups.len);
    for (md.row_groups, moved) |g, *mv| {
        mv.* = try arena.alloc(Moved, cols.len);
        for (slot, mv.*) |k, *m| {
            const lf = fetch.items[k];
            if (lf.chunk_idx >= g.columns.len) return null;
            const ch = g.columns[lf.chunk_idx];
            const meta = ch.meta orelse return null;
            if (ch.file_path.len > 0 or meta.num_values <= 0) return null;
            const start = std.math.cast(u64, meta.startOffset()) orelse return null;
            const end = chunkEnd(rd.boundaries, start);
            if (end <= start) return null;
            m.* = .{ .from = @intCast(start), .len = @intCast(end - start), .to = @intCast(start) };
        }
    }
    const p = Plan{ .fetch = fetch.items, .slot = slot, .names = names, .identity = identity };
    _ = transcode(arena, md, p, moved) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    return p;
}

/// Writes the planned chunks to `w`, then the footer; returns the bytes written.
pub fn copy(arena: std.mem.Allocator, scratch: *std.heap.ArenaAllocator, rd: *Reader, p: Plan, w: *std.Io.Writer) !u64 {
    const moved = try arena.alloc([]Moved, rd.md.row_groups.len);
    try w.writeAll(parquet.magic);
    var off: u64 = parquet.magic.len;
    for (rd.md.row_groups, moved) |g, *mv| {
        defer _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        const chunks = try rd.chunks(sa, g, p.fetch);
        const lens = try sa.alloc(usize, chunks.len);
        for (chunks, lens) |c, *n| n.* = try pageSpan(c.bytes, c.meta.num_values);
        mv.* = try arena.alloc(Moved, p.slot.len);
        for (p.slot, mv.*) |k, *m| {
            try w.writeAll(chunks[k].bytes[0..lens[k]]);
            m.* = .{ .from = @intCast(chunks[k].start), .len = @intCast(lens[k]), .to = @intCast(off) };
            off += lens[k];
        }
    }
    const footer = try transcode(arena, rd.md, p, moved);
    try w.writeAll(footer);
    try w.writeInt(u32, @intCast(footer.len), .little);
    try w.writeAll(parquet.magic);
    return off + footer.len + parquet.trailer_len;
}

/// How far a chunk's pages run in `bytes`: page after page from its start until its
/// data pages hold `num_values`. A page that does not frame within `bytes` before
/// then is a corrupt chunk; what follows the last page (a bloom filter, a page
/// index) is not the chunk's.
pub fn pageSpan(bytes: []const u8, num_values: i64) Error!usize {
    var at: usize = 0;
    var values: i64 = 0;
    while (values < num_values) {
        const pg = parquet.pageAt(bytes, at) catch return Error.CorruptParquetChunk;
        if (pg.header.num_values < 0) return Error.CorruptParquetChunk;
        switch (pg.header.ty) {
            .data_page, .data_page_v2 => values += pg.header.num_values,
            else => {},
        }
        at = pg.next;
    }
    if (values != num_values) return Error.CorruptParquetChunk;
    return at;
}

/// The source footer with the plan applied, each kept chunk moved as `moved` says.
fn transcode(arena: std.mem.Allocator, md: parquet.FileMetaData, p: Plan, moved: []const []const Moved) ![]u8 {
    var out = List(u8).init(arena);
    var w = thrift.Writer.init(&out);
    var r = thrift.Reader.init(md.raw);
    try r.structBegin();
    try w.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        if ((f.id == 2 or f.id == 4 or f.id == 5 or f.id == 7) and f.ty != .list) return Error.NotCopyable;
        switch (f.id) {
            2 => try schemaList(arena, &r, &w, md, p),
            4 => try rowGroups(arena, &r, &w, md, p, moved),
            5 => if (p.identity) try copyField(&r, &w, f) else try keyValues(arena, &r, &w),
            7 => try columnOrders(arena, &r, &w, md, p),
            8, 9 => return Error.NotCopyable,
            else => try copyField(&r, &w, f),
        }
    }
    try r.structEnd();
    try w.structEnd();
    return out.items;
}

/// A field as it stands: its header rewritten in the output's numbering, its value's
/// bytes copied.
fn copyField(r: *thrift.Reader, w: *thrift.Writer, f: thrift.Field) !void {
    const at = r.c.i;
    try r.skip(f.ty);
    try w.fieldBegin(f.ty, f.id);
    try w.out.appendSlice(r.c.p[at..r.c.i]);
}

/// The bytes of each struct of the list whose header comes next.
fn structSpans(arena: std.mem.Allocator, r: *thrift.Reader) ![]const []const u8 {
    const h = try r.readListHeader();
    if (h.elem != .@"struct") return Error.NotCopyable;
    const spans = try arena.alloc([]const u8, h.size);
    for (spans) |*s| {
        const at = r.c.i;
        try r.skipStruct();
        s.* = r.c.p[at..r.c.i];
    }
    return spans;
}

fn readI64(r: *thrift.Reader, f: thrift.Field) !i64 {
    if (f.ty != .i64) return Error.NotCopyable;
    return r.readZigZag();
}

/// The root, then each output column's element under its new name.
fn schemaList(arena: std.mem.Allocator, r: *thrift.Reader, w: *thrift.Writer, md: parquet.FileMetaData, p: Plan) !void {
    const spans = try structSpans(arena, r);
    if (spans.len != md.schema.len) return Error.NotCopyable;
    try w.listBegin(2, .@"struct", 1 + p.slot.len);
    try schemaElement(w, spans[0], null, @intCast(p.slot.len));
    for (p.slot, p.names) |k, name| try schemaElement(w, spans[p.fetch[k].schema_idx], name, null);
}

/// A SchemaElement with its name (field 4) and child count (field 5) replaced where
/// given.
fn schemaElement(w: *thrift.Writer, bytes: []const u8, name: ?[]const u8, children: ?i32) !void {
    var r = thrift.Reader.init(bytes);
    try r.structBegin();
    try w.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        if (f.id == 4 and name != null) {
            try r.skip(f.ty);
            try w.writeBinary(4, name.?);
        } else if (f.id == 5 and children != null) {
            try r.skip(f.ty);
            try w.writeI32(5, children.?);
        } else try copyField(&r, w, f);
    }
    try r.structEnd();
    try w.structEnd();
}

fn rowGroups(arena: std.mem.Allocator, r: *thrift.Reader, w: *thrift.Writer, md: parquet.FileMetaData, p: Plan, moved: []const []const Moved) !void {
    const spans = try structSpans(arena, r);
    if (spans.len != md.row_groups.len) return Error.NotCopyable;
    try w.listBegin(4, .@"struct", spans.len);
    for (spans, md.row_groups, moved) |s, g, mv| try rowGroup(arena, w, s, g, p, mv);
}

/// A RowGroup: its kept chunks (field 1) where they moved, its uncompressed and
/// compressed sizes (fields 2 and 6) summed over them, its first page's offset
/// (field 5) the first kept chunk's, and its sorting columns (field 4) renumbered.
fn rowGroup(arena: std.mem.Allocator, w: *thrift.Writer, bytes: []const u8, g: parquet.RowGroup, p: Plan, moved: []const Moved) !void {
    var uncompressed: i64 = 0;
    var compressed: i64 = 0;
    var first: i64 = std.math.maxInt(i64);
    for (p.slot, moved) |k, m| {
        const meta = g.columns[p.fetch[k].chunk_idx].meta.?;
        uncompressed += meta.total_uncompressed_size;
        compressed += meta.total_compressed_size;
        first = @min(first, m.to);
    }
    var r = thrift.Reader.init(bytes);
    try r.structBegin();
    try w.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        if ((f.id == 1 or f.id == 4) and f.ty != .list) return Error.NotCopyable;
        switch (f.id) {
            1 => {
                const spans = try structSpans(arena, &r);
                if (spans.len != g.columns.len) return Error.NotCopyable;
                try w.listBegin(1, .@"struct", p.slot.len);
                for (p.slot, p.names, moved) |k, name, m| try columnChunk(w, spans[p.fetch[k].chunk_idx], name, m);
            },
            2 => {
                _ = try readI64(&r, f);
                try w.writeI64(2, uncompressed);
            },
            4 => try sortingColumns(arena, &r, w, p),
            5 => {
                _ = try readI64(&r, f);
                try w.writeI64(5, first);
            },
            6 => {
                _ = try readI64(&r, f);
                try w.writeI64(6, compressed);
            },
            else => try copyField(&r, w, f),
        }
    }
    try r.structEnd();
    try w.structEnd();
}

/// A ColumnChunk moved as `m` says: its deprecated offset (field 2) and its
/// metadata's page offsets moved with it, its page index (fields 4 to 7) dropped. A
/// chunk in another file (field 1) or encrypted (fields 8 and 9) is not copied.
fn columnChunk(w: *thrift.Writer, bytes: []const u8, name: []const u8, m: Moved) !void {
    var r = thrift.Reader.init(bytes);
    try r.structBegin();
    try w.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            1, 8, 9 => return Error.NotCopyable,
            2 => {
                const v = try readI64(&r, f);
                try w.writeI64(2, if (v == 0) 0 else m.to);
            },
            3 => {
                if (f.ty != .@"struct") return Error.NotCopyable;
                try w.fieldBegin(.@"struct", 3);
                try columnMeta(&r, w, name, m);
            },
            4, 5, 6, 7 => try r.skip(f.ty),
            else => try copyField(&r, w, f),
        }
    }
    try r.structEnd();
    try w.structEnd();
}

/// ColumnMetaData under its new path (field 3), its data, index and dictionary page
/// offsets (fields 9 to 11) moved with the chunk; an absent dictionary written as 0
/// stays 0. Its bloom filter (fields 14 and 15) is dropped.
fn columnMeta(r: *thrift.Reader, w: *thrift.Writer, name: []const u8, m: Moved) !void {
    try r.structBegin();
    try w.structBegin();
    while (true) {
        const f = try r.readField();
        if (f.ty == .stop) break;
        switch (f.id) {
            3 => {
                try r.skip(f.ty);
                try w.listBegin(3, .binary, 1);
                try w.writeVarint(name.len);
                try w.out.appendSlice(name);
            },
            9, 10, 11 => {
                const v = try readI64(r, f);
                if (v >= m.from and v < m.from + m.len) {
                    try w.writeI64(f.id, v - m.from + m.to);
                } else if (v == 0 and f.id == 11) {
                    try w.writeI64(11, 0);
                } else return Error.NotCopyable;
            },
            14, 15 => try r.skip(f.ty),
            else => try copyField(r, w, f),
        }
    }
    try r.structEnd();
    try w.structEnd();
}

/// The row group's sort order over the output columns: each SortingColumn's leaf
/// (field 1) renumbered to its column, the list cut at the first leaf not kept, as a
/// prefix of a sort order is one too; none kept drops the field.
fn sortingColumns(arena: std.mem.Allocator, r: *thrift.Reader, w: *thrift.Writer, p: Plan) !void {
    const spans = try structSpans(arena, r);
    const at = try arena.alloc(usize, spans.len);
    var kept: usize = 0;
    each: for (spans, at) |s, *a| {
        var sr = thrift.Reader.init(s);
        try sr.structBegin();
        var idx: ?i64 = null;
        while (true) {
            const f = try sr.readField();
            if (f.ty == .stop) break;
            if (f.id == 1 and f.ty == .i32) idx = try sr.readZigZag() else try sr.skip(f.ty);
        }
        const leaf = std.math.cast(usize, idx orelse break :each) orelse break :each;
        a.* = for (p.slot, 0..) |k, i| {
            if (p.fetch[k].chunk_idx == leaf) break i;
        } else break :each;
        kept += 1;
    }
    if (kept == 0) return;
    try w.listBegin(4, .@"struct", kept);
    for (spans[0..kept], at[0..kept]) |s, a| {
        var sr = thrift.Reader.init(s);
        try sr.structBegin();
        try w.structBegin();
        while (true) {
            const f = try sr.readField();
            if (f.ty == .stop) break;
            if (f.id == 1) {
                try sr.skip(f.ty);
                try w.writeI32(1, @intCast(a));
            } else try copyField(&sr, w, f);
        }
        try sr.structEnd();
        try w.structEnd();
    }
}

/// The file's key-value metadata without `ARROW:schema`, which names the source's
/// columns; none left drops the field.
fn keyValues(arena: std.mem.Allocator, r: *thrift.Reader, w: *thrift.Writer) !void {
    const spans = try structSpans(arena, r);
    var keep = List([]const u8).init(arena);
    for (spans) |s| {
        var sr = thrift.Reader.init(s);
        try sr.structBegin();
        const k = try sr.readField();
        const arrow = k.id == 1 and k.ty == .binary and std.mem.eql(u8, try sr.readBinary(), "ARROW:schema");
        if (!arrow) try keep.append(s);
    }
    if (keep.items.len == 0) return;
    try w.listBegin(5, .@"struct", keep.items.len);
    for (keep.items) |s| try w.out.appendSlice(s);
}

/// One ColumnOrder per output column.
fn columnOrders(arena: std.mem.Allocator, r: *thrift.Reader, w: *thrift.Writer, md: parquet.FileMetaData, p: Plan) !void {
    const spans = try structSpans(arena, r);
    if (spans.len != md.leafCount()) return Error.NotCopyable;
    try w.listBegin(7, .@"struct", p.slot.len);
    for (p.slot) |k| try w.out.appendSlice(spans[p.fetch[k].chunk_idx]);
}

const testing = std.testing;
const column = @import("../../exec/column.zig");
const readAllText = @import("testing_util.zig").readAllText;
const types = @import("../../lang/types.zig");
const Writer = @import("write.zig").Writer;

/// A file of `groups` row groups of `rows` rows each, written by basalt's writer:
/// a nullable int, a NOT NULL string, a float and a date.
fn writeFixture(a: std.mem.Allocator, path: []const u8, rows: usize, groups: usize) !void {
    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "name", .ty = types.Type.init(.string) },
        .{ .name = "amt", .ty = types.Type.init(.float).asNullable() },
        .{ .name = "day", .ty = types.Type.init(.date).asNullable() },
    } };
    var w = try Writer.open(a, path, schema, .snappy, .truncate);
    w.group_rows = rows;
    var ids = column.Builder.init(a, schema.fields[0].ty);
    var names = column.Builder.init(a, schema.fields[1].ty);
    var amts = column.Builder.init(a, schema.fields[2].ty);
    var days = column.Builder.init(a, schema.fields[3].ty);
    for (0..rows * groups) |i| {
        try ids.append(if (i % 4 == 3) .null else .{ .int = @intCast(i) });
        try names.append(.{ .string = "abcde"[i % 5 ..][0..1] });
        try amts.append(.{ .float = @as(f64, @floatFromInt(i)) / 2 });
        try days.append(.{ .date = @intCast(19000 + i) });
    }
    var cols = [_]column.Column{ try ids.finish(), try names.finish(), try amts.finish(), try days.finish() };
    try w.writeBatch(a, .{ .schema = &schema, .columns = &cols, .len = rows * groups });
    try w.close();
}

fn copied(a: std.mem.Allocator, src: []const u8, cols: []const Col) !?[]const u8 {
    const rd = try Reader.open(a, src);
    defer rd.close();
    const p = (try plan(a, rd, cols)) orelse return null;
    var out = std.Io.Writer.Allocating.init(a);
    var scratch = std.heap.ArenaAllocator.init(testing.allocator);
    defer scratch.deinit();
    const n = try copy(a, &scratch, rd, p, &out.writer);
    try testing.expectEqual(@as(u64, out.written().len), n);
    return out.written();
}

test "a chunk copy keeps, drops, renames and repeats columns in the SELECT's order, read back as the source's values" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const src = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "src.parquet" });
    try writeFixture(a, src, 3, 2);

    const bytes = (try copied(a, src, &.{ .{ .field = 3, .to = "d" }, .{ .field = 1, .to = "name" }, .{ .field = 3, .to = "again" } })).?;
    try testing.expectEqualStrings(
        \\d=2022-01-08 name=a again=2022-01-08
        \\d=2022-01-09 name=b again=2022-01-09
        \\d=2022-01-10 name=c again=2022-01-10
        \\d=2022-01-11 name=d again=2022-01-11
        \\d=2022-01-12 name=e again=2022-01-12
        \\d=2022-01-13 name=a again=2022-01-13
        \\
    , try readAllText(a, bytes, "out.parquet"));

    const md = try parquet.parseFile(a, bytes);
    try testing.expectEqual(@as(usize, 2), md.row_groups.len);
    try testing.expectEqual(@as(usize, 4), md.schema.len);
    try testing.expectEqual(parquet.Repetition.required, md.schema[2].repetition.?);
    for (md.row_groups) |g| for (g.columns, [_][]const u8{ "d", "name", "again" }) |c, name| {
        try testing.expectEqualStrings(name, c.meta.?.path_in_schema[0]);
        _ = try pageSpan(bytes[@intCast(c.meta.?.startOffset())..], c.meta.?.num_values);
    };
}

test "a chunk copy of every column under its own name rewrites a file basalt wrote byte for byte" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const src = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "src.parquet" });
    try writeFixture(a, src, 4, 3);
    const bytes = (try copied(a, src, &.{ .{ .field = 0, .to = "id" }, .{ .field = 1, .to = "name" }, .{ .field = 2, .to = "amt" }, .{ .field = 3, .to = "day" } })).?;
    try testing.expectEqualSlices(u8, try std.fs.cwd().readFileAlloc(a, src, 1 << 20), bytes);
}

test "pageSpan frames a chunk's pages up to its value count, ignores what follows, and refuses a cut or a short count" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const src = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "src.parquet" });
    try writeFixture(a, src, 50, 1);
    const file = try std.fs.cwd().readFileAlloc(a, src, 1 << 20);
    const md = try parquet.parseFile(a, file);
    const m0 = md.row_groups[0].columns[0].meta.?;
    const m1 = md.row_groups[0].columns[1].meta.?;
    const chunk = file[@intCast(m0.startOffset())..@intCast(m1.startOffset())];
    try testing.expectEqual(chunk.len, try pageSpan(chunk, m0.num_values));
    try testing.expectEqual(chunk.len, try pageSpan(try std.mem.concat(a, u8, &.{ chunk, "trailing bloom filter bytes" }), m0.num_values));
    try testing.expectError(Error.CorruptParquetChunk, pageSpan(chunk[0 .. chunk.len - 1], m0.num_values));
    try testing.expectError(Error.CorruptParquetChunk, pageSpan(chunk, m0.num_values + 1));
}

test "a nested, unknown or out-of-range column is not planned for a copy" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const src = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "src.parquet" });
    try writeFixture(a, src, 3, 1);
    try testing.expect((try copied(a, src, &.{.{ .field = 9, .to = "x" }})) == null);
    try testing.expect((try copied(a, src, &.{})) == null);
}
