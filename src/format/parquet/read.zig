//! Parquet value decoding and the file reader: levels, encodings, page-to-column
//! assembly, row-group pruning, and the byte sources a file is read from.
//!
//! A decompressed page is still encoded. This module turns those bytes into
//! `Value`s, or straight into a column's typed store on the hot paths, and builds
//! a basalt `Column` from them. Encodings handled: PLAIN, the RLE/bit-packed
//! hybrid used for levels and dictionary indices (`PLAIN_DICTIONARY` and
//! `RLE_DICTIONARY` share a wire format), DELTA_BINARY_PACKED,
//! DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY and BYTE_STREAM_SPLIT, in data pages
//! v1 and v2. Every length, count, width and offset comes off the wire and is
//! attacker-controlled, so each is checked before use as a length, shift or cast:
//! a hostile file yields `CorruptParquetPage`, never a trap or a wrong value.
//!
//! Types come from the LogicalType when the writer gave one, else the legacy
//! ConvertedType. TIME and TIMESTAMP are stored in millis, micros or nanos and
//! always read as micros (`TemporalScale`): ignoring the unit once read millisecond
//! timestamps as 1970. Nanos floor-divide, which is monotone, so statistics-based
//! pruning stays sound after conversion.
//!
//! A struct's fields read as flat dotted columns (`addr.city`). A list of scalars,
//! any depth, reads as one `string` column of JSON arrays named for the list,
//! assembled from repetition and definition levels (`ListShape`), which
//! `JSON_EACH` and `json_get` take apart. A list of structs, a map, and any
//! nesting of them span several leaves and are rebuilt row by row from all of
//! their entries into one JSON column (`NNode`, `Assembler`): objects for structs
//! and maps, arrays for lists. Within a row every leaf's entries are contiguous (a
//! repetition level of 0 starts the next row); a repeated node at repetition
//! level R starts an element at every entry whose level is R, and a definition
//! level below a node's own says it is absent — null if optional, an empty list if
//! repeated. Every leaf has an entry for every instance of every ancestor, so the
//! first entry of a node's first leaf answers whether the node is there. Chunks
//! appear in leaf order including leaves a read skips, hence `Leaf.chunk_idx`.
//!
//! Row-group pruning (`groupMayMatch`, `groupBeatsThreshold`, `fileMinMax`) is
//! conservative in one direction only: a group is skipped solely when statistics
//! prove no row can match; anything missing, unknown or unorderable keeps it.
//!
//! `Reader` reads one batch per row group, fetching only the footer and the
//! column chunks a query projects, so resident memory tracks the widest row
//! group's projected columns rather than the file. `Bytes` is where those bytes
//! come from: a local file by pread, a resident buffer, an SFTP or SMB file by
//! offset, or a `Remote` over HTTP range requests (`http(s)://`, or an `az://`
//! blob whose Shared Key signature is redone per range). A server that answers a
//! ranged GET with 200 has sent the whole body; `Remote` keeps it in `whole`,
//! copied into the reader's arena because the batch arena passed to `read` is
//! recycled per batch, and serves later reads from it. `Folder` reads a folder
//! of files as one table; every file must match the first's projected columns in
//! name, order and type, or the read fails naming it rather than misplace values.
//!
//! The reader's parts sit beside it: `encoding.zig` (page value encodings),
//! `logical.zig` (types), `schema.zig` (the leaf columns), `chunk.zig` (a column
//! chunk's pages), `nested.zig` (lists and structs), `stats.zig` (pruning),
//! `remote.zig` (where the bytes come from) and `folder.zig` (many files as one).
//! `write.zig` is the writer, `footer.zig` the file metadata, `thrift.zig` its codec.

const std = @import("std");
const parquet = @import("footer.zig");
const types = @import("../../lang/types.zig");
const column = @import("../../exec/column.zig");
pub const Threshold = @import("../../exec/value.zig").Threshold;
const Value = @import("../../exec/value.zig").Value;
const Decimal = @import("../../exec/value.zig").Decimal;
const eval = @import("../../exec/eval.zig");
const fuzzKernels = @import("testing_util.zig").fuzzKernels;
const fuzzKernels_corpus = @import("testing_util.zig").fuzzKernels_corpus;
const fx_v2 = @import("testing_util.zig").fx_v2;
const readAllText = @import("testing_util.zig").readAllText;

pub const Error = error{
    CorruptParquetPage,
    UnsupportedParquetSchema,
    UnsupportedParquetEncoding,
} || std.mem.Allocator.Error;

pub const BitReader = @import("encoding.zig").BitReader;
pub const bitWidth = @import("encoding.zig").bitWidth;
pub const decodeRleHybrid = @import("encoding.zig").decodeRleHybrid;
pub const PlainCursor = @import("encoding.zig").PlainCursor;
pub const int96ToMicros = @import("encoding.zig").int96ToMicros;
pub const decodeDeltaBinaryPacked = @import("encoding.zig").decodeDeltaBinaryPacked;
pub const decodeDeltaLengthByteArray = @import("encoding.zig").decodeDeltaLengthByteArray;
pub const decodeDeltaByteArray = @import("encoding.zig").decodeDeltaByteArray;
pub const decodeByteStreamSplit = @import("encoding.zig").decodeByteStreamSplit;
pub const TemporalScale = @import("logical.zig").TemporalScale;
pub const temporalScale = @import("logical.zig").temporalScale;
pub const basaltType = @import("logical.zig").basaltType;
pub const coerce = @import("logical.zig").coerce;
pub const Leaf = @import("schema.zig").Leaf;
pub const ListShape = @import("schema.zig").ListShape;
pub const RootRef = @import("schema.zig").RootRef;
pub const collectLeaves = @import("schema.zig").collectLeaves;
pub const readColumnChunk = @import("chunk.zig").readColumnChunk;
pub const readColumnChunkLevels = @import("chunk.zig").readColumnChunkLevels;
pub const Entries = @import("chunk.zig").Entries;
pub const readEntries = @import("chunk.zig").readEntries;
pub const NNode = @import("nested.zig").NNode;
pub const Nested = @import("nested.zig").Nested;
pub const buildNested = @import("nested.zig").buildNested;
pub const assembleNested = @import("nested.zig").assembleNested;
const assembleLists = @import("nested.zig").assembleLists;
pub const Bound = @import("stats.zig").Bound;
pub const groupMayMatch = @import("stats.zig").groupMayMatch;
pub const groupBeatsThreshold = @import("stats.zig").groupBeatsThreshold;
pub const MinMax = @import("stats.zig").MinMax;
pub const fileMinMax = @import("stats.zig").fileMinMax;
const wanted = @import("stats.zig").wanted;
pub const leafType = @import("stats.zig").leafType;
const cmp = @import("stats.zig").cmp;
pub const Bytes = @import("remote.zig").Bytes;
pub const Remote = @import("remote.zig").Remote;
pub const Folder = @import("folder.zig").Folder;
const source_vtable = @import("folder.zig").source_vtable;
const chunkEnd = @import("folder.zig").chunkEnd;
const chunkBoundaries = @import("folder.zig").chunkBoundaries;
const parseFooterOf = @import("folder.zig").parseFooterOf;
pub const isRemote = @import("folder.zig").isRemote;

const driver = @import("../../connect/driver.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const http_client = @import("../../net/http_client.zig");
const objstore = @import("../../store/objstore.zig");
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");

pub const Output = union(enum) {
    leaf: usize,
    nested: *const Nested,
};

pub const Reader = struct {
    arena: std.mem.Allocator,
    src: Bytes,
    md: parquet.FileMetaData,
    schema: types.Schema,
    leaves: []const Leaf,
    outputs: []const Output = &.{},
    boundaries: []const u64 = &.{},
    bounds: []const Bound = &.{},
    threshold: ?*const Threshold = null,
    groups_skipped: usize = 0,
    tally: ?*driver.ScanTally = null,
    rg: usize = 0,
    rg_end: ?usize = null,

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Reader {
        return openProjected(arena, path, null);
    }

    /// `open`, decoding only the named columns. An unknown name is ignored; an empty
    /// projection (COUNT(*)) keeps the narrowest column so batches carry a row count.
    pub fn openProjected(arena: std.mem.Allocator, path: []const u8, want: ?[]const []const u8) !*Reader {
        const src = try Bytes.open(arena, path);
        errdefer src.close();

        var footer_start: u64 = 0;
        const md = try parseFooterOf(arena, src, &footer_start);

        const all = try collectLeaves(arena, md.schema);
        var keep = std.array_list.Managed(Leaf).init(arena);
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var outputs = std.array_list.Managed(Output).init(arena);
        var roots_seen = std.array_list.Managed(usize).init(arena);
        for (all, 0..) |lf, li| {
            const shared = if (lf.root) |rt| for (all, 0..) |other, oi| {
                if (oi != li and other.root != null and other.root.?.idx == rt.idx) break true;
            } else false else false;
            if (shared) {
                const rt = lf.root.?;
                for (roots_seen.items) |x| {
                    if (x == rt.idx) break;
                } else {
                    try roots_seen.append(rt.idx);
                    if (!wanted(want, lf.name)) continue;
                    const tree = try buildNested(arena, md.schema, rt);
                    var ix = std.array_list.Managed(usize).init(arena);
                    for (all) |o| if (o.root != null and o.root.?.idx == rt.idx) {
                        _ = try basaltType(md.schema[o.schema_idx]);
                        try keep.append(o);
                        try ix.append(keep.items.len - 1);
                    };
                    if (ix.items.len != tree.nleaves) return Error.UnsupportedParquetSchema;
                    const n = try arena.create(Nested);
                    n.* = .{ .name = lf.name, .root = tree, .leaves = ix.items };
                    try outputs.append(.{ .nested = n });
                    try fields.append(.{ .name = lf.name, .ty = types.Type.init(.string).asNullable() });
                }
                continue;
            }
            if (!wanted(want, lf.name)) continue;
            try keep.append(lf);
            try outputs.append(.{ .leaf = keep.items.len - 1 });
            try fields.append(.{ .name = lf.name, .ty = try leafType(md.schema[lf.schema_idx], lf) });
        }
        if (fields.items.len == 0 and want != null) {
            for (all) |lf| {
                if (lf.isRepeated()) continue;
                try keep.append(lf);
                try outputs.append(.{ .leaf = keep.items.len - 1 });
                try fields.append(.{
                    .name = lf.name,
                    .ty = (try basaltType(md.schema[lf.schema_idx])).asNullable(),
                });
                break;
            }
        }
        if (fields.items.len == 0) return Error.UnsupportedParquetSchema;

        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .src = src,
            .md = md,
            .schema = .{ .fields = try fields.toOwnedSlice() },
            .leaves = try keep.toOwnedSlice(),
            .outputs = try outputs.toOwnedSlice(),
            .boundaries = try chunkBoundaries(arena, md, footer_start),
        };
        return self;
    }

    /// One row group per call. Empty groups are skipped, since a zero-row batch reads
    /// as end-of-stream downstream.
    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        const last = self.rg_end orelse self.md.row_groups.len;
        while (self.rg < last) {
            const g = self.md.row_groups[self.rg];
            self.rg += 1;
            const rows = std.math.cast(usize, g.num_rows) orelse return Error.CorruptParquetPage;
            if (rows == 0) continue;
            if (self.tally) |t| driver.ScanTally.add(&t.row_groups, 1);
            if (self.bounds.len > 0 and
                !groupMayMatch(self.md.schema, self.leaves, g, self.bounds))
            {
                self.groups_skipped += 1;
                if (self.tally) |t| driver.ScanTally.add(&t.row_groups_skipped, 1);
                continue;
            }
            if (self.threshold) |t| {
                if (!groupBeatsThreshold(self.md.schema, self.leaves, g, t.*)) {
                    self.groups_skipped += 1;
                    if (self.tally) |ty| driver.ScanTally.add(&ty.row_groups_skipped, 1);
                    continue;
                }
            }
            const cols = try arena.alloc(column.Column, self.outputs.len);
            for (self.outputs, 0..) |o, ci| switch (o) {
                .leaf => |li| {
                    const lf = self.leaves[li];
                    const chunk = try self.chunkOf(arena, g, lf);
                    cols[ci] = try readColumnChunkLevels(
                        arena,
                        chunk.bytes,
                        chunk.meta,
                        self.md.schema[lf.schema_idx],
                        rows,
                        lf.max_def,
                        lf.max_rep,
                        lf.list,
                        chunk.start,
                    );
                },
                .nested => |n| {
                    const entries = try arena.alloc(Entries, n.leaves.len);
                    for (entries, n.leaves) |*e, li| {
                        const lf = self.leaves[li];
                        const chunk = try self.chunkOf(arena, g, lf);
                        e.* = try readEntries(arena, chunk.bytes, chunk.meta, self.md.schema[lf.schema_idx], lf.max_def, lf.max_rep, chunk.start);
                    }
                    cols[ci] = try assembleNested(arena, &n.root, entries, rows);
                },
            };
            return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
        }
        return null;
    }

    const Chunk = struct { bytes: []const u8, meta: parquet.ColumnMetaData, start: u64 };

    fn chunkOf(self: *Reader, arena: std.mem.Allocator, g: parquet.RowGroup, lf: Leaf) !Chunk {
        if (lf.chunk_idx >= g.columns.len) return Error.CorruptParquetPage;
        const meta = g.columns[lf.chunk_idx].meta orelse return Error.CorruptParquetPage;
        const start = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
        const end = chunkEnd(self.boundaries, start);
        if (end <= start) return Error.CorruptParquetPage;
        return .{ .bytes = try self.src.range(arena, start, @intCast(end - start)), .meta = meta, .start = start };
    }

    pub fn close(self: *Reader) void {
        self.src.close();
    }

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

/// How a folder file's columns differ from the first file's, in words; null when
/// they do not. Nullability is not compared.
pub fn schemaMismatch(arena: std.mem.Allocator, want: types.Schema, got: types.Schema) ?[]const u8 {
    for (want.fields, 0..) |w, k| {
        if (k >= got.fields.len) return std.fmt.allocPrint(arena, "it has no column `{s}`", .{w.name}) catch "a column is missing";
        const g = got.fields[k];
        if (!std.mem.eql(u8, w.name, g.name)) {
            for (got.fields) |o| if (std.mem.eql(u8, o.name, w.name))
                return std.fmt.allocPrint(arena, "its columns are in another order (`{s}` where `{s}` is expected)", .{ g.name, w.name }) catch "columns in another order";
            return std.fmt.allocPrint(arena, "it has no column `{s}`", .{w.name}) catch "a column is missing";
        }
        if (w.ty.kind != g.ty.kind or w.ty.precision != g.ty.precision or w.ty.scale != g.ty.scale)
            return std.fmt.allocPrint(arena, "column `{s}` is {s} there, not {s}", .{ w.name, @tagName(g.ty.kind), @tagName(w.ty.kind) }) catch "a column has another type";
    }
    if (got.fields.len > want.fields.len)
        return std.fmt.allocPrint(arena, "it has a column `{s}` the first file lacks", .{got.fields[want.fields.len].name}) catch "an extra column";
    return null;
}

pub fn mismatchMessage(arena: std.mem.Allocator, root: []const u8, path: []const u8, first: []const u8, why: []const u8) []const u8 {
    const rel = struct {
        fn f(r: []const u8, x: []const u8) []const u8 {
            return if (std.mem.startsWith(u8, x, r)) x[r.len..] else x;
        }
    }.f;
    return std.fmt.allocPrint(arena, "`{s}` in folder `{s}` does not match `{s}`, its first file: {s}", .{ rel(root, path), root, rel(root, first), why }) catch why;
}

const testing = std.testing;

pub fn listColumn(a: std.mem.Allocator, vals: []const Value) !column.Column {
    var b = column.Builder.init(a, types.Type.init(.int).asNullable());
    for (vals) |v| try b.append(v);
    return b.finish();
}

test "data page v2 with DELTA encodings decodes to the same values as v1" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "v2.parquet" });
    try tmp.dir.writeFile(.{ .sub_path = "v2.parquet", .data = fx_v2 });

    const r = try Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), r.schema.fields.len);
    const b = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 60), b.len);

    try testing.expectEqual(@as(i64, 0), b.columns[0].getValue(0).int);
    try testing.expectEqual(@as(i64, 42), b.columns[0].getValue(42).int);
    try testing.expectEqual(@as(i64, 59), b.columns[0].getValue(59).int);
    try testing.expectEqualStrings("row-0", b.columns[1].getValue(0).string);
    try testing.expectEqualStrings("row-59", b.columns[1].getValue(59).string);
    try testing.expectEqual(@as(f64, 88.5), b.columns[2].getValue(59).float);
    try testing.expectEqual(true, b.columns[3].getValue(0).bool);
}

test "projection keeps only the named columns and never drops all of them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "p.parquet" });
    try tmp.dir.writeFile(.{ .sub_path = "p.parquet", .data = @embedFile("../testdata/zstd.parquet") });

    const all = try Reader.open(a, path);
    try testing.expectEqual(@as(usize, 4), all.schema.fields.len);

    const two = try Reader.openProjected(a, path, &.{ "name", "flag" });
    try testing.expectEqual(@as(usize, 2), two.schema.fields.len);
    try testing.expectEqualStrings("name", two.schema.fields[0].name);
    try testing.expectEqualStrings("flag", two.schema.fields[1].name);
    const b = (try two.next(a)).?;
    try testing.expectEqual(@as(usize, 60), b.len);
    try testing.expectEqualStrings("row-0", b.columns[0].getValue(0).string);
    try testing.expectEqual(true, b.columns[1].getValue(0).bool);

    const one = try Reader.openProjected(a, path, &.{ "name", "nosuch" });
    try testing.expectEqual(@as(usize, 1), one.schema.fields.len);

    const none = try Reader.openProjected(a, path, &.{});
    try testing.expectEqual(@as(usize, 1), none.schema.fields.len);
    const nb = (try none.next(a)).?;
    try testing.expectEqual(@as(usize, 60), nb.len);
}

test "a corrupted file never panics" {
    const good = @embedFile("../testdata/zstd.parquet");
    var buf: [good.len]u8 = undefined;

    var off: usize = 0;
    while (off < good.len) : (off += 7) {
        for ([_]u8{ 0xFF, 0x80, 0x01 }) |bit| {
            @memcpy(&buf, good);
            buf[off] ^= bit;

            var ar = std.heap.ArenaAllocator.init(testing.allocator);
            defer ar.deinit();
            var tmp = testing.tmpDir(.{});
            defer tmp.cleanup();
            try tmp.dir.writeFile(.{ .sub_path = "c.parquet", .data = &buf });
            const dir = try tmp.dir.realpathAlloc(ar.allocator(), ".");
            const path = try std.fs.path.join(ar.allocator(), &.{ dir, "c.parquet" });

            const r = Reader.open(ar.allocator(), path) catch continue;
            defer r.close();
            while (r.next(ar.allocator()) catch null) |b| {
                if (b.len == 0) break;
            }
        }
    }
}

test "an http parquet source routes to the network, never the local filesystem" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();

    _ = Reader.open(ar.allocator(), "http://127.0.0.1:1/nope.parquet") catch |e| {
        try testing.expect(e != error.FileNotFound and e != error.NotDir and e != error.AccessDenied);
        return;
    };
    return error.TestExpectedConnectionError;
}

test "fuzz: page decode kernels survive arbitrary bytes" {
    try std.testing.fuzz({}, fuzzKernels, .{ .corpus = &fuzzKernels_corpus });
    try @import("../../net/fuzzutil.zig").pound(fuzzKernels, &fuzzKernels_corpus);
}

test "parquet LIST columns read as JSON from pyarrow (pages v1 and v2) and polars" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const want =
        \\id=1 xs=[1,2] nest=[[1],[2,3]] ds=["2026-01-01"] recs=[{"a":1,"b":"x"}] m={"k":1}
        \\id=2 xs=[] nest=[[]] ds=null recs=null m=null
        \\id=3 xs=null nest=null ds=[] recs=[] m={}
        \\id=4 xs=[null,5] nest=[null,[4]] ds=["1969-12-31",null] recs=[{"a":2,"b":"y"}] m={"z":2}
        \\
    ;
    try testing.expectEqualStrings(want, try readAllText(a, @embedFile("../testdata/lists_v1.parquet"), "v1.parquet"));
    try testing.expectEqualStrings(want, try readAllText(a, @embedFile("../testdata/lists_v2.parquet"), "v2.parquet"));
    try testing.expectEqualStrings(
        \\id=1 xs=[1,2] ss=["a"]
        \\id=2 xs=[] ss=["b","c"]
        \\id=3 xs=null ss=null
        \\id=4 xs=[null,5] ss=[]
        \\
    , try readAllText(a, @embedFile("../testdata/lists_polars.parquet"), "p.parquet"));
}

test "a nested fixture with bytes flipped anywhere errors or reads, never crashes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    inline for (.{ "../testdata/lists_v1.parquet", "../testdata/lists_v2.parquet" }) |fixture| {
        const good = @embedFile(fixture);
        var buf: [good.len]u8 = undefined;
        var i: usize = 0;
        while (i < good.len) : (i += 1) {
            @memcpy(&buf, good);
            buf[i] ^= 0x5A;
            _ = readAllText(a, &buf, "f.parquet") catch continue;
        }
    }
}

test {
    _ = @import("chunk.zig");
    _ = @import("encoding.zig");
    _ = @import("folder.zig");
    _ = @import("logical.zig");
    _ = @import("nested.zig");
    _ = @import("remote.zig");
    _ = @import("schema.zig");
    _ = @import("stats.zig");
    _ = @import("testing_util.zig");
}
