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
//! always read as micros (`civil.Tick`): ignoring the unit once read millisecond
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
//! Row-group pruning (`groupMayMatch`, `groupMayHoldKeys` for a join's keys,
//! `groupBeatsThreshold`, `fileMinMax`) is conservative in one direction only: a
//! group is skipped solely when statistics prove no row can match; anything
//! missing, unknown or unorderable keeps it.
//!
//! `Reader` reads one batch per row group, fetching only the footer and the
//! column chunks a query projects, so resident memory tracks the widest row
//! group's projected columns rather than the file. A table's file may have rows
//! the table deletes (`Deletes`, a Delta deletion vector): each is dropped from
//! its row group's batch by its position in the whole file, and a row group they
//! delete whole is not fetched. `Bytes` is where those bytes
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
//! `../bytes.zig` (where the bytes come from) and `folder.zig` (many files as one).
//! `write.zig` is the writer, `footer.zig` the file metadata, `thrift.zig` its codec.

const std = @import("std");
const msgfmt = @import("../msgfmt.zig");
const parquet = @import("footer.zig");
const types = @import("../../lang/types.zig");
const column = @import("../../exec/column.zig");
pub const Threshold = @import("../../exec/value.zig").Threshold;
const Value = @import("../../exec/value.zig").Value;
const eval = @import("../../exec/eval.zig");
const stream = @import("../stream.zig");
const fuzzKernels = @import("testing_util.zig").fuzzKernels;
const fuzzKernels_corpus = @import("testing_util.zig").fuzzKernels_corpus;
const fx_v2 = @import("testing_util.zig").fx_v2;
const readAllText = @import("testing_util.zig").readAllText;
const conform = @import("../../lang/conform.zig");

pub const Error = error{
    CorruptParquetPage,
    ByteRangeOutOfFile,
    UnsupportedParquetSchema,
    UnsupportedParquetEncoding,
} || std.mem.Allocator.Error;

pub const BitReader = @import("encoding.zig").BitReader;
pub const bitWidth = @import("encoding.zig").bitWidth;
pub const decodeRleHybrid = @import("encoding.zig").decodeRleHybrid;
pub const int96ToMicros = @import("encoding.zig").int96ToMicros;
pub const decodeDeltaBinaryPacked = @import("encoding.zig").decodeDeltaBinaryPacked;
pub const decodeDeltaLengthByteArray = @import("encoding.zig").decodeDeltaLengthByteArray;
pub const decodeDeltaByteArray = @import("encoding.zig").decodeDeltaByteArray;
pub const decodeByteStreamSplit = @import("encoding.zig").decodeByteStreamSplit;
pub const temporalTick = @import("logical.zig").temporalTick;
pub const basaltType = @import("logical.zig").basaltType;
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
const holdsStruct = @import("nested.zig").holdsStruct;
const leafIndices = @import("nested.zig").leafIndices;
pub const assembleNested = @import("nested.zig").assembleNested;
pub const Bound = @import("stats.zig").Bound;
pub const KeyBound = @import("stats.zig").KeyBound;
pub const groupMayMatch = @import("stats.zig").groupMayMatch;
pub const groupMayHoldKeys = @import("stats.zig").groupMayHoldKeys;
pub const groupBeatsThreshold = @import("stats.zig").groupBeatsThreshold;
pub const MinMax = @import("stats.zig").MinMax;
pub const fileMinMax = @import("stats.zig").fileMinMax;
const wanted = @import("stats.zig").wanted;
pub const leafType = @import("stats.zig").leafType;
pub const Bytes = @import("../bytes.zig").Bytes;
pub const Remote = @import("../bytes.zig").Remote;
pub const Folder = @import("folder.zig").Folder;
pub const Fit = @import("fit.zig").Fit;
pub const Layout = @import("fit.zig").Layout;
pub const View = @import("fit.zig").View;
pub const Deletes = @import("fit.zig").Deletes;
const roaring = @import("../lake/roaring.zig");
const source_vtable = driver.sourceVTable(Reader);
const chunkEnd = @import("folder.zig").chunkEnd;
const chunkBoundaries = @import("folder.zig").chunkBoundaries;
const parseFooterOf = @import("folder.zig").parseFooterOf;

const driver = @import("../../connect/driver.zig");
const Batch = @import("../../exec/batch.zig").Batch;

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
    keys: []const KeyBound = &.{},
    threshold: ?*const Threshold = null,
    groups_skipped: usize = 0,
    tally: ?*driver.ScanTally = null,
    rg: usize = 0,
    rg_end: ?usize = null,
    path: []const u8 = "",
    /// The current row group's chunks, fetched in a few coalesced ranges.
    held: []const Held = &.{},
    /// Set when the file is one of a table's whose schema governs it (`fit.zig`).
    fit: ?*const Fit = null,
    /// The rows the table deletes from this file, loaded into `deleted` when the
    /// first row group is read; `firsts` holds each row group's first row.
    deletes: ?Deletes = null,
    deleted: ?roaring.Bitmap = null,
    firsts: []const u64 = &.{},
    /// Top-level string columns read as dictionary codes only, by name: a pipeline
    /// whose only use of them is to group by them asks for it.
    codes_only: []const []const u8 = &.{},

    fn codesOnly(self: *const Reader, schema_idx: usize) bool {
        const name = self.md.schema[schema_idx].name;
        for (self.codes_only) |c| if (std.mem.eql(u8, c, name)) return true;
        return false;
    }

    pub fn isPath(path: []const u8) bool {
        return std.mem.endsWith(u8, path, ".parquet");
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Reader {
        return openProjected(arena, path, null);
    }

    /// `open`, decoding only the named columns. An unknown name is ignored; an empty
    /// projection (COUNT(*)) keeps the narrowest column so batches carry a row count.
    pub fn openProjected(arena: std.mem.Allocator, path: []const u8, want: ?[]const []const u8) !*Reader {
        return openSized(arena, path, null, want);
    }

    /// `openProjected` with the file's size when a table's log already gave it, which
    /// spares a remote file the request asking for it.
    pub fn openSized(arena: std.mem.Allocator, path: []const u8, size: ?u64, want: ?[]const []const u8) !*Reader {
        return openProjectedOn(arena, try Bytes.openSized(arena, path, size), path, want);
    }

    /// `openProjected` over bytes already open, which the reader then owns (closing
    /// them if it fails): a file planning opened and did not split, read on.
    pub fn openProjectedOn(arena: std.mem.Allocator, src: Bytes, path: []const u8, want: ?[]const []const u8) !*Reader {
        errdefer src.close();
        var footer_start: u64 = 0;
        const md = try parseFooterOf(arena, src, &footer_start);
        return openParsed(arena, src, md, footer_start, path, want);
    }

    /// `openProjectedOn` over a footer already parsed (`parseFooterOf`), which a
    /// table's file may have renamed its columns in (`fit.zig`). The reader owns
    /// `src` once it is returned.
    pub fn openParsed(arena: std.mem.Allocator, src: Bytes, md: parquet.FileMetaData, footer_start: u64, path: []const u8, want: ?[]const []const u8) !*Reader {
        const all = try collectLeaves(arena, md.schema);
        var keep = std.array_list.Managed(Leaf).init(arena);
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var outputs = std.array_list.Managed(Output).init(arena);
        var roots_seen = std.array_list.Managed(usize).init(arena);
        for (all, 0..) |lf, li| {
            const shared = if (lf.root) |rt| for (all, 0..) |other, oi| {
                if (oi != li and other.root != null and other.root.?.idx == rt.idx) break true;
            } else holdsStruct(&try buildNested(arena, md.schema, rt)) else false;
            if (shared) {
                const rt = lf.root.?;
                for (roots_seen.items) |x| {
                    if (x == rt.idx) break;
                } else {
                    try roots_seen.append(rt.idx);
                    if (!wanted(want, lf.name)) continue;
                    const tree = try buildNested(arena, md.schema, rt);
                    var at = std.array_list.Managed(usize).init(arena);
                    try leafIndices(&tree, &at);
                    var ix = std.array_list.Managed(usize).init(arena);
                    for (at.items) |si| {
                        const o = for (all) |x| {
                            if (x.schema_idx == si) break x;
                        } else return Error.UnsupportedParquetSchema;
                        _ = try basaltType(md.schema[o.schema_idx]);
                        try keep.append(o);
                        try ix.append(keep.items.len - 1);
                    }
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
            .path = path,
        };
        return self;
    }

    /// A reader of `path` over the footer and projection `like` already parsed, on
    /// its own connection: a lane of a split file, which then asks for neither the
    /// size nor the footer again.
    pub fn openLike(arena: std.mem.Allocator, path: []const u8, like: *const Reader) !*Reader {
        const src = try Bytes.openSized(arena, path, like.src.size());
        errdefer src.close();
        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .src = src,
            .md = like.md,
            .schema = like.schema,
            .leaves = like.leaves,
            .outputs = like.outputs,
            .boundaries = like.boundaries,
            .path = path,
        };
        return self;
    }

    /// One row group per call. Empty groups are skipped, since a zero-row batch reads
    /// as end-of-stream downstream. A deletion mask is sized only once the columns
    /// have decoded, as only they prove the footer's row count.
    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        const last = self.rg_end orelse self.md.row_groups.len;
        while (self.rg < last) {
            const gi = self.rg;
            const g = self.md.row_groups[gi];
            self.rg += 1;
            const rows = std.math.cast(usize, g.num_rows) orelse return Error.CorruptParquetPage;
            if (rows == 0) continue;
            if (self.tally) |t| driver.ScanTally.add(&t.row_groups, 1);
            if ((self.bounds.len > 0 and !groupMayMatch(self.md.schema, self.leaves, g, self.bounds)) or
                (self.keys.len > 0 and !groupMayHoldKeys(self.md.schema, self.leaves, g, self.keys)))
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
            var gone: ?*const roaring.Bitmap = null;
            var kept = rows;
            if (self.deletes) |d| {
                const bm = try self.deletedRows(d);
                const n = bm.count(self.firsts[gi], rows);
                if (n == rows) continue;
                if (n > 0) gone = bm;
                kept = rows - n;
            }
            if (self.src.coalesces()) self.held = try self.fetchGroup(arena, g);
            const cols = try arena.alloc(column.Column, self.outputs.len);
            for (self.outputs, 0..) |o, ci| switch (o) {
                .leaf => |li| {
                    const lf = self.leaves[li];
                    const chunk = try self.chunkOf(arena, g, lf);
                    cols[ci] = readColumnChunkLevels(
                        arena,
                        chunk.bytes,
                        chunk.meta,
                        self.md.schema[lf.schema_idx],
                        rows,
                        lf.max_def,
                        lf.max_rep,
                        lf.list,
                        chunk.start,
                        self.codesOnly(lf.schema_idx),
                    ) catch |e| return self.named(e);
                },
                .nested => |n| {
                    const entries = try arena.alloc(Entries, n.leaves.len);
                    for (entries, n.leaves) |*e, li| {
                        const lf = self.leaves[li];
                        const chunk = try self.chunkOf(arena, g, lf);
                        e.* = readEntries(arena, chunk.bytes, chunk.meta, self.md.schema[lf.schema_idx], lf.max_def, lf.max_rep, chunk.start) catch |err| return self.named(err);
                    }
                    cols[ci] = try assembleNested(arena, &n.root, entries, rows);
                },
            };
            if (gone) |bm| if (cols.len > 0) {
                const k = try arena.alloc(bool, rows);
                @memset(k, true);
                _ = bm.clear(self.firsts[gi], k);
                try column.gatherColumns(arena, cols, cols, k, kept);
            };
            const b = Batch{ .schema = &self.schema, .columns = cols, .len = kept };
            return if (self.fit) |f| try f.apply(arena, b) else b;
        }
        return null;
    }

    /// The rows the table deletes from this file, loaded into the reader's own
    /// arena on first use, with each row group's first row: a row is numbered by
    /// its position in the whole file, whichever row groups a read skips.
    fn deletedRows(self: *Reader, d: Deletes) !*const roaring.Bitmap {
        if (self.deleted == null) {
            const firsts = try self.arena.alloc(u64, self.md.row_groups.len);
            var total: u64 = 0;
            for (self.md.row_groups, firsts) |g, *f| {
                f.* = total;
                total += std.math.cast(u64, g.num_rows) orelse return Error.CorruptParquetPage;
            }
            self.firsts = firsts;
            self.deleted = try d.load(d.ctx, self.arena, total);
        }
        return &self.deleted.?;
    }

    const Chunk = struct { bytes: []const u8, meta: parquet.ColumnMetaData, start: u64 };

    const Held = struct { start: u64, bytes: []const u8 };

    /// A gap between two chunks read through rather than paying another request:
    /// about a round trip's worth of transfer.
    const hole_limit: u64 = 1 << 20;
    /// The most one request asks for.
    const range_cap: u64 = 64 << 20;

    /// Fetches the chunks of `g` this read decodes as a few ranges, adjacent and
    /// nearby chunks merged into one: each request over a network costs a round trip
    /// and restarts a cold transfer, so one range per chunk ran several times slower.
    fn fetchGroup(self: *Reader, arena: std.mem.Allocator, g: parquet.RowGroup) ![]const Held {
        var spans = std.array_list.Managed([2]u64).init(arena);
        for (self.leaves) |lf| {
            if (lf.chunk_idx >= g.columns.len) return Error.CorruptParquetPage;
            const meta = g.columns[lf.chunk_idx].meta orelse return Error.CorruptParquetPage;
            const start = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
            const end = chunkEnd(self.boundaries, start);
            if (end <= start) return Error.CorruptParquetPage;
            try spans.append(.{ start, end });
        }
        std.sort.insertion([2]u64, spans.items, {}, struct {
            fn lt(_: void, a: [2]u64, b: [2]u64) bool {
                return a[0] < b[0];
            }
        }.lt);
        var held = std.array_list.Managed(Held).init(arena);
        var i: usize = 0;
        while (i < spans.items.len) {
            const lo = spans.items[i][0];
            var hi = spans.items[i][1];
            i += 1;
            while (i < spans.items.len and spans.items[i][0] <= hi + hole_limit and @max(hi, spans.items[i][1]) - lo <= range_cap) : (i += 1)
                hi = @max(hi, spans.items[i][1]);
            try held.append(.{ .start = lo, .bytes = try self.src.range(arena, lo, @intCast(hi - lo)) });
        }
        return held.toOwnedSlice();
    }

    /// A decode failure's account, prefixed with this file, which a folder read needs.
    fn named(self: *const Reader, e: anyerror) anyerror {
        const why = eval.takeFailure(e) orelse return e;
        return stream.explainf(e, "`{s}`: {s}", .{ self.path, why });
    }

    fn chunkOf(self: *Reader, arena: std.mem.Allocator, g: parquet.RowGroup, lf: Leaf) !Chunk {
        if (lf.chunk_idx >= g.columns.len) return Error.CorruptParquetPage;
        const meta = g.columns[lf.chunk_idx].meta orelse return Error.CorruptParquetPage;
        const start = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
        const end = chunkEnd(self.boundaries, start);
        if (end <= start) return Error.CorruptParquetPage;
        for (self.held) |h| if (start >= h.start and end <= h.start + h.bytes.len)
            return .{ .bytes = h.bytes[@intCast(start - h.start)..][0..@intCast(end - start)], .meta = meta, .start = start };
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
/// they do not (`conform.Policy.parquet_folder`). Nullability is not compared.
/// `requested` is the projection, so a column one file stands in with (a
/// projection it holds none of) is not blamed.
pub fn schemaMismatch(arena: std.mem.Allocator, want: types.Schema, got: types.Schema, requested: ?[]const []const u8) ?[]const u8 {
    const out = conform.conform(arena, want, got, conform.Policy.parquet_folder, requested) catch return "its columns differ";
    return switch (out) {
        .cols => null,
        .problem => |p| conform.describe(arena, p),
    };
}

test "unsigned columns read as their unsigned values; a UINT64 past 2^63-1 reads as its DECIMAL(20,0) value" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings(
        \\u8=255 u16=65535 u32=4294967295 u64=9223372036854775807
        \\u8=1 u16=2 u32=3000000000 u64=4
        \\u8=1 u16=2 u32=3000000000 u64=4
        \\
    , try readAllText(a, @embedFile("../testdata/unsigned.parquet"), "u.parquet"));
    try testing.expectEqualStrings(
        \\big=18446744073709551615
        \\
    , try readAllText(a, @embedFile("../testdata/unsigned_big.parquet"), "b.parquet"));
}

test "a folder mismatch shows decimal precision and names the column the first file lacks" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const d102 = types.Schema{ .fields = &.{.{ .name = "f", .ty = types.Type.decimal(10, 2) }} };
    const d123 = types.Schema{ .fields = &.{.{ .name = "f", .ty = types.Type.decimal(12, 3) }} };
    try testing.expectEqualStrings("column `f` is decimal(12,3) there, not decimal(10,2)", schemaMismatch(a, d102, d123, null).?);
    const id = types.Schema{ .fields = &.{.{ .name = "id", .ty = types.Type.init(.int) }} };
    const extra = types.Schema{ .fields = &.{.{ .name = "extra", .ty = types.Type.init(.int) }} };
    try testing.expectEqualStrings("it has a column `extra` the first file lacks", schemaMismatch(a, id, extra, &.{"extra"}).?);
    try testing.expectEqualStrings("it has no column `extra`", schemaMismatch(a, extra, id, &.{"extra"}).?);
    const both = types.Schema{ .fields = &.{ .{ .name = "x", .ty = types.Type.init(.int) }, .{ .name = "id", .ty = types.Type.init(.int) } } };
    try testing.expectEqualStrings("its columns are in another order (`x` where `id` is expected)", schemaMismatch(a, id, both, null).?);
}

pub fn mismatchMessage(arena: std.mem.Allocator, root: []const u8, path: []const u8, first: []const u8, why: []const u8) []const u8 {
    const rel = struct {
        fn f(r: []const u8, x: []const u8) []const u8 {
            return if (std.mem.startsWith(u8, x, r)) x[r.len..] else x;
        }
    }.f;
    return msgfmt.allocPrint(arena, "`{s}` in folder `{s}` does not match `{s}`, its first file: {s}", .{ rel(root, path), root, rel(root, first), why }) catch why;
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

fn deleteRow5(_: *const anyopaque, _: std.mem.Allocator, _: u64) anyerror!roaring.Bitmap {
    const row5 = [_]roaring.Container{.{ .high = 0, .kind = .array, .data = &.{ 5, 0 } }};
    return .{ .containers = &row5, .cardinality = 1, .end = 6 };
}

test "deleted rows drop out, and a footer's row count is trusted for their mask only once the columns hold it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "v2.parquet" });
    try tmp.dir.writeFile(.{ .sub_path = "v2.parquet", .data = fx_v2 });
    const deletes: Deletes = .{ .ctx = undefined, .load = deleteRow5 };

    const r = try Reader.open(a, path);
    r.deletes = deletes;
    const b = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 59), b.len);
    try testing.expectEqual(@as(i64, 4), b.columns[0].getValue(4).int);
    try testing.expectEqual(@as(i64, 6), b.columns[0].getValue(5).int);

    const lying = try Reader.open(a, path);
    lying.deletes = deletes;
    @constCast(lying.md.row_groups)[0].num_rows = 1 << 47;
    try testing.expectError(Error.CorruptParquetPage, lying.next(a));
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
    try @import("../../testing/fuzzutil.zig").pound(fuzzKernels, &fuzzKernels_corpus);
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

test "a list of a one-field struct reads as a list of objects, as DuckDB wrote and reads it" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    try testing.expectEqualStrings(
        \\id=1 xs=[{"ab":0},{"ab":1}]
        \\id=2 xs=[{"ab":null},null]
        \\id=3 xs=[]
        \\id=4 xs=null
        \\
    , try readAllText(ar.allocator(), @embedFile("../testdata/list_one_field_struct.parquet"), "l.parquet"));
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
    _ = @import("fit.zig");
    _ = @import("logical.zig");
    _ = @import("nested.zig");
    _ = @import("../bytes.zig");
    _ = @import("schema.zig");
    _ = @import("stats.zig");
    _ = @import("testing_util.zig");
}

test "schemaMismatch on conform gives the old check's verdict and words on every pair" {
    const old = struct {
        fn oldSchemaMismatch(arena: std.mem.Allocator, want: types.Schema, got: types.Schema, requested: ?[]const []const u8) ?[]const u8 {
            for (want.fields, 0..) |w, k| {
                if (k >= got.fields.len) return msgfmt.allocPrint(arena, "it has no column `{s}`", .{w.name}) catch "a column is missing";
                const g = got.fields[k];
                if (!std.mem.eql(u8, w.name, g.name)) {
                    for (got.fields) |o| if (std.mem.eql(u8, o.name, w.name))
                        return msgfmt.allocPrint(arena, "its columns are in another order (`{s}` where `{s}` is expected)", .{ g.name, w.name }) catch "columns in another order";
                    const asked = if (requested) |r| for (r) |n| {
                        if (std.mem.eql(u8, n, g.name)) break true;
                    } else false else true;
                    if (asked) for (want.fields) |o| {
                        if (std.mem.eql(u8, o.name, g.name)) break;
                    } else return msgfmt.allocPrint(arena, "it has a column `{s}` the first file lacks", .{g.name}) catch "an extra column";
                    return msgfmt.allocPrint(arena, "it has no column `{s}`", .{w.name}) catch "a column is missing";
                }
                if (w.ty.kind != g.ty.kind or w.ty.precision != g.ty.precision or w.ty.scale != g.ty.scale)
                    return msgfmt.allocPrint(arena, "column `{s}` is {s} there, not {s}", .{ w.name, oldTypeName(arena, g.ty), oldTypeName(arena, w.ty) }) catch "a column has another type";
            }
            if (got.fields.len > want.fields.len)
                return msgfmt.allocPrint(arena, "it has a column `{s}` the first file lacks", .{got.fields[want.fields.len].name}) catch "an extra column";
            return null;
        }

        fn oldTypeName(arena: std.mem.Allocator, t: types.Type) []const u8 {
            if (t.kind != .decimal) return @tagName(t.kind);
            return msgfmt.allocPrint(arena, "decimal({d},{d})", .{ t.precision, t.scale }) catch "decimal";
        }
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const T = types.Type;
    const F = types.Schema.Field;
    const i = T.init(.int);
    const schemas = [_]types.Schema{
        .{ .fields = &.{} },
        .{ .fields = &[_]F{.{ .name = "id", .ty = i }} },
        .{ .fields = &[_]F{.{ .name = "id", .ty = i.asNullable() }} },
        .{ .fields = &[_]F{.{ .name = "id", .ty = T.init(.string) }} },
        .{ .fields = &[_]F{.{ .name = "x", .ty = i }} },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "x", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "x", .ty = i }, .{ .name = "id", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "y", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "id", .ty = i }, .{ .name = "x", .ty = i }, .{ .name = "y", .ty = i } } },
        .{ .fields = &[_]F{ .{ .name = "y", .ty = i }, .{ .name = "x", .ty = i } } },
        .{ .fields = &[_]F{.{ .name = "d", .ty = T.decimal(10, 2) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = T.decimal(12, 2) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = T.decimal(10, 3) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = .{ .kind = .string, .scale = 2 } }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = T.nullColumn(true) }} },
        .{ .fields = &[_]F{.{ .name = "d", .ty = T.unknownNull() }} },
    };
    const askeds = [_]?[]const []const u8{ null, &.{}, &.{"id"}, &.{"x"}, &.{"y"}, &.{ "id", "x" }, &.{ "x", "y" }, &.{"d"} };
    for (schemas) |want| for (schemas) |got| for (askeds) |asked| {
        const was = old.oldSchemaMismatch(a, want, got, asked);
        const new = schemaMismatch(a, want, got, asked);
        try testing.expectEqual(was == null, new == null);
        if (was) |o| try testing.expectEqualStrings(o, new.?);
    };
}
