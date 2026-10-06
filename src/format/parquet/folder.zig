//! A folder of Parquet files read as one table, every file checked against the first
//! one's columns.

const Batch = @import("../../exec/batch.zig").Batch;
const Bound = @import("stats.zig").Bound;
const Bytes = @import("remote.zig").Bytes;
const Error = @import("read.zig").Error;
const Reader = @import("read.zig").Reader;
const Threshold = @import("../../exec/value.zig").Threshold;
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const mismatchMessage = @import("read.zig").mismatchMessage;
const objstore = @import("../../store/objstore.zig");
const parquet = @import("footer.zig");
const schemaMismatch = @import("read.zig").schemaMismatch;
const std = @import("std");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const Folder = struct {
    arena: std.mem.Allocator,
    root: []const u8,
    files: []const []const u8,
    project: ?[]const []const u8,
    schema: types.Schema,
    bounds: []const Bound = &.{},
    threshold: ?*const Threshold = null,
    tally: ?*driver.ScanTally = null,
    cur: ?*Reader = null,
    i: usize = 0,

    pub fn open(arena: std.mem.Allocator, root: []const u8, files: []const []const u8, project: ?[]const []const u8) !*Folder {
        if (files.len == 0) return error.EmptyFolder;
        const first = try Reader.openProjected(arena, files[0], project);
        const fields = try arena.alloc(types.Schema.Field, first.schema.fields.len);
        for (fields, first.schema.fields) |*f, src| f.* = .{ .name = src.name, .ty = src.ty.asNullable() };
        const self = try arena.create(Folder);
        self.* = .{ .arena = arena, .root = root, .files = files, .project = project, .schema = .{ .fields = fields }, .cur = first, .i = 1 };
        return self;
    }

    pub fn firstReader(self: *const Folder) ?*Reader {
        return if (self.i == 1) self.cur else null;
    }

    pub fn next(self: *Folder, arena: std.mem.Allocator) !?Batch {
        while (true) {
            if (self.cur) |r| {
                r.bounds = self.bounds;
                r.threshold = self.threshold;
                r.tally = self.tally;
                if (try r.next(arena)) |b| {
                    var out = b;
                    out.schema = &self.schema;
                    return out;
                }
                r.close();
                self.cur = null;
            }
            if (self.i >= self.files.len) return null;
            const path = self.files[self.i];
            self.i += 1;
            const r = try Reader.openProjected(self.arena, path, self.project);
            if (self.mismatch(r.schema)) |why| {
                r.close();
                return eval.explain(error.ParquetFolderMismatch, mismatchMessage(self.arena, self.root, path, self.files[0], why));
            }
            self.cur = r;
        }
    }

    fn mismatch(self: *Folder, got: types.Schema) ?[]const u8 {
        return schemaMismatch(self.arena, self.schema, got);
    }

    pub fn close(self: *Folder) void {
        if (self.cur) |r| r.close();
        self.cur = null;
    }

    pub fn source(self: *Folder) driver.Source {
        return .{ .ptr = self, .vtable = &folder_vtable };
    }

    const folder_vtable = driver.Source.VTable{
        .schema = struct {
            fn f(p: *anyopaque) types.Schema {
                return @as(*Folder, @ptrCast(@alignCast(p))).schema;
            }
        }.f,
        .next = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
                return @as(*Folder, @ptrCast(@alignCast(p))).next(arena);
            }
        }.f,
        .close = struct {
            fn f(p: *anyopaque) void {
                @as(*Folder, @ptrCast(@alignCast(p))).close();
            }
        }.f,
    };
};

pub const source_vtable = driver.Source.VTable{
    .schema = srcSchema,
    .next = srcNext,
    .close = srcClose,
};

fn srcSchema(p: *anyopaque) types.Schema {
    return @as(*Reader, @ptrCast(@alignCast(p))).schema;
}

fn srcNext(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    return @as(*Reader, @ptrCast(@alignCast(p))).next(arena);
}

fn srcClose(p: *anyopaque) void {
    @as(*Reader, @ptrCast(@alignCast(p))).close();
}

/// Ends at the next chunk's start, or the footer. `total_compressed_size` is not
/// used: writers disagree on whether it counts headers, which truncated chunks.
pub fn chunkEnd(boundaries: []const u64, start: u64) u64 {
    for (boundaries) |b| {
        if (b > start) return b;
    }
    return start;
}

pub fn chunkBoundaries(arena: std.mem.Allocator, md: parquet.FileMetaData, footer_start: u64) ![]u64 {
    var out = std.array_list.Managed(u64).init(arena);
    for (md.row_groups) |g| {
        for (g.columns) |c| {
            const m = c.meta orelse continue;
            try out.append(std.math.cast(u64, m.startOffset()) orelse return Error.CorruptParquetPage);
        }
    }
    try out.append(footer_start);
    const sl = try out.toOwnedSlice();
    std.mem.sort(u64, sl, {}, comptime std.sort.asc(u64));
    return sl;
}

pub fn parseFooterOf(arena: std.mem.Allocator, src: Bytes, footer_start: *u64) !parquet.FileMetaData {
    const total = src.size();
    if (total < parquet.trailer_len + parquet.magic.len) return parquet.Error.NotParquet;
    const head = try src.range(arena, 0, parquet.magic.len);
    if (!std.mem.eql(u8, head, parquet.magic)) return parquet.Error.NotParquet;

    const trailer = try src.range(arena, total - parquet.trailer_len, parquet.trailer_len);
    const r = try parquet.footerRange(total, trailer);
    footer_start.* = r.offset;
    const footer = try src.range(arena, r.offset, r.len);
    return parquet.parseFooter(arena, footer);
}

pub fn isRemote(path: []const u8) bool {
    return objstore.isUrl(path) or
        std.mem.startsWith(u8, path, "http://") or
        std.mem.startsWith(u8, path, "https://");
}

test "chunk extents come from the next chunk, never from total_compressed_size" {
    const b = [_]u64{ 4, 100, 250, 900 };
    try testing.expectEqual(@as(u64, 100), chunkEnd(&b, 4));
    try testing.expectEqual(@as(u64, 250), chunkEnd(&b, 100));
    try testing.expectEqual(@as(u64, 900), chunkEnd(&b, 250));
    try testing.expectEqual(@as(u64, 900), chunkEnd(&b, 900));
}

test "remote paths are recognised, local ones left alone" {
    try testing.expect(isRemote("https://host/a.parquet"));
    try testing.expect(isRemote("http://host/a.parquet"));
    try testing.expect(isRemote("az://acct/ctr/a.parquet"));
    try testing.expect(!isRemote("/data/a.parquet"));
    try testing.expect(!isRemote("a.parquet"));
}
