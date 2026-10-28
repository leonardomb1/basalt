//! Morsels: the units a pipeline's source divides into, each read whole by one
//! worker, the workers claiming them from one atomic cursor until none are left. A
//! Parquet source divides at row groups (at files for a remote folder or a table of
//! many files), a CSV, NDJSON or record file into newline-aligned byte ranges of
//! `RunOptions.morsel_bytes`, a SQL read into key ranges. A file's cut depends on the
//! file alone, never on `-j`, so what is built per morsel and combined in morsel order
//! is the same at every `-j`; a CSV cut once per thread left a P/E-core run waiting
//! on its slowest core. The workers number min(-j, morsels); one runs on the caller.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const ReaderSource = @import("sources.zig").ReaderSource;
const SplitCtx = @import("../connect.zig").SplitCtx;
const WorkQueue = @import("../parallel.zig").WorkQueue;
const csv = @import("../../format/csv.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const openSqlQuery = @import("../connect.zig").openSqlQuery;
const pqdecode = @import("../../format/parquet/read.zig");
const trace = @import("../../exec/trace.zig");
const types = @import("../../lang/types.zig");
const wrapProjected = @import("../../connect/split.zig").wrapProjected;

const max_csv_morsels: usize = 1 << 16;

pub const Morsels = union(enum) {
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema, count: usize },
    parquet: PqMorsels,
    sql: struct { split: *const SplitCtx, predicates: []const []const u8, schema: *const types.Schema },

    /// A mapped file cut into ranges of about `bytes`.
    pub fn ofCsv(mapped: *csv.MappedCsv, sch: *const types.Schema, bytes: usize) Morsels {
        const n = std.math.clamp(mapped.body.len / @max(bytes, 1) + 1, 1, max_csv_morsels);
        return .{ .csv = .{ .mapped = mapped, .schema = sch, .count = n } };
    }

    pub fn count(self: *const Morsels) usize {
        return switch (self.*) {
            .csv => |c| c.count,
            .parquet => |m| m.items.len,
            .sql => |s| s.predicates.len,
        };
    }

    /// The workers for `lanes`: no more than morsels (a lane past them only idled),
    /// nor than a remote source serves at once.
    pub fn workers(self: *const Morsels, lanes: usize) usize {
        const cap = if (self.* == .parquet) self.parquet.max_lanes else std.math.maxInt(usize);
        return @max(1, @min(lanes, cap, self.count()));
    }

    pub fn schema(self: *const Morsels) *const types.Schema {
        return switch (self.*) {
            .csv => |c| c.schema,
            .parquet => |m| m.src_schema,
            .sql => |s| s.schema,
        };
    }

    pub fn label(self: *const Morsels) []const u8 {
        return switch (self.*) {
            .csv => "csv",
            .parquet => "parquet",
            .sql => |s| @tagName(s.split.kind),
        };
    }

    pub fn unitName(self: *const Morsels) []const u8 {
        return switch (self.*) {
            .csv => "chunks",
            .parquet => |m| if (m.items.len > 0 and m.items[0].rg_end == null) "files" else "row groups",
            .sql => "key ranges",
        };
    }

    /// Morsel `i` as a source, null when it holds nothing (a file that shrank since
    /// planning). Its reader comes from `scratch`, a Parquet file stays open on `rd`
    /// for the worker's next morsel, and closing the source ends the morsel.
    pub fn open(self: *const Morsels, i: usize, rd: *Reader, scratch: std.mem.Allocator) !?driver.Source {
        switch (self.*) {
            .csv => |c| return try c.mapped.lane(scratch, i, c.count, c.schema),
            .parquet => |*m| {
                const r = (try openItem(m, rd.keep.allocator(), &rd.held, i)) orelse return null;
                rd.item = .{ .r = r, .schema_ = m.src_schema, .own = false };
                return .{ .ptr = &rd.item, .vtable = &ReaderSource.vtable };
            },
            .sql => |s| {
                const q = try wrapProjected(scratch, s.split.base_sql, s.split.proj_select, s.predicates[i], s.split.where_extra);
                return try openSqlQuery(s.split, rd.gpa, q);
            },
        }
    }
};

/// A worker's reading state across the morsels it claims: the Parquet file it has
/// open (reopening a file per row group re-parsed its footer) and the allocator a
/// key range's connection lives in. Init in place; it must not move.
pub const Reader = struct {
    gpa: std.mem.Allocator,
    keep: std.heap.ArenaAllocator,
    held: ?HeldFile = null,
    item: ReaderSource = undefined,

    pub fn init(gpa: std.mem.Allocator) Reader {
        return .{ .gpa = gpa, .keep = .init(gpa) };
    }

    pub fn deinit(self: *Reader) void {
        if (self.held) |h| h.r.close();
        self.held = null;
        self.keep.deinit();
    }
};

/// A worker's share as one source: the morsels it claims off `queue`, read one after
/// another, so a lane builds its chain and opens its sink once. `cur_item` is the
/// morsel being read, which a top-N's ties are broken by.
pub const Stream = struct {
    m: *const Morsels,
    queue: *WorkQueue,
    rd: *Reader,
    scratch: std.mem.Allocator,
    cur: ?driver.Source = null,
    cur_item: usize = 0,
    span: trace.Span = .{ .name = "", .arg = 0, .start = null },
    /// A fixed slice of the morsels instead of claimed ones: `next`, `next + step`, …
    fixed: ?struct { next: usize, step: usize } = null,

    pub fn source(self: *Stream) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn nextIndex(self: *Stream) ?usize {
        if (self.fixed) |*f| {
            if (self.queue.failed.load(.seq_cst) or f.next >= self.queue.nitems) return null;
            defer f.next += f.step;
            return f.next;
        }
        return self.queue.take();
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *Stream = @ptrCast(@alignCast(ptr));
        return self.m.schema().*;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *Stream = @ptrCast(@alignCast(ptr));
        while (true) {
            if (self.cur) |src| {
                if (try src.next(arena)) |b| return b;
                src.close();
                self.cur = null;
                self.span.end();
            }
            const i = self.nextIndex() orelse return null;
            self.span = trace.begin("morsel", i);
            self.cur_item = i;
            self.cur = try self.m.open(i, self.rd, self.scratch);
        }
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Stream = @ptrCast(@alignCast(ptr));
        if (self.cur) |src| src.close();
        self.cur = null;
    }

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

pub const PqMorsels = struct {
    files: []const []const u8,
    root: ?[]const u8 = null,
    items: []const PqItem,
    project: ?[][]const u8,
    bounds: []const pqdecode.Bound,
    keys: []const pqdecode.KeyBound = &.{},
    src_schema: *const types.Schema,
    tally: ?*driver.ScanTally = null,
    max_lanes: usize = std.math.maxInt(usize),
    check_in_lane: bool = false,
    /// A single remote file's reader as planning opened it: lanes reuse its footer
    /// and size, each on its own connection.
    like: ?*const pqdecode.Reader = null,
    /// A table's files, each opened fitted to `view`, the table's projected columns.
    layout: ?*const pqdecode.Layout = null,
    view: ?*const pqdecode.View = null,
};

pub const PqItem = struct { file: u32, rg: u32, rg_end: ?u32 };

pub const HeldFile = struct { file: u32, r: *pqdecode.Reader };

/// The reader positioned on item `i`, reusing `held` when it is the same file (reopening
/// per row group re-parsed the footer); null when a file shrank since it was planned.
pub fn openItem(m: *const PqMorsels, scratch: std.mem.Allocator, held: *?HeldFile, i: usize) !?*pqdecode.Reader {
    const it = m.items[i];
    if (held.*) |h| if (h.file != it.file) {
        h.r.close();
        held.* = null;
    };
    if (held.* == null) {
        const path = m.files[it.file];
        if (m.layout) |lay| {
            const r = try lay.open(scratch, m.view.?, it.file, m.bounds) orelse return null;
            held.* = .{ .file = it.file, .r = r };
            return positioned(m, r, it);
        }
        const r = if (m.like) |l| try pqdecode.Reader.openLike(scratch, path, l) else try pqdecode.Reader.openProjected(scratch, path, m.project);
        if (m.check_in_lane and it.file != 0) if (pqdecode.schemaMismatch(scratch, m.src_schema.*, r.schema, m.project)) |why| {
            r.close();
            return eval.explain(error.ParquetFolderMismatch, pqdecode.mismatchMessage(scratch, m.root orelse "", path, m.files[0], why));
        };
        held.* = .{ .file = it.file, .r = r };
    }
    return positioned(m, held.*.?.r, it);
}

fn positioned(m: *const PqMorsels, r: *pqdecode.Reader, it: PqItem) ?*pqdecode.Reader {
    r.bounds = m.bounds;
    r.keys = m.keys;
    r.tally = m.tally;
    r.rg = it.rg;
    r.rg_end = if (it.rg_end) |e| e else null;
    if (r.rg >= r.md.row_groups.len) return null;
    return r;
}

test "a CSV's morsels follow its size, not the lanes" {
    var body: [10_000]u8 = undefined;
    var mapped: csv.MappedCsv = undefined;
    mapped.body = &body;
    const schema = types.Schema{ .fields = &.{} };
    try std.testing.expectEqual(@as(usize, 1), Morsels.ofCsv(&mapped, &schema, 4 << 20).count());
    const m = Morsels.ofCsv(&mapped, &schema, 1000);
    try std.testing.expectEqual(@as(usize, 11), m.count());
    try std.testing.expectEqual(@as(usize, 4), m.workers(4));
    try std.testing.expectEqual(@as(usize, 11), m.workers(16));
    try std.testing.expectEqual(@as(usize, 1), m.workers(0));
}

test "parquet morsels run no more workers than row groups or a remote cap" {
    const schema = types.Schema{ .fields = &.{} };
    const items = [_]PqItem{ .{ .file = 0, .rg = 0, .rg_end = 1 }, .{ .file = 0, .rg = 1, .rg_end = 2 }, .{ .file = 0, .rg = 2, .rg_end = 3 } };
    var m = Morsels{ .parquet = .{ .files = &.{"x.parquet"}, .items = &items, .project = null, .bounds = &.{}, .src_schema = &schema } };
    try std.testing.expectEqual(@as(usize, 3), m.workers(8));
    try std.testing.expectEqual(@as(usize, 2), m.workers(2));
    m.parquet.max_lanes = 1;
    try std.testing.expectEqual(@as(usize, 1), m.workers(8));
}
