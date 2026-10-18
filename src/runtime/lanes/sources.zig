//! How a read divides into lanes: Parquet row groups, CSV byte ranges cut at line
//! ends, and the source each lane reads its share through.

const Env = @import("../env.zig").Env;
const HeldFile = @import("agg.zig").HeldFile;
const MorselSource = @import("agg.zig").MorselSource;
const PqItem = @import("agg.zig").PqItem;
const PqMorsels = @import("agg.zig").PqMorsels;
const ReaderSource = @import("distinct.zig").ReaderSource;
const RunOptions = @import("../env.zig").RunOptions;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const connect_mod = @import("../connect.zig");
const csv = @import("../../format/csv.zig");
const driver = @import("../../connect/driver.zig");
const filterBounds = @import("../plan.zig").filterBounds;
const folder = @import("../../connect/folder.zig");
const lake = @import("../../format/lake/table.zig");
const openItem = @import("agg.zig").openItem;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const pq_min_lanes = @import("agg.zig").pq_min_lanes;
const pqdecode = @import("../../format/parquet/read.zig");
const projectedColumns = @import("../plan.zig").projectedColumns;
const schemaPtr = @import("../env.zig").schemaPtr;
const sftp = @import("../../store/sftp.zig");
const table_builtins = @import("../../connect/table_builtins.zig");
const builtinArgs = @import("../connect/source.zig").builtinArgs;
const std = @import("std");
const types = @import("../../lang/types.zig");

const LaneRows = union(enum) {
    parquet: *PqMorsels,
    parquet_group: struct { m: *const PqMorsels, group: usize },
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema, chunk: usize, of: usize },
};

/// This lane's row stream, or null when the item holds nothing (a file that shrank
/// since planning). The reader comes from `scratch` because the source borrows it;
/// closing the source releases it.
pub fn laneRowSource(rows: LaneRows, scratch: std.mem.Allocator) !?driver.Source {
    switch (rows) {
        .parquet => |m| {
            const ms = try scratch.create(MorselSource);
            ms.* = .{ .m = m, .scratch = scratch };
            return .{ .ptr = ms, .vtable = &MorselSource.vtable };
        },
        .parquet_group => |g| {
            var held: ?HeldFile = null;
            const rdr = (try openItem(g.m, scratch, &held, g.group)) orelse {
                if (held) |h| h.r.close();
                return null;
            };
            const rs = try scratch.create(ReaderSource);
            rs.* = .{ .r = rdr, .schema_ = g.m.src_schema };
            return .{ .ptr = rs, .vtable = &ReaderSource.vtable };
        },
        .csv => |c| return try c.mapped.lane(scratch, c.chunk, c.of, c.schema),
    }
}

/// Open `rd`'s parquet input as splittable, or null. Local files split at row groups,
/// each footer checked here; a remote folder splits at files; a single remote file
/// splits at row groups when it has two or more and its server honours range
/// requests, each lane on its own connection over the footer read here.
/// `push_stages` stops before a map path's join, whose names are the join's.
pub fn parquetSplit(env: *Env, rd: ast.Read, hints: []const ast.Hint, push_stages: []const ast.Stage, w: ast.Write, opts: RunOptions) anyerror!?LaneSplit {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    if (opts.lanes() < pq_min_lanes) return null;

    var files: []const []const u8 = &.{};
    var root: ?[]const u8 = null;
    if (lake.isTablePath(path, .iceberg)) {
        const fr = (try connect_mod.resolveFolder(env, path, hints)) orelse return null;
        if (fr.kind != .parquet) return null;
        if (fr.table) |t| return tableSplit(env, t.layout, path, push_stages);
        files = fr.files;
        root = path;
    } else {
        const one = try arena.alloc([]const u8, 1);
        one[0] = path;
        files = one;
    }
    const remote = csv.CsvReader.isUrl(path);

    const project = try projectedColumns(env, push_stages);
    const bounds = try filterBounds(env, push_stages);
    const probe = pqdecode.Reader.openProjected(arena, files[0], project) catch return null;
    defer probe.close();
    var schema = probe.schema;
    if (root != null) {
        const fields = try arena.alloc(types.Schema.Field, schema.fields.len);
        for (fields, schema.fields) |*f, x| f.* = .{ .name = x.name, .ty = x.ty.asNullable() };
        schema = .{ .fields = fields };
    }

    var items = std.array_list.Managed(PqItem).init(arena);
    var like: ?*const pqdecode.Reader = null;
    if (remote and root == null) {
        if (!probe.src.rangesHonoured() or probe.md.row_groups.len < 2) return null;
        for (0..probe.md.row_groups.len) |g| try items.append(.{ .file = 0, .rg = @intCast(g), .rg_end = @intCast(g + 1) });
        like = probe;
    } else if (remote) {
        if (files.len < 2) return null;
        for (0..files.len) |k| try items.append(.{ .file = @intCast(k), .rg = 0, .rg_end = null });
    } else {
        for (files, 0..) |f, k| {
            const r = if (k == 0) probe else pqdecode.Reader.openProjected(arena, f, project) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(arena, "could not read parquet `{s}` ({s})", .{ f, @errorName(e) }));
            defer if (k != 0) r.close();
            if (k != 0) if (pqdecode.schemaMismatch(arena, schema, r.schema, project)) |why|
                return planErr(env.diag, pqdecode.mismatchMessage(arena, root.?, f, files[0], why));
            for (0..r.md.row_groups.len) |g| try items.append(.{ .file = @intCast(k), .rg = @intCast(g), .rg_end = @intCast(g + 1) });
        }
        if (items.items.len < 2) return null;
    }
    if (root != null) env.folder_memo = null;
    if (env.scan) |t| {
        driver.ScanTally.add(&t.columns_read, probe.leaves.len);
        driver.ScanTally.add(&t.columns_total, probe.md.leafCount());
    }

    return .{ .parquet = .{
        .tally = env.scan,
        .files = files,
        .root = root,
        .items = items.items,
        .project = project,
        .bounds = bounds,
        .src_schema = try schemaPtr(arena, schema),
        .queue = .{ .nitems = items.items.len },
        .max_lanes = if (!remote) std.math.maxInt(usize) else if (sftp.isUrl(path)) 4 else 8,
        .check_in_lane = remote and root != null,
        .like = like,
    } };
}

/// A table's live files split at row groups, locally each file opened here so one
/// that does not fit the table fails the plan; remotely at files. A file its
/// partition values rule out is left out.
fn tableSplit(env: *Env, layout: *const pqdecode.Layout, path: []const u8, push_stages: []const ast.Stage) anyerror!?LaneSplit {
    const arena = env.arena;
    const project = try projectedColumns(env, push_stages);
    const bounds = try filterBounds(env, push_stages);
    const view = try layout.view(arena, project);
    const remote = csv.CsvReader.isUrl(path);
    var items = std.array_list.Managed(PqItem).init(arena);
    var noted = false;
    for (layout.files, 0..) |_, k| {
        if (layout.pruned(k, bounds)) continue;
        if (remote) {
            try items.append(.{ .file = @intCast(k), .rg = 0, .rg_end = null });
            continue;
        }
        const r = (try layout.open(arena, view, k, &.{})).?;
        defer r.close();
        if (!noted) if (env.scan) |t| {
            noted = true;
            driver.ScanTally.add(&t.columns_read, r.leaves.len);
            driver.ScanTally.add(&t.columns_total, r.md.leafCount());
        };
        for (0..r.md.row_groups.len) |g| try items.append(.{ .file = @intCast(k), .rg = @intCast(g), .rg_end = @intCast(g + 1) });
    }
    if (items.items.len < 2) return null;
    env.folder_memo = null;
    return .{ .parquet = .{
        .tally = env.scan,
        .files = try layout.paths(arena),
        .root = path,
        .items = items.items,
        .project = project,
        .bounds = bounds,
        .src_schema = &view.schema,
        .queue = .{ .nitems = items.items.len },
        .max_lanes = if (!remote) std.math.maxInt(usize) else if (sftp.isUrl(path)) 4 else 8,
        .layout = layout,
        .view = view,
    } };
}

/// Map `rd`'s CSV, NDJSON or record file for splitting, or null; the caller must `close()` it. A
/// quoted newline makes a CSV's byte boundaries undecidable, so that file is closed
/// here and read serially (leaking it before the caller's defer exhausted fds in a FOR
/// EACH); a JSON file that is one array is read serially too.
pub fn csvSplitFile(env: *Env, rd: ast.Read, hints: []const ast.Hint, w: ast.Write) anyerror!?*csv.MappedCsv {
    const path = switch (rd.form) {
        .path => |p| p,
        .builtin => |b| return recordSplitFile(env, b, w),
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    switch (analyze.readFormat(path, env.fmt_in)) {
        .csv => {},
        .json => {
            const m = csv.MappedCsv.openNdjson(env.arena, path, analyze.jsonOptions(hints)) catch return null;
            if (rd.cols.len > 0) try m.project(env.arena, rd.cols);
            return m;
        },
        else => return null,
    }

    var d = env.csv_in;
    d.text = rd.text_cols;
    const mapped = csv.MappedCsv.open(env.arena, path, d) catch return null;
    if (mapped.quoted_newlines) {
        mapped.close();
        return null;
    }
    if (rd.cols.len > 0) try mapped.project(env.arena, rd.cols);
    return mapped;
}

/// A record builtin's file mapped for lanes, or null when it reads serially: not a
/// record reader, remote, compressed, or cut only by reading its records in order.
fn recordSplitFile(env: *Env, call: ast.BuiltinRead, w: ast.Write) anyerror!?*csv.MappedCsv {
    const mode_of = table_builtins.of(call).mode orelse return null;
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    const args = builtinArgs(env, call) catch |e| switch (e) {
        error.OutOfMemory => return e,
        else => return null,
    };
    return csv.MappedCsv.openRecords(env.arena, args[0].string, mode_of(args)) catch |e| switch (e) {
        error.OutOfMemory => return e,
        else => null,
    };
}

const min_lane_bytes = 1 << 20;

/// Lanes for a CSV of `bytes`: the `-j` given, or by default no more than one per MiB,
/// since a lane costs more to start than a small file takes to parse (a 600 KB GROUP BY
/// ran in 4 ms on one lane and 9 ms on twenty).
pub fn csvLanes(bytes: usize, opts: RunOptions) usize {
    return capCsvLanes(bytes, opts.lanes(), opts.threads == null);
}

fn capCsvLanes(bytes: usize, lanes: usize, auto: bool) usize {
    const n = @max(@as(usize, 1), lanes);
    return if (auto) @min(n, bytes / min_lane_bytes + 1) else n;
}

pub const LaneSplit = union(enum) {
    csv: struct { mapped: *csv.MappedCsv, schema: *const types.Schema },
    parquet: PqMorsels,

    /// A CSV's item count is free, one chunk per thread; a parquet file's is its row groups.
    pub fn count(self: LaneSplit, nthreads: usize) usize {
        return switch (self) {
            .csv => nthreads,
            .parquet => |m| m.queue.nitems,
        };
    }

    /// No more lanes than parquet items: a lane past them only spawned and idled.
    pub fn lanes(self: *const LaneSplit, opts: RunOptions) usize {
        return switch (self.*) {
            .csv => |c| csvLanes(c.mapped.body.len, opts),
            .parquet => |m| @max(1, @min(opts.lanes(), m.max_lanes, m.queue.nitems)),
        };
    }

    pub fn schema(self: *const LaneSplit) *const types.Schema {
        return switch (self.*) {
            .csv => |c| c.schema,
            .parquet => |m| m.src_schema,
        };
    }

    pub fn label(self: LaneSplit) []const u8 {
        return switch (self) {
            .csv => "csv",
            .parquet => "parquet",
        };
    }

    pub fn rows(self: *const LaneSplit, i: usize, nitems: usize) LaneRows {
        return switch (self.*) {
            .csv => |c| .{ .csv = .{ .mapped = c.mapped, .schema = c.schema, .chunk = i, .of = nitems } },
            .parquet => |*m| .{ .parquet_group = .{ .m = m, .group = i } },
        };
    }

    /// Rows for a shape that needs no item order (top-N re-sorts): a parquet lane steals row
    /// groups into one heap. Items are still stolen, not indexed by lane, since `spawnJoin`
    /// may start fewer threads than asked and a lane-keyed CSV chunk would be skipped.
    pub fn unorderedRows(self: *LaneSplit, i: usize, nitems: usize) LaneRows {
        return switch (self.*) {
            .csv => |c| .{ .csv = .{ .mapped = c.mapped, .schema = c.schema, .chunk = i, .of = nitems } },
            .parquet => |*m| .{ .parquet = m },
        };
    }

    pub fn abort(self: *LaneSplit) void {
        switch (self.*) {
            .parquet => |*m| m.queue.failed.store(true, .seq_cst),
            .csv => {},
        }
    }

    pub fn unitName(self: LaneSplit) []const u8 {
        return switch (self) {
            .csv => "chunks",
            .parquet => |m| if (m.items.len > 0 and m.items[0].rg_end == null) "files" else "row groups",
        };
    }
};

test "csvLanes: an explicit -j is kept, the default caps at one lane per MiB" {
    try std.testing.expectEqual(@as(usize, 20), csvLanes(1000, .{ .threads = 20 }));
    try std.testing.expectEqual(@as(usize, 1), capCsvLanes(1000, 20, true));
    try std.testing.expectEqual(@as(usize, 7), capCsvLanes(6 << 20, 20, true));
    try std.testing.expectEqual(@as(usize, 14), capCsvLanes(300 << 20, 14, true));
    try std.testing.expectEqual(@as(usize, 1), csvLanes(0, .{ .threads = 0 }));
    try std.testing.expectEqual(@as(usize, 1), csvLanes(1000, .{ .threads = null }));
    try std.testing.expectEqual(@min(@import("../parallel.zig").defaultLanes(), 7), csvLanes(6 << 20, .{ .threads = null }));
}

test "LaneSplit.lanes: parquet runs no more lanes than row groups" {
    const schema = types.Schema{ .fields = &.{} };
    const items = [_]PqItem{ .{ .file = 0, .rg = 0, .rg_end = 1 }, .{ .file = 0, .rg = 1, .rg_end = 2 }, .{ .file = 0, .rg = 2, .rg_end = 3 } };
    var split = LaneSplit{ .parquet = .{
        .files = &.{"x.parquet"},
        .items = &items,
        .project = null,
        .bounds = &.{},
        .src_schema = &schema,
        .queue = .{ .nitems = items.len },
    } };
    try std.testing.expectEqual(@as(usize, 3), split.lanes(.{ .threads = 8 }));
    try std.testing.expectEqual(@as(usize, 2), split.lanes(.{ .threads = 2 }));
    split.parquet.max_lanes = 1;
    try std.testing.expectEqual(@as(usize, 1), split.lanes(.{ .threads = 8 }));
}
