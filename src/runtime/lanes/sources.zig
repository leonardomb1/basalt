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
const openItem = @import("agg.zig").openItem;
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const pq_min_lanes = @import("agg.zig").pq_min_lanes;
const pqdecode = @import("../../format/parquet/read.zig");
const projectedColumns = @import("../plan.zig").projectedColumns;
const schemaPtr = @import("../env.zig").schemaPtr;
const sftp = @import("../../store/sftp.zig");
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
        .csv => |c| {
            const rd = try scratch.create(csv.CsvSliceReader);
            rd.* = .{ .data = c.mapped.chunk(c.chunk, c.of), .schema = c.schema, .dialect = c.mapped.dialect, .slot = c.mapped.slot };
            return rd.source();
        },
    }
}

/// Open `rd`'s parquet input as splittable, or null. Local files split at row groups,
/// each footer checked here; a remote folder splits at files and a single remote file
/// stays serial. `push_stages` stops before a map path's join, whose names are the join's.
pub fn parquetSplit(env: *Env, rd: ast.Read, push_stages: []const ast.Stage, w: ast.Write, opts: RunOptions) anyerror!?LaneSplit {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    if (opts.threads < pq_min_lanes) return null;

    var files: []const []const u8 = &.{};
    var root: ?[]const u8 = null;
    if (folder.isFolder(path)) {
        const fr = (try connect_mod.resolveFolderFmt(env, path, env.fmt_in)) orelse return null;
        if (fr.kind != .parquet) return null;
        files = fr.files;
        root = path;
    } else {
        if (csv.CsvReader.isUrl(path)) return null;
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
    if (remote) {
        if (files.len < 2) return null;
        for (0..files.len) |k| try items.append(.{ .file = @intCast(k), .rg = 0, .rg_end = null });
    } else {
        for (files, 0..) |f, k| {
            const r = if (k == 0) probe else pqdecode.Reader.openProjected(arena, f, project) catch |e|
                return planErrT(env.diag, e, try std.fmt.allocPrint(arena, "could not read parquet `{s}` ({s})", .{ f, @errorName(e) }));
            defer if (k != 0) r.close();
            if (k != 0) if (pqdecode.schemaMismatch(arena, schema, r.schema)) |why|
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
    } };
}

/// Map `rd`'s CSV for splitting, or null; the caller must `close()` it. A quoted newline
/// makes byte boundaries undecidable, so that file is closed here and read serially
/// (leaking it before the caller's defer exhausted fds in a FOR EACH).
pub fn csvSplitFile(env: *Env, rd: ast.Read, w: ast.Write) anyerror!?*csv.MappedCsv {
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;
    if (analyze.readFormat(path, env.fmt_in) != .csv) return null;

    const mapped = csv.MappedCsv.open(env.arena, path, env.csv_in) catch return null;
    if (mapped.quoted_newlines) {
        mapped.close();
        return null;
    }
    if (rd.cols.len > 0) try mapped.project(env.arena, rd.cols);
    return mapped;
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

    pub fn lanes(self: *const LaneSplit, threads: usize) usize {
        const n = @max(@as(usize, 1), threads);
        return switch (self.*) {
            .csv => n,
            .parquet => |m| @min(n, m.max_lanes),
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
