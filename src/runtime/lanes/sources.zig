//! How a read divides into morsels (`morsels.zig`): a Parquet file's or folder's row
//! groups or files, a mapped CSV, NDJSON or record file; and a lane source over one
//! open Parquet reader.

const Env = @import("../env.zig").Env;
const PqItem = @import("morsels.zig").PqItem;
const Morsels = @import("morsels.zig").Morsels;
const Batch = @import("../../exec/batch.zig").Batch;
const Slicer = @import("../../exec/batch.zig").Slicer;
const RunOptions = @import("../env.zig").RunOptions;
const analyze = @import("../analyze.zig");
const ast = @import("../../lang/ast.zig");
const connect_mod = @import("../connect.zig");
const csv = @import("../../format/csv.zig");
const driver = @import("../../connect/driver.zig");
const filterBounds = @import("../plan.zig").filterBounds;
const lake = @import("../../format/lake/table.zig");
const planErr = @import("../env.zig").planErr;
const planErrT = @import("../env.zig").planErrT;
const pqdecode = @import("../../format/parquet/read.zig");
const projectedColumns = @import("../plan.zig").projectedColumns;
const schemaPtr = @import("../env.zig").schemaPtr;
const sftp = @import("../../store/sftp.zig");
const table_builtins = @import("../../connect/table_builtins.zig");
const builtinArgs = @import("../connect/source.zig").builtinArgs;
const std = @import("std");
const msgfmt = @import("../../format/msgfmt.zig");
const types = @import("../../lang/types.zig");

/// One open Parquet reader as a lane source, its row groups handed on in
/// `Batch.max_rows` slices. `own` says whether closing the source closes the reader
/// and gives the slicer's rows back; a lane that keeps its reader open across items
/// (a morsel worker, a join build lane) points `r` at each in turn and keeps the
/// slicer's pages warm across them (a fresh slicer per row group faulted its pages in
/// anew, and a -j 1 scan ran 2x slower), releasing it when it finishes.
pub const ReaderSource = struct {
    r: *pqdecode.Reader,
    schema_: *const types.Schema,
    own: bool = true,
    slicer: Slicer = .{},

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        return self.schema_.*;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        const b = try self.slicer.read(arena, self.r);
        if (b == null and self.own) self.slicer.deinit();
        return b;
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *ReaderSource = @ptrCast(@alignCast(ptr));
        if (!self.own) {
            self.slicer.pend = null;
            return;
        }
        self.r.close();
        self.slicer.deinit();
    }
    pub const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
};

/// Open `rd`'s parquet input as splittable, or null. Local files split at row groups,
/// each footer checked here; a remote folder splits at files; a single remote file
/// splits at row groups when it has two or more and its server honours range
/// requests, each lane on its own connection over the footer read here.
/// `push_stages` stops before a map path's join, whose names are the join's.
pub fn parquetSplit(env: *Env, rd: ast.Read, hints: []const ast.Hint, push_stages: []const ast.Stage, w: ast.Write, opts: RunOptions) anyerror!?Morsels {
    const arena = env.arena;
    const path = switch (rd.form) {
        .path => |p| p,
        else => return null,
    };
    if (w.mode == .upsert and w.mode.upsert.keys.len == 0) return null;

    var files: []const []const u8 = &.{};
    var root: ?[]const u8 = null;
    if (lake.isTablePath(path)) {
        const fr = (try connect_mod.resolveFolder(env, path, hints)) orelse return null;
        if (fr.kind != .parquet) return null;
        if (fr.table) |t| return tableSplit(env, t.layout, path, rd, push_stages, opts.lanes());
        files = fr.files;
        root = path;
    } else {
        const one = try arena.alloc([]const u8, 1);
        one[0] = path;
        files = one;
    }
    const remote = csv.CsvReader.isUrl(path);

    const project = try readColumns(env, rd, push_stages);
    const bounds = try filterBounds(env, push_stages);
    const probe = pqdecode.Reader.openProjected(arena, files[0], project) catch return null;
    var hold = false;
    defer if (!hold) probe.close();
    var schema = probe.schema;
    if (root != null) {
        const fields = try arena.alloc(types.Schema.Field, schema.fields.len);
        for (fields, schema.fields) |*f, x| f.* = .{ .name = x.name, .ty = x.ty.asNullable() };
        schema = .{ .fields = fields };
    }

    var items = std.array_list.Managed(PqItem).init(arena);
    var like: ?*const pqdecode.Reader = null;
    if (remote and root == null) {
        if (!probe.src.rangesHonoured() or probe.md.row_groups.len < 2) {
            if (env.pipe.pq_held) |h| h.src.close();
            env.pipe.pq_held = .{ .path = files[0], .src = probe.src };
            hold = true;
            return null;
        }
        for (0..probe.md.row_groups.len) |g| try items.append(.{ .file = 0, .rg = @intCast(g), .rg_end = @intCast(g + 1) });
        like = probe;
    } else if (remote) {
        if (files.len < 2) return null;
        for (0..files.len) |k| try items.append(.{ .file = @intCast(k), .rg = 0, .rg_end = null });
    } else {
        for (files, 0..) |f, k| {
            const r = if (k == 0) probe else pqdecode.Reader.openProjected(arena, f, project) catch |e|
                return planErrT(env.diag, e, try msgfmt.allocPrint(arena, "could not read parquet `{s}` ({s})", .{ f, @errorName(e) }));
            defer if (k != 0) r.close();
            if (k != 0) if (pqdecode.schemaMismatch(arena, schema, r.schema, project)) |why|
                return planErr(env.diag, pqdecode.mismatchMessage(arena, root.?, f, files[0], why));
            for (0..r.md.row_groups.len) |g| try items.append(.{ .file = @intCast(k), .rg = @intCast(g), .rg_end = @intCast(g + 1) });
        }
        if (items.items.len < 2) return null;
        if (root == null) like = probe;
    }
    if (root != null) env.forgetFolder(path);
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
        .max_lanes = if (!remote) std.math.maxInt(usize) else if (sftp.isUrl(path)) 4 else 8,
        .check_in_lane = remote and root != null,
        .like = like,
    } };
}

/// The columns a split read decodes: what the stages after it provably take, else
/// the read's own `cols` (a joined side narrowed to what follows it), else all.
fn readColumns(env: *Env, rd: ast.Read, push_stages: []const ast.Stage) !?[][]const u8 {
    if (try projectedColumns(env, push_stages)) |p| return p;
    return if (rd.cols.len > 0) @constCast(rd.cols) else null;
}

/// Live files per lane at which a local table splits at files rather than row
/// groups: planning then opens no footer per file, and lanes stay busy.
const files_per_lane = 8;

/// A table's live files split at files when they are remote or many (`files_per_lane`
/// for each lane), each opened only by its lane, the first here for the scan's
/// column counts; otherwise at row groups, each file opened here so one that does
/// not fit the table fails the plan. A file its partition values rule out is left out.
fn tableSplit(env: *Env, layout: *const pqdecode.Layout, path: []const u8, rd: ast.Read, push_stages: []const ast.Stage, lanes: usize) anyerror!?Morsels {
    const arena = env.arena;
    const project = try readColumns(env, rd, push_stages);
    const bounds = try filterBounds(env, push_stages);
    const view = try layout.view(arena, project);
    var live: usize = 0;
    for (layout.files, 0..) |_, k| {
        if (!layout.pruned(k, bounds)) live += 1;
    }
    const remote = csv.CsvReader.isUrl(path);
    const by_file = remote or live >= lanes *| files_per_lane;
    var items = std.array_list.Managed(PqItem).init(arena);
    var noted = false;
    for (layout.files, 0..) |_, k| {
        if (layout.pruned(k, bounds)) continue;
        if (by_file) try items.append(.{ .file = @intCast(k), .rg = 0, .rg_end = null });
        if (by_file and (remote or noted)) continue;
        const r = (try layout.open(arena, view, k, &.{})).?;
        defer r.close();
        if (!noted) {
            noted = true;
            if (env.scan) |t| {
                driver.ScanTally.add(&t.columns_read, r.leaves.len);
                driver.ScanTally.add(&t.columns_total, r.md.leafCount());
            }
        }
        if (by_file) continue;
        for (0..r.md.row_groups.len) |g| try items.append(.{ .file = @intCast(k), .rg = @intCast(g), .rg_end = @intCast(g + 1) });
    }
    if (items.items.len < 2) return null;
    env.forgetFolder(path);
    return .{ .parquet = .{
        .tally = env.scan,
        .files = try layout.paths(arena),
        .root = path,
        .items = items.items,
        .project = project,
        .bounds = bounds,
        .src_schema = &view.schema,
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
