//! Delta table maintenance, run by `CALL`: `vacuum` deletes the files no version
//! within the retention still needs, `optimize` compacts small data files.
//!
//! VACUUM keeps every file the table's newest version reads (its live data files
//! and their deletion vectors) and every file a `remove` still within the
//! retention names, as a reader of an older version may still open it; of the
//! rest, a file older than the retention is deleted. The retention is the
//! table's `delta.deletedFileRetentionDuration` (a week when unset), or a longer
//! one the call asks for; a shorter one is refused, as Spark refuses it, since a
//! concurrent writer's files not yet committed are as new as the retention lets
//! them be. A file's age is its modification time, a local file's from the file
//! system and an object's from its `Last-Modified`. The log (`_delta_log/`) and
//! every path a part of which starts with `_` or `.` are never touched. Nothing
//! is committed: VACUUM changes no version, only which old ones stay readable.
//!
//! OPTIMIZE compacts a partition's small files (under the writer's 128 MiB roll)
//! into as few as the roll allows: of the live files a `where` predicate on the
//! partition columns keeps (all when none), a partition holding two or more small
//! files, or one file with a deletion vector, has those rewritten, their deleted
//! rows dropped. The commit removes the files read and adds the new ones, every
//! action with `dataChange` false, as the table's rows are unchanged; it fails as
//! a conflict only when a concurrent writer removed a file it read, so appends
//! carry on beside it.

const std = @import("std");
const delta = @import("delta.zig");
const ckpt = @import("delta_checkpoint.zig");
const dvs = @import("delta_dv.zig");
const write = @import("delta_write.zig");
const table = @import("table.zig");
const folder = @import("../../connect/folder.zig");
const http_client = @import("../../net/http_client.zig");
const objstore = @import("../../store/objstore.zig");
const msgfmt = @import("../msgfmt.zig");
const merge = @import("delta_merge.zig");
const Folder = @import("../parquet/folder.zig").Folder;
const Guard = @import("../../connect/guard.zig").Guard;
const ast = @import("../../lang/ast.zig");
const sql_parser = @import("../../lang/sql_parser.zig");
const vec = @import("../../exec/eval/vec.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const pagecache = @import("../../exec/pagecache.zig");
const testing = std.testing;

const refuse = table.refuse;

/// What a VACUUM did: the files it deleted, and the files it kept as referenced.
pub const Vacuumed = struct { deleted: usize, kept: usize };

/// VACUUM of `root`'s table: the retention `retain_ms` (null: the table's), the
/// clock `now_ms`.
pub fn vacuum(arena: std.mem.Allocator, root: []const u8, retain_ms: ?i64, now_ms: i64) !Vacuumed {
    if (write.storeProblem(root)) |why| return refuse("cannot vacuum Delta table `{s}`: {s}", .{ root, why });
    const st = try delta.snapshotWholeAt(arena, root, .{});
    try st.checkProtocol();
    const fields = try write.tableFields(arena, st.name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{st.name}));
    try write.writable(st, fields, .append);
    const table_ms = ckpt.retentionMs(st.property("delta.deletedFileRetentionDuration"));
    const keep_ms = retain_ms orelse table_ms;
    if (keep_ms < table_ms)
        return refuse("VACUUM of {s} keeps files for {d} hours, less than the table's `delta.deletedFileRetentionDuration` of {d} hours; a writer's files not yet committed may be that new, so set the property lower to vacuum closer", .{ st.name, @divTrunc(keep_ms, 3_600_000), @divTrunc(table_ms, 3_600_000) });

    var needed = std.StringHashMapUnmanaged(void).empty;
    const whole = st.whole.?;
    for (whole.adds.values()) |a| try need(arena, &needed, st, root, a);
    const cutoff = now_ms -| keep_ms;
    for (whole.removes.values()) |r| {
        const at = if (r.object.get("deletionTimestamp")) |t| (if (t == .integer) t.integer else 0) else 0;
        if (at > cutoff) try need(arena, &needed, st, root, r);
    }

    const listed = try folder.listRaw(arena, root);
    var client: ?std.http.Client = null;
    defer if (client) |*c| c.deinit();
    var out = Vacuumed{ .deleted = 0, .kept = 0 };
    for (listed) |u| {
        const rel = folder.below(u, root);
        if (hidden(rel)) continue;
        if (needed.contains(rel)) {
            out.kept += 1;
            continue;
        }
        const remote = objstore.isUrl(u);
        if (remote and client == null) client = http_client.initClient(arena);
        const made = if (remote) (try (try objstore.parse(arena, u)).modifiedMs(arena, &client.?)) orelse continue else blk: {
            const s = std.fs.cwd().statFile(u) catch |e| if (e == error.FileNotFound) continue else return e;
            break :blk @as(i64, @intCast(@divTrunc(s.mtime, std.time.ns_per_ms)));
        };
        if (made > cutoff) continue;
        if (remote) try (try objstore.parse(arena, u)).delete(arena, &client.?) else std.fs.cwd().deleteFile(u) catch |e| if (e != error.FileNotFound) return e;
        out.deleted += 1;
    }
    return out;
}

/// What an OPTIMIZE did: the files it read and removed, the files it wrote, the
/// partitions it compacted.
pub const Optimized = struct { removed: usize, added: usize, partitions: usize };

/// OPTIMIZE of `root`'s table, its files kept by `where` (SQL over the partition
/// columns; null: every file), written through a sink on `gpa` taking `opts`.
pub fn optimize(gpa: std.mem.Allocator, arena: std.mem.Allocator, root: []const u8, where: ?[]const u8, opts: merge.Opts) !Optimized {
    if (write.storeProblem(root)) |why| return refuse("cannot optimize Delta table `{s}`: {s}", .{ root, why });
    const st = try delta.snapshot(arena, root, .{}, null);
    try st.checkProtocol();
    const fields = try write.tableFields(arena, st.name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{st.name}));
    try write.writable(st, fields, .append);
    for (st.writer_features) |f| if (std.mem.eql(u8, f, "clustering"))
        return refuse("{s} is liquid-clustered (writer feature `clustering`); its OPTIMIZE clusters as it compacts, which basalt does not, and compacting its clustered files into unclustered ones would undo that, so run OPTIMIZE there", .{st.name});
    const all = st.files.values();
    if (all.len == 0) return .{ .removed = 0, .added = 0, .partitions = 0 };
    const fl = try delta.layoutOf(arena, st, root, all);
    const pred: ?*ast.Expr = if (where) |t| blk: {
        var d: sql_parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
        break :blk sql_parser.parseExprStr(arena, t, &d) catch
            return refuse("`delta_optimize`'s predicate `{s}` is not an expression: {s}", .{ t, d.msg });
    } else null;
    const names = st.part_cols;
    var groups = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(usize)).empty;
    for (fl.files, all, 0..) |f, a, i| {
        if (pred) |e| {
            const v = vec.constEval(arena, e, names, f.parts) catch
                return refuse("`delta_optimize`'s predicate `{s}` may name only {s}'s partition columns ({s})", .{ where.?, st.name, if (names.len == 0) "it has none" else try std.mem.join(arena, ", ", names) });
            if (!(v == .bool and v.bool)) continue;
        }
        const small = (a.size orelse 0) < write.roll_bytes or a.dv != null;
        if (!small) continue;
        var key = std.array_list.Managed(u8).init(arena);
        for (f.parts) |pv| {
            try key.appendSlice(try fmtnum.valueToString(arena, pv));
            try key.append(0);
        }
        const g = try groups.getOrPut(arena, key.items);
        if (!g.found_existing) g.value_ptr.* = .empty;
        try g.value_ptr.append(arena, i);
    }
    var picks = std.array_list.Managed(delta.Add).init(arena);
    var parts: usize = 0;
    for (groups.values()) |g| {
        const one_dv = g.items.len == 1 and all[g.items[0]].dv != null;
        if (g.items.len < 2 and !one_dv) continue;
        parts += 1;
        for (g.items) |i| try picks.append(all[i]);
    }
    if (picks.items.len == 0) return .{ .removed = 0, .added = 0, .partitions = 0 };

    const keeps_rows = keepsRows(st);
    const read_lay = blk: {
        const l = (try delta.layoutOf(arena, st, root, picks.items)).layout;
        break :blk if (keeps_rows) try delta.withRows(arena, st, l) else l;
    };
    const plan_schema = if (keeps_rows) (try delta.withRows(arena, st, fl.layout)).schema else fl.layout.schema;
    const p = try write.plan(arena, root, .append, false, null, .snappy, plan_schema);
    p.optimize = true;
    const sink = try write.Sink.open(gpa, arena, p, opts);
    var g = Guard(*write.Sink).init(sink);
    defer g.abort();
    const lay_ptr = try arena.create(@TypeOf(read_lay));
    lay_ptr.* = read_lay;
    const src = try Folder.openLayout(arena, lay_ptr, null);
    var batch_mem = std.heap.ArenaAllocator.init(pagecache.process);
    defer batch_mem.deinit();
    const s = src.source();
    defer s.close();
    while (try s.next(batch_mem.allocator())) |b| {
        try sink.writeBatch(batch_mem.allocator(), b);
        _ = batch_mem.reset(.retain_capacity);
    }
    sink.compacting = picks.items;
    try g.close();
    return .{ .removed = picks.items.len, .added = sink.n_added, .partitions = parts };
}

/// Whether `st`'s table keeps its rows' ids through a rewrite: writer feature
/// `rowTracking` with `delta.enableRowTracking`.
fn keepsRows(st: *const delta.State) bool {
    const on = for (st.writer_features) |f| {
        if (std.mem.eql(u8, f, "rowTracking")) break true;
    } else false;
    if (!on) return false;
    const t = st.property("delta.enableRowTracking") orelse return false;
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, t, " "), "true");
}

/// Puts the files action `a` (an `add` or `remove`) names in `set`, as paths
/// below `root`: its data file and its deletion vector's file.
fn need(arena: std.mem.Allocator, set: *std.StringHashMapUnmanaged(void), st: *const delta.State, root: []const u8, a: std.json.Value) !void {
    if (a != .object) return;
    const p = a.object.get("path") orelse return;
    if (p != .string) return;
    const decoded = try table.percentDecode(arena, p.string);
    try set.put(arena, try below(arena, root, decoded), {});
    const dv = a.object.get("deletionVector") orelse return;
    if (dv != .object) return;
    const d = dvs.descriptor(st.name, decoded, dv) catch return;
    if (d.storage == 'i') return;
    try set.put(arena, try below(arena, root, try dvs.location(arena, root, d)), {});
}

/// A path the log names as a path below `root`: as written when relative, with
/// the root cut off when absolute.
fn below(arena: std.mem.Allocator, root: []const u8, path: []const u8) ![]const u8 {
    const full = table.mapPath(arena, root, path) catch return path;
    return folder.below(full, root);
}

/// Whether a path below the root is one VACUUM never deletes: in the log, or with
/// any part starting `_` or `.`.
fn hidden(rel: []const u8) bool {
    var it = std.mem.splitAny(u8, rel, "/\\");
    while (it.next()) |part| if (part.len > 0 and (part[0] == '_' or part[0] == '.')) return true;
    return false;
}

test "VACUUM never touches the log or a hidden path" {
    try testing.expect(hidden("_delta_log/00000000000000000000.json"));
    try testing.expect(hidden("x=1/.part.crc"));
    try testing.expect(hidden("_change_data/c.parquet"));
    try testing.expect(!hidden("x=1/part-0.parquet"));
    try testing.expect(!hidden("deletion_vector_1.bin"));
}
