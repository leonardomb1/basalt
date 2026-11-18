//! DELETE FROM a Delta table, copy-on-write, in one commit. The rows to delete are
//! found by the caller over `Target.layout`: the table's live files that its
//! conditions may hold rows in, each row carrying its file and its position in it,
//! so the conditions and subqueries run on the engine as any query's do and say
//! which rows go (`hits`). Then:
//!   - a file whose partition values hold every condition, when there are only
//!     conditions, is removed whole without being read;
//!   - on a table writing deletion vectors (`write.writesDeletionVectors`), each
//!     file holding a deleted row is added again with those rows in its vector,
//!     or only removed when none of its rows is left;
//!   - otherwise each such file's other rows are written to new files, through the
//!     sink's partition routing, and the file is removed;
//!   - a table keeping a change data feed gets each deleted row as a `delete`
//!     change, so with one every file holding a deleted row is read.
//! A file the conditions rule out by its partition values is not read. The commit
//! fails as a conflict when a concurrent writer removed a file the DELETE read.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Folder = @import("../parquet/folder.zig").Folder;
const Value = @import("../../exec/value.zig").Value;
const Guard = @import("../../exec/guard.zig").Guard;
const ast = @import("../../lang/ast.zig");
const column = @import("../../exec/column.zig");
const delta = @import("delta.zig");
const dvs = @import("delta_dv.zig");
const fit = @import("../parquet/fit.zig");
const merge = @import("delta_merge.zig");
const pagecache = @import("../../exec/pagecache.zig");
const table = @import("table.zig");
const textconv = @import("../../exec/textconv.zig");
const types = @import("../../lang/types.zig");
const vec = @import("../../exec/eval/vec.zig");
const write = @import("delta_write.zig");
const keepsRows = @import("delta_maint.zig").keepsRows;

const refuse = table.refuse;

pub const tag_name = "\x00file";
pub const pos_name = "\x00pos";

pub const Target = struct {
    st: *delta.State,
    root: []const u8,
    /// The live files the conditions may hold rows in, by their partition values.
    files: []const delta.Add,
    /// Of `files`, those whose partition values hold every condition, deleted whole.
    whole: []const delta.Add,
    /// The rest of `files`, read with each row's file (its place in `match`) and
    /// position in it as the last two columns, `tag` and `pos`.
    match: []const delta.Add,
    layout: *const fit.Layout,
    tag: usize,
    pos: usize,
};

/// The DELETE of `root`'s table at its newest version, refused when basalt does not
/// write the table or it is append-only. `conds` are its conditions on the table,
/// `$` values bound: a file whose partition values make one false or NULL holds no
/// row to delete, and one naming another column rules nothing out. `whole_ok` (no
/// subquery) lets a file whose partition values make every one true go unread.
pub fn open(arena: std.mem.Allocator, root: []const u8, conds: []const *const ast.Expr, whole_ok: bool) !Target {
    if (write.storeProblem(root)) |why| return refuse("cannot delete from Delta table `{s}`: {s}", .{ root, why });
    const st = try delta.snapshot(arena, root, .{}, null);
    try st.checkProtocol();
    const fields = try write.tableFields(arena, st.name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{st.name}));
    try write.writable(st, fields, .append);
    if (st.property("delta.appendOnly")) |v| if (textconv.boolText(.delta_property, v) orelse false)
        return refuse("{s} is append-only (`delta.appendOnly`), so a DELETE cannot remove its rows", .{st.name});
    const all = st.files.values();
    const fl = try delta.layoutOf(arena, st, root, all);
    const feed = feeds(st);
    var files = std.array_list.Managed(delta.Add).init(arena);
    var whole = std.array_list.Managed(delta.Add).init(arena);
    var match = std.array_list.Managed(delta.Add).init(arena);
    var match_files = std.array_list.Managed(fit.Layout.File).init(arena);
    for (all, fl.files) |a, f| {
        var every = true;
        const out = for (conds) |e| {
            const v = vec.constEval(arena, e, st.part_cols, f.parts) catch {
                every = false;
                continue;
            };
            if (v == .null or (v == .bool and !v.bool)) break true;
            if (v != .bool) every = false;
        } else false;
        if (out) continue;
        try files.append(a);
        if (every and whole_ok and !feed and merge.fileRows(arena, a.stats) != null) {
            try whole.append(a);
            continue;
        }
        try match.append(a);
        try match_files.append(f);
    }
    const base = fl.layout;
    const nf = base.schema.fields.len;
    const sfields = try arena.alloc(types.Schema.Field, nf + 2);
    @memcpy(sfields[0..nf], base.schema.fields);
    const int = types.Type.init(.int).asNullable();
    sfields[nf] = .{ .name = tag_name, .ty = int };
    sfields[nf + 1] = .{ .name = pos_name, .ty = int };
    const lay = try arena.create(fit.Layout);
    lay.* = base;
    lay.schema = .{ .fields = sfields };
    lay.part_fields = try withTag(arena, base.part_fields, nf);
    lay.files = try tagged(arena, match_files.items);
    lay.pos_field = nf + 1;
    return .{ .st = st, .root = root, .files = files.items, .whole = whole.items, .match = match.items, .layout = lay, .tag = nf, .pos = nf + 1 };
}

pub const Deleted = struct {
    /// The rows deleted, counted from the files' statistics for a file removed whole.
    rows: u64,
    /// The files removed, and the ones added in their place (rewritten rows, or
    /// a file added again with its deletion vector).
    removed: usize,
    added: usize,
};

/// The rows a DELETE of `t` with `hits` deletes, a file removed whole counted by its
/// statistics.
pub fn rowsOf(arena: std.mem.Allocator, t: *const Target, hits: []const std.DynamicBitSetUnmanaged) !u64 {
    var rows: u64 = 0;
    for (hits) |h| rows += h.count();
    for (t.whole) |f| rows += merge.fileRows(arena, f.stats).? -| try deletedRows(t.st, f);
    return rows;
}

/// Deletes from `t`'s table the rows `hits` marks (per file of `t.match`, by
/// position) and the files `t.whole`, in one commit recording `predicate`, written
/// through a sink on `gpa` taking `opts`.
pub fn apply(gpa: std.mem.Allocator, arena: std.mem.Allocator, t: *const Target, hits: []const std.DynamicBitSetUnmanaged, predicate: []const u8, opts: merge.Opts) !Deleted {
    var touched = std.array_list.Managed(delta.Add).init(arena);
    var touched_hits = std.array_list.Managed(std.DynamicBitSetUnmanaged).init(arena);
    for (t.match, hits) |f, h| if (h.count() > 0) {
        try touched.append(f);
        try touched_hits.append(h);
    };
    const rows = try rowsOf(arena, t, hits);
    if (touched.items.len == 0 and t.whole.len == 0) return .{ .rows = 0, .removed = 0, .added = 0 };

    const st = t.st;
    const keeps_rows = keepsRows(st);
    const dv = write.writesDeletionVectors(st);
    const feed = feeds(st);
    const base = (try delta.layoutOf(arena, st, t.root, st.files.values())).layout;
    const plan_schema = if (keeps_rows) (try delta.withRows(arena, st, base)).schema else base.schema;
    const p = try write.plan(arena, t.root, .append, false, null, .snappy, plan_schema);
    p.snap = st;
    p.delete = predicate;
    const sink = try write.Sink.open(gpa, arena, p, opts);
    var g = Guard(*write.Sink).init(sink);
    defer g.abort();

    if (touched.items.len > 0 and (!dv or feed)) try rewrite(arena, t, sink, touched.items, touched_hits.items, plan_schema.fields.len, dv, feed);
    var readds: []const merge.ReAdd = &.{};
    if (dv) {
        const lists = try arena.alloc([]const u64, touched.items.len);
        for (lists, touched_hits.items) |*l, h| {
            var xs = try std.array_list.Managed(u64).initCapacity(arena, h.count());
            var it = h.iterator(.{});
            while (it.next()) |i| xs.appendAssumeCapacity(i);
            l.* = xs.items;
        }
        readds = try merge.readds(arena, st.name, t.root, touched.items, lists);
    }
    const removed = try std.mem.concat(arena, delta.Add, &.{ touched.items, t.whole });
    const read_set = try arena.alloc([]const u8, t.files.len);
    for (read_set, t.files) |*r, f| r.* = f.raw;
    sink.deleting = .{ .removed = removed, .read_set = read_set, .readds = readds };
    try g.close();
    return .{ .rows = rows, .removed = removed.len, .added = sink.n_added + readds.len };
}

/// Reads `files` again, their variants raw and, where the table keeps them, their
/// row ids, and writes each one's rows `hits` leaves (unless the table takes
/// deletion vectors), and with a change data feed the rows it marks as `delete`
/// changes.
fn rewrite(arena: std.mem.Allocator, t: *const Target, sink: *write.Sink, files: []const delta.Add, hits: []const std.DynamicBitSetUnmanaged, width: usize, dv: bool, feed: bool) !void {
    const st = t.st;
    const fl = try delta.layoutOf(arena, st, t.root, files);
    var l = try delta.rawVariants(arena, fl.layout);
    if (keepsRows(st)) l = try delta.withRows(arena, st, l);
    const nf = l.schema.fields.len;
    const sfields = try arena.alloc(types.Schema.Field, nf + 2);
    @memcpy(sfields[0..nf], l.schema.fields);
    const int = types.Type.init(.int).asNullable();
    sfields[nf] = .{ .name = tag_name, .ty = int };
    sfields[nf + 1] = .{ .name = pos_name, .ty = int };
    const lay = try arena.create(fit.Layout);
    lay.* = l;
    lay.schema = .{ .fields = sfields };
    lay.part_fields = try withTag(arena, l.part_fields, nf);
    lay.files = try tagged(arena, fl.files);
    lay.pos_field = nf + 1;

    const src = try Folder.openLayout(arena, lay, null);
    var mem = std.heap.ArenaAllocator.init(pagecache.process);
    defer mem.deinit();
    const s = src.source();
    defer s.close();
    const cdc_fields = try arena.alloc(types.Schema.Field, width + 1);
    @memcpy(cdc_fields[0..width], sfields[0..width]);
    cdc_fields[width] = .{ .name = fit.change_type_name, .ty = types.Type.init(.string).asNullable() };
    const cdc_schema = try arena.create(types.Schema);
    cdc_schema.* = .{ .fields = cdc_fields };
    while (try s.next(mem.allocator())) |b| {
        defer _ = mem.reset(.retain_capacity);
        const a = mem.allocator();
        const tags = b.columns[nf];
        const pos = b.columns[nf + 1];
        const gone = try a.alloc(bool, b.len);
        const kept = try a.alloc(bool, b.len);
        var n_gone: usize = 0;
        for (gone, kept, 0..) |*x, *k, r| {
            const h = hits[@intCast(tags.getValue(r).int)];
            const i: usize = @intCast(pos.getValue(r).int);
            x.* = i < h.bit_length and h.isSet(i);
            k.* = !x.*;
            n_gone += @intFromBool(x.*);
        }
        if (!dv and n_gone < b.len) {
            const cols = try a.alloc(column.Column, b.columns.len);
            try column.gatherColumns(a, b.columns, cols, kept, b.len - n_gone);
            try sink.route(a, .{ .schema = b.schema, .columns = cols, .len = b.len - n_gone });
        }
        if (feed and n_gone > 0) {
            const cols = try a.alloc(column.Column, width + 1);
            try column.gatherColumns(a, b.columns[0..width], cols[0..width], gone, n_gone);
            var bd = try column.Builder.initCapacity(a, types.Type.init(.string).asNullable(), n_gone);
            try bd.appendRepeated(.{ .string = "delete" }, n_gone);
            cols[width] = try bd.finish();
            try sink.routeChanges(a, .{ .schema = cdc_schema, .columns = cols, .len = n_gone });
        }
    }
}

/// `part_fields` with the file's tag, field `tag`, as one more value fixed per file.
fn withTag(arena: std.mem.Allocator, part_fields: []const usize, tag: usize) ![]const usize {
    const out = try arena.alloc(usize, part_fields.len + 1);
    @memcpy(out[0..part_fields.len], part_fields);
    out[part_fields.len] = tag;
    return out;
}

/// `files` each with its place among them as one more partition value.
fn tagged(arena: std.mem.Allocator, files: []const fit.Layout.File) ![]const fit.Layout.File {
    const out = try arena.alloc(fit.Layout.File, files.len);
    for (out, files, 0..) |*o, f, i| {
        const parts = try arena.alloc(Value, f.parts.len + 1);
        @memcpy(parts[0..f.parts.len], f.parts);
        parts[f.parts.len] = .{ .int = @intCast(i) };
        o.* = f;
        o.parts = parts;
    }
    return out;
}

/// The rows file `f`'s deletion vector already deletes.
fn deletedRows(st: *const delta.State, f: delta.Add) !u64 {
    const d = f.dv orelse return 0;
    return (try dvs.descriptor(st.name, f.path, d)).cardinality;
}

/// Whether `st`'s table keeps a change data feed (`delta.enableChangeDataFeed`).
fn feeds(st: *const delta.State) bool {
    const v = st.property("delta.enableChangeDataFeed") orelse return false;
    return textconv.boolText(.delta_property, v) orelse false;
}
