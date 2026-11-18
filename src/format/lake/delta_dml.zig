//! DELETE and UPDATE of a Delta table, copy-on-write, each in one commit. The rows
//! they change are found by the caller over `Target.layout`: the table's live files
//! that its conditions may hold rows in, each row carrying its file and its position
//! in it, so the conditions, the FROM source and the subqueries run on the engine as
//! any query's do and say which rows change (`hits`). An UPDATE's new rows are
//! written as they are found (`Writer.put`). Then:
//!   - a file whose partition values hold every condition of a DELETE with no
//!     subquery is removed whole without being read;
//!   - on a table writing deletion vectors (`write.writesDeletionVectors`), each
//!     file holding a changed row is added again with those rows in its vector,
//!     or only removed when none of its rows is left;
//!   - otherwise each such file's other rows are written to new files, through the
//!     sink's partition routing, and the file is removed;
//!   - a table keeping a change data feed gets each deleted row as a `delete`
//!     change, each updated one as its `update_preimage` and `update_postimage`, so
//!     with one every file holding a changed row is read.
//! A file the conditions rule out by its partition values is not read. The commit
//! fails as a conflict when a concurrent writer removed a file it read.

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

pub const Op = enum {
    delete,
    update,

    fn verb(self: Op) []const u8 {
        return if (self == .delete) "DELETE" else "UPDATE";
    }
};

pub const Target = struct {
    op: Op,
    st: *delta.State,
    root: []const u8,
    /// The live files the conditions may hold rows in, by their partition values.
    files: []const delta.Add,
    /// Of `files`, those a DELETE removes whole by their partition values.
    whole: []const delta.Add,
    /// The rest of `files`, read with the table's columns (an UPDATE's with the row
    /// ids it keeps, `width` in all), then each row's file (its place in `match`)
    /// and position in it, `tag` and `pos`.
    match: []const delta.Add,
    layout: *const fit.Layout,
    width: usize,
    tag: usize,
    pos: usize,
};

/// The `op` of `root`'s table at its newest version, refused when basalt does not
/// write the table or it is append-only. `conds` are its conditions on the table,
/// `$` values bound: a file whose partition values make one false or NULL holds no
/// row to change, and one naming another column rules nothing out. `whole_ok` (a
/// DELETE with no subquery) lets a file whose partition values make every one true
/// go unread.
pub fn open(arena: std.mem.Allocator, root: []const u8, op: Op, conds: []const *const ast.Expr, whole_ok: bool) !Target {
    if (write.storeProblem(root)) |why| return refuse("cannot {s} Delta table `{s}`: {s}", .{ if (op == .delete) "delete from" else "update", root, why });
    const st = try delta.snapshot(arena, root, .{}, null);
    try st.checkProtocol();
    const fields = try write.tableFields(arena, st.name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{st.name}));
    try write.writable(st, fields, .append);
    if (st.property("delta.appendOnly")) |v| if (textconv.boolText(.delta_property, v) orelse false)
        return refuse("{s} is append-only (`delta.appendOnly`), so {s} cannot change its rows", .{ st.name, if (op == .delete) "a DELETE" else "an UPDATE" });
    const all = st.files.values();
    const fl = try delta.layoutOf(arena, st, root, all);
    if (op == .update) for (fl.layout.variant_types) |vt| if (vt != null)
        return refuse("{s} has a variant column; an UPDATE of it is not supported yet", .{st.name});
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
        if (every and whole_ok and op == .delete and !feed and merge.fileRows(arena, a.stats) != null) {
            try whole.append(a);
            continue;
        }
        try match.append(a);
        try match_files.append(f);
    }
    var base = fl.layout;
    if (op == .update and keepsRows(st)) base = try delta.withRows(arena, st, base);
    const lay = try tagLayout(arena, base, match_files.items);
    const nf = base.schema.fields.len;
    return .{ .op = op, .st = st, .root = root, .files = files.items, .whole = whole.items, .match = match.items, .layout = lay, .width = nf, .tag = nf, .pos = nf + 1 };
}

pub const Done = struct {
    /// The rows changed, a file a DELETE removed whole counted by its statistics.
    rows: u64,
    /// The files removed, and the ones added (rewritten or updated rows, or a file
    /// added again with its deletion vector).
    removed: usize,
    added: usize,
};

/// The rows `hits` marks, and a DELETE's files removed whole by their statistics.
pub fn rowsOf(arena: std.mem.Allocator, t: *const Target, hits: []const std.DynamicBitSetUnmanaged) !u64 {
    var rows: u64 = 0;
    for (hits) |h| rows += h.count();
    for (t.whole) |f| rows += merge.fileRows(arena, f.stats).? -| try deletedRows(t.st, f);
    return rows;
}

/// The commit of an operation on `Target`'s table: its sink, open from `begin`,
/// taking an UPDATE's new rows (`put`) until `finish` rewrites the files they
/// came from and commits.
pub const Writer = struct {
    t: *const Target,
    arena: std.mem.Allocator,
    sink: *write.Sink,
    guard: Guard(*write.Sink),
    dv: bool,
    feed: bool,
    cdc_schema: *types.Schema,

    /// The writer of `t`'s commit recording `predicate`, through a sink on `gpa`
    /// taking `opts`.
    pub fn begin(gpa: std.mem.Allocator, arena: std.mem.Allocator, t: *const Target, predicate: []const u8, opts: merge.Opts) !*Writer {
        const st = t.st;
        const base = (try delta.layoutOf(arena, st, t.root, st.files.values())).layout;
        const plan_schema = if (keepsRows(st)) (try delta.withRows(arena, st, base)).schema else base.schema;
        const p = try write.plan(arena, t.root, .append, false, null, .snappy, plan_schema);
        p.snap = st;
        p.dml = .{ .op = t.op.verb(), .predicate = predicate };
        const width = plan_schema.fields.len;
        const cdc_fields = try arena.alloc(types.Schema.Field, width + 1);
        @memcpy(cdc_fields[0..width], plan_schema.fields);
        cdc_fields[width] = .{ .name = fit.change_type_name, .ty = types.Type.init(.string).asNullable() };
        const cdc_schema = try arena.create(types.Schema);
        cdc_schema.* = .{ .fields = cdc_fields };
        const sink = try write.Sink.open(gpa, arena, p, opts);
        const self = try arena.create(Writer);
        self.* = .{ .t = t, .arena = arena, .sink = sink, .guard = Guard(*write.Sink).init(sink), .dv = write.writesDeletionVectors(st), .feed = feeds(st), .cdc_schema = cdc_schema };
        return self;
    }

    pub fn abort(self: *Writer) void {
        self.guard.abort();
    }

    /// Writes an UPDATE's new rows, `b` shaped as `Target.layout` with the table's
    /// columns as the update leaves them, and with a change data feed their
    /// `update_postimage` changes. A row's id, where the table keeps them, stays;
    /// its commit version is the commit's.
    pub fn put(self: *Writer, arena: std.mem.Allocator, b: Batch) !void {
        if (b.len == 0) return;
        try self.sink.route(arena, b);
        if (self.feed) try self.changes(arena, b, null, b.len, "update_postimage");
    }

    /// Rewrites the files holding a row `hits` marks (per file of `t.match`, by
    /// position) and commits: those rows gone, and a DELETE's files `t.whole`.
    pub fn finish(self: *Writer, hits: []const std.DynamicBitSetUnmanaged) !Done {
        const t = self.t;
        const arena = self.arena;
        var touched = std.array_list.Managed(delta.Add).init(arena);
        var touched_hits = std.array_list.Managed(std.DynamicBitSetUnmanaged).init(arena);
        for (t.match, hits) |f, h| if (h.count() > 0) {
            try touched.append(f);
            try touched_hits.append(h);
        };
        const rows = try rowsOf(arena, t, hits);
        if (touched.items.len == 0 and t.whole.len == 0) {
            self.guard.abort();
            return .{ .rows = 0, .removed = 0, .added = 0 };
        }
        if (touched.items.len > 0 and (!self.dv or self.feed)) try self.rewrite(touched.items, touched_hits.items);
        var readds: []const merge.ReAdd = &.{};
        if (self.dv) {
            const lists = try arena.alloc([]const u64, touched.items.len);
            for (lists, touched_hits.items) |*l, h| {
                var xs = try std.array_list.Managed(u64).initCapacity(arena, h.count());
                var it = h.iterator(.{});
                while (it.next()) |i| xs.appendAssumeCapacity(i);
                l.* = xs.items;
            }
            readds = try merge.readds(arena, t.st.name, t.root, touched.items, lists);
        }
        const removed = try std.mem.concat(arena, delta.Add, &.{ touched.items, t.whole });
        const read_set = try arena.alloc([]const u8, t.files.len);
        for (read_set, t.files) |*r, f| r.* = f.raw;
        self.sink.dml = .{ .removed = removed, .read_set = read_set, .readds = readds };
        try self.guard.close();
        return .{ .rows = rows, .removed = removed.len, .added = self.sink.n_added + readds.len };
    }

    /// Reads `files` again, their variants raw and, where the table keeps them, their
    /// row ids, and writes each one's rows `hits` leaves (unless the table takes
    /// deletion vectors), and with a change data feed the rows it marks as `delete`
    /// or `update_preimage` changes.
    fn rewrite(self: *Writer, files: []const delta.Add, hits: []const std.DynamicBitSetUnmanaged) !void {
        const t = self.t;
        const st = t.st;
        const fl = try delta.layoutOf(self.arena, st, t.root, files);
        var l = try delta.rawVariants(self.arena, fl.layout);
        if (keepsRows(st)) l = try delta.withRows(self.arena, st, l);
        const lay = try tagLayout(self.arena, l, fl.files);
        const nf = l.schema.fields.len;
        const src = try Folder.openLayout(self.arena, lay, null);
        var mem = std.heap.ArenaAllocator.init(pagecache.process);
        defer mem.deinit();
        const s = src.source();
        defer s.close();
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
            if (!self.dv and n_gone < b.len) {
                const cols = try a.alloc(column.Column, b.columns.len);
                try column.gatherColumns(a, b.columns, cols, kept, b.len - n_gone);
                try self.sink.route(a, .{ .schema = b.schema, .columns = cols, .len = b.len - n_gone });
            }
            if (self.feed and n_gone > 0) try self.changes(a, b, gone, n_gone, if (t.op == .delete) "delete" else "update_preimage");
        }
    }

    /// The rows of `b` that `keep` marks (all when null), `n` of them, written as
    /// changes of type `kind`.
    fn changes(self: *Writer, a: std.mem.Allocator, b: Batch, keep: ?[]const bool, n: usize, kind: []const u8) !void {
        const width = self.cdc_schema.fields.len - 1;
        const cols = try a.alloc(column.Column, width + 1);
        if (keep) |k| try column.gatherColumns(a, b.columns[0..width], cols[0..width], k, n) else @memcpy(cols[0..width], b.columns[0..width]);
        var bd = try column.Builder.initCapacity(a, types.Type.init(.string).asNullable(), n);
        try bd.appendRepeated(.{ .string = kind }, n);
        cols[width] = try bd.finish();
        try self.sink.routeChanges(a, .{ .schema = self.cdc_schema, .columns = cols, .len = n });
    }
};

/// Deletes from `t`'s table the rows `hits` marks and the files `t.whole`, in one
/// commit recording `predicate`.
pub fn delete(gpa: std.mem.Allocator, arena: std.mem.Allocator, t: *const Target, hits: []const std.DynamicBitSetUnmanaged, predicate: []const u8, opts: merge.Opts) !Done {
    const w = try Writer.begin(gpa, arena, t, predicate, opts);
    errdefer w.abort();
    return w.finish(hits);
}

/// Layout `l` over `files`, each row with its file's place among them and its
/// position in the file as two more columns.
fn tagLayout(arena: std.mem.Allocator, l: fit.Layout, files: []const fit.Layout.File) !*fit.Layout {
    const nf = l.schema.fields.len;
    const sfields = try arena.alloc(types.Schema.Field, nf + 2);
    @memcpy(sfields[0..nf], l.schema.fields);
    const int = types.Type.init(.int).asNullable();
    sfields[nf] = .{ .name = tag_name, .ty = int };
    sfields[nf + 1] = .{ .name = pos_name, .ty = int };
    const part_fields = try arena.alloc(usize, l.part_fields.len + 1);
    @memcpy(part_fields[0..l.part_fields.len], l.part_fields);
    part_fields[l.part_fields.len] = nf;
    const out = try arena.alloc(fit.Layout.File, files.len);
    for (out, files, 0..) |*o, f, i| {
        const parts = try arena.alloc(Value, f.parts.len + 1);
        @memcpy(parts[0..f.parts.len], f.parts);
        parts[f.parts.len] = .{ .int = @intCast(i) };
        o.* = f;
        o.parts = parts;
    }
    const lay = try arena.create(fit.Layout);
    lay.* = l;
    lay.schema = .{ .fields = sfields };
    lay.part_fields = part_fields;
    lay.files = out;
    lay.pos_field = nf + 1;
    return lay;
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
