//! A Delta table as a LOAD target: rows become Parquet data files under the table's
//! root, then one commit (`delta_log.zig`) adds them, and for a REPLACE removes
//! every file live at the version read. A table is created, at version 0 with
//! protocol (1, 2) and its `metaData`, when `format = 'delta'` names a folder
//! holding no `_delta_log/`; `partition_by` partitions it then, and a later write
//! takes the table's own partition columns.
//!
//! Writing an existing table first replays its log (`delta.snapshot`) and refuses
//! what basalt would not honour: a reader feature it does not read, a writer
//! feature by name, a table property or column metadata that asks the writer to
//! enforce something (invariants, CHECK constraints, generated or identity
//! columns), `delta.appendOnly` under a REPLACE, and rows whose columns are not the
//! table's (by name, any order; no column added or dropped) or whose types differ.
//! A value is written as the table types it: an `integer` column as INT32 with its
//! range checked, a `float` as FLOAT, a `decimal(p,s)` at its scale, a `timestamp`
//! as UTC-adjusted microseconds and a `timestamp_ntz` as local ones. A struct, an
//! array or a map is written as a nested Parquet column, each leaf laid out as the
//! table's primitive there has it; a new table types one as Delta's struct, array
//! or map, every part nullable, and the rows must have an existing one's shape.
//!
//! A partitioned table's rows are routed by their partition values, each to a
//! file of its own under Hive-style `col=value/` folders (Spark's escaping, with
//! `__HIVE_DEFAULT_PARTITION__` for NULL); partition columns are not written into
//! the files, as the log carries them. At most `max_open` files are open at once,
//! the least recently written closed when another is needed, and a file is closed
//! past `roll_bytes`, so the data's memory is bounded by `max_open` Parquet row
//! groups (and an object store's upload buffer each), whatever the number of
//! partitions; the commit, built in memory, grows by an `add` line per file. A
//! local file is synced once closed, and the folders holding them before the
//! commit; a failed write deletes its local files and the folders it made.
//! Each `add` carries the file's statistics: `numRecords`, and per data column
//! `nullCount`, with `minValues` and `maxValues` where they are exact and cheap
//! (numbers, dates, timestamps cut to milliseconds as the protocol allows, strings
//! of up to 32 bytes; a longer string's minimum is its 32-byte prefix). A struct's
//! fields have theirs, nested as objects under the struct's name, as Spark writes
//! them; a list or map, and what lies inside one, has none.
//!
//! An UPSERT matches rows by its key columns and rewrites the files holding a
//! matched row (`delta_merge.zig`); under PARTIAL COLS the rows may leave out
//! table columns other than the key and the listed ones.
//!
//! Under `USING changes` the UPSERT applies a change feed (`delta_merge.zig`):
//! `Plan.change` says where the hidden change columns are among the incoming ones,
//! the application the position is kept under, the position the table holds for
//! it (null: none yet) and the position column's kind; the columns the feed's
//! arguments read that the table lacks are not written, and the commit carries
//! the batch's largest position as a `txn` action, a batch of no rows committing
//! nothing.
//!
//! A commit landing at a version the table checkpoints at (every tenth, or
//! `delta.checkpointInterval`) is followed by a checkpoint (`delta_checkpoint.zig`);
//! one that cannot be written is a warning, as the commit stands.

const std = @import("std");
const pagecache = @import("../../exec/pagecache.zig");
const msgfmt = @import("../msgfmt.zig");
const textconv = @import("../../exec/textconv.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const codecs = @import("../compress/codecs.zig");
const column = @import("../../exec/column.zig");
const delta = @import("delta.zig");
const dlog = @import("delta_log.zig");
const ckpt = @import("delta_checkpoint.zig");
const dtypes = @import("delta_types.zig");
const merge = @import("delta_merge.zig");
const dvs = @import("delta_dv.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const decimal = @import("../../exec/decimal.zig");
const folder = @import("../../connect/folder.zig");
const jsontext = @import("../../exec/jsontext.zig");
const objstore = @import("../../store/objstore.zig");
const pqwrite = @import("../parquet/write.zig");
const fit = @import("../parquet/fit.zig");
const percent = @import("../../net/percent.zig");
const ast = @import("../../lang/ast.zig");
const azure = @import("../../store/azure.zig");
const s3 = @import("../../store/s3.zig");
const table = @import("table.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

const refuse = table.refuse;
const vcol = @import("variant_col.zig");

pub const Mode = union(enum) {
    append,
    replace,
    upsert: Upsert,

    /// How EXPLAIN names it: `append`, `replace`, `upsert on (id) partial cols (a)`.
    pub fn label(self: Mode, arena: std.mem.Allocator) ![]const u8 {
        const u = switch (self) {
            .upsert => |u| u,
            else => return @tagName(self),
        };
        const keys = try std.mem.join(arena, ", ", u.keys);
        const on = if (u.keys.len == 0) "upsert (key from the source)" else try msgfmt.allocPrint(arena, "upsert on ({s})", .{keys});
        const pc = u.partial orelse return on;
        return msgfmt.allocPrint(arena, "{s} partial cols ({s})", .{ on, try std.mem.join(arena, ", ", pc) });
    }
};

/// An UPSERT's key columns (none until a bare UPSERT's are resolved) and, under
/// PARTIAL COLS, the columns a matched row takes from the incoming one.
pub const Upsert = struct {
    keys: []const []const u8,
    partial: ?[]const []const u8 = null,
};

/// The disposition a LOAD names, as a Delta write takes it.
pub fn modeOf(m: ast.WriteMode) Mode {
    return switch (m) {
        .default, .append => .append,
        .overwrite => .replace,
        .upsert => |u| .{ .upsert = .{ .keys = u.keys, .partial = u.partial } },
    };
}

/// Whether incoming column `name` is row tracking's, which the table keeps itself.
fn rowTrackingCol(name: []const u8) bool {
    return std.mem.eql(u8, name, delta.row_id_name) or std.mem.eql(u8, name, delta.row_version_name);
}

pub const Evolve = enum { keep, merge, overwrite };

pub const RowPlan = struct {
    hwm: i64,
    mat_id: []const u8,
    mat_version: []const u8,
    keep: bool,
    src: ?[2]usize = null,
};

/// Row tracking as `st`'s table asks it of a writer (writer feature
/// `rowTracking`): each new file numbered on from the high-water mark, and, where
/// `delta.enableRowTracking` holds, a copied row's id and commit version kept in
/// the materialized columns, which the table must then name; `in` locates the
/// hidden row columns a rewrite's rows carry.
fn rowPlan(st: *const delta.State, name: []const u8, in: []const types.Schema.Field) !?RowPlan {
    const on = for (st.writer_features) |f| {
        if (std.mem.eql(u8, f, "rowTracking")) break true;
    } else false;
    if (!on) return null;
    const keep = if (st.property("delta.enableRowTracking")) |t| textconv.boolText(.delta_property, t) orelse false else false;
    var r = RowPlan{
        .hwm = st.row_hwm orelse -1,
        .mat_id = st.property("delta.rowTracking.materializedRowIdColumnName") orelse "",
        .mat_version = st.property("delta.rowTracking.materializedRowCommitVersionColumnName") orelse "",
        .keep = keep,
    };
    var id_at: ?usize = null;
    var ver_at: ?usize = null;
    for (in, 0..) |f, i| {
        if (std.mem.eql(u8, f.name, delta.row_id_name)) id_at = i;
        if (std.mem.eql(u8, f.name, delta.row_version_name)) ver_at = i;
    }
    if (id_at != null and ver_at != null and keep) {
        if (r.mat_id.len == 0 or r.mat_version.len == 0)
            return refuse("{s} tracks row ids (`delta.enableRowTracking`) but names no materialized row-id and row-commit-version columns, where a rewrite must keep them", .{name});
        r.src = .{ id_at.?, ver_at.? };
    }
    return r;
}

/// A column-mapped table's plan given its files' names: every field of its schema
/// lacking a physical name and id (one `merge_schema` adds, every one under
/// `overwrite_schema`) gets `col-<uuid>` and the next id past
/// `delta.columnMapping.maxColumnId`, raised in the commit's metadata; then each
/// column takes its physical name and its type with struct fields so renamed, and
/// every id is kept for the writer to set on its Parquet field.
fn applyMapping(arena: std.mem.Allocator, p: *Plan, st: *const delta.State) !void {
    var tree = try std.json.parseFromSliceLeaky(std.json.Value, arena, p.schema_string, .{});
    const was: i64 = std.fmt.parseInt(i64, std.mem.trim(u8, st.property("delta.columnMapping.maxColumnId") orelse "0", " "), 10) catch 0;
    var next = was;
    const list = tree.object.getPtr("fields").?;
    for (list.array.items) |*fv| try assignField(arena, fv, &next);
    if (next != was) {
        var aw = std.Io.Writer.Allocating.init(arena);
        try std.json.Stringify.value(tree, .{}, &aw.writer);
        p.schema_string = aw.written();
        p.new_meta = true;
        p.max_column_id = next;
    }
    const ids = try arena.create(std.StringHashMapUnmanaged(i32));
    ids.* = .empty;
    const cols = @constCast(p.cols);
    for (list.array.items, cols) |fv, *c| {
        const md = fv.object.get("metadata").?.object;
        c.phys_name = md.get("delta.columnMapping.physicalName").?.string;
        try ids.put(arena, c.phys_name.?, @intCast(md.get("delta.columnMapping.id").?.integer));
        c.file_ty = try fileType(arena, fv.object.get("type").?, c.carried(), ids);
    }
    p.field_ids = ids;
}

/// Gives schema field `fv` (and the struct fields its type nests) a physical name
/// and id where it has none, ids counting up from `next`.
fn assignField(arena: std.mem.Allocator, fv: *std.json.Value, next: *i64) !void {
    if (fv.* != .object) return;
    const md_ptr = fv.object.getPtr("metadata");
    var md = if (md_ptr) |m| (if (m.* == .object) m.object else std.json.ObjectMap.init(arena)) else std.json.ObjectMap.init(arena);
    if (md.get("delta.columnMapping.id") == null) {
        next.* += 1;
        try md.put("delta.columnMapping.id", .{ .integer = next.* });
    }
    if (md.get("delta.columnMapping.physicalName") == null)
        try md.put("delta.columnMapping.physicalName", .{ .string = try msgfmt.allocPrint(arena, "col-{s}", .{&dlog.uuid()}) });
    try fv.object.put("metadata", .{ .object = md });
    if (fv.object.getPtr("type")) |t| try assignType(arena, t, next);
}

fn assignType(arena: std.mem.Allocator, t: *std.json.Value, next: *i64) anyerror!void {
    if (t.* != .object) return;
    if (t.object.getPtr("fields")) |fs| if (fs.* == .array) for (fs.array.items) |*fv| try assignField(arena, fv, next);
    if (t.object.getPtr("elementType")) |e| try assignType(arena, e, next);
    if (t.object.getPtr("keyType")) |k| try assignType(arena, k, next);
    if (t.object.getPtr("valueType")) |v| try assignType(arena, v, next);
}

/// Type `t` (of `schemaString` type `jt`) as the files hold it: each struct field
/// named by its physical name, whose id goes into `ids`.
fn fileType(arena: std.mem.Allocator, jt: std.json.Value, t: types.Type, ids: *std.StringHashMapUnmanaged(i32)) !types.Type {
    if (!t.isNested() or jt != .object) return t;
    var out = t;
    switch (t.kind) {
        .@"struct" => {
            const jfs = (jt.object.get("fields") orelse return t).array.items;
            const fs = try arena.alloc(types.Type.Field, t.fields.?.len);
            for (fs, t.fields.?, jfs) |*o, f, jf| {
                const md = jf.object.get("metadata").?.object;
                const pn = md.get("delta.columnMapping.physicalName").?.string;
                try ids.put(arena, pn, @intCast(md.get("delta.columnMapping.id").?.integer));
                o.* = .{ .name = pn, .ty = try fileType(arena, jf.object.get("type").?, f.ty, ids) };
            }
            out.fields = fs;
        },
        .array => {
            const e = try arena.create(types.Type);
            e.* = try fileType(arena, jt.object.get("elementType").?, t.elem.?.*, ids);
            out.elem = e;
        },
        .map => {
            const kv = t.fields.?;
            const fs = try arena.alloc(types.Type.Field, 2);
            fs[0] = .{ .name = kv[0].name, .ty = try fileType(arena, jt.object.get("keyType").?, kv[0].ty, ids) };
            fs[1] = .{ .name = kv[1].name, .ty = try fileType(arena, jt.object.get("valueType").?, kv[1].ty, ids) };
            out.fields = fs;
        },
        else => {},
    }
    return out;
}

/// A table's nested column type `t` (`schemaString`'s JSON) grown by the struct
/// fields `in` has that it lacks, at any depth, each added nullable at the end of
/// its struct; null when `in` adds none or is of another shape, which the write
/// then refuses as it would without `merge_schema`.
fn mergedType(arena: std.mem.Allocator, t: std.json.Value, in: types.Type) !?std.json.Value {
    if (t != .object) return null;
    const kind = if (t.object.get("type")) |k| (if (k == .string) k.string else "") else "";
    if (std.mem.eql(u8, kind, "array")) {
        if (in.kind != .array) return null;
        const e = (try mergedType(arena, t.object.get("elementType") orelse return null, (in.elem orelse return null).*)) orelse return null;
        var o = try t.object.clone();
        try o.put("elementType", e);
        return .{ .object = o };
    }
    if (std.mem.eql(u8, kind, "map")) {
        if (in.kind != .map) return null;
        const kv = in.fields orelse return null;
        const v = (try mergedType(arena, t.object.get("valueType") orelse return null, kv[1].ty)) orelse return null;
        var o = try t.object.clone();
        try o.put("valueType", v);
        return .{ .object = o };
    }
    if (!std.mem.eql(u8, kind, "struct") or in.kind != .@"struct") return null;
    const list = t.object.get("fields") orelse return null;
    if (list != .array) return null;
    var out = try std.json.Array.initCapacity(arena, list.array.items.len);
    var grew = false;
    for (list.array.items) |fv| {
        var item = fv;
        if (fv == .object) if (fv.object.get("name")) |n| if (n == .string) for (in.fields orelse &.{}) |inf| if (std.mem.eql(u8, inf.name, n.string)) {
            if (try mergedType(arena, fv.object.get("type") orelse .null, inf.ty)) |g| {
                var o = try fv.object.clone();
                try o.put("type", g);
                item = .{ .object = o };
                grew = true;
            }
        };
        try out.append(item);
    }
    for (in.fields orelse &.{}) |inf| {
        const known = for (list.array.items) |fv| {
            if (fv == .object) if (fv.object.get("name")) |n| if (n == .string and std.mem.eql(u8, n.string, inf.name)) break true;
        } else false;
        if (known) continue;
        var aw = std.Io.Writer.Allocating.init(arena);
        try createdJson(&aw.writer, inf.name, inf.ty, 1);
        var o = std.json.ObjectMap.init(arena);
        try o.put("name", .{ .string = inf.name });
        try o.put("type", try std.json.parseFromSliceLeaky(std.json.Value, arena, aw.written(), .{}));
        try o.put("nullable", .{ .bool = true });
        try o.put("metadata", .{ .object = std.json.ObjectMap.init(arena) });
        try out.append(.{ .object = o });
        grew = true;
    }
    if (!grew) return null;
    var o = try t.object.clone();
    try o.put("fields", .{ .array = out });
    return .{ .object = o };
}

/// A table column the rows leave out (`Col.src`), as PARTIAL COLS allows.
pub const absent = std.math.maxInt(usize);

/// Files open for writing at once; past it the least recently written is closed.
pub const max_open = 16;

/// A data file is closed once this much Parquet is written to it.
pub const roll_bytes: i64 = 128 << 20;

/// One column as the table types it.
pub const Col = struct {
    name: []const u8,
    /// Its Delta type, as `schemaString` names it.
    delta_type: []const u8,
    /// The type its values are written as: a decimal at the table's precision and scale.
    ty: types.Type,
    phys: pqwrite.Physical = .{},
    nullable: bool = true,
    /// Its position in the incoming rows, `absent` when they leave it out.
    src: usize,
    phys_name: ?[]const u8 = null,
    file_ty: ?types.Type = null,
    variant_json: ?std.json.Value = null,
    raw_ty: ?types.Type = null,

    /// The name the table's files and log give it: its physical name under
    /// column mapping, else its own.
    pub fn fileName(self: Col) []const u8 {
        return self.phys_name orelse self.name;
    }

    /// The type a copy of the column's rows carries: its variants as raw Variant
    /// binary, where it holds any.
    pub fn carried(self: Col) types.Type {
        return self.raw_ty orelse self.ty;
    }
};

/// A write planned against the table as it stands.
pub const Plan = struct {
    name: []const u8,
    root: []const u8,
    mode: Mode,
    codec: codecs.Codec,
    /// The table as read, null when this write creates it.
    snap: ?*delta.State,
    /// In the table's column order.
    cols: []const Col,
    /// Which of `cols` partition the table, in `partitionColumns` order.
    parts: []const usize,
    /// Which of `cols` go into the data files.
    data: []const usize,
    schema_string: []const u8,
    /// An UPSERT's key columns, as indices into `cols`.
    keys: []const usize = &.{},
    /// Under PARTIAL COLS, the columns a matched row takes from the incoming one.
    partial: ?[]const usize = null,
    /// The incoming rows' columns.
    in_fields: []const types.Schema.Field = &.{},
    new_meta: bool = false,
    optimize: bool = false,
    field_ids: ?*const std.StringHashMapUnmanaged(i32) = null,
    max_column_id: ?i64 = null,
    row: ?RowPlan = null,

    /// What EXPLAIN and `check` add after the root.
    pub fn label(self: *const Plan, arena: std.mem.Allocator) ![]const u8 {
        const st = self.snap orelse return "created by this write";
        return msgfmt.allocPrint(arena, "version {d}, {d} file{s}", .{ st.version, st.files.count(), if (st.files.count() == 1) "" else "s" });
    }
};

/// Why `root` cannot hold a Delta table basalt commits to, or null: a commit must
/// be created only if absent, which local disks and object stores do in one step.
pub fn storeProblem(root: []const u8) ?[]const u8 {
    if (table.isHttp(root)) return "a URL cannot be listed or written; write a Delta table to a local folder, s3:// or az://";
    for ([_][]const u8{ "sftp://", "smb://" }) |pre| if (std.mem.startsWith(u8, root, pre))
        return "SFTP and SMB have no create-if-absent write basalt can make a commit with, so two writers could overwrite each other's version; write a Delta table to a local folder, s3:// or az://";
    if (std.mem.indexOf(u8, root, "://") != null and !objstore.isUrl(root))
        return "basalt writes Delta tables to a local folder, s3:// or az://";
    return null;
}

/// The write of `schema` into the table at `root`: the table as it stands, or,
/// when `create` and there is none, the one this write makes. With no schema
/// (`check` cannot always know it) only the table itself is judged. A column name
/// that is not UTF-8 is refused: the log's JSON would hold it repaired while the
/// Parquet files keep it raw, and the column would read back as all NULL.
pub fn plan(arena: std.mem.Allocator, root: []const u8, mode: Mode, create: bool, partition_by: ?[]const []const u8, codec: codecs.Codec, schema_in: ?types.Schema) !*Plan {
    return planEvolving(arena, root, mode, create, partition_by, codec, schema_in, .keep);
}

/// `plan` letting the rows change the table's schema as `evolve` says: `merge`
/// adds the columns (and struct fields) the rows have that the table lacks and
/// writes NULL for the table's columns the rows lack; `overwrite`, under a
/// REPLACE, gives the table the rows' schema whole, and `partition_by` (or the
/// table's partitioning, if its columns remain) partitions it.
pub fn planEvolving(arena: std.mem.Allocator, root: []const u8, mode: Mode, create: bool, partition_by: ?[]const []const u8, codec: codecs.Codec, schema_in: ?types.Schema, evolve: Evolve) !*Plan {
    const name = try msgfmt.allocPrint(arena, "Delta table `{s}`", .{root});
    if (storeProblem(root)) |why| return refuse("cannot write {s}: {s}", .{ name, why });
    if (schema_in) |s| for (s.fields) |f| if (!std.unicode.utf8ValidateSlice(f.name))
        return refuse("column \"{f}\" has a name that is not UTF-8, which {s} cannot hold; rename it with `AS`", .{ std.zig.fmtString(f.name), name });
    const log_path = try std.mem.concat(arena, u8, &.{ root, delta.log_dir });
    const listed: []const []const u8 = folder.listRaw(arena, log_path) catch |e|
        if (table.absent(e) or e == azure.Error.AzureContainerMissing or e == s3.Error.S3BucketMissing) &.{} else return e;
    const p = try arena.create(Plan);
    if (!delta.hasVersions(listed, log_path)) {
        if (!create) return refuse("folder `{s}` holds no _delta_log/, so it is no Delta table; `WITH (format = 'delta')` creates one there", .{root});
        p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = null, .cols = &.{}, .parts = &.{}, .data = &.{}, .schema_string = "" };
        if (schema_in) |schema| {
            p.* = try planCreate(arena, name, root, mode, partition_by, codec, schema);
        }
        return p;
    }
    const st = try delta.snapshot(arena, root, .{}, listed);
    try st.checkProtocol();
    const fields = try tableFields(arena, name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{name}));
    try writable(st, fields, mode);
    if (evolve == .overwrite) {
        if (mode != .replace) return refuse("`overwrite_schema` gives {s} the rows' schema, which only a REPLACE may; to add columns as rows arrive, use `merge_schema`", .{name});
        const schema = schema_in orelse {
            p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = st, .cols = &.{}, .parts = &.{}, .data = &.{}, .schema_string = st.schema_string.? };
            return p;
        };
        var parts = partition_by orelse st.part_cols;
        if (partition_by == null) for (st.part_cols) |c| if (indexOf(schema, c) == null) {
            parts = &.{};
            break;
        };
        p.* = try planCreate(arena, name, root, mode, parts, codec, schema);
        p.snap = st;
        p.new_meta = true;
        if (st.mappingMode() != null) try applyMapping(arena, p, st);
        p.row = try rowPlan(st, name, schema.fields);
        return p;
    }
    if (partition_by) |want| if (!sameNames(want, st.part_cols))
        return refuse("{s} is partitioned by ({s}), not ({s}); `partition_by` applies only when a write creates the table", .{ name, try std.mem.join(arena, ", ", st.part_cols), try std.mem.join(arena, ", ", want) });
    p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = st, .cols = &.{}, .parts = &.{}, .data = &.{}, .schema_string = st.schema_string.? };
    const schema = schema_in orelse return p;
    const partial = mode == .upsert and mode.upsert.partial != null;
    const merge_cols = evolve == .merge;
    var cols_list = std.array_list.Managed(Col).init(arena);
    try cols_list.resize(fields.len);
    const cols = cols_list.items;
    var changed = false;
    var tree = if (merge_cols) try std.json.parseFromSliceLeaky(std.json.Value, arena, st.schema_string.?, .{}) else std.json.Value.null;
    for (fields, cols, 0..) |f, *c, fi| {
        const src = indexOf(schema, f.name) orelse {
            if (merge_cols) {
                if (!f.nullable) return refuse("{s}'s column `{s}` is NOT NULL, and the rows lack it; `merge_schema` writes NULL for a column the rows lack, which it cannot hold", .{ name, f.name });
                var none = types.Type.init(.string);
                none.unknown = true;
                c.* = try tableCol(arena, name, f, none, absent);
                continue;
            }
            if (!partial) return refuse("{s} has column `{s}`, which the rows lack; a write gives every column of the table a value (`CAST(NULL AS ...) AS {s}` leaves it empty)", .{ name, f.name, f.name });
            if (f.metadata) |md| if (md.get("CURRENT_DEFAULT") != null)
                return refuse("{s}'s column `{s}` has a default value (`CURRENT_DEFAULT`), which basalt does not apply, so a row an UPSERT inserts would not take it; give `{s}` in the rows", .{ name, f.name, f.name });
            var none = types.Type.init(.string);
            none.unknown = true;
            c.* = try tableCol(arena, name, f, none, absent);
            continue;
        };
        const in_ty = schema.fields[src].ty;
        if (merge_cols and f.ty != .string and !in_ty.unknown) if (try mergedType(arena, f.ty, in_ty)) |grown| {
            var nf = f;
            nf.ty = grown;
            c.* = try tableColLoose(arena, name, nf, in_ty, src, true);
            try tree.object.getPtr("fields").?.array.items[fi].object.put("type", grown);
            changed = true;
            continue;
        };
        c.* = try tableColLoose(arena, name, f, in_ty, src, merge_cols);
    }
    for (schema.fields, 0..) |f, src| if (indexOfField(fields, f.name) == null and !rowTrackingCol(f.name)) {
        if (!merge_cols) return refuse("the rows have column `{s}`, which {s} lacks; basalt does not add columns to a Delta table (`WITH (merge_schema = true)` adds it)", .{ f.name, name });
        const c = try createCol(arena, name, f, src);
        try cols_list.append(c);
        var o = std.json.ObjectMap.init(arena);
        try o.put("name", .{ .string = f.name });
        try o.put("type", if (c.ty.isNested()) try std.json.parseFromSliceLeaky(std.json.Value, arena, c.delta_type, .{}) else .{ .string = c.delta_type });
        try o.put("nullable", .{ .bool = true });
        try o.put("metadata", .{ .object = std.json.ObjectMap.init(arena) });
        try tree.object.getPtr("fields").?.array.append(.{ .object = o });
        changed = true;
    };
    var schema_string = st.schema_string.?;
    if (changed) {
        var aw = std.Io.Writer.Allocating.init(arena);
        try std.json.Stringify.value(tree, .{}, &aw.writer);
        schema_string = aw.written();
    }
    p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = st, .cols = cols_list.items, .parts = &.{}, .data = &.{}, .schema_string = schema_string, .in_fields = schema.fields, .new_meta = changed };
    try split(p, arena, st.part_cols);
    if (mode != .upsert) for (p.parts) |k| if (p.cols[k].src == absent)
        return refuse("{s} is partitioned by `{s}`, which the rows lack; a row's partition column says where it is written", .{ name, p.cols[k].name });
    try upsertCols(p, arena);
    if (st.mappingMode() != null) try applyMapping(arena, p, st);
    p.row = try rowPlan(st, name, schema.fields);
    return p;
}

/// Resolves an UPSERT's key and PARTIAL COLS columns against the table, refusing
/// a key the table or the rows lack, a listed column that is a key, that the
/// table or the rows lack, and a name given twice.
fn upsertCols(p: *Plan, arena: std.mem.Allocator) !void {
    const u = switch (p.mode) {
        .upsert => |u| u,
        else => return,
    };
    if (u.keys.len == 0 or p.cols.len == 0) return;
    const keys = try arena.alloc(usize, u.keys.len);
    for (u.keys, keys, 0..) |kn, *o, i| {
        o.* = colIndex(p.cols, kn) orelse return refuse("{s} has no column `{s}` to match an UPSERT's rows on", .{ p.name, kn });
        if (std.mem.indexOfScalar(usize, keys[0..i], o.*) != null) return refuse("`UPSERT ON` names `{s}` twice", .{kn});
        if (p.cols[o.*].src == absent) return refuse("the rows lack key column `{s}`; an UPSERT matches rows by every key column", .{kn});
        if (p.cols[o.*].variant_json != null) return refuse("key column `{s}` holds a variant, which has no equality to match rows by; key the UPSERT on other columns", .{kn});
    }
    p.keys = keys;
    const listed = u.partial orelse return;
    const out = try arena.alloc(usize, listed.len);
    for (listed, out, 0..) |cn, *o, i| {
        o.* = colIndex(p.cols, cn) orelse return refuse("`PARTIAL COLS` names `{s}`, which {s} lacks", .{ cn, p.name });
        if (std.mem.indexOfScalar(usize, keys, o.*) != null) return refuse("`PARTIAL COLS` names key column `{s}`; a key is what rows are matched by, and a matched row keeps it", .{cn});
        if (std.mem.indexOfScalar(usize, out[0..i], o.*) != null) return refuse("`PARTIAL COLS` names `{s}` twice", .{cn});
        if (p.cols[o.*].src == absent) return refuse("`PARTIAL COLS` names `{s}`, which the rows lack", .{cn});
    }
    p.partial = out;
}

pub fn colIndex(cols: []const Col, name: []const u8) ?usize {
    for (cols, 0..) |c, i| if (std.mem.eql(u8, c.name, name)) return i;
    return null;
}

fn sameNames(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| if (!std.mem.eql(u8, x, y)) return false;
    return true;
}

fn indexOf(schema: types.Schema, name: []const u8) ?usize {
    for (schema.fields, 0..) |f, i| if (std.mem.eql(u8, f.name, name)) return i;
    return null;
}

fn indexOfField(fields: []const Field, name: []const u8) ?usize {
    for (fields, 0..) |f, i| if (std.mem.eql(u8, f.name, name)) return i;
    return null;
}

/// Splits `cols` into partition and data columns; refused when a partition column
/// is missing, repeated, binary, or when no column is left for the files.
fn split(self: *Plan, arena: std.mem.Allocator, part_cols: []const []const u8) !void {
    const parts = try arena.alloc(usize, part_cols.len);
    for (part_cols, parts, 0..) |pc, *o, i| {
        o.* = for (self.cols, 0..) |c, k| {
            if (std.mem.eql(u8, c.name, pc)) break k;
        } else return refuse("{s} has no column `{s}` to partition by", .{ self.name, pc });
        for (parts[0..i]) |prev| if (prev == o.*) return refuse("{s} names partition column `{s}` twice", .{ self.name, pc });
        if (self.cols[o.*].ty.kind == .bytes) return refuse("{s} cannot be partitioned by binary column `{s}`", .{ self.name, pc });
        if (self.cols[o.*].ty.isNested()) return refuse("{s} cannot be partitioned by `{s}`, a list, map or struct; partition by one of its fields taken out as a column", .{ self.name, pc });
    }
    if (parts.len == self.cols.len) return refuse("{s} would be partitioned by every column, leaving none for its data files", .{self.name});
    const data = try arena.alloc(usize, self.cols.len - parts.len);
    var n: usize = 0;
    for (0..self.cols.len) |k| {
        if (std.mem.indexOfScalar(usize, parts, k) != null) continue;
        data[n] = k;
        n += 1;
    }
    self.parts = parts;
    self.data = data;
}

fn planCreate(arena: std.mem.Allocator, name: []const u8, root: []const u8, mode: Mode, partition_by: ?[]const []const u8, codec: codecs.Codec, schema: types.Schema) !Plan {
    var kept: usize = 0;
    for (schema.fields) |f| kept += @intFromBool(!rowTrackingCol(f.name));
    if (kept == 0) return refuse("{s} would have no columns", .{name});
    const cols = try arena.alloc(Col, kept);
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.writeAll("{\"type\":\"struct\",\"fields\":[");
    var n: usize = 0;
    for (schema.fields, 0..) |f, src| {
        if (rowTrackingCol(f.name)) continue;
        const i = n;
        n += 1;
        const c = &cols[i];
        c.* = try createCol(arena, name, f, src);
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"name\":");
        try jsontext.string(w, f.name);
        if (c.ty.isNested())
            try w.print(",\"type\":{s},\"nullable\":true,\"metadata\":{{}}}}", .{c.delta_type})
        else
            try w.print(",\"type\":\"{s}\",\"nullable\":true,\"metadata\":{{}}}}", .{c.delta_type});
    }
    try w.writeAll("]}");
    var p = Plan{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = null, .cols = cols, .parts = &.{}, .data = &.{}, .schema_string = aw.written(), .in_fields = schema.fields };
    try split(&p, arena, partition_by orelse &.{});
    try upsertCols(&p, arena);
    return p;
}

/// A basalt column as a new table's column. A column a whole input held only NULLs
/// in (`Type.nullColumn`) is created as its `sinkType`, a nullable string; a bare
/// NULL has no type to create.
fn createCol(arena: std.mem.Allocator, name: []const u8, f: types.Schema.Field, src: usize) !Col {
    const t = if (f.ty.unknown and f.ty.kind == .string) f.ty.sinkType() else f.ty;
    if (t.unknown) return refuse("column `{s}` has no type (a bare NULL); CAST it to the type {s} should hold", .{ f.name, name });
    switch (t.kind) {
        .decimal => {
            const d = dtypes.createdDecimal(t) orelse
                return refuse("column `{s}` is decimal({d},{d}), whose scale exceeds its precision", .{ f.name, dtypes.createdPrecision(t), t.scale });
            return .{ .name = f.name, .delta_type = try msgfmt.allocPrint(arena, "decimal({d},{d})", .{ d.precision, d.scale }), .ty = d, .src = src };
        },
        .time => return refuse("column `{s}` is a time of day, which Delta has no type for; cast it to a string or store microseconds as a number", .{f.name}),
        .array, .@"struct", .map => {
            var aw = std.Io.Writer.Allocating.init(arena);
            try createdJson(&aw.writer, f.name, t, 0);
            const text = aw.written();
            const v = try std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{});
            return .{ .name = f.name, .delta_type = text, .ty = try delta.tableType(arena, name, f.name, v), .phys = .{ .leaves = try dtypes.leafPhysicals(arena, v) }, .src = src };
        },
        else => {},
    }
    const dt = dtypes.deltaName(t.kind).?;
    return .{ .name = f.name, .delta_type = dt, .ty = types.Type.init(t.kind), .phys = dtypes.physical(dt), .src = src };
}

/// A new table's nested column type `t` as `schemaString` spells it: a struct's
/// fields, an array's element and a map's value each nullable, every primitive
/// named as a top-level column of its type would be.
fn createdJson(w: *std.Io.Writer, col: []const u8, t_in: types.Type, depth: usize) !void {
    if (depth >= types.max_nesting) return refuse("column `{s}` is nested deeper than {d} levels", .{ col, types.max_nesting });
    const t = if (t_in.unknown and t_in.kind == .string) t_in.sinkType() else t_in;
    switch (t.kind) {
        .@"struct" => {
            try w.writeAll("{\"type\":\"struct\",\"fields\":[");
            for (t.fields orelse &.{}, 0..) |f, i| {
                if (i > 0) try w.writeByte(',');
                try w.writeAll("{\"name\":");
                try jsontext.string(w, f.name);
                try w.writeAll(",\"type\":");
                try createdJson(w, col, f.ty, depth + 1);
                try w.writeAll(",\"nullable\":true,\"metadata\":{}}");
            }
            try w.writeAll("]}");
        },
        .array => {
            try w.writeAll("{\"type\":\"array\",\"elementType\":");
            try createdJson(w, col, (t.elem orelse return refuse("column `{s}` is a list of no element type", .{col})).*, depth + 1);
            try w.writeAll(",\"containsNull\":true}");
        },
        .map => {
            const kv = t.fields orelse return refuse("column `{s}` is a map of no key and value types", .{col});
            try w.writeAll("{\"type\":\"map\",\"keyType\":");
            try createdJson(w, col, kv[0].ty, depth + 1);
            try w.writeAll(",\"valueType\":");
            try createdJson(w, col, kv[1].ty, depth + 1);
            try w.writeAll(",\"valueContainsNull\":true}");
        },
        .decimal => {
            const d = dtypes.createdDecimal(t) orelse
                return refuse("column `{s}` holds a decimal({d},{d}), whose scale exceeds its precision", .{ col, dtypes.createdPrecision(t), t.scale });
            try w.print("\"decimal({d},{d})\"", .{ d.precision, d.scale });
        },
        .time => return refuse("column `{s}` holds a time of day, which Delta has no type for; cast it to a string or store microseconds as a number", .{col}),
        else => {
            if (t.unknown) return refuse("column `{s}` holds a part with no type (a bare NULL); CAST it", .{col});
            try w.print("\"{s}\"", .{dtypes.deltaName(t.kind).?});
        },
    }
}

const Field = dtypes.Field;

pub fn tableFields(arena: std.mem.Allocator, name: []const u8, text: []const u8) ![]const Field {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch
        return refuse("{s} has a schemaString that is not JSON", .{name});
    return switch (try dtypes.fields(arena, v, .strict)) {
        .fields => |fs| fs,
        .problem => |p| switch (p) {
            .not_struct => refuse("{s} has a schemaString that is not a struct of fields", .{name}),
            .not_object => refuse("{s} has a schemaString field that is not an object", .{name}),
            .nameless => refuse("{s} has a schemaString field with no name", .{name}),
        },
    };
}

/// A table column `f` as the rows' column of type `in` is written to it.
fn tableCol(arena: std.mem.Allocator, name: []const u8, f: Field, in: types.Type, src: usize) !Col {
    return tableColLoose(arena, name, f, in, src, false);
}

/// `tableCol`; `loose` (under `merge_schema`) lets a struct of the rows leave out
/// fields the table's has, and give its fields in any order.
fn tableColLoose(arena: std.mem.Allocator, name: []const u8, f: Field, in: types.Type, src: usize, loose: bool) !Col {
    if (f.ty != .string) return nestedCol(arena, name, f, in, src, loose);
    const t = f.ty.string;
    if (std.mem.eql(u8, t, "variant")) {
        if (!in.unknown and in.kind != .string)
            return refuse("column `{s}` is {s} in the rows but `variant` in {s}; give it as JSON text (a string)", .{ f.name, @tagName(in.kind), name });
        return .{ .name = f.name, .delta_type = t, .ty = types.Type.init(.string), .raw_ty = vcol.raw_type, .file_ty = vcol.raw_type, .phys = .{ .leaves = try dtypes.leafPhysicals(arena, f.ty) }, .nullable = f.nullable, .src = src, .variant_json = f.ty };
    }
    const c = Col{ .name = f.name, .delta_type = t, .ty = dtypes.scalarType(t, .write) orelse
        return refuse("{s} has column `{s}` of type `{s}`, which basalt does not write", .{ name, f.name, t }), .phys = dtypes.physical(t), .nullable = f.nullable, .src = src };
    if (in.unknown) return c;
    if (in.kind != c.ty.kind)
        return refuse("column `{s}` is {s} in the rows but `{s}` in {s}; cast it", .{ f.name, @tagName(in.kind), t, name });
    if (in.kind == .decimal) {
        const ip: u8 = if (in.precision == 0) 38 else in.precision;
        if (in.scale > c.ty.scale or ip -| in.scale > c.ty.precision - c.ty.scale)
            return refuse("column `{s}` is decimal({d},{d}) in the rows but `{s}` in {s}, which cannot hold every value; cast it", .{ f.name, ip, in.scale, t, name });
    }
    return c;
}

/// A table's nested column `f`, written as the table types it, each leaf laid out
/// as the table's primitive there says (an `integer` as INT32, a `timestamp` as
/// UTC); the rows' column must have its shape, field names and leaf kinds, a
/// decimal leaf one the table's holds every value of.
fn nestedCol(arena: std.mem.Allocator, name: []const u8, f: Field, in: types.Type, src: usize, loose: bool) !Col {
    const ty = try delta.tableType(arena, name, f.name, f.ty);
    var aw = std.Io.Writer.Allocating.init(arena);
    try std.json.Stringify.value(f.ty, .{}, &aw.writer);
    var c = Col{ .name = f.name, .delta_type = aw.written(), .ty = ty, .phys = .{ .leaves = try dtypes.leafPhysicals(arena, f.ty) }, .nullable = f.nullable, .src = src };
    if (vcol.holds(f.ty)) {
        c.variant_json = f.ty;
        c.raw_ty = try vcol.rawType(arena, ty, f.ty);
        c.file_ty = c.raw_ty;
    }
    if (in.unknown) return c;
    if (!fitsNested(in, ty, loose))
        return refuse("column `{s}` is {s} in the rows but {s} in {s}; cast it", .{ f.name, try in.name(arena), try ty.name(arena), name });
    return c;
}

/// Whether values of `in` write as `t`: the same shape and field names, leaves of
/// one kind, a decimal leaf at most `t`'s scale and fitting its integer digits.
fn fitsNested(in: types.Type, t: types.Type, loose: bool) bool {
    if (in.kind != t.kind) return false;
    switch (t.kind) {
        .@"struct", .map => {
            const a = in.fields orelse return false;
            const b = t.fields orelse return false;
            if (loose and t.kind == .@"struct") {
                for (a) |x| {
                    const y = for (b) |y| {
                        if (std.mem.eql(u8, x.name, y.name)) break y;
                    } else return false;
                    if (!fitsNested(x.ty, y.ty, true)) return false;
                }
                return true;
            }
            if (a.len != b.len) return false;
            for (a, b) |x, y| {
                if (t.kind == .@"struct" and !std.mem.eql(u8, x.name, y.name)) return false;
                if (!fitsNested(x.ty, y.ty, loose)) return false;
            }
            return true;
        },
        .array => return fitsNested((in.elem orelse return false).*, (t.elem orelse return false).*, loose),
        .decimal => {
            const ip: u8 = if (in.precision == 0) 38 else in.precision;
            return in.scale <= t.scale and ip -| in.scale <= t.precision - t.scale;
        },
        else => return true,
    }
}

/// The writer features basalt honours: the ones that ask nothing of a writer
/// that adds and removes whole files, and the ones whose demands `writable`
/// checks the table does not make.
const honoured = [_][]const u8{
    "appendOnly",          "invariants",               "checkConstraints",     "changeDataFeed", "generatedColumns",
    "columnMapping",       "identityColumns",          "deletionVectors",      "timestampNtz",   "domainMetadata",
    "vacuumProtocolCheck", "allowColumnDefaults",      "inCommitTimestamp",    "clustering",     "v2Checkpoint",
    "rowTracking",         "typeWidening",             "typeWidening-preview", "variantType",    "variantType-preview",
    "variantShredding",    "variantShredding-preview",
};

/// Statistics `text` with `tightBounds` false, as a file some of whose rows a
/// deletion vector removes keeps them: its bounds hold, but may be wider than its
/// rows'.
fn looseStats(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    var v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch return text;
    if (v != .object) return text;
    try v.object.put("tightBounds", .{ .bool = false });
    var aw = std.Io.Writer.Allocating.init(arena);
    try std.json.Stringify.value(v, .{}, &aw.writer);
    return aw.written();
}

/// Whether an UPSERT into `st` marks the rows it removes in deletion vectors
/// rather than rewriting their files: the protocol's `deletionVectors` reader
/// and writer feature, with `delta.enableDeletionVectors`.
pub fn writesDeletionVectors(st: *const delta.State) bool {
    if (!oneOf("deletionVectors", st.features) or !oneOf("deletionVectors", st.writer_features)) return false;
    const v = st.property("delta.enableDeletionVectors") orelse return false;
    return textconv.boolText(.delta_property, v) orelse false;
}

/// Refuses a table that asks a writer for what basalt does not do.
pub fn writable(st: *const delta.State, fields: []const Field, mode: Mode) !void {
    if (st.writer_version > 7)
        return refuse("{s} needs Delta writer version {d}; basalt writes writer versions 1 to 7", .{ st.name, st.writer_version });
    if (st.writer_version == 7) for (st.writer_features) |f| {
        if (oneOf(f, &honoured)) continue;
        return refuse("{s} uses writer feature `{s}`, which basalt does not honour, so it does not write the table", .{ st.name, f });
    };
    for (fields) |f| {
        const md = f.metadata orelse continue;
        var it = md.iterator();
        while (it.next()) |kv| {
            const k = kv.key_ptr.*;
            const what: ?[]const u8 = if (std.mem.eql(u8, k, "delta.invariants"))
                "an invariant (`delta.invariants`)"
            else if (std.mem.eql(u8, k, "delta.generationExpression"))
                "a generation expression"
            else if (std.mem.startsWith(u8, k, "delta.identity."))
                "an identity column"
            else
                null;
            if (what) |w| return refuse("{s}'s column `{s}` is {s}, which basalt does not enforce, so it does not write the table", .{ st.name, f.name, w });
        }
    }
    if (st.config) |c| {
        var it = c.object.iterator();
        while (it.next()) |kv| if (std.mem.startsWith(u8, kv.key_ptr.*, "delta.constraints."))
            return refuse("{s} has CHECK constraint `{s}`, which basalt does not enforce, so it does not write the table", .{ st.name, kv.key_ptr.*["delta.constraints.".len..] });
    }
    if (mode != .append) if (st.property("delta.appendOnly")) |v| if (textconv.boolText(.delta_property, v).?)
        return refuse("{s} is append-only (`delta.appendOnly`), so {s} cannot remove its files", .{ st.name, if (mode == .replace) "a REPLACE" else "an UPSERT" });
}

fn oneOf(s: []const u8, set: []const []const u8) bool {
    for (set) |x| if (std.mem.eql(u8, s, x)) return true;
    return false;
}

pub const Error = error{ DeltaValueOutOfRange, DeltaNullInNotNull };

/// Spark's escaping of a partition folder's name and value.
fn escapePath(w: *std.Io.Writer, s: []const u8) !void {
    for (s) |ch| {
        const esc = ch < 0x20 or ch == 0x7f or std.mem.indexOfScalar(u8, "\"#%'*/:=?\\{[]^", ch) != null;
        if (esc) try w.print("%{X:0>2}", .{ch}) else try w.writeByte(ch);
    }
}

/// A relative path as the log's URI form: everything but unreserved characters,
/// `/` and `=` percent-encoded.
fn uriPath(arena: std.mem.Allocator, rel: []const u8) ![]const u8 {
    return percent.encodeAlloc(arena, rel, "/=");
}

fn statsJson(arena: std.mem.Allocator, p: *const Plan, sum: pqwrite.Writer.Summary) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.print("{{\"numRecords\":{d}", .{sum.rows});
    var items = std.ArrayList(Stat).empty;
    for ([_]?dtypes.Bound{ .min, .max, null }) |which| {
        try w.print(",\"{s}\":{{", .{if (which) |b| b.key() else "nullCount"});
        var first = true;
        for (p.data, sum.columns[0..p.data.len]) |k, cs| {
            const c = p.cols[k];
            items.clearRetainingCapacity();
            if (c.carried().isNested()) {
                for (cs.leaves) |l| {
                    const ph = if (l.leaf < c.phys.leaves.len) c.phys.leaves[l.leaf] else pqwrite.Physical{};
                    if (!ph.stats) continue;
                    if (try statOf(arena, ph, l, which)) |text| try items.append(arena, .{ .path = l.path, .text = text });
                }
                if (items.items.len == 0) continue;
            } else if (try statOf(arena, c.phys, cs, which)) |text| {
                try items.append(arena, .{ .path = &.{}, .text = text });
            } else continue;
            if (!first) try w.writeByte(',');
            first = false;
            try jsontext.string(w, c.fileName());
            try w.writeByte(':');
            if (c.ty.isNested()) {
                try w.writeByte('{');
                try statTree(w, items.items, 0);
                try w.writeByte('}');
            } else try w.writeAll(items.items[0].text);
        }
        try w.writeByte('}');
    }
    try w.writeByte('}');
    return aw.written();
}

const Stat = struct { path: []const []const u8, text: []const u8 };

/// A column's `which` bound as JSON text, or its null count when `which` is null.
fn statOf(arena: std.mem.Allocator, ph: pqwrite.Physical, cs: pqwrite.Writer.ColumnStats, which: ?dtypes.Bound) !?[]const u8 {
    const b = which orelse return try msgfmt.allocPrint(arena, "{d}", .{cs.nulls});
    const v = (if (b == .max) cs.max else cs.min) orelse return null;
    return try dtypes.statText(arena, ph, v, b);
}

/// A struct's leaf statistics as nested objects, as Delta keeps a struct field's:
/// the leaves in schema order, so those sharing a field below `depth` are adjacent.
fn statTree(w: *std.Io.Writer, items: []const Stat, depth: usize) !void {
    var i: usize = 0;
    while (i < items.len) {
        if (i > 0) try w.writeByte(',');
        const name = items[i].path[depth];
        try jsontext.string(w, name);
        try w.writeByte(':');
        if (items[i].path.len == depth + 1) {
            try w.writeAll(items[i].text);
            i += 1;
            continue;
        }
        var j = i + 1;
        while (j < items.len and items[j].path.len > depth + 1 and std.mem.eql(u8, items[j].path[depth], name)) j += 1;
        try w.writeByte('{');
        try statTree(w, items[i..j], depth + 1);
        try w.writeByte('}');
        i = j;
    }
}

/// A data file being written, its names and values on its own arena.
const change_dir = "_change_data/";

const Open = struct {
    mem: *std.heap.ArenaAllocator,
    writer: *pqwrite.Writer,
    rel: []const u8,
    full: []const u8,
    values: []const ?[]const u8,
    used: u64,
    cdc: bool = false,
};

pub const Sink = struct {
    gpa: std.mem.Allocator,
    mem: std.heap.ArenaAllocator,
    /// The plan files are written by: for an UPSERT, the table's columns in order.
    plan: *const Plan,
    file_schema: types.Schema,
    phys: []const pqwrite.Physical,
    cdc_schema: types.Schema = .{ .fields = &.{} },
    cdc_phys: []const pqwrite.Physical = &.{},
    feed: bool = false,
    files: std.StringArrayHashMapUnmanaged(*Open) = .empty,
    tick: u64 = 0,
    /// The commit being built: its header, then the `add` action of every data
    /// file finished, one line each.
    adds: std.Io.Writer.Allocating,
    txn: [36]u8 = undefined,
    n_added: usize = 0,
    /// Every data file begun, local ones removed when the write fails.
    begun: std.ArrayListUnmanaged([]const u8) = .empty,
    /// The local folders holding a finished file, synced before the commit.
    dirs: std.StringArrayHashMapUnmanaged(void) = .empty,
    /// Whether this write made the table's local root folder.
    made_root: bool = false,
    /// An UPSERT's spooled rows, merged with the table at `close`.
    merging: ?*merge.Merge = null,
    /// Whether an UPSERT's joins or its sort went to disk.
    spilled: bool = false,
    /// The version the commit became, once it is made.
    committed: ?u64 = null,
    row_counts: std.ArrayListUnmanaged(u64) = .empty,
    ict: ?i64 = null,
    compacting: ?[]const delta.Add = null,
    warn: ?merge.Warn = null,
    /// Set once `close` or `abort` has freed everything, so an `abort` after a
    /// failed `close` does nothing.
    done: bool = false,

    /// A sink on `plan_arena` (which outlives it), its buffers on `gpa`.
    pub fn open(gpa: std.mem.Allocator, plan_arena: std.mem.Allocator, p: *const Plan, opts: merge.Opts) !*Sink {
        const self = try plan_arena.create(Sink);
        self.* = .{ .gpa = gpa, .mem = std.heap.ArenaAllocator.init(gpa), .plan = p, .file_schema = undefined, .phys = &.{}, .adds = std.Io.Writer.Allocating.init(gpa), .warn = opts.warn };
        errdefer {
            self.adds.deinit();
            self.mem.deinit();
        }
        const a = self.mem.allocator();
        if (!objstore.isUrl(p.root)) {
            std.fs.cwd().access(dlog.trimDir(p.root), .{}) catch |e| {
                if (e != error.FileNotFound) return e;
                self.made_root = true;
            };
        }
        if (p.mode == .upsert) {
            const out = try plan_arena.create(Plan);
            out.* = p.*;
            const cols = try plan_arena.dupe(Col, p.cols);
            for (cols, 0..) |*c, k| c.src = k;
            out.cols = cols;
            if (out.row) |*r| if (r.keep) {
                if (r.mat_id.len == 0 or r.mat_version.len == 0)
                    return refuse("{s} tracks row ids (`delta.enableRowTracking`) but names no materialized row-id and row-commit-version columns, where an UPSERT's rewrite must keep them", .{p.name});
                r.src = .{ cols.len, cols.len + 1 };
            };
            self.plan = out;
            self.merging = try merge.Merge.init(gpa, p, opts);
        }
        const extra: usize = if (self.plan.row) |r| (if (r.src != null) 2 else 0) else 0;
        const fields = try a.alloc(types.Schema.Field, p.data.len + extra);
        const phys = try a.alloc(pqwrite.Physical, p.data.len + extra);
        for (p.data, fields[0..p.data.len], phys[0..p.data.len]) |k, *f, *ph| {
            const c = p.cols[k];
            f.* = .{ .name = c.fileName(), .ty = (c.file_ty orelse c.ty).asNullable() };
            ph.* = c.phys;
        }
        if (extra == 2) {
            const r = self.plan.row.?;
            fields[p.data.len] = .{ .name = r.mat_id, .ty = types.Type.init(.int).asNullable() };
            fields[p.data.len + 1] = .{ .name = r.mat_version, .ty = types.Type.init(.int).asNullable() };
            phys[p.data.len] = .{};
            phys[p.data.len + 1] = .{};
        }
        self.file_schema = .{ .fields = fields };
        self.phys = phys;
        if (p.mode == .upsert) if (p.snap) |st| if (st.property("delta.enableChangeDataFeed")) |v| if (textconv.boolText(.delta_property, v) orelse false) {
            const cf = try a.alloc(types.Schema.Field, p.data.len + 1);
            @memcpy(cf[0..p.data.len], fields[0..p.data.len]);
            cf[p.data.len] = .{ .name = fit.change_type_name, .ty = types.Type.init(.string).asNullable() };
            const cp = try a.alloc(pqwrite.Physical, p.data.len + 1);
            @memcpy(cp[0..p.data.len], phys[0..p.data.len]);
            cp[p.data.len] = .{};
            self.cdc_schema = .{ .fields = cf };
            self.cdc_phys = cp;
            self.feed = true;
        };
        try self.header();
        return self;
    }

    /// Begins the commit with its `commitInfo` (and, creating the table, its
    /// protocol and `metaData`); each file's `add` follows as it is finished and
    /// the removes at `close`, so the commit is built once, in place.
    fn header(self: *Sink) !void {
        const p = self.plan;
        const a = self.mem.allocator();
        self.txn = dlog.uuid();
        const now = std.time.milliTimestamp();
        const w = &self.adds.writer;
        var pb = std.Io.Writer.Allocating.init(a);
        try pb.writer.writeByte('[');
        for (p.parts, 0..) |k, i| {
            if (i > 0) try pb.writer.writeByte(',');
            try jsontext.string(&pb.writer, p.cols[k].name);
        }
        try pb.writer.writeByte(']');
        switch (p.mode) {
            .upsert => |u| {
                try w.print("{{\"commitInfo\":{{\"timestamp\":{d},\"operation\":\"MERGE\",\"operationParameters\":{{\"predicate\":", .{now});
                var pred = std.Io.Writer.Allocating.init(a);
                for (u.keys, 0..) |k, i| try pred.writer.print("{s}target.`{s}` = source.`{s}`", .{ if (i > 0) " AND " else "", k, k });
                try jsontext.string(w, pred.written());
            },
            else => if (p.optimize) {
                try w.print("{{\"commitInfo\":{{\"timestamp\":{d},\"operation\":\"OPTIMIZE\",\"operationParameters\":{{\"predicate\":\"[]\"", .{now});
            } else {
                try w.print("{{\"commitInfo\":{{\"timestamp\":{d},\"operation\":\"WRITE\",\"operationParameters\":{{\"mode\":\"{s}\",\"partitionBy\":", .{ now, if (p.mode == .append) "Append" else "Overwrite" });
                try jsontext.string(w, pb.written());
            },
        }
        try w.writeByte('}');
        if (p.snap) |st| if (st.property("delta.enableInCommitTimestamps")) |on| if (textconv.boolText(.delta_property, on) orelse false) {
            const ict = @max(now, (st.last_ict orelse 0) + 1);
            self.ict = ict;
            try w.print(",\"inCommitTimestamp\":{d}", .{ict});
        };
        try w.print(",\"isBlindAppend\":{s},\"engineInfo\":\"basalt/{s}\",\"txnId\":\"{s}\"}}}}\n", .{ if (p.mode == .append and p.snap != null and !p.optimize) "true" else "false", @import("build_options").version, &self.txn });
        if (p.snap == null) {
            try w.writeAll("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
            try w.print("{{\"metaData\":{{\"id\":\"{s}\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":", .{&dlog.uuid()});
            try jsontext.string(w, p.schema_string);
            try w.writeAll(",\"partitionColumns\":");
            try w.writeAll(pb.written());
            try w.print(",\"configuration\":{{}},\"createdTime\":{d}}}}}\n", .{now});
        } else if (p.new_meta) {
            const st = p.snap.?;
            var o = if (st.meta_raw) |m| (if (m == .object) try m.object.clone() else std.json.ObjectMap.init(a)) else std.json.ObjectMap.init(a);
            try o.put("schemaString", .{ .string = p.schema_string });
            if (p.max_column_id) |m| {
                var conf = if (o.get("configuration")) |c| (if (c == .object) try c.object.clone() else std.json.ObjectMap.init(a)) else if (st.config) |c| try c.object.clone() else std.json.ObjectMap.init(a);
                try conf.put("delta.columnMapping.maxColumnId", .{ .string = try msgfmt.allocPrint(a, "{d}", .{m}) });
                try o.put("configuration", .{ .object = conf });
            }
            var parts = std.json.Array.init(a);
            for (p.parts) |k| try parts.append(.{ .string = p.cols[k].name });
            try o.put("partitionColumns", .{ .array = parts });
            if (o.get("id") == null) try o.put("id", .{ .string = st.table_id orelse try a.dupe(u8, &dlog.uuid()) });
            if (o.get("format") == null) try o.put("format", try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"provider\":\"parquet\",\"options\":{}}", .{}));
            if (o.get("configuration") == null) try o.put("configuration", st.config orelse .{ .object = std.json.ObjectMap.init(a) });
            try w.writeAll("{\"metaData\":");
            try std.json.Stringify.value(std.json.Value{ .object = o }, .{}, w);
            try w.writeAll("}\n");
        }
    }

    pub fn sink(self: *Sink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sinkVTable(Sink);

    pub fn writeBatch(self: *Sink, arena: std.mem.Allocator, b: Batch) !void {
        if (self.merging) |m| return m.take(b);
        return self.route(arena, b);
    }

    /// Writes rows shaped as `plan` reads them, each to its partition's file: the
    /// rows are grouped by a counting sort on their partition and each group's
    /// write is freed before the next, so the work and memory are the batch's
    /// whatever the number of partitions in it.
    pub fn route(self: *Sink, arena: std.mem.Allocator, b: Batch) !void {
        return self.routeTo(arena, b, false);
    }

    /// Writes change rows to the table's change data files: rows shaped as `plan`
    /// reads them with their `_change_type` last, routed by partition as `route`
    /// routes rows.
    pub fn routeChanges(self: *Sink, arena: std.mem.Allocator, b: Batch) !void {
        return self.routeTo(arena, b, true);
    }

    fn routeTo(self: *Sink, arena: std.mem.Allocator, b: Batch, cdc: bool) !void {
        if (b.len == 0) return;
        try self.check(b);
        const p = self.plan;
        if (p.parts.len == 0) return self.writeRows(arena, b, null, &.{}, "", cdc);
        const Group = struct { n: usize, end: usize = 0, values: []const ?[]const u8 };
        var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;
        const of = try arena.alloc(u32, b.len);
        var key = std.array_list.Managed(u8).init(arena);
        const vals = try arena.alloc(?[]const u8, p.parts.len);
        for (0..b.len) |r| {
            key.clearRetainingCapacity();
            for (p.parts, vals) |k, *v| {
                v.* = dtypes.partitionText(arena, p.cols[k].phys, p.cols[k].ty.scale, b.columns[p.cols[k].src].getValue(r)) catch |e| {
                    if (e == error.DecimalDropsDigits) return decimal.dropsDigitsFailure(p.cols[k].name, p.cols[k].ty);
                    return e;
                };
                if (v.*) |t| {
                    try key.append(1);
                    try key.appendSlice(t);
                } else try key.append(0);
                try key.append(0xff);
            }
            const gop = try groups.getOrPut(arena, key.items);
            if (!gop.found_existing) {
                gop.key_ptr.* = try arena.dupe(u8, key.items);
                gop.value_ptr.* = .{ .n = 0, .values = try arena.dupe(?[]const u8, vals) };
            }
            of[r] = @intCast(gop.index);
            gop.value_ptr.n += 1;
        }
        var at: usize = 0;
        for (groups.values()) |*g| {
            g.end = at;
            at += g.n;
        }
        const idx = try arena.alloc(usize, b.len);
        for (of, 0..) |gi, r| {
            const g = &groups.values()[gi];
            idx[g.end] = r;
            g.end += 1;
        }
        var each = std.heap.ArenaAllocator.init(self.gpa);
        defer each.deinit();
        for (groups.keys(), groups.values()) |k, g| {
            try self.writeRows(each.allocator(), b, idx[g.end - g.n .. g.end], g.values, k, cdc);
            _ = each.reset(.retain_capacity);
        }
    }

    /// Refuses a NULL in a column the table declares NOT NULL, and an integer
    /// outside a narrower column's range, naming the column.
    fn check(self: *Sink, b: Batch) !void {
        for (self.plan.cols) |c| {
            if (c.src == absent) continue;
            const col = b.columns[c.src];
            const narrow = c.phys.int_bits < 64 and col.data == .i64;
            if (c.nullable and !narrow) continue;
            for (0..b.len) |r| {
                if (!col.validity.get(r)) {
                    if (!c.nullable) return eval.explain(Error.DeltaNullInNotNull, try msgfmt.allocPrint(self.mem.allocator(), "column `{s}` is NOT NULL in {s}, but a row holds NULL in it", .{ c.name, self.plan.name }));
                    continue;
                }
                if (!narrow) continue;
                const x = col.data.i64[r];
                const lim = @as(i64, 1) << @intCast(c.phys.int_bits - 1);
                if (x < -lim or x >= lim)
                    return eval.explain(Error.DeltaValueOutOfRange, try msgfmt.allocPrint(self.mem.allocator(), "column `{s}` is `{s}` in {s}, which cannot hold {d}", .{ c.name, c.delta_type, self.plan.name, x }));
            }
        }
    }

    /// Writes the rows of `b` at `rows` (every row when null) to the file of the
    /// partition `key` names.
    fn writeRows(self: *Sink, arena: std.mem.Allocator, b: Batch, rows: ?[]const usize, values: []const ?[]const u8, part_key: []const u8, cdc: bool) !void {
        const p = self.plan;
        const row_src: ?[2]usize = if (cdc) null else if (p.row) |r| r.src else null;
        const cols = try arena.alloc(column.Column, p.data.len + @as(usize, if (row_src != null) 2 else if (cdc) 1 else 0));
        const n = if (rows) |x| x.len else b.len;
        if (row_src) |rs| for (rs, cols[p.data.len..]) |k, *c| {
            c.* = if (rows) |x| try column.permute(arena, b.columns[k], x) else b.columns[k];
        };
        if (cdc) {
            const kind = b.columns[b.columns.len - 1];
            cols[p.data.len] = if (rows) |x| try column.permute(arena, kind, x) else kind;
        }
        const key = if (cdc) try std.mem.concat(arena, u8, &.{ "\x01", part_key }) else part_key;
        for (p.data, cols[0..p.data.len]) |k, *c| {
            const pc = p.cols[k];
            if (pc.src == absent) {
                c.* = try @import("../../exec/op/join.zig").nullColumn(arena, pc.carried(), n);
                continue;
            }
            var src = b.columns[pc.src];
            if (pc.variant_json) |jt| {
                src = vcol.convert(arena, src, pc.ty, jt, null, .raw) catch |e| {
                    if (e == error.OutOfMemory) return e;
                    const bad = vcol.failed_text;
                    return refuse("column `{s}` holds a variant in {s}, but a row holds {s}, which is not JSON", .{ pc.name, p.name, bad[0..@min(bad.len, 60)] });
                };
            } else if (pc.ty.isNested() and !src.ty.eql(pc.ty)) src = try fit.reshape(arena, src, pc.ty);
            c.* = if (rows) |x| try column.permute(arena, src, x) else src;
        }
        const o = try self.fileFor(key, values, cdc);
        o.writer.writeBatch(arena, .{ .schema = if (cdc) &self.cdc_schema else &self.file_schema, .columns = cols, .len = if (rows) |x| x.len else b.len }) catch |e| return self.explainWrite(e);
        self.tick += 1;
        o.used = self.tick;
        if (o.writer.offset >= roll_bytes) try self.finishFile(key);
    }

    /// Writes the deletion vectors of files an UPSERT keeps (`readds`) to one file
    /// under the table, synced before the commit, and adds each file again with
    /// its vector: its own partition values, row-id base and statistics, these no
    /// longer tight (`tightBounds` false) as some rows are gone.
    fn readd(self: *Sink, readds: []const merge.ReAdd, now: i64) !void {
        const a = self.mem.allocator();
        const p = self.plan;
        var out = dvs.Out.init();
        const descs = try a.alloc([]const u8, readds.len);
        for (descs, readds) |*d, r| d.* = try out.add(a, r.rows);
        const full = try std.mem.concat(a, u8, &.{ p.root, try out.name(a) });
        try self.begun.append(a, full);
        if (try objstore.openRemote(a, full, "application/octet-stream")) |rw| {
            errdefer rw.abort();
            rw.io.writeAll(out.bytes.items) catch |e| return rw.specific(e);
            rw.io.flush() catch |e| return rw.specific(e);
            try rw.finish();
        } else {
            const f = try std.fs.cwd().createFile(full, .{});
            defer f.close();
            try f.writeAll(out.bytes.items);
            try f.sync();
            const dir = dlog.trimDir(p.root);
            if (!self.dirs.contains(dir)) try self.dirs.put(a, try a.dupe(u8, dir), {});
        }
        const w = &self.adds.writer;
        for (readds, descs) |r, d| {
            const f = r.add;
            try w.writeAll("{\"add\":{\"path\":");
            try jsontext.string(w, f.raw);
            try w.writeAll(",\"partitionValues\":");
            if (f.parts) |pv| try std.json.Stringify.value(pv, .{}, w) else try w.writeAll("{}");
            if (f.size) |sz| try w.print(",\"size\":{d}", .{sz});
            try w.print(",\"modificationTime\":{d},\"dataChange\":true", .{now});
            if (f.base_row_id) |b| try w.print(",\"baseRowId\":{d}", .{b});
            if (f.row_version) |v| try w.print(",\"defaultRowCommitVersion\":{d}", .{v});
            if (f.stats) |st| {
                try w.writeAll(",\"stats\":");
                try jsontext.string(w, try looseStats(a, st));
            }
            try w.print(",\"deletionVector\":{s}}}}}\n", .{d});
        }
    }

    fn explainWrite(self: *Sink, e: anyerror) anyerror {
        if (e == pqwrite.Error.UnsupportedParquetDecimal)
            return eval.explain(e, msgfmt.allocPrint(self.mem.allocator(), "a value does not fit its column's type in {s} (a decimal past its precision)", .{self.plan.name}) catch return e);
        if (e == pqwrite.Error.ParquetValueOutOfRange)
            return eval.explain(e, msgfmt.allocPrint(self.mem.allocator(), "a value does not fit its column's type in {s} (a decimal past its precision, or a number inside a list, map or struct past its `integer`, `short` or `byte`)", .{self.plan.name}) catch return e);
        return e;
    }

    fn fileFor(self: *Sink, key: []const u8, values: []const ?[]const u8, cdc: bool) !*Open {
        if (self.files.get(key)) |o| return o;
        if (self.files.count() >= max_open) {
            var oldest: usize = 0;
            for (self.files.values(), 0..) |o, i| if (o.used < self.files.values()[oldest].used) {
                oldest = i;
            };
            try self.finishFile(self.files.keys()[oldest]);
        }
        const mem = try self.gpa.create(std.heap.ArenaAllocator);
        mem.* = std.heap.ArenaAllocator.init(pagecache.process);
        errdefer {
            mem.deinit();
            self.gpa.destroy(mem);
        }
        const a = mem.allocator();
        var aw = std.Io.Writer.Allocating.init(a);
        const w = &aw.writer;
        if (cdc) try w.writeAll(change_dir);
        for (self.plan.parts, values) |k, v| {
            try escapePath(w, self.plan.cols[k].fileName());
            try w.writeByte('=');
            if (v) |t| try escapePath(w, t) else try w.writeAll("__HIVE_DEFAULT_PARTITION__");
            try w.writeByte('/');
        }
        try w.print("{s}-{s}{s}", .{ if (cdc) "cdc" else "part", &dlog.uuid(), switch (self.plan.codec) {
            .snappy => ".snappy.parquet",
            .gzip => ".gz.parquet",
            else => ".parquet",
        } });
        const rel = aw.written();
        const full = try std.mem.concat(a, u8, &.{ self.plan.root, rel });
        if (!objstore.isUrl(full)) if (std.fs.path.dirname(full)) |d| try std.fs.cwd().makePath(d);
        const writer = try pqwrite.Writer.openPhysical(a, full, if (cdc) self.cdc_schema else self.file_schema, self.plan.codec, .truncate, if (cdc) self.cdc_phys else self.phys);
        writer.field_ids = self.plan.field_ids;
        try self.begun.append(self.mem.allocator(), try self.mem.allocator().dupe(u8, full));
        const o = try a.create(Open);
        const kept = try a.alloc(?[]const u8, values.len);
        for (kept, values) |*k, v| k.* = if (v) |t| try a.dupe(u8, t) else null;
        o.* = .{ .mem = mem, .writer = writer, .rel = rel, .full = full, .values = kept, .used = self.tick, .cdc = cdc };
        try self.files.put(self.mem.allocator(), try a.dupe(u8, key), o);
        return o;
    }

    /// Closes a file, syncs a local one to disk, and writes its `add` action.
    fn finishFile(self: *Sink, key: []const u8) !void {
        const o = self.files.get(key).?;
        _ = self.files.orderedRemove(key);
        const mem = o.mem;
        defer {
            mem.deinit();
            self.gpa.destroy(mem);
        }
        const a = o.mem.allocator();
        const sum = o.writer.closeSummary(a) catch |e| {
            o.writer.abort();
            return self.explainWrite(e);
        };
        if (!objstore.isUrl(o.full)) {
            const f = try std.fs.cwd().openFile(o.full, .{});
            defer f.close();
            try f.sync();
            const top = dlog.trimDir(self.plan.root);
            var d = std.fs.path.dirname(o.full);
            while (d) |x| : (d = std.fs.path.dirname(x)) {
                const gop = try self.dirs.getOrPut(self.mem.allocator(), x);
                if (gop.found_existing) break;
                gop.key_ptr.* = try self.mem.allocator().dupe(u8, x);
                if (x.len <= top.len) break;
            }
        }
        const p = self.plan;
        const w = &self.adds.writer;
        try w.writeAll(if (o.cdc) "{\"cdc\":{\"path\":" else "{\"add\":{\"path\":");
        try jsontext.string(w, try uriPath(a, o.rel));
        try w.writeAll(",\"partitionValues\":{");
        for (p.parts, o.values, 0..) |k, v, i| {
            if (i > 0) try w.writeByte(',');
            try jsontext.string(w, p.cols[k].fileName());
            try w.writeByte(':');
            if (v) |t| try jsontext.string(w, t) else try w.writeAll("null");
        }
        if (o.cdc) {
            try w.print("}},\"size\":{d},\"dataChange\":false}}}}\n", .{sum.bytes});
            return;
        }
        try w.print("}},\"size\":{d},\"modificationTime\":{d},\"dataChange\":{s},", .{ sum.bytes, std.time.milliTimestamp(), if (self.compacting == null) "true" else "false" });
        if (p.row != null) {
            try w.print("\"baseRowId\":\"@@B{d}@@\",\"defaultRowCommitVersion\":\"@@V@@\",", .{self.row_counts.items.len});
            try self.row_counts.append(self.mem.allocator(), @intCast(sum.rows));
        }
        try w.writeAll("\"stats\":");
        try jsontext.string(w, try statsJson(a, p, sum));
        try w.writeAll("}}\n");
        self.n_added += 1;
    }

    pub fn close(self: *Sink) !void {
        defer self.destroy();
        errdefer self.discard();
        const p = self.plan;
        const a = self.mem.allocator();
        var removes: []const delta.Add = &.{};
        var read_set: []const []const u8 = &.{};
        var readds: []const merge.ReAdd = &.{};
        if (self.merging) |m| {
            if (m.rows > 0) {
                const done = try m.finish(self);
                self.spilled = m.spilled;
                removes = done.removed;
                read_set = done.read_set;
                readds = done.readds;
            } else if (p.snap != null) return;
        } else if (p.mode == .replace) if (p.snap) |st| {
            removes = st.files.values();
            const rs = try a.alloc([]const u8, removes.len);
            for (rs, removes) |*r, f| r.* = f.raw;
            read_set = rs;
        };
        if (self.compacting) |files| {
            removes = files;
            const rs = try a.alloc([]const u8, files.len);
            for (rs, files) |*r, f| r.* = f.raw;
            read_set = rs;
        }
        while (self.files.count() > 0) try self.finishFile(self.files.keys()[0]);
        if (p.snap != null and self.n_added == 0 and ((p.mode == .append and self.compacting == null) or removes.len == 0)) return;
        const now = std.time.milliTimestamp();
        const w = &self.adds.writer;
        for (removes) |f| {
            try w.writeAll("{\"remove\":{\"path\":");
            try jsontext.string(w, f.raw);
            try w.print(",\"deletionTimestamp\":{d},\"dataChange\":{s},\"extendedFileMetadata\":true,\"partitionValues\":", .{ now, if (self.compacting == null) "true" else "false" });
            if (f.parts) |pv| try std.json.Stringify.value(pv, .{}, w) else try w.writeAll("{}");
            if (f.size) |s| try w.print(",\"size\":{d}", .{s});
            if (f.dv) |dv| {
                try w.writeAll(",\"deletionVector\":");
                try std.json.Stringify.value(dv, .{}, w);
            }
            try w.writeAll("}}\n");
        }
        if (readds.len > 0) try self.readd(readds, now);
        if (p.row != null and self.row_counts.items.len > 0)
            try w.writeAll("{\"domainMetadata\":{\"domain\":\"delta.rowTracking\",\"configuration\":\"{\\\"rowIdHighWaterMark\\\":@@H@@}\",\"removed\":false}}\n");
        for (self.dirs.keys()) |d| try dlog.syncDir(d);
        var ts = dlog.TableStore{ .root = p.root };
        self.committed = dlog.commit(a, ts.store(), p.name, .{
            .kind = if (p.snap == null) .create else if (self.compacting != null) .upsert else switch (p.mode) {
                .append => .append,
                .replace => .replace,
                .upsert => .upsert,
            },
            .read_version = if (p.snap) |st| st.version else null,
            .txn_id = &self.txn,
            .body = self.adds.written(),
            .added = self.begun.items,
            .read_set = read_set,
            .key_root = if (p.snap) |st| st.key_root else "",
            .hold = if (self.merging) |m| m.hold() else null,
            .ict = self.ict,
            .rows = if (p.row) |r| (if (self.row_counts.items.len > 0) dlog.RowIds{ .hwm = r.hwm, .counts = self.row_counts.items } else null) else null,
        }) catch |e| {
            if (e == dlog.Error.DeltaCommitUnknown or e == dlog.Error.DeltaCommitUnsynced or e == dlog.Error.DeltaStoreUnsafe) self.begun.clearRetainingCapacity();
            return e;
        };
        self.begun.clearRetainingCapacity();
        self.checkpointAfter();
    }

    /// The table's checkpoint at the version just committed, when that version is
    /// one the table checkpoints at; a failure is warned of, as the commit stands.
    fn checkpointAfter(self: *Sink) void {
        const v = self.committed orelse return;
        const every = if (self.plan.snap) |s| ckpt.interval(s) else ckpt.default_interval;
        if (!ckpt.due(v, every)) return;
        var mem = std.heap.ArenaAllocator.init(pagecache.process);
        defer mem.deinit();
        const now = std.time.milliTimestamp();
        const done = ckpt.write(mem.allocator(), self.plan.root, v, now) catch |e| {
            const why = eval.takeFailure(e) orelse @errorName(e);
            const w = self.warn orelse return;
            const msg = msgfmt.allocPrint(mem.allocator(), "{s} committed version {d}, but its checkpoint could not be written ({s}); the table reads as before, from its commits", .{ self.plan.name, v, why }) catch return;
            w.f(w.ctx, msg);
            return;
        };
        const keep = done.keep_log_ms orelse return;
        _ = ckpt.cleanLog(mem.allocator(), self.plan.root, v, keep, now) catch |e| {
            const why = eval.takeFailure(e) orelse @errorName(e);
            const w = self.warn orelse return;
            const msg = msgfmt.allocPrint(mem.allocator(), "{s} wrote its checkpoint at version {d}, but its expired log files could not all be deleted ({s}); the next checkpoint tries again", .{ self.plan.name, v, why }) catch return;
            w.f(w.ctx, msg);
        };
    }

    pub fn abort(self: *Sink) void {
        if (self.done) return;
        self.discard();
        self.destroy();
    }

    /// Drops the open files and removes the local data files begun, with the
    /// folders they leave empty (the root only if this write made it); remote ones
    /// are left, referenced by no version.
    fn discard(self: *Sink) void {
        for (self.files.values()) |o| {
            o.writer.abort();
            const mem = o.mem;
            mem.deinit();
            self.gpa.destroy(mem);
        }
        self.files.clearRetainingCapacity();
        const top = dlog.trimDir(self.plan.root);
        for (self.begun.items) |f| if (!objstore.isUrl(f)) {
            std.fs.cwd().deleteFile(f) catch {};
            var d = std.fs.path.dirname(f);
            while (d) |x| : (d = std.fs.path.dirname(x)) {
                if (x.len <= top.len) break;
                std.fs.cwd().deleteDir(x) catch break;
            }
        };
        if (self.made_root) std.fs.cwd().deleteDir(top) catch {};
        self.begun.clearRetainingCapacity();
    }

    fn destroy(self: *Sink) void {
        if (self.merging) |m| m.deinit();
        self.merging = null;
        self.adds.deinit();
        self.mem.deinit();
        self.done = true;
    }
};

test "partition folders: Spark's escaping, NULL as the Hive default, the log path URI-encoded" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var aw = std.Io.Writer.Allocating.init(a);
    try escapePath(&aw.writer, "2026-01-01 00:00:00/a=b#c%");
    try testing.expectEqualStrings("2026-01-01 00%3A00%3A00%2Fa%3Db%23c%25", aw.written());
    try testing.expectEqualStrings("ts=2026-01-01%2000%253A00%253A00/part.parquet", try uriPath(a, "ts=2026-01-01 00%3A00%3A00/part.parquet"));
    try testing.expectEqualStrings("r=%C3%A9/x%2By.parquet", try uriPath(a, "r=é/x+y.parquet"));
}

test "a file's statistics stay JSON when a string column holds bytes that are not UTF-8" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const cols = [_]Col{
        .{ .name = "s\xff", .delta_type = "string", .ty = types.Type.init(.string), .src = 0 },
        .{ .name = "n", .delta_type = "long", .ty = types.Type.init(.int), .src = 1 },
    };
    const p = Plan{ .name = "T", .root = "/t/", .mode = .append, .codec = .uncompressed, .snap = null, .cols = &cols, .parts = &.{}, .data = &.{ 0, 1 }, .schema_string = "" };
    var cs = [_]pqwrite.Writer.ColumnStats{
        .{ .nulls = 0, .min = .{ .string = "a\xff" }, .max = .{ .string = "z\xfe" } },
        .{ .nulls = 1, .min = .{ .int = -3 }, .max = .{ .int = 9 } },
    };
    const text = try statsJson(a, &p, .{ .rows = 2, .bytes = 10, .columns = &cs });
    const v = try std.json.parseFromSliceLeaky(std.json.Value, a, text, .{});
    try testing.expect(v.object.get("minValues").?.object.get("s\u{fffd}") == null);
    try testing.expectEqual(@as(i64, -3), v.object.get("minValues").?.object.get("n").?.integer);
    try testing.expectEqual(@as(i64, 9), v.object.get("maxValues").?.object.get("n").?.integer);
    try testing.expectEqual(@as(i64, 0), v.object.get("nullCount").?.object.get("s\u{fffd}").?.integer);
    var line = std.Io.Writer.Allocating.init(a);
    try jsontext.string(&line.writer, text);
    _ = try std.json.parseFromSliceLeaky(std.json.Value, a, line.written(), .{});
}

test "a table's column types from delta_types equal the writer's old tables" {
    const Orig = struct {
        const oracle = struct {
            fn scalarType(t: []const u8) ?types.Type {
                const Simple = struct { []const u8, types.TypeKind };
                for ([_]Simple{
                    .{ "long", .int },     .{ "integer", .int }, .{ "short", .int },           .{ "byte", .int },
                    .{ "double", .float }, .{ "float", .float }, .{ "boolean", .bool },        .{ "string", .string },
                    .{ "binary", .bytes }, .{ "date", .date },   .{ "timestamp", .timestamp }, .{ "timestamp_ntz", .timestamp },
                }) |x| if (std.mem.eql(u8, t, x[0])) return types.Type.init(x[1]);
                return parseDecimal(t);
            }

            fn parseDecimal(t: []const u8) ?types.Type {
                if (std.mem.eql(u8, t, "decimal")) return types.Type.decimal(10, 0);
                if (!std.mem.startsWith(u8, t, "decimal(") or !std.mem.endsWith(u8, t, ")")) return null;
                var it = std.mem.splitScalar(u8, t["decimal(".len .. t.len - 1], ',');
                const p = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch return null;
                const s = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch return null;
                if (p < 1 or p > 38 or s > p) return null;
                return types.Type.decimal(p, s);
            }

            fn phys(t: []const u8) pqwrite.Physical {
                var ph = pqwrite.Physical{};
                const Ints = struct { []const u8, u8 };
                for ([_]Ints{ .{ "integer", 32 }, .{ "short", 16 }, .{ "byte", 8 } }) |x| if (std.mem.eql(u8, t, x[0])) {
                    ph.int_bits = x[1];
                };
                ph.float32 = std.mem.eql(u8, t, "float");
                ph.utc = std.mem.eql(u8, t, "timestamp");
                return ph;
            }

            fn createdName(t: types.Type) ?[]const u8 {
                return switch (t.kind) {
                    .bool => "boolean",
                    .int => "long",
                    .float => "double",
                    .string => "string",
                    .bytes => "binary",
                    .date => "date",
                    .timestamp => "timestamp",
                    else => null,
                };
            }
        };
    };
    const names = [_][]const u8{
        "long",           "integer",      "short",          "byte",          "double",        "float",          "boolean",
        "string",         "binary",       "date",           "timestamp",     "timestamp_ntz", "void",           "variant",
        "Long",           "",             "interval",       "timestamp_tz",  "decimal",       "decimal(10,2)",  "decimal( 10 , 2 )",
        "decimal(38,38)", "decimal(1,0)", "decimal(5,2,9)", "decimal(10)",   "decimal(,2)",   "decimal(39,0)",  "decimal(5,6)",
        "decimal(0,0)",   "decimal()",    "decimal(10,2",   "decimal(-1,0)", "decimal(a,b)",  "decimal(256,0)", "Decimal(10,2)",
    };
    for (names) |t| {
        const old = Orig.oracle.scalarType(t);
        const new = dtypes.scalarType(t, .write);
        try testing.expectEqual(old == null, new == null);
        if (old) |o| {
            try testing.expectEqual(o, new.?);
            try testing.expectEqual(Orig.oracle.phys(t), dtypes.physical(t));
        }
    }
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    inline for (std.meta.fields(types.TypeKind)) |f| {
        const k: types.TypeKind = @enumFromInt(f.value);
        if (Orig.oracle.createdName(types.Type.init(k))) |n| {
            const c = try createCol(ar.allocator(), "T", .{ .name = "c", .ty = types.Type.init(k) }, 0);
            try testing.expectEqualStrings(n, c.delta_type);
            try testing.expectEqual(pqwrite.Physical{ .utc = k == .timestamp }, c.phys);
        }
    }
    const T = types.Type;
    for ([_]struct { T, []const u8 }{ .{ T.decimal(0, 4), "decimal(38,4)" }, .{ T.decimal(60, 2), "decimal(38,2)" }, .{ T.decimal(12, 3), "decimal(12,3)" } }) |x| {
        const c = try createCol(ar.allocator(), "T", .{ .name = "c", .ty = x[0] }, 0);
        try testing.expectEqualStrings(x[1], c.delta_type);
    }
    try testing.expectError(table.Error.LakeTableRefused, createCol(ar.allocator(), "T", .{ .name = "c", .ty = T.decimal(60, 39) }, 0));
    try testing.expectEqualStrings("column `c` is decimal(38,39), whose scale exceeds its precision", eval.takeFailure(table.Error.LakeTableRefused).?);
}

test "a table's column types: narrower integers and floats, decimals that hold every value" {
    const F = struct {
        fn of(t: []const u8) Field {
            return .{ .name = "c", .ty = .{ .string = t }, .nullable = true, .metadata = null };
        }
    };
    const i = try tableCol(testing.failing_allocator, "T", F.of("integer"), types.Type.init(.int), 0);
    try testing.expectEqual(@as(u8, 32), i.phys.int_bits);
    try testing.expect((try tableCol(testing.failing_allocator, "T", F.of("float"), types.Type.init(.float), 0)).phys.float32);
    try testing.expect((try tableCol(testing.failing_allocator, "T", F.of("timestamp"), types.Type.init(.timestamp), 0)).phys.utc);
    try testing.expect(!(try tableCol(testing.failing_allocator, "T", F.of("timestamp_ntz"), types.Type.init(.timestamp), 0)).phys.utc);
    const d = try tableCol(testing.failing_allocator, "T", F.of("decimal(12,3)"), types.Type.decimal(10, 2), 0);
    try testing.expectEqual(@as(u8, 3), d.ty.scale);
    try testing.expectError(table.Error.LakeTableRefused, tableCol(testing.failing_allocator, "T", F.of("decimal(12,3)"), types.Type.decimal(12, 2), 0));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "cannot hold every value") != null);
    try testing.expectError(table.Error.LakeTableRefused, tableCol(testing.failing_allocator, "T", F.of("date"), types.Type.init(.string), 0));
    try testing.expectEqualStrings("column `c` is string in the rows but `date` in T; cast it", eval.takeFailure(table.Error.LakeTableRefused).?);
    var var_ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer var_ar.deinit();
    const v = try tableCol(var_ar.allocator(), "T", F.of("variant"), types.Type.init(.string), 0);
    try testing.expect(v.variant_json != null and v.ty.kind == .string and v.file_ty.?.kind == .@"struct");
    try testing.expectError(table.Error.LakeTableRefused, tableCol(var_ar.allocator(), "T", F.of("variant"), types.Type.init(.int), 0));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "give it as JSON text") != null);
}

test "writable: writer features by name, enforced column metadata, constraints, append-only" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var st = delta.State{ .arena = a, .name = "T", .key_root = "/t/" };
    try writable(&st, &.{}, .replace);
    st.writer_version = 7;
    st.writer_features = &.{ "appendOnly", "deletionVectors", "timestampNtz" };
    try writable(&st, &.{}, .append);
    for ([_][]const u8{"fancyNew"}) |f| {
        st.writer_features = &.{f};
        try testing.expectError(table.Error.LakeTableRefused, writable(&st, &.{}, .append));
        try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, f) != null);
    }
    st.writer_features = &.{};
    var md = std.json.ObjectMap.init(a);
    try md.put("delta.generationExpression", .{ .string = "a + 1" });
    try testing.expectError(table.Error.LakeTableRefused, writable(&st, &.{.{ .name = "g", .ty = .{ .string = "long" }, .nullable = true, .metadata = md }}, .append));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "column `g` is a generation expression") != null);
    var cfg = std.json.ObjectMap.init(a);
    try cfg.put("delta.appendOnly", .{ .string = "true" });
    st.config = .{ .object = cfg };
    try writable(&st, &.{}, .append);
    try testing.expectError(table.Error.LakeTableRefused, writable(&st, &.{}, .replace));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "append-only") != null);
    try cfg.put("delta.constraints.pos", .{ .string = "x > 0" });
    st.config = .{ .object = cfg };
    try testing.expectError(table.Error.LakeTableRefused, writable(&st, &.{}, .append));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "CHECK constraint `pos`") != null);
}
