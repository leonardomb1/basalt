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
//! as UTC-adjusted microseconds and a `timestamp_ntz` as local ones.
//!
//! A partitioned table's rows are routed by their partition values, each to a
//! file of its own under Hive-style `col=value/` folders (Spark's escaping, with
//! `__HIVE_DEFAULT_PARTITION__` for NULL); partition columns are not written into
//! the files, as the log carries them. At most `max_open` files are open at once,
//! the least recently written closed when another is needed, and a file is closed
//! past `roll_bytes`, so memory is bounded by `max_open` Parquet row groups (and
//! an object store's upload buffer each), whatever the number of partitions.
//! Each `add` carries the file's statistics: `numRecords`, and per data column
//! `nullCount`, with `minValues` and `maxValues` where they are exact and cheap
//! (numbers, dates, timestamps cut to milliseconds as the protocol allows, strings
//! of up to 32 bytes; a longer string's minimum is its 32-byte prefix).
//!
//! An UPSERT matches rows by its key columns and rewrites the files holding a
//! matched row (`delta_merge.zig`); under PARTIAL COLS the rows may leave out
//! table columns other than the key and the listed ones.
//!
//! No checkpoint is written: the Parquet writer writes flat columns, and a
//! checkpoint's actions are nested structs and maps.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Value = @import("../../exec/value.zig").Value;
const cast = @import("../../exec/eval/cast.zig");
const codecs = @import("../compress/codecs.zig");
const column = @import("../../exec/column.zig");
const delta = @import("delta.zig");
const dlog = @import("delta_log.zig");
const merge = @import("delta_merge.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const folder = @import("../../connect/folder.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const objstore = @import("../../store/objstore.zig");
const pqwrite = @import("../parquet/write.zig");
const ast = @import("../../lang/ast.zig");
const azure = @import("../../store/azure.zig");
const s3 = @import("../../store/s3.zig");
const table = @import("table.zig");
const time = @import("../../exec/eval/time.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

const refuse = table.refuse;

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
        const on = if (u.keys.len == 0) "upsert (key from the source)" else try std.fmt.allocPrint(arena, "upsert on ({s})", .{keys});
        const pc = u.partial orelse return on;
        return std.fmt.allocPrint(arena, "{s} partial cols ({s})", .{ on, try std.mem.join(arena, ", ", pc) });
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

/// A table column the rows leave out (`Col.src`), as PARTIAL COLS allows.
pub const absent = std.math.maxInt(usize);

/// Files open for writing at once; past it the least recently written is closed.
pub const max_open = 16;

/// A data file is closed once this much Parquet is written to it.
pub const roll_bytes: i64 = 128 << 20;

/// A string longer than this has no maximum in its file's statistics, and its
/// prefix of this many bytes as its minimum.
pub const stat_prefix = 32;

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

    /// What EXPLAIN and `check` add after the root.
    pub fn label(self: *const Plan, arena: std.mem.Allocator) ![]const u8 {
        const st = self.snap orelse return "created by this write";
        return std.fmt.allocPrint(arena, "version {d}, {d} file{s}", .{ st.version, st.files.count(), if (st.files.count() == 1) "" else "s" });
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
/// (`check` cannot always know it) only the table itself is judged.
pub fn plan(arena: std.mem.Allocator, root: []const u8, mode: Mode, create: bool, partition_by: ?[]const []const u8, codec: codecs.Codec, schema_in: ?types.Schema) !*Plan {
    const name = try std.fmt.allocPrint(arena, "Delta table `{s}`", .{root});
    if (storeProblem(root)) |why| return refuse("cannot write {s}: {s}", .{ name, why });
    const log_path = try std.mem.concat(arena, u8, &.{ root, delta.log_dir });
    const listed: []const []const u8 = folder.listRaw(arena, log_path) catch |e|
        if (table.absent(e) or e == azure.Error.AzureContainerMissing or e == s3.Error.S3BucketMissing) &.{} else return e;
    const p = try arena.create(Plan);
    if (!delta.hasVersions(listed, log_path)) {
        if (!create) return refuse("folder `{s}` holds no _delta_log/, so it is no Delta table; `WITH (format = 'delta')` creates one there", .{root});
        p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = null, .cols = &.{}, .parts = &.{}, .data = &.{}, .schema_string = "" };
        if (schema_in) |schema| p.* = try planCreate(arena, name, root, mode, partition_by, codec, schema);
        return p;
    }
    const st = try delta.snapshot(arena, root, .{}, listed);
    try st.checkProtocol();
    const fields = try tableFields(arena, name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{name}));
    try writable(st, fields, mode);
    if (partition_by) |want| if (!sameNames(want, st.part_cols))
        return refuse("{s} is partitioned by ({s}), not ({s}); `partition_by` applies only when a write creates the table", .{ name, try std.mem.join(arena, ", ", st.part_cols), try std.mem.join(arena, ", ", want) });
    p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = st, .cols = &.{}, .parts = &.{}, .data = &.{}, .schema_string = st.schema_string.? };
    const schema = schema_in orelse return p;
    const partial = mode == .upsert and mode.upsert.partial != null;
    const cols = try arena.alloc(Col, fields.len);
    for (fields, cols) |f, *c| {
        const src = indexOf(schema, f.name) orelse {
            if (!partial) return refuse("{s} has column `{s}`, which the rows lack; a write gives every column of the table a value (`CAST(NULL AS ...) AS {s}` leaves it empty)", .{ name, f.name, f.name });
            var none = types.Type.init(.string);
            none.unknown = true;
            c.* = try tableCol(name, f, none, absent);
            continue;
        };
        c.* = try tableCol(name, f, schema.fields[src].ty, src);
    }
    for (schema.fields) |f| if (indexOfField(fields, f.name) == null)
        return refuse("the rows have column `{s}`, which {s} lacks; basalt does not add columns to a Delta table", .{ f.name, name });
    p.* = .{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = st, .cols = cols, .parts = &.{}, .data = &.{}, .schema_string = st.schema_string.?, .in_fields = schema.fields };
    try split(p, arena, st.part_cols);
    try upsertCols(p, arena);
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

fn colIndex(cols: []const Col, name: []const u8) ?usize {
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
    if (schema.fields.len == 0) return refuse("{s} would have no columns", .{name});
    const cols = try arena.alloc(Col, schema.fields.len);
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.writeAll("{\"type\":\"struct\",\"fields\":[");
    for (schema.fields, cols, 0..) |f, *c, i| {
        c.* = try createCol(arena, name, f, i);
        if (i > 0) try w.writeByte(',');
        try w.writeAll("{\"name\":");
        try jsonString(w, f.name);
        try w.print(",\"type\":\"{s}\",\"nullable\":true,\"metadata\":{{}}}}", .{c.delta_type});
    }
    try w.writeAll("]}");
    var p = Plan{ .name = name, .root = root, .mode = mode, .codec = codec, .snap = null, .cols = cols, .parts = &.{}, .data = &.{}, .schema_string = aw.written(), .in_fields = schema.fields };
    try split(&p, arena, partition_by orelse &.{});
    try upsertCols(&p, arena);
    return p;
}

/// A basalt column as a new table's column.
fn createCol(arena: std.mem.Allocator, name: []const u8, f: types.Schema.Field, src: usize) !Col {
    const t = f.ty;
    if (t.unknown) return refuse("column `{s}` has no type (a bare NULL); CAST it to the type {s} should hold", .{ f.name, name });
    const dt: []const u8 = switch (t.kind) {
        .bool => "boolean",
        .int => "long",
        .float => "double",
        .string => "string",
        .bytes => "binary",
        .date => "date",
        .timestamp => "timestamp",
        .decimal => {
            const p: u8 = if (t.precision == 0) 38 else @min(38, t.precision);
            if (t.scale > p) return refuse("column `{s}` is decimal({d},{d}), whose scale exceeds its precision", .{ f.name, p, t.scale });
            return .{ .name = f.name, .delta_type = try std.fmt.allocPrint(arena, "decimal({d},{d})", .{ p, t.scale }), .ty = types.Type.decimal(p, t.scale), .src = src };
        },
        .time => return refuse("column `{s}` is a time of day, which Delta has no type for; cast it to a string or store microseconds as a number", .{f.name}),
        .array, .@"struct" => return refuse("column `{s}` is {s}; basalt writes Delta tables of scalar columns", .{ f.name, if (t.kind == .array) "an array" else "a struct" }),
    };
    return .{ .name = f.name, .delta_type = dt, .ty = types.Type.init(t.kind), .phys = .{ .utc = t.kind == .timestamp }, .src = src };
}

/// A field of a table's `schemaString`.
const Field = struct {
    name: []const u8,
    ty: std.json.Value,
    nullable: bool,
    metadata: ?std.json.ObjectMap,
};

fn tableFields(arena: std.mem.Allocator, name: []const u8, text: []const u8) ![]const Field {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch
        return refuse("{s} has a schemaString that is not JSON", .{name});
    const fields = if (v == .object) v.object.get("fields") else null;
    if (fields == null or fields.? != .array) return refuse("{s} has a schemaString that is not a struct of fields", .{name});
    const out = try arena.alloc(Field, fields.?.array.items.len);
    for (fields.?.array.items, out) |f, *o| {
        if (f != .object) return refuse("{s} has a schemaString field that is not an object", .{name});
        const n = f.object.get("name") orelse return refuse("{s} has a schemaString field with no name", .{name});
        if (n != .string) return refuse("{s} has a schemaString field with no name", .{name});
        const nl = f.object.get("nullable");
        const md = f.object.get("metadata");
        o.* = .{
            .name = n.string,
            .ty = f.object.get("type") orelse .null,
            .nullable = if (nl) |x| (x != .bool or x.bool) else true,
            .metadata = if (md) |m| (if (m == .object) m.object else null) else null,
        };
    }
    return out;
}

/// A table column `f` as the rows' column of type `in` is written to it.
fn tableCol(name: []const u8, f: Field, in: types.Type, src: usize) !Col {
    const t = if (f.ty == .string) f.ty.string else return refuse("{s} has column `{s}` of a nested type; basalt writes Delta tables of scalar columns", .{ name, f.name });
    var c = Col{ .name = f.name, .delta_type = t, .ty = scalarType(t) orelse
        return refuse("{s} has column `{s}` of type `{s}`, which basalt does not write", .{ name, f.name, t }), .nullable = f.nullable, .src = src };
    const Ints = struct { []const u8, u8 };
    for ([_]Ints{ .{ "integer", 32 }, .{ "short", 16 }, .{ "byte", 8 } }) |x| if (std.mem.eql(u8, t, x[0])) {
        c.phys.int_bits = x[1];
    };
    c.phys.float32 = std.mem.eql(u8, t, "float");
    c.phys.utc = std.mem.eql(u8, t, "timestamp");
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

/// The basalt type a Delta primitive's values are, null for one basalt does not write.
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

/// The writer features basalt honours: the ones that ask nothing of a writer
/// that adds and removes whole files, and the ones whose demands `writable`
/// checks the table does not make.
const honoured = [_][]const u8{
    "appendOnly",          "invariants",          "checkConstraints", "changeDataFeed", "generatedColumns",
    "columnMapping",       "identityColumns",     "deletionVectors",  "timestampNtz",   "domainMetadata",
    "vacuumProtocolCheck", "allowColumnDefaults",
};

/// Refuses a table that asks a writer for what basalt does not do.
fn writable(st: *const delta.State, fields: []const Field, mode: Mode) !void {
    if (st.writer_version > 7)
        return refuse("{s} needs Delta writer version {d}; basalt writes writer versions 1 to 7", .{ st.name, st.writer_version });
    if (st.writer_version == 7) for (st.writer_features) |f| {
        if (oneOf(f, &honoured)) continue;
        return refuse("{s} uses writer feature `{s}`, which basalt does not honour, so it does not write the table", .{ st.name, f });
    };
    if (st.mapping) |m| if (!std.mem.eql(u8, m, "none"))
        return refuse("{s} uses column mapping (mode `{s}`), which basalt does not write", .{ st.name, m });
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
    if (mode != .append) if (st.property("delta.appendOnly")) |v| if (std.ascii.eqlIgnoreCase(v, "true"))
        return refuse("{s} is append-only (`delta.appendOnly`), so {s} cannot remove its files", .{ st.name, if (mode == .replace) "a REPLACE" else "an UPSERT" });
    if (mode == .upsert) if (st.property("delta.enableChangeDataFeed")) |v| if (std.ascii.eqlIgnoreCase(v, "true"))
        return refuse("{s} records a change data feed (`delta.enableChangeDataFeed`), which an UPSERT's rewritten files would misreport (every row of a rewritten file as deleted and inserted); basalt does not write the change files that would say which rows changed", .{st.name});
}

fn oneOf(s: []const u8, set: []const []const u8) bool {
    for (set) |x| if (std.mem.eql(u8, s, x)) return true;
    return false;
}

fn jsonString(w: *std.Io.Writer, s: []const u8) !void {
    try std.json.Stringify.encodeJsonString(s, .{}, w);
}

/// `us` as `YYYY-MM-DD HH:MM:SS.ffffff`, with `T` for the space and a trailing `Z`
/// when `utc`; `ms` cuts the fraction to milliseconds.
fn writeTimestamp(w: *std.Io.Writer, us: i64, utc: bool, ms: bool) !void {
    const day = @divFloor(us, 86_400_000_000);
    const rest: u64 = @intCast(us - day * 86_400_000_000);
    const c = time.civilFromDays(day);
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}{c}{d:0>2}:{d:0>2}:{d:0>2}", .{ @as(u64, @intCast(@max(c.y, 0))), c.m, c.d, @as(u8, if (utc) 'T' else ' '), rest / 3_600_000_000, rest / 60_000_000 % 60, rest / 1_000_000 % 60 });
    if (ms) try w.print(".{d:0>3}", .{rest / 1000 % 1000}) else try w.print(".{d:0>6}", .{rest % 1_000_000});
    if (utc) try w.writeByte('Z');
}

fn writeDate(w: *std.Io.Writer, days: i64) !void {
    const c = time.civilFromDays(days);
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u64, @intCast(@max(c.y, 0))), c.m, c.d });
}

/// A partition value as the log's text, null for NULL (and for an empty string,
/// which Delta reads as NULL).
fn partitionText(arena: std.mem.Allocator, c: Col, v: Value) !?[]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    switch (v) {
        .null => return null,
        .string => |s| return if (s.len == 0) null else s,
        .int => |i| try w.print("{d}", .{i}),
        .float => |f| if (c.phys.float32) try w.print("{d}", .{@as(f32, @floatCast(f))}) else try w.print("{d}", .{f}),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .date => |d| try writeDate(w, d),
        .timestamp => |us| try writeTimestamp(w, us, c.phys.utc, false),
        .decimal => |d| {
            const r = cast.rescaleTo(d, c.ty.scale) orelse return Error.DeltaValueOutOfRange;
            try fmtnum.writeDecimal(w, r.unscaled, r.scale);
        },
        else => return Error.DeltaValueOutOfRange,
    }
    return aw.written();
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
    var aw = std.Io.Writer.Allocating.init(arena);
    for (rel) |ch| {
        if (std.ascii.isAlphanumeric(ch) or std.mem.indexOfScalar(u8, "-._~/=", ch) != null) try aw.writer.writeByte(ch) else try aw.writer.print("%{X:0>2}", .{ch});
    }
    return aw.written();
}

/// A statistics value as JSON, null where it is left out.
fn statJson(arena: std.mem.Allocator, c: Col, v: Value, is_max: bool) !?[]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    switch (v) {
        .int => |i| try w.print("{d}", .{i}),
        .float => |f| {
            if (!std.math.isFinite(f)) return null;
            if (c.phys.float32) try w.print("{d}", .{@as(f32, @floatCast(f))}) else try w.print("{d}", .{f});
        },
        .decimal => |d| try fmtnum.writeDecimal(w, d.unscaled, d.scale),
        .date => |d| {
            try w.writeByte('"');
            try writeDate(w, d);
            try w.writeByte('"');
        },
        .timestamp => |us| {
            try w.writeByte('"');
            try writeTimestamp(w, us, c.phys.utc, true);
            try w.writeByte('"');
        },
        .string => |s| {
            if (s.len > stat_prefix) {
                if (is_max) return null;
                var n: usize = stat_prefix;
                while (n > 0 and (s[n] & 0xC0) == 0x80) n -= 1;
                try jsonString(w, s[0..n]);
            } else try jsonString(w, s);
        },
        else => return null,
    }
    return aw.written();
}

fn statsJson(arena: std.mem.Allocator, p: *const Plan, sum: pqwrite.Writer.Summary) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.print("{{\"numRecords\":{d}", .{sum.rows});
    inline for (.{ "minValues", "maxValues" }, .{ false, true }) |key, is_max| {
        try w.print(",\"{s}\":{{", .{key});
        var first = true;
        for (p.data, sum.columns) |k, cs| {
            const v = (if (is_max) cs.max else cs.min) orelse continue;
            const text = (try statJson(arena, p.cols[k], v, is_max)) orelse continue;
            if (!first) try w.writeByte(',');
            first = false;
            try jsonString(w, p.cols[k].name);
            try w.writeByte(':');
            try w.writeAll(text);
        }
        try w.writeByte('}');
    }
    try w.writeAll(",\"nullCount\":{");
    for (p.data, sum.columns, 0..) |k, cs, i| {
        if (i > 0) try w.writeByte(',');
        try jsonString(w, p.cols[k].name);
        try w.print(":{d}", .{cs.nulls});
    }
    try w.writeAll("}}");
    return aw.written();
}

/// A data file the write made.
const Added = struct {
    rel: []const u8,
    values: []const ?[]const u8,
    size: u64,
    stats: []const u8,
};

/// A data file being written.
const Open = struct {
    mem: *std.heap.ArenaAllocator,
    writer: *pqwrite.Writer,
    rel: []const u8,
    full: []const u8,
    values: []const ?[]const u8,
    used: u64,
};

pub const Sink = struct {
    gpa: std.mem.Allocator,
    mem: std.heap.ArenaAllocator,
    /// The plan files are written by: for an UPSERT, the table's columns in order.
    plan: *const Plan,
    file_schema: types.Schema,
    phys: []const pqwrite.Physical,
    files: std.StringArrayHashMapUnmanaged(*Open) = .empty,
    tick: u64 = 0,
    added: std.ArrayListUnmanaged(Added) = .empty,
    /// Every data file begun, local ones removed when the write fails.
    begun: std.ArrayListUnmanaged([]const u8) = .empty,
    /// An UPSERT's spooled rows, merged with the table at `close`.
    merging: ?*merge.Merge = null,
    /// Whether an UPSERT's joins went to disk.
    spilled: bool = false,
    /// The version the commit became, once it is made.
    committed: ?u64 = null,
    /// Set once `close` or `abort` has freed everything, so an `abort` after a
    /// failed `close` does nothing.
    done: bool = false,

    /// A sink on `plan_arena` (which outlives it), its buffers on `gpa`.
    pub fn open(gpa: std.mem.Allocator, plan_arena: std.mem.Allocator, p: *const Plan, opts: merge.Opts) !*Sink {
        const self = try plan_arena.create(Sink);
        self.* = .{ .gpa = gpa, .mem = std.heap.ArenaAllocator.init(gpa), .plan = p, .file_schema = undefined, .phys = &.{} };
        errdefer self.mem.deinit();
        const a = self.mem.allocator();
        if (p.mode == .upsert) {
            const out = try plan_arena.create(Plan);
            out.* = p.*;
            const cols = try plan_arena.dupe(Col, p.cols);
            for (cols, 0..) |*c, k| c.src = k;
            out.cols = cols;
            self.plan = out;
            self.merging = try merge.Merge.init(gpa, p, opts);
        }
        const fields = try a.alloc(types.Schema.Field, p.data.len);
        const phys = try a.alloc(pqwrite.Physical, p.data.len);
        for (p.data, fields, phys) |k, *f, *ph| {
            f.* = .{ .name = p.cols[k].name, .ty = p.cols[k].ty.asNullable() };
            ph.* = p.cols[k].phys;
        }
        self.file_schema = .{ .fields = fields };
        self.phys = phys;
        return self;
    }

    pub fn sink(self: *Sink) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sinkVTable(Sink);

    pub fn writeBatch(self: *Sink, arena: std.mem.Allocator, b: Batch) !void {
        if (self.merging) |m| return m.take(b);
        return self.route(arena, b);
    }

    /// Writes rows shaped as `plan` reads them, each to its partition's file.
    pub fn route(self: *Sink, arena: std.mem.Allocator, b: Batch) !void {
        if (b.len == 0) return;
        try self.check(b);
        const p = self.plan;
        if (p.parts.len == 0) return self.writeRows(arena, b, null, b.len, &.{}, "");
        const Group = struct { keep: []bool, n: usize, values: []const ?[]const u8 };
        var groups: std.StringArrayHashMapUnmanaged(Group) = .empty;
        var key = std.array_list.Managed(u8).init(arena);
        const vals = try arena.alloc(?[]const u8, p.parts.len);
        for (0..b.len) |r| {
            key.clearRetainingCapacity();
            for (p.parts, vals) |k, *v| {
                v.* = try partitionText(arena, p.cols[k], b.columns[p.cols[k].src].getValue(r));
                if (v.*) |t| {
                    try key.append(1);
                    try key.appendSlice(t);
                } else try key.append(0);
                try key.append(0xff);
            }
            const gop = try groups.getOrPut(arena, key.items);
            if (!gop.found_existing) {
                gop.key_ptr.* = try arena.dupe(u8, key.items);
                const keep = try arena.alloc(bool, b.len);
                @memset(keep, false);
                gop.value_ptr.* = .{ .keep = keep, .n = 0, .values = try arena.dupe(?[]const u8, vals) };
            }
            gop.value_ptr.keep[r] = true;
            gop.value_ptr.n += 1;
        }
        for (groups.keys(), groups.values()) |k, g| try self.writeRows(arena, b, g.keep, g.n, g.values, k);
    }

    /// Refuses a NULL in a column the table declares NOT NULL, and an integer
    /// outside a narrower column's range, naming the column.
    fn check(self: *Sink, b: Batch) !void {
        for (self.plan.cols) |c| {
            const col = b.columns[c.src];
            const narrow = c.phys.int_bits < 64 and col.data == .i64;
            if (c.nullable and !narrow) continue;
            for (0..b.len) |r| {
                if (!col.validity.get(r)) {
                    if (!c.nullable) return eval.explain(Error.DeltaNullInNotNull, try std.fmt.allocPrint(self.mem.allocator(), "column `{s}` is NOT NULL in {s}, but a row holds NULL in it", .{ c.name, self.plan.name }));
                    continue;
                }
                if (!narrow) continue;
                const x = col.data.i64[r];
                const lim = @as(i64, 1) << @intCast(c.phys.int_bits - 1);
                if (x < -lim or x >= lim)
                    return eval.explain(Error.DeltaValueOutOfRange, try std.fmt.allocPrint(self.mem.allocator(), "column `{s}` is `{s}` in {s}, which cannot hold {d}", .{ c.name, c.delta_type, self.plan.name, x }));
            }
        }
    }

    fn writeRows(self: *Sink, arena: std.mem.Allocator, b: Batch, keep: ?[]const bool, n: usize, values: []const ?[]const u8, key: []const u8) !void {
        const p = self.plan;
        const cols = try arena.alloc(column.Column, p.data.len);
        for (p.data, cols) |k, *c| {
            const src = b.columns[p.cols[k].src];
            c.* = if (keep) |m| try column.gather(arena, src, m, n) else src;
        }
        const o = try self.fileFor(key, values);
        o.writer.writeBatch(arena, .{ .schema = &self.file_schema, .columns = cols, .len = n }) catch |e| return self.explainWrite(e);
        self.tick += 1;
        o.used = self.tick;
        if (o.writer.offset >= roll_bytes) try self.finishFile(key);
    }

    fn explainWrite(self: *Sink, e: anyerror) anyerror {
        if (e == pqwrite.Error.UnsupportedParquetDecimal or e == pqwrite.Error.ParquetValueOutOfRange)
            return eval.explain(e, std.fmt.allocPrint(self.mem.allocator(), "a value does not fit its column's type in {s} (a decimal past its precision)", .{self.plan.name}) catch return e);
        return e;
    }

    fn fileFor(self: *Sink, key: []const u8, values: []const ?[]const u8) !*Open {
        if (self.files.get(key)) |o| return o;
        if (self.files.count() >= max_open) {
            var oldest: usize = 0;
            for (self.files.values(), 0..) |o, i| if (o.used < self.files.values()[oldest].used) {
                oldest = i;
            };
            try self.finishFile(self.files.keys()[oldest]);
        }
        const a = self.mem.allocator();
        var aw = std.Io.Writer.Allocating.init(a);
        const w = &aw.writer;
        for (self.plan.parts, values) |k, v| {
            try escapePath(w, self.plan.cols[k].name);
            try w.writeByte('=');
            if (v) |t| try escapePath(w, t) else try w.writeAll("__HIVE_DEFAULT_PARTITION__");
            try w.writeByte('/');
        }
        try w.print("part-{s}{s}", .{ &dlog.uuid(), switch (self.plan.codec) {
            .snappy => ".snappy.parquet",
            .gzip => ".gz.parquet",
            else => ".parquet",
        } });
        const rel = aw.written();
        const full = try std.mem.concat(a, u8, &.{ self.plan.root, rel });
        if (!objstore.isUrl(full)) if (std.fs.path.dirname(full)) |d| try std.fs.cwd().makePath(d);
        const mem = try self.gpa.create(std.heap.ArenaAllocator);
        mem.* = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        errdefer {
            mem.deinit();
            self.gpa.destroy(mem);
        }
        const writer = try pqwrite.Writer.openPhysical(mem.allocator(), full, self.file_schema, self.plan.codec, .truncate, self.phys);
        try self.begun.append(a, full);
        const o = try a.create(Open);
        const kept = try a.alloc(?[]const u8, values.len);
        for (kept, values) |*k, v| k.* = if (v) |t| try a.dupe(u8, t) else null;
        o.* = .{ .mem = mem, .writer = writer, .rel = rel, .full = full, .values = kept, .used = self.tick };
        try self.files.put(a, try a.dupe(u8, key), o);
        return o;
    }

    fn finishFile(self: *Sink, key: []const u8) !void {
        const o = self.files.get(key).?;
        _ = self.files.orderedRemove(key);
        defer {
            o.mem.deinit();
            self.gpa.destroy(o.mem);
        }
        const a = self.mem.allocator();
        const sum = o.writer.closeSummary(a) catch |e| {
            o.writer.abort();
            return self.explainWrite(e);
        };
        try self.added.append(a, .{ .rel = o.rel, .values = o.values, .size = sum.bytes, .stats = try statsJson(a, self.plan, sum) });
    }

    pub fn close(self: *Sink) !void {
        defer self.destroy();
        errdefer self.discard();
        const p = self.plan;
        const a = self.mem.allocator();
        var removes: []const delta.Add = &.{};
        var read_set: []const []const u8 = &.{};
        if (self.merging) |m| {
            if (m.rows == 0) return;
            const done = try m.finish(self);
            self.spilled = m.spilled;
            removes = done.removed;
            read_set = done.read_set;
        } else if (p.mode == .replace) if (p.snap) |st| {
            removes = st.files.values();
            const rs = try a.alloc([]const u8, removes.len);
            for (rs, removes) |*r, f| r.* = f.raw;
            read_set = rs;
        };
        while (self.files.count() > 0) try self.finishFile(self.files.keys()[0]);
        if (p.snap != null and self.added.items.len == 0 and (p.mode == .append or removes.len == 0)) return;
        const now = std.time.milliTimestamp();
        const txn = dlog.uuid();
        var aw = std.Io.Writer.Allocating.init(a);
        const w = &aw.writer;
        var pb = std.Io.Writer.Allocating.init(a);
        try pb.writer.writeByte('[');
        for (p.parts, 0..) |k, i| {
            if (i > 0) try pb.writer.writeByte(',');
            try jsonString(&pb.writer, p.cols[k].name);
        }
        try pb.writer.writeByte(']');
        switch (p.mode) {
            .upsert => |u| {
                try w.print("{{\"commitInfo\":{{\"timestamp\":{d},\"operation\":\"MERGE\",\"operationParameters\":{{\"predicate\":", .{now});
                var pred = std.Io.Writer.Allocating.init(a);
                for (u.keys, 0..) |k, i| try pred.writer.print("{s}target.`{s}` = source.`{s}`", .{ if (i > 0) " AND " else "", k, k });
                try jsonString(w, pred.written());
            },
            else => {
                try w.print("{{\"commitInfo\":{{\"timestamp\":{d},\"operation\":\"WRITE\",\"operationParameters\":{{\"mode\":\"{s}\",\"partitionBy\":", .{ now, if (p.mode == .append) "Append" else "Overwrite" });
                try jsonString(w, pb.written());
            },
        }
        try w.print("}},\"isBlindAppend\":{s},\"engineInfo\":\"basalt/{s}\",\"txnId\":\"{s}\"}}}}\n", .{ if (p.mode == .append and p.snap != null) "true" else "false", @import("build_options").version, &txn });
        if (p.snap == null) {
            try w.writeAll("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
            try w.print("{{\"metaData\":{{\"id\":\"{s}\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}}},\"schemaString\":", .{&dlog.uuid()});
            try jsonString(w, p.schema_string);
            try w.writeAll(",\"partitionColumns\":");
            try w.writeAll(pb.written());
            try w.print(",\"configuration\":{{}},\"createdTime\":{d}}}}}\n", .{now});
        }
        for (removes) |f| {
            try w.writeAll("{\"remove\":{\"path\":");
            try jsonString(w, f.raw);
            try w.print(",\"deletionTimestamp\":{d},\"dataChange\":true,\"extendedFileMetadata\":true,\"partitionValues\":", .{now});
            if (f.parts) |pv| try std.json.Stringify.value(pv, .{}, w) else try w.writeAll("{}");
            if (f.size) |s| try w.print(",\"size\":{d}", .{s});
            if (f.dv) |dv| {
                try w.writeAll(",\"deletionVector\":");
                try std.json.Stringify.value(dv, .{}, w);
            }
            try w.writeAll("}}\n");
        }
        const names = try a.alloc([]const u8, self.added.items.len);
        for (self.added.items, names) |f, *nm| {
            nm.* = try std.mem.concat(a, u8, &.{ p.root, f.rel });
            try w.writeAll("{\"add\":{\"path\":");
            try jsonString(w, try uriPath(a, f.rel));
            try w.writeAll(",\"partitionValues\":{");
            for (p.parts, f.values, 0..) |k, v, i| {
                if (i > 0) try w.writeByte(',');
                try jsonString(w, p.cols[k].name);
                try w.writeByte(':');
                if (v) |t| try jsonString(w, t) else try w.writeAll("null");
            }
            try w.print("}},\"size\":{d},\"modificationTime\":{d},\"dataChange\":true,\"stats\":", .{ f.size, now });
            try jsonString(w, f.stats);
            try w.writeAll("}}\n");
        }
        var ts = dlog.TableStore{ .root = p.root };
        self.begun.clearRetainingCapacity();
        self.committed = try dlog.commit(a, ts.store(), p.name, .{
            .kind = if (p.snap == null) .create else switch (p.mode) {
                .append => .append,
                .replace => .replace,
                .upsert => .upsert,
            },
            .read_version = if (p.snap) |st| st.version else null,
            .txn_id = &txn,
            .body = aw.written(),
            .added = names,
            .read_set = read_set,
            .key_root = if (p.snap) |st| st.key_root else "",
            .hold = if (self.merging) |m| m.hold() else null,
        });
    }

    pub fn abort(self: *Sink) void {
        if (self.done) return;
        self.discard();
        self.destroy();
    }

    /// Drops the open files and removes the local data files begun, unless a commit
    /// naming them was attempted; remote ones are left, referenced by no version.
    fn discard(self: *Sink) void {
        for (self.files.values()) |o| {
            o.writer.abort();
            o.mem.deinit();
            self.gpa.destroy(o.mem);
        }
        self.files.clearRetainingCapacity();
        for (self.begun.items) |f| if (!objstore.isUrl(f)) std.fs.cwd().deleteFile(f) catch {};
        self.begun.clearRetainingCapacity();
    }

    fn destroy(self: *Sink) void {
        if (self.merging) |m| m.deinit();
        self.merging = null;
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

test "partition values and statistics as the log writes them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ts = Col{ .name = "t", .delta_type = "timestamp", .ty = types.Type.init(.timestamp), .phys = .{ .utc = true }, .src = 0 };
    const ntz = Col{ .name = "t", .delta_type = "timestamp_ntz", .ty = types.Type.init(.timestamp), .src = 0 };
    const us: i64 = 20454 * 86_400_000_000 + 3_600_000_000 + 1_234_567;
    try testing.expectEqualStrings("2026-01-01T01:00:01.234567Z", (try partitionText(a, ts, .{ .timestamp = us })).?);
    try testing.expectEqualStrings("2026-01-01 01:00:01.234567", (try partitionText(a, ntz, .{ .timestamp = us })).?);
    try testing.expectEqualStrings("\"2026-01-01T01:00:01.234Z\"", (try statJson(a, ts, .{ .timestamp = us }, true)).?);
    try testing.expectEqualStrings("\"2026-01-01 01:00:01.234\"", (try statJson(a, ntz, .{ .timestamp = us }, false)).?);
    const dec = Col{ .name = "d", .delta_type = "decimal(10,2)", .ty = types.Type.decimal(10, 2), .src = 0 };
    try testing.expectEqualStrings("10.50", (try partitionText(a, dec, .{ .decimal = .{ .unscaled = 105, .scale = 1 } })).?);
    try testing.expectEqualStrings("-1.05", (try statJson(a, dec, .{ .decimal = .{ .unscaled = -105, .scale = 2 } }, false)).?);
    const s = Col{ .name = "s", .delta_type = "string", .ty = types.Type.init(.string), .src = 0 };
    try testing.expect(try partitionText(a, s, .{ .string = "" }) == null);
    const long = "é" ** 20;
    try testing.expect(try statJson(a, s, .{ .string = long }, true) == null);
    try testing.expectEqualStrings("\"" ++ "é" ** 16 ++ "\"", (try statJson(a, s, .{ .string = long }, false)).?);
    const f = Col{ .name = "f", .delta_type = "double", .ty = types.Type.init(.float), .src = 0 };
    try testing.expect(try statJson(a, f, .{ .float = std.math.inf(f64) }, true) == null);
    try testing.expectEqualStrings("2026-01-01", (try partitionText(a, .{ .name = "d", .delta_type = "date", .ty = types.Type.init(.date), .src = 0 }, .{ .date = 20454 })).?);
}

test "a table's column types: narrower integers and floats, decimals that hold every value" {
    const F = struct {
        fn of(t: []const u8) Field {
            return .{ .name = "c", .ty = .{ .string = t }, .nullable = true, .metadata = null };
        }
    };
    const i = try tableCol("T", F.of("integer"), types.Type.init(.int), 0);
    try testing.expectEqual(@as(u8, 32), i.phys.int_bits);
    try testing.expect((try tableCol("T", F.of("float"), types.Type.init(.float), 0)).phys.float32);
    try testing.expect((try tableCol("T", F.of("timestamp"), types.Type.init(.timestamp), 0)).phys.utc);
    try testing.expect(!(try tableCol("T", F.of("timestamp_ntz"), types.Type.init(.timestamp), 0)).phys.utc);
    const d = try tableCol("T", F.of("decimal(12,3)"), types.Type.decimal(10, 2), 0);
    try testing.expectEqual(@as(u8, 3), d.ty.scale);
    try testing.expectError(table.Error.LakeTableRefused, tableCol("T", F.of("decimal(12,3)"), types.Type.decimal(12, 2), 0));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "cannot hold every value") != null);
    try testing.expectError(table.Error.LakeTableRefused, tableCol("T", F.of("date"), types.Type.init(.string), 0));
    try testing.expectEqualStrings("column `c` is string in the rows but `date` in T; cast it", eval.takeFailure(table.Error.LakeTableRefused).?);
    try testing.expectError(table.Error.LakeTableRefused, tableCol("T", F.of("variant"), types.Type.init(.string), 0));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "`variant`, which basalt does not write") != null);
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
    for ([_][]const u8{ "rowTracking", "inCommitTimestamp", "clustering", "fancyNew" }) |f| {
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
