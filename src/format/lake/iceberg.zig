//! An Apache Iceberg table's state at one snapshot: its metadata file (named by
//! the user, or the highest-numbered one under `metadata/`, or over http(s) the one
//! `version-hint.text` names), the snapshot (the current one, or `snapshot_id`),
//! its manifest list and manifests in Avro, and the data files they keep live
//! (status ADDED or EXISTING). The table's schema is the metadata's, its data files
//! read by column name. A path the metadata holds under the table's `location` is
//! read under the root the user gave, so a copied or moved table reads its own files.
//!
//! An identity-partitioned column's value comes from the manifest, as the spec
//! asks of a file that lacks the column, and lets a filter skip the file unopened.
//! Refused, where reading on would return wrong rows: a snapshot with live delete
//! files (row-level deletes or deletion vectors), a column renamed or re-added
//! under its old name (files are read by name), a default value basalt does not
//! apply, encrypted files, data files not in Parquet, types basalt does not read,
//! and format versions past 3.

const std = @import("std");
const Layout = @import("../parquet/fit.zig").Layout;
const Value = @import("../../exec/value.zig").Value;
const avro = @import("../avro/read.zig");
const codecs = @import("../compress/codecs.zig");
const folder = @import("../../connect/folder.zig");
const table = @import("table.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const metadata_dir = "metadata/";
const max_metadata = 256 << 20;

const refuse = table.refuse;
const Obj = std.json.ObjectMap;

/// Whether `path` names a metadata file, which `format = 'iceberg'` reads directly.
pub fn isMetadataPath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".metadata.json") or std.mem.endsWith(u8, path, ".metadata.json.gz");
}

/// The version a metadata file's name carries (`v12.metadata.json`,
/// `00012-<uuid>.metadata.json`, and their gzip forms); null for another name.
fn metadataVersion(name: []const u8) ?u64 {
    const suf = for ([_][]const u8{ ".gz.metadata.json", ".metadata.json.gz", ".metadata.json" }) |x| {
        if (std.mem.endsWith(u8, name, x)) break x;
    } else return null;
    var stem = name[0 .. name.len - suf.len];
    if (stem.len > 0 and stem[0] == 'v') stem = stem[1..];
    var n: usize = 0;
    while (n < stem.len and std.ascii.isDigit(stem[n])) n += 1;
    if (n == 0 or (n < stem.len and stem[n] != '-')) return null;
    return std.fmt.parseInt(u64, stem[0..n], 10) catch null;
}

fn gzipped(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".gz.metadata.json") or std.mem.endsWith(u8, path, ".metadata.json.gz");
}

/// Whether `root` holds an Iceberg table's metadata, listing `metadata/` (or, over
/// http(s), which cannot list, fetching `version-hint.text`).
pub fn holds(arena: std.mem.Allocator, root: []const u8) !bool {
    const dir = try std.mem.concat(arena, u8, &.{ root, metadata_dir });
    if (table.isHttp(root)) {
        _ = table.readWhole(arena, try std.mem.concat(arena, u8, &.{ dir, "version-hint.text" })) catch |e| return if (table.absent(e)) false else e;
        return true;
    }
    if (std.mem.indexOf(u8, root, "://") == null) {
        var d = std.fs.cwd().openDir(dir, .{ .iterate = true }) catch |e| return if (table.absent(e)) false else e;
        defer d.close();
        var it = d.iterate();
        while (try it.next()) |en| if (en.kind == .file and metadataVersion(en.name) != null) return true;
        return false;
    }
    const got = folder.listRaw(arena, dir) catch |e| return if (table.absent(e)) false else e;
    for (got) |u| if (metadataVersion(folder.below(u, dir)) != null) return true;
    return false;
}

/// The metadata file a root's table reads: the highest version listed, or over
/// http(s) the one `version-hint.text` names.
fn locate(arena: std.mem.Allocator, name: []const u8, root: []const u8) ![]const u8 {
    const dir = try std.mem.concat(arena, u8, &.{ root, metadata_dir });
    if (table.isHttp(root)) {
        const hint = table.readWhole(arena, try std.mem.concat(arena, u8, &.{ dir, "version-hint.text" })) catch |e| {
            if (table.absent(e)) return refuse("{s} has no metadata/version-hint.text, which an http(s) URL needs, as it cannot be listed; read its metadata file itself: '{s}metadata/<file>.metadata.json' WITH (format = 'iceberg')", .{ name, root });
            return e;
        };
        const t = std.mem.trim(u8, hint, " \r\n\t");
        if (t.len > 0 and std.mem.indexOfNone(u8, t, "0123456789") == null) return std.mem.concat(arena, u8, &.{ dir, "v", t, ".metadata.json" });
        if (std.mem.indexOfAny(u8, t, "/\\") == null and isMetadataPath(t)) return std.mem.concat(arena, u8, &.{ dir, t });
        return refuse("{s} has a metadata/version-hint.text that names no metadata file (`{s}`)", .{ name, t });
    }
    const listed = folder.listRaw(arena, dir) catch |e| {
        if (table.absent(e)) return refuse("folder `{s}` has no metadata/*.metadata.json: it is not an Iceberg table", .{root});
        return e;
    };
    var best: ?[]const u8 = null;
    var best_v: u64 = 0;
    var tie: ?[]const u8 = null;
    for (listed) |u| {
        const rel = folder.below(u, dir);
        if (std.mem.indexOfAny(u8, rel, "/\\") != null) continue;
        const v = metadataVersion(rel) orelse continue;
        if (best == null or v > best_v) {
            best = u;
            best_v = v;
            tie = null;
        } else if (v == best_v) tie = u;
    }
    const b = best orelse return refuse("folder `{s}` has no metadata/*.metadata.json: it is not an Iceberg table", .{root});
    if (tie) |t| return refuse("{s} has two metadata files of version {d} (`{s}`, `{s}`); read the current one by name: '{s}' WITH (format = 'iceberg')", .{ name, best_v, folder.below(b, dir), folder.below(t, dir), b });
    return b;
}

/// Where the metadata's paths are read from: those under the table's `location`
/// are rebased onto `root`, the others read where they are.
const Places = struct {
    arena: std.mem.Allocator,
    root: []const u8,
    rebase: bool,
    location: ?[]const u8,

    fn of(self: Places, p: []const u8) ![]const u8 {
        const m = try table.mapPath(self.arena, self.root, p);
        if (!self.rebase) return m;
        const loc = self.location orelse return m;
        if (std.mem.startsWith(u8, m, loc) and m.len > loc.len and m[loc.len] == '/')
            return std.mem.concat(self.arena, u8, &.{ self.root, m[loc.len + 1 ..] });
        return m;
    }
};

fn get(o: Obj, key: []const u8) ?std.json.Value {
    const v = o.get(key) orelse return null;
    return if (v == .null) null else v;
}

fn int(o: Obj, key: []const u8) ?i64 {
    const v = get(o, key) orelse return null;
    return if (v == .integer) v.integer else null;
}

fn str(o: Obj, key: []const u8) ?[]const u8 {
    const v = get(o, key) orelse return null;
    return if (v == .string) v.string else null;
}

fn obj(v: std.json.Value) ?Obj {
    return if (v == .object) v.object else null;
}

/// A schema's columns as the reader names them, and each field id's full name.
const Columns = struct {
    fields: std.array_list.Managed(types.Schema.Field),
    ids: std.AutoArrayHashMapUnmanaged(i64, []const u8) = .empty,
    /// The top-level column each field id is, for identity partitions.
    top: std.AutoHashMapUnmanaged(i64, usize) = .empty,
};

/// `s`'s field ids and full names, struct fields within as `a.b`.
fn walkIds(arena: std.mem.Allocator, out: *std.AutoArrayHashMapUnmanaged(i64, []const u8), prefix: []const u8, s: Obj) !void {
    const fs = get(s, "fields") orelse return;
    if (fs != .array) return;
    for (fs.array.items) |f| {
        const o = obj(f) orelse continue;
        const id = int(o, "id") orelse continue;
        const full = try std.mem.concat(arena, u8, &.{ prefix, str(o, "name") orelse continue });
        try out.put(arena, id, full);
        if (get(o, "type")) |t| if (obj(t)) |to| if (std.mem.eql(u8, str(to, "type") orelse "", "struct"))
            try walkIds(arena, out, try std.mem.concat(arena, u8, &.{ full, "." }), to);
    }
}

fn addColumns(arena: std.mem.Allocator, name: []const u8, cols: *Columns, prefix: []const u8, s: Obj, top: ?usize) !void {
    const fs = get(s, "fields") orelse return refuse("{s} has a schema with no fields", .{name});
    if (fs != .array) return refuse("{s} has a schema with no fields", .{name});
    for (fs.array.items) |f| {
        const o = obj(f) orelse return refuse("{s} has a schema field that is not an object", .{name});
        const fname = str(o, "name") orelse return refuse("{s} has a schema field with no name", .{name});
        const full = try std.mem.concat(arena, u8, &.{ prefix, fname });
        if (get(o, "initial-default") != null)
            return refuse("{s} has a column `{s}` with an initial-default, which basalt does not apply", .{ name, full });
        const t = get(o, "type") orelse return refuse("{s} has a column `{s}` with no type", .{ name, full });
        const here = top orelse cols.fields.items.len;
        if (int(o, "id")) |id| try cols.top.put(arena, id, here);
        switch (t) {
            .string => |ts| try cols.fields.append(.{ .name = full, .ty = (try primitive(name, full, ts)).asNullable() }),
            .object => |to| {
                const kind = str(to, "type") orelse "";
                if (std.mem.eql(u8, kind, "struct")) {
                    try addColumns(arena, name, cols, try std.mem.concat(arena, u8, &.{ full, "." }), to, here);
                } else if (std.mem.eql(u8, kind, "list") or std.mem.eql(u8, kind, "map")) {
                    try cols.fields.append(.{ .name = full, .ty = types.Type.init(.string).asNullable() });
                } else return refuse("{s} has a column `{s}` of type `{s}`, which basalt does not read", .{ name, full, kind });
            },
            else => return refuse("{s} has a column `{s}` with no type", .{ name, full }),
        }
    }
}

fn primitive(name: []const u8, col: []const u8, t: []const u8) !types.Type {
    const T = types.Type;
    const eq = std.mem.eql;
    if (eq(u8, t, "boolean")) return T.init(.bool);
    if (eq(u8, t, "int") or eq(u8, t, "long")) return T.init(.int);
    if (eq(u8, t, "float") or eq(u8, t, "double")) return T.init(.float);
    if (eq(u8, t, "string") or eq(u8, t, "unknown")) return T.init(.string);
    if (eq(u8, t, "binary") or eq(u8, t, "uuid")) return T.init(.bytes);
    if (std.mem.startsWith(u8, t, "fixed[") and std.mem.endsWith(u8, t, "]")) return T.init(.bytes);
    if (eq(u8, t, "date")) return T.init(.date);
    if (eq(u8, t, "time")) return T.init(.time);
    if (eq(u8, t, "timestamp") or eq(u8, t, "timestamptz")) return T.init(.timestamp);
    if (std.mem.startsWith(u8, t, "decimal(") and std.mem.endsWith(u8, t, ")")) {
        var it = std.mem.splitScalar(u8, t["decimal(".len .. t.len - 1], ',');
        const p = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch 0;
        const s = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch 255;
        if (p >= 1 and p <= 38 and s <= p) return T.decimal(p, s);
    }
    return refuse("{s} has a column `{s}` of type `{s}`, which basalt does not read", .{ name, col, t });
}

/// Refuses a schema whose field ids and names disagree with any of the table's
/// schemas: files are read by column name, so a renamed column would read as null
/// in files written before the rename, and a re-added one as the old column.
fn checkNames(arena: std.mem.Allocator, name: []const u8, read: Obj, all: []const std.json.Value) !void {
    var want: std.AutoArrayHashMapUnmanaged(i64, []const u8) = .empty;
    try walkIds(arena, &want, "", read);
    for (all) |sv| {
        const s = obj(sv) orelse continue;
        var had: std.AutoArrayHashMapUnmanaged(i64, []const u8) = .empty;
        try walkIds(arena, &had, "", s);
        for (had.keys(), had.values()) |id, old| {
            if (want.get(id)) |now| if (!std.mem.eql(u8, now, old))
                return refuse("{s} has a column `{s}` renamed from `{s}`; basalt reads Iceberg data files by column name, so files written before the rename would read it as null", .{ name, now, old });
            for (want.keys(), want.values()) |wid, now| if (wid != id and std.mem.eql(u8, now, old))
                return refuse("{s} has a column `{s}` dropped and added again (field id {d}, before {d}); basalt reads Iceberg data files by column name, so files written before would read the old column", .{ name, now, wid, id });
        }
    }
}

/// One identity partition a manifest's spec holds: the top-level column it is
/// and the partition field id its value is stored under.
const Ident = struct { column: usize, field_id: i64, name: []const u8 };

fn identities(arena: std.mem.Allocator, cols: *const Columns, spec: ?std.json.Value) ![]const Ident {
    const s = spec orelse return &.{};
    const fs = if (s == .array) s.array.items else if (obj(s)) |o| (if (get(o, "fields")) |f| (if (f == .array) f.array.items else return &.{}) else return &.{}) else return &.{};
    var out = std.array_list.Managed(Ident).init(arena);
    for (fs) |f| {
        const o = obj(f) orelse continue;
        if (!std.mem.eql(u8, str(o, "transform") orelse "", "identity")) continue;
        const src = int(o, "source-id") orelse continue;
        const col = cols.top.get(src) orelse continue;
        if (!std.mem.eql(u8, cols.ids.get(src) orelse "", cols.fields.items[col].name)) continue;
        try out.append(.{ .column = col, .field_id = int(o, "field-id") orelse -1, .name = str(o, "name") orelse "" });
    }
    return out.items;
}

/// A partition tuple's value as the column's, or null when its kind is not one
/// a constant is taken for (the file's own column is read then).
fn partitionValue(v: avro.Value, ty: types.Type) ?Value {
    if (v == .null) return .null;
    return switch (ty.kind) {
        .int => if (v == .long) .{ .int = v.long } else null,
        .date => if (v == .long) .{ .date = std.math.cast(i32, v.long) orelse return null } else null,
        .timestamp => if (v == .long) .{ .timestamp = v.long } else null,
        .string => if (v == .bytes and std.unicode.utf8ValidateSlice(v.bytes)) .{ .string = v.bytes } else null,
        .bool => if (v == .bool) .{ .bool = v.bool } else null,
        else => null,
    };
}

const Live = struct {
    path: []const u8,
    size: ?u64,
    parts: std.AutoArrayHashMapUnmanaged(usize, Value) = .empty,
};

const Reading = struct {
    arena: std.mem.Allocator,
    name: []const u8,
    root: []const u8,
    places: Places,
    cols: *const Columns,
    specs: []const std.json.Value,
    files: std.StringArrayHashMapUnmanaged(Live) = .empty,

    fn rel(self: *const Reading, p: []const u8) []const u8 {
        return folder.below(p, self.root);
    }

    fn avroFile(self: *Reading, path: []const u8) !struct { c: avro.Container, recs: []const avro.Value } {
        const bytes = try table.readWhole(self.arena, path);
        const c = avro.Container.open(self.arena, bytes) catch |e| return self.corrupt(path, e);
        if (!avro.codecSupported(c.codec))
            return refuse("{s} has manifest `{s}` written with the Avro codec `{s}`; basalt reads null, deflate, snappy and zstandard", .{ self.name, self.rel(path), c.codec });
        const recs = c.records(self.arena) catch |e| return self.corrupt(path, e);
        return .{ .c = c, .recs = recs };
    }

    fn corrupt(self: *const Reading, path: []const u8, e: anyerror) anyerror {
        if (e == error.OutOfMemory) return e;
        return refuse("{s} has corrupt Avro in `{s}` ({s})", .{ self.name, self.rel(path), @errorName(e) });
    }

    fn manifestList(self: *Reading, path: []const u8) !void {
        const f = try self.avroFile(path);
        for (f.recs) |r| {
            const mp = (r.field("manifest_path") orelse return self.corrupt(path, error.NoManifestPath)).text() orelse return self.corrupt(path, error.NoManifestPath);
            if (r.field("key_metadata")) |k| if (k != .null)
                return refuse("{s} is encrypted (key_metadata on manifest `{s}`), which basalt does not read", .{ self.name, mp });
            const content = if (r.field("content")) |c| c.int() orelse 0 else 0;
            const spec_id = if (r.field("partition_spec_id")) |s| s.int() else null;
            try self.manifest(try self.places.of(mp), content, spec_id);
        }
    }

    fn specById(self: *const Reading, id: ?i64) ?std.json.Value {
        const want = id orelse return null;
        for (self.specs) |s| if (obj(s)) |o| if (int(o, "spec-id") == want) return s;
        return null;
    }

    /// A manifest's live entries: data files kept, delete files refused.
    fn manifest(self: *Reading, path: []const u8, list_content: i64, spec_id: ?i64) !void {
        const f = try self.avroFile(path);
        const deletes = list_content == 1 or std.mem.eql(u8, f.c.metaValue("content") orelse "", "deletes");
        var spec: ?std.json.Value = self.specById(spec_id);
        if (f.c.metaValue("partition-spec")) |text| spec = std.json.parseFromSliceLeaky(std.json.Value, self.arena, text, .{}) catch spec;
        const idents = try identities(self.arena, self.cols, spec);
        for (f.recs) |e| {
            const status = (e.field("status") orelse return self.corrupt(path, error.NoStatus)).int() orelse return self.corrupt(path, error.NoStatus);
            if (status == 2) continue;
            const df = e.field("data_file") orelse return self.corrupt(path, error.NoDataFile);
            const fp = (df.field("file_path") orelse return self.corrupt(path, error.NoFilePath)).text() orelse return self.corrupt(path, error.NoFilePath);
            const content = if (df.field("content")) |c| c.int() orelse 0 else 0;
            if (deletes or content != 0) {
                const kind = if (std.mem.eql(u8, (if (df.field("file_format")) |x| x.text() orelse "" else ""), "PUFFIN")) "deletion vectors" else if (content == 2) "equality deletes" else "position deletes";
                return refuse("{s} has delete files in its snapshot (`{s}`, {s}); basalt does not apply row-level deletes, and reading the data files whole would return deleted rows", .{ self.name, self.rel(try self.places.of(fp)), kind });
            }
            if (df.field("key_metadata")) |k| if (k != .null)
                return refuse("{s} is encrypted (key_metadata on `{s}`), which basalt does not read", .{ self.name, self.rel(try self.places.of(fp)) });
            const fmt = if (df.field("file_format")) |x| x.text() orelse "" else "";
            if (!std.ascii.eqlIgnoreCase(fmt, "parquet"))
                return refuse("{s} has data file `{s}` in {s}; basalt reads Iceberg data files in Parquet", .{ self.name, self.rel(try self.places.of(fp)), fmt });
            const where = try self.places.of(fp);
            const size = if (df.field("file_size_in_bytes")) |s| s.int() else null;
            var live = Live{ .path = where, .size = if (size) |n| (if (n >= 0) @intCast(n) else null) else null };
            const tuple = df.field("partition");
            for (idents) |id| {
                const pv = partitionTuple(tuple, id) orelse continue;
                const v = partitionValue(pv, self.cols.fields.items[id.column].ty) orelse continue;
                try live.parts.put(self.arena, id.column, v);
            }
            try self.files.put(self.arena, where, live);
        }
    }
};

/// A partition tuple's field for `id`, matched by field id, else by name.
fn partitionTuple(tuple: ?avro.Value, id: Ident) ?avro.Value {
    const t = tuple orelse return null;
    if (t != .record) return null;
    for (t.record.schema.fields, t.record.values) |f, v| if (f.id != null and f.id.? == id.field_id) return v;
    return t.field(id.name);
}

fn readMetadata(arena: std.mem.Allocator, name: []const u8, path: []const u8) !Obj {
    var text = try table.readWhole(arena, path);
    if (gzipped(path)) text = codecs.inflateAlloc(arena, text, .gzip, max_metadata) catch |e| {
        if (e == error.OutOfMemory) return e;
        return refuse("{s} has metadata `{s}` that is not gzip", .{ name, path });
    };
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch |e| {
        if (e == error.OutOfMemory) return e;
        return refuse("{s} has metadata `{s}` that is not JSON", .{ name, path });
    };
    return obj(v) orelse refuse("{s} has metadata `{s}` that is not a JSON object", .{ name, path });
}

/// `path` a table's root folder, or with `direct` its metadata file.
pub fn resolve(arena: std.mem.Allocator, path: []const u8, direct: bool, at: table.At) !*const table.Resolved {
    const name = try std.fmt.allocPrint(arena, "Iceberg table `{s}`", .{path});
    var root = path;
    var rebase = true;
    if (direct) {
        const slash = std.mem.lastIndexOfScalar(u8, path, '/') orelse 0;
        const dir = path[0..slash];
        rebase = std.mem.endsWith(u8, dir, "/metadata");
        root = if (rebase) path[0 .. dir.len - "metadata".len] else path[0 .. slash + 1];
    }
    const meta_path = if (direct) path else try locate(arena, name, root);
    const md = try readMetadata(arena, name, meta_path);

    const fv = int(md, "format-version") orelse 1;
    if (fv < 1 or fv > 3) return refuse("{s} is format version {d}; basalt reads Iceberg format versions 1 to 3", .{ name, fv });
    var location = str(md, "location");
    if (location) |l| location = std.mem.trimRight(u8, try table.mapPath(arena, root, l), "/");
    const places = Places{ .arena = arena, .root = root, .rebase = rebase, .location = location };

    const snaps: []const std.json.Value = if (get(md, "snapshots")) |s| (if (s == .array) s.array.items else &.{}) else &.{};
    var snap: ?Obj = null;
    if (at.snapshot_id) |want| {
        for (snaps) |s| if (obj(s)) |o| if (int(o, "snapshot-id") == want) {
            snap = o;
        };
        if (snap == null) {
            var ids = std.array_list.Managed(u8).init(arena);
            for (snaps, 0..) |s, i| if (obj(s)) |o| {
                if (i == 8) {
                    try ids.appendSlice(", …");
                    break;
                }
                try ids.appendSlice(try std.fmt.allocPrint(arena, "{s}{d}", .{ if (i == 0) "" else ", ", int(o, "snapshot-id") orelse 0 }));
            };
            return refuse("{s} has no snapshot {d} (snapshots: {s})", .{ name, want, if (ids.items.len == 0) "none" else ids.items });
        }
    } else if (int(md, "current-snapshot-id")) |cur| if (cur != -1) {
        for (snaps) |s| if (obj(s)) |o| if (int(o, "snapshot-id") == cur) {
            snap = o;
        };
        if (snap == null) return refuse("{s} names current snapshot {d}, which its metadata does not hold", .{ name, cur });
    };

    const all_schemas: []const std.json.Value = if (get(md, "schemas")) |s| (if (s == .array) s.array.items else &.{}) else &.{};
    var schema_id = int(md, "current-schema-id");
    if (at.snapshot_id != null) if (snap) |s| if (int(s, "schema-id")) |id| {
        schema_id = id;
    };
    var schema: ?Obj = null;
    for (all_schemas) |s| if (obj(s)) |o| if (schema_id != null and int(o, "schema-id") == schema_id) {
        schema = o;
    };
    if (schema == null) if (get(md, "schema")) |s| {
        schema = obj(s);
    };
    const sch = schema orelse return refuse("{s} has no schema {d} in its metadata", .{ name, schema_id orelse 0 });
    try checkNames(arena, name, sch, all_schemas);
    const cols = try arena.create(Columns);
    cols.* = .{ .fields = std.array_list.Managed(types.Schema.Field).init(arena) };
    try addColumns(arena, name, cols, "", sch, null);
    try walkIds(arena, &cols.ids, "", sch);
    if (cols.fields.items.len == 0) return refuse("{s} has a schema with no columns", .{name});

    var specs: []const std.json.Value = if (get(md, "partition-specs")) |s| (if (s == .array) s.array.items else &.{}) else &.{};
    if (specs.len == 0) if (get(md, "partition-spec")) |s| {
        const one = try arena.alloc(std.json.Value, 1);
        var o = Obj.init(arena);
        try o.put("spec-id", .{ .integer = 0 });
        try o.put("fields", s);
        one[0] = .{ .object = o };
        specs = one;
    };
    var rd = Reading{ .arena = arena, .name = name, .root = root, .places = places, .cols = cols, .specs = specs };
    if (snap) |s| {
        if (str(s, "manifest-list")) |ml| {
            try rd.manifestList(try places.of(ml));
        } else if (get(s, "manifests")) |ms| {
            if (ms == .array) for (ms.array.items) |m| if (m == .string) try rd.manifest(try places.of(m.string), 0, int(md, "default-spec-id"));
        } else return refuse("{s} has snapshot {d} with neither a manifest list nor manifests", .{ name, int(s, "snapshot-id") orelse 0 });
    }

    var part_cols = std.array_list.Managed(usize).init(arena);
    for (0..cols.fields.items.len) |c| {
        const every = for (rd.files.values()) |f| {
            if (!f.parts.contains(c)) break false;
        } else rd.files.count() > 0;
        if (every) try part_cols.append(c);
    }
    const files = try arena.alloc(Layout.File, rd.files.count());
    for (files, rd.files.values()) |*f, l| {
        const parts = try arena.alloc(Value, part_cols.items.len);
        for (parts, part_cols.items) |*p, c| p.* = l.parts.get(c).?;
        f.* = .{ .path = l.path, .size = l.size, .parts = parts };
    }
    std.mem.sort(Layout.File, files, {}, struct {
        fn lt(_: void, a: Layout.File, b: Layout.File) bool {
            return std.mem.lessThan(u8, a.path, b.path);
        }
    }.lt);

    const layout = try arena.create(Layout);
    layout.* = .{ .name = name, .root = root, .schema = .{ .fields = cols.fields.items }, .part_fields = part_cols.items, .files = files };
    const r = try arena.create(table.Resolved);
    const plural = if (files.len == 1) "" else "s";
    r.* = .{
        .kind = .iceberg,
        .root = path,
        .label = if (snap) |s|
            try std.fmt.allocPrint(arena, "snapshot {d}, {d} file{s}", .{ int(s, "snapshot-id") orelse 0, files.len, plural })
        else
            "no snapshot, 0 files",
        .layout = layout,
    };
    return r;
}

test "metadata file names: versions from either naming, gzip forms, other files skipped" {
    try testing.expectEqual(@as(?u64, 12), metadataVersion("v12.metadata.json"));
    try testing.expectEqual(@as(?u64, 3), metadataVersion("00003-91ece667-5252-4853-b887-31591380dc23.metadata.json"));
    try testing.expectEqual(@as(?u64, 4), metadataVersion("v4.gz.metadata.json"));
    try testing.expectEqual(@as(?u64, 5), metadataVersion("00005-a.metadata.json.gz"));
    for ([_][]const u8{ "version-hint.text", "snap-1-0-a.avro", "a-m0.avro", "vx.metadata.json", "12x.metadata.json", ".metadata.json" }) |n|
        try testing.expect(metadataVersion(n) == null);
    try testing.expect(isMetadataPath("s3://b/t/metadata/v1.metadata.json") and isMetadataPath("/t/metadata/00001-a.metadata.json.gz"));
    try testing.expect(!isMetadataPath("/t/metadata/v1.json"));
}

test "a schema as columns: structs dotted, lists and maps as text, types refused by name" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const v = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"type":"struct","schema-id":0,"fields":[
        \\{"id":1,"name":"id","required":true,"type":"long"},
        \\{"id":2,"name":"amt","required":false,"type":"decimal(10, 2)"},
        \\{"id":3,"name":"geo","required":false,"type":{"type":"struct","fields":[{"id":5,"name":"lat","required":false,"type":"double"}]}},
        \\{"id":4,"name":"tags","required":false,"type":{"type":"list","element-id":6,"element":"string","element-required":false}},
        \\{"id":7,"name":"u","required":false,"type":"uuid"},{"id":8,"name":"f","required":false,"type":"fixed[3]"},
        \\{"id":9,"name":"ts","required":false,"type":"timestamptz"},{"id":10,"name":"n","required":false,"type":"unknown"}]}
    , .{});
    var cols = Columns{ .fields = std.array_list.Managed(types.Schema.Field).init(a) };
    try addColumns(a, "T", &cols, "", v.object, null);
    const want = [_]struct { []const u8, types.TypeKind }{
        .{ "id", .int }, .{ "amt", .decimal }, .{ "geo.lat", .float }, .{ "tags", .string }, .{ "u", .bytes }, .{ "f", .bytes }, .{ "ts", .timestamp }, .{ "n", .string },
    };
    try testing.expectEqual(want.len, cols.fields.items.len);
    for (want, cols.fields.items) |w, f| {
        try testing.expectEqualStrings(w[0], f.name);
        try testing.expectEqual(w[1], f.ty.kind);
        try testing.expect(f.ty.nullable);
    }
    try testing.expectEqual(@as(usize, 2), cols.top.get(5).?);
    const eval = @import("../../exec/eval.zig");
    for ([_][2][]const u8{
        .{ "{\"fields\":[{\"id\":1,\"name\":\"v\",\"type\":\"variant\"}]}", "column `v` of type `variant`, which basalt does not read" },
        .{ "{\"fields\":[{\"id\":1,\"name\":\"t\",\"type\":\"timestamp_ns\"}]}", "column `t` of type `timestamp_ns`" },
        .{ "{\"fields\":[{\"id\":1,\"name\":\"d\",\"type\":\"int\",\"initial-default\":3}]}", "column `d` with an initial-default, which basalt does not apply" },
    }) |c| {
        const bad = try std.json.parseFromSliceLeaky(std.json.Value, a, c[0], .{});
        var bc = Columns{ .fields = std.array_list.Managed(types.Schema.Field).init(a) };
        try testing.expectError(table.Error.LakeTableRefused, addColumns(a, "T", &bc, "", bad.object, null));
        try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, c[1]) != null);
    }
}

test "the name guard: a rename or a name re-added under a new id is refused, an added column is not" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const eval = @import("../../exec/eval.zig");
    const parse = struct {
        fn f(al: std.mem.Allocator, t: []const u8) !std.json.Value {
            return std.json.parseFromSliceLeaky(std.json.Value, al, t, .{});
        }
    }.f;
    const s0 = try parse(a, "{\"fields\":[{\"id\":1,\"name\":\"a\",\"type\":\"int\"},{\"id\":2,\"name\":\"g\",\"type\":{\"type\":\"struct\",\"fields\":[{\"id\":3,\"name\":\"x\",\"type\":\"int\"}]}}]}");
    const added = try parse(a, "{\"fields\":[{\"id\":1,\"name\":\"a\",\"type\":\"int\"},{\"id\":2,\"name\":\"g\",\"type\":{\"type\":\"struct\",\"fields\":[{\"id\":3,\"name\":\"x\",\"type\":\"int\"}]}},{\"id\":4,\"name\":\"b\",\"type\":\"int\"}]}");
    try checkNames(a, "T", added.object, &.{ s0, added });
    const renamed = try parse(a, "{\"fields\":[{\"id\":1,\"name\":\"a\",\"type\":\"int\"},{\"id\":2,\"name\":\"g\",\"type\":{\"type\":\"struct\",\"fields\":[{\"id\":3,\"name\":\"y\",\"type\":\"int\"}]}}]}");
    try testing.expectError(table.Error.LakeTableRefused, checkNames(a, "T", renamed.object, &.{ s0, renamed }));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "column `g.y` renamed from `g.x`") != null);
    const readded = try parse(a, "{\"fields\":[{\"id\":5,\"name\":\"a\",\"type\":\"int\"}]}");
    try testing.expectError(table.Error.LakeTableRefused, checkNames(a, "T", readded.object, &.{ s0, readded }));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "column `a` dropped and added again (field id 5, before 1)") != null);
}

test "paths under the table's location read under the root given" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const p = Places{ .arena = a, .root = "/copy/t/", .rebase = true, .location = "s3://b/wh/t" };
    try testing.expectEqualStrings("/copy/t/data/a.parquet", try p.of("s3a://b/wh/t/data/a.parquet"));
    try testing.expectEqualStrings("s3://b/wh/t2/a.parquet", try p.of("s3://b/wh/t2/a.parquet"));
    try testing.expectEqualStrings("s3://other/a.parquet", try p.of("s3://other/a.parquet"));
    const local = Places{ .arena = a, .root = "http://h/t/", .rebase = true, .location = "/tmp/wh/t" };
    try testing.expectEqualStrings("http://h/t/metadata/snap-1.avro", try local.of("file:///tmp/wh/t/metadata/snap-1.avro"));
    try testing.expectEqualStrings("http://h/t/metadata/snap-1.avro", try local.of("file:/tmp/wh/t/metadata/snap-1.avro"));
}
