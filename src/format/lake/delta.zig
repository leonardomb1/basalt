//! A Delta Lake table's state at one version, replayed from `_delta_log/`: the
//! newest complete checkpoint at or below that version (one Parquet file, or every
//! part of a multi-part one), then each JSON commit after it in order. An `add`
//! puts a file in the table and a `remove` takes it out, keyed as the protocol
//! keys a file: its path resolved against the table's root, with its deletion
//! vector's id, so a commit replacing a file's deletion vector is read whatever
//! order its lines are in. The last `protocol` and `metaData` win. The log's
//! listing is what is read, so `_last_checkpoint`, a hint for readers that do not
//! list, is not needed.
//!
//! The table's schema is `metaData.schemaString`, its partition columns' values
//! each file's `partitionValues`, which win over any copy of the column inside the
//! file. A table using a reader feature basalt does not honour is refused naming
//! it, where reading on would return wrong rows: a deletion vector on a live file,
//! column mapping by name or id, V2 checkpoints, catalog-managed commits, type
//! widening, variants, and any feature basalt does not know.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Layout = @import("../parquet/fit.zig").Layout;
const Reader = @import("../parquet/read.zig").Reader;
const Value = @import("../../exec/value.zig").Value;
const eval = @import("../../exec/eval.zig");
const folder = @import("../../connect/folder.zig");
const table = @import("table.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const log_dir = "_delta_log/";

const refuse = table.refuse;

/// One name in `_delta_log/`: a commit, a checkpoint (or one part of one), a V2
/// checkpoint (named by a UUID), or a file a reader skips (`_last_checkpoint`,
/// `.crc`, compacted commits, sidecars and staged files).
const Name = union(enum) {
    commit: u64,
    checkpoint: struct { version: u64, part: u32, parts: u32 },
    v2: u64,
    other,
};

fn isUuid(s: []const u8) bool {
    if (s.len != 36) return false;
    for (s, 0..) |c, i| {
        const dash = i == 8 or i == 13 or i == 18 or i == 23;
        if (if (dash) c != '-' else !std.ascii.isHex(c)) return false;
    }
    return true;
}

fn parseName(name: []const u8) Name {
    var n: usize = 0;
    while (n < name.len and std.ascii.isDigit(name[n])) n += 1;
    if (n == 0) return .other;
    const v = std.fmt.parseInt(u64, name[0..n], 10) catch return .other;
    const rest = name[n..];
    if (std.mem.eql(u8, rest, ".json")) return .{ .commit = v };
    if (std.mem.eql(u8, rest, ".checkpoint.parquet")) return .{ .checkpoint = .{ .version = v, .part = 1, .parts = 1 } };
    const pre = ".checkpoint.";
    if (!std.mem.startsWith(u8, rest, pre)) return .other;
    if (std.mem.endsWith(u8, rest, ".json") and isUuid(rest[pre.len .. rest.len - ".json".len])) return .{ .v2 = v };
    if (!std.mem.endsWith(u8, rest, ".parquet")) return .other;
    const mid = rest[pre.len .. rest.len - ".parquet".len];
    if (isUuid(mid)) return .{ .v2 = v };
    const dot = std.mem.indexOfScalar(u8, mid, '.') orelse return .other;
    if (dot != 10 or mid.len != 21) return .other;
    const part = std.fmt.parseInt(u32, mid[0..dot], 10) catch return .other;
    const parts = std.fmt.parseInt(u32, mid[dot + 1 ..], 10) catch return .other;
    if (part == 0 or part > parts) return .other;
    return .{ .checkpoint = .{ .version = v, .part = part, .parts = parts } };
}

/// Whether a listing of `_delta_log/` (at `log_path`) holds a commit or a
/// checkpoint, which a table has from its first version on.
pub fn hasVersions(listed: []const []const u8, log_path: []const u8) bool {
    for (listed) |u| {
        const rel = folder.below(u, log_path);
        if (std.mem.indexOfAny(u8, rel, "/\\") != null) continue;
        switch (parseName(rel)) {
            .commit, .checkpoint, .v2 => return true,
            .other => {},
        }
    }
    return false;
}

/// One checkpoint at a version: a classic one has one part. Two of a version (a
/// classic one and a multi-part one, or two part counts) are each kept, and any
/// complete one serves.
const Checkpoint = struct { version: u64, parts: u32 };
const Parts = std.AutoArrayHashMapUnmanaged(u32, []const u8);

/// What `_delta_log/` lists, and which of it a version is read from.
const Log = struct {
    name: []const u8,
    commits: std.AutoHashMapUnmanaged(u64, []const u8) = .empty,
    checkpoints: std.AutoArrayHashMapUnmanaged(Checkpoint, Parts) = .empty,
    /// The newest V2 checkpoint's version, which explains a log a reader of
    /// classic checkpoints cannot reach a version through.
    v2: ?u64 = null,

    fn add(self: *Log, arena: std.mem.Allocator, url: []const u8, rel: []const u8) !void {
        switch (parseName(rel)) {
            .commit => |v| try self.commits.put(arena, v, url),
            .checkpoint => |c| {
                const gop = try self.checkpoints.getOrPut(arena, .{ .version = c.version, .parts = c.parts });
                if (!gop.found_existing) gop.value_ptr.* = .empty;
                try gop.value_ptr.put(arena, c.part, url);
            },
            .v2 => |v| self.v2 = @max(self.v2 orelse v, v),
            .other => {},
        }
    }

    fn latest(self: *const Log) ?u64 {
        var best: ?u64 = null;
        var it = self.commits.keyIterator();
        while (it.next()) |v| best = @max(best orelse v.*, v.*);
        for (self.checkpoints.keys(), self.checkpoints.values()) |c, p| {
            if (p.count() == c.parts) best = @max(best orelse c.version, c.version);
        }
        return best;
    }

    /// The oldest version every file needed to read it is still there for.
    fn oldest(self: *const Log, newest: u64) u64 {
        var lo: u64 = if (self.commits.contains(0)) 0 else newest;
        for (self.checkpoints.keys(), self.checkpoints.values()) |c, p| {
            if (p.count() == c.parts and c.version < lo) lo = c.version;
        }
        return lo;
    }

    /// The checkpoint `target` is replayed from, null to replay every commit from
    /// 0; refused when a commit it needs is missing.
    fn base(self: *const Log, target: u64) !?Checkpoint {
        var best: ?Checkpoint = null;
        var partial: ?Checkpoint = null;
        for (self.checkpoints.keys(), self.checkpoints.values()) |c, p| {
            if (c.version > target) continue;
            if (p.count() == c.parts) {
                if (best == null or c.version > best.?.version) best = c;
            } else if (partial == null or c.version > partial.?.version) partial = c;
        }
        const from: u64 = if (best) |b| b.version + 1 else 0;
        var v = from;
        while (v <= target) : (v += 1) {
            if (self.commits.contains(v)) continue;
            if (self.v2) |x| if (x <= target and (best == null or x > best.?.version))
                return refuse("{s} uses V2 checkpoints (reader feature `v2Checkpoint`, checkpoint {d}), which basalt does not read", .{ self.name, x });
            if (partial) |p| if (best == null or p.version > best.?.version) {
                return refuse("{s}'s checkpoint {d} lists {d} parts but only {d} are in _delta_log/", .{ self.name, p.version, p.parts, self.checkpoints.get(p).?.count() });
            };
            if (best == null and v == 0) {
                const newest = self.latest().?;
                return refuse("{s} has no version {d} (versions {d} to {d} can be read)", .{ self.name, target, self.oldest(newest), newest });
            }
            return refuse("{s} is missing commit {d} from _delta_log/ (commits {d} to {d} are needed to reach version {d})", .{ self.name, v, from, target, target });
        }
        return best;
    }
};

/// A live file: `path` decoded from the log's URI form, `raw` as the log wrote it
/// (what a `remove` must name), `dv` its deletion vector's descriptor, if any.
pub const Add = struct {
    path: []const u8,
    raw: []const u8 = "",
    size: ?u64 = null,
    parts: ?std.json.Value = null,
    dv: ?std.json.Value = null,
};

/// A deletion vector's `uniqueId`: its storage type and path or inline bytes, with
/// `@offset` when it has one.
fn dvId(arena: std.mem.Allocator, storage: []const u8, where: []const u8, offset: ?i64) ![]const u8 {
    if (offset) |o| return std.fmt.allocPrint(arena, "{s}{s}@{d}", .{ storage, where, o });
    return std.mem.concat(arena, u8, &.{ storage, where });
}

fn jsonDv(arena: std.mem.Allocator, v: ?std.json.Value) !?[]const u8 {
    const d = v orelse return null;
    if (d != .object) return null;
    const off = d.object.get("offset");
    return try dvId(arena, str(d.object.get("storageType")) orelse "", str(d.object.get("pathOrInlineDv")) orelse "", if (off) |o| (if (o == .integer) o.integer else null) else null);
}

/// The table as the actions read so far leave it: at `version` once replayed.
pub const State = struct {
    arena: std.mem.Allocator,
    name: []const u8,
    /// The root a log's paths are resolved against to key them, absolute for a
    /// local table, so a file added by a relative path and removed by an absolute
    /// one is one file.
    key_root: []const u8,
    version: u64 = 0,
    reader_version: i64 = 1,
    writer_version: i64 = 1,
    features: []const []const u8 = &.{},
    writer_features: []const []const u8 = &.{},
    table_id: ?[]const u8 = null,
    schema_string: ?[]const u8 = null,
    part_cols: []const []const u8 = &.{},
    config: ?std.json.Value = null,
    mapping: ?[]const u8 = null,
    v2: ?[]const u8 = null,
    files: std.StringArrayHashMapUnmanaged(Add) = .empty,

    /// A file's identity in the log: its path resolved against the root, and its
    /// deletion vector's id. A path whose scheme basalt does not read keys as written.
    fn key(self: *State, decoded: []const u8, dv: ?[]const u8) ![]const u8 {
        const at = table.mapPath(self.arena, self.key_root, decoded) catch |e| blk: {
            if (e != table.Error.LakeTableRefused) return e;
            _ = eval.takeFailure(e);
            break :blk decoded;
        };
        const id = dv orelse return at;
        return std.mem.concat(self.arena, u8, &.{ at, "\x00", id });
    }

    fn put(self: *State, a: Add) !void {
        var x = a;
        x.raw = a.path;
        x.path = try table.percentDecode(self.arena, a.path);
        try self.files.put(self.arena, try self.key(x.path, try jsonDv(self.arena, a.dv)), x);
    }

    fn drop(self: *State, path: []const u8, dv: ?[]const u8) !void {
        _ = self.files.swapRemove(try self.key(try table.percentDecode(self.arena, path), dv));
    }

    fn protocol(self: *State, version: i64, writer: ?i64, features: ?std.json.Value, writer_features: ?std.json.Value) !void {
        self.reader_version = version;
        self.writer_version = writer orelse 1;
        self.features = try strings(self.arena, features);
        self.writer_features = try strings(self.arena, writer_features);
    }

    fn meta(self: *State, id: ?[]const u8, schema: []const u8, parts: ?std.json.Value, config: ?std.json.Value) !void {
        self.table_id = id;
        self.schema_string = schema;
        self.part_cols = try strings(self.arena, parts);
        self.config = if (config) |c| (if (c == .object) c else null) else null;
        self.mapping = null;
        if (self.config) |c| if (c.object.get("delta.columnMapping.mode")) |m| if (m == .string) {
            self.mapping = m.string;
        };
    }

    /// A table property's text, null when unset.
    pub fn property(self: *const State, prop: []const u8) ?[]const u8 {
        const c = self.config orelse return null;
        return str(c.object.get(prop));
    }

    /// One commit line's action; unknown actions and fields are ignored, as the
    /// protocol asks.
    fn action(self: *State, line: []const u8) !void {
        const v = std.json.parseFromSliceLeaky(std.json.Value, self.arena, line, .{}) catch
            return refuse("{s} has a commit in _delta_log/ holding a line that is not JSON", .{self.name});
        if (v != .object) return;
        var it = v.object.iterator();
        while (it.next()) |kv| {
            const body = kv.value_ptr.*;
            if (body != .object) continue;
            const o = body.object;
            const k = kv.key_ptr.*;
            if (std.mem.eql(u8, k, "add")) {
                const p = str(o.get("path")) orelse continue;
                try self.put(.{
                    .path = p,
                    .size = if (o.get("size")) |s| (if (s == .integer and s.integer >= 0) @as(u64, @intCast(s.integer)) else null) else null,
                    .parts = o.get("partitionValues"),
                    .dv = if (o.get("deletionVector")) |d| (if (d == .object) d else null) else null,
                });
            } else if (std.mem.eql(u8, k, "remove")) {
                try self.drop(str(o.get("path")) orelse continue, try jsonDv(self.arena, o.get("deletionVector")));
            } else if (std.mem.eql(u8, k, "metaData")) {
                try self.meta(str(o.get("id")), str(o.get("schemaString")) orelse continue, o.get("partitionColumns"), o.get("configuration"));
            } else if (std.mem.eql(u8, k, "protocol")) {
                const rv = o.get("minReaderVersion") orelse continue;
                if (rv != .integer) continue;
                const wv: ?i64 = if (o.get("minWriterVersion")) |w| (if (w == .integer) w.integer else null) else null;
                try self.protocol(rv.integer, wv, o.get("readerFeatures"), o.get("writerFeatures"));
            }
        }
    }

    /// One checkpoint file's rows, read through the Parquet reader: its `add`s are
    /// the files live at its version, its `remove`s tombstones a reader skips.
    fn checkpoint(self: *State, url: []const u8) !void {
        const r = try Reader.openProjected(self.arena, url, &checkpoint_columns);
        defer r.close();
        while (try r.next(self.arena)) |b| {
            const c_path = b.schema.indexOf("add.path");
            const c_size = b.schema.indexOf("add.size");
            const c_parts = b.schema.indexOf("add.partitionValues");
            const c_dv = b.schema.indexOf("add.deletionVector.storageType");
            const c_dv_at = b.schema.indexOf("add.deletionVector.pathOrInlineDv");
            const c_dv_off = b.schema.indexOf("add.deletionVector.offset");
            const c_dv_size = b.schema.indexOf("add.deletionVector.sizeInBytes");
            const c_dv_card = b.schema.indexOf("add.deletionVector.cardinality");
            const c_id = b.schema.indexOf("metaData.id");
            const c_schema = b.schema.indexOf("metaData.schemaString");
            const c_pcols = b.schema.indexOf("metaData.partitionColumns");
            const c_config = b.schema.indexOf("metaData.configuration");
            const c_rv = b.schema.indexOf("protocol.minReaderVersion");
            const c_wv = b.schema.indexOf("protocol.minWriterVersion");
            const c_feat = b.schema.indexOf("protocol.readerFeatures");
            const c_wfeat = b.schema.indexOf("protocol.writerFeatures");
            const c_v2 = [_]?usize{ b.schema.indexOf("checkpointMetadata.version"), b.schema.indexOf("sidecar.path") };
            for (0..b.len) |i| {
                for (c_v2) |c| if (cell(b, c, i) != .null) {
                    self.v2 = url;
                };
                if (cell(b, c_path, i) == .string) {
                    const size = cell(b, c_size, i);
                    const storage = cell(b, c_dv, i);
                    var dv: ?std.json.Value = null;
                    if (storage == .string) {
                        var o = std.json.ObjectMap.init(self.arena);
                        try o.put("storageType", .{ .string = try self.arena.dupe(u8, storage.string) });
                        const where = cell(b, c_dv_at, i);
                        if (where == .string) try o.put("pathOrInlineDv", .{ .string = try self.arena.dupe(u8, where.string) });
                        const ints = [_]struct { ?usize, []const u8 }{ .{ c_dv_off, "offset" }, .{ c_dv_size, "sizeInBytes" }, .{ c_dv_card, "cardinality" } };
                        for (ints) |f| {
                            const v = cell(b, f[0], i);
                            if (v == .int) try o.put(f[1], .{ .integer = v.int });
                        }
                        dv = .{ .object = o };
                    }
                    try self.put(.{
                        .path = try self.arena.dupe(u8, cell(b, c_path, i).string),
                        .size = if (size == .int and size.int >= 0) @intCast(size.int) else null,
                        .parts = try self.jsonOf(cell(b, c_parts, i)),
                        .dv = dv,
                    });
                }
                if (cell(b, c_schema, i) == .string) {
                    const id = cell(b, c_id, i);
                    try self.meta(if (id == .string) try self.arena.dupe(u8, id.string) else null, try self.arena.dupe(u8, cell(b, c_schema, i).string), try self.jsonOf(cell(b, c_pcols, i)), try self.jsonOf(cell(b, c_config, i)));
                }
                if (cell(b, c_rv, i) == .int) {
                    const wv = cell(b, c_wv, i);
                    try self.protocol(cell(b, c_rv, i).int, if (wv == .int) wv.int else null, try self.jsonOf(cell(b, c_feat, i)), try self.jsonOf(cell(b, c_wfeat, i)));
                }
            }
        }
    }

    fn jsonOf(self: *State, v: Value) !?std.json.Value {
        if (v != .string) return null;
        return std.json.parseFromSliceLeaky(std.json.Value, self.arena, v.string, .{}) catch null;
    }

    /// Refuses a table whose reader features basalt does not honour, each only where
    /// the table uses it: column mapping by name or id, not merely enabled.
    pub fn checkProtocol(self: *const State) !void {
        if (self.reader_version < 1 or self.reader_version > 3)
            return refuse("{s} needs Delta reader version {d}; basalt reads reader versions 1 to 3", .{ self.name, self.reader_version });
        if (self.mapping) |m| if (!std.mem.eql(u8, m, "none"))
            return refuse("{s} uses column mapping (mode `{s}`): its data files name columns by physical name, which basalt does not read", .{ self.name, m });
        if (self.reader_version < 3) return;
        for (self.features) |f| {
            if (oneOf(f, &.{ "columnMapping", "deletionVectors", "timestampNtz", "vacuumProtocolCheck" })) continue;
            if (std.mem.eql(u8, f, "v2Checkpoint"))
                return refuse("{s} uses V2 checkpoints (reader feature `v2Checkpoint`), which basalt does not read", .{self.name});
            if (oneOf(f, &.{ "catalogManaged", "catalogOwned-preview" }))
                return refuse("{s} is catalog-managed (reader feature `{s}`): its newest commits are held by a catalog, not in _delta_log/", .{ self.name, f });
            if (oneOf(f, &.{ "typeWidening", "typeWidening-preview", "variantType", "variantType-preview", "variantShredding", "variantShredding-preview" }))
                return refuse("{s} uses reader feature `{s}`, which basalt does not read", .{ self.name, f });
            return refuse("{s} needs reader feature `{s}`, which basalt does not know; reading without it could return wrong rows", .{ self.name, f });
        }
    }
};

const checkpoint_columns = [_][]const u8{
    "add.path",                          "add.size",                  "add.partitionValues",            "add.deletionVector.storageType",
    "add.deletionVector.pathOrInlineDv", "add.deletionVector.offset", "add.deletionVector.sizeInBytes", "add.deletionVector.cardinality",
    "metaData.id",                       "metaData.schemaString",     "metaData.partitionColumns",      "metaData.configuration",
    "protocol.minReaderVersion",         "protocol.minWriterVersion", "protocol.readerFeatures",        "protocol.writerFeatures",
    "checkpointMetadata.version",        "sidecar.path",
};

fn cell(b: Batch, c: ?usize, row: usize) Value {
    return if (c) |k| b.columns[k].getValue(row) else .null;
}

fn oneOf(s: []const u8, set: []const []const u8) bool {
    for (set) |x| if (std.mem.eql(u8, s, x)) return true;
    return false;
}

fn str(v: ?std.json.Value) ?[]const u8 {
    const x = v orelse return null;
    return if (x == .string) x.string else null;
}

fn strings(arena: std.mem.Allocator, v: ?std.json.Value) ![]const []const u8 {
    const x = v orelse return &.{};
    if (x != .array) return &.{};
    var out = std.array_list.Managed([]const u8).init(arena);
    for (x.array.items) |e| if (e == .string) try out.append(e.string);
    return out.items;
}

/// `schemaString` as the reader's columns: a struct's fields as dotted columns,
/// an array or map as one JSON text column, every column nullable.
fn schemaOf(arena: std.mem.Allocator, name: []const u8, text: []const u8) !types.Schema {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch
        return refuse("{s} has a schemaString that is not JSON", .{name});
    var out = std.array_list.Managed(types.Schema.Field).init(arena);
    try addFields(arena, name, &out, "", v, 0);
    return .{ .fields = out.items };
}

fn addFields(arena: std.mem.Allocator, name: []const u8, out: *std.array_list.Managed(types.Schema.Field), prefix: []const u8, v: std.json.Value, depth: usize) !void {
    if (depth >= table.max_depth) return refuse("{s} has a schemaString nested deeper than {d} levels", .{ name, table.max_depth });
    const fields = if (v == .object) v.object.get("fields") else null;
    if (fields == null or fields.? != .array) return refuse("{s} has a schemaString that is not a struct of fields", .{name});
    for (fields.?.array.items) |f| {
        if (f != .object) continue;
        const fname = str(f.object.get("name")) orelse return refuse("{s} has a schemaString field with no name", .{name});
        const full = try std.mem.concat(arena, u8, &.{ prefix, fname });
        const t = f.object.get("type") orelse return refuse("{s} has a column `{s}` with no type", .{ name, full });
        switch (t) {
            .string => |s| try out.append(.{ .name = full, .ty = (try primitive(name, full, s)).asNullable() }),
            .object => |o| {
                const kind = str(o.get("type")) orelse "";
                if (std.mem.eql(u8, kind, "struct")) {
                    try addFields(arena, name, out, try std.mem.concat(arena, u8, &.{ full, "." }), t, depth + 1);
                } else if (std.mem.eql(u8, kind, "array") or std.mem.eql(u8, kind, "map")) {
                    try out.append(.{ .name = full, .ty = types.Type.init(.string).asNullable() });
                } else return refuse("{s} has a column `{s}` of type `{s}`, which basalt does not read", .{ name, full, kind });
            },
            else => return refuse("{s} has a column `{s}` with no type", .{ name, full }),
        }
    }
}

fn primitive(name: []const u8, col: []const u8, t: []const u8) !types.Type {
    const T = types.Type;
    if (oneOf(t, &.{ "string", "void" })) return T.init(.string);
    if (oneOf(t, &.{ "long", "integer", "short", "byte" })) return T.init(.int);
    if (oneOf(t, &.{ "float", "double" })) return T.init(.float);
    if (std.mem.eql(u8, t, "boolean")) return T.init(.bool);
    if (std.mem.eql(u8, t, "binary")) return T.init(.bytes);
    if (std.mem.eql(u8, t, "date")) return T.init(.date);
    if (oneOf(t, &.{ "timestamp", "timestamp_ntz" })) return T.init(.timestamp);
    if (std.mem.eql(u8, t, "decimal")) return T.decimal(10, 0);
    if (std.mem.startsWith(u8, t, "decimal(") and std.mem.endsWith(u8, t, ")")) {
        const inner = t["decimal(".len .. t.len - 1];
        var it = std.mem.splitScalar(u8, inner, ',');
        const p = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch 0;
        const s = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch 255;
        if (p >= 1 and p <= 38 and s <= p) return T.decimal(p, s);
    }
    return refuse("{s} has a column `{s}` of type `{s}`, which basalt does not read", .{ name, col, t });
}

/// `root`'s table at `at`; `listed` is `_delta_log/`'s listing when detection
/// already made it.
pub fn resolve(arena: std.mem.Allocator, root: []const u8, at: table.At, listed_in: ?[]const []const u8) !*const table.Resolved {
    const st = try snapshot(arena, root, at, listed_in);
    const name = st.name;
    const target = st.version;
    try st.checkProtocol();
    const schema = try schemaOf(arena, name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{name}));
    if (schema.fields.len == 0) return refuse("{s} has a schema with no columns", .{name});

    const part_fields = try arena.alloc(usize, st.part_cols.len);
    for (part_fields, st.part_cols) |*pf, c| pf.* = schema.indexOf(c) orelse
        return refuse("{s} has a partition column `{s}` its schema lacks", .{ name, c });

    const files = try arena.alloc(Layout.File, st.files.count());
    for (files, st.files.values()) |*f, a| {
        const decoded = a.path;
        if (a.dv != null) return refuse("{s} has file `{s}` with a deletion vector (reader feature `deletionVectors`); basalt does not apply deletion vectors, and reading the file whole would return deleted rows", .{ name, decoded });
        const parts = try arena.alloc(Value, part_fields.len);
        for (parts, part_fields, st.part_cols) |*p, fi, c| {
            const text: ?[]const u8 = if (a.parts) |pv| (if (pv == .object) str(pv.object.get(c)) else null) else null;
            p.* = (try table.partitionValue(arena, text, schema.fields[fi].ty)) orelse
                return refuse("{s} has file `{s}` with partition value `{s}` for column `{s}`, which is not a {s}", .{ name, decoded, text.?, c, try schema.fields[fi].ty.name(arena) });
        }
        f.* = .{ .path = try table.mapPath(arena, root, decoded), .size = a.size, .parts = parts };
    }
    std.mem.sort(Layout.File, files, {}, struct {
        fn lt(_: void, a: Layout.File, b: Layout.File) bool {
            return std.mem.lessThan(u8, a.path, b.path);
        }
    }.lt);

    const layout = try arena.create(Layout);
    layout.* = .{ .name = name, .root = root, .schema = schema, .part_fields = part_fields, .files = files };
    const r = try arena.create(table.Resolved);
    r.* = .{
        .kind = .delta,
        .root = root,
        .label = try std.fmt.allocPrint(arena, "version {d}, {d} file{s}", .{ target, files.len, if (files.len == 1) "" else "s" }),
        .layout = layout,
    };
    return r;
}

/// `root`'s log replayed to `at` (the newest version when unpinned), refusing
/// only what makes the replay itself unreliable; what a reader or a writer
/// cannot honour each judges from the state.
pub fn snapshot(arena: std.mem.Allocator, root: []const u8, at: table.At, listed_in: ?[]const []const u8) !*State {
    const name = try std.fmt.allocPrint(arena, "Delta table `{s}`", .{root});
    if (table.isHttp(root))
        return refuse("{s} cannot be read over http(s): a Delta table is found by listing _delta_log/, which a URL cannot do; read it from a local path, s3://, az://, sftp:// or smb://", .{name});
    const log_path = try std.mem.concat(arena, u8, &.{ root, log_dir });
    const listed = listed_in orelse folder.listRaw(arena, log_path) catch |e| {
        if (table.absent(e)) return refuse("folder `{s}` has no _delta_log/: it is not a Delta table", .{root});
        return e;
    };
    var log = Log{ .name = name };
    for (listed) |u| {
        const rel = folder.below(u, log_path);
        if (std.mem.indexOfAny(u8, rel, "/\\") != null) continue;
        try log.add(arena, u, rel);
    }
    const newest = log.latest() orelse return refuse("{s} has no commit in _delta_log/", .{name});
    const target = at.version orelse newest;
    if (target > newest)
        return refuse("{s} has no version {d} (versions {d} to {d} can be read)", .{ name, target, log.oldest(newest), newest });
    const from = try log.base(target);

    var st = State{ .arena = arena, .name = name, .key_root = try table.absoluteRoot(arena, root) };
    if (from) |c| {
        const files = log.checkpoints.get(c).?;
        var k: u32 = 1;
        while (k <= c.parts) : (k += 1) try st.checkpoint(files.get(k).?);
        if (st.v2) |f| return refuse("{s} uses V2 checkpoints (reader feature `v2Checkpoint`, checkpoint `{s}`), which basalt does not read", .{ name, folder.below(f, log_path) });
    }
    var v: u64 = if (from) |f| f.version + 1 else 0;
    while (v <= target) : (v += 1) {
        const text = try table.readWhole(arena, log.commits.get(v).?);
        var lines = std.mem.splitScalar(u8, text, '\n');
        while (lines.next()) |line| {
            const t = std.mem.trim(u8, line, " \r\t");
            if (t.len > 0) try st.action(t);
        }
    }
    st.version = target;
    const out = try arena.create(State);
    out.* = st;
    return out;
}

test "log names: commits, single and multi-part checkpoints, V2 checkpoints, and what a reader skips" {
    try testing.expectEqual(@as(u64, 7), parseName("00000000000000000007.json").commit);
    const cp = parseName("00000000000000000010.checkpoint.parquet").checkpoint;
    try testing.expectEqual(@as(u64, 10), cp.version);
    try testing.expectEqual(@as(u32, 1), cp.parts);
    const mp = parseName("00000000000000000010.checkpoint.0000000002.0000000003.parquet").checkpoint;
    try testing.expectEqual(@as(u32, 2), mp.part);
    try testing.expectEqual(@as(u32, 3), mp.parts);
    try testing.expectEqual(@as(u64, 10), parseName("00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json").v2);
    try testing.expectEqual(@as(u64, 10), parseName("00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.parquet").v2);
    for ([_][]const u8{
        "_last_checkpoint",
        "00000000000000000007.crc",
        "00000000000000000001.00000000000000000004.compacted.json",
        "00000000000000000010.checkpoint.0000000004.0000000003.parquet",
        "00000000000000000010.checkpoint.notauuid.json",
    }) |n| try testing.expect(parseName(n) == .other);
}

test "the base checkpoint: newest complete one at or below the version, commits after it all present" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var log = Log{ .name = "T" };
    for ([_][]const u8{
        "00000000000000000000.json",                                     "00000000000000000001.json",
        "00000000000000000002.json",                                     "00000000000000000002.checkpoint.parquet",
        "00000000000000000003.json",                                     "00000000000000000004.json",
        "00000000000000000004.checkpoint.0000000001.0000000002.parquet", "00000000000000000004.checkpoint.0000000002.0000000002.parquet",
        "00000000000000000005.json",                                     "00000000000000000005.checkpoint.0000000001.0000000002.parquet",
    }) |n| try log.add(a, n, n);
    try testing.expectEqual(@as(?u64, 5), log.latest());
    try testing.expectEqual(@as(u64, 4), (try log.base(5)).?.version);
    try testing.expectEqual(@as(u64, 4), (try log.base(4)).?.version);
    try testing.expectEqual(@as(u64, 2), (try log.base(3)).?.version);
    try testing.expect(try log.base(1) == null);

    var gap = Log{ .name = "T" };
    for ([_][]const u8{ "00000000000000000002.checkpoint.parquet", "00000000000000000003.json", "00000000000000000005.json" }) |n| try gap.add(a, n, n);
    try testing.expectError(table.Error.LakeTableRefused, gap.base(5));
    try testing.expectEqualStrings("T is missing commit 4 from _delta_log/ (commits 3 to 5 are needed to reach version 5)", eval.takeFailure(table.Error.LakeTableRefused).?);
    try testing.expectError(table.Error.LakeTableRefused, gap.base(1));
    try testing.expectEqualStrings("T has no version 1 (versions 2 to 5 can be read)", eval.takeFailure(table.Error.LakeTableRefused).?);

    var part = Log{ .name = "T" };
    for ([_][]const u8{ "00000000000000000003.checkpoint.0000000001.0000000002.parquet", "00000000000000000004.json" }) |n| try part.add(a, n, n);
    try testing.expectError(table.Error.LakeTableRefused, part.base(4));
    try testing.expectEqualStrings("T's checkpoint 3 lists 2 parts but only 1 are in _delta_log/", eval.takeFailure(table.Error.LakeTableRefused).?);

    var both = Log{ .name = "T" };
    for ([_][]const u8{
        "00000000000000000010.checkpoint.0000000001.0000000003.parquet", "00000000000000000010.checkpoint.0000000002.0000000003.parquet",
        "00000000000000000010.checkpoint.parquet",                       "00000000000000000011.json",
        "00000000000000000012.json",
    }) |n| try both.add(a, n, n);
    const c = (try both.base(12)).?;
    try testing.expectEqual(@as(u64, 10), c.version);
    try testing.expectEqual(@as(u32, 1), c.parts);
    try testing.expectEqual(@as(u64, 10), both.oldest(12));

    var v2 = Log{ .name = "T" };
    for ([_][]const u8{ "00000000000000000010.checkpoint.80a083e8-7026-4e79-81be-64bd76c43a11.json", "00000000000000000011.json" }) |n| try v2.add(a, n, n);
    try testing.expectError(table.Error.LakeTableRefused, v2.base(11));
    try testing.expectEqualStrings("T uses V2 checkpoints (reader feature `v2Checkpoint`, checkpoint 10), which basalt does not read", eval.takeFailure(table.Error.LakeTableRefused).?);
}

test "a file is keyed by its resolved path and deletion vector, so line order in a commit does not matter" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const dv = "{\"storageType\":\"u\",\"pathOrInlineDv\":\"ab\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":1}";
    const add_dv = "{\"add\":{\"path\":\"a.parquet\",\"partitionValues\":{},\"deletionVector\":" ++ dv ++ "}}";
    const rm_old = "{\"remove\":{\"path\":\"a.parquet\"}}";
    for ([_][2][]const u8{ .{ add_dv, rm_old }, .{ rm_old, add_dv } }) |order| {
        var st = State{ .arena = a, .name = "T", .key_root = "/t/" };
        try st.action("{\"add\":{\"path\":\"a.parquet\",\"partitionValues\":{}}}");
        for (order) |line| try st.action(line);
        try testing.expectEqual(@as(usize, 1), st.files.count());
        try testing.expect(st.files.values()[0].dv != null);
        try st.action("{\"remove\":{\"path\":\"a.parquet\",\"deletionVector\":" ++ dv ++ "}}");
        try testing.expectEqual(@as(usize, 0), st.files.count());
    }
    var st = State{ .arena = a, .name = "T", .key_root = "/t/" };
    try st.action("{\"add\":{\"path\":\"x%3D1/b.parquet\",\"partitionValues\":{}}}");
    try st.action("{\"remove\":{\"path\":\"file:///t/x%3D1/b.parquet\"}}");
    try testing.expectEqual(@as(usize, 0), st.files.count());
    try st.action("{\"add\":{\"path\":\"hdfs://nn/t/c.parquet\",\"partitionValues\":{}}}");
    try st.action("{\"remove\":{\"path\":\"hdfs://nn/t/c.parquet\"}}");
    try testing.expectEqual(@as(usize, 0), st.files.count());
}

test "protocol: reader versions and features, refused only where the table uses them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var st = State{ .arena = a, .name = "T", .key_root = "/t/" };
    try st.checkProtocol();
    st.reader_version = 2;
    st.mapping = "none";
    try st.checkProtocol();
    st.mapping = "name";
    try testing.expectError(table.Error.LakeTableRefused, st.checkProtocol());
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "column mapping (mode `name`)") != null);
    st.mapping = null;
    st.reader_version = 3;
    st.features = &.{ "deletionVectors", "timestampNtz", "vacuumProtocolCheck", "columnMapping" };
    try st.checkProtocol();
    for ([_][2][]const u8{
        .{ "v2Checkpoint", "V2 checkpoints" },
        .{ "catalogManaged", "catalog-managed" },
        .{ "typeWidening", "reader feature `typeWidening`, which basalt does not read" },
        .{ "variantType", "reader feature `variantType`, which basalt does not read" },
        .{ "fancyNew", "reader feature `fancyNew`, which basalt does not know" },
    }) |c| {
        st.features = &.{c[0]};
        try testing.expectError(table.Error.LakeTableRefused, st.checkProtocol());
        try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, c[1]) != null);
    }
    st.features = &.{};
    st.reader_version = 4;
    try testing.expectError(table.Error.LakeTableRefused, st.checkProtocol());
}

test "schemaString: primitives, a struct as dotted columns, array and map as JSON text" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const s = try schemaOf(a, "T",
        \\{"type":"struct","fields":[
        \\{"name":"id","type":"long","nullable":false,"metadata":{}},
        \\{"name":"amt","type":"decimal(12,3)","nullable":true,"metadata":{}},
        \\{"name":"ts","type":"timestamp_ntz","nullable":true,"metadata":{}},
        \\{"name":"addr","type":{"type":"struct","fields":[{"name":"city","type":"string","nullable":true,"metadata":{}},
        \\  {"name":"geo","type":{"type":"struct","fields":[{"name":"lat","type":"double","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},
        \\{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}},
        \\{"name":"m","type":{"type":"map","keyType":"string","valueType":"long","valueContainsNull":true},"nullable":true,"metadata":{}},
        \\{"name":"nothing","type":"void","nullable":true,"metadata":{}}]}
    );
    const want = [_]struct { []const u8, types.TypeKind }{
        .{ "id", .int },      .{ "amt", .decimal }, .{ "ts", .timestamp },   .{ "addr.city", .string }, .{ "addr.geo.lat", .float },
        .{ "tags", .string }, .{ "m", .string },    .{ "nothing", .string },
    };
    try testing.expectEqual(want.len, s.fields.len);
    for (want, s.fields) |w, f| {
        try testing.expectEqualStrings(w[0], f.name);
        try testing.expectEqual(w[1], f.ty.kind);
        try testing.expect(f.ty.nullable);
    }
    try testing.expectEqual(@as(u8, 12), s.fields[1].ty.precision);
    try testing.expectEqual(@as(u8, 3), s.fields[1].ty.scale);
    try testing.expectError(table.Error.LakeTableRefused, schemaOf(a, "T",
        \\{"type":"struct","fields":[{"name":"v","type":"variant","nullable":true,"metadata":{}}]}
    ));
    try testing.expectEqualStrings("T has a column `v` of type `variant`, which basalt does not read", eval.takeFailure(table.Error.LakeTableRefused).?);

    var deep = std.array_list.Managed(u8).init(a);
    for (0..100_000) |_| try deep.appendSlice("{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":");
    try deep.appendSlice("\"long\"");
    for (0..100_000) |_| try deep.appendSlice("}]}");
    try testing.expectError(table.Error.LakeTableRefused, schemaOf(a, "T", deep.items));
    try testing.expectEqualStrings("T has a schemaString nested deeper than 64 levels", eval.takeFailure(table.Error.LakeTableRefused).?);
}
