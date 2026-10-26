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
//! file. A file's deletion vector (`delta_dv.zig`) is applied as the file is read,
//! so its deleted rows are never returned. Under column mapping (mode `name` or
//! `id`, honoured where the protocol enables the feature) each column is found in
//! the files by its physical name or field id (`fit.Mapping`), and partition values
//! are keyed by physical name. A table using a reader feature basalt does not
//! honour is refused naming it, where reading on would return wrong rows: V2
//! checkpoints, catalog-managed commits, type widening, variants, and any feature
//! basalt does not know.

const std = @import("std");
const msgfmt = @import("../msgfmt.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const Layout = @import("../parquet/fit.zig").Layout;
const Mapping = @import("../parquet/fit.zig").Mapping;
const Reader = @import("../parquet/read.zig").Reader;
const Value = @import("../../exec/value.zig").Value;
const dtypes = @import("delta_types.zig");
const dvs = @import("delta_dv.zig");
const eval = @import("../../exec/eval.zig");
const folder = @import("../../connect/folder.zig");
const table = @import("table.zig");
const types = @import("../../lang/types.zig");
const uuid = @import("../uuid.zig");
const testing = std.testing;

pub const log_dir = "_delta_log/";

/// How many logs `snapshot` has replayed in this process, so a test can count a
/// statement's replays.
pub var replays = std.atomic.Value(u64).init(0);

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
    if (std.mem.endsWith(u8, rest, ".json") and uuid.isText(rest[pre.len .. rest.len - ".json".len])) return .{ .v2 = v };
    if (!std.mem.endsWith(u8, rest, ".parquet")) return .other;
    const mid = rest[pre.len .. rest.len - ".parquet".len];
    if (uuid.isText(mid)) return .{ .v2 = v };
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
/// (what a `remove` must name), `dv` its deletion vector's descriptor, if any, and
/// `stats` its statistics' JSON text, if the log has them.
pub const Add = struct {
    path: []const u8,
    raw: []const u8 = "",
    size: ?u64 = null,
    parts: ?std.json.Value = null,
    dv: ?std.json.Value = null,
    stats: ?[]const u8 = null,
};

/// A file's path, decoded from the log, as the protocol identifies it: resolved
/// against the table's root (`key_root`, absolute for a local table), or as
/// written when its scheme is one basalt does not read.
pub fn fileKey(arena: std.mem.Allocator, key_root: []const u8, decoded: []const u8) ![]const u8 {
    return table.mapPath(arena, key_root, decoded) catch |e| {
        if (e != table.Error.LakeTableRefused) return e;
        _ = eval.takeFailure(e);
        return decoded;
    };
}

/// A deletion vector's `uniqueId`: its storage type and path or inline bytes, with
/// `@offset` when it has one.
fn dvId(arena: std.mem.Allocator, storage: []const u8, where: []const u8, offset: ?i64) ![]const u8 {
    if (offset) |o| return msgfmt.allocPrint(arena, "{s}{s}@{d}", .{ storage, where, o });
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
    fn key(self: *State, a: std.mem.Allocator, decoded: []const u8, dv: ?[]const u8) ![]const u8 {
        const at = try fileKey(a, self.key_root, decoded);
        const id = dv orelse return at;
        return std.mem.concat(a, u8, &.{ at, "\x00", id });
    }

    /// Keeps `a`, copied: what it borrows (a commit line's parse, a checkpoint
    /// batch) is freed once its line or batch is replayed.
    fn put(self: *State, a: Add) !void {
        const raw = try self.arena.dupe(u8, a.path);
        const x = Add{
            .path = try table.percentDecode(self.arena, raw),
            .raw = raw,
            .size = a.size,
            .parts = if (a.parts) |v| try ownJson(self.arena, v, 2) else null,
            .dv = if (a.dv) |v| try ownJson(self.arena, v, 2) else null,
            .stats = if (a.stats) |t| try self.arena.dupe(u8, t) else null,
        };
        try self.files.put(self.arena, try self.key(self.arena, x.path, try jsonDv(self.arena, x.dv)), x);
    }

    fn drop(self: *State, scratch: std.mem.Allocator, path: []const u8, dv: ?[]const u8) !void {
        _ = self.files.swapRemove(try self.key(scratch, try table.percentDecode(scratch, path), dv));
    }

    fn protocol(self: *State, version: i64, writer: ?i64, features: ?std.json.Value, writer_features: ?std.json.Value) !void {
        self.reader_version = version;
        self.writer_version = writer orelse 1;
        self.features = try strings(self.arena, features);
        self.writer_features = try strings(self.arena, writer_features);
    }

    fn meta(self: *State, id: ?[]const u8, schema: []const u8, parts: ?std.json.Value, config: ?std.json.Value) !void {
        self.table_id = if (id) |x| try self.arena.dupe(u8, x) else null;
        self.schema_string = try self.arena.dupe(u8, schema);
        self.part_cols = try strings(self.arena, parts);
        self.config = if (config) |c| (if (c == .object) try ownJson(self.arena, c, 2) else null) else null;
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

    /// The column mapping mode the table's files are read by, null for `none`: the
    /// property counts only where the protocol enables the feature (reader version
    /// 2, or 3 with `columnMapping` among its reader features), as the protocol
    /// says a reader ignores it otherwise.
    pub fn mappingMode(self: *const State) ?[]const u8 {
        const m = self.mapping orelse return null;
        if (std.mem.eql(u8, m, "none")) return null;
        const on = self.reader_version == 2 or (self.reader_version == 3 and oneOf("columnMapping", self.features));
        return if (on) m else null;
    }

    /// One commit line's action, parsed into `scratch`, which the caller frees
    /// after it; unknown actions and fields are ignored, as the protocol asks.
    fn action(self: *State, scratch: std.mem.Allocator, line: []const u8) !void {
        const v = std.json.parseFromSliceLeaky(std.json.Value, scratch, line, .{}) catch
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
                    .stats = str(o.get("stats")),
                });
            } else if (std.mem.eql(u8, k, "remove")) {
                try self.drop(scratch, str(o.get("path")) orelse continue, try jsonDv(scratch, o.get("deletionVector")));
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
    /// Each row group is decoded into memory freed once its rows are kept, and the
    /// file map is sized for the rows up front, as an arena keeps every outgrown table.
    fn checkpoint(self: *State, url: []const u8) !void {
        var reader_mem = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer reader_mem.deinit();
        var batch_mem = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer batch_mem.deinit();
        const scratch = batch_mem.allocator();
        const r = try Reader.openProjected(reader_mem.allocator(), url, &checkpoint_columns);
        defer r.close();
        var rows: usize = 0;
        for (r.md.row_groups) |g| rows +|= std.math.cast(usize, g.num_rows) orelse 0;
        try self.files.ensureUnusedCapacity(self.arena, @min(rows, 1 << 22));
        while (try r.next(scratch)) |b| {
            defer _ = batch_mem.reset(.retain_capacity);
            const c_path = b.schema.indexOf("add.path");
            const c_size = b.schema.indexOf("add.size");
            const c_parts = b.schema.indexOf("add.partitionValues");
            const c_dv = b.schema.indexOf("add.deletionVector.storageType");
            const c_dv_at = b.schema.indexOf("add.deletionVector.pathOrInlineDv");
            const c_dv_off = b.schema.indexOf("add.deletionVector.offset");
            const c_dv_size = b.schema.indexOf("add.deletionVector.sizeInBytes");
            const c_dv_card = b.schema.indexOf("add.deletionVector.cardinality");
            const c_stats = b.schema.indexOf("add.stats");
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
                        var o = std.json.ObjectMap.init(scratch);
                        try o.put("storageType", .{ .string = storage.string });
                        const where = cell(b, c_dv_at, i);
                        if (where == .string) try o.put("pathOrInlineDv", .{ .string = where.string });
                        const ints = [_]struct { ?usize, []const u8 }{ .{ c_dv_off, "offset" }, .{ c_dv_size, "sizeInBytes" }, .{ c_dv_card, "cardinality" } };
                        for (ints) |f| {
                            const v = cell(b, f[0], i);
                            if (v == .int) try o.put(f[1], .{ .integer = v.int });
                        }
                        dv = .{ .object = o };
                    }
                    const stats = cell(b, c_stats, i);
                    try self.put(.{
                        .path = cell(b, c_path, i).string,
                        .size = if (size == .int and size.int >= 0) @intCast(size.int) else null,
                        .parts = jsonOf(scratch, cell(b, c_parts, i)),
                        .dv = dv,
                        .stats = if (stats == .string) stats.string else null,
                    });
                }
                if (cell(b, c_schema, i) == .string) {
                    const id = cell(b, c_id, i);
                    try self.meta(if (id == .string) id.string else null, cell(b, c_schema, i).string, jsonOf(scratch, cell(b, c_pcols, i)), jsonOf(scratch, cell(b, c_config, i)));
                }
                if (cell(b, c_rv, i) == .int) {
                    const wv = cell(b, c_wv, i);
                    try self.protocol(cell(b, c_rv, i).int, if (wv == .int) wv.int else null, jsonOf(scratch, cell(b, c_feat, i)), jsonOf(scratch, cell(b, c_wfeat, i)));
                }
            }
        }
    }

    fn jsonOf(a: std.mem.Allocator, v: Value) ?std.json.Value {
        if (v != .string) return null;
        return std.json.parseFromSliceLeaky(std.json.Value, a, v.string, .{}) catch null;
    }

    /// Refuses a table whose reader version or features basalt does not honour, or
    /// whose column mapping mode the protocol does not define.
    pub fn checkProtocol(self: *const State) !void {
        if (self.reader_version < 1 or self.reader_version > 3)
            return refuse("{s} needs Delta reader version {d}; basalt reads reader versions 1 to 3", .{ self.name, self.reader_version });
        if (self.mappingMode()) |m| if (!oneOf(m, &.{ "name", "id" }))
            return refuse("{s} uses column mapping mode `{s}`, which the Delta protocol does not define (`none`, `name` or `id`); basalt cannot tell how its files name their columns", .{ self.name, m });
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
    "add.path",                       "add.size",                          "add.partitionValues",       "add.deletionVector.storageType",
    "add.stats",                      "add.deletionVector.pathOrInlineDv", "add.deletionVector.offset", "add.deletionVector.sizeInBytes",
    "add.deletionVector.cardinality", "metaData.id",                       "metaData.schemaString",     "metaData.partitionColumns",
    "metaData.configuration",         "protocol.minReaderVersion",         "protocol.minWriterVersion", "protocol.readerFeatures",
    "protocol.writerFeatures",        "checkpointMetadata.version",        "sidecar.path",
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
    for (x.array.items) |e| if (e == .string) try out.append(try arena.dupe(u8, e.string));
    return out.items;
}

/// `v` copied into `arena`, so it outlives the parse it came from. Kept action
/// fields (partition values, a deletion vector, the configuration) are flat maps;
/// nesting past `depth` is dropped.
fn ownJson(arena: std.mem.Allocator, v: std.json.Value, depth: u8) !std.json.Value {
    switch (v) {
        .string => |s| return .{ .string = try arena.dupe(u8, s) },
        .number_string => |s| return .{ .number_string = try arena.dupe(u8, s) },
        .array => |xs| {
            if (depth == 0) return .null;
            var out = try std.json.Array.initCapacity(arena, xs.items.len);
            for (xs.items) |x| out.appendAssumeCapacity(try ownJson(arena, x, depth - 1));
            return .{ .array = out };
        },
        .object => |o| {
            if (depth == 0) return .null;
            var out = std.json.ObjectMap.init(arena);
            try out.ensureTotalCapacity(o.count());
            var it = o.iterator();
            while (it.next()) |kv| out.putAssumeCapacity(try arena.dupe(u8, kv.key_ptr.*), try ownJson(arena, kv.value_ptr.*, depth - 1));
            return .{ .object = out };
        },
        else => return v,
    }
}

/// `schemaString` as the reader's columns: a struct's fields as dotted columns,
/// an array or map as one JSON text column, every column nullable.
fn schemaOf(arena: std.mem.Allocator, name: []const u8, text: []const u8) !types.Schema {
    return (try parseSchema(arena, name, text, null)).schema;
}

const Parsed = struct { schema: types.Schema, mapping: ?*const Mapping = null };

/// `schemaOf`, with how the files name each column when the table maps them `by`.
fn parseSchema(arena: std.mem.Allocator, name: []const u8, text: []const u8, by: ?Mapping.By) !Parsed {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch
        return refuse("{s} has a schemaString that is not JSON", .{name});
    var out = std.array_list.Managed(types.Schema.Field).init(arena);
    try addFields(arena, name, &out, "", v, 0);
    const b = by orelse return .{ .schema = .{ .fields = out.items } };
    const m = try arena.create(Mapping);
    m.* = try Mapping.init(arena, b, (try mapType(arena, name, b, "", v, 0)).children);
    return .{ .schema = .{ .fields = out.items }, .mapping = m };
}

/// A type of `schemaString` as the mapping of what it nests: a struct's fields,
/// each found by its physical name or id, a list's element, a map's key and value.
fn mapType(arena: std.mem.Allocator, name: []const u8, by: Mapping.By, col: []const u8, t: std.json.Value, depth: usize) !Mapping.Node {
    if (depth >= table.max_depth) return refuse("{s} has a schemaString nested deeper than {d} levels", .{ name, table.max_depth });
    if (t != .object) return .{};
    const kind = str(t.object.get("type")) orelse "";
    if (std.mem.eql(u8, kind, "array")) {
        const kids = try arena.alloc(Mapping.Node, 1);
        kids[0] = try mapType(arena, name, by, col, t.object.get("elementType") orelse .null, depth + 1);
        return .{ .kind = .list, .children = kids };
    }
    if (std.mem.eql(u8, kind, "map")) {
        const kids = try arena.alloc(Mapping.Node, 2);
        kids[0] = try mapType(arena, name, by, col, t.object.get("keyType") orelse .null, depth + 1);
        kids[1] = try mapType(arena, name, by, col, t.object.get("valueType") orelse .null, depth + 1);
        return .{ .kind = .map, .children = kids };
    }
    const fields = switch (try dtypes.fields(arena, t, .lenient)) {
        .fields => |fs| fs,
        .problem => return .{},
    };
    var kids = std.array_list.Managed(Mapping.Node).init(arena);
    for (fields) |f| {
        const fname = f.name;
        const full = if (col.len == 0) fname else try std.mem.concat(arena, u8, &.{ col, ".", fname });
        const meta = f.metadata;
        var n = try mapType(arena, name, by, full, f.ty, depth + 1);
        n.name = fname;
        n.physical = (if (meta) |o| str(o.get("delta.columnMapping.physicalName")) else null) orelse "";
        if (meta) |o| if (o.get("delta.columnMapping.id")) |id| if (id == .integer and id.integer >= 0 and id.integer <= std.math.maxInt(i32)) {
            n.id = @intCast(id.integer);
        };
        if (by == .name and n.physical.len == 0)
            return refuse("{s} uses column mapping (mode `name`) but its column `{s}` has no physical name (`delta.columnMapping.physicalName`), so basalt cannot find it in the files", .{ name, full });
        if (by == .id and n.id == null)
            return refuse("{s} uses column mapping (mode `id`) but its column `{s}` has no field id (`delta.columnMapping.id`), so basalt cannot find it in the files", .{ name, full });
        try kids.append(n);
    }
    return .{ .kind = .@"struct", .children = kids.items };
}

fn addFields(arena: std.mem.Allocator, name: []const u8, out: *std.array_list.Managed(types.Schema.Field), prefix: []const u8, v: std.json.Value, depth: usize) !void {
    if (depth >= table.max_depth) return refuse("{s} has a schemaString nested deeper than {d} levels", .{ name, table.max_depth });
    const fields = switch (try dtypes.fields(arena, v, .reading)) {
        .fields => |fs| fs,
        .problem => |p| return switch (p) {
            .nameless => refuse("{s} has a schemaString field with no name", .{name}),
            .not_struct, .not_object => refuse("{s} has a schemaString that is not a struct of fields", .{name}),
        },
    };
    for (fields) |f| {
        const full = try std.mem.concat(arena, u8, &.{ prefix, f.name });
        const t = f.ty;
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
    return dtypes.scalarType(t, .read) orelse
        refuse("{s} has a column `{s}` of type `{s}`, which basalt does not read", .{ name, col, t });
}

/// `root`'s table at `at`; `listed` is `_delta_log/`'s listing when detection
/// already made it.
pub fn resolve(arena: std.mem.Allocator, root: []const u8, at: table.At, listed_in: ?[]const []const u8) !*const table.Resolved {
    const st = try snapshot(arena, root, at, listed_in);
    try st.checkProtocol();
    const layout = try layoutOf(arena, st, root, st.files.values());
    std.sort.pdq(Layout.File, layout.files, {}, struct {
        fn lt(_: void, a: Layout.File, b: Layout.File) bool {
            return std.mem.lessThan(u8, a.path, b.path);
        }
    }.lt);
    const r = try arena.create(table.Resolved);
    r.* = .{
        .kind = .delta,
        .root = root,
        .label = try msgfmt.allocPrint(arena, "version {d}, {d} file{s}", .{ st.version, layout.files.len, if (layout.files.len == 1) "" else "s" }),
        .layout = &layout.layout,
    };
    return r;
}

/// A layout over some of a table's live files, `files` aligned with `adds`.
pub const FileLayout = struct { layout: Layout, files: []Layout.File };

/// `adds` (live files of `st`) as a Parquet layout of the table's schema, in the
/// order given, each file with its deletion vector's descriptor checked (read
/// only when the file is) and, under column mapping, how its columns are named.
pub fn layoutOf(arena: std.mem.Allocator, st: *const State, root: []const u8, adds: []const Add) !*FileLayout {
    const name = st.name;
    const mode = st.mappingMode();
    const by: ?Mapping.By = if (mode) |m| (if (std.mem.eql(u8, m, "id")) .id else .name) else null;
    const parsed = try parseSchema(arena, name, st.schema_string orelse return refuse("{s} has no metaData action in its log", .{name}), by);
    const schema = parsed.schema;
    if (schema.fields.len == 0) return refuse("{s} has a schema with no columns", .{name});

    const part_fields = try arena.alloc(usize, st.part_cols.len);
    const part_keys = try arena.alloc([]const u8, st.part_cols.len);
    for (part_fields, part_keys, st.part_cols) |*pf, *pk, c| {
        pf.* = schema.indexOf(c) orelse
            return refuse("{s} has a partition column `{s}` its schema lacks", .{ name, c });
        pk.* = c;
        if (parsed.mapping) |m| for (m.fields) |n| if (std.mem.eql(u8, n.name.?, c)) {
            if (n.physical.len == 0)
                return refuse("{s} uses column mapping (mode `{s}`) but its partition column `{s}` has no physical name (`delta.columnMapping.physicalName`), by which its files' partition values are keyed", .{ name, mode.?, c });
            pk.* = n.physical;
        };
    }

    const files = try arena.alloc(Layout.File, adds.len);
    for (files, adds) |*f, a| {
        const decoded = a.path;
        const parts = try arena.alloc(Value, part_fields.len);
        for (parts, part_fields, part_keys, st.part_cols) |*p, fi, k, c| {
            const text: ?[]const u8 = if (a.parts) |pv| (if (pv == .object) str(pv.object.get(k)) else null) else null;
            p.* = (try table.partitionValue(arena, text, schema.fields[fi].ty)) orelse
                return refuse("{s} has file `{s}` with partition value `{s}` for column `{s}`, which is not a {s}", .{ name, decoded, text.?, c, try schema.fields[fi].ty.name(arena) });
        }
        f.* = .{ .path = try table.mapPath(arena, root, decoded), .size = a.size, .parts = parts };
        if (a.dv) |dv| {
            const ref = try arena.create(dvs.Ref);
            ref.* = .{ .name = name, .root = root, .file = decoded, .d = try dvs.descriptor(name, decoded, dv) };
            if (ref.d.cardinality > 0) f.deletes = ref.deletes();
        }
    }
    const out = try arena.create(FileLayout);
    out.* = .{ .layout = .{ .name = name, .root = root, .schema = schema, .part_fields = part_fields, .files = files, .mapping = parsed.mapping }, .files = files };
    return out;
}

/// `root`'s log replayed to `at` (the newest version when unpinned), refusing
/// only what makes the replay itself unreliable; what a reader or a writer
/// cannot honour each judges from the state.
pub fn snapshot(arena: std.mem.Allocator, root: []const u8, at: table.At, listed_in: ?[]const []const u8) !*State {
    _ = replays.fetchAdd(1, .monotonic);
    const name = try msgfmt.allocPrint(arena, "Delta table `{s}`", .{root});
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
    var text_mem = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer text_mem.deinit();
    var line_mem = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer line_mem.deinit();
    var v: u64 = if (from) |f| f.version + 1 else 0;
    while (v <= target) : (v += 1) {
        _ = text_mem.reset(.retain_capacity);
        const text = try table.readWhole(text_mem.allocator(), log.commits.get(v).?);
        var lines = std.mem.splitScalar(u8, text, '\n');
        while (lines.next()) |line| {
            const t = std.mem.trim(u8, line, " \r\t");
            if (t.len == 0) continue;
            _ = line_mem.reset(.retain_capacity);
            try st.action(line_mem.allocator(), t);
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
        try st.action(a, "{\"add\":{\"path\":\"a.parquet\",\"partitionValues\":{}}}");
        for (order) |line| try st.action(a, line);
        try testing.expectEqual(@as(usize, 1), st.files.count());
        try testing.expect(st.files.values()[0].dv != null);
        try st.action(a, "{\"remove\":{\"path\":\"a.parquet\",\"deletionVector\":" ++ dv ++ "}}");
        try testing.expectEqual(@as(usize, 0), st.files.count());
    }
    var st = State{ .arena = a, .name = "T", .key_root = "/t/" };
    try st.action(a, "{\"add\":{\"path\":\"x%3D1/b.parquet\",\"partitionValues\":{}}}");
    try st.action(a, "{\"remove\":{\"path\":\"file:///t/x%3D1/b.parquet\"}}");
    try testing.expectEqual(@as(usize, 0), st.files.count());
    try st.action(a, "{\"add\":{\"path\":\"hdfs://nn/t/c.parquet\",\"partitionValues\":{}}}");
    try st.action(a, "{\"remove\":{\"path\":\"hdfs://nn/t/c.parquet\"}}");
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
    try st.checkProtocol();
    try testing.expectEqualStrings("name", st.mappingMode().?);
    st.mapping = "nonsense";
    try testing.expectError(table.Error.LakeTableRefused, st.checkProtocol());
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(table.Error.LakeTableRefused).?, "column mapping mode `nonsense`, which the Delta protocol does not define") != null);
    st.reader_version = 1;
    try st.checkProtocol();
    try testing.expect(st.mappingMode() == null);
    st.reader_version = 3;
    st.mapping = "id";
    try testing.expect(st.mappingMode() == null);
    st.features = &.{"columnMapping"};
    try testing.expectEqualStrings("id", st.mappingMode().?);
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

test "replay keeps only what it needs: a checkpoint's batches and each commit's parse are freed, what it keeps is copied out" {
    const dir = "../testdata/delta/dv-with-columnmapping/_delta_log/";
    const log = [_]struct { []const u8, []const u8 }{
        .{ "00000000000000000010.checkpoint.parquet", @embedFile(dir ++ "00000000000000000010.checkpoint.parquet") },
        .{ "00000000000000000010.json", @embedFile(dir ++ "00000000000000000010.json") },
        .{ "00000000000000000011.json", @embedFile(dir ++ "00000000000000000011.json") },
        .{ "00000000000000000012.json", @embedFile(dir ++ "00000000000000000012.json") },
        .{ "00000000000000000013.json", @embedFile(dir ++ "00000000000000000013.json") },
        .{ "00000000000000000014.json", @embedFile(dir ++ "00000000000000000014.json") },
        .{ "00000000000000000015.json", @embedFile(dir ++ "00000000000000000015.json") },
    };
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.makePath("_delta_log");
    for (log) |f| {
        var buf: [96]u8 = undefined;
        try tmp.dir.writeFile(.{ .sub_path = try std.fmt.bufPrint(&buf, "_delta_log/{s}", .{f[0]}), .data = f[1] });
    }
    const real = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(real);
    const root = try std.mem.concat(testing.allocator, u8, &.{ real, "/" });
    defer testing.allocator.free(root);

    var counting = std.testing.FailingAllocator.init(testing.allocator, .{});
    var ar = std.heap.ArenaAllocator.init(counting.allocator());
    defer ar.deinit();
    const st = try snapshot(ar.allocator(), root, .{}, null);

    try testing.expectEqual(@as(u64, 15), st.version);
    try testing.expectEqualStrings("name", st.mappingMode().?);
    try testing.expect(oneOf("deletionVectors", st.features) and oneOf("columnMapping", st.features));
    try testing.expectEqual(@as(usize, 1), st.part_cols.len);
    var dvs_seen: usize = 0;
    for (st.files.values()) |f| {
        const pv = f.parts.?.object.get("col-60c949ca-b8bc-4330-b931-b73fb4c60037").?;
        try testing.expect(std.mem.indexOf(u8, f.path, pv.string) != null);
        try testing.expect(std.mem.startsWith(u8, f.stats.?, "{\"numRecords\":"));
        if (f.dv) |d| {
            _ = try dvs.descriptor("T", f.path, d);
            dvs_seen += 1;
        }
    }
    try testing.expectEqual(@as(usize, 15), st.files.count());
    try testing.expect(dvs_seen > 0);
    try testing.expect(counting.allocated_bytes - counting.freed_bytes < 160 * 1024);
}

test "schemaString primitives from delta_types read as the reader's old table did" {
    const Orig = struct {
        fn oldPrimitive(t: []const u8) ?types.Type {
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
            return null;
        }
    };
    const names = [_][]const u8{
        "long",           "integer",        "short",          "byte",           "double",        "float",          "boolean",
        "string",         "binary",         "date",           "timestamp",      "timestamp_ntz", "void",           "variant",
        "Long",           "",               "interval",       "timestamp_tz",   "decimal",       "decimal(10,2)",  "decimal( 10 , 2 )",
        "decimal(38,38)", "decimal(1,0)",   "decimal(5,2,9)", "decimal(10)",    "decimal(,2)",   "decimal(39,0)",  "decimal(5,6)",
        "decimal(0,0)",   "decimal()",      "decimal(10,2",   "decimal(-1,0)",  "decimal(a,b)",  "decimal(256,0)", "Decimal(10,2)",
        "decimal(10,)",   "decimal(3,255)", "decimal(+5,2)",  "decimal(05,02)",
    };
    for (names) |t| {
        const old = Orig.oldPrimitive(t);
        const new = primitive("T", "c", t) catch |e| {
            try testing.expect(old == null);
            _ = eval.takeFailure(e);
            continue;
        };
        try testing.expectEqual(old.?.asNullable(), new.asNullable());
    }
}
