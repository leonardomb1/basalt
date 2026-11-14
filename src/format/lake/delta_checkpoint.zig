//! Writing a Delta table's classic checkpoint: after a commit lands at a version
//! that is a multiple of `delta.checkpointInterval` (10 when unset, as Spark has
//! it), the table's state at that version is written to
//! `_delta_log/<20-digit version>.checkpoint.parquet`, one action a row, and
//! `_last_checkpoint` points at it. The state is replayed anew at that version with
//! every action kept whole (`delta.snapshotWhole`), so what another writer put in
//! the log (tags, `txn`, `domainMetadata`, a deletion vector) is carried, not only
//! what basalt reads by.
//!
//! A row holds one of `protocol`, `metaData`, `txn`, `domainMetadata`, `add` or
//! `remove`, the others NULL, each a struct column as the protocol lays it out;
//! the fields the protocol types `int` (a protocol's versions, a deletion vector's
//! offset and size) are written as INT32, as Spark's reader insists. An `add` or
//! `remove` is written with `dataChange` false. A `remove` is kept until
//! `delta.deletedFileRetentionDuration` (a week when unset) past its
//! `deletionTimestamp`, as the tombstone a concurrent reader of an older version
//! and VACUUM rely on.
//!
//! A local checkpoint is written under a hidden name and renamed into place, so a
//! reader never lists a partial one; an object store's upload lands whole. A
//! checkpoint is an optimisation: the commit it follows has landed, so a failure
//! here is the caller's to report as a warning, never the write's failure.
//! `_last_checkpoint` is not moved back past a newer checkpoint another writer made.
//! After a checkpoint the expired log is cleaned (`cleanLog`): commits, older
//! checkpoints and checksum files below the newest checkpoint past
//! `delta.logRetentionDuration` are deleted, unless `delta.enableExpiredLogCleanup`
//! is false.
//!
//! The columns (`columns`) are typed as `schemaString` would type them, so the
//! reader's own type mapping and leaf layouts serve; rows (`Rows`) are built a
//! batch at a time; `Written` is what `_last_checkpoint` records.

const std = @import("std");
const HttpClient = @import("../../net/http_std.zig");
const delta = @import("delta.zig");
const dlog = @import("delta_log.zig");
const dtypes = @import("delta_types.zig");
const table = @import("table.zig");
const column = @import("../../exec/column.zig");
const msgfmt = @import("../../base/msgfmt.zig");
const objstore = @import("../../store/objstore.zig");
const folder = @import("../../store/folder.zig");
const http_client = @import("../../net/http_client.zig");
const pqwrite = @import("../parquet/write.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const default_interval = 10;

const default_retention_ms: i64 = 7 * 24 * 3600 * 1000;

const rows_per_batch = 4096;

const map_ss = "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true}";
const dv_type = "{\"type\":\"struct\",\"fields\":[" ++
    f("storageType", "\"string\"") ++ "," ++ f("pathOrInlineDv", "\"string\"") ++ "," ++ f("offset", "\"integer\"") ++ "," ++
    f("sizeInBytes", "\"integer\"") ++ "," ++ f("cardinality", "\"long\"") ++ "," ++ f("maxRowIndex", "\"long\"") ++ "]}";

fn f(comptime name: []const u8, comptime ty: []const u8) []const u8 {
    return "{\"name\":\"" ++ name ++ "\",\"type\":" ++ ty ++ ",\"nullable\":true,\"metadata\":{}}";
}

fn st(comptime fields: []const u8) []const u8 {
    return "{\"type\":\"struct\",\"fields\":[" ++ fields ++ "]}";
}

const columns = [_]struct { []const u8, []const u8 }{
    .{ "txn", st(f("appId", "\"string\"") ++ "," ++ f("version", "\"long\"") ++ "," ++ f("lastUpdated", "\"long\"")) },
    .{ "add", st(f("path", "\"string\"") ++ "," ++ f("partitionValues", map_ss) ++ "," ++ f("size", "\"long\"") ++ "," ++
        f("modificationTime", "\"long\"") ++ "," ++ f("dataChange", "\"boolean\"") ++ "," ++ f("tags", map_ss) ++ "," ++
        f("deletionVector", dv_type) ++ "," ++ f("baseRowId", "\"long\"") ++ "," ++ f("defaultRowCommitVersion", "\"long\"") ++ "," ++
        f("clusteringProvider", "\"string\"") ++ "," ++ f("stats", "\"string\"")) },
    .{ "remove", st(f("path", "\"string\"") ++ "," ++ f("deletionTimestamp", "\"long\"") ++ "," ++ f("dataChange", "\"boolean\"") ++ "," ++
        f("extendedFileMetadata", "\"boolean\"") ++ "," ++ f("partitionValues", map_ss) ++ "," ++ f("size", "\"long\"") ++ "," ++
        f("tags", map_ss) ++ "," ++ f("deletionVector", dv_type) ++ "," ++ f("baseRowId", "\"long\"") ++ "," ++
        f("defaultRowCommitVersion", "\"long\"") ++ "," ++ f("stats", "\"string\"")) },
    .{ "metaData", st(f("id", "\"string\"") ++ "," ++ f("name", "\"string\"") ++ "," ++ f("description", "\"string\"") ++ "," ++
        f("format", st(f("provider", "\"string\"") ++ "," ++ f("options", map_ss))) ++ "," ++ f("schemaString", "\"string\"") ++ "," ++
        f("partitionColumns", "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}") ++ "," ++
        f("configuration", map_ss) ++ "," ++ f("createdTime", "\"long\"")) },
    .{ "protocol", st(f("minReaderVersion", "\"integer\"") ++ "," ++ f("minWriterVersion", "\"integer\"") ++ "," ++
        f("readerFeatures", "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}") ++ "," ++
        f("writerFeatures", "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}")) },
    .{ "domainMetadata", st(f("domain", "\"string\"") ++ "," ++ f("configuration", "\"string\"") ++ "," ++ f("removed", "\"boolean\"")) },
    .{ "checkpointMetadata", st(f("version", "\"long\"") ++ "," ++ f("tags", map_ss)) },
};

const Col = enum { txn, add, remove, metaData, protocol, domainMetadata, checkpointMetadata };

/// The checkpoint interval `st` asks for: `delta.checkpointInterval` when it is a
/// positive number, else `default_interval`.
pub fn interval(st_: *const delta.State) u64 {
    const t = st_.property("delta.checkpointInterval") orelse return default_interval;
    const n = std.fmt.parseInt(u64, std.mem.trim(u8, t, " "), 10) catch return default_interval;
    return if (n == 0) default_interval else n;
}

/// Whether a commit at `version` is followed by a checkpoint, the table asking
/// for one every `every` commits.
pub fn due(version: u64, every: u64) bool {
    return version > 0 and every > 0 and version % every == 0;
}

/// `delta.deletedFileRetentionDuration` (`interval 7 days`, Spark's spelling, in
/// seconds to weeks) in milliseconds; a week when unset or unreadable.
pub fn retentionMs(text: ?[]const u8) i64 {
    return retentionWith(text, default_retention_ms);
}

/// A duration property as `retentionMs` reads one, `fallback` when unset or unreadable.
pub fn retentionWith(text: ?[]const u8, fallback: i64) i64 {
    const default_retention = fallback;
    const t = std.mem.trim(u8, text orelse return default_retention, " ");
    var it = std.mem.tokenizeScalar(u8, t, ' ');
    var word = it.next() orelse return default_retention;
    if (std.ascii.eqlIgnoreCase(word, "interval")) word = it.next() orelse return default_retention;
    const n = std.fmt.parseInt(i64, word, 10) catch return default_retention;
    const unit = it.next() orelse return default_retention;
    const units = [_]struct { []const u8, i64 }{
        .{ "millisecond", 1 },    .{ "second", 1000 },          .{ "minute", 60 * 1000 },
        .{ "hour", 3600 * 1000 }, .{ "day", 24 * 3600 * 1000 }, .{ "week", 7 * 24 * 3600 * 1000 },
    };
    for (units) |u| {
        const base = if (unit.len > 0 and (unit[unit.len - 1] == 's' or unit[unit.len - 1] == 'S')) unit[0 .. unit.len - 1] else unit;
        if (std.ascii.eqlIgnoreCase(base, u[0])) return std.math.mul(i64, n, u[1]) catch default_retention;
    }
    return default_retention;
}

const default_log_retention_ms: i64 = 30 * 24 * 3600 * 1000;

/// The log of `root`'s table cleaned as Spark cleans it after a checkpoint: of
/// the complete checkpoints at or below `version`, the newest whose commit is
/// older than `keep_ms` before `now_ms` is the oldest version kept, and every
/// commit, checkpoint and checksum file below it is deleted, oldest first, so a
/// cleanup cut short leaves the log readable from a later checkpoint. A commit's
/// age is its `commitInfo` (`inCommitTimestamp`, else `timestamp`). Returns the
/// files deleted.
pub fn cleanLog(arena: std.mem.Allocator, root: []const u8, version: u64, keep_ms: i64, now_ms: i64) !usize {
    const log_path = try std.mem.concat(arena, u8, &.{ root, delta.log_dir });
    const listed = try folder.listRaw(arena, log_path);
    var commits = std.AutoHashMapUnmanaged(u64, []const u8).empty;
    var parts = std.AutoHashMapUnmanaged(u64, u32).empty;
    var whole = std.AutoHashMapUnmanaged(u64, u32).empty;
    const Doomed = struct { v: u64, url: []const u8 };
    var all = std.array_list.Managed(Doomed).init(arena);
    for (listed) |u| {
        const rel = folder.below(u, log_path);
        if (std.mem.indexOfAny(u8, rel, "/\\") != null) continue;
        switch (delta.parseName(rel)) {
            .commit => |v| {
                try commits.put(arena, v, u);
                try all.append(.{ .v = v, .url = u });
            },
            .checkpoint => |c| {
                const g = try parts.getOrPut(arena, c.version);
                if (!g.found_existing) g.value_ptr.* = 0;
                g.value_ptr.* += 1;
                try whole.put(arena, c.version, c.parts);
                try all.append(.{ .v = c.version, .url = u });
            },
            .v2 => |v| {
                try parts.put(arena, v, 1);
                try whole.put(arena, v, 1);
                try all.append(.{ .v = v, .url = u });
            },
            .other => if (versionOf(rel, ".crc")) |v| try all.append(.{ .v = v, .url = u }),
        }
    }
    var cands = std.array_list.Managed(u64).init(arena);
    var it = parts.iterator();
    while (it.next()) |kv| if (kv.key_ptr.* <= version and kv.value_ptr.* == whole.get(kv.key_ptr.*).?) try cands.append(kv.key_ptr.*);
    std.mem.sort(u64, cands.items, {}, std.sort.desc(u64));
    const cutoff = now_ms -| keep_ms;
    const floor = for (cands.items) |c| {
        const at = commits.get(c) orelse continue;
        const ts = commitTime(arena, at) orelse continue;
        if (ts <= cutoff) break c;
    } else return 0;
    std.mem.sort(Doomed, all.items, {}, struct {
        fn lt(_: void, x: Doomed, y: Doomed) bool {
            return x.v < y.v;
        }
    }.lt);
    var gone: usize = 0;
    var client: ?HttpClient = null;
    defer if (client) |*c| c.deinit();
    for (all.items) |d| {
        if (d.v >= floor) break;
        if (objstore.isUrl(d.url)) {
            if (client == null) client = http_client.initClient(arena);
            try (try objstore.parse(arena, d.url)).delete(arena, &client.?);
        } else std.fs.cwd().deleteFile(d.url) catch |e| if (e != error.FileNotFound) return e;
        gone += 1;
    }
    return gone;
}

/// The version a log file named `<20 digits><suffix>` is of.
fn versionOf(name: []const u8, suffix: []const u8) ?u64 {
    if (!std.mem.endsWith(u8, name, suffix) or name.len != 20 + suffix.len) return null;
    return std.fmt.parseInt(u64, name[0..20], 10) catch null;
}

/// When the commit at `url` was made, by its `commitInfo`.
fn commitTime(arena: std.mem.Allocator, url: []const u8) ?i64 {
    const text = table.readWhole(arena, url) catch return null;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const t = std.mem.trim(u8, line, " \r\t");
        if (t.len == 0) continue;
        const v = std.json.parseFromSliceLeaky(std.json.Value, arena, t, .{}) catch continue;
        if (v != .object) continue;
        const ci = v.object.get("commitInfo") orelse continue;
        if (ci != .object) continue;
        for ([_][]const u8{ "inCommitTimestamp", "timestamp" }) |k| if (ci.object.get(k)) |x| if (x == .integer) return x.integer;
        return null;
    }
    return null;
}

pub const Written = struct { version: u64, rows: u64, bytes: u64, adds: u64, keep_log_ms: ?i64 = null };

/// The checkpoint of `root`'s table at `version`, written, and `_last_checkpoint`
/// pointed at it; `now_ms` decides which tombstones have expired.
pub fn write(arena: std.mem.Allocator, root: []const u8, version: u64, now_ms: i64) !Written {
    const s = try delta.snapshotWhole(arena, root, version);
    const whole = s.whole.?;
    const v2 = usesV2(s);
    const schema = try checkpointSchema(arena, s.name, v2);
    const final = try msgfmt.allocPrint(arena, "{s}{s}{d:0>20}.checkpoint.parquet", .{ root, delta.log_dir, version });
    const local = !objstore.isRemote(final);
    const path = if (local) try msgfmt.allocPrint(arena, "{s}{s}.{d:0>20}.checkpoint.parquet.{s}.tmp", .{ root, delta.log_dir, version, &dlog.uuid() }) else final;
    const w = try pqwrite.Writer.openPhysical(arena, path, schema.schema, .snappy, .truncate, schema.phys);
    var done = false;
    defer if (!done) {
        w.abort();
        if (local) std.fs.cwd().deleteFile(path) catch {};
    };
    var rows = Rows{ .arena = arena, .w = w, .schema = schema.schema };
    try rows.init();
    if (v2) try rows.put(.checkpointMetadata, try std.json.parseFromSliceLeaky(std.json.Value, arena, try msgfmt.allocPrint(arena, "{{\"version\":{d}}}", .{version}), .{}));
    if (whole.protocol) |v| try rows.put(.protocol, v);
    if (whole.meta) |v| try rows.put(.metaData, v);
    for (whole.txns.values()) |v| try rows.put(.txn, v);
    for (whole.domains.values()) |v| try rows.put(.domainMetadata, v);
    for (whole.adds.values()) |v| try rows.put(.add, try settled(v));
    const keep_ms = retentionMs(s.property("delta.deletedFileRetentionDuration"));
    for (whole.removes.values()) |v| {
        const at = if (v.object.get("deletionTimestamp")) |t| (if (t == .integer) t.integer else 0) else 0;
        if (at +| keep_ms < now_ms) continue;
        try rows.put(.remove, try settled(v));
    }
    try rows.flush();
    const sum = try w.closeSummary(arena);
    done = true;
    if (local) {
        std.fs.cwd().rename(path, final) catch |e| {
            std.fs.cwd().deleteFile(path) catch {};
            return e;
        };
        try dlog.syncDir(dlog.trimDir(try std.mem.concat(arena, u8, &.{ root, delta.log_dir })));
    }
    var out = Written{ .version = version, .rows = rows.total, .bytes = sum.bytes, .adds = whole.adds.count() };
    const cleans = if (s.property("delta.enableExpiredLogCleanup")) |t| !std.ascii.eqlIgnoreCase(std.mem.trim(u8, t, " "), "false") else true;
    if (cleans) out.keep_log_ms = retentionWith(s.property("delta.logRetentionDuration"), default_log_retention_ms);
    try pointAt(arena, root, out);
    return out;
}

const Schema = struct { schema: types.Schema, phys: []const pqwrite.Physical };

/// Whether the table's checkpoints are V2 (feature `v2Checkpoint`): they then carry
/// a `checkpointMetadata` row, under the classic name the protocol allows them.
fn usesV2(s: *const delta.State) bool {
    for ([_][]const []const u8{ s.features, s.writer_features }) |fs| for (fs) |feat| if (std.mem.eql(u8, feat, "v2Checkpoint")) return true;
    return false;
}

fn checkpointSchema(arena: std.mem.Allocator, name: []const u8, v2: bool) !Schema {
    const n = if (v2) columns.len else columns.len - 1;
    const fields = try arena.alloc(types.Schema.Field, n);
    const phys = try arena.alloc(pqwrite.Physical, n);
    for (columns[0..n], fields, phys) |c, *fd, *ph| {
        const v = try std.json.parseFromSliceLeaky(std.json.Value, arena, c[1], .{});
        fd.* = .{ .name = c[0], .ty = try delta.tableType(arena, name, c[0], v) };
        ph.* = .{ .leaves = try dtypes.leafPhysicals(arena, v) };
    }
    return .{ .schema = .{ .fields = fields }, .phys = phys };
}

/// An `add` or `remove` as a checkpoint holds it: `dataChange` false.
fn settled(v: std.json.Value) !std.json.Value {
    var o = try v.object.clone();
    try o.put("dataChange", .{ .bool = false });
    return .{ .object = o };
}

const Rows = struct {
    arena: std.mem.Allocator,
    w: *pqwrite.Writer,
    schema: types.Schema,
    builders: [columns.len]column.Builder = undefined,
    n: usize = 0,
    total: u64 = 0,
    text: std.Io.Writer.Allocating = undefined,

    fn init(self: *Rows) !void {
        for (self.builders[0..self.schema.fields.len], self.schema.fields) |*b, fd| b.* = column.Builder.init(self.arena, fd.ty);
        self.text = std.Io.Writer.Allocating.init(self.arena);
    }

    fn put(self: *Rows, which: Col, v: std.json.Value) !void {
        self.text.clearRetainingCapacity();
        try std.json.Stringify.value(v, .{}, &self.text.writer);
        for (self.builders[0..self.schema.fields.len], 0..) |*b, i| {
            if (i == @intFromEnum(which)) try b.append(.{ .string = self.text.written() }) else try b.append(.null);
        }
        self.n += 1;
        self.total += 1;
        if (self.n == rows_per_batch) try self.flush();
    }

    fn flush(self: *Rows) !void {
        if (self.n == 0) return;
        const cols = try self.arena.alloc(column.Column, self.schema.fields.len);
        for (self.builders[0..self.schema.fields.len], cols, self.schema.fields) |*b, *c, fd| {
            c.* = try b.finish();
            b.* = column.Builder.init(self.arena, fd.ty);
        }
        try self.w.writeBatch(self.arena, .{ .schema = &self.schema, .columns = cols, .len = self.n });
        self.n = 0;
    }
};

/// `_last_checkpoint` set to `c`, unless it already names a newer version.
fn pointAt(arena: std.mem.Allocator, root: []const u8, c: Written) !void {
    const at = try std.mem.concat(arena, u8, &.{ root, delta.log_dir, "_last_checkpoint" });
    if (table.readWhole(arena, at)) |old| {
        if (std.json.parseFromSliceLeaky(std.json.Value, arena, old, .{})) |v| {
            if (v == .object) if (v.object.get("version")) |n| if (n == .integer and n.integer > @as(i64, @intCast(c.version))) return;
        } else |_| {}
    } else |_| {}
    const text = try msgfmt.allocPrint(arena, "{{\"version\":{d},\"size\":{d},\"sizeInBytes\":{d},\"numOfAddFiles\":{d}}}", .{ c.version, c.rows, c.bytes, c.adds });
    if (try objstore.openRemote(arena, at, "application/json")) |w| {
        errdefer w.abort();
        w.io.writeAll(text) catch |e| return w.specific(e);
        w.io.flush() catch |e| return w.specific(e);
        return w.finish();
    }
    const tmp = try msgfmt.allocPrint(arena, "{s}{s}._last_checkpoint.{s}.tmp", .{ root, delta.log_dir, &dlog.uuid() });
    try std.fs.cwd().writeFile(.{ .sub_path = tmp, .data = text });
    std.fs.cwd().rename(tmp, at) catch |e| {
        std.fs.cwd().deleteFile(tmp) catch {};
        return e;
    };
}

test "a checkpoint interval and a tombstone's retention read as Spark spells them" {
    try testing.expect(!due(0, 10));
    try testing.expect(due(10, 10));
    try testing.expect(!due(11, 10));
    try testing.expect(due(3, 1));
    try testing.expectEqual(default_retention_ms, retentionMs(null));
    try testing.expectEqual(@as(i64, 2 * 3600 * 1000), retentionMs("interval 2 hours"));
    try testing.expectEqual(@as(i64, 24 * 3600 * 1000), retentionMs("interval 1 day"));
    try testing.expectEqual(@as(i64, 30 * 1000), retentionMs("30 seconds"));
    try testing.expectEqual(default_retention_ms, retentionMs("interval fortnight"));
}
