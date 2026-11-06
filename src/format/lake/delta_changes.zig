//! A Delta table's change data feed over a range of versions (`changes_from`,
//! `changes_to`): the rows each commit changed, with `_change_type` (`insert`,
//! `delete`, `update_preimage`, `update_postimage`), `_commit_version` and
//! `_commit_timestamp` after the table's columns. A commit that wrote change data
//! files (`cdc` actions) is read from those alone, their `_change_type` column
//! as written; any other commit's added files read as inserts and its removed
//! files as deletes, every row each held (less its deletion vector's), as the
//! protocol defines. A file a commit adds or removes without changing data
//! (`dataChange` false, as OPTIMIZE writes) holds no change.
//!
//! The range is read in the table's schema at its last version, files of older
//! versions fitted to it by name. It needs the change data feed enabled
//! (`delta.enableChangeDataFeed`) where it starts and in every `metaData` inside
//! it, and every commit in it still in `_delta_log/`; a range by time starts at
//! the first commit at or after its start and ends at the last at or before its end.

const std = @import("std");
const msgfmt = @import("../msgfmt.zig");
const Layout = @import("../parquet/fit.zig").Layout;
const Value = @import("../../exec/value.zig").Value;
const delta = @import("delta.zig");
const dvs = @import("delta_dv.zig");
const roaring = @import("roaring.zig");
const fit = @import("../parquet/fit.zig");
const table = @import("table.zig");
const types = @import("../../lang/types.zig");

const refuse = table.refuse;

pub const version_name = "_commit_version";
pub const timestamp_name = "_commit_timestamp";

const Change = struct { add: delta.Add, kind: ?[]const u8, version: u64, ms: ?i64, pair: ?Pair = null };

const Pair = struct { old: ?std.json.Value, new: ?std.json.Value, take: Take };

const Take = enum { newly_deleted, restored };

const Diff = struct {
    name: []const u8,
    root: []const u8,
    file: []const u8,
    old: ?dvs.Descriptor,
    new: ?dvs.Descriptor,
    take: Take,

    fn deletes(self: *const Diff) fit.Deletes {
        return .{ .ctx = self, .load = load };
    }

    /// The rows a file's reader skips: every row but those the pair changed, the
    /// ones its new deletion vector deletes and its old one did not, or the reverse.
    fn load(ctx: *const anyopaque, arena: std.mem.Allocator, rows: u64) anyerror!roaring.Bitmap {
        const self: *const Diff = @ptrCast(@alignCast(ctx));
        const n = std.math.cast(usize, rows) orelse return error.OutOfMemory;
        const kept_old = try kept(arena, self, self.old, n);
        const kept_new = try kept(arena, self, self.new, n);
        const skip = try arena.alloc(bool, n);
        for (skip, kept_old, kept_new) |*k, o, w| k.* = switch (self.take) {
            .newly_deleted => !(o and !w),
            .restored => !(!o and w),
        };
        return roaring.ofRows(arena, skip);
    }

    fn kept(arena: std.mem.Allocator, self: *const Diff, d: ?dvs.Descriptor, n: usize) ![]bool {
        const keep = try arena.alloc(bool, n);
        @memset(keep, true);
        const desc = d orelse return keep;
        const ref = dvs.Ref{ .name = self.name, .root = self.root, .file = self.file, .d = desc };
        const bm = try dvs.load(arena, &ref, n);
        _ = bm.clear(0, keep);
        return keep;
    }
};

/// `root`'s changes over `at.changes`, as a layout of the files that hold them.
pub fn resolve(arena: std.mem.Allocator, root: []const u8, at: table.At, listed_in: ?[]const []const u8) !*const table.Resolved {
    const c = at.changes.?;
    const ls = try delta.listLog(arena, root, listed_in);
    const name = ls.name;
    const to: u64 = if (c.to) |p| switch (p) {
        .version => |v| v,
        .us => |t| try ls.log.asOf(arena, ls.newest, t),
    } else ls.newest;
    if (to > ls.newest) return refuse("{s} has no version {d} to read changes up to (its newest is {d})", .{ name, to, ls.newest });
    const from: u64 = switch (c.from) {
        .version => |v| v,
        .us => |t| try firstAtOrAfter(arena, &ls.log, ls.newest, t),
    };
    if (from > ls.newest) return refuse("{s} has no version {d} to read changes from (its newest is {d})", .{ name, from, ls.newest });
    if (from > to) return refuse("{s}'s changes would start at version {d}, after they end at version {d}", .{ name, from, to });

    const end = try delta.snapshot(arena, root, .{ .version = to }, listed_in);
    try end.checkProtocol();
    const start = if (from == to) end else try delta.snapshot(arena, root, .{ .version = from }, listed_in);
    try start.checkProtocol();
    if (!fed(start.config)) return refuse("{s} does not record its changes at version {d}: `delta.enableChangeDataFeed` is not `true` there, so what an update or delete changed is not in its log", .{ name, from });

    var changes = std.ArrayList(Change).empty;
    var v = from;
    while (v <= to) : (v += 1) try commitChanges(arena, &ls.log, name, v, &changes);

    const adds = try arena.alloc(delta.Add, changes.items.len);
    for (adds, changes.items) |*a, ch| a.* = ch.add;
    const base = try delta.layoutOf(arena, end, root, adds);
    const l = base.layout;
    for ([_][]const u8{ fit.change_type_name, version_name, timestamp_name }) |n| if (l.schema.indexOf(n) != null)
        return refuse("{s} has a column `{s}`, the name its changes give their own column; rename it to read its changes", .{ name, n });

    const n = l.schema.fields.len;
    const fields = try arena.alloc(types.Schema.Field, n + 3);
    @memcpy(fields[0..n], l.schema.fields);
    fields[n] = .{ .name = fit.change_type_name, .ty = types.Type.init(.string).asNullable() };
    fields[n + 1] = .{ .name = version_name, .ty = types.Type.init(.int).asNullable() };
    fields[n + 2] = .{ .name = timestamp_name, .ty = types.Type.init(.timestamp).asNullable() };
    const part_fields = try arena.alloc(usize, l.part_fields.len + 2);
    @memcpy(part_fields[0..l.part_fields.len], l.part_fields);
    part_fields[l.part_fields.len] = n + 1;
    part_fields[l.part_fields.len + 1] = n + 2;
    for (base.files, changes.items) |*f, ch| {
        if (ch.pair) |pr| {
            const d = try arena.create(Diff);
            d.* = .{
                .name = name,
                .root = root,
                .file = ch.add.path,
                .old = if (pr.old) |j| try dvs.descriptor(name, ch.add.path, j) else null,
                .new = if (pr.new) |j| try dvs.descriptor(name, ch.add.path, j) else null,
                .take = pr.take,
            };
            f.deletes = d.deletes();
        }
        const parts = try arena.alloc(Value, f.parts.len + 2);
        @memcpy(parts[0..f.parts.len], f.parts);
        parts[f.parts.len] = .{ .int = @intCast(ch.version) };
        parts[f.parts.len + 1] = if (ch.ms) |ms| .{ .timestamp = ms * 1000 } else .null;
        f.parts = parts;
        f.change_type = ch.kind;
    }
    const out = try arena.create(Layout);
    out.* = l;
    out.schema = .{ .fields = fields };
    out.part_fields = part_fields;
    out.change_type = n;
    out.files = base.files;

    const r = try arena.create(table.Resolved);
    r.* = .{
        .kind = .delta,
        .root = root,
        .label = try msgfmt.allocPrint(arena, "changes of versions {d} to {d}, {d} file{s}", .{ from, to, base.files.len, if (base.files.len == 1) "" else "s" }),
        .layout = out,
    };
    return r;
}

/// A commit's adds and removes with each file both removed and added again (its
/// deletion vector changed) taken as the rows that change made: the ones the new
/// vector deletes as deletes, the ones it no longer deletes as inserts.
fn paired(arena: std.mem.Allocator, plain: []const Change) ![]Change {
    var out = std.ArrayList(Change).empty;
    const used = try arena.alloc(bool, plain.len);
    @memset(used, false);
    for (plain, 0..) |r, i| {
        if (used[i] or !std.mem.eql(u8, r.kind.?, "delete")) continue;
        const j = for (plain, 0..) |a, k| {
            if (!used[k] and std.mem.eql(u8, a.kind.?, "insert") and std.mem.eql(u8, a.add.path, r.add.path)) break k;
        } else continue;
        used[i] = true;
        used[j] = true;
        var file = plain[j].add;
        file.dv = null;
        file.stats = null;
        const pr = Pair{ .old = r.add.dv, .new = plain[j].add.dv, .take = .newly_deleted };
        try out.append(arena, .{ .add = file, .kind = "delete", .version = r.version, .ms = null, .pair = pr });
        var back = pr;
        back.take = .restored;
        try out.append(arena, .{ .add = file, .kind = "insert", .version = r.version, .ms = null, .pair = back });
    }
    for (plain, used) |c, u| if (!u) try out.append(arena, c);
    return out.items;
}

/// Whether a table configuration enables the change data feed.
fn fed(config: ?std.json.Value) bool {
    const c = config orelse return false;
    if (c != .object) return false;
    const t = delta.str(c.object.get("delta.enableChangeDataFeed")) orelse return false;
    return std.ascii.eqlIgnoreCase(std.mem.trim(u8, t, " "), "true");
}

/// The oldest version committed at or after `t_us`, by commit times, searched by
/// halves; a time after the newest commit is refused.
fn firstAtOrAfter(arena: std.mem.Allocator, log: *const delta.Log, newest: u64, t_us: i64) !u64 {
    const t = @divFloor(t_us, 1000);
    var lo = log.oldest(newest);
    var hi = newest;
    const last = try log.commitMs(arena, hi);
    if (t > last) return refuse("{s} has no commit at or after that time: its newest, version {d}, is from {s}", .{ log.name, hi, try delta.msText(arena, last) });
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (try log.commitMs(arena, mid) >= t) hi = mid else lo = mid + 1;
    }
    return lo;
}

/// Commit `v`'s changes appended to `out`: its change data files when it wrote
/// any, else its data-changing adds and removes.
fn commitChanges(arena: std.mem.Allocator, log: *const delta.Log, name: []const u8, v: u64, out: *std.ArrayList(Change)) !void {
    const url = log.commits.get(v) orelse
        return refuse("{s} is missing commit {d} from _delta_log/, so its changes cannot be read (the log keeps commits for `delta.logRetentionDuration`)", .{ name, v });
    const text = try table.readWhole(arena, url);
    var cdc = std.ArrayList(Change).empty;
    var plain = std.ArrayList(Change).empty;
    var ms: ?i64 = null;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const l = std.mem.trim(u8, line, " \r\t");
        if (l.len == 0) continue;
        const j = std.json.parseFromSliceLeaky(std.json.Value, arena, l, .{}) catch
            return refuse("{s} has a commit in _delta_log/ holding a line that is not JSON", .{name});
        if (j != .object) continue;
        var it = j.object.iterator();
        while (it.next()) |kv| {
            const body = kv.value_ptr.*;
            if (body != .object) continue;
            const o = body.object;
            const k = kv.key_ptr.*;
            if (std.mem.eql(u8, k, "commitInfo")) {
                for ([_][]const u8{ "inCommitTimestamp", "timestamp" }) |key| if (ms == null) if (delta.int(o.get(key))) |x| {
                    ms = x;
                };
                continue;
            }
            if (std.mem.eql(u8, k, "metaData")) {
                if (!fed(o.get("configuration")))
                    return refuse("{s} stops recording its changes at version {d}: its `delta.enableChangeDataFeed` is not `true` from there", .{ name, v });
                continue;
            }
            const is_cdc = std.mem.eql(u8, k, "cdc");
            const is_add = std.mem.eql(u8, k, "add");
            if (!is_cdc and !is_add and !std.mem.eql(u8, k, "remove")) continue;
            if (!is_cdc) if (o.get("dataChange")) |d| if (d == .bool and !d.bool) continue;
            const raw = delta.str(o.get("path")) orelse continue;
            const add = delta.Add{
                .path = try table.percentDecode(arena, raw),
                .raw = raw,
                .size = if (delta.int(o.get("size"))) |s| (if (s >= 0) @as(u64, @intCast(s)) else null) else null,
                .parts = o.get("partitionValues"),
                .dv = if (is_cdc) null else if (o.get("deletionVector")) |d| (if (d == .object) d else null) else null,
                .stats = if (is_add) delta.str(o.get("stats")) else null,
            };
            const ch = Change{ .add = add, .kind = if (is_cdc) null else if (is_add) "insert" else "delete", .version = v, .ms = null };
            try (if (is_cdc) &cdc else &plain).append(arena, ch);
        }
    }
    const got = if (cdc.items.len > 0) cdc.items else try paired(arena, plain.items);
    for (got) |*ch| ch.ms = ms;
    try out.appendSlice(arena, got);
}
