//! Committing to a Delta table's log. A commit is one JSON file,
//! `_delta_log/<20-digit version>.json`, and it becomes the table's next version
//! only if no other writer made that file first: locally a hidden temporary file
//! is synced and hard-linked to the name, which fails when the name exists; in an
//! object store a single PUT carries `If-None-Match: *`. A store that ignores the
//! condition would let two writers overwrite each other's commit, so after a
//! remote commit lands the same create is sent again and must be refused; one
//! that is not fails the write, saying the version it did commit, as a store
//! basalt cannot commit to safely. Data files are
//! written before the commit, so a reader never meets a commit naming a file that
//! is not there yet.
//!
//! When the version is taken, the winner's actions decide, as Delta's conflict
//! rules do: a winner that changed the table's metadata or protocol, created it,
//! or removed a file this commit read fails the commit (paths compared as the
//! reader keys them, resolved against the root); a REPLACE fails on any winner
//! that added or removed files; an UPSERT fails on a winner that added a file
//! which, by its partition values and statistics, may hold a key the UPSERT
//! writes, as the row would then be there twice. Otherwise the commit moves to the
//! next version, a bounded number of times. Every commit carries a fresh `txnId` in its
//! `commitInfo`, so a create whose answer was lost and whose retry found the name
//! taken is recognised as its own. A failed commit names the data files it wrote,
//! which no version references.

const std = @import("std");
const delta = @import("delta.zig");
const eval = @import("../../exec/eval.zig");
const objstore = @import("../../store/objstore.zig");
const http_client = @import("../../net/http_client.zig");
const table = @import("table.zig");
const testing = std.testing;

pub const Error = error{ DeltaCommitConflict, DeltaStoreUnsafe };

/// How many versions a blind append moves past before giving up.
pub const max_attempts = 10;

/// Where a table's commits are read and made; a test hands in its own.
pub const Store = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        read: *const fn (ptr: *anyopaque, arena: std.mem.Allocator, version: u64) anyerror!?[]const u8,
        create: *const fn (ptr: *anyopaque, arena: std.mem.Allocator, version: u64, bytes: []const u8) anyerror!bool,
    };

    /// The commit at `version`, null when there is none.
    pub fn read(self: Store, arena: std.mem.Allocator, version: u64) !?[]const u8 {
        return self.vtable.read(self.ptr, arena, version);
    }

    /// Makes the commit at `version` only if none is there: true when it did.
    pub fn create(self: Store, arena: std.mem.Allocator, version: u64, bytes: []const u8) !bool {
        return self.vtable.create(self.ptr, arena, version, bytes);
    }
};

/// What a commit does, which decides what a concurrent winner may have done.
pub const Kind = enum { create, append, replace, upsert };

/// Whether a file a concurrent commit added (its `add` action) may hold a key an
/// UPSERT writes.
pub const Hold = struct {
    ctx: *const anyopaque,
    may: *const fn (ctx: *const anyopaque, arena: std.mem.Allocator, add: std.json.ObjectMap) anyerror!bool,
};

pub const Commit = struct {
    kind: Kind,
    /// The version the writer read; null when it creates the table.
    read_version: ?u64,
    txn_id: []const u8,
    /// The whole commit, `commitInfo` (holding `txn_id`) first.
    body: []const u8,
    /// The data files the commit adds, named when it fails.
    added: []const []const u8,
    /// The live files the commit read, as the log names them: a winner removing
    /// one of them changed what the commit was computed from.
    read_set: []const []const u8 = &.{},
    /// The root paths are resolved against to compare them (`delta.State.key_root`);
    /// empty compares them decoded, as written.
    key_root: []const u8 = "",
    /// An UPSERT's test of a winner's added file.
    hold: ?Hold = null,
};

/// The log at `root` (a folder path ending in `/`), local or in an object store.
pub const TableStore = struct {
    root: []const u8,

    pub fn store(self: *TableStore) Store {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = Store.VTable{ .read = vtRead, .create = vtCreate };

    fn path(self: *TableStore, arena: std.mem.Allocator, version: u64) ![]const u8 {
        return std.fmt.allocPrint(arena, "{s}_delta_log/{d:0>20}.json", .{ self.root, version });
    }

    fn vtRead(p: *anyopaque, arena: std.mem.Allocator, version: u64) anyerror!?[]const u8 {
        const self: *TableStore = @ptrCast(@alignCast(p));
        return table.readWhole(arena, try self.path(arena, version)) catch |e| if (table.absent(e)) null else e;
    }

    fn vtCreate(p: *anyopaque, arena: std.mem.Allocator, version: u64, bytes: []const u8) anyerror!bool {
        const self: *TableStore = @ptrCast(@alignCast(p));
        const at = try self.path(arena, version);
        if (objstore.isUrl(at)) return createObject(arena, at, bytes);
        return createLocal(arena, self.root, at, version, bytes);
    }
};

/// A hidden file in `_delta_log/` written and synced whole, then hard-linked to
/// the commit's name: the link fails when the name exists, and a reader sees the
/// name only once every byte is there.
fn createLocal(arena: std.mem.Allocator, root: []const u8, at: []const u8, version: u64, bytes: []const u8) !bool {
    const dir_path = try std.mem.concat(arena, u8, &.{ root, "_delta_log" });
    try std.fs.cwd().makePath(dir_path);
    var dir = try std.fs.cwd().openDir(dir_path, .{});
    defer dir.close();
    const tmp = try std.fmt.allocPrint(arena, ".{d:0>20}.json.{s}.tmp", .{ version, &uuid() });
    {
        const f = try dir.createFile(tmp, .{ .exclusive = true });
        defer f.close();
        try f.writeAll(bytes);
        try f.sync();
    }
    defer dir.deleteFile(tmp) catch {};
    std.posix.linkat(dir.fd, tmp, dir.fd, std.fs.path.basename(at), 0) catch |e| switch (e) {
        error.PathAlreadyExists => return false,
        else => return e,
    };
    return true;
}

/// One conditional PUT, then the same create again, which a store honouring the
/// condition refuses; a second success means the condition was ignored.
fn createObject(arena: std.mem.Allocator, at: []const u8, bytes: []const u8) !bool {
    var client = http_client.initClient(arena);
    defer client.deinit();
    const obj = try objstore.parse(arena, at);
    var why: []const u8 = "";
    const made = obj.putIfAbsent(arena, &client, bytes, "application/json", &why) catch |e| return explainStore(e, at, why);
    if (!made) return false;
    if (obj.putIfAbsent(arena, &client, bytes, "application/json", &why) catch |e| return explainStore(e, at, why))
        return eval.explain(Error.DeltaStoreUnsafe, try std.fmt.allocPrint(arena, "`{s}` was committed, but its store ignored `If-None-Match: *` (a second create of it succeeded), so two writers could overwrite each other's commit there; do not write this table from basalt on this store", .{at}));
    return true;
}

fn explainStore(e: anyerror, at: []const u8, why: []const u8) anyerror {
    var buf: [480]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "could not create commit `{s}`: {s}{s}{s}", .{ at, @errorName(e), if (why.len > 0) ": " else "", why }) catch return e;
    return eval.explain(e, msg);
}

/// A random (version 4) UUID, as text.
pub fn uuid() [36]u8 {
    var b: [16]u8 = undefined;
    std.crypto.random.bytes(&b);
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    const hex = "0123456789abcdef";
    var out: [36]u8 = undefined;
    var o: usize = 0;
    for (b, 0..) |x, i| {
        if (i == 4 or i == 6 or i == 8 or i == 10) {
            out[o] = '-';
            o += 1;
        }
        out[o] = hex[x >> 4];
        out[o + 1] = hex[x & 15];
        o += 2;
    }
    return out;
}

/// What a winning commit did, as far as the conflict rules ask.
const Winner = struct {
    meta: bool = false,
    protocol: bool = false,
    adds: std.ArrayListUnmanaged(std.json.ObjectMap) = .empty,
    removed: std.ArrayListUnmanaged([]const u8) = .empty,
    txn: ?[]const u8 = null,
};

fn readWinner(arena: std.mem.Allocator, text: []const u8) !Winner {
    var w = Winner{};
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        const t = std.mem.trim(u8, line, " \r\t");
        if (t.len == 0) continue;
        const v = std.json.parseFromSliceLeaky(std.json.Value, arena, t, .{}) catch continue;
        if (v != .object) continue;
        var it = v.object.iterator();
        while (it.next()) |kv| {
            const k = kv.key_ptr.*;
            const body = kv.value_ptr.*;
            if (std.mem.eql(u8, k, "metaData")) w.meta = true;
            if (std.mem.eql(u8, k, "protocol")) w.protocol = true;
            if (body != .object) continue;
            if (std.mem.eql(u8, k, "add")) try w.adds.append(arena, body.object);
            if (std.mem.eql(u8, k, "remove")) if (body.object.get("path")) |p| if (p == .string)
                try w.removed.append(arena, try table.percentDecode(arena, p.string));
            if (std.mem.eql(u8, k, "commitInfo")) if (body.object.get("txnId")) |x| if (x == .string) {
                w.txn = x.string;
            };
        }
    }
    return w;
}

/// Why the winner at `version` fails `c`, or null when `c` may move past it.
fn conflict(arena: std.mem.Allocator, c: Commit, w: Winner, version: u64) !?[]const u8 {
    if (c.kind == .create or version == 0)
        return try std.fmt.allocPrint(arena, "another writer created the table first (version {d})", .{version});
    if (w.meta or w.protocol)
        return try std.fmt.allocPrint(arena, "version {d}, committed concurrently, changed the table's {s}", .{ version, if (w.meta) "metadata (schema, partitioning or properties)" else "protocol" });
    if (w.removed.items.len > 0 and c.read_set.len > 0) {
        var mine: std.StringHashMapUnmanaged(void) = .empty;
        for (c.read_set) |f| try mine.put(arena, try keyOf(arena, c.key_root, try table.percentDecode(arena, f)), {});
        for (w.removed.items) |r| if (mine.contains(try keyOf(arena, c.key_root, r)))
            return try std.fmt.allocPrint(arena, "version {d}, committed concurrently, removed `{s}`, which this write read", .{ version, r });
    }
    if (c.kind == .replace and (w.adds.items.len > 0 or w.removed.items.len > 0))
        return try std.fmt.allocPrint(arena, "version {d}, committed concurrently, changed the table's files after this REPLACE read them", .{version});
    if (c.hold) |h| for (w.adds.items) |add| if (try h.may(h.ctx, arena, add)) {
        const p = if (add.get("path")) |x| (if (x == .string) try table.percentDecode(arena, x.string) else "?") else "?";
        return try std.fmt.allocPrint(arena, "version {d}, committed concurrently, added `{s}`, which may hold a key this UPSERT writes, so the row could end up in the table twice", .{ version, p });
    };
    return null;
}

fn keyOf(arena: std.mem.Allocator, key_root: []const u8, decoded: []const u8) ![]const u8 {
    if (key_root.len == 0) return decoded;
    return delta.fileKey(arena, key_root, decoded);
}

/// Commits `c`, returning the version it became.
pub fn commit(arena: std.mem.Allocator, store: Store, name: []const u8, c: Commit) !u64 {
    var version: u64 = if (c.read_version) |r| r + 1 else 0;
    var attempts: usize = 0;
    while (true) : (attempts += 1) {
        if (attempts >= max_attempts)
            return fail(arena, name, c, try std.fmt.allocPrint(arena, "it lost the race for {d} versions in a row (the last tried was {d})", .{ max_attempts, version }));
        const made = store.create(arena, version, c.body) catch |e| {
            if (e != Error.DeltaStoreUnsafe and try ours(arena, store, version, c.txn_id)) {
                _ = eval.takeFailure(e);
                return version;
            }
            return e;
        };
        if (made) return version;
        const text = (try store.read(arena, version)) orelse continue;
        const w = try readWinner(arena, text);
        if (w.txn) |t| if (std.mem.eql(u8, t, c.txn_id)) return version;
        if (try conflict(arena, c, w, version)) |why| return fail(arena, name, c, why);
        version += 1;
    }
}

/// Whether the commit at `version` is the one carrying `txn_id`: a create whose
/// answer was lost may still have landed.
fn ours(arena: std.mem.Allocator, store: Store, version: u64, txn_id: []const u8) !bool {
    const text = (store.read(arena, version) catch return false) orelse return false;
    const w = readWinner(arena, text) catch return false;
    return if (w.txn) |t| std.mem.eql(u8, t, txn_id) else false;
}

fn fail(arena: std.mem.Allocator, name: []const u8, c: Commit, why: []const u8) anyerror {
    var msg = std.array_list.Managed(u8).init(arena);
    try msg.writer().print("could not commit to {s}: {s}; nothing was committed", .{ name, why });
    if (c.added.len > 0) {
        try msg.appendSlice(", and the data files it wrote are referenced by no version: ");
        for (c.added[0..@min(c.added.len, 5)], 0..) |f, i| try msg.writer().print("{s}`{s}`", .{ if (i == 0) "" else ", ", f });
        if (c.added.len > 5) try msg.writer().print(" and {d} more", .{c.added.len - 5});
    }
    return eval.explain(Error.DeltaCommitConflict, msg.items);
}

/// A log held in memory, with a hook that lets a winner commit first.
const FakeStore = struct {
    commits: std.AutoHashMapUnmanaged(u64, []const u8) = .empty,
    arena: std.mem.Allocator,
    /// Committed at the version a create is about to take, before it does.
    race: []const []const u8 = &.{},
    raced: usize = 0,
    /// Whether a create lands but reports a failure, as a lost answer does.
    lose_answer: bool = false,
    /// Whether a create lands and then reports the store ignored its condition.
    unsafe: bool = false,

    fn store(self: *FakeStore) Store {
        return .{ .ptr = self, .vtable = &.{ .read = read, .create = create } };
    }

    fn read(p: *anyopaque, _: std.mem.Allocator, version: u64) anyerror!?[]const u8 {
        const self: *FakeStore = @ptrCast(@alignCast(p));
        return self.commits.get(version);
    }

    fn create(p: *anyopaque, _: std.mem.Allocator, version: u64, bytes: []const u8) anyerror!bool {
        const self: *FakeStore = @ptrCast(@alignCast(p));
        if (self.raced < self.race.len and !self.commits.contains(version)) {
            try self.commits.put(self.arena, version, self.race[self.raced]);
            self.raced += 1;
        }
        if (self.commits.contains(version)) return false;
        try self.commits.put(self.arena, version, bytes);
        if (self.lose_answer) {
            self.lose_answer = false;
            return error.ConnectionResetByPeer;
        }
        if (self.unsafe) return Error.DeltaStoreUnsafe;
        return true;
    }
};

fn testCommit(kind: Kind, read_version: ?u64, txn: []const u8) Commit {
    return .{
        .kind = kind,
        .read_version = read_version,
        .txn_id = txn,
        .body = "{\"commitInfo\":{\"txnId\":\"T\"}}\n{\"add\":{\"path\":\"new.parquet\"}}\n",
        .added = &.{"new.parquet"},
        .read_set = &.{ "a.parquet", "x=1/b%20c.parquet" },
    };
}

test "commit: an append moves past winners that only added files; REPLACE and a changed schema fail" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const appended = "{\"commitInfo\":{\"txnId\":\"W\"}}\n{\"add\":{\"path\":\"w.parquet\"}}\n";

    var s1 = FakeStore{ .arena = a, .race = &.{ appended, appended } };
    try testing.expectEqual(@as(u64, 6), try commit(a, s1.store(), "T", testCommit(.append, 3, "T")));

    var s2 = FakeStore{ .arena = a, .race = &.{appended} };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s2.store(), "T", testCommit(.replace, 3, "T")));
    const msg = eval.takeFailure(Error.DeltaCommitConflict).?;
    try testing.expect(std.mem.indexOf(u8, msg, "changed the table's files after this REPLACE read them") != null);
    try testing.expect(std.mem.indexOf(u8, msg, "`new.parquet`") != null);

    var s3 = FakeStore{ .arena = a, .race = &.{"{\"metaData\":{\"id\":\"x\"}}\n"} };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s3.store(), "T", testCommit(.append, 3, "T")));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.DeltaCommitConflict).?, "changed the table's metadata") != null);

    var s4 = FakeStore{ .arena = a, .race = &.{"{\"remove\":{\"path\":\"x%3D1/b%20c.parquet\"}}\n"} };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s4.store(), "T", testCommit(.append, 3, "T")));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.DeltaCommitConflict).?, "removed `x=1/b c.parquet`") != null);

    var s5 = FakeStore{ .arena = a, .race = &.{appended} };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s5.store(), "T", testCommit(.create, null, "T")));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.DeltaCommitConflict).?, "created the table first") != null);
}

test "commit: a lost answer is recognised by txnId; endless winners are bounded" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var s1 = FakeStore{ .arena = a, .lose_answer = true };
    try testing.expectEqual(@as(u64, 0), try commit(a, s1.store(), "T", testCommit(.create, null, "T")));
    var s0 = FakeStore{ .arena = a, .unsafe = true };
    try testing.expectError(Error.DeltaStoreUnsafe, commit(a, s0.store(), "T", testCommit(.create, null, "T")));

    const many = [_][]const u8{"{\"add\":{\"path\":\"w.parquet\"}}"} ** (max_attempts + 1);
    var s2 = FakeStore{ .arena = a, .race = &many };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s2.store(), "T", testCommit(.append, 0, "T")));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.DeltaCommitConflict).?, "lost the race") != null);
}

test "commit: an UPSERT fails on a winner adding a file that may hold its keys, or removing one it read by any path form" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const Hot = struct {
        fn may(_: *const anyopaque, _: std.mem.Allocator, add: std.json.ObjectMap) anyerror!bool {
            const p = add.get("path") orelse return true;
            return std.mem.startsWith(u8, p.string, "hot");
        }
    };
    var c = testCommit(.upsert, 3, "T");
    c.key_root = "/t/";
    c.hold = .{ .ctx = &c, .may = Hot.may };

    var s1 = FakeStore{ .arena = a, .race = &.{"{\"commitInfo\":{\"txnId\":\"W\"}}\n{\"add\":{\"path\":\"cold.parquet\"}}\n"} };
    try testing.expectEqual(@as(u64, 5), try commit(a, s1.store(), "T", c));

    var s2 = FakeStore{ .arena = a, .race = &.{"{\"add\":{\"path\":\"hot%20x.parquet\"}}\n"} };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s2.store(), "T", c));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.DeltaCommitConflict).?, "added `hot x.parquet`, which may hold a key this UPSERT writes") != null);

    var s3 = FakeStore{ .arena = a, .race = &.{"{\"remove\":{\"path\":\"file:///t/x%3D1/b%20c.parquet\"}}\n"} };
    try testing.expectError(Error.DeltaCommitConflict, commit(a, s3.store(), "T", c));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.DeltaCommitConflict).?, "which this write read") != null);

    var s4 = FakeStore{ .arena = a, .race = &.{"{\"remove\":{\"path\":\"other.parquet\"}}\n"} };
    try testing.expectEqual(@as(u64, 5), try commit(a, s4.store(), "T", c));
}

test "local commit: a hidden synced file linked to the name, refused once the name exists" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const root = try std.fmt.allocPrint(a, "{s}/", .{try tmp.dir.realpathAlloc(a, ".")});
    var ts = TableStore{ .root = root };
    const s = ts.store();
    try testing.expect(try s.read(a, 0) == null);
    try testing.expect(try s.create(a, 0, "one"));
    try testing.expect(!try s.create(a, 0, "two"));
    try testing.expectEqualStrings("one", (try s.read(a, 0)).?);
    var dir = try tmp.dir.openDir("_delta_log", .{ .iterate = true });
    defer dir.close();
    var it = dir.iterate();
    var n: usize = 0;
    while (try it.next()) |_| n += 1;
    try testing.expectEqual(@as(usize, 1), n);
}

test "local commit: an append read at version 0 moves past a winner already on disk" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const root = try std.fmt.allocPrint(a, "{s}/", .{try tmp.dir.realpathAlloc(a, ".")});
    var ts = TableStore{ .root = root };
    try testing.expect(try ts.store().create(a, 1, "{\"commitInfo\":{\"txnId\":\"W\"}}\n{\"add\":{\"path\":\"w.parquet\"}}\n"));
    try testing.expectEqual(@as(u64, 2), try commit(a, ts.store(), "T", testCommit(.append, 0, "T")));
    try testing.expectError(Error.DeltaCommitConflict, commit(a, ts.store(), "T", testCommit(.replace, 1, "T2")));
    _ = eval.takeFailure(Error.DeltaCommitConflict);
}

test "uuid: version 4, variant bits, lower-case hex" {
    const u = uuid();
    try testing.expectEqual(@as(u8, '4'), u[14]);
    try testing.expect(std.mem.indexOfScalar(u8, "89ab", u[19]) != null);
    for (u, 0..) |ch, i| {
        if (i == 8 or i == 13 or i == 18 or i == 23) try testing.expectEqual(@as(u8, '-'), ch) else try testing.expect(std.ascii.isHex(ch) and !std.ascii.isUpper(ch));
    }
}
