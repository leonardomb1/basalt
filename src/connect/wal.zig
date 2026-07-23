//! Durable request buffer — the WAL under `ACCEPT ... INTO BUFFER` and the
//! `FROM BUFFER` source (migration.md §10, §13.1 phase 1).
//!
//! Layout inside the buffer directory, for a buffer named `eventos`:
//!   eventos-000001.jsonl    append-only JSONL segments, rotated by size
//!   eventos.state           manifest: seq of the last fully-LOADED segment
//!
//! Contracts:
//!   - `append` writes one JSONL line; `sync` fsyncs the current segment —
//!     the serve layer acks 200 only after `sync` (group commit: many
//!     appends, one sync).
//!   - Segment names are deterministic; the stream-load LABEL for a segment
//!     is its file stem (`eventos-000042`). A crash between "loaded" and
//!     "marked" replays the same label, and StarRocks dedups — effectively
//!     exactly-once with no two-phase commit.
//!   - `markLoaded` advances the manifest via write-to-temp + rename (atomic
//!     on POSIX), so a torn write can't corrupt consumption state.
//!   - Threading: every mutating/positional op takes the internal mutex, so
//!     the accept loop (append/sync) and the flusher (rotateIfNonEmpty,
//!     pendingSegments, markLoaded, purge) can share one Wal. Completed
//!     segments are immutable — `readSegment` needs no lock.

const std = @import("std");

pub const Wal = struct {
    gpa: std.mem.Allocator,
    dir: std.fs.Dir,
    name: []const u8, // owned
    segment_bytes: u64,
    seq: u64, // current (open) segment sequence, 1-based
    cur: ?std.fs.File = null, // created lazily on first append
    cur_size: u64 = 0,
    mu: std.Thread.Mutex = .{},

    /// Open (or resume) the buffer `name` under `dir_path`. If the newest
    /// existing segment is not yet loaded, appending resumes into it — its
    /// label replays on the next flush, which the sink dedups.
    pub fn open(gpa: std.mem.Allocator, dir_path: []const u8, name: []const u8, segment_bytes: u64) !Wal {
        std.fs.cwd().makePath(dir_path) catch |e| switch (e) {
            error.PathAlreadyExists => {},
            else => return e,
        };
        var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
        errdefer dir.close();

        var self = Wal{
            .gpa = gpa,
            .dir = dir,
            .name = try gpa.dupe(u8, name),
            .segment_bytes = segment_bytes,
            .seq = 1,
        };
        errdefer gpa.free(self.name);

        const newest = try self.maxSegmentSeq();
        const loaded = self.loadedUpTo();
        if (newest) |n| {
            if (n > loaded) {
                // resume the unfinished segment
                self.seq = n;
                var fname_buf: [256]u8 = undefined;
                const fname = segmentFileName(&fname_buf, name, n);
                const f = try dir.openFile(fname, .{ .mode = .write_only });
                try f.seekFromEnd(0);
                self.cur = f;
                self.cur_size = (try f.stat()).size;
            } else {
                self.seq = n + 1;
            }
        }
        return self;
    }

    pub fn close(self: *Wal) void {
        if (self.cur) |f| f.close();
        self.dir.close();
        self.gpa.free(self.name);
        self.* = undefined;
    }

    /// The sequence of the segment currently being written. A flusher may
    /// safely read any segment with a smaller sequence.
    pub fn currentSeq(self: *Wal) u64 {
        self.mu.lock();
        defer self.mu.unlock();
        return self.seq;
    }

    /// Append one JSONL line (a `\n` is added). Rotates first when the line
    /// would push the current segment past `segment_bytes`. NOT fsynced —
    /// call `sync` before acking (group commit: N appends, one sync).
    pub fn append(self: *Wal, line: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.cur != null and self.cur_size > 0 and
            self.cur_size + line.len + 1 > self.segment_bytes)
        {
            try self.rotateLocked();
        }
        if (self.cur == null) {
            var fname_buf: [256]u8 = undefined;
            const fname = segmentFileName(&fname_buf, self.name, self.seq);
            self.cur = try self.dir.createFile(fname, .{ .truncate = false });
            try self.cur.?.seekFromEnd(0);
        }
        var iov = [_]std.posix.iovec_const{
            .{ .base = line.ptr, .len = line.len },
            .{ .base = "\n", .len = 1 },
        };
        try self.cur.?.writevAll(&iov);
        self.cur_size += line.len + 1;
    }

    /// Fsync the current segment — the durability point (ack barrier).
    pub fn sync(self: *Wal) !void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.cur) |f| try f.sync();
    }

    /// Close the current segment and start the next one. The closed segment
    /// becomes visible to `pendingSegments`.
    pub fn rotate(self: *Wal) !void {
        self.mu.lock();
        defer self.mu.unlock();
        try self.rotateLocked();
    }

    /// Flusher-side rotation: close the current segment only if it has rows
    /// (so idle buffers don't mint empty segments).
    pub fn rotateIfNonEmpty(self: *Wal) !void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.cur_size > 0) try self.rotateLocked();
    }

    fn rotateLocked(self: *Wal) !void {
        if (self.cur) |f| {
            try f.sync();
            f.close();
            self.cur = null;
        }
        self.cur_size = 0;
        self.seq += 1;
    }

    /// Sequences of segments that are complete (rotated away) and not yet
    /// marked loaded, ascending — the flusher's work list. Caller frees.
    pub fn pendingSegments(self: *Wal, alloc: std.mem.Allocator) ![]u64 {
        self.mu.lock();
        const cur_seq = self.seq;
        self.mu.unlock();
        const loaded = self.loadedUpTo();
        var list = std.array_list.Managed(u64).init(alloc);
        errdefer list.deinit();
        var it = self.dir.iterate();
        while (try it.next()) |e| {
            if (e.kind != .file) continue;
            const s = self.parseSegmentSeq(e.name) orelse continue;
            if (s > loaded and s < cur_seq) try list.append(s);
        }
        const out = try list.toOwnedSlice();
        std.mem.sort(u64, out, {}, std.sort.asc(u64));
        return out;
    }

    /// The stream-load label / file stem for a segment: `<name>-NNNNNN`.
    pub fn labelFor(self: *const Wal, buf: []u8, s: u64) []const u8 {
        return std.fmt.bufPrint(buf, "{s}-{d:0>6}", .{ self.name, s }) catch unreachable;
    }

    /// The file name of a segment: `<name>-NNNNNN.jsonl`.
    pub fn fileFor(self: *const Wal, buf: []u8, s: u64) []const u8 {
        return segmentFileName(buf, self.name, s);
    }

    /// Read a complete segment's bytes (flusher side). Caller frees.
    pub fn readSegment(self: *Wal, alloc: std.mem.Allocator, s: u64, max_bytes: usize) ![]u8 {
        var fname_buf: [256]u8 = undefined;
        return self.dir.readFileAlloc(alloc, segmentFileName(&fname_buf, self.name, s), max_bytes);
    }

    /// Last fully-loaded segment per the manifest (0 = none). Reads a file
    /// that only changes via `markLoaded`'s atomic rename — no lock needed.
    pub fn loadedUpTo(self: *const Wal) u64 {
        var buf: [64]u8 = undefined;
        var fname_buf: [256]u8 = undefined;
        const fname = stateFileName(&fname_buf, self.name);
        const n = self.dir.readFile(fname, &buf) catch return 0;
        const trimmed = std.mem.trim(u8, n, " \t\r\n");
        return std.fmt.parseInt(u64, trimmed, 10) catch 0;
    }

    /// Advance the manifest to `s` (write temp + atomic rename). Loading is
    /// sequential, so `s` must be `loadedUpTo() + 1`-adjacent by convention;
    /// this is not enforced here.
    pub fn markLoaded(self: *Wal, s: u64) !void {
        var content_buf: [64]u8 = undefined;
        const content = try std.fmt.bufPrint(&content_buf, "{d}\n", .{s});
        var tmp_buf: [256]u8 = undefined;
        var fname_buf: [256]u8 = undefined;
        const tmp = try std.fmt.bufPrint(&tmp_buf, "{s}.state.tmp", .{self.name});
        const fname = stateFileName(&fname_buf, self.name);
        {
            const f = try self.dir.createFile(tmp, .{});
            defer f.close();
            try f.writeAll(content);
            try f.sync();
        }
        try self.dir.rename(tmp, fname);
    }

    /// Delete loaded segments (RETAIN UNTIL LOADED). Returns how many were
    /// removed. `RETAIN n HOURS` keeps them; a later purge pass ages them out.
    pub fn purgeLoaded(self: *Wal) !usize {
        const loaded = self.loadedUpTo();
        var removed: usize = 0;
        var names = std.array_list.Managed(u64).init(self.gpa);
        defer names.deinit();
        var it = self.dir.iterate();
        while (try it.next()) |e| {
            if (e.kind != .file) continue;
            const s = self.parseSegmentSeq(e.name) orelse continue;
            if (s <= loaded) try names.append(s);
        }
        for (names.items) |s| {
            var fname_buf: [256]u8 = undefined;
            self.dir.deleteFile(segmentFileName(&fname_buf, self.name, s)) catch continue;
            removed += 1;
        }
        return removed;
    }

    /// Delete LOADED segments older than `hours` (RETAIN n HOURS aging).
    /// Unloaded segments are never touched, no matter their age — retention
    /// bounds reprocessing, not durability. Returns how many were removed.
    pub fn purgeOlderThan(self: *Wal, hours: u32) !usize {
        self.mu.lock();
        defer self.mu.unlock();
        const loaded = self.loadedUpTo();
        const cutoff: i128 = @as(i128, std.time.nanoTimestamp()) -
            @as(i128, hours) * std.time.ns_per_hour;
        var removed: usize = 0;
        var names = std.array_list.Managed(u64).init(self.gpa);
        defer names.deinit();
        var it = self.dir.iterate();
        while (try it.next()) |e| {
            if (e.kind != .file) continue;
            const s = self.parseSegmentSeq(e.name) orelse continue;
            if (s > loaded) continue; // never purge unloaded data
            const st = self.dir.statFile(e.name) catch continue;
            if (st.mtime < cutoff) try names.append(s);
        }
        for (names.items) |s| {
            var fname_buf: [256]u8 = undefined;
            self.dir.deleteFile(segmentFileName(&fname_buf, self.name, s)) catch continue;
            removed += 1;
        }
        return removed;
    }

    /// Total bytes across this buffer's segments — the backpressure input
    /// (over the configured limit ⇒ serve answers 503 + Retry-After).
    pub fn bytesOnDisk(self: *Wal) u64 {
        var total: u64 = 0;
        var it = self.dir.iterate();
        while (it.next() catch null) |e| {
            if (e.kind != .file) continue;
            if (self.parseSegmentSeq(e.name) == null) continue;
            const st = self.dir.statFile(e.name) catch continue;
            total += st.size;
        }
        return total;
    }

    fn maxSegmentSeq(self: *Wal) !?u64 {
        var max: ?u64 = null;
        var it = self.dir.iterate();
        while (try it.next()) |e| {
            if (e.kind != .file) continue;
            const s = self.parseSegmentSeq(e.name) orelse continue;
            if (max == null or s > max.?) max = s;
        }
        return max;
    }

    fn parseSegmentSeq(self: *const Wal, fname: []const u8) ?u64 {
        return segSeqOf(self.name, fname);
    }
};

/// `<name>-NNNNNN.jsonl` -> NNNNNN, or null for anything else.
fn segSeqOf(name: []const u8, fname: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, fname, name)) return null;
    const rest = fname[name.len..];
    if (rest.len < 2 or rest[0] != '-') return null;
    if (!std.mem.endsWith(u8, rest, ".jsonl")) return null;
    const digits = rest[1 .. rest.len - ".jsonl".len];
    if (digits.len == 0) return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

// ---------------------------------------------------------------------------
// FROM BUFFER — the replay/drain source (§13.1 phase 4)
// ---------------------------------------------------------------------------

const types = @import("../lang/types.zig");
const driver = @import("driver.zig");
const request = @import("request.zig");
const batchmod = @import("../exec/batch.zig");

/// A `driver.Source` over a buffer directory: one batch per JSONL segment,
/// ascending. Batch mode reads EVERY segment on disk (retained ones included —
/// that is what "reprocess a RETAIN 24 HOURS buffer" means); the serve flusher
/// (phase 3) instead drains `pendingSegments` one at a time with the segment
/// label. Schema: the declared `ACCEPT BODY` columns, or inferred from the
/// first row when replaying without a declaration.
pub const BufferSource = struct {
    gpa: std.mem.Allocator,
    arena_inst: std.heap.ArenaAllocator,
    dir: std.fs.Dir,
    name: []const u8, // owned by arena
    schema: *types.Schema,
    segs: []u64,
    idx: usize = 0,

    /// `only` restricts the source to a single segment (the serve flusher
    /// drains one segment per run, labeled after it); null = every segment
    /// on disk (batch replay).
    pub fn open(
        gpa: std.mem.Allocator,
        dir_path: []const u8,
        name: []const u8,
        declared: ?[]const types.BodyCol,
        only: ?u64,
    ) !*BufferSource {
        const self = try gpa.create(BufferSource);
        errdefer gpa.destroy(self);
        self.* = .{
            .gpa = gpa,
            .arena_inst = std.heap.ArenaAllocator.init(gpa),
            .dir = undefined,
            .name = undefined,
            .schema = undefined,
            .segs = undefined,
        };
        errdefer self.arena_inst.deinit();
        const arena = self.arena_inst.allocator();
        self.name = try arena.dupe(u8, name);

        self.dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
        errdefer self.dir.close();

        var list = std.array_list.Managed(u64).init(arena);
        var it = self.dir.iterate();
        while (try it.next()) |e| {
            if (e.kind != .file) continue;
            const s = segSeqOf(name, e.name) orelse continue;
            if (only) |o| {
                if (s != o) continue;
            }
            try list.append(s);
        }
        self.segs = try list.toOwnedSlice();
        std.mem.sort(u64, self.segs, {}, std.sort.asc(u64));
        if (only != null and self.segs.len == 0) return error.SegmentNotFound;

        if (declared) |cols| {
            self.schema = try request.schemaFromBodyCols(arena, cols);
        } else {
            if (self.segs.len == 0) return error.BufferEmpty; // nothing to infer from
            const first = try self.readSeg(arena, self.segs[0]);
            const items = try parseLines(arena, first);
            self.schema = try request.inferSchema(arena, items);
        }
        return self;
    }

    pub fn source(self: *BufferSource) driver.Source {
        return .{ .ptr = self, .vtable = &buf_vtable };
    }

    fn readSeg(self: *BufferSource, alloc: std.mem.Allocator, s: u64) ![]u8 {
        var fname_buf: [256]u8 = undefined;
        return self.dir.readFileAlloc(alloc, segmentFileName(&fname_buf, self.name, s), 1 << 30);
    }
};

/// Parse JSONL text into one json.Value per non-empty line.
fn parseLines(arena: std.mem.Allocator, text: []const u8) ![]std.json.Value {
    var items = std.array_list.Managed(std.json.Value).init(arena);
    var it = std.mem.splitScalar(u8, text, '\n');
    while (it.next()) |line| {
        const trimmed = std.mem.trim(u8, line, " \t\r");
        if (trimmed.len == 0) continue;
        const v = std.json.parseFromSliceLeaky(std.json.Value, arena, trimmed, .{}) catch
            return error.BadBufferLine;
        try items.append(v);
    }
    return items.toOwnedSlice();
}

const buf_vtable = driver.Source.VTable{ .schema = bufSchema, .next = bufNext, .close = bufClose };

fn bufSchema(ptr: *anyopaque) types.Schema {
    const self: *BufferSource = @ptrCast(@alignCast(ptr));
    return self.schema.*;
}

fn bufNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?batchmod.Batch {
    const self: *BufferSource = @ptrCast(@alignCast(ptr));
    while (self.idx < self.segs.len) {
        const s = self.segs[self.idx];
        self.idx += 1;
        const text = try self.readSeg(arena, s);
        const items = try parseLines(arena, text);
        if (items.len == 0) continue; // empty segment: skip
        return try request.batchFromJson(arena, self.schema, items);
    }
    return null;
}

fn bufClose(ptr: *anyopaque) void {
    const self: *BufferSource = @ptrCast(@alignCast(ptr));
    self.dir.close();
    self.arena_inst.deinit();
    self.gpa.destroy(self);
}

fn segmentFileName(buf: []u8, name: []const u8, s: u64) []const u8 {
    return std.fmt.bufPrint(buf, "{s}-{d:0>6}.jsonl", .{ name, s }) catch unreachable;
}

fn stateFileName(buf: []u8, name: []const u8) []const u8 {
    return std.fmt.bufPrint(buf, "{s}.state", .{name}) catch unreachable;
}

// ---------------------------------------------------------------------------

const testing = std.testing;

test "wal: append, rotate by size, pending list, label" {
    const gpa = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(base);

    var w = try Wal.open(gpa, base, "eventos", 32); // tiny segments to force rotation
    defer w.close();

    try w.append("{\"a\":1}"); // 8 bytes
    try w.append("{\"a\":2}");
    try w.sync();
    try w.append("{\"a\":3,\"pad\":\"xxxxxxxxxx\"}"); // would cross 32 -> rotates first
    try w.sync();

    try testing.expectEqual(@as(u64, 2), w.currentSeq());
    const pending = try w.pendingSegments(gpa);
    defer gpa.free(pending);
    try testing.expectEqualSlices(u64, &.{1}, pending);

    var lbuf: [64]u8 = undefined;
    try testing.expectEqualStrings("eventos-000001", w.labelFor(&lbuf, 1));

    const seg = try w.readSegment(gpa, 1, 1 << 20);
    defer gpa.free(seg);
    try testing.expectEqualStrings("{\"a\":1}\n{\"a\":2}\n", seg);
}

test "wal: markLoaded is atomic-rename, purge removes loaded segments" {
    const gpa = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(base);

    var w = try Wal.open(gpa, base, "ev", 16);
    defer w.close();
    try w.append("0123456789"); // fills segment 1
    try w.append("0123456789"); // rotates to 2
    try w.append("0123456789"); // rotates to 3
    try w.rotate(); // close 3 too

    var pending = try w.pendingSegments(gpa);
    try testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, pending);
    gpa.free(pending);

    try w.markLoaded(1);
    try testing.expectEqual(@as(u64, 1), w.loadedUpTo());
    pending = try w.pendingSegments(gpa);
    try testing.expectEqualSlices(u64, &.{ 2, 3 }, pending);
    gpa.free(pending);

    // no stray temp file after the rename
    try testing.expectError(error.FileNotFound, w.dir.statFile("ev.state.tmp"));

    try w.markLoaded(3);
    const removed = try w.purgeLoaded();
    try testing.expectEqual(@as(usize, 3), removed);
    try testing.expectEqual(@as(u64, 0), w.bytesOnDisk());
}

test "wal: reopen resumes the unfinished segment; loaded tail starts a new one" {
    const gpa = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(base);

    {
        var w = try Wal.open(gpa, base, "ev", 1 << 20);
        defer w.close();
        try w.append("{\"n\":1}");
        try w.sync();
        // "crash": close without rotating — segment 1 is unfinished
    }
    {
        var w = try Wal.open(gpa, base, "ev", 1 << 20);
        defer w.close();
        try testing.expectEqual(@as(u64, 1), w.currentSeq()); // resumed, not skipped
        try w.append("{\"n\":2}");
        try w.sync();
        const seg_after = try w.readSegment(gpa, 1, 1 << 20);
        defer gpa.free(seg_after);
        try testing.expectEqualStrings("{\"n\":1}\n{\"n\":2}\n", seg_after);
        try w.rotate();
        try w.markLoaded(1);
    }
    {
        // everything loaded -> a fresh open starts the NEXT segment (never
        // append to a segment whose label was already consumed)
        var w = try Wal.open(gpa, base, "ev", 1 << 20);
        defer w.close();
        try testing.expectEqual(@as(u64, 2), w.currentSeq());
    }
}

test "wal: purgeOlderThan removes only LOADED segments past the window" {
    const gpa = testing.allocator;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(gpa, ".");
    defer gpa.free(base);

    var w = try Wal.open(gpa, base, "ev", 4);
    defer w.close();
    try w.append("11111111"); // seg 1 (over threshold -> next append rotates)
    try w.append("22222222"); // seg 2
    try w.rotate(); // close seg 2
    try w.markLoaded(1);

    // Age both segments two hours into the past.
    const past: i128 = std.time.nanoTimestamp() - 2 * std.time.ns_per_hour;
    for ([_][]const u8{ "ev-000001.jsonl", "ev-000002.jsonl" }) |fname| {
        const f = try w.dir.openFile(fname, .{ .mode = .read_write });
        defer f.close();
        try f.updateTimes(past, past);
    }

    // 1h window: seg 1 (loaded, old) purged; seg 2 (old but UNLOADED) kept.
    try testing.expectEqual(@as(usize, 1), try w.purgeOlderThan(1));
    try testing.expectError(error.FileNotFound, w.dir.statFile("ev-000001.jsonl"));
    _ = try w.dir.statFile("ev-000002.jsonl");

    // 24h window: nothing else in range.
    try testing.expectEqual(@as(usize, 0), try w.purgeOlderThan(24));
}
