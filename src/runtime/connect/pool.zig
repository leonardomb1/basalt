//! A run's idle SQL connections, for the queries a plan sends one after another:
//! a table's schema probe, its catalog facts and row statistics, a split's key and
//! MIN/MAX, a target's unique keys, and the read itself. Each used to dial its own
//! connection, whose login cost more than its query: a plain SELECT logged in three
//! times before its first row, a join of two tables five times.
//!
//! A connection goes back to the pool only once its cursor read its reply to the end
//! with no transaction open (`sql.Cursor.detach`), as soon as it did, not when the
//! cursor closes. One whose reply was cut short, whose query failed, that ran a
//! statement (it may have changed the session) or that sits in a transaction is
//! closed instead: an idle pooled connection holds no lock, so the run's own writes
//! never wait on it. The pool never waits for a connection either, dialing one when
//! none is idle, and keeps at most `max_idle` per server and login.
//!
//! Failures, with the shared backoff (`net/retry.zig`): a dial that fails on the
//! network is tried again; a query is a read whose rows have not reached the caller,
//! so one is sent again on a pooled connection found dead (at once, its idle
//! siblings dropped as likely gone too), on a network failure, or when the server
//! says to run it again (a deadlock victim, a serialization failure, a lock wait
//! that timed out). A reply cut off mid-stream is the caller's failure, as before.
//!
//! A connection is matched by every setting it was dialed with, so a connection
//! whose options a loop renders differently never meets the wrong server or login;
//! the pool keeps its own copy of those settings, as a FOR EACH row's arena is gone
//! by the next row. The pool closes what it holds when the run ends.

const std = @import("std");
const DbConfig = @import("../env.zig").DbConfig;
const SqlKind = @import("../env.zig").SqlKind;
const connectSql = @import("../connect.zig").connectSql;
const dupeSchema = @import("../connect.zig").dupeSchema;
const netconn = @import("../../net/conn.zig");
const retry = @import("../../net/retry.zig");
const sql = @import("../../db/sql.zig");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;

/// The idle connections kept for one server and login; one more is closed on return.
pub const max_idle = 4;

pub const Pool = struct {
    gpa: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    idle: std.ArrayListUnmanaged(Idle) = .empty,

    const Idle = struct { kind: SqlKind, cfg: DbConfig, conn: sql.Conn };

    pub fn init(gpa: std.mem.Allocator) Pool {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Pool) void {
        for (self.idle.items) |i| {
            i.conn.close();
            freeConfig(self.gpa, i.cfg);
        }
        self.idle.deinit(self.gpa);
    }

    /// A connection to `cfg`'s server, idle from an earlier query or dialed now (a
    /// dial that fails on the network is tried again, `retry.zig`), lent until it
    /// closes, or until the cursor of a query sent on it closes.
    pub fn acquire(self: *Pool, kind: SqlKind, cfg: DbConfig) !sql.Conn {
        const lease = try self.gpa.create(Lease);
        errdefer self.gpa.destroy(lease);
        const owned = try ownedConfig(self.gpa, cfg);
        errdefer freeConfig(self.gpa, owned);
        lease.* = .{ .pool = self, .kind = kind, .cfg = owned, .inner = null, .reused = false };
        if (self.take(kind, cfg)) |c| {
            lease.inner = c;
            lease.reused = true;
        } else lease.inner = try dial(self.gpa, kind, owned);
        return .{ .ptr = lease, .vtable = &Lease.vtable };
    }

    fn take(self: *Pool, kind: SqlKind, cfg: DbConfig) ?sql.Conn {
        self.mutex.lock();
        defer self.mutex.unlock();
        var i = self.idle.items.len;
        while (i > 0) {
            i -= 1;
            const it = self.idle.items[i];
            if (it.kind != kind or !sameConfig(it.cfg, cfg)) continue;
            const got = self.idle.swapRemove(i);
            freeConfig(self.gpa, got.cfg);
            return got.conn;
        }
        return null;
    }

    /// Takes `conn` and `cfg`, the pool's own copy, back; closes them past `max_idle`.
    fn put(self: *Pool, kind: SqlKind, cfg: DbConfig, conn: sql.Conn) void {
        const kept = blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            var same: usize = 0;
            for (self.idle.items) |it| {
                if (it.kind == kind and sameConfig(it.cfg, cfg)) same += 1;
            }
            if (same >= max_idle) break :blk false;
            self.idle.append(self.gpa, .{ .kind = kind, .cfg = cfg, .conn = conn }) catch break :blk false;
            break :blk true;
        };
        if (kept) return;
        conn.close();
        freeConfig(self.gpa, cfg);
    }

    /// Closes every idle connection to `cfg`'s server: one of them was found dead, and
    /// those left idle as long are as likely gone.
    fn drop(self: *Pool, kind: SqlKind, cfg: DbConfig) void {
        var gone: std.ArrayListUnmanaged(Idle) = .empty;
        defer gone.deinit(self.gpa);
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            var i = self.idle.items.len;
            while (i > 0) {
                i -= 1;
                const it = self.idle.items[i];
                if (it.kind != kind or !sameConfig(it.cfg, cfg)) continue;
                gone.append(self.gpa, self.idle.swapRemove(i)) catch {
                    self.idle.append(self.gpa, it) catch {};
                    break;
                };
            }
        }
        for (gone.items) |it| {
            it.conn.close();
            freeConfig(self.gpa, it.cfg);
        }
    }
};

/// `connectSql`, a dial that fails on the network (refused, reset, timed out) tried
/// again with backoff; a refused login is not.
fn dial(gpa: std.mem.Allocator, kind: SqlKind, cfg: DbConfig) !sql.Conn {
    const Try = struct {
        gpa: std.mem.Allocator,
        kind: SqlKind,
        cfg: DbConfig,
        out: ?sql.Conn = null,

        fn attempt(t: *@This()) !retry.Verdict {
            t.out = try connectSql(t.gpa, t.kind, t.cfg);
            return .done;
        }
    };
    var prng = retry.jitterPrng();
    var t = Try{ .gpa = gpa, .kind = kind, .cfg = cfg };
    try retry.retry(.{ .rand = prng.random(), .base_ms = 250 }, &t, Try.attempt);
    return t.out.?;
}

fn ownedConfig(gpa: std.mem.Allocator, cfg: DbConfig) !DbConfig {
    var out = cfg;
    inline for (std.meta.fields(DbConfig)) |f| {
        if (f.type == []const u8) @field(out, f.name) = "";
    }
    errdefer freeConfig(gpa, out);
    inline for (std.meta.fields(DbConfig)) |f| {
        if (f.type == []const u8) @field(out, f.name) = try gpa.dupe(u8, @field(cfg, f.name));
    }
    return out;
}

fn freeConfig(gpa: std.mem.Allocator, cfg: DbConfig) void {
    inline for (std.meta.fields(DbConfig)) |f| {
        if (f.type == []const u8) gpa.free(@field(cfg, f.name));
    }
}

fn sameConfig(a: DbConfig, b: DbConfig) bool {
    inline for (std.meta.fields(DbConfig)) |f| {
        const x = @field(a, f.name);
        const y = @field(b, f.name);
        if (f.type == []const u8) {
            if (!std.mem.eql(u8, x, y)) return false;
        } else if (!std.meta.eql(x, y)) return false;
    }
    return true;
}

/// A pooled connection on loan: `inner` is null once it was closed. `poolable`
/// turns false once a statement ran on it (it may have changed the session) or a
/// query failed; it is then closed rather than kept.
const Lease = struct {
    pool: *Pool,
    kind: SqlKind,
    cfg: DbConfig,
    inner: ?sql.Conn,
    reused: bool,
    poolable: bool = true,
    /// The server's words on the last query that failed, kept past the connection
    /// it failed on being closed for another try.
    failed_msg: []const u8 = "",
    failed_rerun: bool = false,

    const vtable = sql.Conn.VTable{ .queryCursor = query, .exec = exec, .close = close, .last_error = lastError, .rerun = rerun };

    /// A query is a read, sent again until it opens (`retry.zig`): on a pooled
    /// connection found dead at once, its idle siblings dropped; on a network failure
    /// or a server that says to run it again (a deadlock victim) after a backoff.
    /// Nothing of it reached the caller yet, so sending it again repeats nothing.
    fn query(ptr: *anyopaque, q: []const u8) anyerror!sql.Cursor {
        const self: *Lease = @ptrCast(@alignCast(ptr));
        const Try = struct {
            lease: *Lease,
            q: []const u8,
            out: ?sql.Cursor = null,

            fn attempt(t: *@This()) !retry.Verdict {
                const l = t.lease;
                if (l.inner == null) {
                    l.inner = try connectSql(l.pool.gpa, l.kind, l.cfg);
                    l.reused = false;
                }
                const c = l.inner.?;
                t.out = c.queryCursor(t.q) catch |e| {
                    l.noteFailure(c);
                    const net = netconn.transientNet(e);
                    if (!l.failed_rerun and !net) return .{ .failed = .{ .code = 0, .err = e, .retriable = false } };
                    const was_reused = l.reused;
                    c.close();
                    l.inner = null;
                    if (net and was_reused) {
                        l.pool.drop(l.kind, l.cfg);
                        return .again;
                    }
                    if (net) return e;
                    return .{ .failed = .{ .code = 0, .err = e, .retriable = true } };
                };
                return .done;
            }
        };
        var prng = retry.jitterPrng();
        var t = Try{ .lease = self, .q = q };
        retry.retry(.{ .rand = prng.random(), .base_ms = 250 }, &t, Try.attempt) catch |e| {
            self.poolable = false;
            return sql.rerunError(self.failed_rerun, e);
        };
        const cur = t.out.?;
        const lc = LeaseCursor.create(self, cur) catch |e| {
            cur.close();
            self.inner = null;
            return e;
        };
        return .{ .ptr = lc, .vtable = &LeaseCursor.vtable };
    }

    fn noteFailure(self: *Lease, c: sql.Conn) void {
        const gpa = self.pool.gpa;
        const msg = gpa.dupe(u8, c.lastError()) catch "";
        if (self.failed_msg.len > 0) gpa.free(self.failed_msg);
        self.failed_msg = msg;
        self.failed_rerun = c.rerunnable();
    }

    fn exec(ptr: *anyopaque, q: []const u8) anyerror!void {
        const self: *Lease = @ptrCast(@alignCast(ptr));
        self.poolable = false;
        if (self.inner == null) self.inner = try dial(self.pool.gpa, self.kind, self.cfg);
        try self.inner.?.exec(q);
    }

    fn close(ptr: *anyopaque) void {
        const self: *Lease = @ptrCast(@alignCast(ptr));
        self.end();
    }

    /// Back to the pool when it is idle and sound, else closed; the loan ends.
    fn end(self: *Lease) void {
        const gpa = self.pool.gpa;
        if (self.failed_msg.len > 0) gpa.free(self.failed_msg);
        if (self.inner != null and self.poolable) {
            self.pool.put(self.kind, self.cfg, self.inner.?);
        } else {
            if (self.inner) |c| c.close();
            freeConfig(gpa, self.cfg);
        }
        gpa.destroy(self);
    }

    fn lastError(ptr: *anyopaque) []const u8 {
        const self: *Lease = @ptrCast(@alignCast(ptr));
        if (self.inner) |c| if (c.lastError().len > 0) return c.lastError();
        return self.failed_msg;
    }

    fn rerun(ptr: *anyopaque) bool {
        const self: *Lease = @ptrCast(@alignCast(ptr));
        if (self.inner) |c| return c.rerunnable();
        return self.failed_rerun;
    }
};

/// A query's cursor on a lent connection, which owns the loan as a driver's cursor
/// owns its connection. The loan ends as soon as the reply is read to the end, not
/// when the cursor closes: a statement's sources stay open until the run ends, and a
/// connection left in one was idle yet out of reach. The batches it hands out name
/// its own copy of the schema, as the driver's goes with the connection.
const LeaseCursor = struct {
    lease: ?*Lease,
    inner: sql.Cursor,
    arena: std.heap.ArenaAllocator,
    schema_: *types.Schema,

    const vtable = sql.Cursor.VTable{ .schema = schema, .nextBatch = nextBatch, .close = close, .last_error = lastError };

    fn create(lease: *Lease, inner: sql.Cursor) !*LeaseCursor {
        const gpa = lease.pool.gpa;
        const self = try gpa.create(LeaseCursor);
        errdefer gpa.destroy(self);
        self.* = .{ .lease = lease, .inner = inner, .arena = std.heap.ArenaAllocator.init(gpa), .schema_ = undefined };
        errdefer self.arena.deinit();
        const a = self.arena.allocator();
        self.schema_ = try a.create(types.Schema);
        self.schema_.* = try dupeSchema(a, inner.schema());
        return self;
    }

    fn schema(ptr: *anyopaque) types.Schema {
        const self: *LeaseCursor = @ptrCast(@alignCast(ptr));
        return self.schema_.*;
    }

    fn nextBatch(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *LeaseCursor = @ptrCast(@alignCast(ptr));
        const lease = self.lease orelse return null;
        var b = (self.inner.nextBatch(arena) catch |e| {
            lease.poolable = false;
            return e;
        }) orelse {
            self.finish();
            return null;
        };
        b.schema = self.schema_;
        return b;
    }

    /// The loan ends: the connection back to the pool when the reply was read to
    /// its end, else closed.
    fn finish(self: *LeaseCursor) void {
        const lease = self.lease orelse return;
        self.lease = null;
        if (!self.inner.detach()) lease.inner = null;
        lease.end();
    }

    fn close(ptr: *anyopaque) void {
        const self: *LeaseCursor = @ptrCast(@alignCast(ptr));
        self.finish();
        const gpa = self.arena.child_allocator;
        self.arena.deinit();
        gpa.destroy(self);
    }

    fn lastError(ptr: *anyopaque) []const u8 {
        const self: *LeaseCursor = @ptrCast(@alignCast(ptr));
        if (self.lease == null) return "";
        return self.inner.lastError();
    }
};
