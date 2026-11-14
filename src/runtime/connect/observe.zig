//! What a request cost, for `EXPLAIN ANALYZE`: a source or a connection wrapped so
//! the time and rows of what it sends land on the request the profile recorded. Only
//! a profiled run wraps anything. A scan that claims its request keeps its own figures,
//! so these count for the requests no scan claimed.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Profile = @import("../../exec/profile.zig").Profile;
const Request = @import("../../exec/profile.zig").Request;
const driver = @import("../../exec/driver.zig");
const sql = @import("../../db/sql.zig");
const types = @import("../../lang/types.zig");

fn since(t0: ?std.time.Instant) u64 {
    const s = t0 orelse return 0;
    return (std.time.Instant.now() catch return 0).since(s);
}

pub const Clock = struct {
    t0: ?std.time.Instant,

    pub fn start() Clock {
        return .{ .t0 = std.time.Instant.now() catch null };
    }

    pub fn ns(self: Clock) u64 {
        return since(self.t0);
    }
};

const TimedSource = struct {
    inner: driver.Source,
    req: *Request,

    const vtable = driver.Source.VTable{ .schema = vtSchema, .next = vtNext, .close = vtClose };

    fn vtSchema(ptr: *anyopaque) types.Schema {
        const self: *TimedSource = @ptrCast(@alignCast(ptr));
        return self.inner.schema();
    }

    fn vtNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *TimedSource = @ptrCast(@alignCast(ptr));
        const clock = Clock.start();
        const b = try self.inner.next(arena);
        self.req.ns += clock.ns();
        self.req.calls += 1;
        if (b) |x| self.req.rows += x.len;
        return b;
    }

    /// Touches no request: a statement's sources close after its report is printed
    /// and its profile freed.
    fn vtClose(ptr: *anyopaque) void {
        const self: *TimedSource = @ptrCast(@alignCast(ptr));
        self.inner.close();
    }
};

/// `inner` with its pulls counted on `req`, which `open_ns` went to opening; `inner`
/// itself without a request.
pub fn source(arena: std.mem.Allocator, inner: driver.Source, req: ?*Request, open_ns: u64) !driver.Source {
    const r = req orelse return inner;
    r.ns += open_ns;
    const t = try arena.create(TimedSource);
    t.* = .{ .inner = inner, .req = r };
    return .{ .ptr = t, .vtable = &TimedSource.vtable, .stop = inner.stop };
}

const TimedConn = struct {
    inner: sql.Conn,
    prof: *Profile,
    label: []const u8,
    connect_ns: u64,

    const vtable = sql.Conn.VTable{ .queryCursor = vtQuery, .exec = vtExec, .close = vtClose, .last_error = vtLastError };

    fn vtQuery(ptr: *anyopaque, q: []const u8) anyerror!sql.Cursor {
        const self: *TimedConn = @ptrCast(@alignCast(ptr));
        const req = try self.prof.request(.lookup, self.label, q);
        req.ns = self.connect_ns;
        self.connect_ns = 0;
        const clock = Clock.start();
        const cur = try self.inner.queryCursor(q);
        req.ns += clock.ns();
        const t = self.prof.gpa.create(TimedCursor) catch |e| {
            cur.close();
            return e;
        };
        t.* = .{ .inner = cur, .req = req, .conn = self };
        return .{ .ptr = t, .vtable = &TimedCursor.vtable };
    }

    fn vtExec(ptr: *anyopaque, q: []const u8) anyerror!void {
        const self: *TimedConn = @ptrCast(@alignCast(ptr));
        return self.inner.exec(q);
    }

    fn vtClose(ptr: *anyopaque) void {
        const self: *TimedConn = @ptrCast(@alignCast(ptr));
        self.inner.close();
        self.prof.gpa.destroy(self);
    }

    fn vtLastError(ptr: *anyopaque) []const u8 {
        const self: *TimedConn = @ptrCast(@alignCast(ptr));
        return self.inner.lastError();
    }
};

const TimedCursor = struct {
    inner: sql.Cursor,
    req: *Request,
    conn: *TimedConn,

    const vtable = sql.Cursor.VTable{ .schema = vtSchema, .nextBatch = vtNext, .close = vtClose, .last_error = vtLastError };

    fn vtSchema(ptr: *anyopaque) types.Schema {
        const self: *TimedCursor = @ptrCast(@alignCast(ptr));
        return self.inner.schema();
    }

    fn vtNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *TimedCursor = @ptrCast(@alignCast(ptr));
        const clock = Clock.start();
        const b = try self.inner.nextBatch(arena);
        self.req.ns += clock.ns();
        self.req.calls += 1;
        if (b) |x| self.req.rows += x.len;
        return b;
    }

    /// A cursor consumes its connection, so closing it ends the wrapper of both.
    fn vtClose(ptr: *anyopaque) void {
        const self: *TimedCursor = @ptrCast(@alignCast(ptr));
        const gpa = self.conn.prof.gpa;
        self.inner.close();
        gpa.destroy(self.conn);
        gpa.destroy(self);
    }

    fn vtLastError(ptr: *anyopaque) []const u8 {
        const self: *TimedCursor = @ptrCast(@alignCast(ptr));
        return self.inner.lastError();
    }
};

/// `conn`, one query per connection, each recorded on `prof` as a lookup named
/// `label` as it goes out, `connect_ns` counted on the first. `label` must outlive it.
pub fn lookups(prof: *Profile, conn: sql.Conn, label: []const u8, connect_ns: u64) !sql.Conn {
    const t = prof.gpa.create(TimedConn) catch |e| {
        conn.close();
        return e;
    };
    t.* = .{ .inner = conn, .prof = prof, .label = label, .connect_ns = connect_ns };
    return .{ .ptr = t, .vtable = &TimedConn.vtable };
}
