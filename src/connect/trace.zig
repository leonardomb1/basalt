//! What the layers below a run's `Env` did, for `EXPLAIN ANALYZE`: a listing, a table
//! log's replay and the files it read, a Parquet footer, a modification time, a
//! download, a login. These layers hold no `Env`, and threading one through every
//! store and format reader would serve only a report, so a span reports to the tracer
//! of its thread. A profile installs one while it collects, and the lanes inherit it
//! (`parallel.spawnJoin`); a run without a profile has none, and a span then costs a
//! thread-local read. A span opened inside another is its child.

const std = @import("std");
const scheme = @import("../store/scheme.zig");

pub const Kind = enum { list, replay, log_read, footer, stat, download, login };

pub const Tracer = struct {
    ptr: *anyopaque,
    begin: *const fn (*anyopaque, Kind, []const u8, ?*anyopaque) ?*anyopaque,
    end: *const fn (*anyopaque, *anyopaque, u64, u64) void,
};

pub threadlocal var current: ?Tracer = null;
threadlocal var parent: ?*anyopaque = null;

pub const Span = struct {
    tracer: ?Tracer = null,
    token: ?*anyopaque = null,
    outer: ?*anyopaque = null,
    t0: ?std.time.Instant = null,
    items: u64 = 1,

    pub fn finish(self: *const Span) void {
        const t = self.tracer orelse return;
        const token = self.token orelse return;
        parent = self.outer;
        const ns = if (self.t0) |s| (std.time.Instant.now() catch s).since(s) else 0;
        t.end(t.ptr, token, ns, self.items);
    }
};

/// Starts a span over `what`, a path or URL, shown without its credentials or query
/// values.
pub fn begin(kind: Kind, what: []const u8) Span {
    const t = current orelse return .{};
    var buf: [2048]u8 = undefined;
    const token = t.begin(t.ptr, kind, scheme.shown(&buf, what), parent) orelse return .{};
    const outer = parent;
    parent = token;
    return .{ .tracer = t, .token = token, .outer = outer, .t0 = std.time.Instant.now() catch null };
}

/// Reports work already done, before this thread had a tracer.
pub fn record(kind: Kind, what: []const u8, ns: u64, items: u64) void {
    const t = current orelse return;
    var buf: [2048]u8 = undefined;
    const token = t.begin(t.ptr, kind, scheme.shown(&buf, what), parent) orelse return;
    t.end(t.ptr, token, ns, items);
}
