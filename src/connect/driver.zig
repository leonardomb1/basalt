//! The connector seam. Every source/sink is a "driver" implementing one of these
//! vtables (the std.mem.Allocator pattern: a type-erased pointer + a static
//! vtable). The engine's scan/sink operators talk only to `Source`/`Sink`, so
//! concrete drivers plug in without the core knowing what they are. Vtable fns use
//! `anyerror` to erase driver-specific error sets across the boundary;
//! `sourceVTable`/`sinkVTable` build the shims from a concrete type.
//!
//! Sink lifecycle: `close` flushes and commits once at the end of a successful run
//! and must release every resource even when the final flush fails. `abort` runs
//! instead when the run failed: it discards un-flushed data and never commits, so a
//! sink never pushes its tail buffer for a pipeline that errored; flushes already
//! done stay committed (exactly-once is owned by downstream dedup, per split.zig).
//!
//! A `shared` sink is written under one mutex, and holding it across `writeBatch`
//! would serialise the formatting, the expensive part. A sink that can split
//! rendering from writing sets `renderBatch`/`writeRendered` by hand so lanes
//! format concurrently and lock only for the append; one whose expensive part spans
//! many batches (a parquet row group) offers `openUnit`, whose `UnitEncoder` is fed
//! and sealed on a lane, then committed in unit order one at a time.
//!
//! `FileMode` is the disposition narrowed for file sinks: `.truncate` for a bare
//! `LOAD INTO` or `REPLACE`, `.append` only for formats that extend in place, so
//! `upsert` (a table concept) never reaches a writer. `ScanTally` counts what the
//! sources skipped (parquet row groups, undecoded columns, pushed filters) and is
//! atomic because parallel lanes open readers of their own.

const std = @import("std");
const types = @import("../lang/types.zig");
const Batch = @import("../exec/batch.zig").Batch;

var g_abort = std.atomic.Value(bool).init(false);

/// One atomic store: async-signal-safe, callable from a signal handler.
pub fn requestAbort() void {
    g_abort.store(true, .seq_cst);
}
pub fn aborting() bool {
    return g_abort.load(.seq_cst);
}
pub fn resetAbort() void {
    g_abort.store(false, .seq_cst);
}

pub const ScanTally = struct {
    row_groups: std.atomic.Value(u64) = .init(0),
    row_groups_skipped: std.atomic.Value(u64) = .init(0),
    columns_read: std.atomic.Value(u64) = .init(0),
    columns_total: std.atomic.Value(u64) = .init(0),
    sql_filters: std.atomic.Value(u64) = .init(0),

    pub fn add(v: *std.atomic.Value(u64), n: u64) void {
        _ = v.fetchAdd(n, .monotonic);
    }
};

/// A shown result's row cap. Once the sink holds more rows than it keeps, it sets
/// `stop`, the flag its own statement's sources carry, so they report end of
/// stream at their next pull and the statement finishes as if the data ran out;
/// blocking operators have by then read all they needed, and no other statement
/// sees it.
pub const RowCap = struct {
    max: u64,
    kept: u64 = 0,
    truncated: bool = false,
    stop: ?*std.atomic.Value(bool) = null,
};

test "stop flag: a source reports end of stream once its statement's flag is set; others read on" {
    const Endless = struct {
        fn schemaFn(_: *anyopaque) types.Schema {
            return .{ .fields = &.{} };
        }
        fn nextFn(_: *anyopaque, _: std.mem.Allocator) anyerror!?Batch {
            return Batch{ .schema = undefined, .columns = &.{}, .len = 1 };
        }
        fn closeFn(_: *anyopaque) void {}
        const vt = Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };
    };
    var dummy: u8 = 0;
    var stop = std.atomic.Value(bool).init(false);
    const src = Source{ .ptr = &dummy, .vtable = &Endless.vt, .stop = &stop };
    const other = Source{ .ptr = &dummy, .vtable = &Endless.vt };
    try std.testing.expect((try src.next(std.testing.allocator)) != null);
    stop.store(true, .release);
    try std.testing.expect((try src.next(std.testing.allocator)) == null);
    try std.testing.expect((try other.next(std.testing.allocator)) != null);
}

/// A connector's own words for its last failure on this thread, held in `n` bytes
/// and cut to fit. Each connector keeps one as a `threadlocal var`.
pub fn ThreadNote(comptime n: usize) type {
    return struct {
        buf: [n]u8 = undefined,
        len: usize = 0,

        pub fn get(self: *const @This()) []const u8 {
            return self.buf[0..self.len];
        }

        pub fn clear(self: *@This()) void {
            self.len = 0;
        }

        pub fn set(self: *@This(), text: []const u8) void {
            self.len = @min(text.len, n);
            @memcpy(self.buf[0..self.len], text[0..self.len]);
        }

        pub fn print(self: *@This(), comptime fmt: []const u8, args: anytype) void {
            self.len = (std.fmt.bufPrint(&self.buf, fmt, args) catch self.buf[0..]).len;
        }
    };
}

/// TCP_NODELAY, since drivers flush whole messages then wait (Nagle + delayed ACK
/// stalls each exchange); SO_KEEPALIVE so idle multi-hour loads behind NATs surface
/// a dead peer as an error, not a hang. Best-effort: never fails a connect.
pub fn tuneSocket(handle: std.posix.socket_t) void {
    const one: c_int = 1;
    std.posix.setsockopt(handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one)) catch {};
    std.posix.setsockopt(handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&one)) catch {};
    if (@import("builtin").os.tag == .linux) {
        const idle: c_int = 60;
        const intvl: c_int = 15;
        std.posix.setsockopt(handle, std.posix.IPPROTO.TCP, std.os.linux.TCP.KEEPIDLE, std.mem.asBytes(&idle)) catch {};
        std.posix.setsockopt(handle, std.posix.IPPROTO.TCP, std.os.linux.TCP.KEEPINTVL, std.mem.asBytes(&intvl)) catch {};
    }
}

/// A TCP connection to `host:port`, tuned by `tuneSocket`: the one way a connector
/// opens a socket.
pub fn dial(gpa: std.mem.Allocator, host: []const u8, port: u16) !std.net.Stream {
    const stream = try std.net.tcpConnectToHost(gpa, host, port);
    tuneSocket(stream.handle);
    return stream;
}

/// What kind of failure an error is, for the two retry levels. `net`: the socket
/// failed, transient everywhere. `wire`: an ambient std.Io error a connector raised
/// mid-exchange, transient only at the connector (a disk-full CSV write is not).
/// `lookup`: DNS does not know the host yet, transient for the run only. `busy`: a
/// server said to come back later, transient for the run (a connector's own loop
/// retries by status). `other`: never transient.
pub const Class = enum { net, wire, lookup, busy, other };

pub fn classify(e: anyerror) Class {
    return switch (e) {
        error.ConnectionRefused,
        error.ConnectionTimedOut,
        error.ConnectionResetByPeer,
        error.NetworkUnreachable,
        error.HostUnreachable,
        error.BrokenPipe,
        error.TemporaryNameServerFailure,
        error.ServerClosedConnection,
        error.ConnectionIoFailed,
        => .net,
        error.EndOfStream,
        error.ReadFailed,
        error.WriteFailed,
        error.UnexpectedConnectFailure,
        => .wire,
        error.NameServerFailure,
        error.HostLacksNetworkAddresses,
        error.UnknownHostName,
        => .lookup,
        error.HttpServerBusy,
        error.FtpServerBusy,
        error.HttpTransportFailed,
        error.S3Throttled,
        error.AzureThrottled,
        => .busy,
        else => .other,
    };
}

/// The socket-level transient set for connector retry layers.
pub fn transientNet(e: anyerror) bool {
    return switch (classify(e)) {
        .net, .wire => true,
        .lookup, .busy, .other => false,
    };
}

/// The run-level transient set, behind exit code 75: an unknown host counts, since
/// DNS may not know a host that is still coming up.
pub fn transientRun(e: anyerror) bool {
    return switch (classify(e)) {
        .net, .lookup, .busy => true,
        .wire, .other => false,
    };
}

/// The HTTP statuses a connector's retry loop retries: timeouts, throttling and
/// gateways. `http_client.statusError` still counts 501 and 505+ and not 408 (D12).
pub fn transientStatus(code: u16) bool {
    return switch (code) {
        408, 429, 500, 502, 503, 504 => true,
        else => false,
    };
}

/// A connector failure as `{layer}: {Err}: {note}`, the note being the server's own
/// words; the note part is left out when there is none, the layer part when `layer`
/// is empty.
pub fn connectorNote(w: *std.Io.Writer, layer: []const u8, e: anyerror, note: []const u8) std.Io.Writer.Error!void {
    if (layer.len > 0) try w.print("{s}: ", .{layer});
    try w.writeAll(@errorName(e));
    if (note.len > 0) try w.print(": {s}", .{note});
}

pub fn connectorNoteAlloc(arena: std.mem.Allocator, layer: []const u8, e: anyerror, note: []const u8) ![]const u8 {
    var out: std.Io.Writer.Allocating = .init(arena);
    connectorNote(&out.writer, layer, e, note) catch return error.OutOfMemory;
    return out.toOwnedSlice();
}

pub const FileMode = enum { truncate, append };

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    /// Its statement's row-cap flag (`RowCap.stop`), when the statement has one.
    stop: ?*const std.atomic.Value(bool) = null,

    pub const VTable = struct {
        schema: *const fn (*anyopaque) types.Schema,
        next: *const fn (*anyopaque, std.mem.Allocator) anyerror!?Batch,
        close: *const fn (*anyopaque) void,
    };

    pub fn schema(self: Source) types.Schema {
        return self.vtable.schema(self.ptr);
    }
    /// Checks the abort flag on every pull: a blocking operator drains its input inside
    /// one `next`, so a check in the sink loop alone let a cancelled GROUP BY run on.
    pub fn next(self: Source, arena: std.mem.Allocator) anyerror!?Batch {
        if (aborting()) return error.Aborted;
        if (self.stop) |s| if (s.load(.acquire)) return null;
        return self.vtable.next(self.ptr, arena);
    }
    pub fn close(self: Source) void {
        self.vtable.close(self.ptr);
    }
};

pub const Sink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        writeBatch: *const fn (*anyopaque, std.mem.Allocator, Batch) anyerror!void,
        close: *const fn (*anyopaque) anyerror!void,
        abort: *const fn (*anyopaque) void,
        renderBatch: ?*const fn (*anyopaque, std.mem.Allocator, Batch) anyerror![]const u8 = null,
        writeRendered: ?*const fn (*anyopaque, []const u8) anyerror!void = null,
        openUnit: ?*const fn (*anyopaque, std.mem.Allocator) anyerror!UnitEncoder = null,
    };

    pub fn openUnit(self: Sink, arena: std.mem.Allocator) ?anyerror!UnitEncoder {
        const f = self.vtable.openUnit orelse return null;
        return f(self.ptr, arena);
    }

    pub fn writeBatch(self: Sink, arena: std.mem.Allocator, b: Batch) anyerror!void {
        return self.vtable.writeBatch(self.ptr, arena, b);
    }

    pub fn renderBatch(self: Sink, arena: std.mem.Allocator, b: Batch) ?anyerror![]const u8 {
        const f = self.vtable.renderBatch orelse return null;
        return f(self.ptr, arena, b);
    }

    pub fn writeRendered(self: Sink, bytes: []const u8) anyerror!void {
        return self.vtable.writeRendered.?(self.ptr, bytes);
    }

    pub fn canRender(self: Sink) bool {
        return self.vtable.renderBatch != null and self.vtable.writeRendered != null;
    }
    pub fn close(self: Sink) anyerror!void {
        return self.vtable.close(self.ptr);
    }
    pub fn abort(self: Sink) void {
        self.vtable.abort(self.ptr);
    }
};

pub const UnitEncoder = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write: *const fn (*anyopaque, std.mem.Allocator, Batch) anyerror!void,
        seal: *const fn (*anyopaque) anyerror!void,
        commit: *const fn (*anyopaque) anyerror!void,
        discard: *const fn (*anyopaque) void,
    };

    pub fn write(self: UnitEncoder, arena: std.mem.Allocator, b: Batch) anyerror!void {
        return self.vtable.write(self.ptr, arena, b);
    }
    pub fn seal(self: UnitEncoder) anyerror!void {
        return self.vtable.seal(self.ptr);
    }
    pub fn commit(self: UnitEncoder) anyerror!void {
        return self.vtable.commit(self.ptr);
    }
    pub fn discard(self: UnitEncoder) void {
        self.vtable.discard(self.ptr);
    }
};

/// `T.schema` may be a method or a field (a schema or a pointer to one).
pub fn sourceVTable(comptime T: type) Source.VTable {
    return .{
        .schema = struct {
            fn f(p: *anyopaque) types.Schema {
                const self: *T = @ptrCast(@alignCast(p));
                if (!@hasField(T, "schema")) return self.schema();
                return if (@TypeOf(self.schema) == types.Schema) self.schema else self.schema.*;
            }
        }.f,
        .next = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
                return @as(*T, @ptrCast(@alignCast(p))).next(arena);
            }
        }.f,
        .close = struct {
            fn f(p: *anyopaque) void {
                @as(*T, @ptrCast(@alignCast(p))).close();
            }
        }.f,
    };
}

/// The optional entries are filled when `T` declares `renderBatch` (with
/// `writeRendered`) or `openUnit`.
pub fn sinkVTable(comptime T: type) Sink.VTable {
    return .{
        .renderBatch = if (@hasDecl(T, "renderBatch")) struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror![]const u8 {
                return @as(*T, @ptrCast(@alignCast(p))).renderBatch(arena, b);
            }
        }.f else null,
        .writeRendered = if (@hasDecl(T, "writeRendered")) struct {
            fn f(p: *anyopaque, bytes: []const u8) anyerror!void {
                return @as(*T, @ptrCast(@alignCast(p))).writeRendered(bytes);
            }
        }.f else null,
        .openUnit = if (@hasDecl(T, "openUnit")) struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator) anyerror!UnitEncoder {
                return @as(*T, @ptrCast(@alignCast(p))).openUnit(arena);
            }
        }.f else null,
        .writeBatch = struct {
            fn f(p: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
                return @as(*T, @ptrCast(@alignCast(p))).writeBatch(arena, b);
            }
        }.f,
        .close = struct {
            fn f(p: *anyopaque) anyerror!void {
                return @as(*T, @ptrCast(@alignCast(p))).close();
            }
        }.f,
        .abort = struct {
            fn f(p: *anyopaque) void {
                @as(*T, @ptrCast(@alignCast(p))).abort();
            }
        }.f,
    };
}

const testing = std.testing;

const error_corpus = [_]anyerror{
    error.ConnectionRefused,  error.ConnectionTimedOut,       error.ConnectionResetByPeer,      error.NetworkUnreachable,
    error.HostUnreachable,    error.BrokenPipe,               error.EndOfStream,                error.ReadFailed,
    error.WriteFailed,        error.UnexpectedConnectFailure, error.TemporaryNameServerFailure, error.ServerClosedConnection,
    error.ConnectionIoFailed, error.NameServerFailure,        error.HostLacksNetworkAddresses,  error.UnknownHostName,
    error.HttpServerBusy,     error.FtpServerBusy,            error.HttpTransportFailed,        error.S3Throttled,
    error.AzureThrottled,     error.HttpConnectionClosing,    error.WouldBlock,                 error.OutOfMemory,
    error.PlanFailed,         error.S3RequestFailed,          error.AzureRequestFailed,         error.HttpRequestFailed,
    error.LoginFailed,        error.Aborted,                  error.AccessDenied,               error.FileNotFound,
};

test "classify: transientNet keeps its socket set and transientRun matches the run's isTransient" {
    const Orig = struct {
        fn oldTransientNet(e: anyerror) bool {
            return switch (e) {
                error.ConnectionRefused,
                error.ConnectionTimedOut,
                error.ConnectionResetByPeer,
                error.NetworkUnreachable,
                error.HostUnreachable,
                error.BrokenPipe,
                error.EndOfStream,
                error.ReadFailed,
                error.WriteFailed,
                error.UnexpectedConnectFailure,
                error.TemporaryNameServerFailure,
                error.ServerClosedConnection,
                error.ConnectionIoFailed,
                => true,
                else => false,
            };
        }
        fn oldIsTransient(e: anyerror) bool {
            return switch (e) {
                error.ConnectionRefused,
                error.ConnectionTimedOut,
                error.ConnectionResetByPeer,
                error.NetworkUnreachable,
                error.HostUnreachable,
                error.BrokenPipe,
                error.TemporaryNameServerFailure,
                error.NameServerFailure,
                error.HostLacksNetworkAddresses,
                error.UnknownHostName,
                error.HttpServerBusy,
                error.FtpServerBusy,
                error.HttpTransportFailed,
                error.ServerClosedConnection,
                error.ConnectionIoFailed,
                error.S3Throttled,
                error.AzureThrottled,
                => true,
                else => false,
            };
        }
    };
    const env = @import("../runtime/env.zig");
    for (error_corpus) |e| {
        try testing.expectEqual(Orig.oldTransientNet(e), transientNet(e));
        try testing.expectEqual(Orig.oldIsTransient(e), transientRun(e));
        try testing.expectEqual(env.isTransient(e), transientRun(e));
    }
    try testing.expectEqual(Class.wire, classify(error.EndOfStream));
    try testing.expectEqual(Class.lookup, classify(error.UnknownHostName));
    try testing.expectEqual(Class.busy, classify(error.S3Throttled));
    try testing.expectEqual(Class.other, classify(error.HttpConnectionClosing));
}

test "transientStatus is the object stores' set; HTTP() differs on 408, 501 and 505+ until D12" {
    const http_client = @import("../net/http_client.zig");
    const objstore = @import("../store/objstore.zig");
    var differ: std.ArrayList(u16) = .empty;
    defer differ.deinit(testing.allocator);
    var code: u16 = 100;
    while (code < 600) : (code += 1) {
        try testing.expectEqual(objstore.retriable(code), transientStatus(code));
        const busy = http_client.statusError(code) == error.HttpServerBusy;
        if (busy != transientStatus(code)) try differ.append(testing.allocator, code);
    }
    try testing.expectEqual(@as(usize, 2 + 95), differ.items.len);
    try testing.expectEqual(@as(u16, 408), differ.items[0]);
    try testing.expectEqual(@as(u16, 501), differ.items[1]);
    for (differ.items[2..], 505..) |c, want| try testing.expectEqual(@as(u16, @intCast(want)), c);
}

test "connectorNote: layer, error and the server's words, each part left out when empty" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("sftp: ConnectionResetByPeer: kex failed", try connectorNoteAlloc(a, "sftp", error.ConnectionResetByPeer, "kex failed"));
    try testing.expectEqualStrings("smb: SmbFailed", try connectorNoteAlloc(a, "smb", error.SmbFailed, ""));
    try testing.expectEqualStrings("LoginFailed: Login failed for user 'x'.", try connectorNoteAlloc(a, "", error.LoginFailed, "Login failed for user 'x'."));
    try testing.expectEqualStrings("LoginFailed", try connectorNoteAlloc(a, "", error.LoginFailed, ""));
    var buf: [12]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try testing.expectError(error.WriteFailed, connectorNote(&w, "sftp", error.EndOfStream, "x"));
}

test "dial connects and tunes the socket: no Nagle, keepalive on" {
    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try addr.listen(.{});
    defer server.deinit();
    const stream = try dial(testing.allocator, "127.0.0.1", server.listen_address.getPort());
    defer stream.close();
    var one: c_int = 0;
    try std.posix.getsockopt(stream.handle, std.posix.IPPROTO.TCP, std.posix.TCP.NODELAY, std.mem.asBytes(&one));
    try testing.expect(one != 0);
    one = 0;
    try std.posix.getsockopt(stream.handle, std.posix.SOL.SOCKET, std.posix.SO.KEEPALIVE, std.mem.asBytes(&one));
    try testing.expect(one != 0);
}
