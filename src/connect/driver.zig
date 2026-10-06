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

var g_stop = std.atomic.Value(bool).init(false);

/// Every source reports end of stream at its next pull, so the run finishes as if
/// the data ran out. Set by a row cap only once the sink holds more rows than it
/// keeps, so blocking operators have already read all they needed.
pub fn requestStop() void {
    g_stop.store(true, .seq_cst);
}
pub fn stopping() bool {
    return g_stop.load(.seq_cst);
}
pub fn resetStop() void {
    g_stop.store(false, .seq_cst);
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

pub const RowCap = struct {
    max: u64,
    kept: u64 = 0,
    truncated: bool = false,
};

test "stop flag: a source reports end of stream while it is set, and only then" {
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
    const src = Source{ .ptr = &dummy, .vtable = &Endless.vt };
    defer resetStop();
    try std.testing.expect((try src.next(std.testing.allocator)) != null);
    requestStop();
    try std.testing.expect((try src.next(std.testing.allocator)) == null);
    resetStop();
    try std.testing.expect((try src.next(std.testing.allocator)) != null);
}

test "abort flag: requestAbort sets, resetAbort clears" {
    defer resetAbort();
    requestAbort();
    try std.testing.expect(aborting());
    resetAbort();
    try std.testing.expect(!aborting());
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

/// The socket-level transient set for connector retry layers. Run-level exit codes
/// do not use it: WriteFailed/ReadFailed/EndOfStream are ambient std.Io errors there
/// (a disk-full CSV write is not transient), so connectors rename them at the site.
pub fn transientNet(e: anyerror) bool {
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

pub const FileMode = enum { truncate, append };

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

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
        if (stopping()) return null;
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

pub fn sourceVTable(comptime T: type) Source.VTable {
    return .{
        .schema = struct {
            fn f(p: *anyopaque) types.Schema {
                return @as(*T, @ptrCast(@alignCast(p))).schema();
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

pub fn sinkVTable(comptime T: type) Sink.VTable {
    return .{
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
