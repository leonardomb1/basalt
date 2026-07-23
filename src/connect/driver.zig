//! The connector seam. Every source/sink is a "driver" implementing one of these
//! vtables (the std.mem.Allocator pattern: a type-erased pointer + a static
//! vtable). The engine's scan/sink operators talk only to `Source`/`Sink`, so
//! concrete drivers (CSV now; StarRocks, SQL Server later) plug in without the
//! core knowing what they are. Vtable fns use `anyerror` to erase driver-specific
//! error sets across the boundary.

const std = @import("std");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");

const Batch = batchmod.Batch;

// --- cooperative cancellation flag -----------------------------------------
// Lives here (the layer both runtime and connectors can import) so a driver in
// the middle of a paginated pull can notice an abort between requests instead
// of only at the engine's batch boundaries.
var g_abort = std.atomic.Value(bool).init(false);

/// Ask the current run to stop at the next boundary. One atomic store —
/// async-signal-safe, callable from a signal handler.
pub fn requestAbort() void {
    g_abort.store(true, .seq_cst);
}
pub fn aborting() bool {
    return g_abort.load(.seq_cst);
}
/// Tests only: re-arm after exercising abort paths.
pub fn resetAbort() void {
    g_abort.store(false, .seq_cst);
}

/// Tune a freshly connected DB/data-movement socket. TCP_NODELAY: the drivers
/// flush whole protocol messages (login exchanges, per-batch INSERTs, 4-32K
/// bulk packets) and then wait for the reply — Nagle + delayed ACK stalls each
/// exchange. SO_KEEPALIVE (with a 60s idle probe on Linux): multi-hour loads
/// sit idle through NATs/LBs during slow server-side phases; dead peers should
/// surface as an error, not a hang. Best-effort: tuning never fails a connect.
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

/// The socket-level transient set shared by the connector retry layers (sql
/// reconnect loops, http in-place backoff). Run-level exit-code classification
/// deliberately does NOT consume this set: names like WriteFailed/ReadFailed/
/// EndOfStream are ambient std.Io errors there (a disk-full CSV write is not
/// transient) — connectors map them to specific names at the failure site
/// (e.g. http's HttpTransportFailed) where the peer is known to be a network.
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
        => true,
        else => false,
    };
}

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// The output schema, known before any rows are read.
        schema: *const fn (*anyopaque) types.Schema,
        /// Next batch (allocated in `arena`), or null at end of stream.
        next: *const fn (*anyopaque, std.mem.Allocator) anyerror!?Batch,
        close: *const fn (*anyopaque) void,
    };

    pub fn schema(self: Source) types.Schema {
        return self.vtable.schema(self.ptr);
    }
    pub fn next(self: Source, arena: std.mem.Allocator) anyerror!?Batch {
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
        /// Flush and finalize (commit). Called once at end of a successful run.
        /// Must release every owned resource even when the final flush fails.
        close: *const fn (*anyopaque) anyerror!void,
        /// Failure-path teardown: discard any buffered, un-flushed data and
        /// release every owned resource WITHOUT committing. Called instead of
        /// `close` when the run failed, so a sink never pushes its tail buffer
        /// for a pipeline that errored. Best-effort: must not fail. Data from
        /// flushes that already happened stays committed (the exactly-once
        /// story is owned by downstream dedup, per split.zig).
        abort: *const fn (*anyopaque) void,
    };

    pub fn writeBatch(self: Sink, arena: std.mem.Allocator, b: Batch) anyerror!void {
        return self.vtable.writeBatch(self.ptr, arena, b);
    }
    pub fn close(self: Sink) anyerror!void {
        return self.vtable.close(self.ptr);
    }
    pub fn abort(self: Sink) void {
        self.vtable.abort(self.ptr);
    }
};
