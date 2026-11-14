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
//! atomic because parallel lanes open readers of their own. `RangedFile` is the seam
//! an expression reads a file by byte range through, its opener installed by the
//! run. Dialing a server and explaining a failed login live in `net/conn.zig`.

const std = @import("std");
const types = @import("../lang/types.zig");
const Batch = @import("batch.zig").Batch;

var g_abort = std.atomic.Value(bool).init(false);

/// One atomic store: async-signal-safe, callable from a signal handler.
/// Whether a projection `want` reads column `name`: it names the column, or a
/// path into it (`s.city` for struct column `s`), which a reader that cannot
/// read part of a column reads the whole column for.
pub fn wantsColumn(want: []const []const u8, name: []const u8) bool {
    for (want) |w| if (std.mem.startsWith(u8, w, name) and (w.len == name.len or w[name.len] == '.')) return true;
    return false;
}

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

/// A file read by byte range wherever it is, as `read_bytes` and `file_size` read
/// one. What reaches a remote file (HTTP, object stores, SFTP, SMB) sits above
/// `exec`, so the run installs the opener (`installRangedOpener`) before any
/// expression reads a file; without one, opening fails with `NoRangedOpener`.
pub const RangedFile = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        size: *const fn (*anyopaque) u64,
        range: *const fn (*anyopaque, std.mem.Allocator, u64, usize) anyerror![]const u8,
        close: *const fn (*anyopaque) void,
    };

    pub fn size(self: RangedFile) u64 {
        return self.vtable.size(self.ptr);
    }

    pub fn range(self: RangedFile, arena: std.mem.Allocator, off: u64, len: usize) anyerror![]const u8 {
        return self.vtable.range(self.ptr, arena, off, len);
    }

    pub fn close(self: RangedFile) void {
        self.vtable.close(self.ptr);
    }
};

pub const OpenRanged = *const fn (arena: std.mem.Allocator, path: []const u8) anyerror!RangedFile;

var ranged_opener: std.atomic.Value(?OpenRanged) = .init(null);

pub fn installRangedOpener(open: OpenRanged) void {
    ranged_opener.store(open, .release);
}

pub fn openRanged(arena: std.mem.Allocator, path: []const u8) anyerror!RangedFile {
    const open = ranged_opener.load(.acquire) orelse return error.NoRangedOpener;
    return open(arena, path);
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

    /// Whether the sink encodes an ordered unit on the lane that read it (`openUnit`).
    pub fn encodesUnits(self: Sink) bool {
        return self.vtable.openUnit != null;
    }

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
