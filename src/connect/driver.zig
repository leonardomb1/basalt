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
        close: *const fn (*anyopaque) anyerror!void,
    };

    pub fn writeBatch(self: Sink, arena: std.mem.Allocator, b: Batch) anyerror!void {
        return self.vtable.writeBatch(self.ptr, arena, b);
    }
    pub fn close(self: Sink) anyerror!void {
        return self.vtable.close(self.ptr);
    }
};
