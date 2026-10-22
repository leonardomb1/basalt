//! Files read as one table through one byte stream (`stream.zig`), for any reader of
//! a format that streams: the first file's rows fix the columns, every later file is
//! read into them, as a folder of CSVs shares the first one's header. Stdin is a list
//! of none after the stream.
//!
//! `R` opens with `R.open(arena, gpa, reader, opts)` on an `O` whose `schema`
//! field, when set, fixes the columns, and has `schema`, `next` and `close`; an `O`
//! with a `source_name` field is told which file it reads, for its messages.

const std = @import("std");
const types = @import("../lang/types.zig");
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("../connect/driver.zig");
const Stream = @import("stream.zig").Stream;

pub fn Files(comptime R: type, comptime O: type) type {
    return struct {
        arena: std.mem.Allocator,
        gpa: std.mem.Allocator,
        stream: *Stream,
        rest: []const []const u8,
        reader: ?*R,
        schema: types.Schema,
        opts: O,

        const Self = @This();

        pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, files: []const []const u8, opts: O) !*Self {
            const s = try Stream.open(arena, files[0]);
            errdefer s.close();
            var o = opts;
            if (@hasField(O, "source_name")) o.source_name = files[0];
            return on(arena, gpa, s, files[1..], o);
        }

        pub fn on(arena: std.mem.Allocator, gpa: std.mem.Allocator, s: *Stream, rest: []const []const u8, opts: O) !*Self {
            const self = try arena.create(Self);
            const r = try R.open(arena, gpa, s.reader, opts);
            self.* = .{ .arena = arena, .gpa = gpa, .stream = s, .rest = rest, .reader = r, .schema = r.schema, .opts = opts };
            self.opts.schema = r.schema;
            return self;
        }

        /// A later file that fails to open leaves no reader, so `close` frees each once.
        pub fn next(self: *Self, arena: std.mem.Allocator) !?Batch {
            while (true) {
                const r = self.reader orelse return null;
                if (try r.next(arena)) |b| return b;
                if (self.rest.len == 0) return null;
                r.close();
                self.reader = null;
                try self.stream.reopen(self.rest[0]);
                if (@hasField(O, "source_name")) self.opts.source_name = self.rest[0];
                self.rest = self.rest[1..];
                self.reader = try R.open(self.arena, self.gpa, self.stream.reader, self.opts);
            }
        }

        pub fn close(self: *Self) void {
            if (self.reader) |r| r.close();
            self.stream.close();
        }

        fn schemaFn(ptr: *anyopaque) types.Schema {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.schema;
        }

        fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.next(arena);
        }

        fn closeFn(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.close();
        }

        const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

        pub fn source(self: *Self) driver.Source {
            return .{ .ptr = self, .vtable = &vtable };
        }
    };
}
