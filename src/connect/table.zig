//! `write stdout` sink: renders batches as a left-aligned text table on stdout.
//! Rows are accumulated (stringified into the sink's own allocator, since batch
//! arenas are reset between calls) and the table is laid out on `close`, once all
//! column widths are known. Intended for the REPL and ad-hoc `pipeline run`.

const std = @import("std");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");

const Batch = batchmod.Batch;

pub const TableWriter = struct {
    gpa: std.mem.Allocator,
    names: []const []const u8,
    ncols: usize,
    cells: std.array_list.Managed([]const u8), // row-major, ncols per row
    nrows: usize = 0,

    pub fn open(gpa: std.mem.Allocator, schema: types.Schema) !*TableWriter {
        const self = try gpa.create(TableWriter);
        const names = try gpa.alloc([]const u8, schema.fields.len);
        for (schema.fields, 0..) |f, i| names[i] = try gpa.dupe(u8, f.name);
        self.* = .{
            .gpa = gpa,
            .names = names,
            .ncols = schema.fields.len,
            .cells = std.array_list.Managed([]const u8).init(gpa),
        };
        return self;
    }

    pub fn writeBatch(self: *TableWriter, arena: std.mem.Allocator, batch: Batch) !void {
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            for (batch.columns) |*col| {
                const v = col.getValue(r);
                const s = if (v.isNull()) "" else try eval.valueToString(arena, v);
                try self.cells.append(try self.gpa.dupe(u8, s));
            }
            self.nrows += 1;
        }
    }

    pub fn close(self: *TableWriter) !void {
        // Capture the allocator locally: `deinit()` below destroys `self`, so the
        // deferred free must not touch `self.gpa` afterwards.
        const gpa = self.gpa;
        const widths = try gpa.alloc(usize, self.ncols);
        defer gpa.free(widths);
        for (self.names, 0..) |n, i| widths[i] = n.len;
        for (0..self.nrows) |r| {
            for (0..self.ncols) |c| {
                const len = self.cells.items[r * self.ncols + c].len;
                if (len > widths[c]) widths[c] = len;
            }
        }

        var buf: [8192]u8 = undefined;
        var fw = std.fs.File.stdout().writer(&buf);
        const out = &fw.interface;

        for (self.names, 0..) |n, i| {
            if (i > 0) try out.writeAll("  ");
            try padded(out, n, widths[i]);
        }
        try out.writeByte('\n');
        for (0..self.ncols) |i| {
            if (i > 0) try out.writeAll("  ");
            try out.splatBytesAll("-", widths[i]);
        }
        try out.writeByte('\n');
        for (0..self.nrows) |r| {
            for (0..self.ncols) |c| {
                if (c > 0) try out.writeAll("  ");
                try padded(out, self.cells.items[r * self.ncols + c], widths[c]);
            }
            try out.writeByte('\n');
        }
        try out.print("({d} row{s})\n", .{ self.nrows, if (self.nrows == 1) "" else "s" });
        try out.flush();

        self.deinit();
    }

    fn deinit(self: *TableWriter) void {
        for (self.cells.items) |c| self.gpa.free(c);
        self.cells.deinit();
        for (self.names) |n| self.gpa.free(n);
        self.gpa.free(self.names);
        self.gpa.destroy(self);
    }

    pub fn sink(self: *TableWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose };

    fn vtWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
        const self: *TableWriter = @ptrCast(@alignCast(ptr));
        return self.writeBatch(arena, b);
    }
    fn vtClose(ptr: *anyopaque) anyerror!void {
        const self: *TableWriter = @ptrCast(@alignCast(ptr));
        return self.close();
    }
};

fn padded(out: *std.Io.Writer, s: []const u8, width: usize) !void {
    try out.writeAll(s);
    try out.splatBytesAll(" ", width - s.len);
}
