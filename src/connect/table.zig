//! `write stdout` sink: renders batches as a left-aligned text table on stdout.
//! Rows are accumulated (stringified into the sink's own allocator, since batch
//! arenas are reset between calls) and the table is laid out on `close`, once all
//! column widths are known. Intended for the REPL and ad-hoc `pipeline run`.

const std = @import("std");
const types = @import("../lang/types.zig");
const batchmod = @import("../exec/batch.zig");
const eval = @import("../exec/eval.zig");
const valuemod = @import("../exec/value.zig");
const driver = @import("driver.zig");

const Batch = batchmod.Batch;

pub const TableWriter = struct {
    gpa: std.mem.Allocator,
    names: []const []const u8,
    ncols: usize,
    cells: std.array_list.Managed([]const u8),
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
        defer self.deinit();
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

    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };

    fn vtWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
        const self: *TableWriter = @ptrCast(@alignCast(ptr));
        return self.writeBatch(arena, b);
    }
    fn vtClose(ptr: *anyopaque) anyerror!void {
        const self: *TableWriter = @ptrCast(@alignCast(ptr));
        return self.close();
    }
    fn vtAbort(ptr: *anyopaque) void {
        const self: *TableWriter = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn padded(out: *std.Io.Writer, s: []const u8, width: usize) !void {
    try out.writeAll(s);
    try out.splatBytesAll(" ", width - s.len);
}

/// `--format json` sink: one JSON object per row (NDJSON) on stdout, streamed
/// per batch — unlike the table it never buffers, so memory stays constant.
/// int/float/bool ride as JSON numbers/booleans (non-finite floats as null),
/// decimal as a string (a float would lose precision), temporal types as their
/// ISO text, bytes as base64.
pub const JsonWriter = struct {
    gpa: std.mem.Allocator,
    /// Pre-rendered `"name":` prefixes, escaped once at open.
    keys: []const []const u8,
    buf: [8192]u8 = undefined,
    fw: std.fs.File.Writer = undefined,

    pub fn open(gpa: std.mem.Allocator, schema: types.Schema) !*JsonWriter {
        const self = try gpa.create(JsonWriter);
        self.* = .{ .gpa = gpa, .keys = &.{} };
        self.fw = std.fs.File.stdout().writer(&self.buf);
        const keys = try gpa.alloc([]const u8, schema.fields.len);
        for (schema.fields, 0..) |f, i| {
            var k = std.array_list.Managed(u8).init(gpa);
            try k.append('"');
            try appendJsonEscaped(&k, f.name);
            try k.appendSlice("\":");
            keys[i] = try k.toOwnedSlice();
        }
        self.keys = keys;
        return self;
    }

    pub fn writeBatch(self: *JsonWriter, arena: std.mem.Allocator, batch: Batch) !void {
        const out = &self.fw.interface;
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            try out.writeByte('{');
            for (batch.columns, 0..) |*col, c| {
                if (c > 0) try out.writeByte(',');
                try out.writeAll(self.keys[c]);
                try writeJsonValue(out, arena, col.getValue(r));
            }
            try out.writeAll("}\n");
        }
    }

    fn writeJsonValue(out: *std.Io.Writer, arena: std.mem.Allocator, v: valuemod.Value) !void {
        switch (v) {
            .null => try out.writeAll("null"),
            .bool => |b| try out.writeAll(if (b) "true" else "false"),
            .int => |i| try out.print("{d}", .{i}),
            .float => |f| if (std.math.isFinite(f)) try out.print("{d}", .{f}) else try out.writeAll("null"),
            .bytes => |b| {
                const enc = std.base64.standard.Encoder;
                const dst = try arena.alloc(u8, enc.calcSize(b.len));
                try out.writeByte('"');
                try out.writeAll(enc.encode(dst, b));
                try out.writeByte('"');
            },
            else => {
                const s = try eval.valueToString(arena, v);
                try out.writeByte('"');
                try writeEscapedW(out, s);
                try out.writeByte('"');
            },
        }
    }

    pub fn close(self: *JsonWriter) !void {
        try self.fw.interface.flush();
        self.deinit();
    }

    fn deinit(self: *JsonWriter) void {
        for (self.keys) |k| self.gpa.free(k);
        self.gpa.free(self.keys);
        self.gpa.destroy(self);
    }

    pub fn sink(self: *JsonWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.Sink.VTable{ .writeBatch = vtWrite, .close = vtClose, .abort = vtAbort };

    fn vtWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
        const self: *JsonWriter = @ptrCast(@alignCast(ptr));
        return self.writeBatch(arena, b);
    }
    fn vtClose(ptr: *anyopaque) anyerror!void {
        const self: *JsonWriter = @ptrCast(@alignCast(ptr));
        return self.close();
    }
    fn vtAbort(ptr: *anyopaque) void {
        const self: *JsonWriter = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn appendJsonEscaped(list: *std.array_list.Managed(u8), s: []const u8) !void {
    for (s) |c| switch (c) {
        '"' => try list.appendSlice("\\\""),
        '\\' => try list.appendSlice("\\\\"),
        '\n' => try list.appendSlice("\\n"),
        '\r' => try list.appendSlice("\\r"),
        '\t' => try list.appendSlice("\\t"),
        else => if (c < 0x20) {
            var b: [6]u8 = undefined;
            try list.appendSlice(std.fmt.bufPrint(&b, "\\u{x:0>4}", .{c}) catch unreachable);
        } else try list.append(c),
    };
}

fn writeEscapedW(out: *std.Io.Writer, s: []const u8) !void {
    for (s) |c| switch (c) {
        '"' => try out.writeAll("\\\""),
        '\\' => try out.writeAll("\\\\"),
        '\n' => try out.writeAll("\\n"),
        '\r' => try out.writeAll("\\r"),
        '\t' => try out.writeAll("\\t"),
        else => if (c < 0x20) try out.print("\\u{x:0>4}", .{c}) else try out.writeByte(c),
    };
}

test "json escape covers quotes, backslash, and control bytes" {
    var l = std.array_list.Managed(u8).init(std.testing.allocator);
    defer l.deinit();
    try appendJsonEscaped(&l, "a\"b\\c\nd\x01");
    try std.testing.expectEqualStrings("a\\\"b\\\\c\\nd\\u0001", l.items);
}
