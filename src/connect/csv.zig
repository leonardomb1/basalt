//! Minimal CSV source and sink. The source reads a header row into an all-string
//! schema (empty field = null) and produces batches of string columns; the sink
//! writes a header then serializes each batch. RFC-ish quoting: fields containing
//! comma/quote/newline are double-quoted with `""` escaping.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;

const BATCH_ROWS = 1024;
/// Reader/writer buffer size; also the max CSV line length (a line longer than
/// this yields `error.StreamTooLong`).
const LINE_BUF = 64 * 1024;

pub const CsvReader = struct {
    arena: std.mem.Allocator,
    file: std.fs.File,
    read_buf: [LINE_BUF]u8 = undefined,
    fr: std.fs.File.Reader = undefined,
    schema: types.Schema,
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*CsvReader {
        const self = try arena.create(CsvReader);
        self.* = .{
            .arena = arena,
            .file = try std.fs.cwd().openFile(path, .{}),
            .schema = undefined,
            .done = false,
        };
        self.fr = self.file.reader(&self.read_buf);

        const header = (try self.readLine()) orelse return error.EmptyCsv;
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var it = std.mem.splitScalar(u8, header, ',');
        while (it.next()) |name| {
            try fields.append(.{
                .name = try arena.dupe(u8, std.mem.trim(u8, name, " \t")),
                .ty = types.Type.init(.string).asNullable(),
            });
        }
        self.schema = .{ .fields = try fields.toOwnedSlice() };
        return self;
    }

    pub fn next(self: *CsvReader, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const ncols = self.schema.fields.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders) |*b| b.* = column.Builder.init(arena, types.Type.init(.string).asNullable());

        var rows: usize = 0;
        while (rows < BATCH_ROWS) {
            const line = (try self.readLine()) orelse {
                self.done = true;
                break;
            };
            if (line.len == 0) continue;
            try splitInto(arena, line, builders);
            rows += 1;
        }
        if (rows == 0) return null;

        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
    }

    pub fn close(self: *CsvReader) void {
        self.file.close();
    }

    pub fn source(self: *CsvReader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    fn readLine(self: *CsvReader) !?[]const u8 {
        // Returns a slice into the reader's buffer (invalidated on the next read);
        // safe because `column.Builder.append` dupes string values into the arena.
        const line = (try self.fr.interface.takeDelimiter('\n')) orelse return null;
        var s: []const u8 = line;
        if (s.len > 0 and s[s.len - 1] == '\r') s = s[0 .. s.len - 1];
        return s;
    }
};

fn splitInto(arena: std.mem.Allocator, line: []const u8, builders: []column.Builder) !void {
    var i: usize = 0;
    var col: usize = 0;
    while (col < builders.len) : (col += 1) {
        if (i < line.len and line[i] == '"') {
            i += 1;
            var buf = std.array_list.Managed(u8).init(arena);
            while (i < line.len) {
                if (line[i] == '"') {
                    if (i + 1 < line.len and line[i + 1] == '"') {
                        try buf.append('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                try buf.append(line[i]);
                i += 1;
            }
            try builders[col].append(.{ .string = try buf.toOwnedSlice() });
            if (i < line.len and line[i] == ',') i += 1;
        } else {
            const start = i;
            while (i < line.len and line[i] != ',') i += 1;
            const raw = line[start..i];
            try builders[col].append(if (raw.len == 0) .null else Value{ .string = raw });
            if (i < line.len and line[i] == ',') i += 1;
        }
    }
}

const source_vtable = driver.Source.VTable{
    .schema = srcSchema,
    .next = srcNext,
    .close = srcClose,
};
fn srcSchema(ptr: *anyopaque) types.Schema {
    const self: *CsvReader = @ptrCast(@alignCast(ptr));
    return self.schema;
}
fn srcNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *CsvReader = @ptrCast(@alignCast(ptr));
    return self.next(arena);
}
fn srcClose(ptr: *anyopaque) void {
    const self: *CsvReader = @ptrCast(@alignCast(ptr));
    self.close();
}

pub const CsvWriter = struct {
    file: std.fs.File,
    write_buf: [LINE_BUF]u8 = undefined,
    fw: std.fs.File.Writer = undefined,

    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        self.* = .{ .file = try std.fs.cwd().createFile(path, .{}) };
        self.fw = self.file.writer(&self.write_buf);

        const w = &self.fw.interface;
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try w.writeByte(',');
            try writeField(w, f.name);
        }
        try w.writeByte('\n');
        return self;
    }

    pub fn writeBatch(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) !void {
        const w = &self.fw.interface;
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            for (batch.columns, 0..) |*col, i| {
                if (i > 0) try w.writeByte(',');
                const v = col.getValue(r);
                if (!v.isNull()) try writeField(w, try eval.valueToString(arena, v));
            }
            try w.writeByte('\n');
        }
    }

    pub fn close(self: *CsvWriter) !void {
        try self.fw.interface.flush();
        self.file.close();
    }

    pub fn sink(self: *CsvWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }
};

const sink_vtable = driver.Sink.VTable{
    .writeBatch = sinkWrite,
    .close = sinkClose,
};
fn sinkWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn sinkClose(ptr: *anyopaque) anyerror!void {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    return self.close();
}

fn writeField(w: anytype, s: []const u8) !void {
    if (needsQuote(s)) {
        try w.writeByte('"');
        for (s) |c| {
            if (c == '"') try w.writeByte('"');
            try w.writeByte(c);
        }
        try w.writeByte('"');
    } else {
        try w.writeAll(s);
    }
}

fn needsQuote(s: []const u8) bool {
    for (s) |c| {
        if (c == ',' or c == '"' or c == '\n' or c == '\r') return true;
    }
    return false;
}
