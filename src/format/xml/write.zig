//! Rows as XML for a `LOAD INTO` an `.xml` file: one `<row>` a line inside one
//! `<rows>` root, a column an element named for it, as SQL Server's `FOR XML PATH`
//! writes them; `root` and `row` rename the two. A name an element cannot carry is
//! encoded as FOR XML does (`Valor Total` is `Valor_x0020_Total`), and `read.zig`
//! decodes it back.
//!
//! A NULL is an element left out, so the empty string, `<a></a>`, stays distinct;
//! a nested column, held as NULL, is left out too. Numbers are written as SQL shows
//! them, a non-finite float as XML Schema spells it (`NaN`, `INF`, `-INF`), temporal
//! types ISO text and bytes base64. The file goes through `Output`: it may be
//! gzipped or uploaded, but not appended to, as its root closes on its last line.

const std = @import("std");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const celltext = @import("../../exec/celltext.zig");
const driver = @import("../../connect/driver.zig");
const Output = @import("../output.zig").Output;
const escape = @import("escape.zig");

pub const Names = struct { root: []const u8 = "rows", row: []const u8 = "row" };

pub const Writer = struct {
    out: *Output,
    opens: []const []const u8,
    closes: []const []const u8,
    row_open: []const u8,
    row_close: []const u8,
    root_close: []const u8,

    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, names: Names) !*Writer {
        const out = try Output.open(arena, path, "application/xml", .truncate);
        errdefer out.abort();
        try out.compress(path);
        const self = try init(arena, out, schema, names);
        const w = out.writer();
        try w.writeAll("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<");
        try escape.name(w, names.root);
        try w.writeAll(">\n");
        return self;
    }

    fn init(arena: std.mem.Allocator, out: *Output, schema: types.Schema, names: Names) !*Writer {
        const opens = try arena.alloc([]const u8, schema.fields.len);
        const closes = try arena.alloc([]const u8, schema.fields.len);
        for (schema.fields, opens, closes) |f, *o, *c| {
            o.* = try tag(arena, "<", f.name);
            c.* = try tag(arena, "</", f.name);
        }
        const self = try arena.create(Writer);
        self.* = .{
            .out = out,
            .opens = opens,
            .closes = closes,
            .row_open = try std.fmt.allocPrint(arena, "  {s}", .{try tag(arena, "<", names.row)}),
            .row_close = try std.fmt.allocPrint(arena, "{s}\n", .{try tag(arena, "</", names.row)}),
            .root_close = try std.fmt.allocPrint(arena, "{s}\n", .{try tag(arena, "</", names.root)}),
        };
        return self;
    }

    fn tag(arena: std.mem.Allocator, lead: []const u8, name: []const u8) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.writer.writeAll(lead);
        try escape.name(&aw.writer, name);
        try aw.writer.writeByte('>');
        return aw.written();
    }

    fn renderRows(self: *Writer, w: *std.Io.Writer, batch: Batch) !void {
        for (0..batch.len) |r| {
            try w.writeAll(self.row_open);
            for (batch.columns, self.opens, self.closes) |*col, o, c| {
                if (celltext.isNull(col, r)) continue;
                try w.writeAll(o);
                try celltext.write(w, col, r, Text{});
                try w.writeAll(c);
            }
            try w.writeAll(self.row_close);
        }
    }

    pub fn writeBatch(self: *Writer, _: std.mem.Allocator, batch: Batch) !void {
        self.renderRows(self.out.writer(), batch) catch |e| return self.out.specific(e);
    }

    pub fn renderBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.ensureUnusedCapacity(batch.len * 96 + 64);
        try self.renderRows(&aw.writer, batch);
        return aw.writer.buffered();
    }

    pub fn writeRendered(self: *Writer, bytes: []const u8) !void {
        self.out.writer().writeAll(bytes) catch |e| return self.out.specific(e);
    }

    pub fn close(self: *Writer) !void {
        self.out.writer().writeAll(self.root_close) catch |e| return self.out.specific(e);
        return self.out.finish();
    }

    pub fn abort(self: *Writer) void {
        self.out.abort();
    }

    pub fn sink(self: *Writer) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sinkVTable(Writer);
};

/// The `celltext.write` policy for XML element text.
const Text = struct {
    pub const null_text = "";
    pub const bool_true = "true";
    pub const bool_false = "false";

    pub fn string(_: Text, out: *std.Io.Writer, s: []const u8) !void {
        try escape.text(out, s, .{});
    }

    pub fn bytes(_: Text, out: *std.Io.Writer, s: []const u8) !void {
        try std.base64.standard.Encoder.encodeWriter(out, s);
    }

    pub fn float(_: Text, out: *std.Io.Writer, x: f64) !void {
        if (std.math.isNan(x)) return out.writeAll("NaN");
        if (std.math.isInf(x)) return out.writeAll(if (x > 0) "INF" else "-INF");
        return out.print("{d}", .{x});
    }
};

test "XML rows of every kind; NULL left out, the empty string kept" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const batches = try celltext.goldenBatches(a);
    const last = batches[batches.len - 1];
    const w = try Writer.init(a, try Output.openBorrowed(a, std.fs.File.stdout()), last.schema.*, .{});
    try std.testing.expectEqualStrings(
        "  <row><i>1</i><s>a b</s><d>2.50</d></row>\n  <row></row>\n  <row><i>-2</i><s></s><d>-0.1</d></row>\n",
        try w.renderBatch(a, last),
    );
    const floats = batches[2];
    const f = try Writer.init(a, try Output.openBorrowed(a, std.fs.File.stdout()), floats.schema.*, .{ .row = "r" });
    const got = try f.renderBatch(a, floats);
    try std.testing.expect(std.mem.indexOf(u8, got, "<r><c>NaN</c></r>") != null);
    try std.testing.expect(std.mem.indexOf(u8, got, "<r><c>-INF</c></r>") != null);
}
