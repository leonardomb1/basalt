//! Rows as JSON, for `--format json` on stdout and a `LOAD INTO` a `.json`, `.ndjson`
//! or `.jsonl` file alike: one object per row, keyed by column name. NDJSON (one
//! object a line) unless `array` asks for one top-level array, as DuckDB's `COPY`
//! does; `read.zig` reads either back. The file goes through `Output`, so it may be
//! gzipped, uploaded or appended to (NDJSON only, as an array closes on its last
//! byte).
//!
//! Decimals ride as strings, since a JSON number is a double to most readers and
//! would lose digits; non-finite floats are null, temporal types ISO text and bytes
//! base64. Text that is not valid UTF-8 is repaired, each invalid byte U+FFFD.

const std = @import("std");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const celltext = @import("../../exec/celltext.zig");
const jsontext = @import("../../exec/jsontext.zig");
const driver = @import("../../connect/driver.zig");
const Output = @import("../output.zig").Output;

pub const Shape = enum { ndjson, array };

pub const Writer = struct {
    out: *Output,
    keys: []const []const u8,
    shape: Shape,
    started: bool = false,

    /// An `.append` to a file not ending in a newline adds one first, so the first
    /// new row does not join the last old one.
    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, mode: driver.FileMode, shape: Shape) !*Writer {
        const out = try Output.open(arena, path, "application/json", mode);
        errdefer out.abort();
        const self = try init(arena, out, schema, shape);
        const newline = mode == .append and out.end > 0 and ((try out.lastByte(arena, path)) orelse '\n') != '\n';
        try out.compress(path);
        if (newline) try out.writer().writeByte('\n');
        return self;
    }

    pub fn openStdout(arena: std.mem.Allocator, schema: types.Schema) !*Writer {
        return init(arena, try Output.openBorrowed(arena, std.fs.File.stdout()), schema, .ndjson);
    }

    fn init(arena: std.mem.Allocator, out: *Output, schema: types.Schema, shape: Shape) !*Writer {
        const keys = try arena.alloc([]const u8, schema.fields.len);
        for (schema.fields, keys) |f, *k| {
            var aw = std.Io.Writer.Allocating.init(arena);
            try jsontext.key(&aw.writer, f.name);
            k.* = aw.writer.buffered();
        }
        const self = try arena.create(Writer);
        self.* = .{ .out = out, .keys = keys, .shape = shape };
        return self;
    }

    /// A row ends in a newline as NDJSON; in an array rows are joined by `,\n` and the
    /// separator before a batch is `writeRendered`'s, so lanes render independently.
    fn renderRows(self: *Writer, w: *std.Io.Writer, batch: Batch) !void {
        for (0..batch.len) |r| {
            if (self.shape == .array and r > 0) try w.writeAll(",\n");
            try w.writeByte('{');
            for (batch.columns, self.keys, 0..) |*col, key, c| {
                if (c > 0) try w.writeByte(',');
                try w.writeAll(key);
                try celltext.write(w, col, r, Text{});
            }
            try w.writeByte('}');
            if (self.shape == .ndjson) try w.writeByte('\n');
        }
    }

    pub fn writeBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) !void {
        if (batch.len == 0) return;
        if (self.shape == .ndjson) {
            return self.renderRows(self.out.writer(), batch) catch |e| self.out.specific(e);
        }
        return self.writeRendered(try self.renderBatch(arena, batch));
    }

    pub fn renderBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.ensureUnusedCapacity(batch.len * 64 + 64);
        try self.renderRows(&aw.writer, batch);
        return aw.writer.buffered();
    }

    pub fn writeRendered(self: *Writer, bytes: []const u8) !void {
        if (bytes.len == 0) return;
        const w = self.out.writer();
        if (self.shape == .array) w.writeAll(if (self.started) ",\n" else "[\n") catch |e| return self.out.specific(e);
        w.writeAll(bytes) catch |e| return self.out.specific(e);
        self.started = true;
    }

    pub fn close(self: *Writer) !void {
        if (self.shape == .array)
            self.out.writer().writeAll(if (self.started) "\n]\n" else "[]\n") catch |e| return self.out.specific(e);
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

/// The `celltext.write` policy for JSON rows (`jsontext.Text`).
pub const Text = jsontext.Text;

/// `s` as the inside of a JSON string, repaired to UTF-8 first (`jsontext.chars`).
pub const writeEscaped = jsontext.chars;

test "json escape covers quotes, backslash, control bytes and invalid UTF-8" {
    var buf: [128]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try writeEscaped(&w, "a\"b\\c\nd\x01");
    try std.testing.expectEqualStrings("a\\\"b\\\\c\\nd\\u0001", w.buffered());
    w = std.Io.Writer.fixed(&buf);
    try writeEscaped(&w, "caf\xe9\t\"");
    try std.testing.expectEqualStrings("caf\u{FFFD}\\t\\\"", w.buffered());
}

fn oracleEscaped(out: *std.Io.Writer, s: []const u8) !void {
    if (std.unicode.utf8ValidateSlice(s)) return oracleEscapeValid(out, s);
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| try oracleEscapeValid(out, t);
}

fn oracleEscapeValid(out: *std.Io.Writer, s: []const u8) !void {
    var start: usize = 0;
    for (s, 0..) |c, i| {
        const esc: []const u8 = switch (c) {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            0...8, 11, 12, 14...0x1f => "",
            else => continue,
        };
        try out.writeAll(s[start..i]);
        if (esc.len > 0) try out.writeAll(esc) else try out.print("\\u{x:0>4}", .{c});
        start = i + 1;
    }
    try out.writeAll(s[start..]);
}

const OracleText = struct {
    pub const null_text = "null";
    pub const bool_true = "true";
    pub const bool_false = "false";
    pub const quote_decimal = "\"";
    pub const quote_temporal = "\"";

    pub fn string(_: OracleText, out: *std.Io.Writer, s: []const u8) !void {
        try out.writeByte('"');
        try oracleEscaped(out, s);
        try out.writeByte('"');
    }

    pub fn bytes(_: OracleText, out: *std.Io.Writer, s: []const u8) !void {
        try out.writeByte('"');
        try std.base64.standard.Encoder.encodeWriter(out, s);
        try out.writeByte('"');
    }

    pub fn float(_: OracleText, out: *std.Io.Writer, x: f64) !void {
        if (std.math.isFinite(x)) return out.print("{d}", .{x});
        return out.writeAll("null");
    }
};

test "the jsontext escaper, key and Text policy equal the copies this file held" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x15_0b);
    const r = prng.random();
    var in: [48]u8 = undefined;
    for (0..4000) |it| {
        const n = r.uintLessThan(usize, in.len);
        for (in[0..n]) |*c| c.* = if (it % 2 == 0) r.int(u8) else r.uintLessThan(u8, 0x80);
        var want = std.Io.Writer.Allocating.init(a);
        try want.writer.writeByte('"');
        try oracleEscaped(&want.writer, in[0..n]);
        try want.writer.writeAll("\":");
        var got = std.Io.Writer.Allocating.init(a);
        try jsontext.key(&got.writer, in[0..n]);
        try std.testing.expectEqualStrings(want.written(), got.written());
    }
    for (try celltext.goldenBatches(a)) |batch| for (batch.columns) |*col| for (0..batch.len) |row| {
        var want = std.Io.Writer.Allocating.init(a);
        try celltext.write(&want.writer, col, row, OracleText{});
        var got = std.Io.Writer.Allocating.init(a);
        try celltext.write(&got.writer, col, row, Text{});
        try std.testing.expectEqualStrings(want.written(), got.written());
    };
}

test "JSON rows of every kind, NDJSON and as one array" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const batches = try celltext.goldenBatches(a);
    const json = try a.alloc([]const u8, batches.len);
    for (batches, json) |batch, *j| {
        const w = try Writer.init(a, try Output.openBorrowed(a, std.fs.File.stdout()), batch.schema.*, .ndjson);
        j.* = try w.renderBatch(a, batch);
    }
    const tiny = celltext.golden_tiny;
    try celltext.expectGolden(&.{
        "{\"c\":true}\n{\"c\":false}\n{\"c\":null}\n",
        "{\"c\":0}\n{\"c\":-1}\n{\"c\":-9223372036854775808}\n{\"c\":9223372036854775807}\n{\"c\":null}\n",
        "{\"c\":0.1}\n{\"c\":0}\n{\"c\":-0}\n{\"c\":" ++ tiny ++ "}\n{\"c\":1000000000000000000000}\n{\"c\":-2.25}\n{\"c\":null}\n{\"c\":null}\n{\"c\":null}\n{\"c\":null}\n",
        "{\"c\":\"123.45\"}\n{\"c\":\"-0.0007\"}\n{\"c\":\"5\"}\n{\"c\":\"0.12345\"}\n{\"c\":\"-18446744073709.551615\"}\n{\"c\":\"0.00\"}\n{\"c\":null}\n",
        "{\"c\":\"\"}\n{\"c\":\"plain\"}\n{\"c\":\"tab\\there\"}\n{\"c\":\"line\\nbreak\"}\n{\"c\":\"cr\\rlf\\r\\n\"}\n{\"c\":\"say \\\"hi\\\"\"}\n{\"c\":\"O'Brien\"}\n{\"c\":\"back\\\\slash\\\\\"}\n{\"c\":\"a\\\\Nb\"}\n{\"c\":\"\\\\N \"}\n{\"c\":\"NULL\"}\n{\"c\":\"a\\u0001b\\u0002c\"}\n{\"c\":\"a,b;c:d.e-1 0\"}\n{\"c\":\"\xc3\xb3 \xf0\x9f\x98\x80\"}\n{\"c\":\"\\u0000nul\"}\n{\"c\":\"sixteen bytes and more\\u0001\"}\n{\"c\":null}\n",
        "{\"c\":\"\"}\n{\"c\":\"cGxhaW4=\"}\n{\"c\":\"AQIgXCAJCiI=\"}\n{\"c\":\"w7M=\"}\n{\"c\":null}\n",
        "{\"c\":\"1970-01-01\"}\n{\"c\":\"2024-01-01\"}\n{\"c\":\"0000-01-01\"}\n{\"c\":\"-5877641-06-24\"}\n{\"c\":null}\n",
        "{\"c\":\"00:00:00\"}\n{\"c\":\"01:02:03.000001\"}\n{\"c\":\"23:59:59.999999\"}\n{\"c\":null}\n",
        "{\"c\":\"1970-01-01 00:00:00\"}\n{\"c\":\"2024-01-01 00:00:00.123456\"}\n{\"c\":\"1969-12-31 23:59:59.999999\"}\n{\"c\":\"0001-01-01 00:00:00\"}\n{\"c\":null}\n",
        "{\"c\":null}\n{\"c\":null}\n",
        "{\"i\":1,\"s\":\"a b\",\"d\":\"2.50\"}\n{\"i\":null,\"s\":null,\"d\":null}\n{\"i\":-2,\"s\":\"\",\"d\":\"-0.1\"}\n",
    }, json);
}

test "an array joins batches with commas and closes, an empty one is []" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const batches = try celltext.goldenBatches(a);
    const last = batches[batches.len - 1];
    const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "a.json" });
    const w = try Writer.open(a, path, last.schema.*, .truncate, .array);
    try w.writeBatch(a, last);
    try w.writeRendered(try w.renderBatch(a, last));
    try w.close();
    const row = "{\"i\":1,\"s\":\"a b\",\"d\":\"2.50\"},\n{\"i\":null,\"s\":null,\"d\":null},\n{\"i\":-2,\"s\":\"\",\"d\":\"-0.1\"}";
    try std.testing.expectEqualStrings("[\n" ++ row ++ ",\n" ++ row ++ "\n]\n", try tmp.dir.readFileAlloc(a, "a.json", 1 << 16));
    const e = try Writer.open(a, path, last.schema.*, .truncate, .array);
    try e.close();
    try std.testing.expectEqualStrings("[]\n", try tmp.dir.readFileAlloc(a, "a.json", 1 << 16));
}
