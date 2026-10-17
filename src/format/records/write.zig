//! Rows as raw records, for a `LOAD INTO` with `format = 'records'`: each row's one
//! value written verbatim, then the chosen newline, so a fixed-width, bank or EDI
//! file a query composes lands byte for byte. BYTES are written as they are, TEXT
//! in `encoding` (UTF-8 unless latin-1 or cp1252 is named). The plan admits one
//! STRING or BYTES column only. A NULL row, or text the encoding cannot hold, is
//! an error naming the row's place in the output, never skipped or written empty.
//!
//! The file goes through `Output`, so it may be gzipped, uploaded or appended to.
//! Lanes render batches apart; each rendering leads with a small header (its row
//! count, and the first bad row if any), so `writeRendered`, called in output
//! order, can count rows and name the bad one.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const csv = @import("../csv.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const Output = @import("../output.zig").Output;

pub const Newline = enum {
    lf,
    crlf,
    cr,
    none,

    pub fn bytes(self: Newline) []const u8 {
        return switch (self) {
            .lf => "\n",
            .crlf => "\r\n",
            .cr => "\r",
            .none => "",
        };
    }
};

pub const Options = struct {
    newline: Newline = .lf,
    encoding: csv.Encoding = .utf8,
};

pub const newline_names = "lf, crlf, cr, none, or '\\n', '\\r\\n', '\\r', ''";

/// A `newline` value: a name, case aside, or its escaped (`'\r\n'`) or literal
/// bytes; empty is none.
pub fn parseNewline(s: []const u8) ?Newline {
    inline for (std.meta.fields(Newline)) |f| {
        const nl: Newline = @enumFromInt(f.value);
        if (std.ascii.eqlIgnoreCase(s, f.name) or std.mem.eql(u8, s, nl.bytes())) return nl;
    }
    if (std.mem.eql(u8, s, "\\n")) return .lf;
    if (std.mem.eql(u8, s, "\\r\\n")) return .crlf;
    if (std.mem.eql(u8, s, "\\r")) return .cr;
    return null;
}

const Why = enum(u8) { none, null_value, unencodable };

const head_len = 9;

pub const Writer = struct {
    out: *Output,
    opts: Options,
    rows: u64 = 0,

    /// An `.append` to a file not ending in the newline adds one first, so the first
    /// new record does not join the last old one; with no newline it just continues.
    pub fn open(arena: std.mem.Allocator, path: []const u8, mode: driver.FileMode, opts: Options) !*Writer {
        const out = try Output.open(arena, path, "text/plain", mode);
        errdefer out.abort();
        const nl = opts.newline.bytes();
        const lead = nl.len > 0 and mode == .append and out.end > 0 and ((try out.lastByte(arena, path)) orelse nl[nl.len - 1]) != nl[nl.len - 1];
        try out.compress(path);
        if (lead) try out.writer().writeAll(nl);
        return init(arena, out, opts);
    }

    fn init(arena: std.mem.Allocator, out: *Output, opts: Options) !*Writer {
        const self = try arena.create(Writer);
        self.* = .{ .out = out, .opts = opts };
        return self;
    }

    /// Writes the batch's records until a bad one, whose index and reason it returns.
    fn renderRows(self: *Writer, w: *std.Io.Writer, arena: std.mem.Allocator, batch: Batch) !struct { at: usize, why: Why } {
        const col = &batch.columns[0];
        const nl = self.opts.newline.bytes();
        for (0..batch.len) |r| {
            if (!col.validity.get(r)) return .{ .at = r, .why = .null_value };
            const v = col.data.bytes.at(r);
            const bytes = if (col.ty.kind == .bytes) v else csv.encodeField(arena, self.opts.encoding, v) catch |e| switch (e) {
                error.Unencodable => return .{ .at = r, .why = .unencodable },
                else => |x| return x,
            };
            try w.writeAll(bytes);
            try w.writeAll(nl);
        }
        return .{ .at = batch.len, .why = .none };
    }

    fn badRow(self: *Writer, at: u64, why: Why) anyerror {
        var buf: [256]u8 = undefined;
        const row = self.rows + at + 1;
        return switch (why) {
            .none => unreachable,
            .null_value => eval.explain(error.RecordNull, std.fmt.bufPrint(&buf, "row {d} of the records output is NULL; a records file writes every row's value, so give it one (`coalesce(rec, '')`) or filter the row out", .{row}) catch "a records row is NULL"),
            .unencodable => eval.explain(error.RecordUnencodable, std.fmt.bufPrint(&buf, "row {d} of the records output holds a character {s} cannot encode (or text that is not UTF-8)", .{ row, @tagName(self.opts.encoding) }) catch "a records row cannot be encoded"),
        };
    }

    pub fn writeBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) !void {
        if (batch.len == 0) return;
        const got = self.renderRows(self.out.writer(), arena, batch) catch |e| return self.out.specific(e);
        if (got.why != .none) return self.badRow(got.at, got.why);
        self.rows += batch.len;
    }

    pub fn renderBatch(self: *Writer, arena: std.mem.Allocator, batch: Batch) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.ensureUnusedCapacity(head_len + batch.len * 64);
        try aw.writer.writeAll(&[_]u8{0} ** head_len);
        const got = try self.renderRows(&aw.writer, arena, batch);
        const out = aw.writer.buffered();
        std.mem.writeInt(u32, out[0..4], @intCast(got.at), .little);
        std.mem.writeInt(u32, out[4..8], @intCast(batch.len), .little);
        out[8] = @intFromEnum(got.why);
        return out;
    }

    pub fn writeRendered(self: *Writer, rendered: []const u8) !void {
        if (rendered.len < head_len) return;
        const at = std.mem.readInt(u32, rendered[0..4], .little);
        const n = std.mem.readInt(u32, rendered[4..8], .little);
        const why: Why = @enumFromInt(rendered[8]);
        self.out.writer().writeAll(rendered[head_len..]) catch |e| return self.out.specific(e);
        if (why != .none) return self.badRow(at, why);
        self.rows += n;
    }

    pub fn close(self: *Writer) !void {
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

const testing = std.testing;
const column = @import("../../exec/column.zig");
const types = @import("../../lang/types.zig");

fn textBatch(a: std.mem.Allocator, kind: types.TypeKind, vals: []const ?[]const u8) !Batch {
    var b = column.Builder.init(a, types.Type.init(kind).asNullable());
    for (vals) |v| if (v) |s| try b.append(.{ .string = s }) else try b.append(.null);
    const cols = try a.alloc(column.Column, 1);
    cols[0] = try b.finish();
    const schema = try a.create(types.Schema);
    const fields = try a.alloc(types.Schema.Field, 1);
    fields[0] = .{ .name = "rec", .ty = cols[0].ty };
    schema.* = .{ .fields = fields };
    return .{ .schema = schema, .columns = cols, .len = vals.len };
}

fn writeFile(a: std.mem.Allocator, path: []const u8, mode: driver.FileMode, opts: Options, batches: []const Batch) !void {
    const w = try Writer.open(a, path, mode, opts);
    errdefer w.abort();
    for (batches, 0..) |b, i| if (i % 2 == 0) try w.writeBatch(a, b) else try w.writeRendered(try w.renderBatch(a, b));
    try w.close();
}

test "parseNewline takes names, escapes, literal bytes and empty; nothing else" {
    try testing.expectEqual(Newline.crlf, parseNewline("CRLF").?);
    try testing.expectEqual(Newline.crlf, parseNewline("\\r\\n").?);
    try testing.expectEqual(Newline.crlf, parseNewline("\r\n").?);
    try testing.expectEqual(Newline.lf, parseNewline("\\n").?);
    try testing.expectEqual(Newline.cr, parseNewline("\\r").?);
    try testing.expectEqual(Newline.none, parseNewline("").?);
    try testing.expectEqual(Newline.none, parseNewline("none").?);
    try testing.expect(parseNewline("\\t") == null);
    try testing.expect(parseNewline("\\n\\r") == null);
}

test "records are written verbatim with each newline, across written and rendered batches" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "o.ret" });
    const b1 = try textBatch(a, .string, &.{ "A  01", "" });
    const b2 = try textBatch(a, .bytes, &.{"\x00\xff"});
    const Case = struct { nl: Newline, want: []const u8 };
    for ([_]Case{
        .{ .nl = .lf, .want = "A  01\n\n\x00\xff\n" },
        .{ .nl = .crlf, .want = "A  01\r\n\r\n\x00\xff\r\n" },
        .{ .nl = .cr, .want = "A  01\r\r\x00\xff\r" },
        .{ .nl = .none, .want = "A  01\x00\xff" },
    }) |c| {
        try writeFile(a, path, .truncate, .{ .newline = c.nl }, &.{ b1, b2 });
        try testing.expectEqualStrings(c.want, try tmp.dir.readFileAlloc(a, "o.ret", 1 << 16));
    }
}

test "a NULL row and an unencodable one are errors naming the output row" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "o.ret" });
    const ok = try textBatch(a, .string, &.{ "a", "b" });
    const bad = try textBatch(a, .string, &.{ "c", null });
    for (0..2) |rendered| {
        const w = try Writer.open(a, path, .truncate, .{});
        try w.writeBatch(a, ok);
        const e = if (rendered == 1) w.writeRendered(try w.renderBatch(a, bad)) else w.writeBatch(a, bad);
        try testing.expectError(error.RecordNull, e);
        try testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.RecordNull).?, "row 4 ") != null);
        w.abort();
    }
    const w = try Writer.open(a, path, .truncate, .{ .encoding = .latin1 });
    try w.writeBatch(a, try textBatch(a, .string, &.{"ação"}));
    try testing.expectError(error.RecordUnencodable, w.writeBatch(a, try textBatch(a, .string, &.{ "ok", "€" })));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.RecordUnencodable).?, "row 3 ") != null);
    w.abort();
}

test "latin-1 encodes text but leaves bytes alone; an append adds the newline the file lacks" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "o.ret" });
    try writeFile(a, path, .truncate, .{ .encoding = .latin1, .newline = .crlf }, &.{ try textBatch(a, .string, &.{"ação"}), try textBatch(a, .bytes, &.{"ç"}) });
    try testing.expectEqualStrings("a\xe7\xe3o\r\nç\r\n", try tmp.dir.readFileAlloc(a, "o.ret", 1 << 16));
    try tmp.dir.writeFile(.{ .sub_path = "o.ret", .data = "old" });
    try writeFile(a, path, .append, .{ .newline = .crlf }, &.{try textBatch(a, .string, &.{"new"})});
    try writeFile(a, path, .append, .{ .newline = .crlf }, &.{try textBatch(a, .string, &.{"more"})});
    try testing.expectEqualStrings("old\r\nnew\r\nmore\r\n", try tmp.dir.readFileAlloc(a, "o.ret", 1 << 16));
    try writeFile(a, path, .append, .{ .newline = .none }, &.{try textBatch(a, .string, &.{"x"})});
    try testing.expectEqualStrings("old\r\nnew\r\nmore\r\nx", try tmp.dir.readFileAlloc(a, "o.ret", 1 << 16));
}
