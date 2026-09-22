//! `write stdout` sink: renders batches as a left-aligned text table on stdout.
//! Rows are accumulated (stringified into the sink's own allocator, since batch
//! arenas are reset between calls) and the table is laid out on `close`, once all
//! column widths are known. Intended for the REPL and ad-hoc `pipeline run`.

const std = @import("std");
const types = @import("../lang/types.zig");
const Batch = @import("../exec/batch.zig").Batch;
const eval = @import("../exec/eval.zig");
const Value = @import("../exec/value.zig").Value;
const driver = @import("driver.zig");

/// A finished result as text: what the fitted table and the REPL's `\view` draw
/// from. `cells` is row-major and holds the first `keep_max` rows; when the query
/// produced more, `tail` holds the last few, so a summary can still show both ends.
pub const Grid = struct {
    names: []const []const u8,
    types: []const []const u8,
    right: []const bool,
    cells: []const ?[]const u8,
    tail: []const ?[]const u8 = &.{},
    total_rows: usize,

    pub fn ncols(self: Grid) usize {
        return self.names.len;
    }
    pub fn kept(self: Grid) usize {
        return if (self.names.len == 0) 0 else self.cells.len / self.names.len;
    }
    pub fn cell(self: Grid, r: usize, c: usize) ?[]const u8 {
        return self.cells[r * self.names.len + c];
    }
};

/// Set by the REPL, and by nothing else: fit tables to the terminal and keep the
/// last one for `\view`. `basalt run` leaves it off, so its stdout is the whole
/// result — every row and column — whether or not a terminal is watching.
pub var interactive: bool = false;
var last_store: ?*TableWriter = null;

/// The most recent retained result, valid until the next one replaces it.
pub fn last() ?Grid {
    return if (last_store) |t| t.grid() else null;
}

pub fn dropLast() void {
    if (last_store) |t| t.deinit();
    last_store = null;
}

pub const TableWriter = struct {
    gpa: std.mem.Allocator,
    names: []const []const u8,
    type_labels: []const []const u8,
    right: []const bool,
    ncols: usize,
    cells: std.array_list.Managed(?[]const u8),
    tail: std.array_list.Managed(?[]const u8),
    tail_at: usize = 0,
    nrows: usize = 0,
    /// An interactive session on a terminal: fit the table to it rather than
    /// print every cell.
    tty: bool,

    /// Rows an interactive session keeps. `basalt run` prints everything and keeps it all.
    pub const keep_max = 10_000;
    const tail_rows = 20;
    /// Rows a fitted table shows before it elides the middle.
    pub const show_max = 40;

    pub fn open(gpa: std.mem.Allocator, schema: types.Schema) !*TableWriter {
        const self = try gpa.create(TableWriter);
        const names = try gpa.alloc([]const u8, schema.fields.len);
        const labels = try gpa.alloc([]const u8, schema.fields.len);
        const right = try gpa.alloc(bool, schema.fields.len);
        for (schema.fields, 0..) |f, i| {
            names[i] = try gpa.dupe(u8, f.name);
            labels[i] = try typeLabel(gpa, f.ty);
            right[i] = f.ty.kind.isNumeric();
        }
        self.* = .{
            .gpa = gpa,
            .names = names,
            .type_labels = labels,
            .right = right,
            .ncols = schema.fields.len,
            .cells = std.array_list.Managed(?[]const u8).init(gpa),
            .tail = std.array_list.Managed(?[]const u8).init(gpa),
            .tty = interactive and std.posix.isatty(std.fs.File.stdout().handle),
        };
        return self;
    }

    pub fn writeBatch(self: *TableWriter, arena: std.mem.Allocator, batch: Batch) !void {
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            const overflow = self.tty and self.nrows >= keep_max;
            for (batch.columns, 0..) |*col, c| {
                const v = col.getValue(r);
                const s: ?[]const u8 = if (v.isNull()) null else try self.gpa.dupe(u8, try eval.valueToString(arena, v));
                if (!overflow) {
                    try self.cells.append(s);
                } else if (self.tail.items.len < tail_rows * self.ncols) {
                    try self.tail.append(s);
                } else {
                    // A ring of the last rows, rotated into order on `grid()`.
                    const at = self.tail_at * self.ncols + c;
                    if (self.tail.items[at]) |old| self.gpa.free(old);
                    self.tail.items[at] = s;
                }
            }
            if (overflow and self.tail.items.len == tail_rows * self.ncols and self.nrows >= keep_max + tail_rows)
                self.tail_at = (self.tail_at + 1) % tail_rows;
            self.nrows += 1;
        }
    }

    fn grid(self: *TableWriter) Grid {
        return .{ .names = self.names, .types = self.type_labels, .right = self.right, .cells = self.cells.items, .tail = self.tail.items, .total_rows = self.nrows };
    }

    pub fn close(self: *TableWriter) !void {
        var keep = false;
        defer if (!keep) self.deinit();

        // Streaming, not positional: a second SELECT's writer would otherwise start
        // at offset 0 again and, with stdout redirected to a file, overwrite the first.
        var buf: [8192]u8 = undefined;
        var fw = std.fs.File.stdout().writerStreaming(&buf);
        const out = &fw.interface;
        if (self.tty) {
            self.settleTail();
            const size = termSize(std.fs.File.stdout());
            try renderFitted(out, self.grid(), .{ .width = size.cols, .color = !std.process.hasEnvVarConstant("NO_COLOR"), .hint = "\\view scrolls it" });
            try out.flush();
            dropLast();
            last_store = self;
            keep = true;
            return;
        }
        try self.renderPlain(out);
        try out.flush();
    }

    /// Rotate the tail ring so its rows read oldest first.
    fn settleTail(self: *TableWriter) void {
        if (self.tail_at == 0 or self.ncols == 0) return;
        std.mem.rotate(?[]const u8, self.tail.items, self.tail_at * self.ncols);
        self.tail_at = 0;
    }

    /// Every row and every column, left-aligned: what a pipe or a file receives.
    fn renderPlain(self: *TableWriter, out: *std.Io.Writer) !void {
        const gpa = self.gpa;
        const widths = try gpa.alloc(usize, self.ncols);
        defer gpa.free(widths);
        for (self.names, 0..) |n, i| widths[i] = displayWidth(n);
        for (0..self.nrows) |r| {
            for (0..self.ncols) |c| {
                const len = displayWidth(self.cells.items[r * self.ncols + c] orelse "");
                if (len > widths[c]) widths[c] = len;
            }
        }
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
                try padded(out, self.cells.items[r * self.ncols + c] orelse "", widths[c]);
            }
            try out.writeByte('\n');
        }
        try out.print("({d} row{s})\n", .{ self.nrows, if (self.nrows == 1) "" else "s" });
    }

    /// Failure path: discard the accumulated rows without printing.
    pub fn abort(self: *TableWriter) void {
        self.deinit();
    }

    fn deinit(self: *TableWriter) void {
        for (self.cells.items) |c| if (c) |s| self.gpa.free(s);
        self.cells.deinit();
        for (self.tail.items) |c| if (c) |s| self.gpa.free(s);
        self.tail.deinit();
        for (self.names) |n| self.gpa.free(n);
        self.gpa.free(self.names);
        for (self.type_labels) |n| self.gpa.free(n);
        self.gpa.free(self.type_labels);
        self.gpa.free(self.right);
        self.gpa.destroy(self);
    }

    pub fn sink(self: *TableWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable = driver.sinkVTable(TableWriter);
};

fn typeLabel(gpa: std.mem.Allocator, t: types.Type) ![]const u8 {
    const q: []const u8 = if (t.nullable) "?" else "";
    if (t.kind == .decimal) return std.fmt.allocPrint(gpa, "decimal({d},{d}){s}", .{ t.precision, t.scale, q });
    return std.fmt.allocPrint(gpa, "{s}{s}", .{ @tagName(t.kind), q });
}

pub const TermSize = struct { cols: usize = 80, rows: usize = 24 };

pub fn termSize(file: std.fs.File) TermSize {
    var ws: std.posix.winsize = undefined;
    const rc = std.posix.system.ioctl(file.handle, std.posix.T.IOCGWINSZ, @intFromPtr(&ws));
    if (std.posix.errno(rc) != .SUCCESS or ws.col == 0) return .{};
    return .{ .cols = ws.col, .rows = if (ws.row == 0) 24 else ws.row };
}

pub const Fit = struct {
    width: usize = 80,
    color: bool = false,
    max_rows: usize = TableWriter.show_max,
    /// Appended to the footer when something was elided.
    hint: []const u8 = "",
};

/// The widest a cell is drawn in a fitted table; the rest is `…`.
pub const cell_max = 40;

/// Which columns a `width`-wide terminal can show: all of them, or — in the manner
/// of pandas — as many as fit taken alternately from the left and the right, with
/// the middle elided. `left + right < widths.len` means a `…` column sits between.
pub const ColumnFit = struct { left: usize, right: usize };

pub fn fitColumns(widths: []const usize, width: usize) ColumnFit {
    var all: usize = 0;
    for (widths, 0..) |w, i| all += w + @as(usize, if (i > 0) 2 else 0);
    if (all <= width or widths.len <= 1) return .{ .left = widths.len, .right = 0 };
    // Room for the `…` column and its separator comes off the top.
    var used: usize = 3;
    var left: usize = 0;
    var right: usize = 0;
    while (left + right < widths.len) {
        const take_left = left <= right;
        const idx = if (take_left) left else widths.len - 1 - right;
        const cost = widths[idx] + 2;
        if (used + cost > width and left + right > 0) break;
        used += cost;
        if (take_left) left += 1 else right += 1;
    }
    return .{ .left = left, .right = right };
}

/// A result fitted to a terminal: a row of types under the names, numbers to the
/// right, `NULL` said out loud, long cells cut at `cell_max`, and `…` standing in
/// for the rows and columns that do not fit. The footer always gives the true size.
pub fn renderFitted(out: *std.Io.Writer, g: Grid, fit: Fit) !void {
    const n = g.ncols();
    if (n == 0) return out.print("({d} row{s})\n", .{ g.total_rows, if (g.total_rows == 1) "" else "s" });
    const kept = g.kept();
    const elide_rows = g.total_rows > fit.max_rows;
    const head: usize = if (elide_rows) fit.max_rows / 2 else kept;
    const tail_n: usize = if (elide_rows) fit.max_rows / 2 else 0;
    const tail_rows_kept = g.tail.len / n;
    // The closing rows come from the tail ring when the result outgrew what was
    // kept, and from the end of the kept rows otherwise.
    const tail_from_ring = elide_rows and tail_rows_kept >= tail_n;

    var width_buf: [512]usize = undefined;
    const shown_cols = @min(n, width_buf.len);
    const widths = width_buf[0..shown_cols];
    for (widths, 0..) |*w, c| {
        w.* = @max(displayWidth(g.names[c]), displayWidth(g.types[c]));
        for (0..head) |r| w.* = @max(w.*, cellWidth(g.cell(r, c)));
        for (0..tail_n) |t| w.* = @max(w.*, cellWidth(tailCell(g, tail_from_ring, tail_n, t, c)));
        w.* = @min(w.*, cell_max);
    }
    const cf = fitColumns(widths, fit.width);
    const dim = if (fit.color) "\x1b[2m" else "";
    const bold = if (fit.color) "\x1b[1m" else "";
    const reset = if (fit.color) "\x1b[0m" else "";

    const Row = struct {
        fn write(o: *std.Io.Writer, gg: Grid, ws: []const usize, c: ColumnFit, style: []const u8, rs: []const u8, dm: []const u8, ctx: anytype) !void {
            var first = true;
            for (0..ws.len) |i| {
                const in_left = i < c.left;
                const in_right = i >= ws.len - c.right;
                if (!in_left and !in_right) continue;
                if (!first) try o.writeAll("  ");
                if (in_right and i == ws.len - c.right and c.left + c.right < ws.len) try o.print("{s}…{s}  ", .{ dm, rs });
                first = false;
                try ctx.cell(o, gg, i, ws[i], style, rs, dm);
            }
            if (c.right == 0 and c.left < ws.len) try o.print("  {s}…{s}", .{ dm, rs });
            try o.writeByte('\n');
        }
    };

    const Names = struct {
        fn cell(_: @This(), o: *std.Io.Writer, gg: Grid, i: usize, w: usize, style: []const u8, rs: []const u8, _: []const u8) !void {
            try o.writeAll(style);
            try aligned(o, gg.names[i], w, gg.right[i]);
            try o.writeAll(rs);
        }
    };
    const Types = struct {
        fn cell(_: @This(), o: *std.Io.Writer, gg: Grid, i: usize, w: usize, style: []const u8, rs: []const u8, _: []const u8) !void {
            try o.writeAll(style);
            try aligned(o, gg.types[i], w, gg.right[i]);
            try o.writeAll(rs);
        }
    };
    const Rule = struct {
        fn cell(_: @This(), o: *std.Io.Writer, _: Grid, _: usize, w: usize, _: []const u8, _: []const u8, _: []const u8) !void {
            try o.splatBytesAll("-", w);
        }
    };
    const Gap = struct {
        fn cell(_: @This(), o: *std.Io.Writer, gg: Grid, i: usize, w: usize, _: []const u8, rs: []const u8, dm: []const u8) !void {
            try o.writeAll(dm);
            try aligned(o, "…", w, gg.right[i]);
            try o.writeAll(rs);
        }
    };
    const Data = struct {
        value: *const fn (Grid, usize, usize) ?[]const u8,
        row: usize,
        fn cell(self: @This(), o: *std.Io.Writer, gg: Grid, i: usize, w: usize, _: []const u8, rs: []const u8, dm: []const u8) !void {
            if (self.value(gg, self.row, i)) |s| return alignedCell(o, s, w, gg.right[i]);
            try o.writeAll(dm);
            try aligned(o, "NULL", w, gg.right[i]);
            try o.writeAll(rs);
        }
    };
    const Pick = struct {
        fn kept_(gg: Grid, r: usize, c: usize) ?[]const u8 {
            return gg.cell(r, c);
        }
        fn ring(gg: Grid, r: usize, c: usize) ?[]const u8 {
            return gg.tail[r * gg.names.len + c];
        }
    };

    try Row.write(out, g, widths, cf, bold, reset, dim, Names{});
    try Row.write(out, g, widths, cf, dim, reset, dim, Types{});
    try Row.write(out, g, widths, cf, "", reset, dim, Rule{});
    for (0..head) |r| try Row.write(out, g, widths, cf, "", reset, dim, Data{ .value = Pick.kept_, .row = r });
    if (elide_rows) {
        try Row.write(out, g, widths, cf, "", reset, dim, Gap{});
        for (0..tail_n) |t| {
            if (tail_from_ring)
                try Row.write(out, g, widths, cf, "", reset, dim, Data{ .value = Pick.ring, .row = tail_rows_kept - tail_n + t })
            else
                try Row.write(out, g, widths, cf, "", reset, dim, Data{ .value = Pick.kept_, .row = kept - tail_n + t });
        }
    }

    const cols_cut = cf.left + cf.right < n;
    try out.writeAll(dim);
    try out.writeByte('(');
    try thousands(out, g.total_rows);
    try out.print(" row{s}", .{if (g.total_rows == 1) "" else "s"});
    if (cols_cut or elide_rows) try out.print(" × {d} columns", .{n});
    if (cols_cut) try out.print(", {d} shown", .{cf.left + cf.right});
    if ((cols_cut or elide_rows) and fit.hint.len > 0) try out.print(" — {s}", .{fit.hint});
    try out.print("){s}\n", .{reset});
}

fn tailCell(g: Grid, from_ring: bool, tail_n: usize, t: usize, c: usize) ?[]const u8 {
    const n = g.names.len;
    if (from_ring) return g.tail[(g.tail.len / n - tail_n + t) * n + c];
    return g.cell(g.kept() - tail_n + t, c);
}

fn cellWidth(s: ?[]const u8) usize {
    return displayWidth(s orelse "NULL");
}

fn thousands(out: *std.Io.Writer, v: usize) !void {
    var digits: [24]u8 = undefined;
    const s = std.fmt.bufPrint(&digits, "{d}", .{v}) catch unreachable;
    for (s, 0..) |ch, i| {
        if (i != 0 and (s.len - i) % 3 == 0) try out.writeByte(',');
        try out.writeByte(ch);
    }
}

fn aligned(out: *std.Io.Writer, s: []const u8, width: usize, right: bool) !void {
    const w = displayWidth(s);
    const pad = if (width > w) width - w else 0;
    if (right) try out.splatBytesAll(" ", pad);
    try out.writeAll(s);
    if (!right) try out.splatBytesAll(" ", pad);
}

/// A data cell in `width` columns: control characters flattened to spaces so a
/// newline inside a value cannot break the grid, and the overflow cut with `…`.
pub fn alignedCell(out: *std.Io.Writer, s: []const u8, width: usize, right: bool) !void {
    const w = displayWidth(s);
    const shown = @min(w, width);
    const pad = width - shown;
    if (right) try out.splatBytesAll(" ", pad);
    var cols: usize = 0;
    var i: usize = 0;
    const limit = if (w > width) width -| 1 else width;
    while (i < s.len and cols < limit) {
        var j = i + 1;
        while (j < s.len and s[j] & 0xC0 == 0x80) j += 1;
        if (s[i] < 0x20 or s[i] == 0x7f) try out.writeByte(' ') else try out.writeAll(s[i..j]);
        cols += 1;
        i = j;
    }
    if (w > width and width > 0) try out.writeAll("…");
    if (!right) try out.splatBytesAll(" ", pad);
}

/// Column width in characters, not bytes. `LIQUIDAÇÃO` is ten characters in
/// twelve bytes, and padding by the byte count pushed every later column of that
/// row out of line — which shows up the moment a file is not pure ASCII, i.e. on
/// most non-English data. Counts UTF-8 lead bytes, so a malformed sequence is
/// counted rather than rejected: this is output formatting, not validation. East
/// Asian wide characters still measure one, which is a separate problem.
pub fn displayWidth(s: []const u8) usize {
    var n: usize = 0;
    for (s) |c| {
        if (c & 0xC0 != 0x80) n += 1;
    }
    return n;
}

fn padded(out: *std.Io.Writer, s: []const u8, width: usize) !void {
    try out.writeAll(s);
    const w = displayWidth(s);
    try out.splatBytesAll(" ", if (width > w) width - w else 0);
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
        self.fw = std.fs.File.stdout().writerStreaming(&self.buf);
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

    fn writeJsonValue(out: *std.Io.Writer, arena: std.mem.Allocator, v: Value) !void {
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

    /// Failure path: release without flushing the tail of the stream.
    pub fn abort(self: *JsonWriter) void {
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

    const vtable = driver.sinkVTable(JsonWriter);
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

test "fitColumns: everything when it fits, else both ends around an elided middle" {
    try std.testing.expectEqual(ColumnFit{ .left = 3, .right = 0 }, fitColumns(&.{ 4, 4, 4 }, 80));
    // 6 columns of 10 in 40: `…` takes 3, each column costs 12 → three fit, left first.
    try std.testing.expectEqual(ColumnFit{ .left = 2, .right = 1 }, fitColumns(&.{ 10, 10, 10, 10, 10, 10 }, 40));
    // A first column wider than the terminal is still shown rather than nothing.
    try std.testing.expectEqual(ColumnFit{ .left = 1, .right = 0 }, fitColumns(&.{ 90, 5 }, 40));
}

test "renderFitted: types row, right-aligned numbers, NULL, cut cells, elided rows and columns" {
    const names = [_][]const u8{ "id", "name", "note", "amount" };
    const tys = [_][]const u8{ "int", "string?", "string", "float" };
    const right = [_]bool{ true, false, false, true };
    var cells: [6 * 4]?[]const u8 = undefined;
    const ids = [_][]const u8{ "1", "2", "3", "4", "5", "6" };
    for (0..6) |r| {
        cells[r * 4 + 0] = ids[r];
        cells[r * 4 + 1] = if (r == 1) null else "ana";
        cells[r * 4 + 2] = "a value that is much longer than forty columns wide";
        cells[r * 4 + 3] = "10.5";
    }
    const g = Grid{ .names = &names, .types = &tys, .right = &right, .cells = &cells, .total_rows = 6 };

    var buf: [2048]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try renderFitted(&w, g, .{ .width = 30, .max_rows = 4, .hint = "\\view scrolls it" });
    try std.testing.expectEqualStrings(
        \\ id  name     …  amount
        \\int  string?  …   float
        \\---  -------  …  ------
        \\  1  ana      …    10.5
        \\  2  NULL     …    10.5
        \\  …  …        …       …
        \\  5  ana      …    10.5
        \\  6  ana      …    10.5
        \\(6 rows × 4 columns, 3 shown — \view scrolls it)
        \\
    , w.buffered());

    w = std.Io.Writer.fixed(&buf);
    try renderFitted(&w, g, .{ .width = 200 });
    var it = std.mem.splitScalar(u8, w.buffered(), '\n');
    _ = it.next();
    _ = it.next();
    _ = it.next();
    try std.testing.expectEqualStrings("  1  ana      a value that is much longer than forty …    10.5", it.next().?);
}
