//! `\view`: a full-screen look at the REPL's last result, for the tables a
//! terminal cannot show at once. Arrows scroll by column and by row, the names
//! and types stay pinned on top, and the status line always says where you are.
//! It draws on the alternate screen, so leaving it puts the session back as it was.

const std = @import("std");
const table = @import("../connect/table.zig");
const line = @import("line.zig");

const Grid = table.Grid;

/// The widest a column is drawn here — wider than the inline table allows, since
/// scrolling sideways is cheap and a truncated value is the reason one came.
const col_max = 60;

const View = struct {
    g: Grid,
    widths: []usize,
    row0: usize = 0,
    col0: usize = 0,

    /// How many columns fit from `from` in `width`, always at least one.
    fn colsFrom(self: View, from: usize, width: usize) usize {
        var used: usize = 0;
        var n: usize = 0;
        var c = from;
        while (c < self.widths.len) : (c += 1) {
            const cost = self.widths[c] + @as(usize, if (n > 0) 2 else 0);
            if (used + cost > width and n > 0) break;
            used += cost;
            n += 1;
        }
        return @max(n, 1);
    }

    /// The leftmost first column that still ends the screen on the last one: as far
    /// right as scrolling goes, so the final page is a full one.
    fn lastStart(self: View, width: usize) usize {
        var used: usize = 0;
        var c = self.widths.len;
        while (c > 0) {
            const cost = self.widths[c - 1] + @as(usize, if (used > 0) 2 else 0);
            if (used + cost > width and c < self.widths.len) break;
            used += cost;
            c -= 1;
        }
        return c;
    }

    fn draw(self: *View, out: *std.Io.Writer, size: table.TermSize) !void {
        const body = if (size.rows > 4) size.rows - 4 else 1;
        const rows = self.g.kept();
        const ncols = self.colsFrom(self.col0, size.cols);

        try out.writeAll("\x1b[H");
        try self.header(out, ncols, .names);
        try self.header(out, ncols, .types);
        try self.header(out, ncols, .rule);
        var r = self.row0;
        var drawn: usize = 0;
        while (drawn < body) : (drawn += 1) {
            if (r < rows) {
                for (self.col0..self.col0 + ncols, 0..) |c, i| {
                    if (i > 0) try out.writeAll("  ");
                    if (self.g.cell(r, c)) |s| {
                        try table.alignedCell(out, s, self.widths[c], self.g.right[c]);
                    } else {
                        try out.writeAll("\x1b[2m");
                        try table.alignedCell(out, "NULL", self.widths[c], self.g.right[c]);
                        try out.writeAll("\x1b[0m");
                    }
                }
                r += 1;
            }
            try out.writeAll("\x1b[K\r\n");
        }
        const last_row = @min(self.row0 + body, rows);
        try out.print("\x1b[7m rows {d}-{d} of {d}", .{ @min(self.row0 + 1, rows), last_row, self.g.total_rows });
        if (rows < self.g.total_rows) try out.print(" (first {d} kept)", .{rows});
        try out.print(" \xc2\xb7 columns {d}-{d} of {d} \xc2\xb7 arrows scroll, Ctrl-arrows and PgUp/PgDn page, Home/End, g/G, q quits \x1b[K\x1b[0m", .{ self.col0 + 1, self.col0 + ncols, self.widths.len });
        try out.flush();
    }

    fn header(self: *View, out: *std.Io.Writer, ncols: usize, what: enum { names, types, rule }) !void {
        for (self.col0..self.col0 + ncols, 0..) |c, i| {
            if (i > 0) try out.writeAll("  ");
            switch (what) {
                .names => {
                    try out.writeAll("\x1b[1m");
                    try table.alignedCell(out, self.g.names[c], self.widths[c], self.g.right[c]);
                    try out.writeAll("\x1b[0m");
                },
                .types => {
                    try out.writeAll("\x1b[2m");
                    try table.alignedCell(out, self.g.types[c], self.widths[c], self.g.right[c]);
                    try out.writeAll("\x1b[0m");
                },
                .rule => try out.splatBytesAll("-", self.widths[c]),
            }
        }
        try out.writeAll("\x1b[K\r\n");
    }
};

/// Column widths over every kept row, so nothing shifts while scrolling.
fn measure(gpa: std.mem.Allocator, g: Grid) ![]usize {
    const widths = try gpa.alloc(usize, g.ncols());
    for (widths, 0..) |*w, c| {
        w.* = @max(table.displayWidth(g.names[c]), table.displayWidth(g.types[c]));
        for (0..g.kept()) |r| w.* = @max(w.*, table.displayWidth(g.cell(r, c) orelse "NULL"));
        w.* = @min(w.*, col_max);
    }
    return widths;
}

/// Where a move lands, clamped so the last screenful stays full. Split out so the
/// arithmetic is testable without a terminal.
pub fn clampScroll(pos: usize, delta: isize, total: usize, page: usize) usize {
    const max: usize = if (total > page) total - page else 0;
    const next: isize = @as(isize, @intCast(pos)) + delta;
    if (next <= 0) return 0;
    return @min(@as(usize, @intCast(next)), max);
}

/// Show `g` until the user leaves. Both ends must be terminals.
pub fn run(gpa: std.mem.Allocator, g: Grid) !void {
    if (g.ncols() == 0) return;
    const in_fd = std.fs.File.stdin().handle;
    const out_file = std.fs.File.stdout();

    const orig = try std.posix.tcgetattr(in_fd);
    var raw = orig;
    raw.lflag.ECHO = false;
    raw.lflag.ICANON = false;
    raw.lflag.ISIG = false;
    raw.lflag.IEXTEN = false;
    raw.iflag.IXON = false;
    raw.cc[@intFromEnum(std.posix.V.MIN)] = 1;
    raw.cc[@intFromEnum(std.posix.V.TIME)] = 0;
    try std.posix.tcsetattr(in_fd, .NOW, raw);
    defer std.posix.tcsetattr(in_fd, .NOW, orig) catch {};

    var buf: [16 * 1024]u8 = undefined;
    var fw = out_file.writer(&buf);
    const out = &fw.interface;
    try out.writeAll("\x1b[?1049h\x1b[?25l\x1b[2J");
    defer {
        out.writeAll("\x1b[?25h\x1b[?1049l") catch {};
        out.flush() catch {};
    }

    const widths = try measure(gpa, g);
    defer gpa.free(widths);
    var v = View{ .g = g, .widths = widths };

    while (true) {
        const size = table.termSize(out_file);
        const body = if (size.rows > 4) size.rows - 4 else 1;
        try v.draw(out, size);
        const page_cols: isize = @intCast(v.colsFrom(v.col0, size.cols));
        const page_rows: isize = @intCast(body);
        const col_end = v.lastStart(size.cols);
        v.col0 = @min(v.col0, col_end);
        switch (try line.readKey(in_fd)) {
            .nav => |nav| switch (nav.to) {
                .left => v.col0 = clampScroll(v.col0, -1, col_end + 1, 1),
                .right => v.col0 = clampScroll(v.col0, 1, col_end + 1, 1),
                .word_left => v.col0 = clampScroll(v.col0, -page_cols, col_end + 1, 1),
                .word_right => v.col0 = clampScroll(v.col0, page_cols, col_end + 1, 1),
                .up => v.row0 = clampScroll(v.row0, -1, g.kept(), body),
                .down => v.row0 = clampScroll(v.row0, 1, g.kept(), body),
                .home => v.col0 = 0,
                .end => v.col0 = col_end,
                .buf_home => v.row0 = 0,
                .buf_end => v.row0 = clampScroll(v.row0, std.math.maxInt(i32), g.kept(), body),
            },
            .enter => v.row0 = clampScroll(v.row0, 1, g.kept(), body),
            .page_up => v.row0 = clampScroll(v.row0, -page_rows, g.kept(), body),
            .page_down => v.row0 = clampScroll(v.row0, page_rows, g.kept(), body),
            .interrupt, .eof_or_delete => return,
            .char => |c| switch (c) {
                'q', 'Q' => return,
                'g' => v.row0 = 0,
                'G' => v.row0 = clampScroll(v.row0, std.math.maxInt(i32), g.kept(), body),
                'h' => v.col0 = clampScroll(v.col0, -1, col_end + 1, 1),
                'l' => v.col0 = clampScroll(v.col0, 1, col_end + 1, 1),
                'k' => v.row0 = clampScroll(v.row0, -1, g.kept(), body),
                'j' => v.row0 = clampScroll(v.row0, 1, g.kept(), body),
                ' ' => v.row0 = clampScroll(v.row0, page_rows, g.kept(), body),
                else => {},
            },
            else => {},
        }
    }
}

test "clampScroll stops at both ends and keeps the last page full" {
    try std.testing.expectEqual(@as(usize, 0), clampScroll(0, -1, 100, 10));
    try std.testing.expectEqual(@as(usize, 5), clampScroll(4, 1, 100, 10));
    try std.testing.expectEqual(@as(usize, 90), clampScroll(85, 10, 100, 10));
    try std.testing.expectEqual(@as(usize, 0), clampScroll(0, 5, 3, 10));
    try std.testing.expectEqual(@as(usize, 9), clampScroll(8, 5, 10, 1));
}
