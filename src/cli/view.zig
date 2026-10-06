//! `\view`: a full-screen look at the REPL's last result, for the tables a
//! terminal cannot show at once. Arrows move a column cursor and scroll by row,
//! the names and types stay pinned on top, and the status line always says where
//! you are. `s` sorts by the cursor's column, `/` filters it, as you type.
//! It draws on the alternate screen, so leaving it puts the session back as it was.
//!
//! Sorting and filtering rearrange the rows the REPL kept (the first
//! `TableWriter.keep_max` of a result) and never run the query again; when a
//! result was larger, the status line says the rows shown are of those kept.
//! Each column has its own filter and all of them apply. A filter is a substring
//! (any case), its negation (`!text`), or a comparison (`>= 100`, `< 2026-01-01`),
//! by value on a number column and as text on any other, which orders ISO dates
//! and times rightly. Columns are drawn up to `col_max` wide, wider than the
//! inline table, since a truncated value is the reason one came here.

const std = @import("std");
const table = @import("../connect/table.zig");
const types = @import("../lang/types.zig");
const line = @import("line.zig");

const Grid = table.Grid;
const palette = table.palette;

const col_max = 60;

pub const Dir = enum { asc, desc };

pub const Filter = struct {
    op: Op,
    text: []const u8,

    pub const Op = enum { contains, excludes, eq, ne, lt, le, gt, ge };

    pub fn parse(src: []const u8) ?Filter {
        const t = std.mem.trim(u8, src, " ");
        if (t.len == 0) return null;
        const ops = [_]struct { s: []const u8, op: Op }{
            .{ .s = ">=", .op = .ge }, .{ .s = "<=", .op = .le }, .{ .s = "!=", .op = .ne }, .{ .s = "<>", .op = .ne },
            .{ .s = ">", .op = .gt },  .{ .s = "<", .op = .lt },  .{ .s = "=", .op = .eq },  .{ .s = "!", .op = .excludes },
        };
        for (ops) |o| if (std.mem.startsWith(u8, t, o.s)) {
            const rest = std.mem.trim(u8, t[o.s.len..], " ");
            if (rest.len == 0) return null;
            return .{ .op = o.op, .text = rest };
        };
        return .{ .op = .contains, .text = t };
    }

    /// Does `cell` pass? A NULL passes only `!text`, since it contains nothing.
    pub fn keeps(self: Filter, kind: ?types.TypeKind, cell: ?[]const u8) bool {
        const c = cell orelse return self.op == .excludes;
        switch (self.op) {
            .contains => return std.ascii.indexOfIgnoreCase(c, self.text) != null,
            .excludes => return std.ascii.indexOfIgnoreCase(c, self.text) == null,
            else => {},
        }
        const ord = order(kind, c, self.text);
        return switch (self.op) {
            .eq => ord == .eq,
            .ne => ord != .eq,
            .lt => ord == .lt,
            .le => ord != .gt,
            .gt => ord == .gt,
            .ge => ord != .lt,
            .contains, .excludes => unreachable,
        };
    }
};

fn numeric(kind: ?types.TypeKind) bool {
    return if (kind) |k| k.isNumeric() else false;
}

/// Two values of a column, in its type's order: numbers by value (non-numbers
/// after them), false before true, the rest as text, case aside first.
pub fn order(kind: ?types.TypeKind, a: []const u8, b: []const u8) std.math.Order {
    if (numeric(kind)) {
        const x = std.fmt.parseFloat(f64, a) catch null;
        const y = std.fmt.parseFloat(f64, b) catch null;
        if (x != null and y != null) return std.math.order(x.?, y.?);
        if (x != null) return .lt;
        if (y != null) return .gt;
    }
    if (kind == .bool) return std.math.order(@intFromBool(std.mem.eql(u8, a, "true")), @intFromBool(std.mem.eql(u8, b, "true")));
    const ci = std.ascii.orderIgnoreCase(a, b);
    return if (ci != .eq) ci else std.mem.order(u8, a, b);
}

pub const ColFilter = struct { col: usize, f: Filter };

/// The kept rows to show: those every filter keeps, sorted stably by the sort
/// column with NULLs last whichever way it runs. gpa-owned.
pub fn arrange(gpa: std.mem.Allocator, g: Grid, kinds: []const ?types.TypeKind, sort: ?struct { col: usize, dir: Dir }, filters: []const ColFilter) ![]usize {
    var rows = std.array_list.Managed(usize).init(gpa);
    errdefer rows.deinit();
    rows: for (0..g.kept()) |r| {
        for (filters) |fl| if (!fl.f.keeps(kinds[fl.col], g.cell(r, fl.col))) continue :rows;
        try rows.append(r);
    }
    if (sort) |so| {
        const Ctx = struct {
            g: Grid,
            col: usize,
            kind: ?types.TypeKind,
            desc: bool,
            fn less(cx: @This(), x: usize, y: usize) bool {
                const a = cx.g.cell(x, cx.col);
                const b = cx.g.cell(y, cx.col);
                if (a == null or b == null) return a != null and b == null;
                const o = order(cx.kind, a.?, b.?);
                return if (cx.desc) o == .gt else o == .lt;
            }
        };
        std.sort.block(usize, rows.items, Ctx{ .g = g, .col = so.col, .kind = kinds[so.col], .desc = so.dir == .desc }, Ctx.less);
    }
    return rows.toOwnedSlice();
}

const View = struct {
    gpa: std.mem.Allocator,
    g: Grid,
    widths: []usize,
    kinds: []?types.TypeKind,
    color: bool,
    rows: []usize,
    row0: usize = 0,
    col0: usize = 0,
    cur: usize = 0,
    sort_col: ?usize = null,
    sort_dir: Dir = .asc,
    filters: []std.array_list.Managed(u8),
    typing: bool = false,

    fn filterOf(self: *const View, c: usize) ?Filter {
        return Filter.parse(self.filters[c].items);
    }

    fn anyFilter(self: *const View) bool {
        for (0..self.filters.len) |c| if (self.filterOf(c) != null) return true;
        return false;
    }

    fn rearrange(self: *View) !void {
        var fs = std.array_list.Managed(ColFilter).init(self.gpa);
        defer fs.deinit();
        for (0..self.filters.len) |c| if (self.filterOf(c)) |f| try fs.append(.{ .col = c, .f = f });
        const rows = try arrange(
            self.gpa,
            self.g,
            self.kinds,
            if (self.sort_col) |c| .{ .col = c, .dir = self.sort_dir } else null,
            fs.items,
        );
        self.gpa.free(self.rows);
        self.rows = rows;
        self.row0 = 0;
    }

    /// `s`: off, ascending, descending, off on the cursor's column; another
    /// column starts ascending.
    fn cycleSort(self: *View) !void {
        if (self.sort_col != self.cur) {
            self.sort_col = self.cur;
            self.sort_dir = .asc;
        } else if (self.sort_dir == .asc) {
            self.sort_dir = .desc;
        } else self.sort_col = null;
        try self.rearrange();
    }

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

    fn follow(self: *View, width: usize) void {
        if (self.cur < self.col0) self.col0 = self.cur;
        while (self.cur >= self.col0 + self.colsFrom(self.col0, width)) self.col0 += 1;
    }

    fn style(self: *const View, s: []const u8) []const u8 {
        return if (self.color) s else "";
    }

    fn draw(self: *View, out: *std.Io.Writer, size: table.TermSize) !void {
        const body = if (size.rows > 4) size.rows - 4 else 1;
        const ncols = self.colsFrom(self.col0, size.cols);

        try out.writeAll("\x1b[H");
        try self.header(out, ncols, .names);
        try self.header(out, ncols, .types);
        try self.header(out, ncols, .rule);
        var i = self.row0;
        var drawn: usize = 0;
        while (drawn < body) : (drawn += 1) {
            if (i < self.rows.len) {
                const r = self.rows[i];
                for (self.col0..self.col0 + ncols, 0..) |c, k| {
                    if (k > 0) try out.writeAll("  ");
                    const at = c == self.cur;
                    if (at) try out.writeAll(self.style("\x1b[1m"));
                    if (self.g.cell(r, c)) |s| {
                        try out.writeAll(self.style(palette.value(self.kinds[c], s)));
                        try table.alignedCell(out, s, self.widths[c], self.g.right[c]);
                    } else {
                        try out.writeAll(self.style(palette.null_));
                        try table.alignedCell(out, "NULL", self.widths[c], self.g.right[c]);
                    }
                    try out.writeAll(self.style(palette.reset));
                }
                i += 1;
            }
            try out.writeAll("\x1b[K\r\n");
        }
        try self.status(out, size, ncols, body);
        try out.flush();
    }

    fn header(self: *View, out: *std.Io.Writer, ncols: usize, what: enum { names, types, rule }) !void {
        for (self.col0..self.col0 + ncols, 0..) |c, k| {
            if (k > 0) try out.writeAll("  ");
            switch (what) {
                .names => {
                    var buf: [256]u8 = undefined;
                    const arrow: []const u8 = if (self.sort_col == c) (if (self.sort_dir == .asc) " \xe2\x86\x91" else " \xe2\x86\x93") else "";
                    const mark: []const u8 = if (self.filterOf(c) != null) " \xe2\x89\x88" else "";
                    const label = std.fmt.bufPrint(&buf, "{s}{s}{s}", .{ self.g.names[c], arrow, mark }) catch self.g.names[c];
                    try out.writeAll(if (c == self.cur) "\x1b[7m" else self.style(palette.name));
                    try table.alignedCell(out, label, self.widths[c], self.g.right[c]);
                    try out.writeAll("\x1b[0m");
                },
                .types => {
                    try out.writeAll(self.style(palette.type_label));
                    try table.alignedCell(out, self.g.types[c], self.widths[c], self.g.right[c]);
                    try out.writeAll(self.style(palette.reset));
                },
                .rule => {
                    try out.writeAll(self.style(palette.rule));
                    try out.splatBytesAll(if (c == self.cur) "=" else "-", self.widths[c]);
                    try out.writeAll(self.style(palette.reset));
                },
            }
        }
        try out.writeAll("\x1b[K\r\n");
    }

    /// The bottom line, in reverse video and cut to the terminal's width, since a
    /// wrapped one would scroll the screen.
    fn status(self: *View, out: *std.Io.Writer, size: table.TermSize, ncols: usize, body: usize) !void {
        var buf: [1024]u8 = undefined;
        var w = std.Io.Writer.fixed(&buf);
        if (self.typing) {
            w.print(" filter {s}: {s}\xe2\x96\x8f   text \xc2\xb7 !text \xc2\xb7 >= 100 \xc2\xb7 enter keeps \xc2\xb7 esc drops", .{ self.g.names[self.cur], self.filters[self.cur].items }) catch {};
        } else {
            const kept = self.g.kept();
            const shown = self.rows.len;
            const last_row = @min(self.row0 + body, shown);
            w.print(" rows {d}-{d} of {d}", .{ @min(self.row0 + 1, shown), last_row, shown }) catch {};
            if (shown < kept) w.print(" matching, of {d}", .{kept}) catch {};
            if (kept < self.g.total_rows) w.print(" (the first {d} of {d} kept)", .{ kept, self.g.total_rows }) catch {};
            if (self.sort_col) |c| w.print(" \xc2\xb7 by {s} {s}", .{ self.g.names[c], if (self.sort_dir == .asc) "\xe2\x86\x91" else "\xe2\x86\x93" }) catch {};
            for (0..self.filters.len) |c| if (self.filterOf(c) != null) {
                w.print(" \xc2\xb7 {s}: {s}", .{ self.g.names[c], std.mem.trim(u8, self.filters[c].items, " ") }) catch {};
            };
            w.print(" \xc2\xb7 col {d}/{d} ({d} shown) \xc2\xb7 \xe2\x86\x90\xe2\x86\x92 column \xc2\xb7 s sort \xc2\xb7 / filter \xc2\xb7 esc clears \xc2\xb7 q quits", .{ self.cur + 1, self.widths.len, ncols }) catch {};
        }
        try out.writeAll("\x1b[7m");
        try cutTo(out, w.buffered(), size.cols -| 1);
        try out.writeAll("\x1b[K\x1b[0m");
    }
};

fn cutTo(out: *std.Io.Writer, s: []const u8, cols: usize) !void {
    var n: usize = 0;
    var i: usize = 0;
    while (i < s.len and n < cols) {
        var j = i + 1;
        while (j < s.len and s[j] & 0xC0 == 0x80) j += 1;
        try out.writeAll(s[i..j]);
        n += 1;
        i = j;
    }
}

/// Column widths over every kept row, so nothing shifts while scrolling or
/// sorting, with room after the name for a sort arrow and the filter mark.
fn measure(gpa: std.mem.Allocator, g: Grid) ![]usize {
    const widths = try gpa.alloc(usize, g.ncols());
    for (widths, 0..) |*w, c| {
        w.* = @max(table.displayWidth(g.names[c]) + 4, table.displayWidth(g.types[c]));
        for (0..g.kept()) |r| w.* = @max(w.*, table.displayWidth(g.cell(r, c) orelse "NULL"));
        w.* = @min(w.*, col_max);
    }
    return widths;
}

/// Where a move lands, clamped so the last screenful stays full.
pub fn clampScroll(pos: usize, delta: isize, total: usize, page: usize) usize {
    const max: usize = if (total > page) total - page else 0;
    const next: isize = @as(isize, @intCast(pos)) + delta;
    if (next <= 0) return 0;
    return @min(@as(usize, @intCast(next)), max);
}

/// Shows `g` until the user leaves; both ends must be terminals. Esc restores
/// the filters first, then the sort; Backspace removes a whole UTF-8 character.
pub fn run(gpa: std.mem.Allocator, g: Grid) !void {
    if (g.ncols() == 0) return;
    const in_fd = std.fs.File.stdin().handle;
    const out_file = std.fs.File.stdout();

    const orig = try line.enterRaw(in_fd);
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
    const kinds = try gpa.alloc(?types.TypeKind, g.ncols());
    defer gpa.free(kinds);
    for (kinds, g.types) |*k, t| k.* = table.kindOf(t);
    const filters = try gpa.alloc(std.array_list.Managed(u8), g.ncols());
    defer gpa.free(filters);
    for (filters) |*f| f.* = std.array_list.Managed(u8).init(gpa);
    defer for (filters) |*f| f.deinit();
    var v = View{
        .gpa = gpa,
        .g = g,
        .widths = widths,
        .kinds = kinds,
        .color = !std.process.hasEnvVarConstant("NO_COLOR"),
        .rows = &.{},
        .filters = filters,
    };
    defer gpa.free(v.rows);
    try v.rearrange();
    var before = std.array_list.Managed(u8).init(gpa);
    defer before.deinit();

    while (true) {
        const size = table.termSize(out_file);
        const body = if (size.rows > 4) size.rows - 4 else 1;
        v.follow(size.cols);
        try v.draw(out, size);
        const page_rows: isize = @intCast(body);
        const page_cols = v.colsFrom(v.col0, size.cols);
        const n = v.rows.len;
        const key = try line.readKey(in_fd);

        if (v.typing) {
            const ft = &v.filters[v.cur];
            switch (key) {
                .enter, .ctrl_enter => v.typing = false,
                .escape, .interrupt => {
                    v.typing = false;
                    ft.clearRetainingCapacity();
                    try ft.appendSlice(before.items);
                    try v.rearrange();
                },
                .backspace => if (ft.items.len > 0) {
                    var cut = ft.items.len - 1;
                    while (cut > 0 and ft.items[cut] & 0xC0 == 0x80) cut -= 1;
                    ft.shrinkRetainingCapacity(cut);
                    try v.rearrange();
                },
                .kill_line, .word_back => {
                    ft.clearRetainingCapacity();
                    try v.rearrange();
                },
                .char => |c| {
                    try ft.append(c);
                    try v.rearrange();
                },
                .paste_begin => {
                    const text = try line.readPasted(gpa, in_fd);
                    defer gpa.free(text);
                    for (text) |c| if (c >= 0x20) try ft.append(c);
                    try v.rearrange();
                },
                else => {},
            }
            continue;
        }

        switch (key) {
            .nav => |nav| switch (nav.to) {
                .left => v.cur -|= 1,
                .right => v.cur = @min(v.cur + 1, v.widths.len - 1),
                .word_left => v.cur -|= page_cols,
                .word_right => v.cur = @min(v.cur + page_cols, v.widths.len - 1),
                .up => v.row0 = clampScroll(v.row0, -1, n, body),
                .down => v.row0 = clampScroll(v.row0, 1, n, body),
                .home => v.cur = 0,
                .end => v.cur = v.widths.len - 1,
                .buf_home => v.row0 = 0,
                .buf_end => v.row0 = clampScroll(v.row0, std.math.maxInt(i32), n, body),
            },
            .enter => v.row0 = clampScroll(v.row0, 1, n, body),
            .page_up => v.row0 = clampScroll(v.row0, -page_rows, n, body),
            .page_down => v.row0 = clampScroll(v.row0, page_rows, n, body),
            .interrupt, .eof_or_delete => return,
            .escape => if (v.anyFilter()) {
                for (v.filters) |*f| f.clearRetainingCapacity();
                try v.rearrange();
            } else if (v.sort_col != null) {
                v.sort_col = null;
                try v.rearrange();
            },
            .char => |c| switch (c) {
                'q', 'Q' => return,
                'g' => v.row0 = 0,
                'G' => v.row0 = clampScroll(v.row0, std.math.maxInt(i32), n, body),
                'h' => v.cur -|= 1,
                'l' => v.cur = @min(v.cur + 1, v.widths.len - 1),
                'k' => v.row0 = clampScroll(v.row0, -1, n, body),
                'j' => v.row0 = clampScroll(v.row0, 1, n, body),
                ' ' => v.row0 = clampScroll(v.row0, page_rows, n, body),
                's', 'S' => try v.cycleSort(),
                '/' => {
                    before.clearRetainingCapacity();
                    try before.appendSlice(v.filters[v.cur].items);
                    v.typing = true;
                },
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

fn testGrid() Grid {
    const S = struct {
        const names = [_][]const u8{ "uf", "valor", "dia" };
        const tys = [_][]const u8{ "string?", "decimal(10,2)?", "date?" };
        const right = [_]bool{ false, true, false };
        const cells = [_]?[]const u8{
            "SP", "10.50",  "2026-03-01",
            "rj", "-2.00",  "2026-01-15",
            "sp", null,     "2026-02-10",
            "MG", "100.00", null,
            "Sp", "9.99",   "2025-12-31",
        };
    };
    return .{ .names = &S.names, .types = &S.tys, .right = &S.right, .cells = &S.cells, .total_rows = 5 };
}

const test_kinds = [_]?types.TypeKind{ .string, .decimal, .date };

test "view: a number column sorts by value, NULLs last either way, and stably" {
    const gpa = std.testing.allocator;
    const g = testGrid();
    const up = try arrange(gpa, g, &test_kinds, .{ .col = 1, .dir = .asc }, &.{});
    defer gpa.free(up);
    try std.testing.expectEqualSlices(usize, &.{ 1, 4, 0, 3, 2 }, up);
    const down = try arrange(gpa, g, &test_kinds, .{ .col = 1, .dir = .desc }, &.{});
    defer gpa.free(down);
    try std.testing.expectEqualSlices(usize, &.{ 3, 0, 4, 1, 2 }, down);
    const uf = try arrange(gpa, g, &test_kinds, .{ .col = 0, .dir = .asc }, &.{});
    defer gpa.free(uf);
    try std.testing.expectEqualSlices(usize, &.{ 3, 1, 0, 4, 2 }, uf);
    const dia = try arrange(gpa, g, &test_kinds, .{ .col = 2, .dir = .asc }, &.{});
    defer gpa.free(dia);
    try std.testing.expectEqualSlices(usize, &.{ 4, 1, 2, 0, 3 }, dia);
}

test "view: a filter keeps a substring in any case, excludes with !, compares by type" {
    const gpa = std.testing.allocator;
    const g = testGrid();
    const sp = try arrange(gpa, g, &test_kinds, null, &.{.{ .col = 0, .f = Filter.parse("sp").? }});
    defer gpa.free(sp);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2, 4 }, sp);
    const not_sp = try arrange(gpa, g, &test_kinds, null, &.{.{ .col = 0, .f = Filter.parse("!sp").? }});
    defer gpa.free(not_sp);
    try std.testing.expectEqualSlices(usize, &.{ 1, 3 }, not_sp);
    const big = try arrange(gpa, g, &test_kinds, .{ .col = 1, .dir = .desc }, &.{.{ .col = 1, .f = Filter.parse(">= 9.5").? }});
    defer gpa.free(big);
    try std.testing.expectEqualSlices(usize, &.{ 3, 0, 4 }, big);
    const y2026 = try arrange(gpa, g, &test_kinds, null, &.{.{ .col = 2, .f = Filter.parse(">=2026-01-01").? }});
    defer gpa.free(y2026);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1, 2 }, y2026);
    const both = try arrange(gpa, g, &test_kinds, null, &.{ .{ .col = 0, .f = Filter.parse("sp").? }, .{ .col = 1, .f = Filter.parse("> 10").? } });
    defer gpa.free(both);
    try std.testing.expectEqualSlices(usize, &.{0}, both);
    try std.testing.expect(Filter.parse("  ") == null);
    try std.testing.expect(Filter.parse(">=") == null);
}

test "view: the status line is cut to the terminal and says what was kept" {
    const gpa = std.testing.allocator;
    var g = testGrid();
    g.total_rows = 2_000_000;
    var widths = [_]usize{ 6, 13, 10 };
    var kinds = test_kinds;
    var filters: [3]std.array_list.Managed(u8) = undefined;
    for (&filters) |*f| f.* = std.array_list.Managed(u8).init(gpa);
    defer for (&filters) |*f| f.deinit();
    var v = View{ .gpa = gpa, .g = g, .widths = &widths, .kinds = &kinds, .color = false, .rows = &.{}, .filters = &filters };
    defer gpa.free(v.rows);
    try v.rearrange();
    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();
    try v.status(&out.writer, .{ .cols = 300, .rows = 24 }, 3, 20);
    try std.testing.expect(std.mem.indexOf(u8, out.written(), "the first 5 of 2000000 kept") != null);
    var narrow = std.Io.Writer.Allocating.init(gpa);
    defer narrow.deinit();
    try v.status(&narrow.writer, .{ .cols = 30, .rows = 24 }, 3, 20);
    const text = narrow.written()["\x1b[7m".len .. narrow.written().len - "\x1b[K\x1b[0m".len];
    try std.testing.expect(std.unicode.utf8CountCodepoints(text) catch 0 <= 29);
}
