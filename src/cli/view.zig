//! `\view`: a full-screen look at the REPL's last result, for the tables a
//! terminal cannot show at once. Arrows move a column cursor and scroll by row,
//! the names and types stay pinned on top, and the status line always says where
//! you are. `s` sorts by the cursor's column, `/` filters it, as you type, and
//! `f` finds text across every column once Enter is pressed: the rows narrow to
//! those holding it and each match is highlighted where it shows. `a` makes the
//! find and the filters ignore accents as well as case, as `search(…, true)` does. It draws on the
//! alternate screen, so leaving it puts the session back as it was.
//!
//! Sorting and filtering rearrange the rows the REPL kept (the first
//! `TableWriter.keep_max` of a result) and never run the query again; when a
//! result was larger, the status line says the rows shown are of those kept.
//! Each column has its own filter and all of them apply. A filter is a substring
//! (any case), its negation (`!text`), or a comparison (`>= 100`, `< 2026-01-01`),
//! by value on a number column and as text on any other, which orders ISO dates
//! and times rightly. Columns are drawn up to `col_max` wide, wider than the
//! inline table, since a truncated value is the reason one came here.
//!
//! A find is a row search: words are AND-ed and each may be in any
//! column, `-word` keeps the rows without it, `col:word` looks in that column
//! only (a name that is no column leaves the whole token as text), and
//! `"two words"` is one phrase; all in any case, a NULL holding nothing.

const std = @import("std");
const table = @import("../connect/table.zig");
const types = @import("../lang/types.zig");
const line = @import("line.zig");
const search = @import("../exec/search.zig");

const Grid = table.Grid;
const palette = table.palette;

const col_max = 60;

pub const Dir = enum { asc, desc };

pub const Filter = struct {
    op: Op,
    text: []const u8,
    fold: search.Fold = .case,

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
            .contains => return search.findFold(c, self.text, 0, self.fold) != null,
            .excludes => return search.findFold(c, self.text, 0, self.fold) == null,
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

/// `f`'s search over the whole row, the engine's `search` rules (`search.zig`)
/// bound to the grid's column names.
pub const Find = struct {
    query: search.Query = .{},
    terms: []const search.Bound = &.{},

    pub fn parse(gpa: std.mem.Allocator, src: []const u8, names: []const []const u8) !Find {
        return parseFold(gpa, src, names, .case);
    }

    pub fn parseFold(gpa: std.mem.Allocator, src: []const u8, names: []const []const u8, fold: search.Fold) !Find {
        var q = try search.Query.parse(gpa, src);
        q.fold = fold;
        errdefer q.deinit(gpa);
        return .{ .query = q, .terms = try q.bind(gpa, names) };
    }

    pub fn deinit(self: *Find, gpa: std.mem.Allocator) void {
        gpa.free(self.terms);
        self.query.deinit(gpa);
        self.terms = &.{};
    }

    pub fn active(self: Find) bool {
        return self.terms.len > 0;
    }

    pub fn keeps(self: Find, g: Grid, r: usize) bool {
        const Row = struct {
            g: Grid,
            r: usize,
            fn at(cx: @This(), c: usize) ?[]const u8 {
                return cx.g.cell(cx.r, c);
            }
        };
        return search.matches(self.terms, g.ncols(), Row{ .g = g, .r = r }, Row.at);
    }

    pub fn needles(self: Find, col: usize, buf: [][]const u8) [][]const u8 {
        return search.needles(self.terms, col, buf);
    }
};

/// `s` drawn as `table.alignedCell` draws it, with every case-insensitive
/// occurrence of a needle in `hit` style and the rest in `base`.
pub fn markedCell(out: *std.Io.Writer, s: []const u8, width: usize, right: bool, needles: []const []const u8, fold: search.Fold, base: []const u8, hit: []const u8) !void {
    if (needles.len == 0) {
        try out.writeAll(base);
        return table.alignedCell(out, s, width, right);
    }
    var marks_buf: [col_max * 4]bool = undefined;
    const marks = marks_buf[0..@min(s.len, marks_buf.len)];
    @memset(marks, false);
    for (needles) |nd| {
        if (nd.len == 0) continue;
        var from: usize = 0;
        while (from < s.len) {
            const sp = search.findFold(s, nd, from, fold) orelse break;
            for (sp.start..@min(sp.end, marks.len)) |k| marks[k] = true;
            from = sp.end;
        }
    }
    const w = table.displayWidth(s);
    const pad = width - @min(w, width);
    try out.writeAll(base);
    if (right) try out.splatBytesAll(" ", pad);
    const limit = if (w > width) width -| 1 else width;
    var cols: usize = 0;
    var i: usize = 0;
    var in_hit = false;
    while (i < s.len and cols < limit) {
        var j = i + 1;
        while (j < s.len and s[j] & 0xC0 == 0x80) j += 1;
        const m = i < marks.len and marks[i];
        if (m != in_hit) {
            try out.writeAll(if (m) hit else "\x1b[0m");
            if (!m) try out.writeAll(base);
            in_hit = m;
        }
        if (s[i] < 0x20 or s[i] == 0x7f) try out.writeByte(' ') else try out.writeAll(s[i..j]);
        cols += 1;
        i = j;
    }
    if (in_hit) {
        try out.writeAll("\x1b[0m");
        try out.writeAll(base);
    }
    if (w > width and width > 0) try out.writeAll("…");
    if (!right) try out.splatBytesAll(" ", pad);
}

pub const ColFilter = struct { col: usize, f: Filter };

/// The kept rows to show: those every filter keeps, sorted stably by the sort
/// column with NULLs last whichever way it runs. gpa-owned.
pub fn arrange(gpa: std.mem.Allocator, g: Grid, kinds: []const ?types.TypeKind, sort: ?Sort, filters: []const ColFilter) ![]usize {
    return arrangeWith(gpa, g, kinds, sort, filters, .{});
}

pub const Sort = struct { col: usize, dir: Dir };

/// `arrange`, keeping only the rows `find` holds as well.
pub fn arrangeWith(gpa: std.mem.Allocator, g: Grid, kinds: []const ?types.TypeKind, sort: ?Sort, filters: []const ColFilter, find: Find) ![]usize {
    var rows = std.array_list.Managed(usize).init(gpa);
    errdefer rows.deinit();
    rows: for (0..g.kept()) |r| {
        for (filters) |fl| if (!fl.f.keeps(kinds[fl.col], g.cell(r, fl.col))) continue :rows;
        if (!find.keeps(g, r)) continue;
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
    finding: bool = false,
    whole: bool = false,
    rerun: bool = false,
    accents: bool = false,
    find_query: std.array_list.Managed(u8) = undefined,
    find_text: []const u8 = "",
    find: Find = .{},

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
        for (0..self.filters.len) |c| if (self.filterOf(c)) |f| {
            var ff = f;
            ff.fold = self.fold();
            try fs.append(.{ .col = c, .f = ff });
        };
        const rows = try arrangeWith(
            self.gpa,
            self.g,
            self.kinds,
            if (self.sort_col) |c| .{ .col = c, .dir = self.sort_dir } else null,
            fs.items,
            self.find,
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

    fn fold(self: *const View) search.Fold {
        return if (self.accents) .accents else .case;
    }

    /// `a`: accents ignored or not, by the find and the filters alike.
    fn toggleAccents(self: *View) !void {
        self.accents = !self.accents;
        try self.setFind(self.find_text);
    }

    fn style(self: *const View, s: []const u8) []const u8 {
        return if (self.color) s else "";
    }

    /// Replaces the find with `text` (copied) and rearranges.
    fn setFind(self: *View, text: []const u8) !void {
        const owned = try self.gpa.dupe(u8, std.mem.trim(u8, text, " "));
        errdefer self.gpa.free(owned);
        var f = try Find.parseFold(self.gpa, owned, self.g.names, self.fold());
        errdefer f.deinit(self.gpa);
        self.find.deinit(self.gpa);
        self.gpa.free(self.find_text);
        self.find = f;
        self.find_text = owned;
        try self.rearrange();
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
                        var nb: [8][]const u8 = undefined;
                        var base_buf: [32]u8 = undefined;
                        const base = std.fmt.bufPrint(&base_buf, "{s}{s}", .{ if (at) self.style("\x1b[1m") else "", self.style(palette.value(self.kinds[c], s)) }) catch "";
                        try markedCell(out, s, self.widths[c], self.g.right[c], self.find.needles(c, &nb), self.fold(), base, if (self.color) "\x1b[30;43m" else "\x1b[7m");
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
        } else if (self.finding) {
            const what: []const u8 = if (self.whole) "find in the whole result (runs the query again)" else "find";
            w.print(" {s}: {s}\xe2\x96\x8f   words AND-ed \xc2\xb7 -word \xc2\xb7 col:word \xc2\xb7 \"two words\" \xc2\xb7 enter finds \xc2\xb7 esc drops", .{ what, self.find_query.items }) catch {};
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
            if (self.find.active()) w.print(" \xc2\xb7 find: {s}", .{self.find_text}) catch {};
            if (self.accents) w.writeAll(" \xc2\xb7 accents ignored") catch {};
            w.print(" \xc2\xb7 col {d}/{d} ({d} shown) \xc2\xb7 \xe2\x86\x90\xe2\x86\x92 column \xc2\xb7 s sort \xc2\xb7 / filter \xc2\xb7 f find{s} \xc2\xb7 a accents \xc2\xb7 esc clears \xc2\xb7 q quits", .{ self.cur + 1, self.widths.len, ncols, if (self.rerun) " (F: whole result)" else "" }) catch {};
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

/// What the user left the view for: nothing, or `F`'s search to run over the
/// whole result (`text` gpa-owned), which only the REPL can do.
pub const Exit = union(enum) {
    quit,
    rerun: struct { text: []u8, accents: bool },
};

/// Shows `g` until the user leaves; both ends must be terminals. Esc clears the
/// find first, then the filters, then the sort; Backspace removes a whole UTF-8
/// character. `rerun` says whether `F` is offered.
pub fn run(gpa: std.mem.Allocator, g: Grid, rerun: bool) !Exit {
    if (g.ncols() == 0) return .quit;
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
        .rerun = rerun,
    };
    defer gpa.free(v.rows);
    v.find_query = std.array_list.Managed(u8).init(gpa);
    defer v.find_query.deinit();
    defer v.find.deinit(gpa);
    defer gpa.free(v.find_text);
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

        if (v.finding) {
            const q = &v.find_query;
            switch (key) {
                .enter, .ctrl_enter => {
                    v.finding = false;
                    if (v.whole) {
                        const text = std.mem.trim(u8, q.items, " ");
                        if (text.len > 0) return .{ .rerun = .{ .text = try gpa.dupe(u8, text), .accents = v.accents } };
                    } else try v.setFind(q.items);
                },
                .escape, .interrupt => v.finding = false,
                .backspace => if (q.items.len > 0) {
                    var cut = q.items.len - 1;
                    while (cut > 0 and q.items[cut] & 0xC0 == 0x80) cut -= 1;
                    q.shrinkRetainingCapacity(cut);
                },
                .kill_line, .word_back => q.clearRetainingCapacity(),
                .char => |c| try q.append(c),
                .paste_begin => {
                    const text = try line.readPasted(gpa, in_fd);
                    defer gpa.free(text);
                    for (text) |c| if (c >= 0x20) try q.append(c);
                },
                else => {},
            }
            continue;
        }

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
            .interrupt, .eof_or_delete => return .quit,
            .escape => if (v.find.active()) {
                try v.setFind("");
            } else if (v.anyFilter()) {
                for (v.filters) |*f| f.clearRetainingCapacity();
                try v.rearrange();
            } else if (v.sort_col != null) {
                v.sort_col = null;
                try v.rearrange();
            },
            .char => |c| switch (c) {
                'q', 'Q' => return .quit,
                'g' => v.row0 = 0,
                'G' => v.row0 = clampScroll(v.row0, std.math.maxInt(i32), n, body),
                'h' => v.cur -|= 1,
                'l' => v.cur = @min(v.cur + 1, v.widths.len - 1),
                'k' => v.row0 = clampScroll(v.row0, -1, n, body),
                'j' => v.row0 = clampScroll(v.row0, 1, n, body),
                ' ' => v.row0 = clampScroll(v.row0, page_rows, n, body),
                's', 'S' => try v.cycleSort(),
                'a', 'A' => try v.toggleAccents(),
                '/' => {
                    before.clearRetainingCapacity();
                    try before.appendSlice(v.filters[v.cur].items);
                    v.typing = true;
                },
                'f' => {
                    v.find_query.clearRetainingCapacity();
                    try v.find_query.appendSlice(v.find_text);
                    v.whole = false;
                    v.finding = true;
                },
                'F' => if (rerun) {
                    v.find_query.clearRetainingCapacity();
                    try v.find_query.appendSlice(v.find_text);
                    v.whole = true;
                    v.finding = true;
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

test "view: a find parses words, exclusions, a column and a phrase" {
    const gpa = std.testing.allocator;
    const names = [_][]const u8{ "uf", "valor", "dia" };
    var f = try Find.parse(gpa, "sp -2026-03 UF:rj \"two words\" nope:x", &names);
    defer f.deinit(gpa);
    try std.testing.expectEqual(@as(usize, 5), f.terms.len);
    try std.testing.expectEqualStrings("sp", f.terms[0].text);
    try std.testing.expect(f.terms[0].col == null and !f.terms[0].not);
    try std.testing.expectEqualStrings("2026-03", f.terms[1].text);
    try std.testing.expect(f.terms[1].not);
    try std.testing.expectEqual(@as(?usize, 0), f.terms[2].col);
    try std.testing.expectEqualStrings("rj", f.terms[2].text);
    try std.testing.expectEqualStrings("two words", f.terms[3].text);
    try std.testing.expect(f.terms[4].col == null);
    try std.testing.expectEqualStrings("nope:x", f.terms[4].text);
    var empty = try Find.parse(gpa, "   ", &names);
    defer empty.deinit(gpa);
    try std.testing.expect(!empty.active());
}

test "view: a find keeps the rows holding every word in any column, in any case" {
    const gpa = std.testing.allocator;
    const g = testGrid();
    var sp = try Find.parse(gpa, "sp", g.names);
    defer sp.deinit(gpa);
    const a = try arrangeWith(gpa, g, &test_kinds, null, &.{}, sp);
    defer gpa.free(a);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2, 4 }, a);

    var across = try Find.parse(gpa, "sp 2026", g.names);
    defer across.deinit(gpa);
    const b = try arrangeWith(gpa, g, &test_kinds, null, &.{}, across);
    defer gpa.free(b);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2 }, b);

    var not = try Find.parse(gpa, "-sp", g.names);
    defer not.deinit(gpa);
    const c = try arrangeWith(gpa, g, &test_kinds, null, &.{}, not);
    defer gpa.free(c);
    try std.testing.expectEqualSlices(usize, &.{ 1, 3 }, c);

    var col = try Find.parse(gpa, "dia:12", g.names);
    defer col.deinit(gpa);
    const d = try arrangeWith(gpa, g, &test_kinds, null, &.{}, col);
    defer gpa.free(d);
    try std.testing.expectEqualSlices(usize, &.{4}, d);

    var anywhere = try Find.parse(gpa, "01", g.names);
    defer anywhere.deinit(gpa);
    const e = try arrangeWith(gpa, g, &test_kinds, .{ .col = 1, .dir = .desc }, &.{.{ .col = 0, .f = Filter.parse("!mg").? }}, anywhere);
    defer gpa.free(e);
    try std.testing.expectEqualSlices(usize, &.{ 0, 1 }, e);
}

test "view: a found text is marked in any case inside the cell, the padding kept" {
    const gpa = std.testing.allocator;
    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();
    try markedCell(&out.writer, "Sao Paulo SP", 14, false, &.{"sp"}, .case, "", "[");
    try std.testing.expectEqualStrings("Sao Paulo [SP\x1b[0m  ", out.written());

    var two = std.Io.Writer.Allocating.init(gpa);
    defer two.deinit();
    try markedCell(&two.writer, "abcabc", 8, true, &.{ "b", "C" }, .case, "<", "[");
    try std.testing.expectEqualStrings("<  a[bc\x1b[0m<a[bc\x1b[0m<", two.written());

    var cut = std.Io.Writer.Allocating.init(gpa);
    defer cut.deinit();
    try markedCell(&cut.writer, "xxxxxxxxmatch", 6, false, &.{"match"}, .case, "", "[");
    try std.testing.expectEqualStrings("xxxxx…", cut.written());

    var none = std.Io.Writer.Allocating.init(gpa);
    defer none.deinit();
    try markedCell(&none.writer, "plain", 6, false, &.{}, .case, "", "[");
    try std.testing.expectEqualStrings("plain ", none.written());
}

test "view: a filter and a found text fold accented letters like upper()/lower()" {
    try std.testing.expect(Filter.parse("são").?.keeps(.string, "SÃO PAULO"));
    try std.testing.expect(!Filter.parse("!CRÉDITO").?.keeps(.string, "crédito"));
    const gpa = std.testing.allocator;
    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();
    try markedCell(&out.writer, "TIPO CRÉDITO", 12, false, &.{"crédito"}, .case, "", "[");
    try std.testing.expectEqualStrings("TIPO [CRÉDITO\x1b[0m", out.written());
}

test "view: with accents ignored, the filters, the find and the marks match unaccented text" {
    const gpa = std.testing.allocator;
    var f = Filter.parse("credito").?;
    try std.testing.expect(!f.keeps(.string, "CRÉDITO"));
    f.fold = .accents;
    try std.testing.expect(f.keeps(.string, "CRÉDITO"));

    const S = struct {
        const names = [_][]const u8{"n"};
        const tys = [_][]const u8{"string?"};
        const right = [_]bool{false};
        const cells = [_]?[]const u8{ "CRÉDITO", "débito", "credito" };
    };
    const g = Grid{ .names = &S.names, .types = &S.tys, .right = &S.right, .cells = &S.cells, .total_rows = 3 };
    const kinds = [_]?types.TypeKind{.string};
    var plain = try Find.parse(gpa, "credito", g.names);
    defer plain.deinit(gpa);
    const a = try arrangeWith(gpa, g, &kinds, null, &.{}, plain);
    defer gpa.free(a);
    try std.testing.expectEqualSlices(usize, &.{2}, a);
    var loose = try Find.parseFold(gpa, "credito", g.names, .accents);
    defer loose.deinit(gpa);
    const b = try arrangeWith(gpa, g, &kinds, null, &.{}, loose);
    defer gpa.free(b);
    try std.testing.expectEqualSlices(usize, &.{ 0, 2 }, b);

    var out = std.Io.Writer.Allocating.init(gpa);
    defer out.deinit();
    try markedCell(&out.writer, "TIPO CRÉDITO", 12, false, &.{"credito"}, .accents, "", "[");
    try std.testing.expectEqualStrings("TIPO [CRÉDITO\x1b[0m", out.written());
}
