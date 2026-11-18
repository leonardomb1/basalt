//! Changing the rows of one sheet of an existing workbook: the rows a DELETE drops,
//! the ones an UPDATE gives new values, the ones an INSERT adds after the last. The
//! workbook is written again whole, every part but the sheet (and, when a changed
//! row holds a date, time or timestamp, the styles) copied as it is: the other
//! sheets, the shared strings, the styles, the workbook's own parts. In the sheet,
//! every row the change leaves is copied byte for byte, its row and cell numbers
//! moved up past the rows deleted above it; an updated row is written as the
//! writer writes a row (`write.zig`), with three styles added for its temporal
//! cells; the `dimension` and `autoFilter` ranges follow.
//!
//! A sheet whose rows a change would move under something that names them by
//! place is refused, as the move would leave that pointing at other cells: a
//! formula, merged cells, conditional formats, data validations, hyperlinks,
//! tables, drawings or comments in it, or a formula or defined name elsewhere in
//! the workbook that names it.

const std = @import("std");
const column = @import("../../exec/column.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const zipsrc = @import("../compress/zip.zig");
const zipwrite = @import("../compress/zipwrite.zig");
const read = @import("read.zig");
const write = @import("write.zig");
const eval = @import("../../exec/eval.zig");
const msgfmt = @import("../../base/msgfmt.zig");

/// A changed row: its sheet row number, and its new cells (`batch`'s row `at`, one
/// column per sheet column from the read's first) or none for a deleted row.
pub const Change = struct { row: u32, batch: ?Batch = null, at: usize = 0 };

pub const Plan = struct {
    loc: read.Located,
    /// The sheet column of the read's first column (0 for `A`).
    c0: u32,
    /// Sorted by row; a row at most once.
    changes: []const Change,
    /// An INSERT's rows, one column per sheet column from `c0`, after the last row.
    appended: []const Batch = &.{},
};

/// Why `loc`'s sheet cannot have its rows changed, or null.
pub fn problem(arena: std.mem.Allocator, path: []const u8, loc: read.Located) !?[]const u8 {
    const sheet_marks = [_]struct { tag: []const u8, what: []const u8 }{
        .{ .tag = "<f>", .what = "a formula" },
        .{ .tag = "<f ", .what = "a formula" },
        .{ .tag = "<mergeCells", .what = "merged cells" },
        .{ .tag = "<conditionalFormatting", .what = "conditional formats" },
        .{ .tag = "<dataValidations", .what = "data validations" },
        .{ .tag = "<hyperlinks", .what = "hyperlinks" },
        .{ .tag = "<tableParts", .what = "a table" },
        .{ .tag = "<drawing", .what = "a drawing" },
        .{ .tag = "<legacyDrawing", .what = "comments" },
    };
    var tags: [sheet_marks.len][]const u8 = undefined;
    for (&tags, sheet_marks) |*t, m| t.* = m.tag;
    if (try findIn(arena, path, loc.sheet_part, &tags, false)) |i|
        return try msgfmt.allocPrint(arena, "sheet `{s}` holds {s}, which names its cells by place and would point elsewhere once rows move", .{ loc.sheet_name, sheet_marks[i].what });
    const quoted = try msgfmt.allocPrint(arena, "'{s}'!", .{try std.mem.replaceOwned(u8, arena, loc.sheet_name, "'", "''")});
    const plain = try msgfmt.allocPrint(arena, "{s}!", .{loc.sheet_name});
    const refs = [_][]const u8{ quoted, plain };
    for (loc.others) |o| if (try findIn(arena, path, o, &refs, true) != null)
        return try msgfmt.allocPrint(arena, "a formula in another sheet names sheet `{s}`, and would point elsewhere once its rows move", .{loc.sheet_name});
    if (try findIn(arena, path, loc.workbook, &refs, true) != null)
        return try msgfmt.allocPrint(arena, "a defined name names sheet `{s}`, and would point elsewhere once its rows move", .{loc.sheet_name});
    return null;
}

/// The index in `pats` of the first found in part `name`, case-insensitively when
/// `fold`; null when none is, or the part is not there.
fn findIn(arena: std.mem.Allocator, path: []const u8, name: []const u8, pats: []const []const u8, fold: bool) !?usize {
    const m = zipsrc.openMember(arena, path, name) catch |e| switch (e) {
        error.ZipMemberNotFound => return null,
        else => return e,
    };
    defer m.close();
    var longest: usize = 0;
    for (pats) |p| longest = @max(longest, p.len);
    var carry = std.array_list.Managed(u8).init(arena);
    while (true) {
        const chunk = m.reader.peekGreedy(1) catch |e| switch (e) {
            error.EndOfStream => return null,
            else => |x| return x,
        };
        try carry.appendSlice(chunk);
        m.reader.toss(chunk.len);
        for (pats, 0..) |p, i| {
            const hit = if (fold) std.ascii.indexOfIgnoreCase(carry.items, p) else std.mem.indexOf(u8, carry.items, p);
            if (hit != null) return i;
        }
        if (carry.items.len > longest) {
            const keep = longest;
            std.mem.copyForwards(u8, carry.items[0..keep], carry.items[carry.items.len - keep ..]);
            carry.items.len = keep;
        }
    }
}

/// Writes the workbook at `path` again into `out` with `plan`'s changes.
pub fn rewrite(arena: std.mem.Allocator, path: []const u8, plan: Plan, out: *std.Io.Writer) !void {
    const z = try zipwrite.Writer.init(arena, out);
    defer z.deinit();
    var styles = write.Styles{};
    var new_styles: ?[]const u8 = null;
    if (temporal(plan)) if (plan.loc.styles) |sp| {
        const patched = try addStyles(arena, path, sp);
        new_styles = patched.text;
        styles = patched.ids;
    };
    for (try zipsrc.names(arena, path)) |name| {
        if (std.mem.eql(u8, name, plan.loc.sheet_part)) {
            const w = try z.begin(name);
            try editSheet(arena, path, plan, styles, w);
            try z.end();
            continue;
        }
        if (new_styles != null and std.mem.eql(u8, name, plan.loc.styles.?)) {
            try z.addStored(name, new_styles.?);
            continue;
        }
        const m = try zipsrc.openMember(arena, path, name);
        defer m.close();
        const w = try z.begin(name);
        _ = try m.reader.streamRemaining(w);
        try z.end();
    }
    try z.finish();
}

/// Whether a changed or added row holds a date, time or timestamp, which needs a
/// style the workbook may lack.
fn temporal(plan: Plan) bool {
    for (plan.changes) |c| if (c.batch) |b| for (b.columns) |col| switch (col.ty.kind) {
        .date, .time, .timestamp => return true,
        else => {},
    };
    for (plan.appended) |b| for (b.columns) |col| switch (col.ty.kind) {
        .date, .time, .timestamp => return true,
        else => {},
    };
    return false;
}

const Patched = struct { text: []const u8, ids: write.Styles };

/// The styles part with three cell formats added at the end of `cellXfs`, Excel's
/// built-in date (14), date and time (22) and time (21) number formats, and their
/// indices.
fn addStyles(arena: std.mem.Allocator, path: []const u8, part: []const u8) !Patched {
    const m = try zipsrc.openMember(arena, path, part);
    defer m.close();
    var aw = std.Io.Writer.Allocating.init(arena);
    _ = try m.reader.streamRemaining(&aw.writer);
    const text = aw.written();
    const open = std.mem.indexOf(u8, text, "<cellXfs") orelse return error.BadXml;
    const close = std.mem.indexOfPos(u8, text, open, "</cellXfs>") orelse return error.BadXml;
    var n: usize = 0;
    var at = open;
    while (std.mem.indexOfPos(u8, text, at + 1, "<xf")) |i| {
        if (i >= close) break;
        n += 1;
        at = i;
    }
    const xf = "<xf numFmtId=\"{d}\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>";
    var o = std.array_list.Managed(u8).init(arena);
    const head_end = (std.mem.indexOfScalarPos(u8, text, open, '>') orelse return error.BadXml) + 1;
    const head = text[open..head_end];
    try o.appendSlice(text[0..open]);
    if (std.mem.indexOf(u8, head, "count=\"")) |c| {
        const vs = c + "count=\"".len;
        const ve = std.mem.indexOfScalarPos(u8, head, vs, '"') orelse return error.BadXml;
        try o.appendSlice(head[0..vs]);
        try o.writer().print("{d}", .{n + 3});
        try o.appendSlice(head[ve..]);
    } else try o.appendSlice(head);
    try o.appendSlice(text[head_end..close]);
    for ([_]u16{ 14, 22, 21 }) |id| try o.writer().print(xf, .{id});
    try o.appendSlice(text[close..]);
    return .{
        .text = o.items,
        .ids = .{
            .date = try msgfmt.allocPrint(arena, "{d}", .{n}),
            .timestamp = try msgfmt.allocPrint(arena, "{d}", .{n + 1}),
            .time = try msgfmt.allocPrint(arena, "{d}", .{n + 2}),
        },
    };
}

/// The sheet's XML with the plan's changes, streamed from the old into `w`.
fn editSheet(arena: std.mem.Allocator, path: []const u8, plan: Plan, st: write.Styles, w: *std.Io.Writer) !void {
    const m = try zipsrc.openMember(arena, path, plan.loc.sheet_part);
    defer m.close();
    const r = m.reader;
    var piece = std.array_list.Managed(u8).init(arena);
    var row = std.array_list.Managed(u8).init(arena);
    var ed = Editor{ .arena = arena, .plan = plan, .st = st, .w = w };
    var in_data = false;
    while (try nextPiece(r, &piece)) |p| {
        const lt = std.mem.lastIndexOfScalar(u8, p, '<') orelse {
            try w.writeAll(p);
            continue;
        };
        const tag = p[lt..];
        try w.writeAll(p[0..lt]);
        if (!in_data) {
            if (isTag(tag, "dimension") or isTag(tag, "autoFilter")) {
                try w.writeAll(try ed.moveRef(tag));
                continue;
            }
            if (isTag(tag, "sheetData")) {
                if (std.mem.endsWith(u8, tag, "/>")) {
                    try w.writeAll("<sheetData>");
                    try ed.finishData();
                    try w.writeAll("</sheetData>");
                    continue;
                }
                in_data = true;
            }
            try w.writeAll(tag);
            continue;
        }
        if (std.mem.startsWith(u8, tag, "</sheetData")) {
            try ed.finishData();
            in_data = false;
            try w.writeAll(tag);
            continue;
        }
        if (isTag(tag, "row")) {
            row.clearRetainingCapacity();
            try row.appendSlice(tag);
            if (!std.mem.endsWith(u8, tag, "/>")) while (true) {
                const q = (try nextPiece(r, &piece)) orelse return error.BadXml;
                try row.appendSlice(q);
                const ql = std.mem.lastIndexOfScalar(u8, q, '<') orelse continue;
                if (std.mem.startsWith(u8, q[ql..], "</row")) break;
            };
            try ed.row(row.items);
            continue;
        }
        try w.writeAll(tag);
    }
}

/// The next run of text up to and including a `>`, or the rest; null at the end.
fn nextPiece(r: *std.Io.Reader, buf: *std.array_list.Managed(u8)) !?[]const u8 {
    buf.clearRetainingCapacity();
    while (true) {
        const part = r.takeDelimiterInclusive('>') catch |e| switch (e) {
            error.StreamTooLong => {
                const held = r.buffered();
                try buf.appendSlice(held);
                r.toss(held.len);
                continue;
            },
            error.EndOfStream => {
                const held = r.buffered();
                if (held.len == 0 and buf.items.len == 0) return null;
                try buf.appendSlice(held);
                r.toss(held.len);
                return buf.items;
            },
            else => |x| return x,
        };
        try buf.appendSlice(part);
        return buf.items;
    }
}

/// Whether `tag` opens element `name` (`<name ` or `<name>` or `<name/>`).
fn isTag(tag: []const u8, name: []const u8) bool {
    if (tag.len < name.len + 2 or tag[0] != '<' or !std.mem.eql(u8, tag[1 .. 1 + name.len], name)) return false;
    const c = tag[1 + name.len];
    return c == ' ' or c == '>' or c == '/' or c == '\t' or c == '\n' or c == '\r';
}

const Editor = struct {
    arena: std.mem.Allocator,
    plan: Plan,
    st: write.Styles,
    w: *std.Io.Writer,
    /// The next change not yet passed, and the rows deleted before the one at hand.
    ci: usize = 0,
    shift: u32 = 0,
    /// The old number of the last row seen, and the new number of the last written.
    last_old: u32 = 0,
    last_new: u32 = 0,

    /// One `<row>` element of the old sheet, `text` whole: dropped, replaced or
    /// copied with its numbers moved up.
    fn row(self: *Editor, text: []const u8) !void {
        const n = rowNumber(text) orelse self.last_old + 1;
        try self.passTo(n);
        self.last_old = n;
        if (self.ci < self.plan.changes.len and self.plan.changes[self.ci].row == n) {
            const c = self.plan.changes[self.ci];
            self.ci += 1;
            if (c.batch == null) {
                self.shift += 1;
                return;
            }
            return self.render(n - self.shift, c.batch.?, c.at);
        }
        const now = n - self.shift;
        self.last_new = now;
        if (self.shift == 0) return self.w.writeAll(text);
        return self.w.writeAll(try renumber(self.arena, text, now));
    }

    /// Changes to rows before `n` the sheet holds no element for: an empty row the
    /// read saw, now deleted (moving later rows up) or given values.
    fn passTo(self: *Editor, n: u32) !void {
        while (self.ci < self.plan.changes.len and self.plan.changes[self.ci].row < n) {
            const c = self.plan.changes[self.ci];
            self.ci += 1;
            if (c.batch) |b| try self.render(c.row - self.shift, b, c.at) else self.shift += 1;
        }
    }

    fn render(self: *Editor, n: u32, b: Batch, at: usize) !void {
        self.last_new = n;
        try self.w.print("<row r=\"{d}\">", .{n});
        for (b.columns, 0..) |*col, i| try write.writeCell(self.w, col, at, try read.colName(self.arena, self.plan.c0 + @as(u32, @intCast(i))), n, self.st);
        try self.w.writeAll("</row>");
    }

    /// The end of the rows: changes past the last element, then an INSERT's rows.
    fn finishData(self: *Editor) !void {
        try self.passTo(std.math.maxInt(u32));
        for (self.plan.appended) |b| for (0..b.len) |i| try self.render(self.last_new + 1, b, i);
    }

    /// `tag` (a `dimension` or `autoFilter`) with its range's last row moved by the
    /// rows deleted at or above it and those an INSERT adds.
    fn moveRef(self: *Editor, tag: []const u8) ![]const u8 {
        const key = "ref=\"";
        const vs = (std.mem.indexOf(u8, tag, key) orelse return tag) + key.len;
        const ve = std.mem.indexOfScalarPos(u8, tag, vs, '"') orelse return tag;
        const ref = tag[vs..ve];
        const colon = std.mem.indexOfScalar(u8, ref, ':') orelse return tag;
        const end = ref[colon + 1 ..];
        var d: usize = 0;
        while (d < end.len and std.ascii.isAlphabetic(end[d])) d += 1;
        const last = std.fmt.parseInt(u32, end[d..], 10) catch return tag;
        var gone: u32 = 0;
        for (self.plan.changes) |c| if (c.batch == null and c.row <= last) {
            gone += 1;
        };
        var added: u32 = 0;
        for (self.plan.appended) |b| added += @intCast(b.len);
        const moved = @max(1, last - gone + added);
        return msgfmt.allocPrint(self.arena, "{s}{s}{d}{s}", .{ tag[0 .. vs + colon + 1], end[0..d], moved, tag[ve..] });
    }
};

/// The `r` of a `<row>` start tag, null when it has none.
fn rowNumber(text: []const u8) ?u32 {
    const gt = std.mem.indexOfScalar(u8, text, '>') orelse text.len;
    const head = text[0..gt];
    const at = std.mem.indexOf(u8, head, " r=\"") orelse return null;
    const vs = at + 4;
    const ve = std.mem.indexOfScalarPos(u8, head, vs, '"') orelse return null;
    return std.fmt.parseInt(u32, head[vs..ve], 10) catch null;
}

/// `text`, a `<row>` element, renumbered `n`: its own `r` and every cell's.
fn renumber(arena: std.mem.Allocator, text: []const u8, n: u32) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    var i: usize = 0;
    var first = true;
    while (std.mem.indexOfPos(u8, text, i, " r=\"")) |at| {
        const vs = at + 4;
        const ve = std.mem.indexOfScalarPos(u8, text, vs, '"') orelse break;
        try out.appendSlice(text[i..vs]);
        const open = std.mem.lastIndexOfScalar(u8, text[0..at], '<') orelse 0;
        const is_row = first and std.mem.startsWith(u8, text[open..], "<row");
        const is_cell = std.mem.startsWith(u8, text[open..], "<c ") or std.mem.startsWith(u8, text[open..], "<c\t");
        if (is_row) {
            try out.writer().print("{d}", .{n});
        } else if (is_cell) {
            var d = vs;
            while (d < ve and std.ascii.isAlphabetic(text[d])) d += 1;
            try out.appendSlice(text[vs..d]);
            try out.writer().print("{d}", .{n});
        } else try out.appendSlice(text[vs..ve]);
        first = false;
        i = ve;
    }
    try out.appendSlice(text[i..]);
    return out.items;
}

test "a row renumbered: its own number and every cell's, other attributes kept" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("<row r=\"3\" spans=\"1:2\"><c r=\"A3\" s=\"1\"><v>1</v></c><c r=\"AB3\" t=\"s\"><v>4</v></c></row>", try renumber(a, "<row r=\"7\" spans=\"1:2\"><c r=\"A7\" s=\"1\"><v>1</v></c><c r=\"AB7\" t=\"s\"><v>4</v></c></row>", 3));
    try std.testing.expectEqual(@as(?u32, 12), rowNumber("<row r=\"12\" ht=\"15\"><c/></row>"));
    try std.testing.expect(isTag("<row r=\"1\">", "row"));
    try std.testing.expect(!isTag("<rowBreaks count=\"1\">", "row"));
}
