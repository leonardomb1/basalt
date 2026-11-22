//! Excel workbook writer: `LOAD INTO 'report.xlsx' AS ...`, one worksheet named
//! `Sheet1` or `WITH (sheet = '...')`, a header row of column names, then a row
//! per result row.
//!
//! The workbook streams in constant memory through `Output` (a local `.part`, an
//! object upload, SFTP, SMB): the small parts are stored whole, the sheet is
//! deflated as it is written (`compress/zipwrite.zig`), and every string is an
//! inline string, as a shared-string table would hold every distinct text until
//! the end. Every row and cell carries its reference, so a null is simply absent.
//!
//! Cells: null is no cell; a bool is a boolean cell; numbers are numbers. Excel
//! holds a number as a double, so an integer beyond 2^53 and a decimal of more
//! than 15 significant digits are written as text keeping every digit, rather
//! than rounded silently; a non-finite float is an empty cell. A date, timestamp
//! or time is a serial number in an ISO format (`yyyy-mm-dd`, `yyyy-mm-dd
//! hh:mm:ss`, `hh:mm:ss`), so it reads back as a temporal, to the millisecond
//! Excel keeps (finer digits are cut, never rounded into the next day). Excel's
//! calendar starts in 1900 and wrongly counts a 29 February 1900, so a date or
//! timestamp before 1 March 1900 or after 9999 is written as ISO text instead.
//! Bytes are base64 text. Text is XML-escaped, a control character as
//! `_xHHHH_` and a literal `_xHHHH_` as `_x005F_xHHHH_`; invalid UTF-8 is
//! repaired to U+FFFD. Arrays and structs are refused, as Arrow refuses them.
//!
//! Excel's limits are refusals, never truncation: 16,384 columns, 1,048,576 rows
//! (the header and 1,048,575 data rows), 32,767 characters a cell, a sheet name of
//! 1 to 31 characters without `[]:*?/\`, and 4 GiB for the sheet's XML (no zip64).

const std = @import("std");
const types = @import("../../lang/types.zig");
const column = @import("../../exec/column.zig");
const celltext = @import("../../exec/celltext.zig");
const format = @import("../../exec/eval/format.zig");
const decimal = @import("../../base/decimal.zig");
const eval = @import("../../exec/eval.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const driver = @import("../../exec/driver.zig");
const Output = @import("../output.zig").Output;
const zipwrite = @import("../compress/zipwrite.zig");
const read = @import("read.zig");
const serial = @import("serial.zig");
const zipsrc = @import("../compress/zip.zig");
const utf16 = @import("../../base/utf16.zig");
const escape = @import("../xml/escape.zig");
const msgfmt = @import("../../base/msgfmt.zig");

pub const max_cols = 16384;
pub const max_rows = 1048576;
pub const max_cell_chars = 32767;
pub const content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

const style_date = "1";
const style_timestamp = "2";
const style_time = "3";

/// Why Excel would refuse `name` as a sheet name, or null.
pub fn sheetNameProblem(name: []const u8) ?[]const u8 {
    if (name.len == 0) return "a sheet name cannot be empty";
    if (!std.unicode.utf8ValidateSlice(name)) return "a sheet name must be valid UTF-8";
    if (utf16.unitLen(name) > 31) return "a sheet name is at most 31 characters";
    if (std.mem.indexOfAny(u8, name, "[]:*?/\\") != null) return "a sheet name cannot hold any of `[ ] : * ? / \\`";
    for (name) |c| if (c < 0x20) return "a sheet name cannot hold a control character";
    if (name[0] == '\'' or name[name.len - 1] == '\'') return "a sheet name cannot start or end with `'`";
    if (std.ascii.eqlIgnoreCase(name, "History")) return "`History` is a name Excel reserves";
    return null;
}

pub const Writer = struct {
    out: *Output,
    zip: *zipwrite.Writer,
    sheet: *std.Io.Writer,
    refs: []const []const u8,
    row: u32 = 1,
    styles: Styles = .{},

    /// Refuses a schema Excel cannot hold before anything is written.
    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, sheet_name: []const u8) !*Writer {
        try refuse(schema, sheet_name);
        const self = try begin(arena, path, schema);
        errdefer self.abort();
        self.start(arena, schema, sheet_name) catch |e| return self.out.specific(e);
        return self;
    }

    /// `open` into the workbook already at `path`, laid out as `lay`: the worksheet
    /// `sheet_name` is written anew in its place, or added after the others when the
    /// workbook has none of that name, and every other part is copied as it is. The
    /// styles gain three temporal formats when the rows hold dates or times, and the
    /// calculation chain is dropped, as Excel rebuilds it and a stale one names cells
    /// the new sheet no longer has, and when another sheet holds a formula the
    /// workbook asks to be calculated again as it opens (`recalcOnLoad`), as the
    /// formula's cached value is stale. A sheet with parts of its own (a table, a drawing,
    /// comments, a pivot table) is refused, as writing it anew would leave them
    /// behind, and so is a name a chart sheet holds.
    pub fn openInto(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, sheet_name: []const u8, lay: read.Layout) !*Writer {
        try refuse(schema, sheet_name);
        if (lay.taken) return explainf(error.XlsxBadSheetName, "sheet `{s}`: a chart sheet of the workbook has that name", .{sheet_name});
        const names = try zipsrc.names(arena, path);
        if (lay.sheet_part) |sp| if (hasName(names, try read.relsOf(arena, sp)))
            return explainf(error.XlsxSheetHasParts, "sheet `{s}` has parts of its own (a table, a drawing, comments or a pivot table), which writing it anew would leave behind; write the rows to another sheet", .{lay.sheet_name});
        const temporal = for (schema.fields) |f| switch (f.ty.kind) {
            .date, .time, .timestamp => break true,
            else => {},
        } else false;
        var st = Styles{ .date1904 = lay.date1904 };
        var styles_text: ?[]const u8 = null;
        if (temporal) {
            const sp = lay.styles orelse return explainf(error.XlsxNoStyles, "the workbook has no styles part, which its new dates and times need; write them to a new workbook", .{});
            const p = try withTemporalStyles(arena, try memberText(arena, path, sp));
            styles_text = p.text;
            st = p.ids;
            st.date1904 = lay.date1904;
        }
        const added: ?[]const u8 = if (lay.sheet_part == null) try freshSheetPart(arena, names, lay.workbook) else null;
        const part = lay.sheet_part orelse added.?;
        var types_text = try dropEmpty(arena, try memberText(arena, path, "[Content_Types].xml"), "calcChain");
        var rels_text = try dropEmpty(arena, try memberText(arena, path, lay.workbook_rels), "calcChain");
        var wb_text = try memberText(arena, path, lay.workbook);
        if (added) |np| {
            const rid = try msgfmt.allocPrint(arena, "rId{d}", .{maxAfter(rels_text, "Id=\"rId") + 1});
            types_text = try insertBeforeClose(arena, types_text, "Types", try msgfmt.allocPrint(arena, "<Override PartName=\"/{s}\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>", .{np}));
            rels_text = try insertBeforeClose(arena, rels_text, "Relationships", try msgfmt.allocPrint(arena, "<Relationship Id=\"{s}\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"{s}\"/>", .{ rid, relTarget(lay.workbook, np) }));
            wb_text = try addSheetEntry(arena, wb_text, sheet_name, rid);
        }
        var kept = std.array_list.Managed([]const u8).init(arena);
        for (names) |n| if (std.mem.indexOf(u8, n, "worksheets/") != null and std.mem.endsWith(u8, n, ".xml") and !std.mem.eql(u8, n, part)) try kept.append(n);
        if (try anyFormula(arena, path, kept.items)) wb_text = try recalcOnLoad(arena, wb_text);

        const self = try begin(arena, path, schema);
        errdefer self.abort();
        self.styles = st;
        self.copyInto(arena, path, names, part, lay, types_text, rels_text, wb_text, styles_text) catch |e| return self.out.specific(e);
        self.startSheet(schema, part) catch |e| return self.out.specific(e);
        return self;
    }

    fn copyInto(self: *Writer, arena: std.mem.Allocator, path: []const u8, names: []const []const u8, part: []const u8, lay: read.Layout, types_text: []const u8, rels_text: []const u8, wb_text: []const u8, styles_text: ?[]const u8) !void {
        for (names) |name| {
            if (std.mem.eql(u8, name, part) or std.mem.endsWith(u8, name, "/calcChain.xml")) continue;
            if (std.mem.eql(u8, name, "[Content_Types].xml")) {
                try self.zip.addStored(name, types_text);
                continue;
            }
            if (std.mem.eql(u8, name, lay.workbook_rels)) {
                try self.zip.addStored(name, rels_text);
                continue;
            }
            if (std.mem.eql(u8, name, lay.workbook)) {
                try self.zip.addStored(name, wb_text);
                continue;
            }
            if (styles_text) |t| if (std.mem.eql(u8, name, lay.styles.?)) {
                try self.zip.addStored(name, t);
                continue;
            };
            const m = try zipsrc.openMember(arena, path, name);
            defer m.close();
            const w = try self.zip.begin(name);
            _ = try m.reader.streamRemaining(w);
            try self.zip.end();
        }
    }

    fn refuse(schema: types.Schema, sheet_name: []const u8) !void {
        if (schema.fields.len > max_cols)
            return explainf(error.XlsxTooManyColumns, "an Excel sheet holds at most {d} columns; this result has {d}", .{ max_cols, schema.fields.len });
        for (schema.fields) |f| if (f.ty.isNested())
            return explainf(error.XlsxNestedColumn, "an Excel sheet cannot hold the list, map or struct column `{s}`", .{f.name});
        if (sheetNameProblem(sheet_name)) |why| return explainf(error.XlsxBadSheetName, "sheet `{s}`: {s}", .{ sheet_name, why });
    }

    fn begin(arena: std.mem.Allocator, path: []const u8, schema: types.Schema) !*Writer {
        const self = try arena.create(Writer);
        const out = try Output.open(arena, path, content_type, .truncate);
        errdefer out.abort();
        const refs = try arena.alloc([]const u8, schema.fields.len);
        for (refs, 0..) |*r, i| r.* = try read.colName(arena, @intCast(i));
        const z = try zipwrite.Writer.init(arena, out.raw());
        self.* = .{ .out = out, .zip = z, .sheet = undefined, .refs = refs };
        return self;
    }

    fn start(self: *Writer, arena: std.mem.Allocator, schema: types.Schema, sheet_name: []const u8) !void {
        try self.zip.addStored("[Content_Types].xml", content_types);
        try self.zip.addStored("_rels/.rels", root_rels);
        var wb = std.Io.Writer.Allocating.init(arena);
        try wb.writer.writeAll(workbook_head);
        try escape.text(&wb.writer, sheet_name, .{ .attr = true });
        try wb.writer.writeAll(workbook_tail);
        try self.zip.addStored("xl/workbook.xml", wb.written());
        try self.zip.addStored("xl/_rels/workbook.xml.rels", workbook_rels);
        try self.zip.addStored("xl/styles.xml", styles);
        try self.startSheet(schema, "xl/worksheets/sheet1.xml");
    }

    /// Opens sheet part `part` and writes its header row.
    fn startSheet(self: *Writer, schema: types.Schema, part: []const u8) !void {
        self.sheet = try self.zip.begin(part);
        const w = self.sheet;
        try w.writeAll(sheet_head);
        try w.writeAll("<row r=\"1\">");
        for (schema.fields, self.refs) |f, ref| {
            try w.print("<c r=\"{s}1\"", .{ref});
            try self.text(f.name);
        }
        try w.writeAll("</row>");
    }

    pub fn writeBatch(self: *Writer, _: std.mem.Allocator, batch: Batch) !void {
        self.rows(batch) catch |e| return self.out.specific(e);
    }

    fn rows(self: *Writer, batch: Batch) !void {
        const w = self.sheet;
        for (0..batch.len) |r| {
            if (self.row == max_rows)
                return explainf(error.XlsxTooManyRows, "an Excel sheet holds {d} rows, the header and {d} data rows; this result has more", .{ max_rows, max_rows - 1 });
            self.row += 1;
            try w.print("<row r=\"{d}\">", .{self.row});
            for (batch.columns, self.refs) |*c, ref| try self.cell(c, r, ref);
            try w.writeAll("</row>");
        }
        if (self.zip.memberSize() > std.math.maxInt(u32))
            return explainf(error.ZipTooLarge, "the sheet's XML passed 4 GiB at row {d}, more than a workbook without zip64 holds", .{self.row});
    }

    fn cell(self: *Writer, c: *const column.Column, r: usize, ref: []const u8) !void {
        return writeCell(self.sheet, c, r, ref, self.row, self.styles);
    }

    /// Finishes a cell opened up to its reference as an inline string.
    fn text(self: *Writer, s: []const u8) !void {
        return writeText(self.sheet, s, self.row);
    }

    pub fn close(self: *Writer) !void {
        self.finishSheet() catch |e| {
            const why = self.out.specific(e);
            self.abort();
            return why;
        };
        self.zip.deinit();
        return self.out.finish();
    }

    fn finishSheet(self: *Writer) !void {
        try self.sheet.writeAll("</sheetData></worksheet>");
        try self.zip.end();
        try self.zip.finish();
    }

    pub fn abort(self: *Writer) void {
        self.zip.deinit();
        self.out.abort();
    }

    pub fn sink(self: *Writer) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }
};

const sink_vtable = driver.sinkVTable(Writer);

/// Part `name` of the workbook at `path`, whole.
pub fn memberText(arena: std.mem.Allocator, path: []const u8, name: []const u8) ![]const u8 {
    const m = try zipsrc.openMember(arena, path, name);
    defer m.close();
    var aw = std.Io.Writer.Allocating.init(arena);
    _ = try m.reader.streamRemaining(&aw.writer);
    return aw.written();
}

pub const Patched = struct { text: []const u8, ids: Styles };

/// Styles part `text` with the cell formats of Excel's built-in date (14), date and
/// time (22) and time (21) number formats in `cellXfs`, and their indices: one this
/// function added before, by an earlier edit, is used again, so editing a workbook
/// over and over does not grow its styles; the others are added at the end.
pub fn withTemporalStyles(arena: std.mem.Allocator, text: []const u8) !Patched {
    const open = std.mem.indexOf(u8, text, "<cellXfs") orelse return error.BadXml;
    const close = std.mem.indexOfPos(u8, text, open, "</cellXfs>") orelse return error.BadXml;
    const fmts = [_]u16{ 14, 22, 21 };
    var own: [fmts.len][]const u8 = undefined;
    for (&own, fmts) |*s, id| s.* = try msgfmt.allocPrint(arena, "<xf numFmtId=\"{d}\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>", .{id});
    var found = [_]?usize{null} ** fmts.len;
    var n: usize = 0;
    var at = open;
    while (std.mem.indexOfPos(u8, text, at + 1, "<xf")) |i| {
        if (i >= close) break;
        for (own, &found) |s, *f| if (f.* == null and std.mem.startsWith(u8, text[i..], s)) {
            f.* = n;
        };
        n += 1;
        at = i;
    }
    var idx: [fmts.len]usize = undefined;
    var added: usize = 0;
    for (found, &idx) |f, *x| x.* = f orelse blk: {
        added += 1;
        break :blk n + added - 1;
    };
    var o = std.array_list.Managed(u8).init(arena);
    const head_end = (std.mem.indexOfScalarPos(u8, text, open, '>') orelse return error.BadXml) + 1;
    const head = text[open..head_end];
    try o.appendSlice(text[0..open]);
    if (std.mem.indexOf(u8, head, "count=\"")) |c| {
        const vs = c + "count=\"".len;
        const ve = std.mem.indexOfScalarPos(u8, head, vs, '"') orelse return error.BadXml;
        try o.appendSlice(head[0..vs]);
        try o.writer().print("{d}", .{n + added});
        try o.appendSlice(head[ve..]);
    } else try o.appendSlice(head);
    try o.appendSlice(text[head_end..close]);
    for (own, found) |s, f| if (f == null) try o.appendSlice(s);
    try o.appendSlice(text[close..]);
    return .{
        .text = o.items,
        .ids = .{
            .date = try msgfmt.allocPrint(arena, "{d}", .{idx[0]}),
            .timestamp = try msgfmt.allocPrint(arena, "{d}", .{idx[1]}),
            .time = try msgfmt.allocPrint(arena, "{d}", .{idx[2]}),
        },
    };
}

/// Workbook part `text` asking the application that opens it to calculate every
/// formula again (`fullCalcOnLoad`), as rows it changed leave the values a formula
/// cached stale. The `calcPr` goes where the schema orders it, before the elements
/// that follow it, under the root's namespace prefix; a workbook with none of those
/// and no closing root tag stays as it is.
pub fn recalcOnLoad(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    const pre = rootPrefix(text);
    const calc = try msgfmt.allocPrint(arena, "<{s}calcPr", .{pre});
    if (std.mem.indexOf(u8, text, calc)) |at| {
        const end = std.mem.indexOfScalarPos(u8, text, at, '>') orelse return error.BadXml;
        const tag = text[at..end];
        if (std.mem.indexOf(u8, tag, "fullCalcOnLoad=\"")) |f| {
            const vs = at + f + "fullCalcOnLoad=\"".len;
            const ve = std.mem.indexOfScalarPos(u8, text, vs, '"') orelse return error.BadXml;
            return std.mem.concat(arena, u8, &.{ text[0..vs], "1", text[ve..] });
        }
        const after = at + calc.len;
        return std.mem.concat(arena, u8, &.{ text[0..after], " fullCalcOnLoad=\"1\"", text[after..] });
    }
    const later = [_][]const u8{ "<{s}oleSize", "<{s}customWorkbookViews", "<{s}pivotCaches", "<{s}smartTagPr", "<{s}smartTagTypes", "<{s}webPublishing", "<{s}fileRecoveryPr", "<{s}webPublishObjects", "<{s}extLst", "</{s}workbook>" };
    var at: ?usize = null;
    inline for (later) |t| if (std.mem.indexOf(u8, text, try msgfmt.allocPrint(arena, t, .{pre}))) |i| {
        at = if (at) |a| @min(a, i) else i;
    };
    const i = at orelse return text;
    return std.mem.concat(arena, u8, &.{ text[0..i], try msgfmt.allocPrint(arena, "<{s}calcPr fullCalcOnLoad=\"1\"/>", .{pre}), text[i..] });
}

/// The namespace prefix of the workbook's root element with its colon (`x:`), or
/// none.
fn rootPrefix(text: []const u8) []const u8 {
    if (std.mem.indexOf(u8, text, "<workbook") != null) return "";
    const at = std.mem.indexOf(u8, text, ":workbook") orelse return "";
    const lt = std.mem.lastIndexOfScalar(u8, text[0..at], '<') orelse return "";
    return text[lt + 1 .. at + 1];
}

/// Whether any of the worksheet parts `parts` holds a formula, bare or under a
/// namespace prefix.
pub fn anyFormula(arena: std.mem.Allocator, path: []const u8, parts: []const []const u8) !bool {
    for (parts) |p| if (try findIn(arena, path, p, &.{ "<f>", "<f ", ":f>", ":f " }, false) != null) return true;
    return false;
}

/// The index in `pats` of the first found in part `name`, case-insensitively when
/// `fold`; null when none is, or the part is not there. The part streams, a chunk at
/// a time, so a large sheet is never held whole.
pub fn findIn(arena: std.mem.Allocator, path: []const u8, name: []const u8, pats: []const []const u8, fold: bool) !?usize {
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

fn hasName(names: []const []const u8, name: []const u8) bool {
    for (names) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

/// A worksheet part name the package does not use, beside the workbook part.
fn freshSheetPart(arena: std.mem.Allocator, names: []const []const u8, workbook: []const u8) ![]const u8 {
    const dir = if (std.mem.lastIndexOfScalar(u8, workbook, '/')) |i| workbook[0 .. i + 1] else "";
    var k: usize = 1;
    while (true) : (k += 1) {
        const name = try msgfmt.allocPrint(arena, "{s}worksheets/sheet{d}.xml", .{ dir, k });
        if (!hasName(names, name)) return name;
    }
}

/// `part`'s target in a relationship of `workbook`: relative to its folder, or from
/// the package root when it lies elsewhere.
fn relTarget(workbook: []const u8, part: []const u8) []const u8 {
    const dir = if (std.mem.lastIndexOfScalar(u8, workbook, '/')) |i| workbook[0 .. i + 1] else "";
    return if (std.mem.startsWith(u8, part, dir)) part[dir.len..] else part;
}

/// The largest number right after an occurrence of `key` in `text`, 0 when none.
fn maxAfter(text: []const u8, key: []const u8) usize {
    var best: usize = 0;
    var i: usize = 0;
    while (std.mem.indexOfPos(u8, text, i, key)) |at| {
        var j = at + key.len;
        var n: usize = 0;
        while (j < text.len and std.ascii.isDigit(text[j])) : (j += 1) n = n *| 10 +| (text[j] - '0');
        best = @max(best, n);
        i = at + key.len;
    }
    return best;
}

/// Where the last closing tag of element `name` (under any prefix) starts in `text`,
/// and that prefix with its colon.
fn lastClose(text: []const u8, name: []const u8) ?struct { at: usize, prefix: []const u8 } {
    var end = text.len;
    while (std.mem.lastIndexOf(u8, text[0..end], "</")) |i| {
        end = i;
        const rest = text[i + 2 ..];
        const gt = std.mem.indexOfScalar(u8, rest, '>') orelse continue;
        const tag = std.mem.trimRight(u8, rest[0..gt], " \t\r\n");
        const colon = std.mem.indexOfScalar(u8, tag, ':');
        const local = if (colon) |c| tag[c + 1 ..] else tag;
        if (std.mem.eql(u8, local, name)) return .{ .at = i, .prefix = if (colon) |c| tag[0 .. c + 1] else "" };
    }
    return null;
}

/// `text` with `insertion` right before the last closing tag of element `name`.
fn insertBeforeClose(arena: std.mem.Allocator, text: []const u8, name: []const u8, insertion: []const u8) ![]const u8 {
    const c = lastClose(text, name) orelse return error.BadXml;
    return std.mem.concat(arena, u8, &.{ text[0..c.at], insertion, text[c.at..] });
}

/// `text` without each empty element whose tag holds `needle`.
fn dropEmpty(arena: std.mem.Allocator, text: []const u8, needle: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    var i: usize = 0;
    var from: usize = 0;
    while (std.mem.indexOfPos(u8, text, from, needle)) |hit| {
        from = hit + needle.len;
        const open = std.mem.lastIndexOfScalar(u8, text[0..hit], '<') orelse continue;
        if (open < i) continue;
        const close = std.mem.indexOfPos(u8, text, hit, "/>") orelse continue;
        if (std.mem.indexOfScalar(u8, text[open..close], '>') != null) continue;
        try out.appendSlice(text[i..open]);
        i = close + 2;
        from = i;
    }
    try out.appendSlice(text[i..]);
    return out.items;
}

/// Workbook part `text` with a `sheet` entry named `name` for relationship `rid` at
/// the end of its `sheets`, numbered past the others, in the prefix the workbook binds
/// to the relationships namespace (declared on the entry when it binds none).
fn addSheetEntry(arena: std.mem.Allocator, text: []const u8, name: []const u8, rid: []const u8) ![]const u8 {
    const c = lastClose(text, "sheets") orelse return error.BadXml;
    const ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    const prefix: ?[]const u8 = blk: {
        const q = std.mem.indexOf(u8, text, "=\"" ++ ns ++ "\"") orelse break :blk null;
        const decl = std.mem.lastIndexOf(u8, text[0..q], "xmlns:") orelse break :blk null;
        break :blk text[decl + "xmlns:".len .. q];
    };
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    try w.print("<{s}sheet name=\"", .{c.prefix});
    try escape.text(w, name, .{ .attr = true });
    try w.print("\" sheetId=\"{d}\" ", .{maxAfter(text, "sheetId=\"") + 1});
    if (prefix) |p| try w.print("{s}:id=\"{s}\"/>", .{ p, rid }) else try w.print("r:id=\"{s}\" xmlns:r=\"{s}\"/>", .{ rid, ns });
    return std.mem.concat(arena, u8, &.{ text[0..c.at], aw.written(), text[c.at..] });
}

/// The cell styles a writer's temporal cells name, as indices into the workbook's
/// `cellXfs`: the writer's own, or those added to a workbook it edits; and that
/// workbook's date system, as a 1904 workbook counts its serials from 1904-01-01.
pub const Styles = struct { date: []const u8 = style_date, timestamp: []const u8 = style_timestamp, time: []const u8 = style_time, date1904: bool = false };

/// Cell `r` of `c` at column `ref` of sheet row `row`, nothing for a null, its temporal
/// styles `st`.
pub fn writeCell(w: *std.Io.Writer, c: *const column.Column, r: usize, ref: []const u8, row: u32, st: Styles) !void {
    if (!c.validity.get(r)) return;
    switch (c.ty.kind) {
        .string => {
            try w.print("<c r=\"{s}{d}\"", .{ ref, row });
            return writeText(w, c.data.bytes.at(r), row);
        },
        .bytes => {
            const b = c.data.bytes.at(r);
            if (std.base64.standard.Encoder.calcSize(b.len) > max_cell_chars) return tooLong(row, ref);
            try w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>", .{ ref, row });
            try std.base64.standard.Encoder.encodeWriter(w, b);
            return w.writeAll("</t></is></c>");
        },
        .bool => return w.print("<c r=\"{s}{d}\" t=\"b\"><v>{d}</v></c>", .{ ref, row, @intFromBool(c.data.b[r]) }),
        .int => {
            const v = c.data.i64[r];
            if (@abs(v) <= 1 << 53) return w.print("<c r=\"{s}{d}\"><v>{d}</v></c>", .{ ref, row, v });
            return w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>{d}</t></is></c>", .{ ref, row, v });
        },
        .float => {
            const x = c.data.f64[r];
            if (!std.math.isFinite(x)) return;
            const ax = @abs(x);
            if (ax == 0 or (ax >= 1e-6 and ax < 1e15)) return w.print("<c r=\"{s}{d}\"><v>{d}</v></c>", .{ ref, row, x });
            return w.print("<c r=\"{s}{d}\"><v>{e}</v></c>", .{ ref, row, x });
        },
        .decimal => {
            const d = column.decimals(c.*).at(r);
            try w.print("<c r=\"{s}{d}\"", .{ ref, row });
            if (decimal.exactInDouble(d.unscaled)) {
                try w.writeAll("><v>");
                try decimal.writeDecimal(w, d.unscaled, d.scale);
                return w.writeAll("</v></c>");
            }
            try w.writeAll(" t=\"inlineStr\"><is><t>");
            try decimal.writeDecimal(w, d.unscaled, d.scale);
            return w.writeAll("</t></is></c>");
        },
        .date => {
            const days: i64 = c.data.i32[r];
            if (serial.ofDays(days, st.date1904)) |v|
                return w.print("<c r=\"{s}{d}\" s=\"{s}\"><v>{d}</v></c>", .{ ref, row, st.date, v });
            try w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>", .{ ref, row });
            try format.writeDate(w, days);
            return w.writeAll("</t></is></c>");
        },
        .timestamp => {
            const us = c.data.i64[r];
            if (serial.ofMicros(us, st.date1904)) |v|
                return w.print("<c r=\"{s}{d}\" s=\"{s}\"><v>{d}</v></c>", .{ ref, row, st.timestamp, v });
            try w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>", .{ ref, row });
            try format.writeTimestamp(w, us);
            return w.writeAll("</t></is></c>");
        },
        .time => {
            return w.print("<c r=\"{s}{d}\" s=\"{s}\"><v>{d}</v></c>", .{ ref, row, st.time, serial.ofTime(c.data.i64[r]) });
        },
        .array, .@"struct", .map => unreachable,
    }
}

/// Finishes a cell opened up to its reference as an inline string, in sheet row `row`.
pub fn writeText(w: *std.Io.Writer, s: []const u8, row: u32) !void {
    const valid = std.unicode.utf8ValidateSlice(s);
    if (s.len > max_cell_chars and charsOf(s, valid) > max_cell_chars) return tooLong(row, "");
    const preserve = s.len > 0 and (isSpace(s[0]) or isSpace(s[s.len - 1]));
    try w.writeAll(if (preserve) " t=\"inlineStr\"><is><t xml:space=\"preserve\">" else " t=\"inlineStr\"><is><t>");
    try escape.text(w, s, .{ .ooxml = true });
    try w.writeAll("</t></is></c>");
}

pub fn explainf(e: anyerror, comptime fmt: []const u8, args: anytype) anyerror {
    @branchHint(.cold);
    var buf: [480]u8 = undefined;
    return eval.explain(e, msgfmt.bufPrint(&buf, fmt, args) catch fmt);
}

fn tooLong(row: u32, ref: []const u8) anyerror {
    return explainf(error.XlsxCellTooLong, "an Excel cell holds at most {d} characters; the one at row {d}{s}{s} has more", .{ max_cell_chars, row, if (ref.len > 0) ", column " else "", ref });
}

fn charsOf(s: []const u8, valid: bool) usize {
    if (valid) return utf16.unitLen(s);
    var n: usize = 0;
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| n += utf16.unitLen(t);
    return n;
}

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

const xml_decl = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";
const ns_main = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
const ns_rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const ns_pkg_rel = "http://schemas.openxmlformats.org/package/2006/relationships";

const content_types = xml_decl ++
    "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">" ++
    "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>" ++
    "<Default Extension=\"xml\" ContentType=\"application/xml\"/>" ++
    "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>" ++
    "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>" ++
    "<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>" ++
    "</Types>";

const root_rels = xml_decl ++
    "<Relationships xmlns=\"" ++ ns_pkg_rel ++ "\">" ++
    "<Relationship Id=\"rId1\" Type=\"" ++ ns_rel ++ "/officeDocument\" Target=\"xl/workbook.xml\"/>" ++
    "</Relationships>";

const workbook_head = xml_decl ++
    "<workbook xmlns=\"" ++ ns_main ++ "\" xmlns:r=\"" ++ ns_rel ++ "\"><sheets><sheet name=\"";
const workbook_tail = "\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>";

const workbook_rels = xml_decl ++
    "<Relationships xmlns=\"" ++ ns_pkg_rel ++ "\">" ++
    "<Relationship Id=\"rId1\" Type=\"" ++ ns_rel ++ "/worksheet\" Target=\"worksheets/sheet1.xml\"/>" ++
    "<Relationship Id=\"rId2\" Type=\"" ++ ns_rel ++ "/styles\" Target=\"styles.xml\"/>" ++
    "</Relationships>";

const styles = xml_decl ++
    "<styleSheet xmlns=\"" ++ ns_main ++ "\">" ++
    "<numFmts count=\"3\">" ++
    "<numFmt numFmtId=\"164\" formatCode=\"yyyy-mm-dd\"/>" ++
    "<numFmt numFmtId=\"165\" formatCode=\"yyyy-mm-dd hh:mm:ss\"/>" ++
    "<numFmt numFmtId=\"166\" formatCode=\"hh:mm:ss\"/>" ++
    "</numFmts>" ++
    "<fonts count=\"1\"><font><sz val=\"11\"/><name val=\"Calibri\"/><family val=\"2\"/></font></fonts>" ++
    "<fills count=\"2\"><fill><patternFill patternType=\"none\"/></fill><fill><patternFill patternType=\"gray125\"/></fill></fills>" ++
    "<borders count=\"1\"><border><left/><right/><top/><bottom/><diagonal/></border></borders>" ++
    "<cellStyleXfs count=\"1\"><xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\"/></cellStyleXfs>" ++
    "<cellXfs count=\"4\">" ++
    "<xf numFmtId=\"0\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\"/>" ++
    "<xf numFmtId=\"164\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>" ++
    "<xf numFmtId=\"165\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>" ++
    "<xf numFmtId=\"166\" fontId=\"0\" fillId=\"0\" borderId=\"0\" xfId=\"0\" applyNumberFormat=\"1\"/>" ++
    "</cellXfs>" ++
    "<cellStyles count=\"1\"><cellStyle name=\"Normal\" xfId=\"0\" builtinId=\"0\"/></cellStyles>" ++
    "</styleSheet>";

const sheet_head = xml_decl ++ "<worksheet xmlns=\"" ++ ns_main ++ "\"><sheetData>";

test "sheet names Excel refuses" {
    try std.testing.expect(sheetNameProblem("Sheet1") == null);
    try std.testing.expect(sheetNameProblem("Ação de março 2026") == null);
    try std.testing.expect(sheetNameProblem("ççççççççççççççççççççççççççççççç") == null);
    try std.testing.expect(sheetNameProblem("") != null);
    try std.testing.expect(sheetNameProblem("a" ** 32) != null);
    try std.testing.expect(sheetNameProblem("a/b") != null);
    try std.testing.expect(sheetNameProblem("q?") != null);
    try std.testing.expect(sheetNameProblem("'x") != null);
    try std.testing.expect(sheetNameProblem("history") != null);
}

test "text escapes: markup, controls, CR, noncharacters and literal escapes" {
    var aw = std.Io.Writer.Allocating.init(std.testing.allocator);
    defer aw.deinit();
    try escape.text(&aw.writer, "a<b>&c\r\n\t\x00_x0041_ _x12_ \u{FFFE}\u{FFFF}é", .{ .ooxml = true });
    try std.testing.expectEqualStrings("a&lt;b&gt;&amp;c_x000D_\n\t_x0000__x005F_x0041_ _x12_ _xFFFE__xFFFF_é", aw.written());
}

test "Excel's bounds: columns refused at open, a cell's characters counted, not its bytes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = ar.allocator();
    const path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "b.xlsx" });

    const ty = types.Type.init(.string).asNullable();
    const fields = try a.alloc(types.Schema.Field, max_cols + 1);
    for (fields, 0..) |*f, i| f.* = .{ .name = try msgfmt.allocPrint(a, "c{d}", .{i}), .ty = ty };
    try std.testing.expectError(error.XlsxTooManyColumns, Writer.open(a, path, .{ .fields = fields }, "Sheet1"));
    const wide = try Writer.open(a, path, .{ .fields = fields[0..max_cols] }, "Sheet1");
    wide.abort();

    const one = types.Schema{ .fields = fields[0..1] };
    const at_bound = "ç" ** (max_cell_chars - 2) ++ "\u{1F600}";
    const cases = [_]struct { s: []const u8, ok: bool }{
        .{ .s = "ç" ** max_cell_chars, .ok = true },
        .{ .s = "ç" ** (max_cell_chars + 1), .ok = false },
        .{ .s = at_bound, .ok = true },
        .{ .s = at_bound ++ "x", .ok = false },
        .{ .s = "\xff" ** (max_cell_chars + 1), .ok = false },
    };
    for (cases) |c| {
        var b = column.Builder.init(a, ty);
        try b.append(.{ .string = c.s });
        const cols = try a.alloc(column.Column, 1);
        cols[0] = try b.finish();
        const w = try Writer.open(a, path, one, "Sheet1");
        const got = w.writeBatch(a, .{ .schema = &one, .columns = cols, .len = 1 });
        if (c.ok) {
            try got;
            try w.close();
            const r = try read.Reader.open(a, std.testing.allocator, path, .{});
            defer r.close();
            const rb = (try r.next(a)).?;
            try std.testing.expectEqualStrings(c.s, rb.columns[0].data.bytes.at(0));
        } else {
            try std.testing.expectError(error.XlsxCellTooLong, got);
            w.abort();
        }
    }
}

test "decimals a double holds exactly" {
    try std.testing.expect(decimal.exactInDouble(123456789012345));
    try std.testing.expect(decimal.exactInDouble(-12345678901234500000));
    try std.testing.expect(!decimal.exactInDouble(1234567890123456));
}

const Dump = struct {
    pub const null_text = "∅";
    pub const bool_true = "true";
    pub const bool_false = "false";

    pub fn string(_: Dump, w: *std.Io.Writer, s: []const u8) !void {
        try w.writeAll(s);
    }
};

test "the written formats read back as dates, timestamps and times" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = ar.allocator();
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "golden.xlsx" });

    const batches = try celltext.goldenBatches(a);
    for (batches[0 .. batches.len - 1]) |b| {
        if (b.schema.fields[0].ty.kind == .array) {
            try std.testing.expectError(error.XlsxNestedColumn, Writer.open(a, path, b.schema.*, "Sheet1"));
            continue;
        }
        const wr = try Writer.open(a, path, b.schema.*, "Dados");
        try wr.writeBatch(a, b);
        try wr.close();
        const r = try read.Reader.open(a, std.testing.allocator, path, .{ .sheet = "Dados" });
        defer r.close();
        var gw = std.Io.Writer.Allocating.init(a);
        try gw.writer.writeAll(@tagName(r.schema.fields[0].ty.kind));
        while (try r.next(a)) |rb| for (0..rb.len) |i| {
            try gw.writer.writeByte('|');
            try celltext.write(&gw.writer, &rb.columns[0], i, Dump{});
        };
        const want: []const u8 = switch (b.schema.fields[0].ty.kind) {
            .bool => "bool|true|false",
            .int => "string|0|-1|-9223372036854775808|9223372036854775807",
            .float => "float|0.1|0|-0|" ++ celltext.golden_tiny ++ "|1000000000000000000000|-2.25",
            .decimal => "string|123.45|-0.0007|5|0.12345|-18446744073709.551615|0.00",
            .string => "string||plain|tab\there|line\nbreak|cr\rlf\r\n|say \"hi\"|O'Brien|back\\slash\\|a\\Nb|\\N |NULL|a\x01b\x02c|a,b;c:d.e-1 0|\xc3\xb3 \xf0\x9f\x98\x80|\x00nul|sixteen bytes and more\x01",
            .bytes => "string||cGxhaW4=|AQIgXCAJCiI=|w7M=",
            .date => "string|1970-01-01|2024-01-01|0000-01-01|-5877641-06-24",
            .time => "time|00:00:00|01:02:03|23:59:59.999000",
            .timestamp => "string|1970-01-01 00:00:00|2024-01-01 00:00:00.123|1969-12-31 23:59:59.999|0001-01-01 00:00:00",
            else => unreachable,
        };
        try std.testing.expectEqualStrings(want, gw.written());
    }
}

test "temporal styles added by an earlier edit are used again, so styles do not grow edit after edit" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const base = "<styleSheet><cellXfs count=\"2\"><xf numFmtId=\"0\"/><xf numFmtId=\"4\" fontId=\"1\"/></cellXfs></styleSheet>";
    const once = try withTemporalStyles(a, base);
    try std.testing.expectEqualStrings("2", once.ids.date);
    try std.testing.expectEqualStrings("3", once.ids.timestamp);
    try std.testing.expectEqualStrings("4", once.ids.time);
    try std.testing.expect(std.mem.indexOf(u8, once.text, "count=\"5\"") != null);
    const twice = try withTemporalStyles(a, once.text);
    try std.testing.expectEqualStrings(once.text, twice.text);
    try std.testing.expectEqualStrings(once.ids.date, twice.ids.date);
    try std.testing.expectEqualStrings(once.ids.time, twice.ids.time);
}
