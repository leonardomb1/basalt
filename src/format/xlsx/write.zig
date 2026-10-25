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
const eval = @import("../../exec/eval.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const driver = @import("../../connect/driver.zig");
const Output = @import("../output.zig").Output;
const zipwrite = @import("../compress/zipwrite.zig");
const read = @import("read.zig");
const escape = @import("../xml/escape.zig");

pub const max_cols = 16384;
pub const max_rows = 1048576;
pub const max_cell_chars = 32767;
pub const content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";

const epoch_days = 25569;
const first_day = 61 - epoch_days;
const last_day = 2958465 - epoch_days;
const day_ms = 86_400_000;

const style_date = "1";
const style_timestamp = "2";
const style_time = "3";

/// Why Excel would refuse `name` as a sheet name, or null.
pub fn sheetNameProblem(name: []const u8) ?[]const u8 {
    if (name.len == 0) return "a sheet name cannot be empty";
    if (!std.unicode.utf8ValidateSlice(name)) return "a sheet name must be valid UTF-8";
    if (utf16Len(name) > 31) return "a sheet name is at most 31 characters";
    if (std.mem.indexOfAny(u8, name, "[]:*?/\\") != null) return "a sheet name cannot hold any of `[ ] : * ? / \\`";
    for (name) |c| if (c < 0x20) return "a sheet name cannot hold a control character";
    if (name[0] == '\'' or name[name.len - 1] == '\'') return "a sheet name cannot start or end with `'`";
    if (std.ascii.eqlIgnoreCase(name, "History")) return "`History` is a name Excel reserves";
    return null;
}

fn utf16Len(s: []const u8) usize {
    var n: usize = 0;
    for (s) |c| {
        if (c & 0xC0 != 0x80) n += 1;
        if (c >= 0xF0) n += 1;
    }
    return n;
}

pub const Writer = struct {
    out: *Output,
    zip: *zipwrite.Writer,
    sheet: *std.Io.Writer,
    refs: []const []const u8,
    row: u32 = 1,

    /// Refuses a schema Excel cannot hold before anything is written.
    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, sheet_name: []const u8) !*Writer {
        if (schema.fields.len > max_cols)
            return explainf(error.XlsxTooManyColumns, "an Excel sheet holds at most {d} columns; this result has {d}", .{ max_cols, schema.fields.len });
        for (schema.fields) |f| if (f.ty.kind == .array or f.ty.kind == .@"struct")
            return explainf(error.XlsxNestedColumn, "an Excel sheet cannot hold the array or struct column `{s}`", .{f.name});
        if (sheetNameProblem(sheet_name)) |why| return explainf(error.XlsxBadSheetName, "sheet `{s}`: {s}", .{ sheet_name, why });

        const self = try arena.create(Writer);
        const out = try Output.open(arena, path, content_type, .truncate);
        errdefer out.abort();
        const refs = try arena.alloc([]const u8, schema.fields.len);
        for (refs, 0..) |*r, i| r.* = try read.colName(arena, @intCast(i));
        const z = try zipwrite.Writer.init(arena, out.raw());
        errdefer z.deinit();
        self.* = .{ .out = out, .zip = z, .sheet = undefined, .refs = refs };
        self.start(arena, schema, sheet_name) catch |e| return out.specific(e);
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
        self.sheet = try self.zip.begin("xl/worksheets/sheet1.xml");
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
        if (!c.validity.get(r)) return;
        const w = self.sheet;
        switch (c.ty.kind) {
            .string => {
                try w.print("<c r=\"{s}{d}\"", .{ ref, self.row });
                return self.text(c.data.bytes.at(r));
            },
            .bytes => {
                const b = c.data.bytes.at(r);
                if (std.base64.standard.Encoder.calcSize(b.len) > max_cell_chars) return tooLong(self.row, ref);
                try w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>", .{ ref, self.row });
                try std.base64.standard.Encoder.encodeWriter(w, b);
                return w.writeAll("</t></is></c>");
            },
            .bool => return w.print("<c r=\"{s}{d}\" t=\"b\"><v>{d}</v></c>", .{ ref, self.row, @intFromBool(c.data.b[r]) }),
            .int => {
                const v = c.data.i64[r];
                if (@abs(v) <= 1 << 53) return w.print("<c r=\"{s}{d}\"><v>{d}</v></c>", .{ ref, self.row, v });
                return w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>{d}</t></is></c>", .{ ref, self.row, v });
            },
            .float => {
                const x = c.data.f64[r];
                if (!std.math.isFinite(x)) return;
                const ax = @abs(x);
                if (ax == 0 or (ax >= 1e-6 and ax < 1e15)) return w.print("<c r=\"{s}{d}\"><v>{d}</v></c>", .{ ref, self.row, x });
                return w.print("<c r=\"{s}{d}\"><v>{e}</v></c>", .{ ref, self.row, x });
            },
            .decimal => {
                const d = c.decAt(r);
                try w.print("<c r=\"{s}{d}\"", .{ ref, self.row });
                if (exactInDouble(d.unscaled)) {
                    try w.writeAll("><v>");
                    try format.writeDecimal(w, d.unscaled, d.scale);
                    return w.writeAll("</v></c>");
                }
                try w.writeAll(" t=\"inlineStr\"><is><t>");
                try format.writeDecimal(w, d.unscaled, d.scale);
                return w.writeAll("</t></is></c>");
            },
            .date => {
                const days: i64 = c.data.i32[r];
                if (days >= first_day and days <= last_day)
                    return w.print("<c r=\"{s}{d}\" s=\"" ++ style_date ++ "\"><v>{d}</v></c>", .{ ref, self.row, days + epoch_days });
                try w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>", .{ ref, self.row });
                try format.writeDate(w, days);
                return w.writeAll("</t></is></c>");
            },
            .timestamp => {
                const us = c.data.i64[r];
                const ms = @divFloor(us, 1000);
                const days = @divFloor(ms, day_ms);
                if (days >= first_day and days <= last_day) {
                    const serial = @as(f64, @floatFromInt(ms + epoch_days * day_ms)) / day_ms;
                    return w.print("<c r=\"{s}{d}\" s=\"" ++ style_timestamp ++ "\"><v>{d}</v></c>", .{ ref, self.row, serial });
                }
                try w.print("<c r=\"{s}{d}\" t=\"inlineStr\"><is><t>", .{ ref, self.row });
                try format.writeTimestamp(w, us);
                return w.writeAll("</t></is></c>");
            },
            .time => {
                const ms = @mod(@divFloor(c.data.i64[r], 1000), day_ms);
                const serial = @as(f64, @floatFromInt(ms)) / day_ms;
                return w.print("<c r=\"{s}{d}\" s=\"" ++ style_time ++ "\"><v>{d}</v></c>", .{ ref, self.row, serial });
            },
            .array, .@"struct" => unreachable,
        }
    }

    /// Finishes a cell opened up to its reference as an inline string.
    fn text(self: *Writer, s: []const u8) !void {
        const w = self.sheet;
        const valid = std.unicode.utf8ValidateSlice(s);
        if (s.len > max_cell_chars and charsOf(s, valid) > max_cell_chars) return tooLong(self.row, "");
        const preserve = s.len > 0 and (isSpace(s[0]) or isSpace(s[s.len - 1]));
        try w.writeAll(if (preserve) " t=\"inlineStr\"><is><t xml:space=\"preserve\">" else " t=\"inlineStr\"><is><t>");
        try escape.text(w, s, .{ .ooxml = true });
        try w.writeAll("</t></is></c>");
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

fn explainf(e: anyerror, comptime fmt: []const u8, args: anytype) anyerror {
    var buf: [480]u8 = undefined;
    return eval.explain(e, std.fmt.bufPrint(&buf, fmt, args) catch fmt);
}

fn tooLong(row: u32, ref: []const u8) anyerror {
    return explainf(error.XlsxCellTooLong, "an Excel cell holds at most {d} characters; the one at row {d}{s}{s} has more", .{ max_cell_chars, row, if (ref.len > 0) ", column " else "", ref });
}

fn charsOf(s: []const u8, valid: bool) usize {
    if (valid) return utf16Len(s);
    var n: usize = 0;
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| n += utf16Len(t);
    return n;
}

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

/// Whether a double holds the decimal exactly enough to print back: at most 15
/// significant digits.
fn exactInDouble(unscaled: i128) bool {
    var m: u128 = @abs(unscaled);
    while (m != 0 and m % 10 == 0) m /= 10;
    return m < 1_000_000_000_000_000;
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
    for (fields, 0..) |*f, i| f.* = .{ .name = try std.fmt.allocPrint(a, "c{d}", .{i}), .ty = ty };
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
    try std.testing.expect(exactInDouble(123456789012345));
    try std.testing.expect(exactInDouble(-12345678901234500000));
    try std.testing.expect(!exactInDouble(1234567890123456));
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
