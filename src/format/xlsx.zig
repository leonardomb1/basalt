//! Excel workbook reader: `FROM 'report.xlsx'` / `.xlsm`, with
//! `WITH (sheet = '...', header = false, range = 'B3:F200')`.
//!
//! An `.xlsx` is a zip of XML parts, found through relationship files rather than
//! fixed names: `_rels/.rels` names the workbook, the workbook's own `.rels` its
//! sheets, shared strings and styles, and a target may be relative or absolute.
//! Each part streams out of the zip through `xml.Tokenizer`.
//!
//! A sheet is read twice. The first pass, at plan time, walks every row to decide
//! each column's type and the rows that hold data; the second streams those rows
//! out as batches, and a cell that no longer fits its column means the file changed
//! between the passes. Cells carry their own types, so the first pass is not a
//! sniff: a "Total" row of text at the bottom of a number column makes that column
//! text rather than failing the load where it is reached. What stays in memory is
//! the shared-string table (capped at `max_strings_bytes`), not the rows.
//!
//! Types, per column: numbers are `int` when every one is an integer below 2^53
//! (past that a double has already lost digits) and `float` otherwise; numbers in
//! a date or time format are `date`, `timestamp` or `time`; booleans are `bool`;
//! any text makes the column `string`, where a number keeps the digits the file
//! stored. Empty cells and error values (`#N/A`, `#DIV/0!`) are null. A formula
//! reads as the value Excel last computed; nothing is evaluated. An ISO 8601 date
//! cell, as strict OOXML writes it, stays text.

const std = @import("std");
const types = @import("../lang/types.zig");
const Value = @import("../exec/value.zig").Value;
const column = @import("../exec/column.zig");
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("../connect/driver.zig");
const zipsrc = @import("zipsrc.zig");
const xml = @import("xml.zig");

pub const Error = error{
    NotXlsx,
    XlsxSheetNotFound,
    XlsxBadRange,
    XlsxTooManyStrings,
    XlsxChanged,
    BadXml,
    XmlTokenTooLong,
    OutOfMemory,
    ReadFailed,
};

const max_strings_bytes: usize = 512 << 20;

const batch_rows = 4096;

const max_cols = 16384;
const max_rows = 1048576;

pub fn isPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".xlsx") or std.ascii.endsWithIgnoreCase(path, ".xlsm");
}

pub const Options = struct {
    sheet: ?[]const u8 = null,
    header: bool = true,
    range: ?Range = null,
};

pub const Range = struct {
    c0: u32,
    r0: u32,
    c1: ?u32 = null,
    r1: ?u32 = null,
};

pub fn parseRange(s: []const u8) ?Range {
    const colon = std.mem.indexOfScalar(u8, s, ':');
    const a = cellRef(if (colon) |c| s[0..c] else s) orelse return null;
    var r = Range{ .c0 = a.col, .r0 = a.row orelse 1 };
    if (colon) |c| {
        const b = cellRef(s[c + 1 ..]) orelse return null;
        if (b.col < r.c0) return null;
        r.c1 = b.col;
        if (b.row) |br| {
            if (br < r.r0) return null;
            r.r1 = br;
        }
    }
    return r;
}

const Ref = struct { col: u32, row: ?u32 };

/// `C5` is column 2, row 5; `C` alone is column 2, no row.
fn cellRef(s: []const u8) ?Ref {
    var i: usize = 0;
    var col: u32 = 0;
    while (i < s.len and std.ascii.isAlphabetic(s[i])) : (i += 1) {
        col = col * 26 + (std.ascii.toUpper(s[i]) - 'A' + 1);
        if (col > max_cols) return null;
    }
    if (i == 0) return null;
    if (i == s.len) return .{ .col = col - 1, .row = null };
    const row = std.fmt.parseInt(u32, s[i..], 10) catch return null;
    if (row == 0 or row > max_rows) return null;
    return .{ .col = col - 1, .row = row };
}

pub fn colName(arena: std.mem.Allocator, col: u32) ![]const u8 {
    var buf: [4]u8 = undefined;
    var n: usize = 0;
    var c = col + 1;
    while (c > 0) : (n += 1) {
        buf[3 - n] = @intCast('A' + (c - 1) % 26);
        c = (c - 1) / 26;
    }
    return arena.dupe(u8, buf[4 - n ..]);
}

const Fmt = enum { number, date, datetime, time };

/// Built-in format ids that are dates and times. 27-36 and 50-58 are East Asian
/// locale dates; 46 (`[h]:mm:ss`) is a duration and stays a number.
fn builtinFmt(id: u32) Fmt {
    return switch (id) {
        14...17, 27...36, 50...58 => .date,
        22 => .datetime,
        18...21, 45, 47 => .time,
        else => .number,
    };
}

/// Date and time tokens outside quoted text, `[...]` sections and escapes, in the first
/// section. `m` is a month beside `y`/`d` and a minute beside `h`/`s`; `[h]` is a duration.
fn customFmt(code: []const u8) Fmt {
    var date = false;
    var time = false;
    var i: usize = 0;
    while (i < code.len) : (i += 1) {
        const c = code[i];
        switch (c) {
            '"' => {
                i += 1;
                while (i < code.len and code[i] != '"') i += 1;
            },
            '\\', '_', '*' => i += 1,
            '[' => {
                const end = std.mem.indexOfScalarPos(u8, code, i, ']') orelse return .number;
                const inner = code[i + 1 .. end];
                if (inner.len > 0 and (std.ascii.toLower(inner[0]) == 'h' or std.ascii.toLower(inner[0]) == 's' or std.ascii.toLower(inner[0]) == 'm') and
                    std.mem.indexOfNone(u8, inner, "hHmMsS") == null) return .number;
                i = end;
            },
            ';' => break,
            else => switch (std.ascii.toLower(c)) {
                'y', 'd' => date = true,
                'h', 's' => time = true,
                else => {},
            },
        }
    }
    if (date and time) return .datetime;
    if (date) return .date;
    if (time) return .time;
    return .number;
}

const CellType = enum {
    number,
    shared,
    formula_text,
    inline_text,
    boolean,
    err,
    iso_date,

    /// Parsed where the tag is read: the attribute's bytes live in the tokenizer's
    /// buffer, which the next read may move.
    fn of(t: ?[]const u8) CellType {
        const v = t orelse return .number;
        if (std.mem.eql(u8, v, "s")) return .shared;
        if (std.mem.eql(u8, v, "str")) return .formula_text;
        if (std.mem.eql(u8, v, "inlineStr")) return .inline_text;
        if (std.mem.eql(u8, v, "b")) return .boolean;
        if (std.mem.eql(u8, v, "e")) return .err;
        if (std.mem.eql(u8, v, "d")) return .iso_date;
        return .number;
    }
};

const Cell = struct {
    col: u32,
    kind: enum { empty, number, text, boolean, err },
    text: []const u8 = "",
    num: f64 = 0,
    fmt: Fmt = .number,
};

const Stats = struct {
    text: bool = false,
    boolean: bool = false,
    number: bool = false,
    integral: bool = true,
    dated: bool = false,
    timed: bool = false,
    fraction: bool = false,
    any: bool = false,
    header: ?[]const u8 = null,

    fn note(self: *Stats, c: Cell) void {
        switch (c.kind) {
            .empty, .err => return,
            .text => self.text = true,
            .boolean => self.boolean = true,
            .number => switch (c.fmt) {
                .number => {
                    self.number = true;
                    if (c.num != @trunc(c.num) or @abs(c.num) >= 9007199254740992.0) self.integral = false;
                },
                .date, .datetime => {
                    self.dated = true;
                    if (c.fmt == .datetime or c.num != @trunc(c.num)) self.fraction = true;
                },
                .time => {
                    self.timed = true;
                    if (c.num >= 1) self.fraction = true;
                },
            },
        }
        self.any = true;
    }

    fn kind(self: Stats) types.TypeKind {
        if (!self.any or self.text) return .string;
        const nkinds = @as(u8, @intFromBool(self.boolean)) + @intFromBool(self.number) + @intFromBool(self.dated or self.timed);
        if (nkinds > 1) return .string;
        if (self.boolean) return .bool;
        if (self.number) return if (self.integral) .int else .float;
        if (self.timed and !self.dated and !self.fraction) return .time;
        if (self.dated and !self.timed and !self.fraction) return .date;
        return .timestamp;
    }
};

const Part = struct { arena: std.mem.Allocator, path: []const u8, member: *zipsrc.Member, tk: xml.Tokenizer };

fn openPart(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8, name: []const u8) !Part {
    const m = try zipsrc.openMember(arena, path, name);
    return .{ .arena = arena, .path = path, .member = m, .tk = xml.Tokenizer.init(gpa, m.reader) };
}

fn closePart(p: *Part) void {
    p.tk.deinit();
    p.member.close();
}

/// `target` from a relationship in part `base`: relative to `base`'s folder, or from
/// the package root when it starts with `/`. `..` steps out of a folder.
fn resolveTarget(arena: std.mem.Allocator, base: []const u8, target: []const u8) ![]const u8 {
    if (target.len > 0 and target[0] == '/') return arena.dupe(u8, target[1..]);
    const dir = if (std.mem.lastIndexOfScalar(u8, base, '/')) |i| base[0 .. i + 1] else "";
    var parts = std.array_list.Managed([]const u8).init(arena);
    var it = std.mem.tokenizeScalar(u8, dir, '/');
    while (it.next()) |p| try parts.append(p);
    var jt = std.mem.tokenizeScalar(u8, target, '/');
    while (jt.next()) |p| {
        if (std.mem.eql(u8, p, "..")) {
            _ = parts.pop();
        } else if (!std.mem.eql(u8, p, ".")) try parts.append(p);
    }
    return std.mem.join(arena, "/", parts.items);
}

fn relsOf(arena: std.mem.Allocator, part: []const u8) ![]const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, part, '/');
    const dir = if (slash) |i| part[0 .. i + 1] else "";
    const base = if (slash) |i| part[i + 1 ..] else part;
    return std.fmt.allocPrint(arena, "{s}_rels/{s}.rels", .{ dir, base });
}

const Rel = struct { id: []const u8, kind: []const u8, target: []const u8 };

/// The relationships in `rels`, targets resolved against `base`. A part that is not
/// there has none.
fn readRels(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8, rels: []const u8, base: []const u8) ![]Rel {
    var p = openPart(arena, gpa, path, rels) catch |e| switch (e) {
        error.ZipMemberNotFound => return &.{},
        else => return e,
    };
    defer closePart(&p);
    var out = std.array_list.Managed(Rel).init(arena);
    while (try p.tk.next()) |t| switch (t) {
        .open => |o| if (std.mem.eql(u8, o.name, "Relationship")) {
            if (o.attr("TargetMode")) |m| if (std.mem.eql(u8, m, "External")) continue;
            const ty = o.attr("Type") orelse continue;
            try out.append(.{
                .id = try decodeAttr(arena, o.attr("Id") orelse continue),
                .kind = try arena.dupe(u8, ty[if (std.mem.lastIndexOfScalar(u8, ty, '/')) |i| i + 1 else 0..]),
                .target = try resolveTarget(arena, base, try decodeAttr(arena, o.attr("Target") orelse continue)),
            });
        },
        else => {},
    };
    return out.items;
}

fn decodeAttr(arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
    if (std.mem.indexOfScalar(u8, raw, '&') == null) return arena.dupe(u8, raw);
    var out = std.array_list.Managed(u8).init(arena);
    try xml.decodeInto(&out, raw, false);
    return out.items;
}

fn relKind(rels: []const Rel, kind: []const u8) ?[]const u8 {
    for (rels) |r| if (std.mem.eql(u8, r.kind, kind)) return r.target;
    return null;
}

pub const Reader = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    path: []const u8,
    sheet_part: []const u8,
    strings: []const []const u8,
    styles: []const Fmt,
    date1904: bool,
    schema: types.Schema,
    c0: u32,
    c1: u32,
    first_row: u32,
    last_row: u32,
    kinds: []types.TypeKind,

    part: ?Part = null,
    next_row: u32 = 0,
    seen: u32 = 0,
    pending: ?u32 = null,
    row_cells: std.array_list.Managed(Cell),
    row_arena: std.heap.ArenaAllocator,
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8, opts: Options) !*Reader {
        const self = try arena.create(Reader);
        self.* = undefined;
        self.arena = arena;
        self.gpa = gpa;
        self.path = path;
        self.part = null;
        self.seen = 0;
        self.pending = null;
        self.done = false;
        self.row_cells = .init(gpa);
        errdefer self.row_cells.deinit();
        self.row_arena = std.heap.ArenaAllocator.init(gpa);
        errdefer self.row_arena.deinit();

        const root = try readRels(arena, gpa, path, "_rels/.rels", "");
        const wb_part = relKind(root, "officeDocument") orelse return error.NotXlsx;
        const wb_rels = try readRels(arena, gpa, path, try relsOf(arena, wb_part), wb_part);

        var chosen: ?[]const u8 = null;
        self.date1904 = false;
        {
            var p = try openPart(arena, gpa, path, wb_part);
            defer closePart(&p);
            while (try p.tk.next()) |t| switch (t) {
                .open => |o| if (std.mem.eql(u8, o.name, "workbookPr")) {
                    if (o.attr("date1904")) |v| self.date1904 = std.mem.eql(u8, v, "1") or std.ascii.eqlIgnoreCase(v, "true");
                } else if (std.mem.eql(u8, o.name, "sheet")) {
                    const name = try decodeAttr(arena, o.attr("name") orelse continue);
                    const rid = o.attr("id") orelse continue;
                    for (wb_rels) |r| {
                        if (!std.mem.eql(u8, r.id, rid)) continue;
                        if (!std.mem.eql(u8, r.kind, "worksheet")) break;
                        const want = if (opts.sheet) |w| std.ascii.eqlIgnoreCase(w, name) else chosen == null;
                        if (want and chosen == null) chosen = r.target;
                        break;
                    }
                },
                else => {},
            };
        }
        self.sheet_part = chosen orelse return error.XlsxSheetNotFound;

        self.strings = if (relKind(wb_rels, "sharedStrings")) |sp| try readStrings(arena, gpa, path, sp) else &.{};
        self.styles = if (relKind(wb_rels, "styles")) |sp| try readStyles(arena, gpa, path, sp) else &.{};
        try self.firstPass(opts);
        return self;
    }

    fn firstPass(self: *Reader, opts: Options) !void {
        const a = self.arena;
        var stats = std.array_list.Managed(Stats).init(a);
        var p = try openPart(a, self.gpa, self.path, self.sheet_part);
        defer closePart(&p);
        var row_cells = std.array_list.Managed(Cell).init(self.gpa);
        defer row_cells.deinit();
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();

        const rng = opts.range;
        var header_row: ?u32 = if (rng) |r| (if (opts.header) r.r0 else null) else null;
        const data_from: ?u32 = if (rng) |r| (if (opts.header) r.r0 + 1 else r.r0) else null;
        var first: ?u32 = null;
        var last: u32 = 0;
        var cmin: u32 = std.math.maxInt(u32);
        var cmax: u32 = 0;
        var prev_row: u32 = 0;

        while (try nextRow(&p.tk, &row_cells, scratch.allocator(), self, &prev_row)) |row| {
            defer _ = scratch.reset(.retain_capacity);
            if (rng) |r| {
                if (row < r.r0) continue;
                if (r.r1) |r1| if (row > r1) break;
            }
            var any = false;
            for (row_cells.items) |c| {
                if (c.kind == .empty) continue;
                if (rng) |r| {
                    if (c.col < r.c0) continue;
                    if (r.c1) |c1| if (c.col > c1) continue;
                }
                any = true;
            }
            if (!any and header_row == null) continue;
            if (header_row == null) {
                if (opts.header) header_row = row;
            }
            const is_header = header_row != null and row == header_row.?;
            if (!is_header and data_from != null and row < data_from.?) continue;
            for (row_cells.items) |c| {
                if (c.kind == .empty) continue;
                if (rng) |r| {
                    if (c.col < r.c0) continue;
                    if (r.c1) |c1| if (c.col > c1) continue;
                }
                if (c.col >= stats.items.len) try stats.appendNTimes(.{}, c.col + 1 - stats.items.len);
                cmin = @min(cmin, c.col);
                cmax = @max(cmax, c.col);
                if (is_header) {
                    stats.items[c.col].header = try a.dupe(u8, try renderText(scratch.allocator(), c, self.date1904));
                } else stats.items[c.col].note(c);
            }
            if (is_header) continue;
            if (first == null) first = row;
            if (any) last = row;
        }

        if (rng) |r| {
            cmin = r.c0;
            if (r.c1) |c1| cmax = c1 else if (cmax < cmin) cmax = cmin;
        } else if (cmin == std.math.maxInt(u32)) {
            cmin = 0;
            cmax = 0;
        }
        if (cmax >= stats.items.len) try stats.appendNTimes(.{}, cmax + 1 - stats.items.len);
        self.c0 = cmin;
        self.c1 = cmax;
        self.first_row = first orelse 1;
        self.last_row = if (first == null) 0 else last;

        const n = cmax - cmin + 1;
        const fields = try a.alloc(types.Schema.Field, n);
        self.kinds = try a.alloc(types.TypeKind, n);
        var seen = std.StringHashMap(void).init(a);
        for (fields, self.kinds, 0..) |*f, *k, i| {
            const st = stats.items[cmin + i];
            k.* = st.kind();
            const want = if (st.header) |h| (if (h.len > 0) h else try colName(a, @intCast(cmin + i))) else try colName(a, @intCast(cmin + i));
            var name = want;
            var dup: usize = 2;
            while (seen.contains(name)) : (dup += 1) name = try std.fmt.allocPrint(a, "{s}_{d}", .{ want, dup });
            try seen.put(name, {});
            f.* = .{ .name = name, .ty = types.Type.init(k.*).asNullable() };
        }
        self.schema = .{ .fields = fields };
    }

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn close(self: *Reader) void {
        if (self.part) |*p| closePart(p);
        self.part = null;
        self.row_cells.deinit();
        self.row_arena.deinit();
    }

    /// Rows the file left out between data rows are emitted empty, as are rows past the
    /// file's last up to `last_row`.
    fn nextBatch(self: *Reader, arena: std.mem.Allocator) !?Batch {
        if (self.done or self.last_row == 0) return null;
        if (self.part == null) {
            self.part = try openPart(self.arena, self.gpa, self.path, self.sheet_part);
            self.next_row = self.first_row;
        }
        const n = self.c1 - self.c0 + 1;
        const builders = try arena.alloc(column.Builder, n);
        for (builders, self.schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);
        var rows: usize = 0;
        while (rows < batch_rows and self.next_row <= self.last_row) {
            if (self.pending == null) {
                _ = self.row_arena.reset(.retain_capacity);
                self.pending = (try nextRow(&self.part.?.tk, &self.row_cells, self.row_arena.allocator(), self, &self.seen)) orelse max_rows + 1;
            }
            const num = self.pending.?;
            if (num < self.next_row) {
                self.pending = null;
                continue;
            }
            defer {
                self.next_row += 1;
                rows += 1;
            }
            if (num > self.next_row) {
                for (builders) |*b| try b.append(.null);
                continue;
            }
            self.pending = null;
            const cells = self.row_cells.items;
            var slot: usize = 0;
            for (builders, self.kinds, 0..) |*b, k, i| {
                const col: u32 = self.c0 + @as(u32, @intCast(i));
                while (slot < cells.len and cells[slot].col < col) slot += 1;
                const c: Cell = if (slot < cells.len and cells[slot].col == col) cells[slot] else .{ .col = col, .kind = .empty };
                try b.append(try self.convert(arena, c, k));
            }
        }
        if (self.next_row > self.last_row) self.done = true;
        if (rows == 0) return null;
        const cols = try arena.alloc(column.Column, n);
        for (builders, cols) |*b, *c| c.* = try b.finish();
        return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
    }

    fn convert(self: *Reader, arena: std.mem.Allocator, c: Cell, k: types.TypeKind) !Value {
        if (c.kind == .empty or c.kind == .err) return .null;
        return switch (k) {
            .string => .{ .string = try arena.dupe(u8, try renderText(arena, c, self.date1904)) },
            .bool => if (c.kind == .boolean) Value{ .bool = c.num != 0 } else error.XlsxChanged,
            .int => if (c.kind == .number) Value{ .int = @intFromFloat(c.num) } else error.XlsxChanged,
            .float => if (c.kind == .number) Value{ .float = c.num } else error.XlsxChanged,
            .date => if (c.kind == .number) Value{ .date = @intCast(serialDays(c.num, self.date1904)) } else error.XlsxChanged,
            .timestamp => if (c.kind == .number) Value{ .timestamp = serialMicros(c.num, self.date1904) } else error.XlsxChanged,
            .time => if (c.kind == .number) Value{ .time = dayMicros(c.num) } else error.XlsxChanged,
            else => error.XlsxChanged,
        };
    }

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.schema;
    }
    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.nextBatch(arena);
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        self.close();
    }
};

pub fn sheetNames(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    const root = try readRels(arena, gpa, path, "_rels/.rels", "");
    const wb_part = relKind(root, "officeDocument") orelse return error.NotXlsx;
    const wb_rels = try readRels(arena, gpa, path, try relsOf(arena, wb_part), wb_part);
    var names = std.array_list.Managed([]const u8).init(arena);
    var p = try openPart(arena, gpa, path, wb_part);
    defer closePart(&p);
    while (try p.tk.next()) |t| switch (t) {
        .open => |o| if (std.mem.eql(u8, o.name, "sheet")) {
            const rid = o.attr("id") orelse continue;
            for (wb_rels) |r| if (std.mem.eql(u8, r.id, rid) and std.mem.eql(u8, r.kind, "worksheet")) {
                try names.append(try decodeAttr(arena, o.attr("name") orelse break));
                break;
            };
        },
        else => {},
    };
    return names.items;
}

/// Days from the workbook's epoch (1899-12-30 or 1904-01-01) to 1970-01-01. Excel
/// counts a 29 February 1900 that never was, so 1900 serials below 61 read a day late.
fn epochOffset(date1904: bool) i64 {
    return if (date1904) 24107 else 25569;
}

fn serialDays(serial: f64, date1904: bool) i64 {
    return @as(i64, @intFromFloat(@floor(serial))) - epochOffset(date1904);
}

/// To the millisecond: a day's fraction in a double cannot say 23:59:59.5 exactly, and
/// Excel itself keeps times to the millisecond.
fn serialMicros(serial: f64, date1904: bool) i64 {
    const ms: i64 = @intFromFloat(@round(serial * 86_400_000.0));
    return (ms - epochOffset(date1904) * 86_400_000) * 1000;
}

fn dayMicros(serial: f64) i64 {
    const frac = serial - @floor(serial);
    const ms: i64 = @intFromFloat(@round(frac * 86_400_000.0));
    return ms * 1000;
}

fn renderText(arena: std.mem.Allocator, c: Cell, date1904: bool) ![]const u8 {
    return switch (c.kind) {
        .empty, .err => "",
        .text => c.text,
        .boolean => if (c.num != 0) "true" else "false",
        .number => switch (c.fmt) {
            .number => c.text,
            .date => fmtDate(arena, serialDays(c.num, date1904)),
            .datetime => fmtTimestamp(arena, serialMicros(c.num, date1904)),
            .time => if (c.num < 1) fmtTime(arena, dayMicros(c.num)) else fmtTimestamp(arena, serialMicros(c.num, date1904)),
        },
    };
}

/// ISO `YYYY-MM-DD` for days since 1970, before it too (the
/// civil-from-days algorithm), since a 1904 workbook's serials start in 1904.
fn fmtDate(arena: std.mem.Allocator, days: i64) ![]const u8 {
    const z = days + 719468;
    const era = @divFloor(z, 146097);
    const doe: u64 = @intCast(z - era * 146097);
    const yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    const doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp = (5 * doy + 2) / 153;
    const d = doy - (153 * mp + 2) / 5 + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    const y = @as(i64, @intCast(yoe)) + era * 400 + @intFromBool(m <= 2);
    return std.fmt.allocPrint(arena, "{d:0>4}-{d:0>2}-{d:0>2}", .{ @as(u64, @intCast(@max(y, 0))), m, d });
}

fn fmtTime(arena: std.mem.Allocator, us: i64) ![]const u8 {
    const t: u64 = @intCast(@max(us, 0));
    const s = t / 1_000_000;
    const ms = (t % 1_000_000) / 1000;
    if (ms == 0) return std.fmt.allocPrint(arena, "{d:0>2}:{d:0>2}:{d:0>2}", .{ s / 3600, s / 60 % 60, s % 60 });
    return std.fmt.allocPrint(arena, "{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}", .{ s / 3600, s / 60 % 60, s % 60, ms });
}

fn fmtTimestamp(arena: std.mem.Allocator, us: i64) ![]const u8 {
    const days = @divFloor(us, 86_400_000_000);
    return std.fmt.allocPrint(arena, "{s} {s}", .{ try fmtDate(arena, days), try fmtTime(arena, us - days * 86_400_000_000) });
}

/// The next `<row>` into `out` (cells in column order), or null after the last. Rows
/// and cells without an `r` attribute follow the one before.
fn nextRow(tk: *xml.Tokenizer, out: *std.array_list.Managed(Cell), scratch: std.mem.Allocator, rd: *const Reader, prev_row: *u32) !?u32 {
    out.clearRetainingCapacity();
    while (try tk.next()) |t| {
        const o = switch (t) {
            .open => |o| o,
            else => continue,
        };
        if (!std.mem.eql(u8, o.name, "row")) continue;
        const num = if (o.attr("r")) |r| (std.fmt.parseInt(u32, r, 10) catch return error.BadXml) else prev_row.* + 1;
        prev_row.* = num;
        if (o.self_closing) return num;
        var col: u32 = 0;
        var started = false;
        while (try tk.next()) |ct| switch (ct) {
            .open => |c| {
                if (!std.mem.eql(u8, c.name, "c")) {
                    try tk.skip(c);
                    continue;
                }
                const at = if (c.attr("r")) |r| ((cellRef(r) orelse return error.BadXml).col) else if (started) col + 1 else 0;
                started = true;
                col = at;
                var cell = Cell{ .col = at, .kind = .empty };
                const ty = CellType.of(c.attr("t"));
                if (c.attr("s")) |sv| {
                    const si = std.fmt.parseInt(usize, sv, 10) catch 0;
                    if (si < rd.styles.len) cell.fmt = rd.styles[si];
                }
                if (!c.self_closing) try readCell(tk, &cell, ty, scratch, rd);
                if (cell.kind == .number and cell.fmt != .number and cell.num < 0) cell.fmt = .number;
                try out.append(cell);
            },
            .close => |name| if (std.mem.eql(u8, name, "row")) break,
            else => {},
        };
        std.mem.sort(Cell, out.items, {}, struct {
            fn lt(_: void, x: Cell, y: Cell) bool {
                return x.col < y.col;
            }
        }.lt);
        return num;
    }
    return null;
}

/// The body of a `<c>`, read as its `t` says. A formula (`<f>`) is skipped: its cached
/// `<v>` is the value.
fn readCell(tk: *xml.Tokenizer, cell: *Cell, ty: CellType, scratch: std.mem.Allocator, rd: *const Reader) !void {
    while (try tk.next()) |t| switch (t) {
        .open => |o| {
            if (std.mem.eql(u8, o.name, "v")) {
                const v = try collectText(tk, o, scratch, false);
                switch (ty) {
                    .shared => {
                        const i = std.fmt.parseInt(usize, std.mem.trim(u8, v, " "), 10) catch return error.BadXml;
                        if (i >= rd.strings.len) return error.BadXml;
                        cell.* = .{ .col = cell.col, .kind = .text, .text = rd.strings[i] };
                    },
                    .formula_text, .inline_text => {
                        var out = std.array_list.Managed(u8).init(scratch);
                        try out.appendSlice(v);
                        xml.unescapeOoxml(&out, 0);
                        cell.* = .{ .col = cell.col, .kind = .text, .text = out.items };
                    },
                    .boolean => cell.* = .{ .col = cell.col, .kind = .boolean, .num = if (std.mem.eql(u8, std.mem.trim(u8, v, " "), "1")) 1 else 0 },
                    .err => cell.kind = .err,
                    .iso_date => cell.* = .{ .col = cell.col, .kind = .text, .text = v },
                    .number => {
                        const digits = std.mem.trim(u8, v, " \t\r\n");
                        if (digits.len == 0) continue;
                        const num = std.fmt.parseFloat(f64, digits) catch return error.BadXml;
                        cell.kind = .number;
                        cell.num = num;
                        cell.text = digits;
                    },
                }
            } else if (std.mem.eql(u8, o.name, "is")) {
                cell.* = .{ .col = cell.col, .kind = .text, .text = try collectText(tk, o, scratch, true) };
            } else try tk.skip(o);
        },
        .close => |name| if (std.mem.eql(u8, name, "c")) return,
        else => {},
    };
    return error.BadXml;
}

/// The text inside `open` up to its close, decoded. With `runs`, only `<t>` counts (a
/// rich-text string's runs, joined) and a phonetic hint (`<rPh>`) is skipped.
fn collectText(tk: *xml.Tokenizer, open: xml.Tag, arena: std.mem.Allocator, runs: bool) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    if (open.self_closing) return out.items;
    var in_t = !runs;
    var depth: usize = 1;
    while (try tk.next()) |t| switch (t) {
        .open => |o| {
            if (runs and std.mem.eql(u8, o.name, "rPh")) {
                try tk.skip(o);
                continue;
            }
            if (runs and std.mem.eql(u8, o.name, "t")) in_t = !o.self_closing;
            if (!o.self_closing) depth += 1;
        },
        .close => |name| {
            depth -= 1;
            if (depth == 0) return out.items;
            if (runs and std.mem.eql(u8, name, "t")) in_t = false;
        },
        .text => |s| if (in_t) try xml.decodeInto(&out, s, runs),
        .cdata => |s| if (in_t) try out.appendSlice(s),
    };
    return error.BadXml;
}

fn readStrings(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8, part: []const u8) ![]const []const u8 {
    var p = openPart(arena, gpa, path, part) catch |e| switch (e) {
        error.ZipMemberNotFound => return &.{},
        else => return e,
    };
    defer closePart(&p);
    var out = std.array_list.Managed([]const u8).init(arena);
    var bytes: usize = 0;
    while (try p.tk.next()) |t| switch (t) {
        .open => |o| if (std.mem.eql(u8, o.name, "si")) {
            const s = try collectText(&p.tk, o, arena, true);
            bytes += s.len + @sizeOf([]const u8);
            if (bytes > max_strings_bytes) return error.XlsxTooManyStrings;
            try out.append(s);
        },
        else => {},
    };
    return out.items;
}

/// Per cell format (`cellXfs`, by index), what its number format makes of a number.
/// `cellStyleXfs` is skipped: cells point into `cellXfs`.
fn readStyles(arena: std.mem.Allocator, gpa: std.mem.Allocator, path: []const u8, part: []const u8) ![]const Fmt {
    var p = openPart(arena, gpa, path, part) catch |e| switch (e) {
        error.ZipMemberNotFound => return &.{},
        else => return e,
    };
    defer closePart(&p);
    var custom = std.AutoHashMap(u32, Fmt).init(arena);
    var out = std.array_list.Managed(Fmt).init(arena);
    var in_xfs = false;
    while (try p.tk.next()) |t| switch (t) {
        .open => |o| {
            if (std.mem.eql(u8, o.name, "numFmt")) {
                const id = std.fmt.parseInt(u32, o.attr("numFmtId") orelse continue, 10) catch continue;
                const code = try decodeAttr(arena, o.attr("formatCode") orelse continue);
                try custom.put(id, customFmt(code));
            } else if (std.mem.eql(u8, o.name, "cellXfs")) {
                in_xfs = !o.self_closing;
            } else if (std.mem.eql(u8, o.name, "cellStyleXfs")) {
                try p.tk.skip(o);
            } else if (in_xfs and std.mem.eql(u8, o.name, "xf")) {
                const id = std.fmt.parseInt(u32, o.attr("numFmtId") orelse "0", 10) catch 0;
                try out.append(custom.get(id) orelse builtinFmt(id));
                if (!o.self_closing) try p.tk.skip(o);
            }
        },
        .close => |name| if (std.mem.eql(u8, name, "cellXfs")) {
            in_xfs = false;
        },
        else => {},
    };
    return out.items;
}

test "ranges, cell references, column names" {
    const r = parseRange("B3:F200").?;
    try std.testing.expectEqual(@as(u32, 1), r.c0);
    try std.testing.expectEqual(@as(u32, 3), r.r0);
    try std.testing.expectEqual(@as(u32, 5), r.c1.?);
    try std.testing.expectEqual(@as(u32, 200), r.r1.?);
    const open_end = parseRange("AA10:AC").?;
    try std.testing.expectEqual(@as(u32, 26), open_end.c0);
    try std.testing.expect(open_end.r1 == null);
    try std.testing.expect(parseRange("F3:B4") == null);
    try std.testing.expect(parseRange("3:4") == null);
    try std.testing.expect(parseRange("A0") == null);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectEqualStrings("A", try colName(ar.allocator(), 0));
    try std.testing.expectEqualStrings("Z", try colName(ar.allocator(), 25));
    try std.testing.expectEqualStrings("AA", try colName(ar.allocator(), 26));
    try std.testing.expectEqualStrings("XFD", try colName(ar.allocator(), 16383));
}

test "number formats: dates, times, durations, quoted text" {
    try std.testing.expectEqual(Fmt.date, customFmt("dd/mm/yyyy;@"));
    try std.testing.expectEqual(Fmt.datetime, customFmt("yyyy-mm-dd h:mm:ss"));
    try std.testing.expectEqual(Fmt.time, customFmt("hh:mm"));
    try std.testing.expectEqual(Fmt.number, customFmt("[h]:mm:ss"));
    try std.testing.expectEqual(Fmt.number, customFmt("0.00 \"dias\""));
    try std.testing.expectEqual(Fmt.number, customFmt("#,##0.00;[Red]-#,##0.00"));
    try std.testing.expectEqual(Fmt.date, customFmt("[$-416]d \"de\" mmmm \"de\" yyyy"));
    try std.testing.expectEqual(Fmt.number, customFmt("General"));
    try std.testing.expectEqual(Fmt.date, builtinFmt(14));
    try std.testing.expectEqual(Fmt.time, builtinFmt(21));
    try std.testing.expectEqual(Fmt.number, builtinFmt(46));
}

test "relationship targets resolve against their part" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("xl/worksheets/sheet1.xml", try resolveTarget(a, "xl/workbook.xml", "worksheets/sheet1.xml"));
    try std.testing.expectEqualStrings("xl/worksheets/sheet1.xml", try resolveTarget(a, "xl/workbook.xml", "/xl/worksheets/sheet1.xml"));
    try std.testing.expectEqualStrings("xl/sharedStrings.xml", try resolveTarget(a, "xl/sub/workbook.xml", "../sharedStrings.xml"));
    try std.testing.expectEqualStrings("xl/_rels/workbook.xml.rels", try relsOf(a, "xl/workbook.xml"));
}

/// `name:type,...` then one `|`-joined line per row, null as `∅`, for tests.
fn dumpFixture(arena: std.mem.Allocator, bytes: []const u8, opts: Options) ![]const u8 {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "f.xlsx", .data = bytes });
    const path = try tmp.dir.realpathAlloc(arena, "f.xlsx");
    const r = try Reader.open(arena, std.testing.allocator, path, opts);
    defer r.close();
    var out = std.array_list.Managed(u8).init(arena);
    const w = out.writer();
    for (r.schema.fields, 0..) |f, i| try w.print("{s}{s}:{s}", .{ if (i > 0) "," else "", f.name, @tagName(f.ty.kind) });
    try w.writeByte('\n');
    while (try r.nextBatch(arena)) |b| {
        for (0..b.len) |row| {
            for (b.columns, 0..) |*c, i| {
                if (i > 0) try w.writeByte('|');
                switch (c.getValue(row)) {
                    .null => try w.writeAll("∅"),
                    .int => |v| try w.print("{d}", .{v}),
                    .float => |v| try w.print("{d}", .{v}),
                    .bool => |v| try w.writeAll(if (v) "true" else "false"),
                    .string => |v| try w.writeAll(v),
                    .date => |v| try w.writeAll(try fmtDate(arena, v)),
                    .timestamp => |v| try w.writeAll(try fmtTimestamp(arena, v)),
                    .time => |v| try w.writeAll(try fmtTime(arena, v)),
                    else => try w.writeAll("?"),
                }
            }
            try w.writeByte('\n');
        }
    }
    return out.items;
}

test "openpyxl workbook: inline strings, typed columns, the first sheet, a styled empty tail" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const got = try dumpFixture(ar.allocator(), @embedFile("testdata/openpyxl.xlsx"), .{});
    try std.testing.expectEqualStrings(
        \\id:int,nome:string,valor:float,ativo:bool,dia:date,quando:timestamp,hora:time,misto:string
        \\1|São Paulo|10.5|true|2026-10-03|2026-10-03 12:30:15|08:15:00|7
        \\2|Ação, "aspas"|-3|false|2024-02-29|2024-02-29 23:59:59.500|23:00:30|texto
        \\3|∅|0.001|∅|∅|∅|∅|∅
        \\4|line
        \\break|12345678.25|true|1999-12-31|1999-12-31 00:00:00|00:00:01|2.5
        \\
    , got);
}

test "openpyxl workbook: a named sheet, a range under a title, no header" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bytes = @embedFile("testdata/openpyxl.xlsx");
    try std.testing.expectEqualStrings("codigo:string,qtd:int\nx1|5\nx2|7\n", try dumpFixture(a, bytes, .{ .sheet = "notas", .range = parseRange("A3:B5").? }));
    try std.testing.expectEqualStrings("A:string,B:int\nx1|5\nx2|7\n", try dumpFixture(a, bytes, .{ .sheet = "Notas", .header = false, .range = parseRange("A4:B").? }));
    try std.testing.expectError(error.XlsxSheetNotFound, dumpFixture(a, bytes, .{ .sheet = "Nope" }));
}

test "duckdb workbook: shared strings, relative targets" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const got = try dumpFixture(ar.allocator(), @embedFile("testdata/duckdb.xlsx"), .{});
    try std.testing.expectEqualStrings(
        \\id:int,nome:string,valor:float,dia:date
        \\1|nome 1|1.25|2026-01-02
        \\2|nome 2|2.5|2026-01-03
        \\3|nome 3|3.75|2026-01-04
        \\4|nome 4|5|2026-01-05
        \\5|nome 5|6.25|2026-01-06
        \\
    , got);
}

test "edge workbook: prefixes, rows and cells without r, escapes, 1904 dates, a chartsheet first" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const got = try dumpFixture(ar.allocator(), @embedFile("testdata/edge.xlsx"), .{});
    try std.testing.expectEqualStrings(
        "nome:string,2026:date,C:string,total:string,total_2:string,F:float,G:string\n" ++
            "rich|1904-01-01|∅|9007199254740993|merged|∅|∅\n" ++
            "line\rtwo|2027-01-02|in_x0041_line|1|∅|0.1|Silva & Filhos\r&lt;\n" ++
            "∅|∅|∅|∅|∅|∅|∅\n" ++
            "nome!|∅|∅|true|∅|∅|∅\n",
        got,
    );
}
