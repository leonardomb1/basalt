//! What Tab completion fetches: a connection's tables and columns, a file's columns,
//! an http connection's resources and local paths, cached per session.

const DeclStore = @import("entry.zig").DeclStore;
const Editor = @import("line.zig").Editor;
const Session = @import("repl.zig").Session;
const analyze = @import("../runtime/analyze.zig");
const complete = @import("complete.zig");
const parser = @import("../lang/sql_parser.zig");
const runtime = @import("../runtime/run.zig");
const std = @import("std");
const declareFrom = @import("cmd_complete.zig").declareFrom;
const writeOffer = @import("cmd_complete.zig").writeOffer;
const writeOfferIn = @import("cmd_complete.zig").writeOfferIn;

pub const Catalog = struct {
    arena: std.heap.ArenaAllocator,
    tables: std.StringHashMap([]const []const u8),
    columns: std.StringHashMap([]const complete.Column),

    pub fn init(gpa: std.mem.Allocator) Catalog {
        return .{ .arena = std.heap.ArenaAllocator.init(gpa), .tables = std.StringHashMap([]const []const u8).init(gpa), .columns = std.StringHashMap([]const complete.Column).init(gpa) };
    }
    pub fn deinit(self: *Catalog) void {
        self.tables.deinit();
        self.columns.deinit();
        self.arena.deinit();
    }
};

pub const Completer = struct {
    gpa: std.mem.Allocator,
    decls: *const DeclStore,
    catalog: *Catalog,
    connect: bool = true,
};

pub const Offer = struct { start: usize = 0, items: []const complete.Candidate = &.{} };

/// How Tab asks a source a question without printing anything; errors read as no rows.
fn fetchColumn(cx: *const Completer, select: []const u8) []const []const u8 {
    const a = cx.catalog.arena.allocator();
    const rows = fetchRows(cx, select);
    const out = a.alloc([]const u8, rows.len) catch return &.{};
    for (rows, out) |r, *o| o.* = r[0];
    return out;
}

/// Every row of `SELECT ...` as its cells, split as basalt's own CSV writes them:
/// on commas outside quotes, a doubled quote read as one.
fn fetchRows(cx: *const Completer, select: []const u8) []const []const []const u8 {
    const a = cx.catalog.arena.allocator();
    var scratch = std.heap.ArenaAllocator.init(cx.gpa);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var path_buf: [96]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/tmp/basalt-tab-{d}-{d}.csv", .{ std.os.linux.getpid(), std.time.milliTimestamp() }) catch return &.{};
    defer std.fs.cwd().deleteFile(path) catch {};

    var text = std.array_list.Managed(u8).init(sa);
    for (cx.decls.items.items) |e| {
        text.appendSlice(e.text) catch return &.{};
        text.appendSlice(";\n") catch return &.{};
    }
    text.writer().print("LOAD INTO '{s}' AS {s};", .{ path, select }) catch return &.{};
    var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
    const prog = parser.parseSource(sa, text.items, &pdiag) catch return &.{};
    var rdiag: runtime.Diag = .{};
    _ = runtime.run(cx.gpa, prog, .{ .log = .{ .quiet = true, .summary = .none } }, &rdiag) catch return &.{};
    const data = std.fs.cwd().readFileAlloc(sa, path, 1 << 22) catch return &.{};

    var out = std.array_list.Managed([]const []const u8).init(a);
    var lines = std.mem.splitScalar(u8, data, '\n');
    _ = lines.next();
    while (lines.next()) |ln| {
        if (ln.len == 0) continue;
        out.append(csvCells(a, ln) catch return &.{}) catch return &.{};
    }
    return out.toOwnedSlice() catch &.{};
}

pub fn csvCells(a: std.mem.Allocator, line: []const u8) ![]const []const u8 {
    var cells = std.array_list.Managed([]const u8).init(a);
    var cell = std.array_list.Managed(u8).init(a);
    var quoted = false;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const c = line[i];
        if (quoted) {
            if (c != '"') {
                try cell.append(c);
            } else if (i + 1 < line.len and line[i + 1] == '"') {
                try cell.append('"');
                i += 1;
            } else quoted = false;
        } else if (c == '"') {
            quoted = true;
        } else if (c == ',') {
            try cells.append(try cell.toOwnedSlice());
        } else if (c != '\r') try cell.append(c);
    }
    try cells.append(try cell.toOwnedSlice());
    return cells.toOwnedSlice();
}

pub fn httpResources(arena: std.mem.Allocator, cx: *const Completer, conn: []const u8) !?[]const []const u8 {
    const is_http = for (cx.decls.items.items) |e| {
        if (e.kind == .connection and std.ascii.eqlIgnoreCase(e.name, conn))
            break std.ascii.eqlIgnoreCase(connTypeOf(e.text) orelse "", "http");
    } else false;
    if (!is_http) return null;
    var out = std.array_list.Managed([]const u8).init(arena);
    for (cx.decls.items.items) |e| {
        if (e.kind != .resource or e.name.len <= conn.len or e.name[conn.len] != '.') continue;
        if (std.ascii.eqlIgnoreCase(e.name[0..conn.len], conn)) try out.append(e.name[conn.len + 1 ..]);
    }
    return try out.toOwnedSlice();
}

pub fn connTables(cx: *const Completer, conn: []const u8) []const []const u8 {
    if (cx.catalog.tables.get(conn)) |t| return t;
    if (!cx.connect) return &.{};
    const a = cx.catalog.arena.allocator();
    const q = std.fmt.allocPrint(a, "SELECT table_schema || '.' || table_name AS t FROM {s}.QUERY($$SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW') AND TABLE_SCHEMA NOT IN ('information_schema', 'pg_catalog', 'mysql', 'performance_schema', 'sys', '_statistics_') ORDER BY 1, 2$$)", .{conn}) catch return &.{};
    const rows = fetchColumn(cx, q);
    cx.catalog.tables.put(a.dupe(u8, conn) catch return rows, rows) catch {};
    return rows;
}

/// Table names are queried in upper case, as `SHOW TABLES` spells them, and
/// aliased so every dialect answers the same names.
fn sourceColumns(cx: *const Completer, key: []const u8) []const complete.Column {
    if (cx.catalog.columns.get(key)) |c| return c;
    const a = cx.catalog.arena.allocator();
    var rows: []const complete.Column = &.{};
    if (key[0] == '\'') {
        var scratch = std.heap.ArenaAllocator.init(cx.gpa);
        defer scratch.deinit();
        const sa = scratch.allocator();
        blk: {
            const text = std.fmt.allocPrint(sa, "SELECT * FROM {s};", .{key}) catch break :blk;
            var pdiag: parser.Diagnostic = .{ .msg = "", .line = 0, .col = 0 };
            const prog = parser.parseSource(sa, text, &pdiag) catch break :blk;
            var adiag = analyze.Diag{};
            const plan = analyze.analyze(sa, prog, &adiag) catch break :blk;
            const schema = plan.outputs[0].source.schema orelse break :blk;
            const cols = a.alloc(complete.Column, schema.fields.len) catch break :blk;
            for (schema.fields, cols) |f, *c| c.* = .{
                .name = a.dupe(u8, f.name) catch break :blk,
                .type = f.ty.name(a) catch break :blk,
            };
            rows = cols;
        }
    } else {
        if (!cx.connect) return rows;
        var parts = std.mem.splitScalar(u8, key, '.');
        const conn = parts.next().?;
        const schema = parts.next() orelse return rows;
        const tbl = parts.next() orelse return rows;
        const q = std.fmt.allocPrint(a, "SELECT col_name, col_type FROM {s}.QUERY($$SELECT COLUMN_NAME AS col_name, DATA_TYPE AS col_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{s}' AND TABLE_NAME = '{s}' ORDER BY ORDINAL_POSITION$$)", .{ conn, schema, tbl }) catch return rows;
        const got = fetchRows(cx, q);
        const cols = a.alloc(complete.Column, got.len) catch return rows;
        for (got, cols) |r, *c| c.* = .{ .name = r[0], .type = if (r.len > 1) r[1] else "" };
        rows = cols;
    }
    cx.catalog.columns.put(a.dupe(u8, key) catch return rows, rows) catch {};
    return rows;
}

pub fn connTypeOf(text: []const u8) ?[]const u8 {
    var it = std.mem.tokenizeAny(u8, text, " \t\r\n(");
    while (it.next()) |w| {
        if (std.ascii.eqlIgnoreCase(w, "type")) return it.next();
    }
    return null;
}

pub fn suggest(ctx: *anyopaque, arena: std.mem.Allocator, text: []const u8, cursor: usize) anyerror!Editor.Suggestions {
    const sess: *Session = @ptrCast(@alignCast(ctx));
    const cx = sess.completer();
    const offer = try suggestFor(arena, &cx, text, cursor);
    const items = try arena.alloc([]const u8, offer.items.len);
    for (offer.items, items) |cand, *it| it.* = cand.text;
    return .{ .start = offer.start, .items = items };
}

/// Never asks for the columns of a name the cursor is still typing at the end of the
/// text, which would send a catalog query on every keystroke.
pub fn suggestFor(arena: std.mem.Allocator, cx: *const Completer, text: []const u8, cursor: usize) anyerror!Offer {
    var conns = std.array_list.Managed([]const u8).init(arena);
    var fns = std.array_list.Managed([]const u8).init(arena);
    var params = std.array_list.Managed([]const u8).init(arena);
    for (cx.decls.items.items) |e| switch (e.kind) {
        .connection => try conns.append(e.name),
        .function => try fns.append(e.name),
        .param, .let => try params.append(e.name),
        .endpoint, .resource => {},
    };

    var ctes = std.array_list.Managed([]const u8).init(arena);
    var i: usize = 0;
    while (i + 4 < text.len) : (i += 1) {
        const at_with = std.ascii.eqlIgnoreCase(text[i..@min(text.len, i + 4)], "with") and (i == 0 or !std.ascii.isAlphanumeric(text[i - 1]));
        if (!(at_with or text[i] == ',')) continue;
        var j = if (at_with) i + 4 else i + 1;
        while (j < text.len and (text[j] == ' ' or text[j] == '\n')) j += 1;
        const ns = j;
        while (j < text.len and (std.ascii.isAlphanumeric(text[j]) or text[j] == '_')) j += 1;
        if (j == ns) continue;
        var k = j;
        while (k < text.len and text[k] == ' ') k += 1;
        if (k + 2 < text.len and std.ascii.eqlIgnoreCase(text[k .. k + 2], "as") and text[k + 2] == ' ') try ctes.append(text[ns..j]);
    }

    var tables = std.array_list.Managed(complete.ConnTables).init(arena);
    var columns = std.array_list.Managed(complete.Column).init(arena);
    for (conns.items) |c| {
        var pos: usize = 0;
        var wanted = false;
        while (std.mem.indexOfPos(u8, text, pos, c)) |p| : (pos = p + c.len) {
            if (p > 0 and (std.ascii.isAlphanumeric(text[p - 1]) or text[p - 1] == '_')) continue;
            if (p + c.len >= text.len or text[p + c.len] != '.') continue;
            wanted = true;
            var e = p + c.len + 1;
            var dots: usize = 0;
            while (e < text.len and (std.ascii.isAlphanumeric(text[e]) or text[e] == '_' or text[e] == '.')) : (e += 1) {
                if (text[e] == '.') dots += 1;
            }
            const typing = cursor > p and cursor <= e;
            if (dots == 1 and !typing and (e == text.len or text[e] != '(')) for (sourceColumns(cx, text[p..e])) |col| try columns.append(col);
        }
        if (wanted) try tables.append(.{ .conn = c, .tables = try httpResources(arena, cx, c) orelse connTables(cx, c) });
    }
    var q: usize = 0;
    while (std.mem.indexOfScalarPos(u8, text, q, '\'')) |open| {
        const close = std.mem.indexOfScalarPos(u8, text, open + 1, '\'') orelse break;
        q = close + 1;
        if (q >= cursor and open < cursor) continue;
        const lit = text[open..q];
        const file_like = for ([_][]const u8{ ".csv'", ".parquet'", ".gz'", ".zst'", ".arrow'", ".arrows'", ".feather'", ".ipc'" }) |ext| {
            if (std.ascii.endsWithIgnoreCase(lit, ext)) break true;
        } else false;
        if (file_like)
            for (sourceColumns(cx, lit)) |col| try columns.append(col);
    }

    const r = try complete.complete(arena, .{
        .connections = conns.items,
        .ctes = ctes.items,
        .functions = fns.items,
        .params = params.items,
        .tables = tables.items,
        .columns = columns.items,
    }, text, cursor);
    switch (r) {
        .none => return .{ .start = complete.wordStart(text, cursor) },
        .candidates => |c| return .{ .start = c.start, .items = c.items },
        .path => |p| {
            const paths = try listPaths(arena, p.partial);
            const items = try arena.alloc(complete.Candidate, paths.len);
            for (paths, items) |pth, *it| it.* = .{ .text = pth, .kind = .path };
            return .{ .start = p.start, .items = items };
        },
    }
}

fn listPaths(arena: std.mem.Allocator, partial: []const u8) ![]const []const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, partial, '/');
    const dir_part = if (slash) |s| partial[0 .. s + 1] else "";
    const name_part = if (slash) |s| partial[s + 1 ..] else partial;
    var dir = std.fs.cwd().openDir(if (dir_part.len == 0) "." else dir_part, .{ .iterate = true }) catch return &.{};
    defer dir.close();
    var out = std.array_list.Managed([]const u8).init(arena);
    var it = dir.iterate();
    while (try it.next()) |e| {
        if (name_part.len == 0 and e.name[0] == '.') continue;
        if (!std.mem.startsWith(u8, e.name, name_part)) continue;
        try out.append(try std.fmt.allocPrint(arena, "{s}{s}{s}", .{ dir_part, e.name, if (e.kind == .directory) "/" else "" }));
    }
    std.mem.sort([]const u8, out.items, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);
    return out.toOwnedSlice();
}

test "suggestFor: a half-typed script's own declarations and a local file's columns" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "order_id,order_total\n1,2\n" });
    const dir = try tmp.dir.realpathAlloc(a, ".");

    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    var catalog = Catalog.init(std.testing.allocator);
    defer catalog.deinit();
    const text = try std.fmt.allocPrint(a, "CREATE FUNCTION tax(x) AS x * 2;\nPARAM since DATE;\nSELECT ta FROM '{s}/t.csv' WHERE order_", .{dir});
    try declareFrom(&decls, a, text);
    const cx = Completer{ .gpa = std.testing.allocator, .decls = &decls, .catalog = &catalog, .connect = false };

    const at_fn = std.mem.indexOf(u8, text, "SELECT ta").? + "SELECT ta".len;
    const fns = try suggestFor(a, &cx, text, at_fn);
    try std.testing.expectEqualStrings("tax", fns.items[0].text);
    try std.testing.expectEqual(complete.Kind.function, fns.items[0].kind);

    const cols = try suggestFor(a, &cx, text, text.len);
    try std.testing.expectEqual(@as(usize, 2), cols.items.len);
    try std.testing.expectEqualStrings("order_id", cols.items[0].text);
    try std.testing.expectEqual(complete.Kind.column, cols.items[1].kind);
    try std.testing.expectEqual(text.len - "order_".len, cols.start);

    var aw = std.Io.Writer.Allocating.init(a);
    try writeOffer(&aw.writer, cols, text.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, aw.written(), .{});
    try std.testing.expectEqual(@as(usize, 2), parsed.value.object.get("items").?.array.items.len);
}

test "an offer with no candidates still starts at the word being typed, not the top of the cell" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    var catalog = Catalog.init(std.testing.allocator);
    defer catalog.deinit();
    const cx = Completer{ .gpa = std.testing.allocator, .decls = &decls, .catalog = &catalog, .connect = false };

    const text = "SELECT * FROM sal";
    const none = try suggestFor(a, &cx, text, text.len);
    try std.testing.expectEqual(@as(usize, 0), none.items.len);
    try std.testing.expectEqual(@as(usize, 14), none.start);

    const gap = try suggestFor(a, &cx, "SELECT ", 7);
    try std.testing.expectEqual(@as(usize, 0), gap.items.len);
    try std.testing.expectEqual(@as(usize, 7), gap.start);

    const wide = "SELECT 'é' FROM sal";
    const w = try suggestFor(a, &cx, wide, wide.len);
    var aw = std.Io.Writer.Allocating.init(a);
    try writeOfferIn(&aw.writer, w, wide.len, wide, true);
    try std.testing.expectEqualStrings("{\"start\":16,\"end\":19,\"items\":[]}", aw.written());
}

test "a file's columns are offered with their type, in the offer and in its JSON" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "name,amount,when_at\nx,1.5,2024-01-02\n" });
    const dir = try tmp.dir.realpathAlloc(a, ".");

    var decls = DeclStore.init(std.testing.allocator);
    defer decls.deinit();
    var catalog = Catalog.init(std.testing.allocator);
    defer catalog.deinit();
    const cx = Completer{ .gpa = std.testing.allocator, .decls = &decls, .catalog = &catalog, .connect = false };

    const text = try std.fmt.allocPrint(a, "SELECT * FROM '{s}/t.csv' WHERE na", .{dir});
    const n = try suggestFor(a, &cx, text, text.len);
    try std.testing.expectEqual(@as(usize, 1), n.items.len);
    try std.testing.expectEqual(complete.Kind.column, n.items[0].kind);
    try std.testing.expectEqualStrings("string", n.items[0].detail);

    var aw = std.Io.Writer.Allocating.init(a);
    try writeOffer(&aw.writer, n, text.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, aw.written(), .{});
    const item = parsed.value.object.get("items").?.array.items[0].object;
    try std.testing.expectEqualStrings("string", item.get("detail").?.string);

    const wtext = try std.fmt.allocPrint(a, "SELECT * FROM '{s}/t.csv' WHERE wh", .{dir});
    const w = try suggestFor(a, &cx, wtext, wtext.len);
    var found = false;
    for (w.items) |c| if (std.mem.eql(u8, c.text, "when_at")) {
        found = true;
        try std.testing.expectEqual(complete.Kind.column, c.kind);
        try std.testing.expectEqualStrings("date", c.detail);
    };
    try std.testing.expect(found);
}

test "csvCells: quoted cells keep their commas and doubled quotes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const c = try csvCells(ar.allocator(), "amt,\"numeric(10,2)\",\"say \"\"hi\"\"\"\r");
    try std.testing.expectEqual(@as(usize, 3), c.len);
    try std.testing.expectEqualStrings("amt", c[0]);
    try std.testing.expectEqualStrings("numeric(10,2)", c[1]);
    try std.testing.expectEqualStrings("say \"hi\"", c[2]);
}
