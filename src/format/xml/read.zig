//! XML as rows, for a file, stdin or any location a CSV reads from. A row is an
//! element named by `row` wherever it sits, or, without `row`, each child of the
//! document's root. Its columns are its attributes and its child elements:
//!
//! - a child holding only text is a column of that text, and its attributes are
//!   columns `child@attr`; an attribute of the row named like one of its children is
//!   `@attr`, so neither hides the other;
//! - a child holding elements, or one repeated, is a column of JSON text (`@attr`
//!   keys, `#text` for mixed text, a repeated name an array), which `json_get` and
//!   `JSON_EACH` take apart, as a JSON file's nested values;
//! - text directly inside the row, beside its children, is a column named for the row.
//!
//! Columns come from the first `sample` rows in the order first met; a name first
//! met past the sample is dropped, as a JSON file's key is. Text is typed as a CSV's
//! cells are (`texttype.zig`): a child left empty (`<a/>`) is the empty string in a
//! text column and NULL in a typed one, an absent child or `xsi:nil="true"` is NULL,
//! and a value past the sample its typed column cannot read is an error.
//!
//! Names lose their namespace prefix and have FOR XML's `_xHHHH_` decoded. The
//! document's declared encoding is honoured (UTF-8, ISO-8859-1, windows-1252, or
//! `encoding` overriding it); a UTF-16 document is refused, and text that is not
//! valid UTF-8 is repaired. The input streams, a row at a time.

const std = @import("std");
const types = @import("../../lang/types.zig");
const Batch = @import("../../exec/batch.zig").Batch;
const column = @import("../../exec/column.zig");
const celltext = @import("../../exec/celltext.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const csv = @import("../csv.zig");
const jsontext = @import("../../exec/jsontext.zig");
const texttype = @import("../texttype.zig");
const token = @import("token.zig");
const escape = @import("escape.zig");

pub const Encoding = csv.Encoding;

pub const Options = struct {
    row: ?[]const u8 = null,
    encoding: ?Encoding = null,
    sample: usize = 1024,
    batch_rows: usize = 2048,
    schema: ?types.Schema = null,
    text: []const []const u8 = &.{},
    source_name: []const u8 = "",
};

const Kind = enum { elem, attr, child_attr };

const Cell = struct {
    kind: Kind,
    name: []const u8,
    value: ?[]const u8 = null,
    group: []const *const Node = &.{},
};

const Rendered = struct { value: ?[]const u8, nested: bool };

const Node = struct {
    name: []const u8,
    attrs: []const Attr,
    text: []const u8,
    children: []const *const Node,
    nil: bool,
};

const Attr = struct { name: []const u8, value: []const u8 };

pub const Reader = struct {
    arena: std.mem.Allocator,
    tz: token.Tokenizer,
    opts: Options,
    enc: Encoding,
    schema: types.Schema = .{ .fields = &.{} },
    index: std.StringHashMapUnmanaged(usize) = .empty,
    arrays: std.StringHashMapUnmanaged(void) = .empty,
    names: std.StringHashMapUnmanaged([]const u8) = .empty,
    depth: usize = 0,
    rooted: bool = false,
    row_bytes: usize = 0,
    row_elems: usize = 0,
    row_name: []const u8 = "",
    held: []const []const Cell = &.{},
    held_at: usize = 0,
    rows_seen: u64 = 0,
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, in: *std.Io.Reader, opts: Options) !*Reader {
        const self = try arena.create(Reader);
        self.* = .{ .arena = arena, .tz = undefined, .opts = opts, .enc = .utf8 };
        self.enc = opts.encoding orelse try declared(in);
        self.tz = token.Tokenizer.init(gpa, in);
        errdefer self.tz.deinit();
        if (opts.schema) |sch| {
            try self.setSchema(sch);
            return self;
        }
        var held = std.array_list.Managed([]const Cell).init(arena);
        while (held.items.len < opts.sample) {
            const row = (try self.nextRow(arena)) orelse break;
            try held.append(row);
        }
        self.held = held.items;
        for (held.items) |row| for (row) |c| if (c.group.len > 0) {
            if (c.group.len > 1) try self.arrays.put(arena, c.name, {});
            for (c.group) |n| try self.noteArrays(arena, n, c.name);
        };
        try self.setSchema(try self.infer(arena, held.items, held.items.len < opts.sample));
        return self;
    }

    /// Notes each path below `n` (`items/item`) where a name repeats among siblings, so
    /// it is an array in every row, one item or many, as the sample showed it.
    fn noteArrays(self: *Reader, arena: std.mem.Allocator, n: *const Node, path: []const u8) !void {
        for (try groupByName(arena, n.children)) |group| {
            const sub = try std.fmt.allocPrint(arena, "{s}/{s}", .{ path, group[0].name });
            if (group.len > 1) try self.arrays.put(arena, sub, {});
            for (group) |c| try self.noteArrays(arena, c, sub);
        }
    }

    /// A cell's value: an element's text, or JSON text for one that holds elements or
    /// is an array; an attribute's text.
    fn render(self: *const Reader, arena: std.mem.Allocator, c: Cell) !Rendered {
        if (c.group.len == 0) return .{ .value = c.value, .nested = false };
        const array = c.group.len > 1 or self.arrays.contains(c.name);
        const first = c.group[0];
        if (!array) {
            if (first.nil) return .{ .value = null, .nested = false };
            if (first.children.len == 0) return .{ .value = first.text, .nested = false };
        }
        var aw = std.Io.Writer.Allocating.init(arena);
        if (array) try aw.writer.writeByte('[');
        for (c.group, 0..) |n, k| {
            if (k > 0) try aw.writer.writeByte(',');
            try self.writeJson(arena, &aw.writer, n, c.name);
        }
        if (array) try aw.writer.writeByte(']');
        return .{ .value = aw.written(), .nested = true };
    }

    /// An element as JSON: a string when it is text alone, null when nil, else an
    /// object of `@attr` keys, its children by name (an array where the name repeats
    /// or repeated in the sample) and `#text`.
    fn writeJson(self: *const Reader, arena: std.mem.Allocator, w: *std.Io.Writer, n: *const Node, path: []const u8) !void {
        if (n.nil) return w.writeAll("null");
        if (n.attrs.len == 0 and n.children.len == 0) return jsontext.string(w, n.text);
        try w.writeByte('{');
        var first = true;
        for (n.attrs) |a| {
            try sep(w, &first);
            try writeKey(w, "@", a.name);
            try jsontext.string(w, a.value);
        }
        for (try groupByName(arena, n.children)) |group| {
            const c = group[0];
            const sub = try std.fmt.allocPrint(arena, "{s}/{s}", .{ path, c.name });
            try sep(w, &first);
            try writeKey(w, "", c.name);
            if (group.len == 1 and !self.arrays.contains(sub)) {
                try self.writeJson(arena, w, c, sub);
                continue;
            }
            try w.writeByte('[');
            for (group, 0..) |o, k| {
                if (k > 0) try w.writeByte(',');
                try self.writeJson(arena, w, o, sub);
            }
            try w.writeByte(']');
        }
        if (n.text.len > 0 and (n.children.len == 0 or !blank(n.text))) {
            try sep(w, &first);
            try writeKey(w, "", "#text");
            try jsontext.string(w, n.text);
        }
        try w.writeByte('}');
    }

    /// The columns of the sampled rows, in the order first met, typed from their text.
    fn infer(self: *Reader, arena: std.mem.Allocator, rows: []const []const Cell, whole: bool) !types.Schema {
        var keys = std.array_list.Managed(Cell).init(arena);
        var sniff = std.array_list.Managed(texttype.Column).init(arena);
        var seen = std.StringHashMapUnmanaged(usize).empty;
        var elems = std.StringHashMapUnmanaged(void).empty;
        for (rows) |row| for (row) |c| {
            const key = try keyOf(arena, c.kind, c.name);
            const gop = try seen.getOrPut(arena, key);
            if (!gop.found_existing) {
                gop.value_ptr.* = keys.items.len;
                try keys.append(c);
                try sniff.append(.{});
                if (c.kind == .elem) try elems.put(arena, c.name, {});
            }
            const r = try self.render(arena, c);
            const v = r.value orelse continue;
            if (r.nested) sniff.items[gop.value_ptr.*].note(v, true) else if (v.len > 0) sniff.items[gop.value_ptr.*].note(v, false);
        };
        const fields = try arena.alloc(types.Schema.Field, keys.items.len);
        for (fields, keys.items) |*f, k| f.* = .{
            .name = switch (k.kind) {
                .elem, .child_attr => k.name,
                .attr => if (elems.contains(k.name)) try std.fmt.allocPrint(arena, "@{s}", .{k.name}) else k.name,
            },
            .ty = undefined,
        };
        texttype.settle(fields, sniff.items, whole, self.opts.text);
        return .{ .fields = fields };
    }

    fn setSchema(self: *Reader, sch: types.Schema) !void {
        self.schema = sch;
        self.index = .empty;
        for (sch.fields, 0..) |f, i| try self.index.put(self.arena, f.name, i);
    }

    /// The column a cell lands in: an element by its name, a row's attribute by
    /// `@name` where a child took the name and else by its own, a child's attribute
    /// by `child@attr`.
    fn columnOf(self: *const Reader, c: Cell) ?usize {
        return switch (c.kind) {
            .elem, .child_attr => self.index.get(c.name),
            .attr => blk: {
                var buf: [256]u8 = undefined;
                if (c.name.len < buf.len) {
                    buf[0] = '@';
                    @memcpy(buf[1..][0..c.name.len], c.name);
                    if (self.index.get(buf[0 .. c.name.len + 1])) |i| break :blk i;
                }
                break :blk self.index.get(c.name);
            },
        };
    }

    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const builders = try arena.alloc(column.Builder, self.schema.fields.len);
        for (builders, self.schema.fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, self.opts.batch_rows);
        const vals = try arena.alloc(?Cell, builders.len);
        var n: usize = 0;
        while (n < self.opts.batch_rows) : (n += 1) {
            const row = if (self.held_at < self.held.len) blk: {
                self.held_at += 1;
                break :blk self.held[self.held_at - 1];
            } else (try self.nextRow(arena)) orelse {
                self.done = true;
                break;
            };
            @memset(vals, null);
            for (row) |c| if (self.columnOf(c)) |i| {
                vals[i] = c;
            };
            for (builders, vals, self.schema.fields) |*b, v, f| try self.put(arena, b, v, f.name);
        }
        if (n == 0) return null;
        const cols = try arena.alloc(column.Column, builders.len);
        for (builders, cols) |*b, *c| c.* = try b.finish();
        return .{ .schema = &self.schema, .columns = cols, .len = n };
    }

    fn put(self: *Reader, arena: std.mem.Allocator, b: *column.Builder, cell: ?Cell, name: []const u8) !void {
        const c = try self.render(arena, cell orelse return b.append(.null));
        const v = c.value orelse return b.append(.null);
        if (c.nested and b.ty.kind != .string) return self.mismatch(name, v, b.ty.kind, "nested or repeated elements");
        texttype.appendCell(b, v, v.len == 0 or c.nested) catch |e| switch (e) {
            error.TextTypeMismatch => return self.mismatch(name, v, b.ty.kind, "a value"),
            else => return e,
        };
    }

    fn mismatch(self: *Reader, name: []const u8, v: []const u8, kind: types.TypeKind, what: []const u8) anyerror {
        return eval.explain(error.TextTypeMismatch, std.fmt.allocPrint(self.arena, "{s}{s}XML row {d}: `{s}` holds {s} (`{s}`) that is not {s}, as its first rows typed it; read it with `CAST({s} AS VARCHAR)` to keep it as text", .{ self.opts.source_name, if (self.opts.source_name.len > 0) ": " else "", self.rows_seen, name, what, eval.clip(v), @tagName(kind), name }) catch "XML row holds a value its column cannot read");
    }

    /// The next row's cells, the document read up to it. A document that ends before
    /// its root closes, holds no element, or has text outside its root is not XML
    /// read whole, so it is an error rather than fewer rows.
    fn nextRow(self: *Reader, arena: std.mem.Allocator) !?[]const Cell {
        while (try self.tz.next()) |t| switch (t) {
            .text => |raw| if (self.depth == 0 and !blank(raw))
                return self.malformed("it holds text outside its root element; is it XML?"),
            .open => |o| {
                if (self.depth == 0 and self.rooted and self.opts.row == null)
                    return self.malformed("it has a second root element");
                self.rooted = true;
                const name = try self.nameOf(arena, o.name);
                const is_row = if (self.opts.row) |r| std.mem.eql(u8, name, r) else self.depth == 1;
                if (is_row) {
                    self.rows_seen += 1;
                    self.row_bytes = 0;
                    self.row_elems = 0;
                    self.row_name = name;
                    return try self.readRow(arena, o, name);
                }
                if (!o.self_closing) self.depth += 1;
            },
            .close => self.depth -|= 1,
            .cdata => {},
        };
        if (!self.rooted) return self.malformed("it holds no XML element");
        if (self.depth > 0) return self.malformed("it ends before its root element closes; is it cut short?");
        return null;
    }

    fn malformed(self: *Reader, why: []const u8) anyerror {
        return eval.explain(error.BadXml, std.fmt.allocPrint(self.arena, "{s}{s}{s}", .{ self.opts.source_name, if (self.opts.source_name.len > 0) ": " else "", why }) catch why);
    }

    fn readRow(self: *Reader, arena: std.mem.Allocator, o: token.Tag, name: []const u8) ![]const Cell {
        var cells = std.array_list.Managed(Cell).init(arena);
        const attrs = try self.attrsOf(arena, o);
        for (attrs) |a| try cells.append(.{ .kind = .attr, .name = a.name, .value = a.value });
        if (o.self_closing) return cells.items;
        const node = try self.readBody(arena, name, attrs, false, 0);
        for (try groupByName(arena, node.children)) |group| {
            const child = group[0];
            try cells.append(.{ .kind = .elem, .name = child.name, .group = group });
            if (group.len == 1 and child.children.len == 0)
                for (child.attrs) |a| try cells.append(.{ .kind = .child_attr, .name = try std.fmt.allocPrint(arena, "{s}@{s}", .{ child.name, a.name }), .value = a.value });
        }
        if (node.text.len > 0 and (node.children.len == 0 or !blank(node.text)))
            try cells.append(.{ .kind = .elem, .name = name, .value = node.text });
        return cells.items;
    }

    /// The rest of an element just opened, children and text, up to its close; a
    /// row nested past `max_depth` is refused, which bounds every walk of its tree.
    fn readBody(self: *Reader, arena: std.mem.Allocator, name: []const u8, attrs: []const Attr, nil: bool, depth: usize) anyerror!*Node {
        if (depth > max_depth)
            return self.malformed(std.fmt.allocPrint(self.arena, "row {d}, a `{s}` element, nests elements more than {d} deep", .{ self.rows_seen, self.row_name, max_depth }) catch "a row nests too deep");
        var children = std.array_list.Managed(*const Node).init(arena);
        var text = Text{};
        while (try self.tz.next()) |t| {
            self.row_bytes += switch (t) {
                .open => |c| c.name.len + c.attrs.len,
                .close => |c| c.len,
                .text, .cdata => |raw| raw.len,
            };
            if (t == .open) self.row_elems += 1;
            if (self.row_bytes > max_row_bytes or self.row_elems > max_row_elems)
                return self.malformed(std.fmt.allocPrint(self.arena, "row {d}, a `{s}` element, passes {d} MB or {d} elements, so it is not one row of many; name the repeating element with `WITH (row = '...')`", .{ self.rows_seen, self.row_name, max_row_bytes >> 20, max_row_elems }) catch "a row passes the size of one row");
            switch (t) {
                .open => |c| {
                    const cname = try self.nameOf(arena, c.name);
                    const cattrs = try self.attrsOf(arena, c);
                    const cnil = isNil(c);
                    if (c.self_closing) {
                        const leaf = try arena.create(Node);
                        leaf.* = .{ .name = cname, .attrs = cattrs, .text = "", .children = &.{}, .nil = cnil };
                        try children.append(leaf);
                    } else try children.append(try self.readBody(arena, cname, cattrs, cnil, depth + 1));
                },
                .text => |raw| try text.add(self, arena, raw, true),
                .cdata => |raw| try text.add(self, arena, raw, false),
                .close => break,
            }
        } else return self.malformed("it ends inside a row; is it cut short?");
        const node = try arena.create(Node);
        node.* = .{ .name = name, .attrs = attrs, .text = text.get(), .children = children.items, .nil = nil };
        return node;
    }

    /// An element's text, kept as one exact copy when it comes in one piece, as a
    /// leaf's does, and joined only when it comes in several.
    const Text = struct {
        one: []const u8 = "",
        many: ?std.array_list.Managed(u8) = null,

        fn add(t: *Text, r: *Reader, arena: std.mem.Allocator, raw: []const u8, entities: bool) !void {
            if (t.many) |*m| return r.appendText(arena, m, raw, entities);
            if (t.one.len == 0) {
                t.one = try r.textOf(arena, raw, entities);
                return;
            }
            var m = std.array_list.Managed(u8).init(arena);
            try m.appendSlice(t.one);
            try r.appendText(arena, &m, raw, entities);
            t.many = m;
        }

        fn get(t: *const Text) []const u8 {
            return if (t.many) |m| m.items else t.one;
        }
    };

    /// `raw` decoded as `appendText` does, copied once when nothing needs decoding.
    fn textOf(self: *Reader, arena: std.mem.Allocator, raw: []const u8, entities: bool) ![]const u8 {
        if (self.enc == .utf8 and (!entities or std.mem.indexOfScalar(u8, raw, '&') == null) and std.unicode.utf8ValidateSlice(raw))
            return arena.dupe(u8, raw);
        var m = std.array_list.Managed(u8).init(arena);
        try self.appendText(arena, &m, raw, entities);
        return m.items;
    }

    /// `raw` decoded from the document's encoding and, unless CDATA, its entities.
    fn appendText(self: *Reader, arena: std.mem.Allocator, out: *std.array_list.Managed(u8), raw: []const u8, entities: bool) !void {
        const start = out.items.len;
        const s = if (self.enc == .utf8) raw else try csv.decodeField(arena, self.enc, raw);
        if (entities) try token.decodeInto(out, s, false) else try out.appendSlice(s);
        if (self.enc == .utf8 and !std.unicode.utf8ValidateSlice(out.items[start..])) {
            const bad = try arena.dupe(u8, out.items[start..]);
            out.shrinkRetainingCapacity(start);
            var runs = celltext.Utf8Runs{ .s = bad };
            while (runs.next()) |r| try out.appendSlice(r);
        }
    }

    /// A name decoded once and kept, as the same few names recur on every row; past
    /// `max_names` distinct ones, a document of unique names is decoded per use.
    fn nameOf(self: *Reader, arena: std.mem.Allocator, raw: []const u8) ![]const u8 {
        if (self.names.get(raw)) |n| return n;
        const keep = self.names.count() < max_names;
        const a = if (keep) self.arena else arena;
        const s = if (self.enc == .utf8) raw else try csv.decodeField(a, self.enc, raw);
        const name = try a.dupe(u8, try escape.decodeName(a, s));
        if (keep) try self.names.put(self.arena, try self.arena.dupe(u8, raw), name);
        return name;
    }

    const max_names = 4096;
    const max_row_bytes = 64 << 20;
    const max_row_elems = 100_000;
    const max_depth = 128;

    /// A tag's attributes, decoded, but for namespace declarations and `xsi:` ones.
    fn attrsOf(self: *Reader, arena: std.mem.Allocator, o: token.Tag) ![]const Attr {
        var out = std.array_list.Managed(Attr).init(arena);
        var it = o.iterator();
        while (it.next()) |a| {
            if (std.mem.eql(u8, a.name, "xmlns") or std.mem.startsWith(u8, a.name, "xmlns:") or std.mem.startsWith(u8, a.name, "xsi:")) continue;
            try out.append(.{ .name = try self.nameOf(arena, token.localName(a.name)), .value = try self.textOf(arena, a.value, true) });
        }
        return out.items;
    }

    pub fn close(self: *Reader) void {
        self.tz.deinit();
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.schema;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.next(arena);
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        self.close();
    }

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

/// XML files read as one table (`files.Files`).
pub const Files = @import("../files.zig").Files(Reader, Options);

/// `children` grouped by name, in the order each name first appears: a few are
/// matched pairwise, many through a map, so a wide row costs linear time.
fn groupByName(arena: std.mem.Allocator, children: []const *const Node) ![]const []const *const Node {
    const gid = try arena.alloc(u32, children.len);
    var firsts: std.ArrayListUnmanaged(u32) = .empty;
    var map: std.StringHashMapUnmanaged(u32) = .empty;
    const hashed = children.len > 16;
    for (children, gid, 0..) |c, *g, i| {
        if (hashed) {
            const gop = try map.getOrPut(arena, c.name);
            if (!gop.found_existing) {
                gop.value_ptr.* = @intCast(firsts.items.len);
                try firsts.append(arena, @intCast(i));
            }
            g.* = gop.value_ptr.*;
            continue;
        }
        g.* = for (firsts.items, 0..) |f, k| {
            if (std.mem.eql(u8, children[f].name, c.name)) break @intCast(k);
        } else blk: {
            try firsts.append(arena, @intCast(i));
            break :blk @intCast(firsts.items.len - 1);
        };
    }
    const fill = try arena.alloc(u32, firsts.items.len);
    @memset(fill, 0);
    for (gid) |g| fill[g] += 1;
    const slots = try arena.alloc(*const Node, children.len);
    const groups = try arena.alloc([]*const Node, firsts.items.len);
    var at: usize = 0;
    for (groups, fill) |*g, *n| {
        g.* = slots[at..][0..n.*];
        at += n.*;
        n.* = 0;
    }
    for (children, gid) |c, g| {
        groups[g][fill[g]] = c;
        fill[g] += 1;
    }
    return groups;
}

fn keyOf(arena: std.mem.Allocator, kind: Kind, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "{c}{s}", .{ @as(u8, switch (kind) {
        .elem => 'e',
        .attr => 'a',
        .child_attr => 'c',
    }), name });
}

fn isNil(o: token.Tag) bool {
    var it = o.iterator();
    while (it.next()) |a| if (std.mem.eql(u8, token.localName(a.name), "nil") and std.mem.startsWith(u8, a.name, "xsi:"))
        return std.mem.eql(u8, a.value, "true") or std.mem.eql(u8, a.value, "1");
    return false;
}

fn blank(s: []const u8) bool {
    return std.mem.trim(u8, s, " \t\r\n").len == 0;
}

fn sep(w: *std.Io.Writer, first: *bool) !void {
    if (!first.*) try w.writeByte(',');
    first.* = false;
}

fn writeKey(w: *std.Io.Writer, prefix: []const u8, name: []const u8) !void {
    try w.writeByte('"');
    try w.writeAll(prefix);
    try jsontext.chars(w, name);
    try w.writeAll("\":");
}

/// The encoding the document declares, after a UTF-8 byte-order mark is passed;
/// UTF-8 when it declares none. UTF-16 and anything else basalt does not decode are
/// refused.
fn declared(in: *std.Io.Reader) !Encoding {
    const head = in.peekGreedy(1) catch |e| switch (e) {
        error.EndOfStream => return .utf8,
        else => return e,
    };
    if (std.mem.startsWith(u8, head, "\xef\xbb\xbf")) {
        in.toss(3);
        return .utf8;
    }
    if (std.mem.startsWith(u8, head, "\xff\xfe") or std.mem.startsWith(u8, head, "\xfe\xff"))
        return eval.explain(error.XmlUnsupportedEncoding, "the document is UTF-16; basalt reads XML in UTF-8, ISO-8859-1 or windows-1252");
    if (!std.mem.startsWith(u8, head, "<?xml")) return .utf8;
    const end = std.mem.indexOf(u8, head, "?>") orelse return .utf8;
    const tag = token.Tag{ .name = "xml", .attrs = head[5..end], .self_closing = false };
    const name = tag.attr("encoding") orelse return .utf8;
    if (std.ascii.eqlIgnoreCase(name, "us-ascii") or std.ascii.eqlIgnoreCase(name, "ascii")) return .utf8;
    return csv.parseEncoding(name) orelse eval.explain(error.XmlUnsupportedEncoding, "the document declares an encoding basalt does not read; it reads UTF-8, ISO-8859-1 and windows-1252, and `WITH (encoding = ...)` overrides a wrong declaration");
}

const testing = std.testing;

fn readAll(a: std.mem.Allocator, doc: []const u8, opts: Options) !struct { schema: types.Schema, rows: [][]const ?[]const u8 } {
    var in = std.Io.Reader.fixed(doc);
    const r = try Reader.open(a, a, &in, opts);
    var rows = std.array_list.Managed([]const ?[]const u8).init(a);
    while (try r.next(a)) |b| for (0..b.len) |i| {
        const row = try a.alloc(?[]const u8, b.columns.len);
        for (b.columns, row) |*c, *v| v.* = if (c.validity.get(i)) try eval.valueToString(a, c.getValue(i)) else null;
        try rows.append(row);
    };
    return .{ .schema = r.schema, .rows = rows.items };
}

test "children, attributes, nesting, repeats, empty and nil, typed as CSV text" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const doc =
        \\<?xml version="1.0"?>
        \\<orders xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        \\  <order id="1" status="new">
        \\    <customer>Ana &amp; Co</customer><total currency="BRL">10.50</total>
        \\    <ship><city>Recife</city></ship><tag>a</tag><tag>b</tag><note/><code>007</code>
        \\  </order>
        \\  <order id="2"><customer><![CDATA[<Bob>]]></customer><total xsi:nil="true"/><id>99</id></order>
        \\</orders>
    ;
    const got = try readAll(a, doc, .{});
    const names = [_][]const u8{ "@id", "status", "customer", "total", "total@currency", "ship", "tag", "note", "code", "id" };
    try testing.expectEqual(names.len, got.schema.fields.len);
    for (names, got.schema.fields) |n, f| try testing.expectEqualStrings(n, f.name);
    try testing.expectEqual(types.TypeKind.int, got.schema.fields[0].ty.kind);
    try testing.expectEqual(types.TypeKind.float, got.schema.fields[3].ty.kind);
    try testing.expectEqual(types.TypeKind.string, got.schema.fields[8].ty.kind);
    const r0 = got.rows[0];
    try testing.expectEqualStrings("Ana & Co", r0[2].?);
    try testing.expectEqualStrings("BRL", r0[4].?);
    try testing.expectEqualStrings("{\"city\":\"Recife\"}", r0[5].?);
    try testing.expectEqualStrings("[\"a\",\"b\"]", r0[6].?);
    try testing.expectEqualStrings("", r0[7].?);
    try testing.expectEqualStrings("007", r0[8].?);
    try testing.expect(r0[9] == null);
    const r1 = got.rows[1];
    try testing.expectEqualStrings("2", r1[0].?);
    try testing.expectEqualStrings("<Bob>", r1[2].?);
    try testing.expect(r1[3] == null);
    try testing.expect(r1[7] == null);
    try testing.expectEqualStrings("99", r1[9].?);
}

test "row names an element at any depth; FOR XML names decode; latin-1 is declared" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const doc = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><r><meta n=\"2\"/><items><i><Valor_x0020_Total>1</Valor_x0020_Total><nome>Jos\xe9</nome></i><i><Valor_x0020_Total>2</Valor_x0020_Total></i></items></r>";
    const got = try readAll(a, doc, .{ .row = "i" });
    try testing.expectEqual(@as(usize, 2), got.rows.len);
    try testing.expectEqualStrings("Valor Total", got.schema.fields[0].name);
    try testing.expectEqualStrings("José", got.rows[0][1].?);
    try testing.expect(got.rows[1][1] == null);
}

test "a value past the sample its typed column cannot read is an error; UTF-16 is refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectError(error.TextTypeMismatch, readAll(a, "<r><x><n>1</n></x><x><n>abc</n></x></r>", .{ .sample = 1 }));
    try testing.expectError(error.TextTypeMismatch, readAll(a, "<r><x><n>1</n></x><x><n>2</n><n>3</n></x></r>", .{ .sample = 1 }));
    try testing.expectError(error.XmlUnsupportedEncoding, readAll(a, "\xff\xfe<\x00r\x00>\x00", .{}));
}

test "a document cut short, with no element or text outside its root, is an error, not fewer rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectError(error.BadXml, readAll(a, "<r><x><n>1</n></x><x><n>2</n></x>", .{}));
    try testing.expectError(error.BadXml, readAll(a, "<r><x><n>1</n></x><x><n>2", .{}));
    try testing.expectError(error.BadXml, readAll(a, "{\"a\":1}\n{\"a\":2}\n", .{}));
    try testing.expectError(error.BadXml, readAll(a, "", .{}));
    try testing.expectError(error.BadXml, readAll(a, "<r><x/></r><r><x/></r>", .{}));
    var deep = std.array_list.Managed(u8).init(a);
    try deep.appendSlice("<r><row>");
    for (0..20_000) |_| try deep.appendSlice("<a>");
    for (0..20_000) |_| try deep.appendSlice("</a>");
    try deep.appendSlice("</row></r>");
    try testing.expectError(error.BadXml, readAll(a, deep.items, .{}));
    try testing.expectEqual(@as(usize, 0), (try readAll(a, "<r/>", .{})).rows.len);
}
