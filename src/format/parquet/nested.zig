//! Nested columns (lists and structs) assembled from their leaves' repetition and
//! definition levels into one JSON text value per row.

const Entries = @import("chunk.zig").Entries;
const Error = @import("read.zig").Error;
const ListShape = @import("schema.zig").ListShape;
const RootRef = @import("schema.zig").RootRef;
const Value = @import("../../exec/value.zig").Value;
const capacity_hint = @import("encoding.zig").capacity_hint;
const column = @import("../../exec/column.zig");
const eval = @import("../../exec/eval.zig");
const jsontext = @import("../../exec/jsontext.zig");
const parquet = @import("footer.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const entriesOf = @import("testing_util.zig").entriesOf;
const testing = std.testing;

pub const NNode = struct {
    name: []const u8,
    def: u32,
    rep: u32,
    optional: bool,
    repeated: bool,
    kind: Kind,
    children: []NNode = &.{},
    first_leaf: usize,
    nleaves: usize,
    max_def: u32 = 0,
    schema_idx: usize = 0,

    pub const Kind = enum { leaf, group, list, map };
};

pub const Nested = struct {
    name: []const u8,
    root: NNode,
    leaves: []const usize,
};

/// Leaves are numbered depth-first, the order their chunks appear in. LIST and MAP
/// are honoured only in the spec's single-repeated-child shape.
fn buildNode(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, pos: *usize, def: u32, rep: u32, next_leaf: *usize, depth: usize) Error!NNode {
    if (pos.* >= schema.len or depth > 64) return Error.UnsupportedParquetSchema;
    const e = schema[pos.*];
    pos.* += 1;
    const r = e.repetition orelse .required;
    const d2 = def + @as(u32, if (r == .required) 0 else 1);
    const r2 = rep + @as(u32, if (r == .repeated) 1 else 0);
    var node = NNode{
        .schema_idx = pos.* - 1,
        .name = e.name,
        .def = d2,
        .rep = r2,
        .optional = r == .optional,
        .repeated = r == .repeated,
        .kind = .leaf,
        .first_leaf = next_leaf.*,
        .nleaves = 0,
    };
    if (e.isLeaf()) {
        node.max_def = d2;
        node.nleaves = 1;
        next_leaf.* += 1;
        return node;
    }
    const n: usize = @intCast(@max(0, e.num_children));
    const kids = try arena.alloc(NNode, n);
    for (kids) |*k| k.* = try buildNode(arena, schema, pos, d2, r2, next_leaf, depth + 1);
    node.children = kids;
    node.nleaves = next_leaf.* - node.first_leaf;
    const conv = e.converted_type orelse -1;
    node.kind = if (n == 1 and kids[0].repeated and conv == 3)
        .list
    else if (n == 1 and kids[0].repeated and (conv == 1 or conv == 2) and kids[0].children.len == 2)
        .map
    else
        .group;
    return node;
}

/// The tree under `root`, a struct's hidden fields (`isHidden`) left out where
/// another field remains, so they are neither read nor shown; leaves numbered
/// depth-first over what is kept.
pub fn buildNested(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, root: RootRef) Error!NNode {
    var pos = root.idx;
    var next: usize = 0;
    var tree = try buildNode(arena, schema, &pos, root.base_def, root.base_rep, &next, 0);
    try prune(arena, &tree);
    next = 0;
    number(&tree, &next);
    return tree;
}

/// Whether a field is one a table hides from its file (`fit.Mapping`): a column
/// it dropped, whose name starts with a NUL byte.
pub fn isHidden(name: []const u8) bool {
    return name.len > 0 and name[0] == 0;
}

/// Drops a struct's hidden fields when one is left; a repeated group keeps them,
/// as how many fields it has says whether it is a list's element.
fn prune(arena: std.mem.Allocator, n: *NNode) Error!void {
    for (n.children) |*c| try prune(arena, c);
    if (n.kind != .group or n.repeated) return;
    var shown: usize = 0;
    for (n.children) |c| shown += @intFromBool(!isHidden(c.name));
    if (shown == 0 or shown == n.children.len) return;
    const kids = try arena.alloc(NNode, shown);
    var k: usize = 0;
    for (n.children) |c| if (!isHidden(c.name)) {
        kids[k] = c;
        k += 1;
    };
    n.children = kids;
}

fn number(n: *NNode, next: *usize) void {
    n.first_leaf = next.*;
    if (n.kind == .leaf and n.children.len == 0) {
        n.nleaves = 1;
        next.* += 1;
        return;
    }
    for (n.children) |*c| number(c, next);
    n.nleaves = next.* - n.first_leaf;
}

/// The schema indices of `n`'s leaves, in their order in the tree.
pub fn leafIndices(n: *const NNode, out: *std.array_list.Managed(usize)) Error!void {
    if (n.kind == .leaf and n.children.len == 0) return out.append(n.schema_idx);
    for (n.children) |*c| try leafIndices(c, out);
}

/// Whether assembling `n` shows an object: a struct or a map anywhere in it, as
/// the assembler reads it. A list of a one-field struct has a single leaf, yet
/// its elements are objects.
pub fn holdsStruct(n: *const NNode) bool {
    return switch (n.kind) {
        .leaf => false,
        .group, .map => true,
        .list => if (Assembler.elementOf(&n.children[0], false)) |el| holdsStruct(el) else holdsStruct(&n.children[0]),
    };
}

const Span = struct { lo: usize, hi: usize };

const Assembler = struct {
    arena: std.mem.Allocator,
    entries: []const Entries,
    out: *std.array_list.Managed(u8),

    fn firstDef(self: *const Assembler, n: *const NNode, spans: []const Span) Error!u32 {
        const s = spans[n.first_leaf];
        if (s.lo >= s.hi) return Error.CorruptParquetPage;
        return self.entries[n.first_leaf].defs[s.lo];
    }

    fn field(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        if (n.repeated) return self.array(n, spans, elementOf(n, true));
        return self.value(n, spans);
    }

    fn value(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        if (n.optional and try self.firstDef(n, spans) < n.def) return self.out.appendSlice("null");
        switch (n.kind) {
            .leaf => {
                const s = spans[n.first_leaf];
                if (s.hi - s.lo != 1) return Error.CorruptParquetPage;
                const e = self.entries[n.first_leaf];
                try jsonValue(self.out, if (e.defs[s.lo] < n.max_def) .null else e.vals.getValue(s.lo));
            },
            .group => try self.object(n, spans),
            .list => try self.array(&n.children[0], spans, elementOf(&n.children[0], false)),
            .map => try self.mapObject(&n.children[0], spans),
        }
    }

    fn object(self: *Assembler, n: *const NNode, spans: []const Span) anyerror!void {
        try self.out.append('{');
        var first = true;
        for (n.children) |*c| {
            if (isHidden(c.name)) continue;
            if (!first) try self.out.append(',');
            first = false;
            try appendJson(self.out, jsontext.string, .{c.name});
            try self.out.append(':');
            try self.field(c, spans);
        }
        try self.out.append('}');
    }

    /// The node itself for a bare repeated field, or a repeated group with several
    /// fields or a legacy name (`array`, `*_tuple`); else, three-level, its one child.
    fn elementOf(rep: *const NNode, bare: bool) ?*const NNode {
        if (bare or rep.kind == .leaf or rep.children.len != 1) return null;
        if (std.mem.eql(u8, rep.name, "array") or std.mem.endsWith(u8, rep.name, "_tuple")) return null;
        return &rep.children[0];
    }

    /// One element per entry of the first leaf at `rep.rep`; none when the first
    /// entry's definition stops short of `rep.def`.
    fn array(self: *Assembler, rep: *const NNode, spans: []const Span, element: ?*const NNode) anyerror!void {
        if (try self.firstDef(rep, spans) < rep.def) return self.out.appendSlice("[]");
        try self.out.append('[');
        var count: ?usize = null;
        const cut = try self.arena.alloc([]Span, rep.nleaves);
        for (cut, 0..) |*c, k| {
            const li = rep.first_leaf + k;
            c.* = try self.split(li, spans[li], rep.rep);
            if (count) |n| {
                if (n != c.len) return Error.CorruptParquetPage;
            } else count = c.len;
        }
        const sub = try self.arena.dupe(Span, spans);
        for (0..count.?) |i| {
            if (i > 0) try self.out.append(',');
            for (cut, 0..) |c, k| sub[rep.first_leaf + k] = c[i];
            if (element) |el| {
                try self.field(el, sub);
            } else {
                var as_value = rep.*;
                as_value.repeated = false;
                as_value.optional = false;
                try self.value(&as_value, sub);
            }
        }
        try self.out.append(']');
    }

    fn mapObject(self: *Assembler, kv: *const NNode, spans: []const Span) anyerror!void {
        if (try self.firstDef(kv, spans) < kv.def) return self.out.appendSlice("{}");
        try self.out.append('{');
        const cut = try self.arena.alloc([]Span, kv.nleaves);
        var count: ?usize = null;
        for (cut, 0..) |*c, k| {
            const li = kv.first_leaf + k;
            c.* = try self.split(li, spans[li], kv.rep);
            if (count) |n| {
                if (n != c.len) return Error.CorruptParquetPage;
            } else count = c.len;
        }
        const sub = try self.arena.dupe(Span, spans);
        const key = &kv.children[0];
        for (0..count.?) |i| {
            if (i > 0) try self.out.append(',');
            for (cut, 0..) |c, k| sub[kv.first_leaf + k] = c[i];
            var kbuf = std.array_list.Managed(u8).init(self.arena);
            var ka = Assembler{ .arena = self.arena, .entries = self.entries, .out = &kbuf };
            if (key.kind == .leaf and key.nleaves == 1) {
                const s = sub[key.first_leaf];
                if (s.hi - s.lo != 1) return Error.CorruptParquetPage;
                const e = self.entries[key.first_leaf];
                const kv_val = if (e.defs[s.lo] < key.max_def) Value.null else e.vals.getValue(s.lo);
                try kbuf.appendSlice(if (kv_val == .null) "null" else try eval.valueToString(self.arena, kv_val));
            } else try ka.field(key, sub);
            try appendJson(self.out, jsontext.string, .{kbuf.items});
            try self.out.append(':');
            try self.field(&kv.children[1], sub);
        }
        try self.out.append('}');
    }

    /// Cuts where a new element at repetition level `r` begins. Every entry within an
    /// instance repeats at `r` or deeper.
    fn split(self: *Assembler, li: usize, span: Span, r: u32) Error![]Span {
        const reps = self.entries[li].reps;
        var out = std.array_list.Managed(Span).init(self.arena);
        var start = span.lo;
        var i = span.lo + 1;
        while (i < span.hi) : (i += 1) {
            if (reps[i] < r) return Error.CorruptParquetPage;
            if (reps[i] == r) {
                try out.append(.{ .lo = start, .hi = i });
                start = i;
            }
        }
        try out.append(.{ .lo = start, .hi = span.hi });
        return out.items;
    }
};

/// `f(w, args...)` appended to `buf`, through a writer lent `buf`'s own memory.
fn appendJson(buf: *std.array_list.Managed(u8), comptime f: anytype, args: anytype) Error!void {
    const gpa = buf.allocator;
    var list = buf.moveToUnmanaged();
    var aw = std.Io.Writer.Allocating.fromArrayList(gpa, &list);
    const r = @call(.auto, f, .{&aw.writer} ++ args);
    list = aw.toArrayList();
    buf.* = list.toManaged(gpa);
    r catch return error.OutOfMemory;
}

pub fn assembleNested(arena: std.mem.Allocator, root: *const NNode, entries: []const Entries, rows: usize) anyerror!column.Column {
    if (entries.len != root.nleaves) return Error.CorruptParquetPage;
    const starts = try arena.alloc([]usize, entries.len);
    for (entries, starts) |e, *st| {
        var list = std.array_list.Managed(usize).init(arena);
        for (e.reps, 0..) |r, i| if (r == 0) try list.append(i);
        if (list.items.len != rows or (rows > 0 and list.items[0] != 0)) return Error.CorruptParquetPage;
        try list.append(e.reps.len);
        st.* = list.items;
    }
    var out = try column.Builder.initCapacity(arena, types.Type.init(.string).asNullable(), @min(rows, capacity_hint));
    var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer scratch.deinit();
    const spans = try arena.alloc(Span, entries.len);
    for (0..rows) |row| {
        _ = scratch.reset(.retain_capacity);
        const sa = scratch.allocator();
        for (spans, starts) |*sp, st| sp.* = .{ .lo = st[row], .hi = st[row + 1] };
        var buf = std.array_list.Managed(u8).init(sa);
        var asm_ = Assembler{ .arena = sa, .entries = entries, .out = &buf };
        try asm_.field(root, spans);
        if (std.mem.eql(u8, buf.items, "null")) try out.append(.null) else try out.append(.{ .string = buf.items });
    }
    return out.finish();
}

pub const Levels = struct {
    reps: std.array_list.Managed(u32),
    defs: std.array_list.Managed(u32),
};

/// Repetition 0 starts a row and `r > 0` a new element at depth `r`; below it,
/// each level's definition threshold says element, empty list, or null.
pub fn assembleLists(
    arena: std.mem.Allocator,
    elems: column.Column,
    reps: []const u32,
    defs: []const u32,
    max_def: u32,
    shape: ListShape,
    rows: usize,
) Error!column.Column {
    if (reps.len != defs.len or reps.len != elems.len) return Error.CorruptParquetPage;
    const depth = shape.rep_def.len;
    var out = try column.Builder.initCapacity(arena, types.Type.init(.string).asNullable(), @min(rows, capacity_hint));
    var buf = std.array_list.Managed(u8).init(arena);
    var open: usize = 0;
    var in_row = false;
    var row_null = false;
    var done: usize = 0;

    for (reps, defs, 0..) |r, d, i| {
        if (r > depth or d > max_def) return Error.CorruptParquetPage;
        if (r == 0) {
            if (in_row) {
                try finishRow(&out, &buf, &open, row_null);
                done += 1;
            }
            in_row = true;
            row_null = false;
            if (d + 1 < shape.rep_def[0]) {
                row_null = true;
                continue;
            }
            try buf.append('[');
            open = 1;
        } else {
            if (!in_row or row_null or r > open or d < shape.rep_def[r - 1]) return Error.CorruptParquetPage;
            while (open > r) : (open -= 1) try buf.append(']');
            try buf.append(',');
        }
        var lvl = open;
        while (true) {
            if (d < shape.rep_def[lvl - 1]) break;
            if (lvl == depth) {
                try jsonValue(&buf, if (d < max_def) .null else elems.getValue(i));
                break;
            }
            if (d + 1 < shape.rep_def[lvl]) {
                try buf.appendSlice("null");
                break;
            }
            try buf.append('[');
            lvl += 1;
            open = lvl;
        }
    }
    if (in_row) {
        try finishRow(&out, &buf, &open, row_null);
        done += 1;
    }
    if (done != rows) return Error.CorruptParquetPage;
    return out.finish();
}

fn finishRow(out: *column.Builder, buf: *std.array_list.Managed(u8), open: *usize, row_null: bool) Error!void {
    if (row_null) {
        try out.append(.null);
    } else {
        while (open.* > 0) : (open.* -= 1) try buf.append(']');
        try out.append(.{ .string = buf.items });
    }
    buf.clearRetainingCapacity();
}

/// A leaf value as JSON in the nested shape (`jsontext.nested`): decimals bare,
/// bytes as their text.
fn jsonValue(buf: *std.array_list.Managed(u8), v: Value) Error!void {
    return appendJson(buf, jsontext.scalar, .{ v, jsontext.nested });
}

test "assembleNested: leaves that disagree, or a row count that does not match, are a corrupt page" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "recs", .repetition = .optional, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .repetition = .optional, .num_children = 2 },
        .{ .name = "a", .ty = .int64, .repetition = .optional },
        .{ .name = "b", .ty = .int64, .repetition = .optional },
    };
    const root = try buildNested(a, &schema, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    const ea = try entriesOf(a, &.{ .{ .int = 1 }, .null, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 3, 0, 1, 2 });
    const eb = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 4, 0, 1, 2 });
    const got = try assembleNested(a, &root, &.{ ea, eb }, 4);
    try testing.expectEqualStrings("[{\"a\":1,\"b\":2},{\"a\":null,\"b\":3}]", got.getValue(0).string);
    try testing.expect(got.getValue(1) == .null);
    try testing.expectEqualStrings("[]", got.getValue(2).string);
    try testing.expectEqualStrings("[null]", got.getValue(3).string);

    const eb3 = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .{ .int = 9 }, .null, .null, .null }, &.{ 0, 1, 1, 0, 0, 0 }, &.{ 4, 4, 4, 0, 1, 2 });
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ ea, eb3 }, 4));
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ ea, eb }, 5));
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &root, &.{ea}, 4));
}

test "assembleLists: a row count past the levels read is a corrupt page, not an allocation" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var eb = try column.Builder.initCapacity(a, types.Type.init(.int).asNullable(), 1);
    try eb.append(.{ .int = 7 });
    const elems = try eb.finish();
    const shape: ListShape = .{ .rep_def = &.{1} };
    const one = try assembleLists(a, elems, &.{0}, &.{1}, 1, shape, 1);
    try testing.expectEqualStrings("[7]", one.getValue(0).string);
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, elems, &.{0}, &.{1}, 1, shape, 1 << 47));
}

fn newJson(a: std.mem.Allocator, v: Value) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(a);
    try jsonValue(&buf, v);
    return buf.items;
}

test "jsonValue on jsontext.scalar writes what the old encoder did for valid text and every other kind" {
    const old = struct {
        fn oldJsonValue(arena: std.mem.Allocator, buf: *std.array_list.Managed(u8), v: Value) Error!void {
            switch (v) {
                .null => try buf.appendSlice("null"),
                .int => |x| try buf.writer().print("{d}", .{x}),
                .bool => |x| try buf.appendSlice(if (x) "true" else "false"),
                .float => |x| if (std.math.isFinite(x)) try buf.writer().print("{d}", .{x}) else try buf.appendSlice("null"),
                .decimal => try buf.appendSlice(eval.valueToString(arena, v) catch return error.OutOfMemory),
                else => {
                    var aw = std.Io.Writer.Allocating.init(arena);
                    std.json.Stringify.encodeJsonString(eval.valueToString(arena, v) catch return error.OutOfMemory, .{}, &aw.writer) catch return error.OutOfMemory;
                    try buf.appendSlice(aw.written());
                },
            }
        }
    };
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const D = @import("../../exec/value.zig").Decimal;
    const vals = [_]Value{
        .null,
        .{ .bool = true },
        .{ .bool = false },
        .{ .int = 0 },
        .{ .int = -1 },
        .{ .int = std.math.maxInt(i64) },
        .{ .int = std.math.minInt(i64) },
        .{ .float = 0.0 },
        .{ .float = -0.0 },
        .{ .float = 1.5 },
        .{ .float = -2.25 },
        .{ .float = 0.1 },
        .{ .float = 1.0 / 3.0 },
        .{ .float = 1e15 },
        .{ .float = 999999999999999.9 },
        .{ .float = 1e-6 },
        .{ .float = 9.999999e-7 },
        .{ .float = 123456789.123 },
        .{ .float = 1e300 },
        .{ .float = -1e-300 },
        .{ .float = 5e-324 },
        .{ .float = std.math.floatMax(f64) },
        .{ .float = 9007199254740993.0 },
        .{ .float = 0.30000000000000004 },
        .{ .float = std.math.nan(f64) },
        .{ .float = std.math.inf(f64) },
        .{ .float = -std.math.inf(f64) },
        .{ .decimal = D{ .unscaled = 12345, .scale = 2 } },
        .{ .decimal = D{ .unscaled = -5, .scale = 3 } },
        .{ .decimal = D{ .unscaled = 7, .scale = 0 } },
        .{ .string = "" },
        .{ .string = "plain" },
        .{ .string = "q\"b\\s/\n\r\t\x01\x1f\x7f" },
        .{ .string = "a\u{e7}\u{e3}o \u{1F600}" },
        .{ .bytes = "raw" },
        .{ .date = 0 },
        .{ .date = -719_162 },
        .{ .date = 2_932_896 },
        .{ .time = 0 },
        .{ .time = 86_399_999_999 },
        .{ .timestamp = 0 },
        .{ .timestamp = -1 },
        .{ .timestamp = 1_583_298_367_123_456 },
    };
    for (vals) |v| {
        var want = std.array_list.Managed(u8).init(a);
        try old.oldJsonValue(a, &want, v);
        try testing.expectEqualStrings(want.items, try newJson(a, v));
    }
    var x: f64 = 1e-8;
    while (x < 1e17) : (x *= 1.37) for ([_]f64{ x, -x, x + 0.1, @round(x) }) |f| {
        var want = std.array_list.Managed(u8).init(a);
        try old.oldJsonValue(a, &want, .{ .float = f });
        try testing.expectEqualStrings(want.items, try newJson(a, .{ .float = f }));
    };
}

test "a nested leaf string holding invalid UTF-8 is valid JSON, each bad byte a U+FFFD" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const got = try newJson(a, .{ .string = "a\xffb" });
    try testing.expectEqualStrings("\"a\u{FFFD}b\"", got);
    try testing.expectEqualStrings("a\u{FFFD}b", try std.json.parseFromSliceLeaky([]const u8, a, got, .{}));
}

test "nested BYTES stay their text, not base64 (D8 pending); \\b and \\f are spelled \\u00XX" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("\"\\u0001ab\"", try newJson(a, .{ .bytes = "\x01ab" }));
    try testing.expectEqualStrings("\"\\u0008\\u000c\"", try newJson(a, .{ .string = "\x08\x0c" }));
}
