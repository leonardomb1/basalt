//! Nested columns (lists, maps and structs) assembled from their leaves'
//! repetition and definition levels into basalt's nested storage (`column.Nested`).
//!
//! A column's tree (`NNode`) gives its type (`fieldType`): a group is a struct of
//! its fields (hidden ones left out, `isHidden`), a LIST group a list of its
//! element, a MAP group a map of its key and value, and a bare repeated field (the
//! legacy two-level form, or a repeated group of several fields, `array` or
//! `*_tuple`) a list of the field itself. A node is nullable when it is OPTIONAL.
//!
//! The assembly is Dremel's, top down over instances (`Inst`). An instance is,
//! for every leaf under a node, the span of that leaf's entries one value of the
//! node covers; a column's instances are its rows, each leaf's span running from
//! a repetition level of 0 to the next. The first entry of a node's first leaf
//! answers whether the node is there (its definition level reaches the node's).
//! A list's instance splits into its elements wherever its repeated node's
//! repetition level starts an entry, every leaf alike, and the elements are the
//! child's instances. A leaf's values are one gather of its entries by their
//! index (`column.permute`), never boxed or rendered, so a read moves typed
//! buffers as a flat column's read does.

const Entries = @import("chunk.zig").Entries;
const Error = @import("read.zig").Error;
const RootRef = @import("schema.zig").RootRef;
const Value = @import("../../exec/value.zig").Value;
const basaltType = @import("logical.zig").basaltType;
const column = @import("../../exec/column.zig");
const eval = @import("../../exec/eval.zig");
const jsontext = @import("../../exec/jsontext.zig");
const json_out = @import("../../exec/nested.zig");
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
    ty: types.Type,
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

/// Keeps of struct `n`, named `path`, only the fields a dotted name in `want`
/// reaches (`add.path` keeps `add`'s `path`), each pruned alike; all of them when
/// `want` names `n` itself. A list or a map is kept whole. Leaves are renumbered
/// over what is kept.
pub fn keepPaths(arena: std.mem.Allocator, n: *NNode, path: []const u8, want: []const []const u8) Error!void {
    try keepIn(arena, n, path, want);
    var next: usize = 0;
    number(n, &next);
}

fn keepIn(arena: std.mem.Allocator, n: *NNode, path: []const u8, want: []const []const u8) Error!void {
    if (n.kind != .group or n.repeated) return;
    for (want) |w| if (std.mem.eql(u8, w, path)) return;
    var kids = std.array_list.Managed(NNode).init(arena);
    for (n.children) |c| {
        const cp = try std.mem.concat(arena, u8, &.{ path, ".", c.name });
        if (!reaches(want, cp)) continue;
        var k = c;
        try keepIn(arena, &k, cp, want);
        try kids.append(k);
    }
    if (kids.items.len > 0) n.children = kids.items;
}

const reaches = @import("../../connect/driver.zig").wantsColumn;

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

/// The type `n` reads as where it is a field: a repeated field a list of itself.
pub fn fieldType(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, n: *const NNode) Error!types.Type {
    if (!n.repeated) return valueType(arena, schema, n);
    return listType(arena, try valueType(arena, schema, &asValue(n)), false);
}

fn valueType(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, n: *const NNode) Error!types.Type {
    const t: types.Type = switch (n.kind) {
        .leaf => try basaltType(schema[n.schema_idx]),
        .group => blk: {
            var fields = std.array_list.Managed(types.Type.Field).init(arena);
            for (n.children) |*c| if (!isHidden(c.name)) try fields.append(.{ .name = c.name, .ty = try fieldType(arena, schema, c) });
            break :blk .{ .kind = .@"struct", .fields = fields.items };
        },
        .list => blk: {
            const rep = &n.children[0];
            const el = if (elementOf(rep, false)) |e| try fieldType(arena, schema, e) else try valueType(arena, schema, &asValue(rep));
            break :blk try listType(arena, el, false);
        },
        .map => blk: {
            const kv = n.children[0].children;
            const pair = try arena.dupe(types.Type.Field, &.{
                .{ .name = "key", .ty = (try fieldType(arena, schema, &kv[0])).withNull(false) },
                .{ .name = "value", .ty = try fieldType(arena, schema, &kv[1]) },
            });
            break :blk .{ .kind = .map, .fields = pair };
        },
    };
    return t.withNull(n.optional);
}

fn listType(arena: std.mem.Allocator, elem: types.Type, nullable: bool) Error!types.Type {
    const e = try arena.create(types.Type);
    e.* = elem;
    return .{ .kind = .array, .nullable = nullable, .elem = e };
}

/// A repeated node read as one element of the list it makes: neither repeated nor
/// optional, as each entry is there.
fn asValue(rep: *const NNode) NNode {
    var v = rep.*;
    v.repeated = false;
    v.optional = false;
    return v;
}

/// The node itself for a bare repeated field, or a repeated group with several
/// fields or a legacy name (`array`, `*_tuple`); else, three-level, its one child.
fn elementOf(rep: *const NNode, bare: bool) ?*const NNode {
    if (bare or rep.kind == .leaf or rep.children.len != 1) return null;
    if (std.mem.eql(u8, rep.name, "array") or std.mem.endsWith(u8, rep.name, "_tuple")) return null;
    return &rep.children[0];
}

const Span = struct { lo: usize, hi: usize };

/// The values of a node: `n` of them, `spans[leaf][i]` the entries of leaf `leaf`
/// that value `i` covers, for every leaf under the node (the others unused).
const Inst = struct { n: usize, spans: []const []const Span };

const Assembler = struct {
    arena: std.mem.Allocator,
    schema: []const parquet.SchemaElement,
    entries: []const Entries,

    fn def(self: *const Assembler, n: *const NNode, inst: Inst, i: usize) Error!u32 {
        const s = (try spansOf(n, inst))[i];
        if (s.lo >= s.hi or s.lo >= self.entries[n.first_leaf].defs.len) return Error.CorruptParquetPage;
        return self.entries[n.first_leaf].defs[s.lo];
    }

    /// The first leaf's span of each of `inst`, which a file whose schema gives a
    /// node no leaf, or whose leaves disagree, does not have.
    fn spansOf(n: *const NNode, inst: Inst) Error![]const Span {
        if (n.nleaves == 0 or n.first_leaf >= inst.spans.len or inst.spans[n.first_leaf].len < inst.n) return Error.CorruptParquetPage;
        return inst.spans[n.first_leaf][0..inst.n];
    }

    fn field(self: *Assembler, n: *const NNode, inst: Inst) anyerror!column.Column {
        if (n.repeated) return self.list(n, null, inst, null, try fieldType(self.arena, self.schema, n));
        return self.value(n, inst);
    }

    /// Which of `inst` are there: all for a required node, else those whose first
    /// entry reaches the node's definition level.
    fn present(self: *Assembler, n: *const NNode, inst: Inst) !column.Bitmap {
        var bm = try column.Bitmap.initFull(self.arena, inst.n);
        if (n.optional) for (0..inst.n) |i| if (try self.def(n, inst, i) < n.def) bm.setValid(i, false);
        return bm;
    }

    fn value(self: *Assembler, n: *const NNode, inst: Inst) anyerror!column.Column {
        const ty = try valueType(self.arena, self.schema, n);
        switch (n.kind) {
            .leaf => {
                const idx = try self.arena.alloc(usize, inst.n);
                for (idx, try spansOf(n, inst)) |*x, s| {
                    if (s.hi - s.lo != 1) return Error.CorruptParquetPage;
                    x.* = s.lo;
                }
                var c = try column.permute(self.arena, self.entries[n.first_leaf].vals, idx);
                c.ty = ty;
                return c;
            },
            .group => {
                const bm = try self.present(n, inst);
                var kids = std.array_list.Managed(column.Column).init(self.arena);
                for (n.children) |*c| if (!isHidden(c.name)) try kids.append(try self.field(c, inst));
                return self.nested(ty, inst.n, bm, .{ .fields = kids.items });
            },
            .list => {
                const rep = &n.children[0];
                return self.list(rep, elementOf(rep, false), inst, try self.present(n, inst), ty);
            },
            .map => return self.list(&n.children[0], null, inst, try self.present(n, inst), ty),
        }
    }

    /// A list (or a map, when `ty` is one) whose entries are `rep`'s, the instances
    /// `valid` marks null having none: each instance split where `rep`'s repetition
    /// level starts an entry, every leaf alike, unless its first entry stops short
    /// of `rep` (an empty list).
    fn list(self: *Assembler, rep: *const NNode, element: ?*const NNode, inst: Inst, valid: ?column.Bitmap, ty: types.Type) anyerror!column.Column {
        const bm = valid orelse try column.Bitmap.initFull(self.arena, inst.n);
        if (try self.leafList(rep, element, inst, bm, ty)) |c| return c;
        const offsets = try self.arena.alloc(i32, inst.n + 1);
        offsets[0] = 0;
        const parts = try self.arena.alloc(std.ArrayListUnmanaged(Span), self.entries.len);
        for (parts) |*p| p.* = .empty;
        var total: usize = 0;
        for (0..inst.n) |i| {
            if (bm.get(i) and try self.def(rep, inst, i) >= rep.def) {
                var count: ?usize = null;
                for (rep.first_leaf..rep.first_leaf + rep.nleaves) |li| {
                    if (li >= inst.spans.len or i >= inst.spans[li].len) return Error.CorruptParquetPage;
                    const before = parts[li].items.len;
                    try self.split(&parts[li], li, inst.spans[li][i], rep.rep);
                    const got = parts[li].items.len - before;
                    if (count) |c| {
                        if (c != got) return Error.CorruptParquetPage;
                    } else count = got;
                }
                total += count orelse return Error.CorruptParquetPage;
            }
            offsets[i + 1] = @intCast(try column.checkSpan(total));
        }
        const spans = try self.arena.alloc([]const Span, self.entries.len);
        for (spans, parts) |*s, p| s.* = p.items;
        const child_inst = Inst{ .n = total, .spans = spans };
        const child = if (ty.kind == .map) try self.entriesOf(rep, child_inst, ty) else if (element) |el| try self.field(el, child_inst) else try self.value(&asValue(rep), child_inst);
        return self.nested(ty, inst.n, bm, .{ .list = .{ .offsets = offsets, .child = child } });
    }

    /// A list whose element is one leaf (`list<float>`, an embedding): every entry
    /// of an instance that holds elements is one element, so the offsets are the
    /// spans' lengths, with no span per element. When the instances' entries are
    /// all elements, end to end (no NULL or empty list among them), the leaf's
    /// values as decoded are the child; else the elements are gathered by index.
    /// Null for any other list, which `list` splits entry by entry.
    fn leafList(self: *Assembler, rep: *const NNode, element: ?*const NNode, inst: Inst, bm: column.Bitmap, ty: types.Type) anyerror!?column.Column {
        if (ty.kind == .map or rep.nleaves != 1) return null;
        const bare = asValue(rep);
        const leaf = element orelse &bare;
        if (leaf.kind != .leaf or leaf.repeated) return null;
        const spans = try spansOf(rep, inst);
        const e = self.entries[rep.first_leaf];
        const offsets = try self.arena.alloc(i32, inst.n + 1);
        offsets[0] = 0;
        var total: usize = 0;
        var cursor: usize = 0;
        var whole = true;
        for (spans, 0..) |sp, i| {
            if (sp.lo >= sp.hi or sp.hi > e.reps.len) return Error.CorruptParquetPage;
            if (bm.get(i) and e.defs[sp.lo] >= rep.def) {
                for (e.reps[sp.lo + 1 .. sp.hi]) |r| if (r != rep.rep) return Error.CorruptParquetPage;
                if (sp.lo != cursor) whole = false;
                cursor = sp.hi;
                total += sp.hi - sp.lo;
            } else whole = false;
            offsets[i + 1] = @intCast(try column.checkSpan(total));
        }
        var child = if (whole and cursor == e.vals.len) e.vals else blk: {
            const idx = try self.arena.alloc(usize, total);
            var k: usize = 0;
            for (spans, 0..) |sp, i| if (bm.get(i) and e.defs[sp.lo] >= rep.def) {
                for (sp.lo..sp.hi) |x| {
                    idx[k] = x;
                    k += 1;
                }
            };
            break :blk try column.permute(self.arena, e.vals, idx);
        };
        child.ty = try valueType(self.arena, self.schema, leaf);
        return try self.nested(ty, inst.n, bm, .{ .list = .{ .offsets = offsets, .child = child } });
    }

    /// A map's entries as the struct of its key and value.
    fn entriesOf(self: *Assembler, kv: *const NNode, inst: Inst, map: types.Type) anyerror!column.Column {
        const kids = try self.arena.alloc(column.Column, 2);
        kids[0] = try self.field(&kv.children[0], inst);
        kids[1] = try self.field(&kv.children[1], inst);
        return self.nested(column.entryType(map), inst.n, try column.Bitmap.initFull(self.arena, inst.n), .{ .fields = kids });
    }

    fn nested(self: *Assembler, ty: types.Type, n: usize, bm: column.Bitmap, body: column.Nested.Body) !column.Column {
        const node = try self.arena.create(column.Nested);
        node.* = .{ .ty = ty, .body = body };
        return column.Column.derived(ty, n, bm, .{ .nested = node });
    }

    /// The entries of `span` cut where a new element at repetition level `r` begins,
    /// appended to `out`. Every entry within an instance repeats at `r` or deeper.
    fn split(self: *Assembler, out: *std.ArrayListUnmanaged(Span), li: usize, span: Span, r: u32) Error!void {
        const reps = self.entries[li].reps;
        if (span.hi > reps.len or span.lo >= span.hi) return Error.CorruptParquetPage;
        var start = span.lo;
        for (span.lo + 1..span.hi) |i| {
            if (reps[i] < r) return Error.CorruptParquetPage;
            if (reps[i] == r) {
                try out.append(self.arena, .{ .lo = start, .hi = i });
                start = i;
            }
        }
        try out.append(self.arena, .{ .lo = start, .hi = span.hi });
    }
};

/// Column `root` of `rows` rows from its leaves' `entries`, in leaf order: each
/// row's entries are every leaf's from one repetition level of 0 to the next.
pub fn assembleNested(arena: std.mem.Allocator, schema: []const parquet.SchemaElement, root: *const NNode, entries: []const Entries, rows: usize) anyerror!column.Column {
    if (entries.len != root.nleaves) return Error.CorruptParquetPage;
    const spans = try arena.alloc([]const Span, entries.len);
    for (entries, spans) |e, *sp| {
        if (e.reps.len != e.defs.len or e.reps.len != e.vals.len) return Error.CorruptParquetPage;
        const out = try arena.alloc(Span, rows);
        var k: usize = 0;
        for (e.reps, 0..) |r, i| if (r == 0) {
            if (k == rows) return Error.CorruptParquetPage;
            if (k > 0) out[k - 1].hi = i;
            out[k] = .{ .lo = i, .hi = e.reps.len };
            k += 1;
        } else if (i == 0) return Error.CorruptParquetPage;
        if (k != rows) return Error.CorruptParquetPage;
        sp.* = out;
    }
    var a = Assembler{ .arena = arena, .schema = schema, .entries = entries };
    var col = try a.field(root, .{ .n = rows, .spans = spans });
    col.ty = col.ty.asNullable();
    return col;
}

fn newJson(a: std.mem.Allocator, v: Value) ![]const u8 {
    var out = std.Io.Writer.Allocating.init(a);
    try jsontext.scalar(&out.writer, v, jsontext.nested);
    return out.written();
}

/// Row `r` of an assembled column as its JSON, null when the row is.
fn rowJson(a: std.mem.Allocator, c: column.Column, r: usize) !?[]const u8 {
    const v = c.getValue(r);
    if (v == .null) return null;
    return try json_out.jsonText(a, v.nested);
}

fn expectRows(a: std.mem.Allocator, c: column.Column, want: []const ?[]const u8) !void {
    try testing.expectEqual(want.len, c.len);
    for (want, 0..) |w, r| {
        const got = try rowJson(a, c, r);
        if (w) |x| try testing.expectEqualStrings(x, got.?) else try testing.expect(got == null);
    }
}

test "assembleNested: a list of structs from its leaves' levels; leaves that disagree, or a row count that does not match, are a corrupt page" {
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
    const ty = try fieldType(a, &schema, &root);
    try testing.expectEqualStrings("list(struct(a int, b int))", try ty.name(a));
    const ea = try entriesOf(a, &.{ .{ .int = 1 }, .null, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 3, 0, 1, 2 });
    const eb = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .null, .null, .null }, &.{ 0, 1, 0, 0, 0 }, &.{ 4, 4, 0, 1, 2 });
    const got = try assembleNested(a, &schema, &root, &.{ ea, eb }, 4);
    try expectRows(a, got, &.{ "[{\"a\":1,\"b\":2},{\"a\":null,\"b\":3}]", null, "[]", "[null]" });
    try testing.expectEqual(@as(i64, 3), got.data.nested.body.list.child.data.nested.body.fields[1].getValue(1).int);

    const eb3 = try entriesOf(a, &.{ .{ .int = 2 }, .{ .int = 3 }, .{ .int = 9 }, .null, .null, .null }, &.{ 0, 1, 1, 0, 0, 0 }, &.{ 4, 4, 4, 0, 1, 2 });
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &schema, &root, &.{ ea, eb3 }, 4));
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &schema, &root, &.{ ea, eb }, 5));
    try testing.expectError(Error.CorruptParquetPage, assembleNested(a, &schema, &root, &.{ea}, 4));
}

test "assembleNested: null, empty, a null element, nested lists, a legacy bare repeated field and a struct's fields" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const flat = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "l", .repetition = .optional, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .ty = .int64, .repetition = .optional },
    };
    const fr = try buildNested(a, &flat, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    const fe = try entriesOf(a, &.{ .{ .int = 1 }, .{ .int = 2 }, .null, .null, .null, .{ .int = 5 } }, &.{ 0, 1, 0, 0, 0, 1 }, &.{ 3, 3, 1, 0, 2, 3 });
    try expectRows(a, try assembleNested(a, &flat, &fr, &.{fe}, 4), &.{ "[1,2]", "[]", null, "[null,5]" });

    const deep = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "l", .repetition = .optional, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .repetition = .optional, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .ty = .int64, .repetition = .optional },
    };
    const dr = try buildNested(a, &deep, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    const de = try entriesOf(a, &.{ .{ .int = 1 }, .{ .int = 2 }, .{ .int = 3 }, .null, .null, .{ .int = 4 } }, &.{ 0, 1, 2, 0, 1, 1 }, &.{ 5, 5, 5, 3, 2, 5 });
    try expectRows(a, try assembleNested(a, &deep, &dr, &.{de}, 2), &.{ "[[1],[2,3]]", "[[],null,[4]]" });

    const bare = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "xs", .ty = .int32, .repetition = .repeated },
    };
    const br = try buildNested(a, &bare, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    try testing.expectEqualStrings("list(int)", try (try fieldType(a, &bare, &br)).name(a));
    const be = try entriesOf(a, &.{ .{ .int = 7 }, .{ .int = 8 }, .null }, &.{ 0, 1, 0 }, &.{ 1, 1, 0 });
    try expectRows(a, try assembleNested(a, &bare, &br, &.{be}, 2), &.{ "[7,8]", "[]" });

    const st = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "s", .repetition = .optional, .num_children = 2 },
        .{ .name = "x", .ty = .int64, .repetition = .required },
        .{ .name = "y", .ty = .int64, .repetition = .optional },
    };
    const sr = try buildNested(a, &st, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    try testing.expectEqualStrings("struct(x int, y int)", try (try fieldType(a, &st, &sr)).name(a));
    const sx = try entriesOf(a, &.{ .{ .int = 1 }, .null, .{ .int = 3 } }, &.{ 0, 0, 0 }, &.{ 1, 0, 1 });
    const sy = try entriesOf(a, &.{ .null, .null, .{ .int = 4 } }, &.{ 0, 0, 0 }, &.{ 1, 0, 2 });
    try expectRows(a, try assembleNested(a, &st, &sr, &.{ sx, sy }, 3), &.{ "{\"x\":1,\"y\":null}", null, "{\"x\":3,\"y\":4}" });
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

test "D8: nested BYTES are base64 in a list, a map value and a map key; \\b and \\f are spelled \\u00XX" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectEqualStrings("\"AWFi\"", try newJson(a, .{ .bytes = "\x01ab" }));
    try testing.expectEqualStrings("\"\\u0008\\u000c\"", try newJson(a, .{ .string = "\x08\x0c" }));

    const ls = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "l", .repetition = .required, .num_children = 1, .converted_type = 3 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .ty = .byte_array, .repetition = .optional },
    };
    const lr = try buildNested(a, &ls, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    var eb = try column.Builder.initCapacity(a, types.Type.init(.bytes).asNullable(), 3);
    try eb.append(.{ .bytes = "\xff" });
    try eb.append(.{ .bytes = "\xfe" });
    try eb.append(.null);
    const elems: Entries = .{ .vals = try eb.finish(), .reps = &.{ 0, 1, 1 }, .defs = &.{ 2, 2, 1 } };
    try expectRows(a, try assembleNested(a, &ls, &lr, &.{elems}, 1), &.{"[\"/w==\",\"/g==\",null]"});

    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 1 },
        .{ .name = "m", .repetition = .optional, .num_children = 1, .converted_type = 1 },
        .{ .name = "key_value", .repetition = .repeated, .num_children = 2 },
        .{ .name = "key", .ty = .byte_array, .repetition = .required },
        .{ .name = "value", .ty = .byte_array, .repetition = .optional },
    };
    const root = try buildNested(a, &schema, .{ .idx = 1, .base_def = 0, .base_rep = 0 });
    var kb = try column.Builder.initCapacity(a, types.Type.init(.bytes).asNullable(), 2);
    try kb.append(.{ .bytes = "\xff" });
    try kb.append(.{ .bytes = "\xfe" });
    var vb = try column.Builder.initCapacity(a, types.Type.init(.bytes).asNullable(), 2);
    try vb.append(.{ .bytes = "\x01ab" });
    try vb.append(.null);
    const keys: Entries = .{ .vals = try kb.finish(), .reps = &.{ 0, 1 }, .defs = &.{ 2, 2 } };
    const vals: Entries = .{ .vals = try vb.finish(), .reps = &.{ 0, 1 }, .defs = &.{ 3, 2 } };
    const got = (try rowJson(a, try assembleNested(a, &schema, &root, &.{ keys, vals }, 1), 0)).?;
    try testing.expectEqualStrings("{\"/w==\":\"AWFi\",\"/g==\":null}", got);
    try testing.expect(try std.json.validate(a, got));
}
