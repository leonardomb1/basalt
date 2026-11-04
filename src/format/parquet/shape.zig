//! A nested column's Parquet shape, from its basalt type: the schema elements as
//! the format's spec lays them out, and the plan the shredder (`shred.zig`) walks.
//!
//! A struct is a group of its fields. A list is the three-level form the spec
//! requires of new files, `<name> (LIST) { repeated group list { <elem> element } }`,
//! and a map `<name> (MAP) { repeated group key_value { required <key> key;
//! <value> value } }`. A node is OPTIONAL when its type is nullable, else REQUIRED;
//! a map's key is always REQUIRED. Leaves carry no physical type here: the writer
//! maps each leaf's scalar type as it maps a flat column's.
//!
//! Each plan node holds the levels the shredder emits: `def`, the definition level
//! once the node is present; for a list or map, `entry_def` and `entry_rep`, the
//! levels once an entry is present, the second the level a further entry repeats
//! at. Every level comes from `schema.step`, the rule the reader's walk takes its
//! levels from, so what is written is what is read back; a test holds the plan's
//! leaves against `schema.collectLeaves` over the elements. A struct also takes
//! `slot`, where its fields' values sit in the shredder's scratch while a row's
//! object is walked; `Shape.slots` counts them.

const std = @import("std");
const types = @import("../../lang/types.zig");
const footer = @import("footer.zig");
const schema = @import("schema.zig");
const logical = @import("logical.zig");

pub const Kind = enum { leaf, group, list, map };

pub const Node = struct {
    name: []const u8,
    kind: Kind,
    ty: types.Type,
    nullable: bool,
    def: u32,
    rep: u32,
    entry_def: u32 = 0,
    entry_rep: u32 = 0,
    children: []Node = &.{},
    first_leaf: u32 = 0,
    leaves: u32 = 0,
    slot: u32 = 0,
};

pub const Shape = struct {
    root: *const Node,
    elements: []const footer.SchemaElement,
    leaves: []const *const Node,
    paths: []const []const []const u8,
    slots: u32,
};

pub const Error = error{ OutOfMemory, UnsupportedNestedType };

/// Top-level column `name` of type `ty` as Parquet groups and leaves.
pub fn of(arena: std.mem.Allocator, name: []const u8, ty: types.Type) Error!Shape {
    var b = Builder{ .arena = arena, .elements = .empty, .leaves = .empty };
    const root = try arena.create(Node);
    root.* = try b.node(name, ty, ty.nullable, 0, 0);
    try b.number(root);
    return .{ .root = root, .elements = b.elements.items, .leaves = b.leaves.items, .paths = b.paths.items, .slots = b.slots };
}

const Builder = struct {
    arena: std.mem.Allocator,
    elements: std.ArrayList(footer.SchemaElement),
    leaves: std.ArrayList(*const Node),
    paths: std.ArrayList([]const []const u8) = .empty,
    path: std.ArrayList([]const u8) = .empty,
    slots: u32 = 0,

    fn add(b: *Builder, e: footer.SchemaElement) Error!void {
        try b.elements.append(b.arena, e);
    }

    /// A node and its subtree, its elements appended in schema order (depth first).
    fn node(b: *Builder, name: []const u8, ty: types.Type, nullable: bool, def: u32, rep: u32) Error!Node {
        const r: footer.Repetition = if (nullable) .optional else .required;
        const d, _ = schema.step(def, rep, r);
        switch (ty.kind) {
            .@"struct" => {
                const fields = ty.fields orelse return Error.UnsupportedNestedType;
                if (fields.len == 0) return Error.UnsupportedNestedType;
                try b.add(.{ .name = name, .repetition = r, .num_children = @intCast(fields.len) });
                const kids = try b.arena.alloc(Node, fields.len);
                for (fields, kids) |f, *k| k.* = try b.node(f.name, f.ty, f.ty.nullable, d, rep);
                return .{ .name = name, .kind = .group, .ty = ty, .nullable = nullable, .def = d, .rep = rep, .children = kids };
            },
            .array => {
                const elem = ty.elem orelse return Error.UnsupportedNestedType;
                try b.add(.{ .name = name, .repetition = r, .num_children = 1, .converted_type = logical.conv_list });
                try b.add(.{ .name = "list", .repetition = .repeated, .num_children = 1 });
                const ed, const er = schema.step(d, rep, .repeated);
                const kids = try b.arena.alloc(Node, 1);
                kids[0] = try b.node("element", elem.*, elem.nullable, ed, er);
                return .{ .name = name, .kind = .list, .ty = ty, .nullable = nullable, .def = d, .rep = rep, .entry_def = ed, .entry_rep = er, .children = kids };
            },
            .map => {
                const kv = ty.fields orelse return Error.UnsupportedNestedType;
                if (kv.len != 2) return Error.UnsupportedNestedType;
                try b.add(.{ .name = name, .repetition = r, .num_children = 1, .converted_type = logical.conv_map });
                try b.add(.{ .name = "key_value", .repetition = .repeated, .num_children = 2 });
                const ed, const er = schema.step(d, rep, .repeated);
                const kids = try b.arena.alloc(Node, 2);
                kids[0] = try b.node("key", kv[0].ty, false, ed, er);
                kids[1] = try b.node("value", kv[1].ty, kv[1].ty.nullable, ed, er);
                if (kids[0].kind != .leaf) return Error.UnsupportedNestedType;
                return .{ .name = name, .kind = .map, .ty = ty, .nullable = nullable, .def = d, .rep = rep, .entry_def = ed, .entry_rep = er, .children = kids };
            },
            else => {
                try b.add(.{ .name = name, .repetition = r });
                return .{ .name = name, .kind = .leaf, .ty = ty, .nullable = nullable, .def = d, .rep = rep };
            },
        }
    }

    /// Each node's first leaf and leaf count, leaves listed in schema order with
    /// their paths from the column down, and each struct's scratch slots. A list's
    /// and a map's repeated group is named on the path as the elements name it.
    fn number(b: *Builder, n: *Node) Error!void {
        try b.path.append(b.arena, n.name);
        defer _ = b.path.pop();
        if (n.kind == .list) try b.path.append(b.arena, "list");
        if (n.kind == .map) try b.path.append(b.arena, "key_value");
        defer if (n.kind == .list or n.kind == .map) {
            _ = b.path.pop();
        };
        n.first_leaf = @intCast(b.leaves.items.len);
        if (n.kind == .leaf) {
            try b.leaves.append(b.arena, n);
            try b.paths.append(b.arena, try b.arena.dupe([]const u8, b.path.items));
            n.leaves = 1;
            return;
        }
        if (n.kind == .group) {
            n.slot = b.slots;
            b.slots += @intCast(n.children.len);
        }
        for (n.children) |*k| try b.number(k);
        n.leaves = @as(u32, @intCast(b.leaves.items.len)) - n.first_leaf;
    }
};

const testing = std.testing;

fn scalar(kind: types.TypeKind, nullable: bool) types.Type {
    return types.Type.init(kind).withNull(nullable);
}

test "shape: structs, three-level lists and maps take the spec's layout, and their leaves the reader's levels" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const code = scalar(.string, false);
    const country = scalar(.string, true);
    const lang = types.Type{ .kind = .@"struct", .nullable = true, .fields = &.{ .{ .name = "code", .ty = code }, .{ .name = "country", .ty = country } } };
    const langs = types.Type{ .kind = .array, .nullable = true, .elem = &lang };
    const attrs = types.Type{ .kind = .map, .nullable = true, .fields = &.{ .{ .name = "key", .ty = scalar(.string, false) }, .{ .name = "value", .ty = scalar(.int, true) } } };
    const doc = types.Type{ .kind = .@"struct", .nullable = true, .fields = &.{
        .{ .name = "id", .ty = scalar(.int, false) },
        .{ .name = "langs", .ty = langs },
        .{ .name = "attrs", .ty = attrs },
    } };
    const s = try of(a, "doc", doc);

    const names = [_][]const u8{ "doc", "id", "langs", "list", "element", "code", "country", "attrs", "key_value", "key", "value" };
    try testing.expectEqual(names.len, s.elements.len);
    for (names, s.elements) |n, e| try testing.expectEqualStrings(n, e.name);
    try testing.expectEqual(@as(?i32, logical.conv_list), s.elements[2].converted_type);
    try testing.expectEqual(footer.Repetition.repeated, s.elements[3].repetition.?);
    try testing.expectEqual(@as(?i32, logical.conv_map), s.elements[7].converted_type);
    try testing.expectEqual(footer.Repetition.required, s.elements[9].repetition.?);

    var with_root: std.ArrayList(footer.SchemaElement) = .empty;
    try with_root.append(a, .{ .name = "schema", .num_children = 1 });
    for (s.elements) |e| {
        var x = e;
        if (x.num_children == 0) x.ty = .byte_array;
        try with_root.append(a, x);
    }
    const read = try schema.collectLeaves(a, with_root.items);
    try testing.expectEqual(s.leaves.len, read.len);
    for (s.leaves, read, s.paths) |mine, theirs, path| {
        try testing.expectEqual(theirs.max_def, mine.def);
        try testing.expectEqual(theirs.max_rep, mine.rep);
        try testing.expect(path.len > 0);
    }
    const want_path = [_][]const u8{ "doc", "langs", "list", "element", "country" };
    try testing.expectEqual(want_path.len, s.paths[2].len);
    for (want_path, s.paths[2]) |w, g| try testing.expectEqualStrings(w, g);
    const defs = [_]u32{ 1, 4, 5, 3, 4 };
    for (defs, s.leaves) |d, l| try testing.expectEqual(d, l.def);
    try testing.expectEqual(@as(u32, 3), s.root.children[1].entry_def);
    try testing.expectEqual(@as(u32, 1), s.root.children[1].entry_rep);
    try testing.expectEqual(@as(u32, 2), s.root.children[1].leaves);
}

test "shape: a struct without fields, a list without an element type and a nested map key are refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectError(Error.UnsupportedNestedType, of(a, "s", .{ .kind = .@"struct", .fields = &.{} }));
    try testing.expectError(Error.UnsupportedNestedType, of(a, "l", .{ .kind = .array }));
    const inner = types.Type{ .kind = .@"struct", .fields = &.{.{ .name = "x", .ty = scalar(.int, true) }} };
    try testing.expectError(Error.UnsupportedNestedType, of(a, "m", .{ .kind = .map, .fields = &.{ .{ .name = "key", .ty = inner }, .{ .name = "value", .ty = scalar(.int, true) } } }));
}
