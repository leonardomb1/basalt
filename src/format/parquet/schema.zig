//! The schema tree: its leaf columns, their dotted paths, and which groups are lists.

const Error = @import("read.zig").Error;
const parquet = @import("footer.zig");
const std = @import("std");
const Value = @import("../../exec/value.zig").Value;
const assembleLists = @import("nested.zig").assembleLists;
const listColumn = @import("read.zig").listColumn;
const testing = std.testing;

pub const Leaf = struct {
    schema_idx: usize,
    chunk_idx: usize,
    name: []const u8,
    max_def: u32,
    max_rep: u32,
    list: ?ListShape = null,
    root: ?RootRef = null,

    pub fn isRepeated(self: Leaf) bool {
        return self.max_rep > 0;
    }
};

pub const ListShape = struct {
    rep_def: []const u32,
};

pub const RootRef = struct { idx: usize, base_def: u32, base_rep: u32 };

const PathNode = struct { name: []const u8, idx: usize, def: u32, repeated: bool, list_group: bool };

fn isListGroup(e: parquet.SchemaElement) bool {
    if (e.converted_type) |c| if (c == 1 or c == 2 or c == 3) return true;
    return false;
}

/// Resolves each leaf's levels and dotted name from its ancestors. A list is
/// named at a LIST/MAP group wrapping its first repeated node (three-level), else
/// at that node (legacy two-level). Depth is capped so a hostile schema cannot recurse forever.
pub fn collectLeaves(arena: std.mem.Allocator, schema: []const parquet.SchemaElement) Error![]Leaf {
    var out = std.array_list.Managed(Leaf).init(arena);
    var path = std.array_list.Managed(PathNode).init(arena);
    if (schema.len == 0) return Error.UnsupportedParquetSchema;
    var pos: usize = 1;
    var chunk: usize = 0;
    const root_children: usize = @intCast(@max(0, schema[0].num_children));
    for (0..root_children) |_| {
        try walkNode(arena, schema, &pos, &chunk, &out, &path, 0, 0);
    }
    return out.toOwnedSlice();
}

fn walkNode(
    arena: std.mem.Allocator,
    schema: []const parquet.SchemaElement,
    pos: *usize,
    chunk: *usize,
    out: *std.array_list.Managed(Leaf),
    path: *std.array_list.Managed(PathNode),
    def: u32,
    rep: u32,
) Error!void {
    if (pos.* >= schema.len) return Error.UnsupportedParquetSchema;
    if (path.items.len >= 64) return Error.UnsupportedParquetSchema;
    const e = schema[pos.*];
    const idx = pos.*;
    pos.* += 1;

    const r = e.repetition orelse .required;
    const d2 = def + @as(u32, if (r == .required) 0 else 1);
    const r2 = rep + @as(u32, if (r == .repeated) 1 else 0);
    try path.append(.{ .name = e.name, .idx = idx, .def = d2, .repeated = r == .repeated, .list_group = !e.isLeaf() and isListGroup(e) });
    defer _ = path.pop();

    if (e.isLeaf()) {
        var leaf = Leaf{
            .schema_idx = idx,
            .chunk_idx = chunk.*,
            .name = try joinPath(arena, path.items),
            .max_def = d2,
            .max_rep = r2,
        };
        if (r2 > 0) {
            var first: usize = 0;
            while (!path.items[first].repeated) first += 1;
            const root = if (first > 0 and path.items[first - 1].list_group) first - 1 else first;
            leaf.name = try joinPath(arena, path.items[0 .. root + 1]);
            const rep_def = try arena.alloc(u32, r2);
            var k: usize = 0;
            for (path.items) |pn| if (pn.repeated) {
                rep_def[k] = pn.def;
                k += 1;
            };
            leaf.list = .{ .rep_def = rep_def };
            var base_rep: u32 = 0;
            for (path.items[0..root]) |pn| if (pn.repeated) {
                base_rep += 1;
            };
            leaf.root = .{
                .idx = path.items[root].idx,
                .base_def = if (root > 0) path.items[root - 1].def else 0,
                .base_rep = base_rep,
            };
        }
        try out.append(leaf);
        chunk.* += 1;
        return;
    }
    const n: usize = @intCast(@max(0, e.num_children));
    for (0..n) |_| try walkNode(arena, schema, pos, chunk, out, path, d2, r2);
}

fn joinPath(arena: std.mem.Allocator, nodes: []const PathNode) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    for (nodes, 0..) |pn, i| {
        if (i > 0) try buf.append('.');
        try buf.appendSlice(pn.name);
    }
    return buf.toOwnedSlice();
}

test "schema walk resolves levels and dotted names for nested groups" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = [_]parquet.SchemaElement{
        .{ .name = "root", .num_children = 3 },
        .{ .name = "id", .ty = .int32, .repetition = .required },
        .{ .name = "addr", .repetition = .optional, .num_children = 2 },
        .{ .name = "city", .ty = .byte_array, .repetition = .optional },
        .{ .name = "zip", .ty = .byte_array, .repetition = .required },
        .{ .name = "tags", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .ty = .byte_array, .repetition = .required },
    };
    const leaves = try collectLeaves(a, &schema);
    try testing.expectEqual(@as(usize, 4), leaves.len);

    try testing.expectEqualStrings("id", leaves[0].name);
    try testing.expectEqual(@as(u32, 0), leaves[0].max_def);
    try testing.expect(!leaves[0].isRepeated());

    try testing.expectEqualStrings("addr.city", leaves[1].name);
    try testing.expectEqual(@as(u32, 2), leaves[1].max_def);
    try testing.expectEqualStrings("addr.zip", leaves[2].name);
    try testing.expectEqual(@as(u32, 1), leaves[2].max_def);

    try testing.expectEqualStrings("tags", leaves[3].name);
    try testing.expect(leaves[3].isRepeated());
    try testing.expectEqualSlices(u32, &.{1}, leaves[3].list.?.rep_def);

    try testing.expectEqual(@as(usize, 3), leaves[3].chunk_idx);
}

test "assembleLists: null, empty, a null element, and nested lists from levels" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const flat = ListShape{ .rep_def = &.{2} };
    const vals = [_]Value{ .{ .int = 1 }, .{ .int = 2 }, .null, .null, .null, .{ .int = 5 } };
    const reps = [_]u32{ 0, 1, 0, 0, 0, 1 };
    const defs = [_]u32{ 3, 3, 1, 0, 2, 3 };
    const got = try assembleLists(a, try listColumn(a, &vals), &reps, &defs, 3, flat, 4);
    try testing.expectEqualStrings("[1,2]", got.getValue(0).string);
    try testing.expectEqualStrings("[]", got.getValue(1).string);
    try testing.expect(got.getValue(2) == .null);
    try testing.expectEqualStrings("[null,5]", got.getValue(3).string);

    const nested = ListShape{ .rep_def = &.{ 2, 4 } };
    const nvals = [_]Value{ .{ .int = 1 }, .{ .int = 2 }, .{ .int = 3 }, .null, .null, .{ .int = 4 } };
    const nreps = [_]u32{ 0, 1, 2, 0, 1, 1 };
    const ndefs = [_]u32{ 5, 5, 5, 3, 2, 5 };
    const ngot = try assembleLists(a, try listColumn(a, &nvals), &nreps, &ndefs, 5, nested, 2);
    try testing.expectEqualStrings("[[1],[2,3]]", ngot.getValue(0).string);
    try testing.expectEqualStrings("[[],null,[4]]", ngot.getValue(1).string);

    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &reps, &defs, 3, flat, 5));
    const bad_reps = [_]u32{ 0, 2, 0, 0, 0, 1 };
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &bad_reps, &defs, 3, flat, 4));
    const bad_defs = [_]u32{ 3, 1, 1, 0, 2, 3 };
    try testing.expectError(Error.CorruptParquetPage, assembleLists(a, try listColumn(a, &vals), &reps, &bad_defs, 3, flat, 4));
}
