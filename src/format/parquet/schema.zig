//! The schema tree's leaf columns: each one's levels and dotted path, and its
//! chunk's place among a row group's.

const Error = @import("read.zig").Error;
const parquet = @import("footer.zig");
const std = @import("std");
const testing = std.testing;

pub const Leaf = struct {
    schema_idx: usize,
    chunk_idx: usize,
    name: []const u8,
    max_def: u32,
    max_rep: u32,

    pub fn isRepeated(self: Leaf) bool {
        return self.max_rep > 0;
    }
};

pub const RootRef = struct { idx: usize, base_def: u32, base_rep: u32 };

const PathNode = struct { name: []const u8 };

/// Resolves each leaf's levels and dotted name from its ancestors. Depth is capped
/// so a hostile schema cannot recurse forever.
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
    const d2, const r2 = step(def, rep, r);
    try path.append(.{ .name = e.name });
    defer _ = path.pop();

    if (e.isLeaf()) {
        const leaf = Leaf{
            .schema_idx = idx,
            .chunk_idx = chunk.*,
            .name = try joinPath(arena, path.items),
            .max_def = d2,
            .max_rep = r2,
        };
        try out.append(leaf);
        chunk.* += 1;
        return;
    }
    const n: usize = @intCast(@max(0, e.num_children));
    for (0..n) |_| try walkNode(arena, schema, pos, chunk, out, path, d2, r2);
}

/// The definition and repetition levels below a node of repetition `r` under levels
/// `def` and `rep`: a node that may be absent adds a definition level, a repeated
/// one a repetition level too. The one rule the reader's walk and the writer's
/// shape (`shape.zig`) both take their levels from.
pub fn step(def: u32, rep: u32, r: parquet.Repetition) struct { u32, u32 } {
    return .{ def + @intFromBool(r != .required), rep + @intFromBool(r == .repeated) };
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

    try testing.expectEqualStrings("tags.element", leaves[3].name);
    try testing.expect(leaves[3].isRepeated());

    try testing.expectEqual(@as(usize, 3), leaves[3].chunk_idx);
}
