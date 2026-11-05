//! `UNNEST`: one output row per element of a delimited string or a JSON array.

const Batch = @import("../batch.zig").Batch;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const json = @import("../json.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");
const Scan = @import("../op.zig").Scan;
const TestSource = @import("testing_util.zig").TestSource;
const kvBatch = @import("testing_util.zig").kvBatch;
const testing = std.testing;

pub const Explode = struct {
    stats: Stats = .{},
    child: Op,
    field_idx: usize,
    delim: []const u8,
    json: bool = false,
    out_schema: *const types.Schema,

    pub fn next(self: *Explode, arena: std.mem.Allocator) anyerror!?Batch {
        while (try self.child.next(arena)) |b| {
            const out = try self.explodeBatch(arena, b);
            if (out.len > 0) return out;
        }
        return null;
    }

    pub fn transform(self: *Explode, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        return self.explodeBatch(arena, b);
    }

    fn listed(self: *Explode, arena: std.mem.Allocator, b: Batch) !Batch {
        return listedBatch(arena, b, self.field_idx, self.out_schema);
    }

    /// Split a delimited string column, or with `json` a JSON array, into one row per
    /// element, other columns repeated; a list or a map column is taken as it is
    /// (`listed`). Null or missing cells produce zero rows.
    fn explodeBatch(self: *Explode, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
        if (b.columns[self.field_idx].data == .nested) return self.listed(arena, b);
        const ncols = b.columns.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders, self.out_schema.fields) |*bd, f| bd.* = column.Builder.init(arena, f.ty);

        var n: usize = 0;
        var r: usize = 0;
        while (r < b.len) : (r += 1) {
            const fv = b.columns[self.field_idx].getValue(r);
            const s = switch (fv) {
                .string => |x| x,
                .bytes => |x| x,
                else => continue,
            };
            if (self.json) {
                for (try jsonElems(arena, s)) |elem| {
                    for (b.columns, 0..) |*c, ci| {
                        try builders[ci].append(if (ci == self.field_idx) elem else c.getValue(r));
                    }
                    n += 1;
                }
                continue;
            }
            var it = std.mem.splitSequence(u8, s, self.delim);
            while (it.next()) |elem| {
                for (b.columns, 0..) |*c, ci| {
                    if (ci == self.field_idx) {
                        try builders[ci].append(.{ .string = elem });
                    } else {
                        try builders[ci].append(c.getValue(r));
                    }
                }
                n += 1;
            }
        }

        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*bd, i| cols[i] = try bd.finish();
        return Batch{ .schema = self.out_schema, .columns = cols, .len = n };
    }
};

/// A list or map column's rows as rows: each other column's row gathered once per
/// element, the elements (a map's key and value structs) the child column's own
/// rows, so nothing is boxed or rendered.
fn listedBatch(arena: std.mem.Allocator, b: Batch, at: usize, schema: *const types.Schema) !Batch {
    const col = b.columns[at];
    const l = col.data.nested.body.list;
    var total: usize = 0;
    for (0..b.len) |r| if (col.validity.get(r)) {
        total += @intCast(l.offsets[r + 1] - l.offsets[r]);
    };
    const parent = try arena.alloc(usize, total);
    const elems = try arena.alloc(usize, total);
    var w: usize = 0;
    for (0..b.len) |r| if (col.validity.get(r)) {
        for (@as(usize, @intCast(l.offsets[r]))..@as(usize, @intCast(l.offsets[r + 1]))) |x| {
            parent[w] = r;
            elems[w] = x;
            w += 1;
        }
    };
    const cols = try arena.alloc(column.Column, b.columns.len);
    for (cols, b.columns, schema.fields, 0..) |*o, c, f, i| {
        o.* = if (i == at) try column.permute(arena, l.child, elems) else try column.permute(arena, c, parent);
        o.ty = f.ty;
    }
    return .{ .schema = schema, .columns = cols, .len = total };
}

/// The elements of the JSON array `text` as cells; a JSON null is no elements.
fn jsonElems(arena: std.mem.Allocator, text: []const u8) ![]const Value {
    try json.validate(arena, text);
    switch (json.rootKind(text)) {
        .array => {},
        .null => return &.{},
        .other => return error.JsonNotArray,
    }
    var out = std.array_list.Managed(Value).init(arena);
    var it = json.Elements.root(text);
    while (it.next()) |raw| try out.append(switch (try json.cell(arena, raw)) {
        .null => .null,
        .text => |t| .{ .string = t },
    });
    return out.items;
}

test "explode splits delimited strings, repeats other columns, drops null cells" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const schema = types.Schema{ .fields = &.{
        .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
        .{ .name = "tags", .ty = types.Type.init(.string).asNullable() },
    } };
    const batches = [_]Batch{try kvBatch(a, &schema, &.{ 1, 2, 3, 4 }, &.{ "a,b", null, "c", "" })};
    var ts = TestSource{ .schema_ = schema, .batches = &batches };
    var scan = Scan{ .src = ts.src() };
    var ex = Explode{ .child = .{ .scan = &scan }, .field_idx = 1, .delim = ",", .out_schema = &schema };

    const b = (try (Op{ .explode = &ex }).next(a)).?;
    try testing.expectEqual(@as(usize, 4), b.len);
    const want_ids = [_]i64{ 1, 1, 3, 4 };
    const want_tags = [_][]const u8{ "a", "b", "c", "" };
    for (want_ids, want_tags, 0..) |wi, wt, r| {
        try testing.expectEqual(wi, b.columns[0].getValue(r).int);
        try testing.expectEqualStrings(wt, b.columns[1].getValue(r).string);
    }
}
