//! `UNNEST`: one output row per element of a delimited string or a JSON array.

const Batch = @import("../batch.zig").Batch;
const Op = @import("../op.zig").Op;
const Stats = @import("../op.zig").Stats;
const Value = @import("../value.zig").Value;
const column = @import("../column.zig");
const json = @import("../json.zig");
const std = @import("std");
const types = @import("../../lang/types.zig");

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

    /// Split a delimited string column, or with `json` a JSON array, into one row per
    /// element, other columns repeated. Null or missing cells produce zero rows.
    fn explodeBatch(self: *Explode, arena: std.mem.Allocator, b: Batch) anyerror!Batch {
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
