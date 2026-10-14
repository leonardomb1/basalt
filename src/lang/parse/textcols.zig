//! Which of a CSV read's columns a query uses only through `CAST`, so the reader
//! hands them over as their text and each cast converts the cell as written: a
//! zip code `01310100` sniffed as an INT stays `01310100` under `CAST(cep AS
//! STRING)`, and `CAST(v AS DECIMAL(12,2))` parses the digits exactly.
//!
//! Every query (`parseQuery`) keeps a `ColUses`, chained to the query around it. A
//! column reference counts as a bare use of each part of its name, in its own query
//! and every query around it; `CAST(<column> AS T)` turns its innermost bare use into
//! a cast. A star, `IDENTIFIER(...)` or `search(...)` reads columns the parser cannot
//! name, which makes the query opaque. Uses are counted by name alone and propagate
//! outward, so a doubt always counts as a bare use: a column is read as text only
//! when nothing in the query could read it as its sniffed type.

const std = @import("std");
const ast = @import("../ast.zig");

pub const ColUses = struct {
    parent: ?*ColUses,
    bare: std.StringHashMapUnmanaged(u32) = .empty,
    cast: std.StringHashMapUnmanaged(void) = .empty,
    opaque_: bool = false,

    pub fn bareUse(self: *ColUses, a: std.mem.Allocator, q: ast.QualName) !void {
        var u: ?*ColUses = self;
        while (u) |c| : (u = c.parent) for (q.parts) |p| {
            const e = try c.bare.getOrPut(a, p);
            e.value_ptr.* = if (e.found_existing) e.value_ptr.* + 1 else 1;
        };
    }

    pub fn castUse(self: *ColUses, a: std.mem.Allocator, q: ast.QualName) !void {
        for (q.parts) |p| {
            if (self.bare.getPtr(p)) |n| n.* -|= 1;
            try self.cast.put(a, p, {});
        }
    }

    /// An `ORDER BY` name the SELECT list outputs reads that output, not the column.
    pub fn outputUse(self: *ColUses, q: ast.QualName) void {
        var u: ?*ColUses = self;
        while (u) |c| : (u = c.parent) for (q.parts) |p| {
            if (c.bare.getPtr(p)) |n| n.* -|= 1;
        };
    }

    pub fn opaqueUse(self: *ColUses, all: bool) void {
        var u: ?*ColUses = self;
        while (u) |c| : (u = if (all) c.parent else null) c.opaque_ = true;
    }

    /// The columns cast and never used bare, none when the query is opaque.
    pub fn textCols(self: *const ColUses, a: std.mem.Allocator) ![]const []const u8 {
        if (self.opaque_) return &.{};
        var out = std.array_list.Managed([]const u8).init(a);
        var it = self.cast.keyIterator();
        while (it.next()) |k| {
            if ((self.bare.get(k.*) orelse 0) == 0) try out.append(k.*);
        }
        return out.toOwnedSlice();
    }
};
