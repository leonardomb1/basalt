//! A nested column's values striped into its leaves, as Dremel shreds records:
//! each leaf gets, per value it holds, a repetition level (0 starts a row, a
//! higher one repeats the list or map at that depth), a definition level (how
//! deep the path is present: below the leaf's own `def`, an ancestor is null,
//! absent or empty) and, at its full depth, the value. A row is basalt's JSON text
//! for the column, walked where it lies (`json.Members`, `json.Elements`): no tree
//! is built and nothing is copied, a struct's fields picked into the scratch slots
//! its plan node owns (`shape.Node.slot`) as its object is read once.
//!
//! What a sink receives: `level(leaf, rep, def, value)`, the value the leaf's JSON
//! scalar as written, or a map key's content as written (escapes unresolved), or
//! null where the leaf is not reached. A JSON `null` is the absent value. A
//! container of the wrong JSON kind (an object for a list) is `NotNested`; a null
//! where the type is not nullable, a map key among them, is `NullInRequired`.
//! Duplicate object keys take the last, as `json.zig` reads them.

const std = @import("std");
const json = @import("../../exec/json.zig");
const shape = @import("shape.zig");

pub const Leaf = union(enum) { json: []const u8, key: []const u8 };

pub const Error = error{ InvalidJson, NotNested, NullInRequired, OutOfMemory };

/// The shredder for sink type `S`, whose `level` returns `S.Error!void`.
pub fn Shredder(comptime S: type) type {
    return struct {
        const Self = @This();
        pub const E = S.Error || Error;

        sh: *const shape.Shape,
        slots: []?[]const u8,
        sink: S,

        pub fn init(arena: std.mem.Allocator, sh: *const shape.Shape, sink: S) error{OutOfMemory}!Self {
            return .{ .sh = sh, .slots = try arena.alloc(?[]const u8, sh.slots), .sink = sink };
        }

        /// One row: `text` the column's JSON, null for a NULL value. The text is
        /// validated first, as the walk assumes valid JSON.
        pub fn row(self: *Self, arena: std.mem.Allocator, text: ?[]const u8) E!void {
            if (text) |t| try json.validate(arena, t);
            try self.value(self.sh.root, text, 0);
        }

        fn value(self: *Self, n: *const shape.Node, v: ?[]const u8, rep: u32) E!void {
            const t = v orelse return self.absent(n, rep);
            if (t[0] == 'n') return self.absent(n, rep);
            switch (n.kind) {
                .leaf => try self.sink.level(n.first_leaf, rep, n.def, .{ .json = t }),
                .group => {
                    if (t[0] != '{') return Error.NotNested;
                    const got = self.slots[n.slot..][0..n.children.len];
                    @memset(got, null);
                    var it = json.Members.root(t);
                    while (it.next()) |m| {
                        for (n.children, 0..) |k, i| if (std.mem.eql(u8, k.name, m.key)) {
                            got[i] = m.value;
                            break;
                        };
                    }
                    for (n.children, 0..) |*k, i| try self.value(k, got[i], rep);
                },
                .list => {
                    if (t[0] != '[') return Error.NotNested;
                    var it = json.Elements.root(t);
                    var r = rep;
                    while (it.next()) |e| : (r = n.entry_rep) try self.value(&n.children[0], e, r);
                    if (r == rep) try self.fill(n, rep, n.def);
                },
                .map => {
                    if (t[0] != '{') return Error.NotNested;
                    var it = json.Members.root(t);
                    var r = rep;
                    while (it.next()) |m| : (r = n.entry_rep) {
                        try self.sink.level(n.children[0].first_leaf, r, n.children[0].def, .{ .key = m.key });
                        try self.value(&n.children[1], m.value, r);
                    }
                    if (r == rep) try self.fill(n, rep, n.def);
                },
            }
        }

        /// A node that is null or not there: every leaf below it at the level of
        /// the node's parent.
        fn absent(self: *Self, n: *const shape.Node, rep: u32) E!void {
            if (!n.nullable) return Error.NullInRequired;
            try self.fill(n, rep, n.def - 1);
        }

        fn fill(self: *Self, n: *const shape.Node, rep: u32, def: u32) E!void {
            for (n.first_leaf..n.first_leaf + n.leaves) |l| try self.sink.level(@intCast(l), rep, def, null);
        }
    };
}

const testing = std.testing;
const types = @import("../../lang/types.zig");

const Rec = struct {
    leaf: u32,
    rep: u32,
    def: u32,
    v: ?[]const u8,
};

const Recorder = struct {
    pub const Error = error{OutOfMemory};
    out: *std.ArrayList(Rec),
    a: std.mem.Allocator,

    pub fn level(self: Recorder, leaf: u32, rep: u32, def: u32, v: ?Leaf) error{OutOfMemory}!void {
        const text: ?[]const u8 = if (v) |x| switch (x) {
            .json => |j| j,
            .key => |k| k,
        } else null;
        try self.out.append(self.a, .{ .leaf = leaf, .rep = rep, .def = def, .v = text });
    }
};

fn expectShred(a: std.mem.Allocator, ty: types.Type, rows: []const ?[]const u8, want: []const Rec) !void {
    const sh = try shape.of(a, "c", ty);
    var got: std.ArrayList(Rec) = .empty;
    var s = try Shredder(Recorder).init(a, &sh, .{ .out = &got, .a = a });
    for (rows) |r| try s.row(a, r);
    try testing.expectEqual(want.len, got.items.len);
    for (want, got.items) |w, g| {
        try testing.expectEqual(w.leaf, g.leaf);
        try testing.expectEqual(w.rep, g.rep);
        try testing.expectEqual(w.def, g.def);
        if (w.v) |wv| try testing.expectEqualStrings(wv, g.v.?) else try testing.expect(g.v == null);
    }
}

fn scalar(kind: types.TypeKind, nullable: bool) types.Type {
    return types.Type.init(kind).withNull(nullable);
}

test "shred: a struct's lists with a null element, empty, absent and a NULL row; a missing required field is refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const str = scalar(.string, true);
    const tags = types.Type{ .kind = .array, .nullable = true, .elem = &str };
    const doc = types.Type{ .kind = .@"struct", .nullable = true, .fields = &.{ .{ .name = "id", .ty = scalar(.int, false) }, .{ .name = "tags", .ty = tags } } };
    try expectShred(a, doc, &.{ "{\"id\":1,\"tags\":[\"a\",null]}", "{\"tags\":[],\"id\":2}", "{\"id\":3}", null }, &.{
        .{ .leaf = 0, .rep = 0, .def = 1, .v = "1" },
        .{ .leaf = 1, .rep = 0, .def = 4, .v = "\"a\"" },
        .{ .leaf = 1, .rep = 1, .def = 3, .v = null },
        .{ .leaf = 0, .rep = 0, .def = 1, .v = "2" },
        .{ .leaf = 1, .rep = 0, .def = 2, .v = null },
        .{ .leaf = 0, .rep = 0, .def = 1, .v = "3" },
        .{ .leaf = 1, .rep = 0, .def = 1, .v = null },
        .{ .leaf = 0, .rep = 0, .def = 0, .v = null },
        .{ .leaf = 1, .rep = 0, .def = 0, .v = null },
    });
    try testing.expectError(Error.NullInRequired, expectShred(a, doc, &.{"{\"tags\":[\"x\"]}"}, &.{}));
    try testing.expectError(Error.NotNested, expectShred(a, doc, &.{"{\"id\":1,\"tags\":{}}"}, &.{}));
    try testing.expectError(Error.InvalidJson, expectShred(a, doc, &.{"{\"id\":"}, &.{}));
}

test "shred: a map's keys and values, a null value and an empty map" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const m = types.Type{ .kind = .map, .nullable = true, .fields = &.{ .{ .name = "key", .ty = scalar(.string, false) }, .{ .name = "value", .ty = scalar(.int, true) } } };
    try expectShred(a, m, &.{ "{\"a\":1,\"b\":null}", "{}" }, &.{
        .{ .leaf = 0, .rep = 0, .def = 2, .v = "a" },
        .{ .leaf = 1, .rep = 0, .def = 3, .v = "1" },
        .{ .leaf = 0, .rep = 1, .def = 2, .v = "b" },
        .{ .leaf = 1, .rep = 1, .def = 2, .v = null },
        .{ .leaf = 0, .rep = 0, .def = 1, .v = null },
        .{ .leaf = 1, .rep = 0, .def = 1, .v = null },
    });
}

test "shred: a list of lists repeats at each depth, an empty inner list and a null one apart" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const int = scalar(.int, true);
    const inner = types.Type{ .kind = .array, .nullable = true, .elem = &int };
    const outer = types.Type{ .kind = .array, .nullable = true, .elem = &inner };
    try expectShred(a, outer, &.{ "[[1,2],[],null]", "[[3]]" }, &.{
        .{ .leaf = 0, .rep = 0, .def = 5, .v = "1" },
        .{ .leaf = 0, .rep = 2, .def = 5, .v = "2" },
        .{ .leaf = 0, .rep = 1, .def = 3, .v = null },
        .{ .leaf = 0, .rep = 1, .def = 2, .v = null },
        .{ .leaf = 0, .rep = 0, .def = 5, .v = "3" },
    });
}
