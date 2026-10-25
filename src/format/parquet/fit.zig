//! A table whose own schema governs its Parquet files, as a Delta table's does: the
//! files are the table's live ones, each read by column name and fitted to the
//! table. A column a file lacks reads as null (a column added since it was written),
//! a partition column is the file's constant from the table's log and wins over any
//! copy in the file, and a column whose kind differs from the table's is refused
//! naming the file. A decimal differing in precision alone is relabelled.
//! A pushed bound on a partition column skips a whole file without opening it.
//!
//! A table may name its files' columns otherwise than itself (`Mapping`, Delta's
//! column mapping): by a physical name or a Parquet field id. A file's footer is
//! then renamed to the table's names before anything reads it, so projection,
//! nested names and row-group statistics all see the table's; a column the table
//! does not map (dropped, or an old column whose name a new one took) is renamed
//! to a name no column has, so it is never read. A file may also have rows the
//! table deletes (`Deletes`), which its reader skips.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Bound = @import("stats.zig").Bound;
const Bytes = @import("../bytes.zig").Bytes;
const Reader = @import("read.zig").Reader;
const SchemaElement = @import("footer.zig").SchemaElement;
const Value = @import("../../exec/value.zig").Value;
const cmp = @import("stats.zig").cmp;
const column = @import("../../exec/column.zig");
const eval = @import("../../exec/eval.zig");
const parseFooterOf = @import("folder.zig").parseFooterOf;
const roaring = @import("../lake/roaring.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const Error = error{ LakeFileMismatch, LakeEmptySchema };

pub const Layout = struct {
    /// How messages name the table: "Delta table `s3://b/t/`".
    name: []const u8,
    root: []const u8,
    schema: types.Schema,
    /// The schema indices of the partition columns, each file's `parts` aligned to it.
    part_fields: []const usize = &.{},
    files: []const File,
    mapping: ?*const Mapping = null,

    pub const File = struct {
        path: []const u8,
        size: ?u64 = null,
        parts: []const Value = &.{},
        deletes: ?Deletes = null,
    };

    pub fn paths(self: *const Layout, arena: std.mem.Allocator) ![]const []const u8 {
        const out = try arena.alloc([]const u8, self.files.len);
        for (out, self.files) |*o, f| o.* = f.path;
        return out;
    }

    fn partOf(self: *const Layout, field: usize) ?usize {
        return std.mem.indexOfScalar(usize, self.part_fields, field);
    }

    /// The table's columns a read keeps, in table order. A projection naming none
    /// (COUNT(*)) keeps one so batches carry a row count: a partition column when
    /// there is one, as it costs no read.
    pub fn view(self: *const Layout, arena: std.mem.Allocator, want: ?[]const []const u8) !*const View {
        var idx = std.array_list.Managed(usize).init(arena);
        for (self.schema.fields, 0..) |f, i| {
            const keep = if (want) |w| for (w) |n| {
                if (std.mem.eql(u8, n, f.name)) break true;
            } else false else true;
            if (keep) try idx.append(i);
        }
        if (idx.items.len == 0) {
            if (self.schema.fields.len == 0) return Error.LakeEmptySchema;
            try idx.append(if (self.part_fields.len > 0) self.part_fields[0] else 0);
        }
        const fields = try arena.alloc(types.Schema.Field, idx.items.len);
        for (fields, idx.items) |*f, i| f.* = self.schema.fields[i];
        const v = try arena.create(View);
        v.* = .{ .schema = .{ .fields = fields }, .fields = idx.items };
        return v;
    }

    /// Whether a file's partition constants rule out every row a bound keeps; a
    /// null constant fails every comparison.
    pub fn pruned(self: *const Layout, i: usize, bounds: []const Bound) bool {
        const file = self.files[i];
        for (bounds) |b| {
            const fi = self.schema.indexOf(b.column) orelse continue;
            const p = self.partOf(fi) orelse continue;
            if (p >= file.parts.len) continue;
            const c = file.parts[p];
            if (c == .null) return true;
            const o = cmp(c, b.value) orelse continue;
            const keeps = switch (b.op) {
                .lt => o == .lt,
                .le => o != .gt,
                .gt => o == .gt,
                .ge => o != .lt,
                .eq => o == .eq,
            };
            if (!keeps) return true;
        }
        return false;
    }

    /// File `i` opened for `v`, its batches fitted to it; null when `bounds` prune it.
    pub fn open(self: *const Layout, arena: std.mem.Allocator, v: *const View, i: usize, bounds: []const Bound) !?*Reader {
        if (self.pruned(i, bounds)) return null;
        const file = self.files[i];
        var names = std.array_list.Managed([]const u8).init(arena);
        for (v.fields) |fi| if (self.partOf(fi) == null) try names.append(self.schema.fields[fi].name);
        const r = self.openFile(arena, file, names.items) catch |e| {
            if (e == error.OutOfMemory) return e;
            const why = eval.peekFailure(e) orelse @errorName(e);
            return eval.explain(e, std.fmt.allocPrint(arena, "{s} could not read its file `{s}` ({s})", .{ self.name, below(file.path, self.root), why }) catch self.name);
        };
        errdefer r.close();
        r.deletes = file.deletes;
        const outs = try arena.alloc(Fit.Out, v.fields.len);
        for (outs, v.fields) |*o, fi| {
            const want = self.schema.fields[fi];
            if (self.partOf(fi)) |p| {
                o.* = .{ .value = if (p < file.parts.len) file.parts[p] else .null };
                continue;
            }
            const k = r.schema.indexOf(want.name) orelse {
                o.* = .{ .value = .null };
                continue;
            };
            const got = r.schema.fields[k].ty;
            const fits = got.kind == want.ty.kind and (got.kind != .decimal or got.scale == want.ty.scale);
            if (!fits) return eval.explain(Error.LakeFileMismatch, std.fmt.allocPrint(arena, "{s} has file `{s}` whose column `{s}` is {s}, not {s} as in the table's schema", .{
                self.name, below(file.path, self.root), want.name, try got.name(arena), try want.ty.name(arena),
            }) catch self.name);
            o.* = .{ .column = k };
        }
        const f = try arena.create(Fit);
        f.* = .{ .schema = &v.schema, .outs = outs };
        r.fit = f;
        return r;
    }

    fn openFile(self: *const Layout, arena: std.mem.Allocator, file: File, names: []const []const u8) !*Reader {
        const m = self.mapping orelse return Reader.openSized(arena, file.path, file.size, names);
        const src = try Bytes.openSized(arena, file.path, file.size);
        errdefer src.close();
        var footer_start: u64 = 0;
        var md = try parseFooterOf(arena, src, &footer_start);
        md.schema = try m.rename(arena, md.schema);
        return Reader.openParsed(arena, src, md, footer_start, file.path, names);
    }
};

/// The rows a table deletes from one of its files, loaded when the file's first
/// row group is read: `load` is given the file's row count, and refuses a
/// deletion past it.
pub const Deletes = struct {
    ctx: *const anyopaque,
    load: *const fn (ctx: *const anyopaque, arena: std.mem.Allocator, rows: u64) anyerror!roaring.Bitmap,
};

/// How a table's columns are found in its files: each by a physical name or a
/// field id, a struct's fields likewise, and a list's element or a map's key and
/// value by position, as Parquet nests them.
pub const Mapping = struct {
    by: By,
    fields: []const Node,
    /// Each struct field by its parent node (0 for the table) and its physical
    /// name or id, built once by `init` and only read after, so lanes share it.
    index: std.HashMapUnmanaged(Key, *const Node, KeyContext, std.hash_map.default_max_load_percentage) = .empty,

    pub const By = enum { name, id };

    const Key = struct { parent: usize, name: []const u8 = "", id: i32 = 0 };

    const KeyContext = struct {
        pub fn hash(_: KeyContext, k: Key) u64 {
            var h = std.hash.Wyhash.init(k.parent);
            h.update(k.name);
            h.update(std.mem.asBytes(&k.id));
            return h.final();
        }
        pub fn eql(_: KeyContext, a: Key, b: Key) bool {
            return a.parent == b.parent and a.id == b.id and std.mem.eql(u8, a.name, b.name);
        }
    };

    pub fn init(arena: std.mem.Allocator, by: By, fields: []const Node) !Mapping {
        var m = Mapping{ .by = by, .fields = fields };
        try m.add(arena, 0, fields);
        return m;
    }

    fn add(self: *Mapping, arena: std.mem.Allocator, parent: usize, nodes: []const Node) !void {
        for (nodes) |*n| {
            if (n.name != null) {
                const k: ?Key = switch (self.by) {
                    .name => .{ .parent = parent, .name = n.physical },
                    .id => if (n.id) |id| .{ .parent = parent, .id = id } else null,
                };
                if (k) |key| try self.index.put(arena, key, n);
            }
            try self.add(arena, @intFromPtr(n), n.children);
        }
    }

    fn find(self: *const Mapping, parent: usize, e: SchemaElement) ?*const Node {
        const k: Key = switch (self.by) {
            .name => .{ .parent = parent, .name = e.name },
            .id => .{ .parent = parent, .id = e.field_id orelse return null },
        };
        return self.index.get(k);
    }

    pub const Node = struct {
        /// The table's name; null for a list's element or a map's key or value,
        /// which keep the file's.
        name: ?[]const u8 = null,
        physical: []const u8 = "",
        id: ?i32 = null,
        kind: Kind = .leaf,
        /// A struct's fields, a list's element, or a map's key and value.
        children: []const Node = &.{},

        pub const Kind = enum { leaf, @"struct", list, map };
    };

    /// `schema` with every column renamed to the table's name for it, or to one
    /// no column has when the table does not map it. A file mapped by field id
    /// that carries none is refused rather than read as all NULLs.
    pub fn rename(self: *const Mapping, arena: std.mem.Allocator, schema: []const SchemaElement) ![]SchemaElement {
        if (schema.len == 0) return error.UnsupportedParquetSchema;
        if (self.by == .id) for (schema[1..]) |e| {
            if (e.field_id != null) break;
        } else return eval.explain(Error.LakeFileMismatch, "it carries no Parquet field ids, by which the table (column mapping mode `id`) finds its columns, so basalt does not read it as all NULLs");
        const out = try arena.dupe(SchemaElement, schema);
        var w = Renamer{ .arena = arena, .src = schema, .out = out, .m = self };
        try w.children(@intCast(@max(0, schema[0].num_children)), 0, 0);
        return out;
    }
};

/// Walks a file's schema depth-first beside a table's `Mapping`, writing each
/// element's new name into `out`.
const Renamer = struct {
    arena: std.mem.Allocator,
    src: []const SchemaElement,
    out: []SchemaElement,
    m: *const Mapping,
    pos: usize = 1,

    fn next(self: *Renamer, depth: usize) !SchemaElement {
        if (self.pos >= self.src.len or depth > 64) return error.UnsupportedParquetSchema;
        defer self.pos += 1;
        return self.src[self.pos];
    }

    fn children(self: *Renamer, count: usize, parent: usize, depth: usize) anyerror!void {
        for (0..count) |_| {
            const at = self.pos;
            const e = try self.next(depth);
            const node = self.m.find(parent, e);
            self.out[at].name = if (node) |n| n.name orelse e.name else try std.mem.concat(self.arena, u8, &.{ "\x00", e.name });
            try self.below(e, node, depth);
        }
    }

    fn below(self: *Renamer, e: SchemaElement, node: ?*const Mapping.Node, depth: usize) anyerror!void {
        const kids: usize = @intCast(@max(0, e.num_children));
        const n = node orelse return self.skip(kids, depth);
        switch (n.kind) {
            .leaf => return self.skip(kids, depth),
            .@"struct" => return self.children(kids, @intFromPtr(n), depth + 1),
            .list => {
                if (kids != 1 or n.children.len != 1 or self.pos >= self.src.len) return self.skip(kids, depth);
                const rep = self.src[self.pos];
                const wrapper = rep.num_children == 1 and !std.mem.eql(u8, rep.name, "array") and !std.mem.endsWith(u8, rep.name, "_tuple");
                if (wrapper) _ = try self.next(depth + 1);
                try self.inner(&n.children[0], depth + 2);
            },
            .map => {
                if (kids != 1 or n.children.len != 2 or self.pos >= self.src.len or self.src[self.pos].num_children != 2) return self.skip(kids, depth);
                _ = try self.next(depth + 1);
                try self.inner(&n.children[0], depth + 2);
                try self.inner(&n.children[1], depth + 2);
            },
        }
    }

    fn inner(self: *Renamer, node: *const Mapping.Node, depth: usize) anyerror!void {
        try self.below(try self.next(depth), node, depth);
    }

    fn skip(self: *Renamer, count: usize, depth: usize) anyerror!void {
        for (0..count) |_| {
            const e = try self.next(depth + 1);
            try self.skip(@intCast(@max(0, e.num_children)), depth + 1);
        }
    }
};

fn below(path: []const u8, root: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, path, root)) path[root.len..] else path;
}

/// The columns one read of a table keeps, as indices into its schema.
pub const View = struct {
    schema: types.Schema,
    fields: []const usize,
};

/// One file's batches as the table's: each output a column of the file's batch or
/// a constant repeated down the batch.
pub const Fit = struct {
    schema: *const types.Schema,
    outs: []const Out,

    pub const Out = union(enum) { column: usize, value: Value };

    pub fn apply(self: *const Fit, arena: std.mem.Allocator, b: Batch) !Batch {
        const cols = try arena.alloc(column.Column, self.outs.len);
        for (cols, self.outs, self.schema.fields) |*c, o, f| switch (o) {
            .column => |k| {
                c.* = b.columns[k];
                c.ty = f.ty;
            },
            .value => |val| {
                var bd = try column.Builder.initCapacity(arena, f.ty, b.len);
                try bd.appendRepeated(val, b.len);
                c.* = try bd.finish();
            },
        };
        return .{ .schema = self.schema, .columns = cols, .len = b.len };
    }
};

test "a view keeps table order, and a projection naming no column keeps a partition one" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lay = Layout{
        .name = "t",
        .root = "/t/",
        .schema = .{ .fields = &.{
            .{ .name = "id", .ty = types.Type.init(.int).asNullable() },
            .{ .name = "day", .ty = types.Type.init(.date).asNullable() },
            .{ .name = "amt", .ty = types.Type.decimal(10, 2).asNullable() },
        } },
        .part_fields = &.{1},
        .files = &.{
            .{ .path = "/t/a.parquet", .parts = &.{.{ .date = 10 }} },
            .{ .path = "/t/b.parquet", .parts = &.{.null} },
        },
    };
    const v = try lay.view(a, &.{ "amt", "id", "nosuch" });
    try testing.expectEqual(@as(usize, 2), v.fields.len);
    try testing.expectEqualStrings("id", v.schema.fields[0].name);
    try testing.expectEqualStrings("amt", v.schema.fields[1].name);
    const none = try lay.view(a, &.{});
    try testing.expectEqualStrings("day", none.schema.fields[0].name);

    const eq = [_]Bound{.{ .column = "day", .op = .eq, .value = .{ .date = 10 } }};
    try testing.expect(!lay.pruned(0, &eq));
    try testing.expect(lay.pruned(1, &eq));
    const gt = [_]Bound{.{ .column = "day", .op = .gt, .value = .{ .date = 10 } }};
    try testing.expect(lay.pruned(0, &gt));
    const other = [_]Bound{.{ .column = "id", .op = .gt, .value = .{ .int = 100 } }};
    try testing.expect(!lay.pruned(0, &other));
}

test "a fit repeats constants and relabels a file column with the table's type" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const file_schema = types.Schema{ .fields = &.{.{ .name = "amt", .ty = types.Type.decimal(9, 2) }} };
    var bd = column.Builder.init(a, types.Type.decimal(9, 2));
    try bd.append(.{ .decimal = .{ .unscaled = 150, .scale = 2 } });
    try bd.append(.null);
    var cols = [_]column.Column{try bd.finish()};
    const table = types.Schema{ .fields = &.{
        .{ .name = "amt", .ty = types.Type.decimal(10, 2).asNullable() },
        .{ .name = "region", .ty = types.Type.init(.string).asNullable() },
        .{ .name = "gone", .ty = types.Type.init(.int).asNullable() },
    } };
    const f = Fit{ .schema = &table, .outs = &.{ .{ .column = 0 }, .{ .value = .{ .string = "eu" } }, .{ .value = .null } } };
    const out = try f.apply(a, .{ .schema = &file_schema, .columns = &cols, .len = 2 });
    try testing.expectEqual(@as(usize, 3), out.columns.len);
    try testing.expectEqual(@as(u8, 10), out.columns[0].ty.precision);
    try testing.expectEqual(@as(i128, 150), out.columns[0].getValue(0).decimal.unscaled);
    try testing.expectEqualStrings("eu", out.columns[1].getValue(1).string);
    try testing.expect(out.columns[2].getValue(0) == .null);
}

test "a mapping renames a file's columns to the table's, nested ones too, and hides what it does not map" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = [_]SchemaElement{
        .{ .name = "schema", .num_children = 5 },
        .{ .name = "c1", .ty = .int64, .repetition = .optional, .field_id = 1 },
        .{ .name = "c2", .repetition = .optional, .num_children = 1, .field_id = 2 },
        .{ .name = "c3", .ty = .int32, .repetition = .optional, .field_id = 3 },
        .{ .name = "c4", .repetition = .optional, .num_children = 1, .converted_type = 3, .field_id = 4 },
        .{ .name = "list", .repetition = .repeated, .num_children = 1 },
        .{ .name = "element", .repetition = .optional, .num_children = 1 },
        .{ .name = "c5", .ty = .int32, .repetition = .optional, .field_id = 5 },
        .{ .name = "c6", .repetition = .optional, .num_children = 1, .converted_type = 1, .field_id = 6 },
        .{ .name = "key_value", .repetition = .repeated, .num_children = 2 },
        .{ .name = "key", .ty = .int32, .repetition = .required },
        .{ .name = "value", .repetition = .optional, .num_children = 1 },
        .{ .name = "c7", .ty = .int32, .repetition = .optional, .field_id = 7 },
        .{ .name = "old", .ty = .int32, .repetition = .optional, .field_id = 9 },
    };
    const leaf = Mapping.Node{};
    const fields = [_]Mapping.Node{
        .{ .name = "a", .physical = "c1", .id = 1 },
        .{ .name = "s", .physical = "c2", .id = 2, .kind = .@"struct", .children = &.{.{ .name = "x", .physical = "c3", .id = 3 }} },
        .{ .name = "l", .physical = "c4", .id = 4, .kind = .list, .children = &.{.{ .kind = .@"struct", .children = &.{.{ .name = "y", .physical = "c5", .id = 5 }} }} },
        .{ .name = "m", .physical = "c6", .id = 6, .kind = .map, .children = &.{ leaf, .{ .kind = .@"struct", .children = &.{.{ .name = "z", .physical = "c7", .id = 7 }} } } },
        .{ .name = "old", .physical = "c8", .id = 8 },
    };
    const want = [_][]const u8{ "schema", "a", "s", "x", "l", "list", "element", "y", "m", "key_value", "key", "value", "z", "\x00old" };
    for ([_]Mapping.By{ .name, .id }) |by| {
        const m = try Mapping.init(a, by, &fields);
        const got = try m.rename(a, &schema);
        for (want, got) |w, g| try testing.expectEqualStrings(w, g.name);
        try testing.expectEqualStrings("c1", schema[1].name);
    }

    var bare = schema;
    for (&bare) |*e| e.field_id = null;
    const by_id = try Mapping.init(a, .id, &fields);
    try testing.expectError(Error.LakeFileMismatch, by_id.rename(a, &bare));
    try testing.expect(std.mem.indexOf(u8, eval.takeFailure(Error.LakeFileMismatch).?, "carries no Parquet field ids") != null);
    const deep = try Mapping.init(a, .name, &fields);
    try testing.expectError(error.UnsupportedParquetSchema, deep.rename(a, bare[0..6]));
}
