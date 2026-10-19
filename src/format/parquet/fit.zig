//! A table whose own schema governs its Parquet files, as a Delta or Iceberg table's
//! does: the files are the table's live ones, each read by column name and fitted
//! to the table. A column a file lacks reads as null (a column added since it was
//! written), a partition column is the file's constant from the table's log and wins
//! over any copy in the file, and a column whose kind differs from the table's is
//! refused naming the file. A decimal differing in precision alone is relabelled.
//! A pushed bound on a partition column skips a whole file without opening it.
//! When the table gives its columns field ids (Iceberg's) and a file carries them
//! too, a column the file holds under another name, or a name the file gives
//! another id, is refused: read by name it would be null, or another column.

const std = @import("std");
const Batch = @import("../../exec/batch.zig").Batch;
const Bound = @import("stats.zig").Bound;
const Reader = @import("read.zig").Reader;
const Value = @import("../../exec/value.zig").Value;
const collectLeaves = @import("schema.zig").collectLeaves;
const cmp = @import("stats.zig").cmp;
const column = @import("../../exec/column.zig");
const eval = @import("../../exec/eval.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const Error = error{ LakeFileMismatch, LakeEmptySchema };

pub const Layout = struct {
    /// How messages name the table: "Delta table `s3://b/t/`".
    name: []const u8,
    root: []const u8,
    schema: types.Schema,
    /// Each column's field id, aligned with `schema`; empty when the table has none.
    ids: []const ?i64 = &.{},
    /// The schema indices of the partition columns, each file's `parts` aligned to it.
    part_fields: []const usize = &.{},
    files: []const File,

    pub const File = struct {
        path: []const u8,
        size: ?u64 = null,
        parts: []const Value = &.{},
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
        const r = Reader.openSized(arena, file.path, file.size, names.items) catch |e| {
            if (e == error.OutOfMemory) return e;
            const why = eval.peekFailure(e) orelse @errorName(e);
            return eval.explain(e, std.fmt.allocPrint(arena, "{s} could not read its file `{s}` ({s})", .{ self.name, below(file.path, self.root), why }) catch self.name);
        };
        errdefer r.close();
        if (self.ids.len > 0) try self.checkIds(arena, r, file, v);
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

    /// Refuses a file whose field ids say a wanted column is held under another
    /// name, or that a name the table reads is another column; a file without
    /// field ids is read by name.
    fn checkIds(self: *const Layout, arena: std.mem.Allocator, r: *const Reader, file: File, v: *const View) !void {
        for (r.md.schema) |e| {
            if (e.field_id != null) break;
        } else return;
        const all = try collectLeaves(arena, r.md.schema);
        for (v.fields) |fi| {
            if (self.partOf(fi) != null) continue;
            const want = self.ids[fi] orelse continue;
            const name = self.schema.fields[fi].name;
            var named: ?[]const u8 = null;
            var name_id: ?i32 = null;
            var has_name = false;
            for (all) |lf| {
                const e = r.md.schema[if (lf.root) |rt| rt.idx else lf.schema_idx];
                if (std.mem.eql(u8, lf.name, name)) {
                    has_name = true;
                    name_id = e.field_id;
                }
                if (e.field_id) |id| if (id == want) {
                    named = lf.name;
                };
            }
            if (has_name and (name_id == null or name_id.? == want)) continue;
            if (!has_name and named == null) continue;
            const rel = below(file.path, self.root);
            const msg = if (has_name)
                std.fmt.allocPrint(arena, "{s} has file `{s}` whose column `{s}` has field id {d}, not {d} as in the table's schema: a column was dropped and another added under its name, and basalt reads data files by column name", .{ self.name, rel, name, name_id.?, want })
            else
                std.fmt.allocPrint(arena, "{s} has file `{s}` holding column `{s}` (field id {d}) as `{s}`: the column was renamed, and basalt reads data files by column name", .{ self.name, rel, name, want, named.? });
            return eval.explain(Error.LakeFileMismatch, msg catch self.name);
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
                for (0..b.len) |_| try bd.append(val);
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
