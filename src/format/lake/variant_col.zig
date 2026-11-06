//! Variant columns at any depth of a table's type: a top-level column, a struct's
//! field, a list's element or a map's value, found by the column's Delta type
//! (`variant` where one sits). `convert` walks a column beside its Delta type and
//! turns each variant there into JSON text (a STRING, as a query reads it) or raw
//! Variant binary (the struct of `value` and `metadata` a file holds, as a copy of
//! the row keeps it), leaving every other part fitted to the table's type as
//! `fit.reshape` fits it.
//!
//! A variant comes as a file holds it, the struct of `metadata`, `value` and,
//! shredded, `typed_value`, or as JSON text to write. A shredded value is rebuilt
//! as the Parquet shredding spec says: its `typed_value` when set (a primitive as
//! the variant type its Parquet type stands for, a struct as an object of the
//! fields set in it, with the rest of a partly shredded object from `value`, a
//! list as an array of its elements), else its `value`; a field both leave unset
//! is missing, an array element or a top-level value both leave unset a variant
//! null. A value the spec forbids (both set where they may not be, an object
//! field given twice or shredded beside a `value` that is no object) or a
//! `typed_value` of a type no variant primitive stands for (an unsigned integer,
//! a fixed-length binary that is no UUID or decimal) fails as `VariantCorrupt`. An unshredded value
//! copies through as its bytes.

const std = @import("std");
const column = @import("../../exec/column.zig");
const types = @import("../../lang/types.zig");
const footer = @import("../parquet/footer.zig");
const fit = @import("../parquet/fit.zig");
const variant = @import("variant.zig");
const testing = std.testing;

pub const Error = variant.Error || error{CastFailed};

pub const raw_type = types.Type{ .kind = .@"struct", .nullable = true, .fields = &.{
    .{ .name = "value", .ty = .{ .kind = .bytes, .nullable = true } },
    .{ .name = "metadata", .ty = .{ .kind = .bytes, .nullable = true } },
} };

pub const Mode = enum { text, raw };

/// Whether Delta type `jt` holds a variant anywhere.
pub fn holds(jt: std.json.Value) bool {
    switch (jt) {
        .string => |s| return std.mem.eql(u8, s, "variant"),
        .object => |o| {
            for ([_][]const u8{ "elementType", "keyType", "valueType" }) |k| if (o.get(k)) |x| if (holds(x)) return true;
            const fs = o.get("fields") orelse return false;
            if (fs != .array) return false;
            for (fs.array.items) |f| if (f == .object) if (f.object.get("type")) |t| if (holds(t)) return true;
            return false;
        },
        else => return false,
    }
}

/// Field `name` of Delta struct type `jt`'s fields.
fn fieldType(jt: std.json.Value, name: []const u8) ?std.json.Value {
    if (jt != .object) return null;
    const fs = jt.object.get("fields") orelse return null;
    if (fs != .array) return null;
    for (fs.array.items) |f| if (f == .object) {
        const n = f.object.get("name") orelse continue;
        if (n == .string and std.mem.eql(u8, n.string, name)) return f.object.get("type");
    };
    return null;
}

fn sub(jt: std.json.Value, key: []const u8) ?std.json.Value {
    return if (jt == .object) jt.object.get(key) else null;
}

/// Table type `ty` (a variant as STRING) with each variant where `jt` puts one as
/// `raw_type`.
pub fn rawType(arena: std.mem.Allocator, ty: types.Type, jt: std.json.Value) error{OutOfMemory}!types.Type {
    if (!holds(jt)) return ty;
    if (jt == .string) {
        var t = raw_type;
        t.nullable = ty.nullable;
        return t;
    }
    var out = ty;
    switch (ty.kind) {
        .@"struct" => {
            const fs = try arena.alloc(types.Type.Field, ty.fields.?.len);
            for (fs, ty.fields.?) |*o, f| o.* = .{ .name = f.name, .ty = if (fieldType(jt, f.name)) |x| try rawType(arena, f.ty, x) else f.ty };
            out.fields = fs;
        },
        .array => {
            const e = try arena.create(types.Type);
            e.* = try rawType(arena, ty.elem.?.*, sub(jt, "elementType") orelse .null);
            out.elem = e;
        },
        .map => {
            const kv = ty.fields.?;
            const fs = try arena.alloc(types.Type.Field, 2);
            fs[0] = kv[0];
            fs[1] = .{ .name = kv[1].name, .ty = try rawType(arena, kv[1].ty, sub(jt, "valueType") orelse .null) };
            out.fields = fs;
        },
        else => {},
    }
    return out;
}

pub const SNode = struct {
    e: footer.SchemaElement,
    kids: []const SNode,

    pub fn child(self: *const SNode, name: []const u8) ?*const SNode {
        for (self.kids) |*k| if (std.mem.eql(u8, k.e.name, name)) return k;
        return null;
    }

    /// A list's element: its repeated child's one child, or the repeated child
    /// itself where it is the element (the legacy two-level form).
    fn element(self: *const SNode) ?*const SNode {
        if (self.kids.len != 1) return null;
        const rep = &self.kids[0];
        if (rep.kids.len == 1 and rep.child("value") == null and rep.child("typed_value") == null) return &rep.kids[0];
        return rep;
    }

    fn mapValue(self: *const SNode) ?*const SNode {
        if (self.kids.len != 1 or self.kids[0].kids.len != 2) return null;
        return &self.kids[0].kids[1];
    }
};

/// The schema elements of a file's footer (the root first) as the root's tree.
pub fn schemaTree(arena: std.mem.Allocator, elems: []const footer.SchemaElement) error{ OutOfMemory, VariantCorrupt }!SNode {
    if (elems.len == 0) return error.VariantCorrupt;
    var at: usize = 0;
    return treeAt(arena, elems, &at, 0);
}

fn treeAt(arena: std.mem.Allocator, elems: []const footer.SchemaElement, at: *usize, depth: usize) error{ OutOfMemory, VariantCorrupt }!SNode {
    if (at.* >= elems.len or depth > 64) return error.VariantCorrupt;
    const e = elems[at.*];
    at.* += 1;
    const n: usize = @intCast(@max(0, e.num_children));
    const kids = try arena.alloc(SNode, n);
    for (kids) |*k| k.* = try treeAt(arena, elems, at, depth + 1);
    return .{ .e = e, .kids = kids };
}

/// File schema `elems` with each `typed_value` leaf holding a nanosecond
/// timestamp below a variant column (a top-level field of `fields` that `tops`
/// marks) as a plain int64, so the reader keeps the digits it would round to
/// microseconds; `primOf` reads such a leaf by the schema as it was.
pub fn rawNanos(arena: std.mem.Allocator, elems: []const footer.SchemaElement, fields: []const types.Schema.Field, tops: []const bool) error{OutOfMemory}![]footer.SchemaElement {
    const out = try arena.dupe(footer.SchemaElement, elems);
    if (elems.len == 0) return out;
    var at: usize = 1;
    const n: usize = @intCast(@max(0, elems[0].num_children));
    for (0..n) |_| {
        if (at >= out.len) break;
        const is_variant = for (fields, tops) |f, t| {
            if (t and std.mem.eql(u8, f.name, out[at].name)) break true;
        } else false;
        at = walkNanos(out, at, is_variant);
    }
    return out;
}

fn walkNanos(out: []footer.SchemaElement, at: usize, under: bool) usize {
    const e = &out[at];
    var next = at + 1;
    if (under and e.num_children == 0 and std.mem.eql(u8, e.name, "typed_value")) if (e.logical_type) |lt| if (lt == .timestamp and lt.timestamp.unit == .nanos) {
        e.logical_type = .{ .integer = .{ .bit_width = 64, .signed = true } };
        e.converted_type = null;
    };
    for (0..@as(usize, @intCast(@max(0, e.num_children)))) |_| {
        if (next >= out.len) break;
        next = walkNanos(out, next, under);
    }
    return next;
}

const Shape = union(enum) {
    prim: variant.Prim,
    array: *const Group,
    object: []const Member,
};

const Group = struct { typed: ?Shape };

const Member = struct { name: []const u8, group: Group };

/// The shape of shredded group `g` (a variant, an object's field or an array's
/// element): its `typed_value`'s, if it has one.
fn groupOf(arena: std.mem.Allocator, g: *const SNode, depth: usize) error{ OutOfMemory, VariantCorrupt }!Group {
    const t = g.child("typed_value") orelse return .{ .typed = null };
    return .{ .typed = try shapeOf(arena, t, depth + 1) };
}

fn shapeOf(arena: std.mem.Allocator, t: *const SNode, depth: usize) error{ OutOfMemory, VariantCorrupt }!Shape {
    if (depth > 64) return error.VariantCorrupt;
    if (t.kids.len == 0) return .{ .prim = try primOf(t.e) };
    if (t.e.converted_type == list_converted) {
        const el = t.element() orelse return error.VariantCorrupt;
        const g = try arena.create(Group);
        g.* = try groupOf(arena, el, depth);
        return .{ .array = g };
    }
    const ms = try arena.alloc(Member, t.kids.len);
    for (ms, t.kids) |*m, *k| m.* = .{ .name = k.e.name, .group = try groupOf(arena, k, depth) };
    return .{ .object = ms };
}

const list_converted = 3;

/// The variant primitive a shredded leaf's Parquet type stands for.
fn primOf(e: footer.SchemaElement) error{VariantCorrupt}!variant.Prim {
    const p = e.ty orelse return error.VariantCorrupt;
    if (e.logical_type) |lt| switch (lt) {
        .string => return .string,
        .uuid => return .uuid,
        .date => return .date,
        .decimal => |d| return decimalOf(d.precision),
        .time => return .time,
        .timestamp => |ts| return switch (ts.unit) {
            .nanos => if (ts.adjusted_to_utc) .timestamp_nanos else .timestamp_nanos_ntz,
            else => if (ts.adjusted_to_utc) .timestamp else .timestamp_ntz,
        },
        .integer => |i| return if (!i.signed) error.VariantCorrupt else switch (i.bit_width) {
            8 => .int8,
            16 => .int16,
            32 => .int32,
            else => .int64,
        },
        else => {},
    };
    if (e.converted_type) |c| switch (c) {
        0 => return .string,
        5 => return decimalOf(e.precision orelse 38),
        6 => return .date,
        8 => return .time,
        10 => return .timestamp,
        15 => return .int8,
        16 => return .int16,
        else => {},
    };
    return switch (p) {
        .boolean => .boolean,
        .int32 => .int32,
        .int64 => .int64,
        .float => .float,
        .double => .double,
        .byte_array => .binary,
        else => error.VariantCorrupt,
    };
}

fn decimalOf(precision: i32) variant.Prim {
    if (precision <= 9) return .decimal4;
    if (precision <= 18) return .decimal8;
    return .decimal16;
}

/// Column `col` (shaped as a file or the rows hold it) in table type `want`, its
/// variants where `jt` puts them as `mode` asks; `node` is the file's schema
/// below the column, needed only to rebuild shredded values.
pub fn convert(arena: std.mem.Allocator, col: column.Column, want: types.Type, jt: std.json.Value, node: ?*const SNode, mode: Mode) Error!column.Column {
    if (!holds(jt)) return fit.reshape(arena, col, want);
    if (jt == .string) return leaf(arena, col, node, mode);
    const out_ty = try outType(arena, want, jt, mode);
    if (col.ty.kind != want.kind or col.data != .nested) return fit.reshape(arena, col, want);
    const nd = col.data.nested;
    const body: column.Nested.Body = switch (nd.body) {
        .list => |l| blk: {
            const child_want = column.childType(want);
            const kid = if (want.kind == .map)
                try mapEntries(arena, l.child, child_want, jt, if (node) |n| n.mapValue() else null, mode)
            else
                try convert(arena, l.child, child_want, sub(jt, "elementType") orelse .null, if (node) |n| n.element() else null, mode);
            break :blk .{ .list = .{ .offsets = l.offsets, .child = kid } };
        },
        .fields => |fs| blk: {
            const wf = want.fields orelse &.{};
            const kids = try arena.alloc(column.Column, wf.len);
            for (wf, kids) |f, *k| {
                const at = for (col.ty.fields orelse &.{}, 0..) |gf, i| {
                    if (std.mem.eql(u8, gf.name, f.name)) break i;
                } else null;
                const fj = fieldType(jt, f.name) orelse .null;
                k.* = if (at) |i|
                    try convert(arena, fs[i], f.ty, fj, if (node) |n| n.child(f.name) else null, mode)
                else
                    try nullsOf(arena, try outType(arena, f.ty, fj, mode), col.len);
            }
            break :blk .{ .fields = kids };
        },
    };
    const n = try arena.create(column.Nested);
    n.* = .{ .ty = out_ty, .body = body };
    var out = col;
    out.ty = out_ty;
    out.data = .{ .nested = n };
    return out;
}

/// A map's entries (the struct of `key` and `value`) with its values converted.
fn mapEntries(arena: std.mem.Allocator, entries: column.Column, want: types.Type, jt: std.json.Value, node: ?*const SNode, mode: Mode) Error!column.Column {
    const fs = entries.data.nested.body.fields;
    const kv = want.fields.?;
    const kids = try arena.alloc(column.Column, 2);
    kids[0] = try fit.reshape(arena, fs[0], kv[0].ty);
    kids[1] = try convert(arena, fs[1], kv[1].ty, sub(jt, "valueType") orelse .null, node, mode);
    const out_ty = types.Type{ .kind = .@"struct", .fields = try arena.dupe(types.Type.Field, &.{ kv[0], .{ .name = kv[1].name, .ty = kids[1].ty } }) };
    const n = try arena.create(column.Nested);
    n.* = .{ .ty = out_ty, .body = .{ .fields = kids } };
    var out = entries;
    out.ty = out_ty;
    out.data = .{ .nested = n };
    return out;
}

fn outType(arena: std.mem.Allocator, want: types.Type, jt: std.json.Value, mode: Mode) error{OutOfMemory}!types.Type {
    return if (mode == .raw) rawType(arena, want, jt) else textType(arena, want, jt);
}

/// `want` with each variant as STRING, whichever form it came in.
fn textType(arena: std.mem.Allocator, want: types.Type, jt: std.json.Value) error{OutOfMemory}!types.Type {
    if (!holds(jt)) return want;
    if (jt == .string) return types.Type.init(.string).asNullable();
    var out = want;
    switch (want.kind) {
        .@"struct" => {
            const fs = try arena.alloc(types.Type.Field, want.fields.?.len);
            for (fs, want.fields.?) |*o, f| o.* = .{ .name = f.name, .ty = if (fieldType(jt, f.name)) |x| try textType(arena, f.ty, x) else f.ty };
            out.fields = fs;
        },
        .array => {
            const e = try arena.create(types.Type);
            e.* = try textType(arena, want.elem.?.*, sub(jt, "elementType") orelse .null);
            out.elem = e;
        },
        .map => {
            const kv = want.fields.?;
            const fs = try arena.alloc(types.Type.Field, 2);
            fs[0] = kv[0];
            fs[1] = .{ .name = kv[1].name, .ty = try textType(arena, kv[1].ty, sub(jt, "valueType") orelse .null) };
            out.fields = fs;
        },
        else => {},
    }
    return out;
}

fn nullsOf(arena: std.mem.Allocator, ty: types.Type, n: usize) error{OutOfMemory}!column.Column {
    var b = try column.Builder.initCapacity(arena, ty.asNullable(), n);
    for (0..n) |_| try b.appendNull();
    return b.finish();
}

/// One variant column: JSON text kept or encoded (`raw`); a file's struct
/// decoded (`text`), copied through or rebuilt when shredded.
fn leaf(arena: std.mem.Allocator, col: column.Column, node: ?*const SNode, mode: Mode) Error!column.Column {
    if (col.ty.kind == .string) return if (mode == .text) col else encodeText(arena, col);
    if (col.ty.kind != .@"struct" or col.data != .nested) return error.VariantCorrupt;
    const fs = col.data.nested.body.fields;
    const at = struct {
        fn of(c: column.Column, name: []const u8) ?usize {
            for (c.ty.fields.?, 0..) |f, i| if (std.mem.eql(u8, f.name, name)) return i;
            return null;
        }
    }.of;
    const mi = at(col, "metadata") orelse return error.VariantCorrupt;
    const vi = at(col, "value");
    const ti = at(col, "typed_value");
    if (ti == null and mode == .raw) {
        const kids = try arena.alloc(column.Column, 2);
        kids[0] = if (vi) |i| fs[i] else try nullsOf(arena, .{ .kind = .bytes }, col.len);
        kids[1] = fs[mi];
        const n = try arena.create(column.Nested);
        n.* = .{ .ty = raw_type, .body = .{ .fields = kids } };
        var out = col;
        out.ty = raw_type;
        out.data = .{ .nested = n };
        return out;
    }
    const g: Group = if (ti != null) blk: {
        const sn = node orelse return error.VariantCorrupt;
        break :blk try groupOf(arena, sn, 0);
    } else .{ .typed = null };
    var text_b = if (mode == .text) try column.Builder.initCapacity(arena, types.Type.init(.string).asNullable(), col.len) else null;
    var vb = if (mode == .raw) try column.Builder.initCapacity(arena, types.Type.init(.bytes).asNullable(), col.len) else null;
    var mb = if (mode == .raw) try column.Builder.initCapacity(arena, types.Type.init(.bytes).asNullable(), col.len) else null;
    for (0..col.len) |r| {
        const meta = fs[mi].getValue(r);
        if (!col.validity.get(r) or meta == .null) {
            if (text_b) |*b| try b.appendNull() else {
                try vb.?.appendNull();
                try mb.?.appendNull();
            }
            continue;
        }
        if (ti == null) {
            const v = fs[vi orelse return error.VariantCorrupt].getValue(r);
            if (v == .null) return error.VariantCorrupt;
            try text_b.?.append(.{ .string = try variant.decode(arena, v.bytes, meta.bytes) });
            continue;
        }
        const tree = (try rebuild(arena, .{ .value = if (vi) |i| fs[i] else null, .typed = fs[ti.?] }, g, r, meta.bytes, 0)) orelse variant_null;
        if (text_b) |*b| {
            try b.append(.{ .string = try variant.render(arena, tree) });
        } else {
            const e = try variant.build(arena, tree);
            try vb.?.append(.{ .bytes = e.value });
            try mb.?.append(.{ .bytes = e.metadata });
        }
    }
    if (text_b) |*b| return b.finish();
    const kids = try arena.alloc(column.Column, 2);
    kids[0] = try vb.?.finish();
    kids[1] = try mb.?.finish();
    const n = try arena.create(column.Nested);
    n.* = .{ .ty = raw_type, .body = .{ .fields = kids } };
    var out = col;
    out.ty = raw_type;
    out.data = .{ .nested = n };
    return out;
}

/// JSON text column `col` as raw Variant binary; text that is not JSON fails.
fn encodeText(arena: std.mem.Allocator, col: column.Column) Error!column.Column {
    var vb = try column.Builder.initCapacity(arena, types.Type.init(.bytes).asNullable(), col.len);
    var mb = try column.Builder.initCapacity(arena, types.Type.init(.bytes).asNullable(), col.len);
    for (0..col.len) |r| {
        const v = col.getValue(r);
        if (v == .null) {
            try vb.appendNull();
            try mb.appendNull();
            continue;
        }
        const e = (try variant.encode(arena, v.string)) orelse {
            failed_text = v.string;
            return error.VariantCorrupt;
        };
        try vb.append(.{ .bytes = e.value });
        try mb.append(.{ .bytes = e.metadata });
    }
    const kids = try arena.alloc(column.Column, 2);
    kids[0] = try vb.finish();
    kids[1] = try mb.finish();
    const n = try arena.create(column.Nested);
    n.* = .{ .ty = raw_type, .body = .{ .fields = kids } };
    return .{ .ty = raw_type, .len = col.len, .validity = col.validity, .data = .{ .nested = n } };
}

pub threadlocal var failed_text: []const u8 = "";

const Cols = struct { value: ?column.Column, typed: ?column.Column };

const variant_null = variant.Node{ .prim = &[_]u8{0} };

/// Row `r` of shredded group `c` (shaped `g`) as a variant tree, null when the
/// group leaves it missing.
fn rebuild(arena: std.mem.Allocator, c: Cols, g: Group, r: usize, meta: []const u8, depth: usize) Error!?variant.Node {
    if (depth > 64) return error.VariantCorrupt;
    const v: ?[]const u8 = if (c.value) |vc| (if (vc.validity.get(r)) vc.getValue(r).bytes else null) else null;
    const tc = c.typed orelse return if (v) |b| try variant.parse(arena, b, meta) else null;
    const shape = g.typed orelse return error.VariantCorrupt;
    if (!tc.validity.get(r)) return if (v) |b| try variant.parse(arena, b, meta) else null;
    switch (shape) {
        .prim => |p| {
            if (v != null) return error.VariantCorrupt;
            return .{ .prim = try variant.primOf(arena, tc.getValue(r), p) };
        },
        .array => |eg| {
            if (v != null) return error.VariantCorrupt;
            const l = tc.data.nested.body.list;
            const lo: usize = @intCast(l.offsets[r]);
            const hi: usize = @intCast(l.offsets[r + 1]);
            const items = try arena.alloc(variant.Node, hi - lo);
            const ec = try groupCols(l.child);
            for (items, lo..) |*o, j| o.* = (try rebuild(arena, ec, eg.*, j, meta, depth + 1)) orelse variant_null;
            return .{ .array = items };
        },
        .object => |ms| {
            var fields = std.ArrayList(variant.Field).empty;
            const tf = tc.data.nested.body.fields;
            for (ms) |m| {
                const i = for (tc.ty.fields.?, 0..) |f, k| {
                    if (std.mem.eql(u8, f.name, m.name)) break k;
                } else return error.VariantCorrupt;
                const got = (try rebuild(arena, try groupCols(tf[i]), m.group, r, meta, depth + 1)) orelse continue;
                try fields.append(arena, .{ .key = m.name, .value = got });
            }
            if (v) |b| {
                const rest = try variant.parse(arena, b, meta);
                if (rest != .object) return error.VariantCorrupt;
                for (rest.object) |f| {
                    for (ms) |m| if (std.mem.eql(u8, m.name, f.key)) return error.VariantCorrupt;
                    try fields.append(arena, f);
                }
            }
            return .{ .object = fields.items };
        },
    }
}

/// A shredded group's columns: its `value` and `typed_value`.
fn groupCols(g: column.Column) Error!Cols {
    if (g.data != .nested or g.data.nested.body != .fields) return error.VariantCorrupt;
    const fs = g.data.nested.body.fields;
    var out = Cols{ .value = null, .typed = null };
    for (g.ty.fields.?, fs) |f, k| {
        if (std.mem.eql(u8, f.name, "value")) out.value = k;
        if (std.mem.eql(u8, f.name, "typed_value")) out.typed = k;
    }
    return out;
}

test "a Delta type holds a variant anywhere it nests one" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const j = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \\{"type":"struct","fields":[{"name":"s","type":{"type":"array","elementType":"variant","containsNull":true},"nullable":true,"metadata":{}}]}
    , .{});
    try testing.expect(holds(j));
    try testing.expect(!holds(.{ .string = "long" }));
    const t = types.Type{ .kind = .@"struct", .fields = &.{.{ .name = "s", .ty = .{ .kind = .array, .elem = &types.Type.init(.string) } }} };
    const raw = try rawType(a, t, j);
    try testing.expectEqual(types.TypeKind.@"struct", raw.fields.?[0].ty.elem.?.kind);
}

/// Whether variant trees `a` and `b` hold the same value of the same types: a
/// decimal by its scale and digits, a string whatever its length's encoding.
fn sameTree(arena: std.mem.Allocator, a: variant.Node, b: variant.Node) !bool {
    switch (a) {
        .prim => |x| {
            if (b != .prim) return false;
            const y = b.prim;
            const sx = stringOf(x);
            const sy = stringOf(y);
            if (sx != null or sy != null) return sx != null and sy != null and std.mem.eql(u8, sx.?, sy.?);
            const dx = decimalOf2(x);
            const dy = decimalOf2(y);
            if (dx != null or dy != null) return dx != null and dy != null and dx.?.scale == dy.?.scale and dx.?.unscaled == dy.?.unscaled;
            return std.mem.eql(u8, x, y);
        },
        .array => |xs| {
            if (b != .array or b.array.len != xs.len) return false;
            for (xs, b.array) |x, y| if (!try sameTree(arena, x, y)) return false;
            return true;
        },
        .object => |fs| {
            if (b != .object or b.object.len != fs.len) return false;
            const l = try arena.dupe(variant.Field, fs);
            const r = try arena.dupe(variant.Field, b.object);
            const lt = struct {
                fn f(_: void, p: variant.Field, q: variant.Field) bool {
                    return std.mem.order(u8, p.key, q.key) == .lt;
                }
            }.f;
            std.mem.sort(variant.Field, l, {}, lt);
            std.mem.sort(variant.Field, r, {}, lt);
            for (l, r) |p, q| if (!std.mem.eql(u8, p.key, q.key) or !try sameTree(arena, p.value, q.value)) return false;
            return true;
        },
    }
}

fn stringOf(p: []const u8) ?[]const u8 {
    if (p[0] & 3 == 1) return p[1..];
    if (p[0] >> 2 == 16) return p[5..];
    return null;
}

fn decimalOf2(p: []const u8) ?struct { scale: u8, unscaled: i128 } {
    return switch (p[0] >> 2) {
        8 => .{ .scale = p[1], .unscaled = std.mem.readInt(i32, p[2..6], .little) },
        9 => .{ .scale = p[1], .unscaled = std.mem.readInt(i64, p[2..10], .little) },
        10 => .{ .scale = p[1], .unscaled = std.mem.readInt(i128, p[2..18], .little) },
        else => null,
    };
}

test "shredded variants from Apache's corpus read as their expected values, as JSON text and as raw binary of the same types" {
    const delta = @import("delta.zig");
    const Folder = @import("../parquet/folder.zig").Folder;
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    const Case = struct { n: []const u8, file: []const u8, rows: []const []const u8 };
    const cases = [_]Case{
        .{ .n = "018", .file = @embedFile("../testdata/variant/case-018.parquet"), .rows = &.{@embedFile("../testdata/variant/case-018_row-0.variant.bin")} },
        .{ .n = "033", .file = @embedFile("../testdata/variant/case-033.parquet"), .rows = &.{@embedFile("../testdata/variant/case-033_row-0.variant.bin")} },
        .{ .n = "044", .file = @embedFile("../testdata/variant/case-044.parquet"), .rows = &.{@embedFile("../testdata/variant/case-044_row-0.variant.bin")} },
        .{ .n = "126", .file = @embedFile("../testdata/variant/case-126.parquet"), .rows = &.{ @embedFile("../testdata/variant/case-126_row-0.variant.bin"), @embedFile("../testdata/variant/case-126_row-1.variant.bin") } },
        .{ .n = "134", .file = @embedFile("../testdata/variant/case-134.parquet"), .rows = &.{@embedFile("../testdata/variant/case-134_row-0.variant.bin")} },
        .{ .n = "040", .file = @embedFile("../testdata/variant/case-040.parquet"), .rows = &.{} },
    };
    for (cases) |c| {
        const dir = try std.fmt.allocPrint(a, "c{s}/_delta_log", .{c.n});
        try tmp.dir.makePath(dir);
        try tmp.dir.writeFile(.{ .sub_path = try std.fmt.allocPrint(a, "c{s}/x.parquet", .{c.n}), .data = c.file });
        try tmp.dir.writeFile(.{ .sub_path = try std.fmt.allocPrint(a, "{s}/00000000000000000000.json", .{dir}), .data = try std.fmt.allocPrint(a,
            \\{{"protocol":{{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["variantType","variantShredding"],"writerFeatures":["variantType","variantShredding"]}}}}
            \\{{"metaData":{{"id":"x","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{{\"type\":\"struct\",\"fields\":[{{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{{}}}},{{\"name\":\"var\",\"type\":\"variant\",\"nullable\":true,\"metadata\":{{}}}}]}}","partitionColumns":[],"configuration":{{}}}}}}
            \\{{"add":{{"path":"x.parquet","partitionValues":{{}},"size":{d},"modificationTime":0,"dataChange":true}}}}
            \\
        , .{c.file.len}) });
        const root = try std.fmt.allocPrint(a, "{s}/c{s}/", .{ base, c.n });
        const r = try delta.resolve(a, root, .{}, null);
        const raw = try a.create(fit.Layout);
        raw.* = try delta.rawVariants(a, r.layout.*);
        for ([_]*const fit.Layout{ r.layout, raw }) |lay| {
            const src = (try Folder.openLayout(a, lay, null)).source();
            defer src.close();
            var got: usize = 0;
            while (true) {
                const b = src.next(a) catch |e| {
                    try testing.expect(c.rows.len == 0);
                    try testing.expect(e != error.OutOfMemory);
                    break;
                } orelse break;
                const col = b.columns[1];
                for (0..b.len) |i| {
                    try testing.expect(got < c.rows.len);
                    const want = c.rows[got];
                    const ml = try variant.metadataLen(want);
                    if (lay == r.layout) {
                        try testing.expectEqualStrings(try variant.decode(a, want[ml..], want[0..ml]), col.getValue(i).string);
                    } else {
                        const kids = col.data.nested.body.fields;
                        const tree = try variant.parse(a, kids[0].getValue(i).bytes, kids[1].getValue(i).bytes);
                        try testing.expect(try sameTree(a, tree, try variant.parse(a, want[ml..], want[0..ml])));
                    }
                    got += 1;
                }
            }
            try testing.expectEqual(c.rows.len, got);
        }
    }
}
