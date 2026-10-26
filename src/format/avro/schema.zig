//! Avro schemas: the JSON a container file's header carries, parsed into a tree of
//! `Node`s with named types resolved (a record may name itself, as a linked list
//! does), and the columns a read gives them (`Plan`).
//!
//! A read is a table, so the top record's fields are its columns, a nested record's
//! fields flattened to dotted names (`addr.city`) as a Parquet struct's are, and a
//! value of any other type a single column `value`. A union of `null` and one type
//! is that type, nullable; one of numbers only is INT or FLOAT, of strings and enums
//! STRING. Arrays, maps, a record that recurses and any other union are one STRING
//! column holding the value as compact JSON, bytes as base64 and temporal values as
//! ISO text, as basalt's JSON writer writes them; a `null` branch of any union is a
//! SQL NULL, never the text `null`.
//!
//! Logical types map to what basalt keeps: `date` to DATE, `time-*` to TIME,
//! `timestamp-*` and `local-timestamp-*` to TIMESTAMP (nanoseconds floored to the
//! microsecond), `decimal` to DECIMAL(p,s), `uuid` to STRING. A decimal wider than 38
//! digits is refused naming its field rather than read inexactly; a logical type the
//! spec does not define, or one on the wrong type, reads as its underlying type, as
//! the spec asks.

const std = @import("std");
const msgfmt = @import("../msgfmt.zig");
const types = @import("../../lang/types.zig");
const stream = @import("../stream.zig");

pub const Error = error{ BadAvroSchema, UnsupportedAvro, OutOfMemory };

/// How deep a schema, and a value it decodes, may nest: an untrusted file is an
/// error past it, never a stack overflow.
pub const max_depth = 64;

pub const Logical = enum { none, date, time_millis, time_micros, timestamp_millis, timestamp_micros, timestamp_nanos, decimal, uuid };

pub const Node = struct {
    kind: Kind,
    logical: Logical = .none,
    precision: u8 = 0,
    scale: u8 = 0,
    name: []const u8 = "",
    fields: []const Field = &.{},
    symbols: []const []const u8 = &.{},
    items: ?*const Node = null,
    branches: []const *const Node = &.{},
    size: usize = 0,

    pub const Kind = enum { null, boolean, int, long, float, double, bytes, string, record, @"enum", array, map, @"union", fixed };
    pub const Field = struct { name: []const u8, node: *const Node };
};

/// `text`, a schema's JSON, as its tree; anything the spec does not allow fails
/// naming what, through `stream.explainf`.
pub fn parse(arena: std.mem.Allocator, text: []const u8) Error!*const Node {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return bad("the schema in its header is not JSON", .{}),
    };
    var p = Parser{ .arena = arena };
    return p.node(v, "");
}

inline fn bad(comptime fmt: []const u8, args: anytype) Error {
    return @errorCast(stream.explainf(error.BadAvroSchema, "a malformed Avro schema: " ++ fmt, args));
}

const Parser = struct {
    arena: std.mem.Allocator,
    named: std.StringHashMapUnmanaged(*Node) = .empty,
    depth: usize = 0,

    fn node(self: *Parser, v: std.json.Value, space: []const u8) Error!*const Node {
        self.depth += 1;
        defer self.depth -= 1;
        if (self.depth > max_depth) return bad("types nest deeper than {d}", .{max_depth});
        return switch (v) {
            .string => |s| self.byName(s, space),
            .array => |a| self.unionOf(a.items, space),
            .object => |o| self.complex(o, space),
            else => bad("a type is a name, an object or an array of types", .{}),
        };
    }

    fn make(self: *Parser, n: Node) Error!*Node {
        const out = try self.arena.create(Node);
        out.* = n;
        return out;
    }

    fn primitive(s: []const u8) ?Node.Kind {
        const prims = [_]Node.Kind{ .null, .boolean, .int, .long, .float, .double, .bytes, .string };
        for (prims) |k| if (std.mem.eql(u8, s, @tagName(k))) return k;
        return null;
    }

    fn byName(self: *Parser, s: []const u8, space: []const u8) Error!*const Node {
        if (primitive(s)) |k| return self.make(.{ .kind = k });
        if (self.named.get(s)) |n| return n;
        if (std.mem.indexOfScalar(u8, s, '.') == null and space.len > 0) {
            const full = try msgfmt.allocPrint(self.arena, "{s}.{s}", .{ space, s });
            if (self.named.get(full)) |n| return n;
        }
        return bad("type `{s}` is not defined before it is used", .{s});
    }

    fn unionOf(self: *Parser, items: []const std.json.Value, space: []const u8) Error!*const Node {
        if (items.len == 0) return bad("a union has no types", .{});
        const branches = try self.arena.alloc(*const Node, items.len);
        for (items, branches) |it, *b| {
            b.* = try self.node(it, space);
            if (b.*.kind == .@"union") return bad("a union holds another union", .{});
        }
        for (branches, 0..) |a, i| for (branches[i + 1 ..]) |b| {
            const named = a.name.len > 0 or b.name.len > 0;
            if (if (named) std.mem.eql(u8, a.name, b.name) else a.kind == b.kind)
                return bad("a union holds the type `{s}` twice", .{if (a.name.len > 0) a.name else @tagName(a.kind)});
        };
        return self.make(.{ .kind = .@"union", .branches = branches });
    }

    fn complex(self: *Parser, o: std.json.ObjectMap, space: []const u8) Error!*const Node {
        const ty = o.get("type") orelse return bad("a type object has no `type`", .{});
        const t = switch (ty) {
            .string => |s| s,
            else => {
                const inner = try self.node(ty, space);
                return self.annotate(inner, o);
            },
        };
        if (std.mem.eql(u8, t, "record") or std.mem.eql(u8, t, "error")) return self.record(o, space);
        if (std.mem.eql(u8, t, "enum")) return self.enumOf(o, space);
        if (std.mem.eql(u8, t, "fixed")) return self.fixed(o, space);
        if (std.mem.eql(u8, t, "array")) {
            const items = o.get("items") orelse return bad("an array has no `items`", .{});
            return self.make(.{ .kind = .array, .items = try self.node(items, space) });
        }
        if (std.mem.eql(u8, t, "map")) {
            const values = o.get("values") orelse return bad("a map has no `values`", .{});
            return self.make(.{ .kind = .map, .items = try self.node(values, space) });
        }
        const base = try self.byName(t, space);
        return self.annotate(base, o);
    }

    /// A primitive with its `logicalType` read; a node already named is shared, so it
    /// is copied before it is annotated.
    fn annotate(self: *Parser, base: *const Node, o: std.json.ObjectMap) Error!*const Node {
        const lt = switch (o.get("logicalType") orelse return base) {
            .string => |s| s,
            else => return base,
        };
        var n = base.*;
        n.logical = logicalOf(lt, n.kind, n.size);
        if (n.logical == .decimal) {
            const p = intField(o, "precision") orelse return bad("a decimal has no `precision`", .{});
            const s = intField(o, "scale") orelse 0;
            if (p < 1 or s < 0 or s > p) return bad("a decimal's precision is {d} and its scale {d}", .{ p, s });
            if (n.kind == .fixed) {
                const most = maxDigits(n.size);
                if (p > most) return bad("a {d}-byte fixed decimal holds {d} digits at most, not {d}", .{ n.size, most, p });
            }
            if (p > 38) return @errorCast(stream.explainf(error.UnsupportedAvro, "a DECIMAL({d},{d}), wider than the 38 digits basalt holds", .{ p, s }));
            n.precision = @intCast(p);
            n.scale = @intCast(s);
        }
        if (n.logical == .none) return base;
        return self.make(n);
    }

    fn logicalOf(lt: []const u8, kind: Node.Kind, size: usize) Logical {
        const Case = struct { []const u8, Node.Kind, Logical };
        const cases = [_]Case{
            .{ "date", .int, .date },
            .{ "time-millis", .int, .time_millis },
            .{ "time-micros", .long, .time_micros },
            .{ "timestamp-millis", .long, .timestamp_millis },
            .{ "timestamp-micros", .long, .timestamp_micros },
            .{ "timestamp-nanos", .long, .timestamp_nanos },
            .{ "local-timestamp-millis", .long, .timestamp_millis },
            .{ "local-timestamp-micros", .long, .timestamp_micros },
            .{ "local-timestamp-nanos", .long, .timestamp_nanos },
            .{ "uuid", .string, .uuid },
        };
        for (cases) |c| if (std.mem.eql(u8, lt, c[0]) and kind == c[1]) return c[2];
        if (std.mem.eql(u8, lt, "decimal") and (kind == .bytes or (kind == .fixed and size > 0))) return .decimal;
        return .none;
    }

    fn maxDigits(size: usize) i64 {
        if (size >= 64) return std.math.maxInt(i64);
        const bits: f64 = @floatFromInt(8 * size - 1);
        return @intFromFloat(@floor(bits * std.math.log10(2.0)));
    }

    fn intField(o: std.json.ObjectMap, key: []const u8) ?i64 {
        return switch (o.get(key) orelse return null) {
            .integer => |i| i,
            else => null,
        };
    }

    fn strField(o: std.json.ObjectMap, key: []const u8) ?[]const u8 {
        return switch (o.get(key) orelse return null) {
            .string => |s| s,
            else => null,
        };
    }

    /// The fullname a named type defines, and registers it; a name defined twice is
    /// refused, as the spec does.
    fn define(self: *Parser, o: std.json.ObjectMap, space: []const u8, n: *Node) Error![]const u8 {
        const name = strField(o, "name") orelse return bad("a named type has no `name`", .{});
        const ns = strField(o, "namespace") orelse space;
        const full = if (std.mem.indexOfScalar(u8, name, '.') != null or ns.len == 0) name else try msgfmt.allocPrint(self.arena, "{s}.{s}", .{ ns, name });
        const gop = try self.named.getOrPut(self.arena, full);
        if (gop.found_existing) return bad("type `{s}` is defined twice", .{full});
        gop.value_ptr.* = n;
        n.name = full;
        return full;
    }

    fn spaceOf(full: []const u8) []const u8 {
        const dot = std.mem.lastIndexOfScalar(u8, full, '.') orelse return "";
        return full[0..dot];
    }

    fn record(self: *Parser, o: std.json.ObjectMap, space: []const u8) Error!*const Node {
        const n = try self.make(.{ .kind = .record });
        const full = try self.define(o, space, n);
        const list = switch (o.get("fields") orelse return bad("record `{s}` has no `fields`", .{full})) {
            .array => |a| a.items,
            else => return bad("record `{s}`'s `fields` is not an array", .{full}),
        };
        const fields = try self.arena.alloc(Node.Field, list.len);
        for (list, fields, 0..) |item, *f, i| {
            const fo = switch (item) {
                .object => |x| x,
                else => return bad("a field of record `{s}` is not an object", .{full}),
            };
            const fname = strField(fo, "name") orelse return bad("a field of record `{s}` has no `name`", .{full});
            for (fields[0..i]) |prev| if (std.mem.eql(u8, prev.name, fname)) return bad("record `{s}` has two fields `{s}`", .{ full, fname });
            const fty = fo.get("type") orelse return bad("field `{s}` of record `{s}` has no `type`", .{ fname, full });
            f.* = .{ .name = fname, .node = try self.node(fty, spaceOf(full)) };
        }
        n.fields = fields;
        return n;
    }

    fn enumOf(self: *Parser, o: std.json.ObjectMap, space: []const u8) Error!*const Node {
        const n = try self.make(.{ .kind = .@"enum" });
        const full = try self.define(o, space, n);
        const list = switch (o.get("symbols") orelse return bad("enum `{s}` has no `symbols`", .{full})) {
            .array => |a| a.items,
            else => return bad("enum `{s}`'s `symbols` is not an array", .{full}),
        };
        const syms = try self.arena.alloc([]const u8, list.len);
        for (list, syms) |item, *s| s.* = switch (item) {
            .string => |x| x,
            else => return bad("a symbol of enum `{s}` is not a string", .{full}),
        };
        n.symbols = syms;
        return n;
    }

    fn fixed(self: *Parser, o: std.json.ObjectMap, space: []const u8) Error!*const Node {
        const n = try self.make(.{ .kind = .fixed });
        const full = try self.define(o, space, n);
        const size = intField(o, "size") orelse return bad("fixed `{s}` has no `size`", .{full});
        if (size < 0) return bad("fixed `{s}` has a negative size", .{full});
        n.size = @intCast(size);
        const lt = strField(o, "logicalType") orelse return n;
        n.logical = logicalOf(lt, .fixed, n.size);
        if (n.logical != .decimal) {
            n.logical = .none;
            return n;
        }
        const annotated = try self.annotate(&.{ .kind = .fixed, .size = n.size }, o);
        n.logical = annotated.logical;
        n.precision = annotated.precision;
        n.scale = annotated.scale;
        return n;
    }
};

/// How each value of a row reaches the columns: `leaf` decodes into one column,
/// `text` renders as JSON into one, `record` runs its fields' steps in order, and
/// `choice` reads a union's branch and runs that branch's step, a `nulls` branch
/// setting every column of the union NULL.
pub const Step = union(enum) {
    leaf: Target,
    text: Target,
    record: []const Step,
    choice: Choice,
    nulls,

    pub const Target = struct { col: usize, node: *const Node };
    pub const Choice = struct { node: *const Node, branches: []const Step, lo: usize, hi: usize };
};

pub const Plan = struct {
    step: Step,
    schema: types.Schema,
};

/// The columns `root` reads as and the step that fills them.
pub fn plan(arena: std.mem.Allocator, root: *const Node) Error!Plan {
    var b = Planner{ .arena = arena };
    const step = if (root.kind == .record)
        try b.recordStep(root, "", false)
    else
        try b.valueStep(root, "value", false);
    return .{ .step = step, .schema = .{ .fields = b.cols.items } };
}

const Planner = struct {
    arena: std.mem.Allocator,
    cols: std.ArrayListUnmanaged(types.Schema.Field) = .empty,
    open: std.ArrayListUnmanaged([]const u8) = .empty,

    fn add(self: *Planner, name: []const u8, ty: types.Type) Error!usize {
        try self.cols.append(self.arena, .{ .name = name, .ty = ty });
        return self.cols.items.len - 1;
    }

    fn recordStep(self: *Planner, n: *const Node, prefix: []const u8, nullable: bool) Error!Step {
        try self.open.append(self.arena, n.name);
        defer _ = self.open.pop();
        const steps = try self.arena.alloc(Step, n.fields.len);
        for (n.fields, steps) |f, *s| {
            const name = if (prefix.len == 0) f.name else try msgfmt.allocPrint(self.arena, "{s}.{s}", .{ prefix, f.name });
            s.* = try self.valueStep(f.node, name, nullable);
        }
        return .{ .record = steps };
    }

    fn recursive(self: *Planner, n: *const Node) bool {
        for (self.open.items) |o| if (std.mem.eql(u8, o, n.name)) return true;
        return false;
    }

    fn valueStep(self: *Planner, n: *const Node, name: []const u8, nullable: bool) Error!Step {
        switch (n.kind) {
            .record => if (!self.recursive(n)) return self.recordStep(n, name, nullable),
            .@"union" => return self.unionStep(n, name, nullable),
            else => if (leafType(n)) |ty| return .{ .leaf = .{ .col = try self.add(name, ty.withNull(nullable or n.kind == .null)), .node = n } },
        }
        return .{ .text = .{ .col = try self.add(name, types.Type.init(.string).withNull(nullable)), .node = n } };
    }

    fn unionStep(self: *Planner, n: *const Node, name: []const u8, nullable: bool) Error!Step {
        var real: ?*const Node = null;
        var count: usize = 0;
        for (n.branches) |b| if (b.kind != .null) {
            real = b;
            count += 1;
        };
        const has_null = count < n.branches.len;
        const lo = self.cols.items.len;
        const branches = try self.arena.alloc(Step, n.branches.len);
        if (count == 0) {
            const col = try self.add(name, types.Type.nullColumn(true));
            for (branches) |*s| s.* = .{ .leaf = .{ .col = col, .node = n.branches[0] } };
        } else if (count == 1) {
            const inner = try self.valueStep(real.?, name, nullable or has_null);
            for (branches, n.branches) |*s, b| s.* = if (b.kind == .null) .nulls else inner;
        } else if (commonType(n.branches)) |ty| {
            const col = try self.add(name, ty.withNull(nullable or has_null));
            for (branches, n.branches) |*s, b| s.* = if (b.kind == .null) .nulls else .{ .leaf = .{ .col = col, .node = b } };
        } else {
            const col = try self.add(name, types.Type.init(.string).withNull(nullable or has_null));
            for (branches, n.branches) |*s, b| s.* = if (b.kind == .null) .nulls else .{ .text = .{ .col = col, .node = b } };
        }
        return .{ .choice = .{ .node = n, .branches = branches, .lo = lo, .hi = self.cols.items.len } };
    }
};

/// Whether a value of `n` may take no bytes at all, so a count of them is not
/// bounded by the bytes left in its block.
pub fn zeroWidth(n: *const Node) bool {
    return switch (n.kind) {
        .null => true,
        .fixed => n.size == 0,
        .record => for (n.fields) |f| {
            if (!zeroWidth(f.node)) break false;
        } else true,
        else => false,
    };
}

/// The column a plain value reads into, null for one read as JSON text.
pub fn leafType(n: *const Node) ?types.Type {
    return switch (n.kind) {
        .null => types.Type.nullColumn(true),
        .boolean => types.Type.init(.bool),
        .int => types.Type.init(switch (n.logical) {
            .date => .date,
            .time_millis => .time,
            else => .int,
        }),
        .long => types.Type.init(switch (n.logical) {
            .time_micros => .time,
            .timestamp_millis, .timestamp_micros, .timestamp_nanos => .timestamp,
            else => .int,
        }),
        .float, .double => types.Type.init(.float),
        .string, .@"enum" => types.Type.init(.string),
        .bytes, .fixed => if (n.logical == .decimal) types.Type.decimal(n.precision, n.scale) else types.Type.init(.bytes),
        .record, .array, .map, .@"union" => null,
    };
}

/// The one column a union of several types reads into: INT for whole numbers, FLOAT
/// once a float joins them, STRING for strings and enums; null for any other mix.
fn commonType(branches: []const *const Node) ?types.Type {
    var s: types.Sample = .none;
    for (branches) |b| if (b.kind != .null) {
        s = types.widenSample(s, sampleKind(b) orelse return null);
    };
    return switch (s) {
        .one => |k| types.Type.init(k),
        .none, .mixed => null,
    };
}

/// The kind a union branch adds to its union's sample: a plain number or string, an
/// enum or a uuid; null for any other branch, which no common column holds.
fn sampleKind(b: *const Node) ?types.TypeKind {
    const plain = b.logical == .none;
    return switch (b.kind) {
        .int, .long => if (plain) .int else null,
        .float, .double => if (plain) .float else null,
        .@"enum" => .string,
        .string => if (plain or b.logical == .uuid) .string else null,
        else => null,
    };
}

/// Whether `name` is one Avro accepts for a field: `[A-Za-z_][A-Za-z0-9_]*`.
pub fn validName(name: []const u8) bool {
    if (name.len == 0) return false;
    for (name, 0..) |c, i| switch (c) {
        'A'...'Z', 'a'...'z', '_' => {},
        '0'...'9' => if (i == 0) return false,
        else => return false,
    };
    return true;
}

const testing = std.testing;

fn planOf(a: std.mem.Allocator, text: []const u8) !Plan {
    return plan(a, try parse(a, text));
}

test "a record's fields are its columns, nested records flattened and the rest typed" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const p = try planOf(a,
        \\{"type":"record","name":"r","namespace":"ns","fields":[
        \\ {"name":"id","type":"long"},
        \\ {"name":"name","type":["null","string"]},
        \\ {"name":"addr","type":["null",{"type":"record","name":"A","fields":[{"name":"city","type":"string"},{"name":"zip","type":"int"}]}]},
        \\ {"name":"again","type":"ns.A"},
        \\ {"name":"tags","type":{"type":"array","items":"string"}},
        \\ {"name":"n","type":["int","long"]},
        \\ {"name":"x","type":["int","double","null"]},
        \\ {"name":"mix","type":["string","int"]},
        \\ {"name":"d","type":{"type":"int","logicalType":"date"}},
        \\ {"name":"ts","type":{"type":"long","logicalType":"local-timestamp-millis"}},
        \\ {"name":"amt","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
        \\ {"name":"f","type":{"type":"fixed","name":"F","size":8,"logicalType":"decimal","precision":18,"scale":4}},
        \\ {"name":"e","type":{"type":"enum","name":"E","symbols":["A","B"]}},
        \\ {"name":"odd","type":{"type":"string","logicalType":"date"}}
        \\]}
    );
    const want = [_]struct { []const u8, types.TypeKind, bool }{
        .{ "id", .int, false },      .{ "name", .string, true },        .{ "addr.city", .string, true },
        .{ "addr.zip", .int, true }, .{ "again.city", .string, false }, .{ "again.zip", .int, false },
        .{ "tags", .string, false }, .{ "n", .int, false },             .{ "x", .float, true },
        .{ "mix", .string, false },  .{ "d", .date, false },            .{ "ts", .timestamp, false },
        .{ "amt", .decimal, false }, .{ "f", .decimal, false },         .{ "e", .string, false },
        .{ "odd", .string, false },
    };
    try testing.expectEqual(want.len, p.schema.fields.len);
    for (want, p.schema.fields) |w, f| {
        try testing.expectEqualStrings(w[0], f.name);
        try testing.expectEqual(w[1], f.ty.kind);
        try testing.expectEqual(w[2], f.ty.nullable);
    }
    try testing.expectEqual(@as(u8, 18), p.schema.fields[13].ty.precision);
    try testing.expectEqual(@as(u8, 4), p.schema.fields[13].ty.scale);
}

test "a record that names itself reads its recursion as JSON text; a bare type is one column" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const p = try planOf(a,
        \\{"type":"record","name":"L","fields":[{"name":"v","type":"long"},{"name":"next","type":["null","L"]}]}
    );
    try testing.expectEqual(@as(usize, 2), p.schema.fields.len);
    try testing.expectEqualStrings("next", p.schema.fields[1].name);
    try testing.expectEqual(types.TypeKind.string, p.schema.fields[1].ty.kind);
    const bare = try planOf(a, "\"long\"");
    try testing.expectEqualStrings("value", bare.schema.fields[0].name);
}

test "a malformed or unsupported schema is refused naming why" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const bads = [_][]const u8{
        "{",
        "\"nosuch\"",
        "{\"type\":\"record\",\"name\":\"r\"}",
        "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"a\",\"type\":\"int\"}]}",
        "[\"int\",\"int\"]",
        "[]",
        "{\"type\":\"bytes\",\"logicalType\":\"decimal\"}",
        "{\"type\":\"fixed\",\"name\":\"f\",\"size\":2,\"logicalType\":\"decimal\",\"precision\":9}",
    };
    for (bads) |b| try testing.expectError(error.BadAvroSchema, parse(a, b));
    const deep = try msgfmt.allocPrint(a, "{s}\"int\"{s}", .{ "{\"type\":\"array\",\"items\":" ** 70, "}" ** 70 });
    try testing.expectError(error.BadAvroSchema, parse(a, deep));
    try testing.expectError(error.UnsupportedAvro, parse(a, "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":40}"));
}

test "a `null` field reads as a column of unknown type, so a UNION with any other column unifies" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const p = try planOf(a,
        \\{"type":"record","name":"r","fields":[{"name":"n","type":"null"},{"name":"u","type":["null"]},{"name":"s","type":["null","string"]}]}
    );
    for (p.schema.fields[0..2]) |f| {
        try testing.expectEqual(types.TypeKind.string, f.ty.kind);
        try testing.expect(f.ty.nullable);
        for ([_]types.Type{ types.Type.init(.int), types.Type.decimal(10, 2), types.Type.init(.date) }) |other| {
            const u = types.Type.unify(f.ty, other) orelse return error.TestUnexpectedResult;
            try testing.expectEqual(other.kind, u.kind);
        }
    }
    try testing.expect(!p.schema.fields[2].ty.unknown);
    try testing.expect(types.Type.unify(p.schema.fields[2].ty, types.Type.init(.int)) == null);
}

fn commonTypeBefore(branches: []const *const Node) ?types.Type {
    var ints = true;
    var nums = true;
    var strs = true;
    for (branches) |b| {
        if (b.kind == .null) continue;
        const plain = b.logical == .none;
        ints = ints and plain and (b.kind == .int or b.kind == .long);
        nums = nums and plain and (b.kind == .int or b.kind == .long or b.kind == .float or b.kind == .double);
        strs = strs and (b.kind == .@"enum" or (b.kind == .string and (plain or b.logical == .uuid)));
    }
    if (ints) return types.Type.init(.int);
    if (nums) return types.Type.init(.float);
    if (strs) return types.Type.init(.string);
    return null;
}

test "commonType on widenSample equals the flag lattice it replaced on every union of up to three branches" {
    const kinds = comptime std.enums.values(Node.Kind);
    const logicals = comptime std.enums.values(Logical);
    var nodes: [kinds.len * logicals.len]Node = undefined;
    for (kinds, 0..) |k, i| for (logicals, 0..) |l, j| {
        nodes[i * logicals.len + j] = .{ .kind = k, .logical = l };
    };
    var ptrs: [nodes.len]*const Node = undefined;
    for (&ptrs, &nodes) |*p, *n| p.* = n;
    var buf: [3]*const Node = undefined;
    var seen: usize = 0;
    for (0..ptrs.len) |x| for (0..ptrs.len) |y| for (0..ptrs.len + 1) |z| {
        buf[0] = ptrs[x];
        buf[1] = ptrs[y];
        var len: usize = 2;
        if (z < ptrs.len) {
            buf[2] = ptrs[z];
            len = 3;
        }
        var nonnull: usize = 0;
        for (buf[0..len]) |b| nonnull += @intFromBool(b.kind != .null);
        if (nonnull < 2) continue;
        const before = commonTypeBefore(buf[0..len]);
        const after = commonType(buf[0..len]);
        try testing.expectEqual(before == null, after == null);
        if (before) |t| try testing.expectEqual(t.kind, after.?.kind);
        seen += 1;
    };
    try testing.expect(seen > 100_000);
}

test "validName follows Avro's name rule" {
    try testing.expect(validName("a_1"));
    try testing.expect(validName("_x"));
    try testing.expect(!validName("1a"));
    try testing.expect(!validName("a.b"));
    try testing.expect(!validName(""));
    try testing.expect(!validName("é"));
}
