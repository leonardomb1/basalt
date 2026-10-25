//! Delta's type names and statistics text, held once for the reader, the writer
//! and the UPSERT merge:
//!   - the primitive name table: which basalt type a `schemaString` primitive is,
//!     how its values are laid out in Parquet, and the name a new table's column
//!     of a basalt type is given;
//!   - `fields`, one level of a struct type's fields, each caller keeping its own
//!     recursion and its own wording for a malformed schema;
//!   - a file's `minValues`/`maxValues` as the writer spells them (`statText`) and
//!     as the merge reads them back (`statValue`), with `stat_prefix`, the length
//!     past which a string statistic is cut.
//! The writer and reader disagree in places (a 32-byte string maximum, a
//! timestamp kept to milliseconds, an infinite float); those disagreements are
//! kept as they are and pinned by the tests below.

const std = @import("std");
const Value = @import("../../exec/value.zig").Value;
const cast = @import("../../exec/eval/cast.zig");
const decimal = @import("../../exec/decimal.zig");
const fmtnum = @import("../../exec/eval/format.zig");
const pqwrite = @import("../parquet/write.zig");
const table = @import("table.zig");
const time = @import("../../exec/eval/time.zig");
const types = @import("../../lang/types.zig");
const testing = std.testing;

pub const Error = error{DeltaValueOutOfRange};

/// A string statistic longer than this has no maximum, and its first this many
/// bytes (cut back to a character boundary) as its minimum; one this long or
/// longer read back as a maximum bounds nothing.
pub const stat_prefix = 32;

/// A primitive of `schemaString`: the basalt type its values are and their
/// Parquet layout; `writable` false for one basalt reads but does not write.
pub const Prim = struct {
    name: []const u8,
    kind: types.TypeKind,
    phys: pqwrite.Physical = .{},
    writable: bool = true,
};

pub const prims = [_]Prim{
    .{ .name = "string", .kind = .string },
    .{ .name = "void", .kind = .string, .writable = false },
    .{ .name = "long", .kind = .int },
    .{ .name = "integer", .kind = .int, .phys = .{ .int_bits = 32 } },
    .{ .name = "short", .kind = .int, .phys = .{ .int_bits = 16 } },
    .{ .name = "byte", .kind = .int, .phys = .{ .int_bits = 8 } },
    .{ .name = "double", .kind = .float },
    .{ .name = "float", .kind = .float, .phys = .{ .float32 = true } },
    .{ .name = "boolean", .kind = .bool },
    .{ .name = "binary", .kind = .bytes },
    .{ .name = "date", .kind = .date },
    .{ .name = "timestamp", .kind = .timestamp, .phys = .{ .utc = true } },
    .{ .name = "timestamp_ntz", .kind = .timestamp },
};

pub fn prim(t: []const u8) ?Prim {
    for (prims) |p| if (std.mem.eql(u8, t, p.name)) return p;
    return null;
}

/// Who asks for a primitive's type: a read takes `void`, a write does not.
pub const Use = enum { read, write };

/// The basalt type of primitive `t`, null for one basalt does not read (or write).
pub fn scalarType(t: []const u8, comptime use: Use) ?types.Type {
    if (prim(t)) |p| return if (use == .write and !p.writable) null else types.Type.init(p.kind);
    return decimalType(t);
}

/// `t`'s Parquet layout as the writer gives a table's existing column: narrow
/// integers, 4-byte floats, `timestamp` as UTC; the default for any other name.
pub fn physical(t: []const u8) pqwrite.Physical {
    return if (prim(t)) |p| p.phys else .{};
}

/// `decimal` as decimal(10,0), `decimal(p,s)` (spaces around either number
/// allowed) for 1 <= p <= 38 and s <= p; null for anything else.
pub fn decimalType(t: []const u8) ?types.Type {
    if (std.mem.eql(u8, t, "decimal")) return types.Type.decimal(10, 0);
    if (!std.mem.startsWith(u8, t, "decimal(") or !std.mem.endsWith(u8, t, ")")) return null;
    var it = std.mem.splitScalar(u8, t["decimal(".len .. t.len - 1], ',');
    const p = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch return null;
    const s = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse "", " "), 10) catch return null;
    if (p < 1 or p > 38 or s > p) return null;
    return types.Type.decimal(p, s);
}

/// The name a new table gives a column of non-decimal kind `k`, null for a kind
/// Delta has no primitive for (and for decimal, see `createdDecimal`).
pub fn deltaName(k: types.TypeKind) ?[]const u8 {
    return switch (k) {
        .bool => "boolean",
        .int => "long",
        .float => "double",
        .string => "string",
        .bytes => "binary",
        .date => "date",
        .timestamp => "timestamp",
        .decimal, .time, .array, .@"struct" => null,
    };
}

/// A decimal column of a new table: precision 0 (unbounded) as 38, at most 38;
/// null when its scale then exceeds its precision.
pub fn createdDecimal(t: types.Type) ?types.Type {
    const p: u8 = if (t.precision == 0) 38 else @min(38, t.precision);
    if (t.scale > p) return null;
    return types.Type.decimal(p, t.scale);
}

/// A field of a struct type in `schemaString`; `ty` is `.null` when it has none.
pub const Field = struct {
    name: []const u8,
    ty: std.json.Value,
    nullable: bool,
    metadata: ?std.json.ObjectMap,
};

/// What `fields` does with a malformed field: refuse it or pass over it.
pub const Bad = enum { refuse, skip };

/// `not_struct`, a type with no `fields` array, comes back under every lenience:
/// the reader and writer refuse it, the column mapping takes it as a leaf.
pub const Problem = enum { not_struct, not_object, nameless };

pub const Lenience = struct {
    not_object: Bad,
    nameless: Bad,

    /// The writer's: every malformed field refused.
    pub const strict = Lenience{ .not_object = .refuse, .nameless = .refuse };
    /// The reader's columns: a field that is not an object passed over.
    pub const reading = Lenience{ .not_object = .skip, .nameless = .refuse };
    /// The reader's column mapping: every malformed field passed over.
    pub const lenient = Lenience{ .not_object = .skip, .nameless = .skip };
};

pub const Walk = union(enum) {
    fields: []const Field,
    problem: Problem,
};

/// The fields of struct type `v`, one level, or the first malformed part
/// `lenience` refuses. A field's `nullable` is true unless it is `false`.
pub fn fields(arena: std.mem.Allocator, v: std.json.Value, comptime lenience: Lenience) !Walk {
    const list = if (v == .object) v.object.get("fields") else null;
    if (list == null or list.? != .array) return .{ .problem = .not_struct };
    var out = try std.array_list.Managed(Field).initCapacity(arena, list.?.array.items.len);
    for (list.?.array.items) |f| {
        if (f != .object) {
            if (lenience.not_object == .refuse) return .{ .problem = .not_object };
            continue;
        }
        const n = f.object.get("name") orelse .null;
        if (n != .string) {
            if (lenience.nameless == .refuse) return .{ .problem = .nameless };
            continue;
        }
        const nl = f.object.get("nullable");
        const md = f.object.get("metadata");
        out.appendAssumeCapacity(.{
            .name = n.string,
            .ty = f.object.get("type") orelse .null,
            .nullable = if (nl) |x| (x != .bool or x.bool) else true,
            .metadata = if (md) |m| (if (m == .object) m.object else null) else null,
        });
    }
    return .{ .fields = out.items };
}

pub fn jsonString(w: *std.Io.Writer, s: []const u8) !void {
    try std.json.Stringify.encodeJsonString(s, .{}, w);
}

/// `us` as `YYYY-MM-DD HH:MM:SS.ffffff`, with `T` for the space and a trailing `Z`
/// when `utc`; `ms` cuts the fraction to milliseconds. A year outside 1..9999 has
/// no such spelling and is `DeltaValueOutOfRange`.
pub fn writeTimestamp(w: *std.Io.Writer, us: i64, utc: bool, ms: bool) !void {
    const day = @divFloor(us, 86_400_000_000);
    const rest: u64 = @intCast(@mod(us, 86_400_000_000));
    const y = try fourDigitYear(day);
    const c = time.civilFromDays(day);
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}{c}{d:0>2}:{d:0>2}:{d:0>2}", .{ y, c.m, c.d, @as(u8, if (utc) 'T' else ' '), rest / 3_600_000_000, rest / 60_000_000 % 60, rest / 1_000_000 % 60 });
    if (ms) try w.print(".{d:0>3}", .{rest / 1000 % 1000}) else try w.print(".{d:0>6}", .{rest % 1_000_000});
    if (utc) try w.writeByte('Z');
}

pub fn writeDate(w: *std.Io.Writer, days: i64) !void {
    const y = try fourDigitYear(days);
    const c = time.civilFromDays(days);
    try w.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ y, c.m, c.d });
}

fn fourDigitYear(days: i64) Error!u64 {
    const y = time.civilFromDays(days).y;
    if (y < 1 or y > 9999) return Error.DeltaValueOutOfRange;
    return @intCast(y);
}

/// A float as Delta's text: plain digits for a magnitude from 1e-4 to below 1e16,
/// exponent form past them, NaN and the infinities as Java spells them; a 4-byte
/// `float` column's value as its shortest 4-byte text.
pub fn writeDouble(w: *std.Io.Writer, x: f64, float32: bool) !void {
    const v: f64 = if (float32) @as(f32, @floatCast(x)) else x;
    if (std.math.isNan(v)) return w.writeAll("NaN");
    if (std.math.isInf(v)) return w.writeAll(if (v > 0) "Infinity" else "-Infinity");
    const plain = v == 0 or (@abs(v) >= 1e-4 and @abs(v) < 1e16);
    if (float32) {
        const f: f32 = @floatCast(v);
        if (plain) try w.print("{d}", .{f}) else try w.print("{e}", .{f});
    } else if (plain) try w.print("{d}", .{v}) else try w.print("{e}", .{v});
}

/// A partition value as the log's text, null for NULL (and for an empty string,
/// which Delta reads as NULL); a decimal at the column's `scale`.
pub fn partitionText(arena: std.mem.Allocator, phys: pqwrite.Physical, scale: u8, v: Value) !?[]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    switch (v) {
        .null => return null,
        .string => |s| return if (s.len == 0) null else s,
        .int => |i| try w.print("{d}", .{i}),
        .float => |f| try writeDouble(w, f, phys.float32),
        .bool => |b| try w.writeAll(if (b) "true" else "false"),
        .date => |d| try writeDate(w, d),
        .timestamp => |us| try writeTimestamp(w, us, phys.utc, false),
        .decimal => |d| {
            const r = decimal.rescaleTo(d, scale) orelse return Error.DeltaValueOutOfRange;
            try decimal.writeDecimal(w, r.unscaled, r.scale);
        },
        else => return Error.DeltaValueOutOfRange,
    }
    return aw.written();
}

/// Which statistic of a file: its `minValues` or its `maxValues`.
pub const Bound = enum {
    min,
    max,

    pub fn key(self: Bound) []const u8 {
        return switch (self) {
            .min => "minValues",
            .max => "maxValues",
        };
    }
};

/// A statistics value as JSON, null where it is left out: a non-finite float, a
/// date or timestamp outside years 1..9999, a long string's maximum, and any
/// kind other than int, float, decimal, date, timestamp and string. A decimal is
/// written at its own scale, a timestamp to milliseconds.
pub fn statText(arena: std.mem.Allocator, phys: pqwrite.Physical, v: Value, which: Bound) !?[]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    const w = &aw.writer;
    switch (v) {
        .int => |i| try w.print("{d}", .{i}),
        .float => |f| {
            if (!std.math.isFinite(f)) return null;
            try writeDouble(w, f, phys.float32);
        },
        .decimal => |d| try decimal.writeDecimal(w, d.unscaled, d.scale),
        .date => |d| {
            try w.writeByte('"');
            writeDate(w, d) catch |e| return if (e == Error.DeltaValueOutOfRange) null else e;
            try w.writeByte('"');
        },
        .timestamp => |us| {
            try w.writeByte('"');
            writeTimestamp(w, us, phys.utc, true) catch |e| return if (e == Error.DeltaValueOutOfRange) null else e;
            try w.writeByte('"');
        },
        .string => |s| {
            if (s.len > stat_prefix) {
                if (which == .max) return null;
                var n: usize = stat_prefix;
                while (n > 0 and (s[n] & 0xC0) == 0x80) n -= 1;
                try jsonString(w, s[0..n]);
            } else try jsonString(w, s);
        },
        else => return null,
    }
    return aw.written();
}

/// A file's `stats` text as an object with numbers kept as their text, null when
/// it is not a JSON object.
pub fn parseStats(arena: std.mem.Allocator, text: []const u8) ?std.json.ObjectMap {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{ .parse_numbers = false }) catch return null;
    return if (v == .object) v.object else null;
}

fn numText(v: std.json.Value) ?[]const u8 {
    return switch (v) {
        .number_string, .string => |s| s,
        else => null,
    };
}

/// Whether a file's statistics say every value of `name` is NULL.
pub fn allNull(s: std.json.ObjectMap, name: []const u8) bool {
    const n = numText(s.get("numRecords") orelse return false) orelse return false;
    const nc = s.get("nullCount") orelse return false;
    if (nc != .object) return false;
    const c = numText(nc.object.get(name) orelse return false) orelse return false;
    const rows = std.fmt.parseInt(u64, n, 10) catch return false;
    return rows > 0 and (std.fmt.parseInt(u64, c, 10) catch return false) == rows;
}

/// A file's `minValues` or `maxValues` entry for `name` as a value of `ty`, null
/// when absent or when it may not bound the column: a string as long as a writer
/// may cut one to, any binary, a NaN. A timestamp is kept to milliseconds, so a
/// maximum is widened by the rest of its millisecond.
pub fn statValue(arena: std.mem.Allocator, s: std.json.ObjectMap, which: Bound, name: []const u8, ty: types.Type) !?Value {
    const m = s.get(which.key()) orelse return null;
    if (m != .object) return null;
    const text = numText(m.object.get(name) orelse return null) orelse return null;
    if (ty.kind == .string and which == .max and text.len >= stat_prefix) return null;
    if (ty.kind == .bytes) return null;
    const v = (try table.partitionValue(arena, text, ty)) orelse return null;
    if (v == .float and std.math.isNan(v.float)) return null;
    if (which == .max and v == .timestamp) return .{ .timestamp = v.timestamp + 999 };
    return v;
}

test "delta_types: every primitive name, read and write, with its layout" {
    const T = types.Type;
    const Case = struct { []const u8, types.TypeKind };
    for ([_]Case{
        .{ "string", .string }, .{ "long", .int },    .{ "integer", .int },         .{ "short", .int },
        .{ "byte", .int },      .{ "float", .float }, .{ "double", .float },        .{ "boolean", .bool },
        .{ "binary", .bytes },  .{ "date", .date },   .{ "timestamp", .timestamp }, .{ "timestamp_ntz", .timestamp },
    }) |c| {
        try testing.expectEqual(c[1], scalarType(c[0], .read).?.kind);
        try testing.expectEqual(c[1], scalarType(c[0], .write).?.kind);
        try testing.expect(!scalarType(c[0], .read).?.unknown);
    }
    try testing.expectEqual(types.TypeKind.string, scalarType("void", .read).?.kind);
    try testing.expect(scalarType("void", .write) == null);
    for ([_][]const u8{ "variant", "Long", "", "interval", "struct", "timestamp_tz" }) |t| {
        try testing.expect(scalarType(t, .read) == null);
        try testing.expect(scalarType(t, .write) == null);
    }
    try testing.expectEqual(@as(u8, 32), physical("integer").int_bits);
    try testing.expectEqual(@as(u8, 16), physical("short").int_bits);
    try testing.expectEqual(@as(u8, 8), physical("byte").int_bits);
    try testing.expectEqual(@as(u8, 64), physical("long").int_bits);
    try testing.expect(physical("float").float32);
    try testing.expect(!physical("double").float32);
    try testing.expect(physical("timestamp").utc);
    try testing.expect(!physical("timestamp_ntz").utc);
    try testing.expectEqual(pqwrite.Physical{}, physical("decimal(10,2)"));
    try testing.expectEqual(pqwrite.Physical{}, physical("variant"));
    try testing.expectEqual(T.decimal(12, 3), scalarType("decimal(12,3)", .write).?);
}

test "delta_types: decimal names, the verdicts the reader's and writer's parsers share" {
    const T = types.Type;
    try testing.expectEqual(T.decimal(10, 0), decimalType("decimal").?);
    try testing.expectEqual(T.decimal(10, 2), decimalType("decimal(10,2)").?);
    try testing.expectEqual(T.decimal(10, 2), decimalType("decimal( 10 , 2 )").?);
    try testing.expectEqual(T.decimal(38, 38), decimalType("decimal(38,38)").?);
    try testing.expectEqual(T.decimal(1, 0), decimalType("decimal(1,0)").?);
    try testing.expectEqual(T.decimal(5, 2), decimalType("decimal(5,2,9)").?);
    for ([_][]const u8{ "decimal(10)", "decimal(,2)", "decimal(39,0)", "decimal(5,6)", "decimal(0,0)", "decimal()", "decimal(10,2", "decimal(-1,0)", "decimal(a,b)", "decimal(256,0)", "Decimal(10,2)" }) |t|
        try testing.expect(decimalType(t) == null);
}

test "delta_types: a new table's column names and decimals" {
    const T = types.Type;
    try testing.expectEqualStrings("boolean", deltaName(.bool).?);
    try testing.expectEqualStrings("long", deltaName(.int).?);
    try testing.expectEqualStrings("double", deltaName(.float).?);
    try testing.expectEqualStrings("string", deltaName(.string).?);
    try testing.expectEqualStrings("binary", deltaName(.bytes).?);
    try testing.expectEqualStrings("date", deltaName(.date).?);
    try testing.expectEqualStrings("timestamp", deltaName(.timestamp).?);
    for ([_]types.TypeKind{ .decimal, .time, .array, .@"struct" }) |k| try testing.expect(deltaName(k) == null);
    inline for (std.meta.fields(types.TypeKind)) |f| {
        const k: types.TypeKind = @enumFromInt(f.value);
        if (deltaName(k)) |n| try testing.expectEqual(k, scalarType(n, .write).?.kind);
    }
    try testing.expectEqual(T.decimal(38, 4), createdDecimal(T.decimal(0, 4)).?);
    try testing.expectEqual(T.decimal(38, 2), createdDecimal(T.decimal(60, 2)).?);
    try testing.expectEqual(T.decimal(12, 3), createdDecimal(T.decimal(12, 3)).?);
    try testing.expect(createdDecimal(T.decimal(5, 6)) == null);
    try testing.expect(createdDecimal(T.decimal(0, 39)) == null);
}

fn parseJson(a: std.mem.Allocator, text: []const u8) !std.json.Value {
    return std.json.parseFromSliceLeaky(std.json.Value, a, text, .{});
}

test "delta_types: fields reads one level, nullable and metadata as the writer took them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const v = try parseJson(a,
        \\{"type":"struct","fields":[
        \\{"name":"id","type":"long","nullable":false,"metadata":{"k":"v"}},
        \\{"name":"s","type":{"type":"struct","fields":[{"name":"x","type":"integer"}]},"nullable":true,"metadata":{}},
        \\{"name":"n","type":"string","nullable":"no","metadata":[]},
        \\{"name":"t"}]}
    );
    inline for (.{ Lenience.strict, Lenience.reading, Lenience.lenient }) |l| {
        const fs = (try fields(a, v, l)).fields;
        try testing.expectEqual(@as(usize, 4), fs.len);
        try testing.expectEqualStrings("id", fs[0].name);
        try testing.expectEqualStrings("long", fs[0].ty.string);
        try testing.expect(!fs[0].nullable);
        try testing.expectEqualStrings("v", fs[0].metadata.?.get("k").?.string);
        try testing.expect(fs[1].ty == .object);
        try testing.expect(fs[1].nullable);
        try testing.expectEqual(@as(usize, 0), fs[1].metadata.?.count());
        try testing.expect(fs[2].nullable);
        try testing.expect(fs[2].metadata == null);
        try testing.expect(fs[3].ty == .null);
        try testing.expect(fs[3].nullable);
        try testing.expect(fs[3].metadata == null);
        const inner = (try fields(a, fs[1].ty, l)).fields;
        try testing.expectEqual(@as(usize, 1), inner.len);
        try testing.expectEqualStrings("x", inner[0].name);
    }
}

test "delta_types: fields pins each copy's lenience: writer refuses, reader skips non-objects, mapping skips all" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const not_obj = try parseJson(a, "{\"fields\":[1,{\"name\":\"a\",\"type\":\"long\"}]}");
    try testing.expectEqual(Problem.not_object, (try fields(a, not_obj, .strict)).problem);
    try testing.expectEqual(@as(usize, 1), (try fields(a, not_obj, .reading)).fields.len);
    try testing.expectEqual(@as(usize, 1), (try fields(a, not_obj, .lenient)).fields.len);

    for ([_][]const u8{ "{\"fields\":[{\"type\":\"long\"},{\"name\":\"a\"}]}", "{\"fields\":[{\"name\":7},{\"name\":\"a\"}]}" }) |text| {
        const nameless = try parseJson(a, text);
        try testing.expectEqual(Problem.nameless, (try fields(a, nameless, .strict)).problem);
        try testing.expectEqual(Problem.nameless, (try fields(a, nameless, .reading)).problem);
        const kept = (try fields(a, nameless, .lenient)).fields;
        try testing.expectEqual(@as(usize, 1), kept.len);
        try testing.expectEqualStrings("a", kept[0].name);
    }

    for ([_][]const u8{ "\"long\"", "[]", "{}", "{\"type\":\"struct\"}", "{\"fields\":{}}", "{\"type\":\"array\",\"elementType\":\"long\"}" }) |text| {
        const bad = try parseJson(a, text);
        inline for (.{ Lenience.strict, Lenience.reading, Lenience.lenient }) |l|
            try testing.expectEqual(Problem.not_struct, (try fields(a, bad, l)).problem);
    }
    inline for (.{ Lenience.strict, Lenience.reading, Lenience.lenient }) |l|
        try testing.expectEqual(@as(usize, 0), (try fields(a, try parseJson(a, "{\"fields\":[]}"), l)).fields.len);
}

test "delta_types: partition text and statistics as the writer spells them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ts = physical("timestamp");
    const ntz = physical("timestamp_ntz");
    const us: i64 = 20454 * 86_400_000_000 + 3_600_000_000 + 1_234_567;
    try testing.expectEqualStrings("2026-01-01T01:00:01.234567Z", (try partitionText(a, ts, 0, .{ .timestamp = us })).?);
    try testing.expectEqualStrings("2026-01-01 01:00:01.234567", (try partitionText(a, ntz, 0, .{ .timestamp = us })).?);
    try testing.expectEqualStrings("\"2026-01-01T01:00:01.234Z\"", (try statText(a, ts, .{ .timestamp = us }, .max)).?);
    try testing.expectEqualStrings("\"2026-01-01 01:00:01.234\"", (try statText(a, ntz, .{ .timestamp = us }, .min)).?);
    try testing.expectEqualStrings("10.50", (try partitionText(a, .{}, 2, .{ .decimal = .{ .unscaled = 105, .scale = 1 } })).?);
    try testing.expectEqualStrings("1.05", (try statText(a, .{}, .{ .decimal = .{ .unscaled = 105, .scale = 2 } }, .min)).?);
    try testing.expectEqualStrings("10.5", (try statText(a, .{}, .{ .decimal = .{ .unscaled = 105, .scale = 1 } }, .min)).?);
    try testing.expectEqualStrings("11", (try partitionText(a, .{}, 0, .{ .decimal = .{ .unscaled = 105, .scale = 1 } })).?);
    try testing.expectError(Error.DeltaValueOutOfRange, partitionText(a, .{}, 2, .{ .decimal = .{ .unscaled = std.math.maxInt(i128), .scale = 0 } }));
    try testing.expect(try partitionText(a, .{}, 0, .{ .string = "" }) == null);
    try testing.expect(try partitionText(a, .{}, 0, .null) == null);
    try testing.expectEqualStrings("eu", (try partitionText(a, .{}, 0, .{ .string = "eu" })).?);
    try testing.expectEqualStrings("-7", (try partitionText(a, .{}, 0, .{ .int = -7 })).?);
    try testing.expectEqualStrings("true", (try partitionText(a, .{}, 0, .{ .bool = true })).?);
    try testing.expectError(Error.DeltaValueOutOfRange, partitionText(a, .{}, 0, .{ .bytes = "x" }));
    try testing.expectEqualStrings("2026-01-01", (try partitionText(a, .{}, 0, .{ .date = 20454 })).?);
    try testing.expectError(Error.DeltaValueOutOfRange, partitionText(a, .{}, 0, .{ .date = -719163 }));
    try testing.expectError(Error.DeltaValueOutOfRange, partitionText(a, .{}, 0, .{ .date = 2932897 }));
    try testing.expectEqualStrings("0001-01-01", (try partitionText(a, .{}, 0, .{ .date = -719162 })).?);
    try testing.expectEqualStrings("9999-12-31", (try partitionText(a, .{}, 0, .{ .date = 2932896 })).?);
    try testing.expectError(Error.DeltaValueOutOfRange, partitionText(a, ts, 0, .{ .timestamp = std.math.minInt(i64) }));
    try testing.expect(try statText(a, ts, .{ .timestamp = std.math.maxInt(i64) }, .max) == null);
    try testing.expect(try statText(a, .{}, .{ .date = -719163 }, .min) == null);
    try testing.expectEqualStrings("\"0001-01-01\"", (try statText(a, .{}, .{ .date = -719162 }, .min)).?);
    try testing.expectEqualStrings("\"1969-12-31 23:59:59.999\"", (try statText(a, ntz, .{ .timestamp = -1 }, .min)).?);
    try testing.expectEqualStrings("-42", (try statText(a, .{}, .{ .int = -42 }, .max)).?);
    try testing.expect(try statText(a, .{}, .{ .bool = true }, .max) == null);
    try testing.expect(try statText(a, .{}, .{ .bytes = "ab" }, .min) == null);
    try testing.expect(try statText(a, .{}, .null, .min) == null);
    try testing.expectEqualStrings("\"a\\\"b\"", (try statText(a, .{}, .{ .string = "a\"b" }, .max)).?);
}

test "delta_types: floats as partition values and statistics, read back exactly" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const d = physical("double");
    const f = physical("float");
    const Case = struct { f64, []const u8 };
    for ([_]Case{ .{ 1e301, "1e301" }, .{ -2.5e-300, "-2.5e-300" }, .{ 1e-7, "1e-7" }, .{ 0.0001, "0.0001" }, .{ 123.5, "123.5" }, .{ 1e16, "1e16" }, .{ 0, "0" }, .{ std.math.nan(f64), "NaN" }, .{ -std.math.inf(f64), "-Infinity" } }) |c| {
        const text = (try partitionText(a, d, 0, .{ .float = c[0] })).?;
        try testing.expectEqualStrings(c[1], text);
        const back = (try table.partitionValue(a, text, types.Type.init(.float))).?.float;
        if (std.math.isNan(c[0])) try testing.expect(std.math.isNan(back)) else try testing.expectEqual(c[0], back);
    }
    try testing.expectEqualStrings("Infinity", (try partitionText(a, f, 0, .{ .float = 1e300 })).?);
    try testing.expectEqualStrings("0.1", (try partitionText(a, f, 0, .{ .float = 0.1 })).?);
    try testing.expectEqualStrings("1e-10", (try statText(a, f, .{ .float = 1e-10 }, .min)).?);
    try testing.expectEqualStrings("1e300", (try statText(a, d, .{ .float = 1e300 }, .max)).?);
    try testing.expect(try statText(a, d, .{ .float = std.math.inf(f64) }, .max) == null);
    try testing.expect(try statText(a, d, .{ .float = std.math.nan(f64) }, .min) == null);
    try testing.expectEqualStrings("Infinity", (try statText(a, f, .{ .float = 1e300 }, .max)).?);
}

test "delta_types: string statistics cut at stat_prefix, on a character boundary" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const long = "é" ** 20;
    try testing.expect(try statText(a, .{}, .{ .string = long }, .max) == null);
    try testing.expectEqualStrings("\"" ++ "é" ** 16 ++ "\"", (try statText(a, .{}, .{ .string = long }, .min)).?);
    const odd = "a" ++ "é" ** 20;
    try testing.expectEqualStrings("\"a" ++ "é" ** 15 ++ "\"", (try statText(a, .{}, .{ .string = odd }, .min)).?);
    const exact = "a" ** stat_prefix;
    try testing.expectEqualStrings("\"" ++ exact ++ "\"", (try statText(a, .{}, .{ .string = exact }, .max)).?);
    try testing.expectEqualStrings("\"" ++ exact ++ "\"", (try statText(a, .{}, .{ .string = exact ++ "b" }, .min)).?);
    try testing.expect(try statText(a, .{}, .{ .string = exact ++ "b" }, .max) == null);
}

test "delta_types: statValue reads what the merge reads" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const T = types.Type;
    const s = parseStats(a, "{\"numRecords\":3,\"minValues\":{\"id\":15,\"f\":0.5,\"d\":\"2026-01-01\",\"x\":1.25,\"b\":\"ab\",\"n\":null},\"maxValues\":{\"id\":30,\"t\":\"1970-01-01T00:00:01.000Z\",\"f\":\"NaN\",\"g\":\"Infinity\",\"bad\":\"zz\"},\"nullCount\":{\"id\":0}}").?;
    try testing.expectEqual(@as(i64, 15), (try statValue(a, s, .min, "id", T.init(.int))).?.int);
    try testing.expectEqual(@as(i64, 30), (try statValue(a, s, .max, "id", T.init(.int))).?.int);
    try testing.expectEqual(@as(f64, 0.5), (try statValue(a, s, .min, "f", T.init(.float))).?.float);
    try testing.expectEqual(@as(i32, 20454), (try statValue(a, s, .min, "d", T.init(.date))).?.date);
    try testing.expectEqual(@as(i128, 125), (try statValue(a, s, .min, "x", T.decimal(10, 2))).?.decimal.unscaled);
    try testing.expectEqual(@as(i64, 1_000_999), (try statValue(a, s, .max, "t", T.init(.timestamp))).?.timestamp);
    try testing.expect(try statValue(a, s, .max, "f", T.init(.float)) == null);
    try testing.expectEqual(std.math.inf(f64), (try statValue(a, s, .max, "g", T.init(.float))).?.float);
    try testing.expect(try statValue(a, s, .min, "b", T.init(.bytes)) == null);
    try testing.expect(try statValue(a, s, .min, "n", T.init(.int)) == null);
    try testing.expect(try statValue(a, s, .max, "bad", T.init(.int)) == null);
    try testing.expect(try statValue(a, s, .min, "missing", T.init(.int)) == null);
    try testing.expect(try statValue(a, s, .min, "t", T.init(.timestamp)) == null);

    const strs = parseStats(a, "{\"minValues\":{\"s\":\"" ++ "a" ** 32 ++ "\"},\"maxValues\":{\"s\":\"" ++ "a" ** 32 ++ "\",\"u\":\"" ++ "a" ** 31 ++ "\"}}").?;
    try testing.expect(try statValue(a, strs, .max, "s", T.init(.string)) == null);
    try testing.expectEqualStrings("a" ** 32, (try statValue(a, strs, .min, "s", T.init(.string))).?.string);
    try testing.expectEqualStrings("a" ** 31, (try statValue(a, strs, .max, "u", T.init(.string))).?.string);

    try testing.expect(parseStats(a, "not json") == null);
    try testing.expect(parseStats(a, "[1]") == null);
    const no_obj = parseStats(a, "{\"minValues\":3}").?;
    try testing.expect(try statValue(a, no_obj, .min, "id", T.init(.int)) == null);
    try testing.expectEqualStrings("minValues", Bound.min.key());
    try testing.expectEqualStrings("maxValues", Bound.max.key());
}

test "delta_types: allNull only when nullCount equals a positive numRecords" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expect(allNull(parseStats(a, "{\"numRecords\":3,\"nullCount\":{\"id\":3}}").?, "id"));
    try testing.expect(allNull(parseStats(a, "{\"numRecords\":\"3\",\"nullCount\":{\"id\":\"3\"}}").?, "id"));
    try testing.expect(!allNull(parseStats(a, "{\"numRecords\":3,\"nullCount\":{\"id\":2}}").?, "id"));
    try testing.expect(!allNull(parseStats(a, "{\"numRecords\":0,\"nullCount\":{\"id\":0}}").?, "id"));
    try testing.expect(!allNull(parseStats(a, "{\"numRecords\":3,\"nullCount\":{\"x\":3}}").?, "id"));
    try testing.expect(!allNull(parseStats(a, "{\"nullCount\":{\"id\":3}}").?, "id"));
    try testing.expect(!allNull(parseStats(a, "{\"numRecords\":3,\"nullCount\":[3]}").?, "id"));
    try testing.expect(!allNull(parseStats(a, "{\"numRecords\":-1,\"nullCount\":{\"id\":-1}}").?, "id"));
}

test "delta_types: statistics the writer spells read back as the merge reads them" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const T = types.Type;
    const Case = struct { pqwrite.Physical, T, Value };
    const us: i64 = 20454 * 86_400_000_000 + 1_234_567;
    for ([_]Case{
        .{ .{}, T.init(.int), .{ .int = -9_007_199_254_740_993 } },
        .{ .{}, T.init(.int), .{ .int = std.math.maxInt(i64) } },
        .{ physical("double"), T.init(.float), .{ .float = 1e-300 } },
        .{ physical("double"), T.init(.float), .{ .float = -123.456 } },
        .{ .{}, T.init(.date), .{ .date = -719162 } },
        .{ .{}, T.init(.date), .{ .date = 2932896 } },
        .{ physical("timestamp"), T.init(.timestamp), .{ .timestamp = us } },
        .{ physical("timestamp_ntz"), T.init(.timestamp), .{ .timestamp = -us } },
        .{ .{}, T.init(.string), .{ .string = "café" } },
        .{ .{}, T.decimal(10, 2), .{ .decimal = .{ .unscaled = -105, .scale = 2 } } },
    }) |c| {
        for ([_]Bound{ .min, .max }) |which| {
            const text = (try statText(a, c[0], c[2], which)).?;
            const json = try std.fmt.allocPrint(a, "{{\"{s}\":{{\"c\":{s}}}}}", .{ which.key(), text });
            const back = (try statValue(a, parseStats(a, json).?, which, "c", c[1])).?;
            switch (c[2]) {
                .timestamp => |t| if (which == .max) try testing.expect(back.timestamp >= t) else try testing.expect(back.timestamp <= t),
                .int => |i| try testing.expectEqual(i, back.int),
                .float => |x| try testing.expectEqual(x, back.float),
                .date => |x| try testing.expectEqual(x, back.date),
                .string => |x| try testing.expectEqualStrings(x, back.string),
                .decimal => |x| try testing.expectEqual(x.unscaled, back.decimal.unscaled),
                else => unreachable,
            }
        }
    }
    const t_max = (try statValue(a, parseStats(a, try std.fmt.allocPrint(a, "{{\"maxValues\":{{\"c\":{s}}}}}", .{(try statText(a, physical("timestamp"), .{ .timestamp = us }, .max)).?})).?, .max, "c", T.init(.timestamp))).?;
    try testing.expectEqual(@as(i64, 20454 * 86_400_000_000 + 1_234_999), t_max.timestamp);
}
