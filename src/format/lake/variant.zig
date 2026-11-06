//! Delta's `variant` type in the Variant binary encoding (Apache Parquet's
//! VariantEncoding): a value is its `metadata`, a dictionary of the object keys it
//! uses, and its `value`, a tree of typed parts. basalt holds a variant as its
//! JSON text: `decode` renders a value so, `encode` builds one from JSON text.
//!
//! A value's first byte holds its basic type in the low two bits and a header in
//! the other six: a primitive (the header its type id), a short string (the
//! header its length, up to 63), an object (field ids, offsets and values, its
//! fields in the order of their keys) or an array (offsets and values). Decoding
//! renders every primitive the format defines: integers and floats as numbers, a
//! decimal as its exact digits, dates, timestamps, times and UUIDs as quoted
//! text, binary as quoted base64, a NaN or infinite float as `null`, each as
//! basalt's JSON spells its own values. Encoding takes
//! JSON as `std.json` parses it: an integer in the narrowest width that holds it,
//! a number with a fraction or exponent as a decimal when its digits fit 38 places
//! and its exponent none, else a double; strings short where they fit; keys
//! deduplicated into one sorted dictionary. A value whose bytes run short, name a
//! key past the dictionary or a type the format lacks fails with `VariantCorrupt`.

const std = @import("std");
const jsontext = @import("../../exec/jsontext.zig");
const Value = @import("../../exec/value.zig").Value;
const testing = std.testing;

pub const Error = error{ VariantCorrupt, OutOfMemory };

/// The JSON text of variant `value` under dictionary `metadata`.
pub fn decode(arena: std.mem.Allocator, value: []const u8, metadata: []const u8) Error![]const u8 {
    const dict = try Dict.parse(metadata);
    var aw = std.Io.Writer.Allocating.init(arena);
    _ = render(&aw.writer, dict, value, 0) catch |e| return switch (e) {
        error.WriteFailed, error.OutOfMemory => error.OutOfMemory,
        error.VariantCorrupt => error.VariantCorrupt,
    };
    return aw.written();
}

const Dict = struct {
    offset_size: usize,
    size: usize,
    offsets: []const u8,
    bytes: []const u8,

    fn parse(m: []const u8) Error!Dict {
        if (m.len < 1) return error.VariantCorrupt;
        if (m[0] & 0x0f != 1) return error.VariantCorrupt;
        const os: usize = (m[0] >> 6) + 1;
        if (m.len < 1 + os) return error.VariantCorrupt;
        const size = readUint(m[1..][0..os]);
        const offsets_len = std.math.mul(usize, size + 1, os) catch return error.VariantCorrupt;
        if (m.len < 1 + os + offsets_len) return error.VariantCorrupt;
        const offsets = m[1 + os ..][0..offsets_len];
        const bytes = m[1 + os + offsets_len ..];
        return .{ .offset_size = os, .size = size, .offsets = offsets, .bytes = bytes };
    }

    fn key(self: Dict, id: usize) Error![]const u8 {
        if (id >= self.size) return error.VariantCorrupt;
        const lo = readUint(self.offsets[id * self.offset_size ..][0..self.offset_size]);
        const hi = readUint(self.offsets[(id + 1) * self.offset_size ..][0..self.offset_size]);
        if (lo > hi or hi > self.bytes.len) return error.VariantCorrupt;
        return self.bytes[lo..hi];
    }
};

fn readUint(b: []const u8) usize {
    var x: usize = 0;
    for (b, 0..) |byte, i| x |= @as(usize, byte) << @intCast(8 * i);
    return x;
}

fn take(v: []const u8, at: usize, n: usize) Error![]const u8 {
    if (at > v.len or n > v.len - at) return error.VariantCorrupt;
    return v[at..][0..n];
}

const max_depth = 64;

/// Renders the value at the start of `v`; returns how many bytes it took.
fn render(w: *std.Io.Writer, d: Dict, v: []const u8, depth: usize) (Error || std.Io.Writer.Error)!usize {
    if (depth > max_depth) return error.VariantCorrupt;
    if (v.len == 0) return error.VariantCorrupt;
    const basic = v[0] & 0x03;
    const header = v[0] >> 2;
    switch (basic) {
        0 => return primitive(w, v, header),
        1 => {
            const s = try take(v, 1, header);
            try jsontext.string(w, s);
            return 1 + header;
        },
        2 => {
            const offset_size: usize = (header & 0x03) + 1;
            const id_size: usize = ((header >> 2) & 0x03) + 1;
            const large = (header >> 4) & 1 == 1;
            const count_size: usize = if (large) 4 else 1;
            const n = readUint(try take(v, 1, count_size));
            const ids_at = 1 + count_size;
            const ids = try take(v, ids_at, std.math.mul(usize, n, id_size) catch return error.VariantCorrupt);
            const offs_at = ids_at + ids.len;
            const offs = try take(v, offs_at, std.math.mul(usize, n + 1, offset_size) catch return error.VariantCorrupt);
            const data_at = offs_at + offs.len;
            try w.writeByte('{');
            for (0..n) |i| {
                if (i > 0) try w.writeByte(',');
                try jsontext.string(w, try d.key(readUint(ids[i * id_size ..][0..id_size])));
                try w.writeByte(':');
                const off = readUint(offs[i * offset_size ..][0..offset_size]);
                if (data_at + off > v.len) return error.VariantCorrupt;
                _ = try render(w, d, v[data_at + off ..], depth + 1);
            }
            try w.writeByte('}');
            return data_at + readUint(offs[n * offset_size ..][0..offset_size]);
        },
        else => {
            const offset_size: usize = (header & 0x03) + 1;
            const large = (header >> 2) & 1 == 1;
            const count_size: usize = if (large) 4 else 1;
            const n = readUint(try take(v, 1, count_size));
            const offs_at = 1 + count_size;
            const offs = try take(v, offs_at, std.math.mul(usize, n + 1, offset_size) catch return error.VariantCorrupt);
            const data_at = offs_at + offs.len;
            try w.writeByte('[');
            for (0..n) |i| {
                if (i > 0) try w.writeByte(',');
                const off = readUint(offs[i * offset_size ..][0..offset_size]);
                if (data_at + off > v.len) return error.VariantCorrupt;
                _ = try render(w, d, v[data_at + off ..], depth + 1);
            }
            try w.writeByte(']');
            return data_at + readUint(offs[n * offset_size ..][0..offset_size]);
        },
    }
}

fn int(comptime T: type, b: []const u8) T {
    return std.mem.readInt(T, b[0..@sizeOf(T)], .little);
}

fn primitive(w: *std.Io.Writer, v: []const u8, id: u8) (Error || std.Io.Writer.Error)!usize {
    if (id == 20) {
        const b = try take(v, 1, 16);
        try w.print("\"{x}-{x}-{x}-{x}-{x}\"", .{ b[0..4], b[4..6], b[6..8], b[8..10], b[10..16] });
        return 17;
    }
    const p = try primitiveValue(v, id);
    try jsontext.scalar(w, p.value, jsontext.nested);
    return p.len;
}

const Primitive = struct { value: Value, len: usize };

/// The primitive of type `id` at the start of `v` as basalt's value, and how many
/// bytes it takes (a UUID, which basalt has no value for, aside); a nanosecond
/// timestamp keeps its microseconds.
fn primitiveValue(v: []const u8, id: u8) Error!Primitive {
    return switch (id) {
        0 => .{ .value = .null, .len = 1 },
        1, 2 => .{ .value = .{ .bool = id == 1 }, .len = 1 },
        3 => .{ .value = .{ .int = int(i8, try take(v, 1, 1)) }, .len = 2 },
        4 => .{ .value = .{ .int = int(i16, try take(v, 1, 2)) }, .len = 3 },
        5 => .{ .value = .{ .int = int(i32, try take(v, 1, 4)) }, .len = 5 },
        6 => .{ .value = .{ .int = int(i64, try take(v, 1, 8)) }, .len = 9 },
        7 => .{ .value = .{ .float = @bitCast(int(u64, try take(v, 1, 8))) }, .len = 9 },
        14 => .{ .value = .{ .float = @as(f32, @bitCast(int(u32, try take(v, 1, 4)))) }, .len = 5 },
        8 => .{ .value = .{ .decimal = .{ .unscaled = int(i32, (try take(v, 1, 5))[1..]), .scale = v[1] } }, .len = 6 },
        9 => .{ .value = .{ .decimal = .{ .unscaled = int(i64, (try take(v, 1, 9))[1..]), .scale = v[1] } }, .len = 10 },
        10 => .{ .value = .{ .decimal = .{ .unscaled = int(i128, (try take(v, 1, 17))[1..]), .scale = v[1] } }, .len = 18 },
        11 => .{ .value = .{ .date = int(i32, try take(v, 1, 4)) }, .len = 5 },
        12, 13 => .{ .value = .{ .timestamp = int(i64, try take(v, 1, 8)) }, .len = 9 },
        18, 19 => .{ .value = .{ .timestamp = @divFloor(int(i64, try take(v, 1, 8)), 1000) }, .len = 9 },
        17 => .{ .value = .{ .time = int(i64, try take(v, 1, 8)) }, .len = 9 },
        15, 16 => blk: {
            const n: usize = int(u32, try take(v, 1, 4));
            const b = try take(v, 5, n);
            break :blk .{ .value = if (id == 16) .{ .string = b } else .{ .bytes = b }, .len = 5 + n };
        },
        else => error.VariantCorrupt,
    };
}

pub const Encoded = struct { value: []const u8, metadata: []const u8 };

/// JSON text `text` as a variant; null for text that is not JSON.
pub fn encode(arena: std.mem.Allocator, text: []const u8) Error!?Encoded {
    const j = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{ .parse_numbers = false }) catch |e| return if (e == error.OutOfMemory) error.OutOfMemory else null;
    var keys = std.StringArrayHashMapUnmanaged(void).empty;
    try collectKeys(arena, j, &keys, 0);
    const sorted = try arena.dupe([]const u8, keys.keys());
    std.mem.sort([]const u8, sorted, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lt);
    var ids = std.StringHashMapUnmanaged(u32).empty;
    for (sorted, 0..) |k, i| try ids.put(arena, k, @intCast(i));
    var out = std.ArrayList(u8).empty;
    try put(arena, &out, j, &ids, 0);
    return .{ .value = out.items, .metadata = try metadataOf(arena, sorted) };
}

fn collectKeys(arena: std.mem.Allocator, j: std.json.Value, keys: *std.StringArrayHashMapUnmanaged(void), depth: usize) Error!void {
    if (depth > max_depth) return error.VariantCorrupt;
    switch (j) {
        .object => |o| {
            var it = o.iterator();
            while (it.next()) |kv| {
                try keys.put(arena, kv.key_ptr.*, {});
                try collectKeys(arena, kv.value_ptr.*, keys, depth + 1);
            }
        },
        .array => |xs| for (xs.items) |x| try collectKeys(arena, x, keys, depth + 1),
        else => {},
    }
}

fn metadataOf(arena: std.mem.Allocator, keys: []const []const u8) Error![]const u8 {
    var total: usize = 0;
    for (keys) |k| total += k.len;
    const os = widthFor(@max(total, keys.len));
    var m = std.ArrayList(u8).empty;
    try m.append(arena, 0x01 | 0x10 | @as(u8, @intCast((os - 1) << 6)));
    try putUint(arena, &m, keys.len, os);
    var at: usize = 0;
    try putUint(arena, &m, 0, os);
    for (keys) |k| {
        at += k.len;
        try putUint(arena, &m, at, os);
    }
    for (keys) |k| try m.appendSlice(arena, k);
    return m.items;
}

fn widthFor(n: usize) usize {
    if (n <= 0xff) return 1;
    if (n <= 0xffff) return 2;
    if (n <= 0xffffff) return 3;
    return 4;
}

fn putUint(arena: std.mem.Allocator, out: *std.ArrayList(u8), x: usize, width: usize) Error!void {
    for (0..width) |i| try out.append(arena, @truncate(x >> @intCast(8 * i)));
}

fn put(arena: std.mem.Allocator, out: *std.ArrayList(u8), j: std.json.Value, ids: *const std.StringHashMapUnmanaged(u32), depth: usize) Error!void {
    if (depth > max_depth) return error.VariantCorrupt;
    switch (j) {
        .null => try out.append(arena, 0),
        .bool => |b| try out.append(arena, if (b) 1 << 2 else 2 << 2),
        .integer => |x| try putInt(arena, out, x),
        .float => |x| try putDouble(arena, out, x),
        .number_string => |t| try putNumber(arena, out, t),
        .string => |s| try putString(arena, out, s),
        .array => |xs| {
            var parts = std.ArrayList(u8).empty;
            const offs = try arena.alloc(usize, xs.items.len + 1);
            for (xs.items, 0..) |x, i| {
                offs[i] = parts.items.len;
                try put(arena, &parts, x, ids, depth + 1);
            }
            offs[xs.items.len] = parts.items.len;
            const os = widthFor(parts.items.len);
            const large = xs.items.len > 0xff;
            try out.append(arena, 3 | @as(u8, @intCast((os - 1) | (@as(usize, @intFromBool(large)) << 2))) << 2);
            try putUint(arena, out, xs.items.len, if (large) 4 else 1);
            for (offs) |o| try putUint(arena, out, o, os);
            try out.appendSlice(arena, parts.items);
        },
        .object => |o| {
            const names = try arena.dupe([]const u8, o.keys());
            std.mem.sort([]const u8, names, {}, struct {
                fn lt(_: void, a: []const u8, b: []const u8) bool {
                    return std.mem.order(u8, a, b) == .lt;
                }
            }.lt);
            var parts = std.ArrayList(u8).empty;
            const offs = try arena.alloc(usize, names.len + 1);
            var max_id: usize = 0;
            for (names, 0..) |nm, i| {
                offs[i] = parts.items.len;
                max_id = @max(max_id, ids.get(nm).?);
                try put(arena, &parts, o.get(nm).?, ids, depth + 1);
            }
            offs[names.len] = parts.items.len;
            const os = widthFor(parts.items.len);
            const is = widthFor(max_id);
            const large = names.len > 0xff;
            try out.append(arena, 2 | @as(u8, @intCast((os - 1) | ((is - 1) << 2) | (@as(usize, @intFromBool(large)) << 4))) << 2);
            try putUint(arena, out, names.len, if (large) 4 else 1);
            for (names) |nm| try putUint(arena, out, ids.get(nm).?, is);
            for (offs) |off| try putUint(arena, out, off, os);
            try out.appendSlice(arena, parts.items);
        },
    }
}

fn putInt(arena: std.mem.Allocator, out: *std.ArrayList(u8), x: i64) Error!void {
    if (std.math.cast(i8, x)) |y| {
        try out.append(arena, 3 << 2);
        try out.append(arena, @bitCast(y));
    } else if (std.math.cast(i16, x)) |y| {
        try out.append(arena, 4 << 2);
        try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(i16, y)));
    } else if (std.math.cast(i32, x)) |y| {
        try out.append(arena, 5 << 2);
        try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(i32, y)));
    } else {
        try out.append(arena, 6 << 2);
        try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(i64, x)));
    }
}

fn putDouble(arena: std.mem.Allocator, out: *std.ArrayList(u8), x: f64) Error!void {
    try out.append(arena, 7 << 2);
    try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(u64, @bitCast(x))));
}

/// A JSON number's text: an integer as `putInt`, one with a fraction (no
/// exponent) as a decimal of its exact digits when they fit 38, else a double.
fn putNumber(arena: std.mem.Allocator, out: *std.ArrayList(u8), t: []const u8) Error!void {
    if (std.fmt.parseInt(i64, t, 10)) |x| return putInt(arena, out, x) else |_| {}
    if (std.mem.indexOfAny(u8, t, "eE") == null) if (std.mem.indexOfScalar(u8, t, '.')) |dot| {
        const scale = t.len - dot - 1;
        var digits = std.ArrayList(u8).empty;
        try digits.appendSlice(arena, t[0..dot]);
        try digits.appendSlice(arena, t[dot + 1 ..]);
        if (scale <= 38) if (std.fmt.parseInt(i128, digits.items, 10)) |u| {
            const mag: u128 = @abs(u);
            if (mag < std.math.pow(u128, 10, 38)) {
                const width: u8 = if (mag < 1_000_000_000) 4 else if (mag < 1_000_000_000_000_000_000) 8 else 16;
                try out.append(arena, @as(u8, switch (width) {
                    4 => 8,
                    8 => 9,
                    else => 10,
                }) << 2);
                try out.append(arena, @intCast(scale));
                switch (width) {
                    4 => try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(i32, @intCast(u)))),
                    8 => try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(i64, @intCast(u)))),
                    else => try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(i128, u))),
                }
                return;
            }
        } else |_| {};
    };
    const f = std.fmt.parseFloat(f64, t) catch return error.VariantCorrupt;
    return putDouble(arena, out, f);
}

fn putString(arena: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8) Error!void {
    if (s.len <= 63) {
        try out.append(arena, 1 | @as(u8, @intCast(s.len)) << 2);
        return out.appendSlice(arena, s);
    }
    try out.append(arena, 16 << 2);
    try out.appendSlice(arena, std.mem.asBytes(&std.mem.nativeToLittle(u32, @intCast(s.len))));
    try out.appendSlice(arena, s);
}

test "a variant built from JSON text renders back as the same JSON" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_][]const u8{
        "null",
        "true",
        "[1,-200,70000,5000000000,2.5,-0.001,12345678901234567890.123]",
        "{\"b\":{\"z\":[],\"a\":\"x\"},\"a\":null,\"long\":\"" ++ "x" ** 70 ++ "\"}",
        "[{\"k\":1},{\"k\":2,\"j\":[true,false]}]",
        "1.5e3",
    }) |t| {
        const e = (try encode(a, t)).?;
        const back = try decode(a, e.value, e.metadata);
        const want = if (std.mem.eql(u8, t, "1.5e3")) "1500" else if (std.mem.startsWith(u8, t, "[{")) "[{\"k\":1},{\"j\":[true,false],\"k\":2}]" else if (t[0] == '{') "{\"a\":null,\"b\":{\"a\":\"x\",\"z\":[]},\"long\":\"" ++ "x" ** 70 ++ "\"}" else t;
        try testing.expectEqualStrings(want, back);
    }
    try testing.expect((try encode(a, "{nope")) == null);
}

test "a variant's primitives render as JSON: dates and timestamps quoted, binary as base64, a NaN as null" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const meta = [_]u8{ 0x01, 0x00, 0x00 };
    const date = [_]u8{ 11 << 2, 0xe6, 0x4f, 0x00, 0x00 };
    try testing.expectEqualStrings("\"2026-01-01\"", try decode(a, &date, &meta));
    var ts: [9]u8 = undefined;
    ts[0] = 12 << 2;
    std.mem.writeInt(i64, ts[1..9], 1_767_225_600_000_000 + 1_500_000, .little);
    try testing.expectEqualStrings("\"2026-01-01 00:00:01.500000\"", try decode(a, &ts, &meta));
    const bin = [_]u8{ 15 << 2, 2, 0, 0, 0, 'h', 'i' };
    try testing.expectEqualStrings("\"aGk=\"", try decode(a, &bin, &meta));
    var nan: [9]u8 = undefined;
    nan[0] = 7 << 2;
    std.mem.writeInt(u64, nan[1..9], @bitCast(std.math.nan(f64)), .little);
    try testing.expectEqualStrings("null", try decode(a, &nan, &meta));
    try testing.expectError(error.VariantCorrupt, decode(a, &[_]u8{ 0x02, 1, 5, 0, 0 }, &meta));
}
