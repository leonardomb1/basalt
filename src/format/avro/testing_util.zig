//! Test helpers shared by the Avro reader's and writer's tests: random schemas and
//! the datums they encode, built into container files under any codec, and a
//! mutator that corrupts only a file's blocks so a fuzz run gets past its header.

const std = @import("std");
const schema = @import("schema.zig");
const read = @import("read.zig");
const write = @import("write.zig");
const varint = @import("../varint.zig");
const pow10_i128 = @import("../../exec/value.zig").pow10_i128;

const Node = schema.Node;

/// Random schemas and the datums they encode, from one seeded generator.
pub const Gen = struct {
    r: std.Random,
    a: std.mem.Allocator,
    names: usize = 0,

    fn name(g: *Gen) ![]const u8 {
        g.names += 1;
        return std.fmt.allocPrint(g.a, "n{d}", .{g.names});
    }

    /// A top-level schema's JSON: mostly a record of one to six fields, sometimes a
    /// bare type.
    pub fn top(g: *Gen) ![]const u8 {
        if (g.r.uintLessThan(u8, 8) == 0) return g.ty(1);
        return g.record(0, null);
    }

    fn record(g: *Gen, depth: usize, self_ref: ?[]const u8) ![]const u8 {
        const n = try g.name();
        var w = std.Io.Writer.Allocating.init(g.a);
        try w.writer.print("{{\"type\":\"record\",\"name\":\"{s}\",\"fields\":[", .{n});
        const k = 1 + g.r.uintLessThan(usize, 6);
        for (0..k) |i| {
            if (i > 0) try w.writer.writeByte(',');
            const t = if (self_ref == null and depth < 2 and g.r.uintLessThan(u8, 12) == 0)
                try std.fmt.allocPrint(g.a, "[\"null\",\"{s}\"]", .{n})
            else
                try g.ty(depth + 1);
            try w.writer.print("{{\"name\":\"f{d}\",\"type\":{s}}}", .{ i, t });
        }
        try w.writer.writeAll("]}");
        return w.written();
    }

    const prims = [_][]const u8{ "\"boolean\"", "\"int\"", "\"long\"", "\"float\"", "\"double\"", "\"bytes\"", "\"string\"" };
    const logicals = [_][]const u8{
        "{\"type\":\"int\",\"logicalType\":\"date\"}",
        "{\"type\":\"int\",\"logicalType\":\"time-millis\"}",
        "{\"type\":\"long\",\"logicalType\":\"time-micros\"}",
        "{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}",
        "{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}",
        "{\"type\":\"long\",\"logicalType\":\"timestamp-nanos\"}",
        "{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}",
        "{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}",
        "{\"type\":\"string\",\"logicalType\":\"uuid\"}",
        "{\"type\":\"string\",\"logicalType\":\"no-such-type\"}",
    };

    fn ty(g: *Gen, depth: usize) anyerror![]const u8 {
        const deep = depth >= 4;
        return switch (g.r.uintLessThan(u8, if (deep) 4 else 10)) {
            0, 1 => prims[g.r.uintLessThan(usize, prims.len)],
            2 => logicals[g.r.uintLessThan(usize, logicals.len)],
            3 => g.decimal(),
            4 => std.fmt.allocPrint(g.a, "{{\"type\":\"enum\",\"name\":\"{s}\",\"symbols\":[\"A\",\"B\",\"C\"]}}", .{try g.name()}),
            5 => std.fmt.allocPrint(g.a, "{{\"type\":\"fixed\",\"name\":\"{s}\",\"size\":{d}}}", .{ try g.name(), g.r.uintLessThan(usize, 9) }),
            6 => g.record(depth, null),
            7 => std.fmt.allocPrint(g.a, "{{\"type\":\"array\",\"items\":{s}}}", .{try g.ty(depth + 1)}),
            8 => std.fmt.allocPrint(g.a, "{{\"type\":\"map\",\"values\":{s}}}", .{try g.ty(depth + 1)}),
            else => g.unionOf(depth),
        };
    }

    fn decimal(g: *Gen) ![]const u8 {
        if (g.r.boolean()) {
            const p = 1 + g.r.uintLessThan(u8, 38);
            return std.fmt.allocPrint(g.a, "{{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":{d},\"scale\":{d}}}", .{ p, g.r.uintAtMost(u8, p) });
        }
        const size = 1 + g.r.uintLessThan(usize, 16);
        const most: u8 = @intCast(@min(38, @as(usize, @intFromFloat(@floor(@as(f64, @floatFromInt(8 * size - 1)) * std.math.log10(2.0))))));
        if (most == 0) return "\"bytes\"";
        const p = 1 + g.r.uintLessThan(u8, most);
        return std.fmt.allocPrint(g.a, "{{\"type\":\"fixed\",\"name\":\"{s}\",\"size\":{d},\"logicalType\":\"decimal\",\"precision\":{d},\"scale\":{d}}}", .{ try g.name(), size, p, g.r.uintAtMost(u8, p) });
    }

    /// A union of distinct branches: null, primitives, and at most one array, map
    /// and record each, as the spec allows.
    fn unionOf(g: *Gen, depth: usize) ![]const u8 {
        const pool = [_][]const u8{ "\"null\"", "\"boolean\"", "\"int\"", "\"long\"", "\"float\"", "\"double\"", "\"bytes\"", "\"string\"", "array", "map", "record" };
        var used = [_]bool{false} ** pool.len;
        var w = std.Io.Writer.Allocating.init(g.a);
        try w.writer.writeByte('[');
        const k = 1 + g.r.uintLessThan(usize, 4);
        var n: usize = 0;
        for (0..k) |_| {
            const i = g.r.uintLessThan(usize, pool.len);
            if (used[i]) continue;
            used[i] = true;
            if (n > 0) try w.writer.writeByte(',');
            n += 1;
            const p = pool[i];
            if (std.mem.eql(u8, p, "array")) {
                try w.writer.print("{{\"type\":\"array\",\"items\":{s}}}", .{try g.ty(depth + 1)});
            } else if (std.mem.eql(u8, p, "map")) {
                try w.writer.print("{{\"type\":\"map\",\"values\":{s}}}", .{try g.ty(depth + 1)});
            } else if (std.mem.eql(u8, p, "record")) {
                try w.writer.writeAll(try g.record(depth + 1, null));
            } else try w.writer.writeAll(p);
        }
        try w.writer.writeByte(']');
        return w.written();
    }

    /// A random datum of `n`, appended as the binary encoding; a branch back to a
    /// recursive record is cut off by taking `null` once deep.
    pub fn datum(g: *Gen, out: *std.array_list.Managed(u8), n: *const Node, depth: usize) !void {
        const r = g.r;
        switch (n.kind) {
            .null => {},
            .boolean => try out.append(@intFromBool(r.boolean())),
            .int => try long(out, switch (n.logical) {
                .time_millis => r.intRangeLessThan(i64, 0, 86_400_000),
                else => g.edge(i32),
            }),
            .long => try long(out, switch (n.logical) {
                .time_micros => r.intRangeLessThan(i64, 0, 86_400_000_000),
                .timestamp_millis => r.intRangeAtMost(i64, -9_000_000_000_000_000 / 1000, 9_000_000_000_000_000 / 1000),
                else => g.edge(i64),
            }),
            .float => try out.appendSlice(&std.mem.toBytes(g.bits(u32))),
            .double => try out.appendSlice(&std.mem.toBytes(g.bits(u64))),
            .string, .bytes => {
                if (n.logical == .decimal) return g.decimalBytes(out, n, null);
                const k = r.uintLessThan(usize, 24);
                try long(out, @intCast(k));
                for (0..k) |_| try out.append(r.int(u8));
            },
            .fixed => {
                if (n.logical == .decimal) return g.decimalBytes(out, n, n.size);
                for (0..n.size) |_| try out.append(r.int(u8));
            },
            .@"enum" => try long(out, @intCast(r.uintLessThan(usize, n.symbols.len))),
            .record => for (n.fields) |f| try g.datum(out, f.node, depth + 1),
            .array, .map => {
                const blocks = if (depth > 6) 0 else r.uintLessThan(usize, 3);
                for (0..blocks) |_| {
                    const k = 1 + r.uintLessThan(usize, 3);
                    var body = std.array_list.Managed(u8).init(g.a);
                    for (0..k) |_| {
                        if (n.kind == .map) {
                            try long(&body, 1);
                            try body.append('a' + r.uintLessThan(u8, 26));
                        }
                        try g.datum(&body, n.items.?, depth + 1);
                    }
                    if (r.boolean()) {
                        try long(out, @intCast(k));
                    } else {
                        try long(out, -@as(i64, @intCast(k)));
                        try long(out, @intCast(body.items.len));
                    }
                    try out.appendSlice(body.items);
                }
                try long(out, 0);
            },
            .@"union" => {
                var i = r.uintLessThan(usize, n.branches.len);
                if (depth > 6) for (n.branches, 0..) |b, j| if (b.kind == .null) {
                    i = j;
                };
                try long(out, @intCast(i));
                try g.datum(out, n.branches[i], depth + 1);
            },
        }
    }

    fn edge(g: *Gen, comptime T: type) i64 {
        return switch (g.r.uintLessThan(u8, 6)) {
            0 => std.math.minInt(T),
            1 => std.math.maxInt(T),
            2 => 0,
            3 => -1,
            else => g.r.int(T),
        };
    }

    fn bits(g: *Gen, comptime T: type) T {
        const F = if (T == u32) f32 else f64;
        return switch (g.r.uintLessThan(u8, 6)) {
            0 => @bitCast(std.math.nan(F)),
            1 => @bitCast(std.math.inf(F)),
            2 => @bitCast(-@as(F, 0)),
            else => g.r.int(T),
        };
    }

    /// An unscaled value below 10^precision, as big-endian two's complement: `size`
    /// bytes for a fixed, else the fewest bytes or a few sign bytes more.
    fn decimalBytes(g: *Gen, out: *std.array_list.Managed(u8), n: *const Node, size: ?usize) !void {
        const bound = pow10_i128[n.precision];
        var v: i128 = @rem(@as(i128, @bitCast(g.r.int(u128))), bound);
        if (g.r.uintLessThan(u8, 4) == 0) v = if (g.r.boolean()) bound - 1 else -(bound - 1);
        const be = std.mem.toBytes(std.mem.nativeToBig(i128, v));
        const sign: u8 = if (v < 0) 0xFF else 0;
        var start: usize = 0;
        while (start < 15 and be[start] == sign and (be[start + 1] & 0x80 != 0) == (v < 0)) start += 1;
        const want = size orelse (16 - start) + g.r.uintLessThan(usize, 3) * @as(usize, @intFromBool(g.r.uintLessThan(u8, 4) == 0));
        if (size == null) try long(out, @intCast(want));
        if (want >= 16) {
            try out.appendNTimes(sign, want - 16);
            try out.appendSlice(&be);
        } else try out.appendSlice(be[16 - want ..]);
    }
};

fn long(out: *std.array_list.Managed(u8), v: i64) !void {
    try varint.write(out, varint.zigzag(v));
}

pub const sync = "0123456789abcdef".*;

/// A container file of `schema_json` holding `bodies`, one block each, compressed
/// with `codec`.
pub fn file(a: std.mem.Allocator, schema_json: []const u8, codec: read.Codec, bodies: []const Block) ![]const u8 {
    var out = std.array_list.Managed(u8).init(a);
    try out.appendSlice(try write.header(a, schema_json, codec, sync));
    for (bodies) |b| {
        const data = try write.compress(a, codec, b.bytes);
        try long(&out, @intCast(b.count));
        try long(&out, @intCast(data.len));
        try out.appendSlice(data);
        try out.appendSlice(&sync);
    }
    return out.items;
}

pub const Block = struct { count: usize, bytes: []const u8 };

/// A random valid file: a random schema and `rows` datums split into random blocks.
pub fn randomFile(g: *Gen, codec: read.Codec, rows: usize) !struct { bytes: []const u8, schema_json: []const u8, blocks: []const Block } {
    const json = try g.top();
    const root = try schema.parse(g.a, json);
    var blocks = std.array_list.Managed(Block).init(g.a);
    var left = rows;
    while (left > 0) {
        const k = @min(left, 1 + g.r.uintLessThan(usize, 8));
        var body = std.array_list.Managed(u8).init(g.a);
        for (0..k) |_| try g.datum(&body, root, 0);
        try blocks.append(.{ .count = k, .bytes = body.items });
        left -= k;
    }
    return .{ .bytes = try file(g.a, json, codec, blocks.items), .schema_json = json, .blocks = blocks.items };
}

/// Where a file's header ends and its first block begins.
pub fn headerLen(a: std.mem.Allocator, bytes: []const u8) !usize {
    var in = std.Io.Reader.fixed(bytes);
    _ = try read.readHeader(a, &in);
    return in.seek;
}

/// `bytes` with its blocks corrupted, the header left whole: bits flipped, a run
/// stamped with an extreme varint, bytes dropped, inserted or cut off at the end.
pub fn mutateBody(r: std.Random, a: std.mem.Allocator, bytes: []const u8, head: usize) ![]const u8 {
    var out = std.array_list.Managed(u8).init(a);
    try out.appendSlice(bytes);
    if (out.items.len <= head) return out.items;
    for (0..1 + r.uintLessThan(usize, 4)) |_| {
        const body = out.items.len - head;
        if (body == 0) break;
        const at = head + r.uintLessThan(usize, body);
        switch (r.uintLessThan(u8, 6)) {
            0, 1 => out.items[at] ^= @as(u8, 1) << r.int(u3),
            2 => {
                const big = [_][]const u8{ &.{ 0xFF, 0xFF, 0xFF, 0xFF, 0x0F }, &.{ 0xFE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01 }, &.{0x01}, &.{ 0x80, 0x80, 0x80 } };
                const s = big[r.uintLessThan(usize, big.len)];
                const n = @min(s.len, out.items.len - at);
                @memcpy(out.items[at..][0..n], s[0..n]);
            },
            3 => _ = out.orderedRemove(at),
            4 => try out.insert(at, r.int(u8)),
            else => out.items.len = at,
        }
    }
    return out.items;
}

/// The iterations a fuzz test runs: `default`, or `FUZZ_ITERS` for a real hunt, as
/// `testing/fuzzutil.zig` reads it.
pub fn iters(default: usize) usize {
    const s = std.posix.getenv("FUZZ_ITERS") orelse return default;
    return std.fmt.parseInt(usize, s, 10) catch default;
}
