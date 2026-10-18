//! Avro object container files, read whole: the format Iceberg keeps its manifest
//! lists and manifests in. A file is a header (magic, a metadata map holding the
//! writer's schema as JSON and the codec, a 16-byte sync marker) and then blocks,
//! each an object count, a byte length, that many bytes of records, compressed by
//! the codec, and the sync marker again.
//!
//! The schema is parsed with `std.json` into `Schema`: the primitives, records,
//! enums, arrays, maps, unions and fixed, with named types referenced by full or
//! short name. Logical types (`date`, `timestamp-micros`, Iceberg's map-as-array)
//! decode as their underlying type. Values decode into a generic `Value` tree.
//! Codecs: null, deflate, snappy (its CRC checked) and zstandard; bzip2 and xz are
//! refused by name.
//!
//! Every count and length comes from an untrusted file, so each is checked against
//! the bytes left, nesting is bounded, and the values a block may yield are capped
//! by its size: a corrupt file is an error, never a crash or a runaway allocation.

const std = @import("std");
const codecs = @import("../compress/codecs.zig");
const testing = std.testing;

pub const Error = error{ AvroCorrupt, AvroBadSchema, AvroUnsupportedCodec } || std.mem.Allocator.Error;

const max_depth = 64;
const max_block = 256 << 20;

pub const Schema = union(enum) {
    null,
    boolean,
    int,
    long,
    float,
    double,
    bytes,
    string,
    fixed: usize,
    @"enum": []const []const u8,
    array: *const Schema,
    map: *const Schema,
    @"union": []const *const Schema,
    record: *const Record,
};

pub const Record = struct {
    name: []const u8,
    fields: []const Field = &.{},

    pub fn index(self: *const Record, name: []const u8) ?usize {
        for (self.fields, 0..) |f, i| if (std.mem.eql(u8, f.name, name)) return i;
        return null;
    }
};

/// A record field; `id` is the `field-id` attribute Iceberg puts on its fields.
pub const Field = struct {
    name: []const u8,
    id: ?i64 = null,
    ty: *const Schema,
};

/// A decoded datum. Ints and longs are `long`, floats and doubles `double`, and
/// bytes, strings and fixed `bytes`; a union is the branch it holds.
pub const Value = union(enum) {
    null,
    bool: bool,
    long: i64,
    double: f64,
    bytes: []const u8,
    @"enum": []const u8,
    array: []const Value,
    map: []const Entry,
    record: Rec,

    pub const Entry = struct { key: []const u8, value: Value };
    pub const Rec = struct { schema: *const Record, values: []const Value };

    /// A record's field by name; null for a missing field or a value not a record.
    pub fn field(self: Value, name: []const u8) ?Value {
        if (self != .record) return null;
        const i = self.record.schema.index(name) orelse return null;
        return self.record.values[i];
    }

    pub fn text(self: Value) ?[]const u8 {
        return if (self == .bytes) self.bytes else null;
    }

    pub fn int(self: Value) ?i64 {
        return if (self == .long) self.long else null;
    }
};

/// A container's header and the blocks after it, not yet decoded.
pub const Container = struct {
    schema: *const Schema,
    codec: []const u8,
    meta: []const Value.Entry,
    sync: *const [16]u8,
    blocks: []const u8,

    pub fn open(arena: std.mem.Allocator, bytes: []const u8) Error!Container {
        if (bytes.len < 4 or !std.mem.eql(u8, bytes[0..4], "Obj\x01")) return Error.AvroCorrupt;
        var d = Decoder{ .arena = arena, .buf = bytes, .pos = 4, .budget = bytes.len + 64 };
        const meta_schema = Schema{ .map = &.bytes };
        const meta = (try d.value(&meta_schema)).map;
        if (d.buf.len - d.pos < 16) return Error.AvroCorrupt;
        const sync = d.buf[d.pos..][0..16];
        var schema_text: ?[]const u8 = null;
        var codec: []const u8 = "null";
        for (meta) |e| {
            if (std.mem.eql(u8, e.key, "avro.schema")) schema_text = e.value.bytes;
            if (std.mem.eql(u8, e.key, "avro.codec")) codec = e.value.bytes;
        }
        return .{
            .schema = try parseSchema(arena, schema_text orelse return Error.AvroBadSchema),
            .codec = codec,
            .meta = meta,
            .sync = sync,
            .blocks = d.buf[d.pos + 16 ..],
        };
    }

    /// A metadata value by key, as Iceberg puts `format-version` and `content` there.
    pub fn metaValue(self: Container, key: []const u8) ?[]const u8 {
        for (self.meta) |e| if (std.mem.eql(u8, e.key, key)) return e.value.bytes;
        return null;
    }

    /// Every record of every block, each block's sync marker checked.
    pub fn records(self: Container, arena: std.mem.Allocator) Error![]const Value {
        if (!codecSupported(self.codec)) return Error.AvroUnsupportedCodec;
        var out = std.array_list.Managed(Value).init(arena);
        var d = Decoder{ .arena = arena, .buf = self.blocks, .budget = 0 };
        while (d.pos < d.buf.len) {
            const count = try d.length();
            const size = try d.length();
            if (size > d.buf.len - d.pos or d.buf.len - d.pos - size < 16) return Error.AvroCorrupt;
            const raw = d.buf[d.pos..][0..size];
            d.pos += size;
            if (!std.mem.eql(u8, d.buf[d.pos..][0..16], self.sync)) return Error.AvroCorrupt;
            d.pos += 16;
            const data = try decompress(arena, self.codec, raw);
            if (count > data.len + 64) return Error.AvroCorrupt;
            var bd = Decoder{ .arena = arena, .buf = data, .budget = data.len + 64 };
            try out.ensureUnusedCapacity(count);
            for (0..count) |_| out.appendAssumeCapacity(try bd.value(self.schema));
            if (bd.pos != data.len) return Error.AvroCorrupt;
        }
        return out.items;
    }
};

/// Whether basalt decodes a container written with `codec`.
pub fn codecSupported(codec: []const u8) bool {
    for ([_][]const u8{ "null", "deflate", "snappy", "zstandard" }) |c| if (std.mem.eql(u8, codec, c)) return true;
    return false;
}

fn decompress(arena: std.mem.Allocator, codec: []const u8, raw: []const u8) Error![]const u8 {
    if (std.mem.eql(u8, codec, "null")) return raw;
    if (std.mem.eql(u8, codec, "deflate"))
        return codecs.inflateAlloc(arena, raw, .raw, max_block) catch |e| return if (e == error.OutOfMemory) Error.OutOfMemory else Error.AvroCorrupt;
    if (std.mem.eql(u8, codec, "zstandard"))
        return codecs.zstdAlloc(arena, raw, max_block) catch |e| return if (e == error.OutOfMemory) Error.OutOfMemory else Error.AvroCorrupt;
    if (std.mem.eql(u8, codec, "snappy")) {
        if (raw.len < 4) return Error.AvroCorrupt;
        const body = raw[0 .. raw.len - 4];
        const n = codecs.snappyLen(body) orelse return Error.AvroCorrupt;
        if (n > max_block or n > body.len *| 64 +| 64) return Error.AvroCorrupt;
        const out = codecs.decompress(arena, .snappy, body, n) catch |e| return if (e == error.OutOfMemory) Error.OutOfMemory else Error.AvroCorrupt;
        if (std.hash.Crc32.hash(out) != std.mem.readInt(u32, raw[raw.len - 4 ..][0..4], .big)) return Error.AvroCorrupt;
        return out;
    }
    return Error.AvroUnsupportedCodec;
}

/// The binary encoding of one value after another; `budget` caps the array and
/// map items a buffer yields, as a zero-width item (null, an empty record) costs
/// no bytes.
const Decoder = struct {
    arena: std.mem.Allocator,
    buf: []const u8,
    pos: usize = 0,
    budget: usize,
    depth: usize = 0,

    fn long(self: *Decoder) Error!i64 {
        var v: u64 = 0;
        var shift: u7 = 0;
        while (true) {
            if (self.pos >= self.buf.len) return Error.AvroCorrupt;
            const b = self.buf[self.pos];
            self.pos += 1;
            if (shift == 63 and b > 1) return Error.AvroCorrupt;
            v |= @as(u64, b & 0x7F) << @intCast(shift);
            if (b & 0x80 == 0) break;
            shift += 7;
            if (shift > 63) return Error.AvroCorrupt;
        }
        return @as(i64, @bitCast(v >> 1)) ^ -@as(i64, @intCast(v & 1));
    }

    /// A non-negative long that counts or sizes something in the buffer.
    fn length(self: *Decoder) Error!usize {
        const n = try self.long();
        if (n < 0) return Error.AvroCorrupt;
        return std.math.cast(usize, n) orelse Error.AvroCorrupt;
    }

    fn take(self: *Decoder, n: usize) Error![]const u8 {
        if (n > self.buf.len - self.pos) return Error.AvroCorrupt;
        defer self.pos += n;
        return self.buf[self.pos..][0..n];
    }

    /// A block count: negative when its byte size follows, which is skipped.
    fn blockCount(self: *Decoder) Error!usize {
        const n = try self.long();
        if (n == std.math.minInt(i64)) return Error.AvroCorrupt;
        if (n < 0) _ = try self.length();
        const c: usize = @intCast(@abs(n));
        if (c > self.budget) return Error.AvroCorrupt;
        self.budget -= c;
        return c;
    }

    fn value(self: *Decoder, s: *const Schema) Error!Value {
        self.depth += 1;
        defer self.depth -= 1;
        if (self.depth > max_depth) return Error.AvroCorrupt;
        switch (s.*) {
            .null => return .null,
            .boolean => {
                const b = (try self.take(1))[0];
                if (b > 1) return Error.AvroCorrupt;
                return .{ .bool = b == 1 };
            },
            .int => {
                const v = try self.long();
                if (v < std.math.minInt(i32) or v > std.math.maxInt(i32)) return Error.AvroCorrupt;
                return .{ .long = v };
            },
            .long => return .{ .long = try self.long() },
            .float => return .{ .double = @as(f32, @bitCast(std.mem.readInt(u32, (try self.take(4))[0..4], .little))) },
            .double => return .{ .double = @bitCast(std.mem.readInt(u64, (try self.take(8))[0..8], .little)) },
            .bytes, .string => return .{ .bytes = try self.take(try self.length()) },
            .fixed => |n| return .{ .bytes = try self.take(n) },
            .@"enum" => |syms| {
                const i = try self.length();
                if (i >= syms.len) return Error.AvroCorrupt;
                return .{ .@"enum" = syms[i] };
            },
            .@"union" => |branches| {
                const i = try self.length();
                if (i >= branches.len) return Error.AvroCorrupt;
                return self.value(branches[i]);
            },
            .array => |item| {
                var out = std.array_list.Managed(Value).init(self.arena);
                while (true) {
                    const n = try self.blockCount();
                    if (n == 0) break;
                    try out.ensureUnusedCapacity(n);
                    for (0..n) |_| out.appendAssumeCapacity(try self.value(item));
                }
                return .{ .array = out.items };
            },
            .map => |val| {
                var out = std.array_list.Managed(Value.Entry).init(self.arena);
                while (true) {
                    const n = try self.blockCount();
                    if (n == 0) break;
                    try out.ensureUnusedCapacity(n);
                    for (0..n) |_| {
                        const k = try self.take(try self.length());
                        out.appendAssumeCapacity(.{ .key = k, .value = try self.value(val) });
                    }
                }
                return .{ .map = out.items };
            },
            .record => |r| {
                const vals = try self.arena.alloc(Value, r.fields.len);
                for (vals, r.fields) |*v, f| v.* = try self.value(f.ty);
                return .{ .record = .{ .schema = r, .values = vals } };
            },
        }
    }
};

/// A writer's schema JSON as a `Schema`.
pub fn parseSchema(arena: std.mem.Allocator, text: []const u8) Error!*const Schema {
    const v = std.json.parseFromSliceLeaky(std.json.Value, arena, text, .{}) catch |e|
        return if (e == error.OutOfMemory) Error.OutOfMemory else Error.AvroBadSchema;
    var p = SchemaParser{ .arena = arena };
    return p.parse(v, "");
}

const SchemaParser = struct {
    arena: std.mem.Allocator,
    names: std.StringHashMapUnmanaged(*const Schema) = .empty,
    depth: usize = 0,

    fn parse(self: *SchemaParser, v: std.json.Value, ns: []const u8) Error!*const Schema {
        self.depth += 1;
        defer self.depth -= 1;
        if (self.depth > max_depth) return Error.AvroBadSchema;
        switch (v) {
            .string => |s| return self.named(s, ns),
            .array => |a| {
                const branches = try self.arena.alloc(*const Schema, a.items.len);
                for (branches, a.items) |*b, x| b.* = try self.parse(x, ns);
                return self.node(.{ .@"union" = branches });
            },
            .object => |o| {
                const t = o.get("type") orelse return Error.AvroBadSchema;
                if (t != .string) return self.parse(t, ns);
                const kind = t.string;
                if (std.mem.eql(u8, kind, "record") or std.mem.eql(u8, kind, "error")) return self.record(o, ns);
                if (std.mem.eql(u8, kind, "enum")) {
                    const syms = o.get("symbols") orelse return Error.AvroBadSchema;
                    if (syms != .array) return Error.AvroBadSchema;
                    const out = try self.arena.alloc([]const u8, syms.array.items.len);
                    for (out, syms.array.items) |*x, s| x.* = if (s == .string) s.string else return Error.AvroBadSchema;
                    return self.define(o, ns, .{ .@"enum" = out });
                }
                if (std.mem.eql(u8, kind, "fixed")) {
                    const size = o.get("size") orelse return Error.AvroBadSchema;
                    if (size != .integer or size.integer < 0) return Error.AvroBadSchema;
                    return self.define(o, ns, .{ .fixed = @intCast(size.integer) });
                }
                if (std.mem.eql(u8, kind, "array"))
                    return self.node(.{ .array = try self.parse(o.get("items") orelse return Error.AvroBadSchema, ns) });
                if (std.mem.eql(u8, kind, "map"))
                    return self.node(.{ .map = try self.parse(o.get("values") orelse return Error.AvroBadSchema, ns) });
                return self.named(kind, ns);
            },
            else => return Error.AvroBadSchema,
        }
    }

    fn node(self: *SchemaParser, s: Schema) Error!*const Schema {
        const n = try self.arena.create(Schema);
        n.* = s;
        return n;
    }

    fn named(self: *SchemaParser, s: []const u8, ns: []const u8) Error!*const Schema {
        const prims = [_]struct { []const u8, Schema }{
            .{ "null", .null },   .{ "boolean", .boolean }, .{ "int", .int },     .{ "long", .long },
            .{ "float", .float }, .{ "double", .double },   .{ "bytes", .bytes }, .{ "string", .string },
        };
        for (prims) |p| if (std.mem.eql(u8, s, p[0])) return self.node(p[1]);
        if (std.mem.indexOfScalar(u8, s, '.') == null and ns.len > 0)
            if (self.names.get(try std.mem.concat(self.arena, u8, &.{ ns, ".", s }))) |n| return n;
        return self.names.get(s) orelse Error.AvroBadSchema;
    }

    /// A named type's full name and the namespace its members resolve names in.
    fn fullName(self: *SchemaParser, o: std.json.ObjectMap, ns: []const u8) Error!struct { full: []const u8, ns: []const u8 } {
        const name = if (o.get("name")) |n| (if (n == .string) n.string else return Error.AvroBadSchema) else "";
        if (std.mem.lastIndexOfScalar(u8, name, '.')) |dot| return .{ .full = name, .ns = name[0..dot] };
        const own = if (o.get("namespace")) |n| (if (n == .string) n.string else ns) else ns;
        if (own.len == 0 or name.len == 0) return .{ .full = name, .ns = own };
        return .{ .full = try std.mem.concat(self.arena, u8, &.{ own, ".", name }), .ns = own };
    }

    fn define(self: *SchemaParser, o: std.json.ObjectMap, ns: []const u8, s: Schema) Error!*const Schema {
        const n = try self.node(s);
        const name = try self.fullName(o, ns);
        if (name.full.len > 0) try self.names.put(self.arena, name.full, n);
        return n;
    }

    /// A record registers its name before its fields parse, so a field may refer
    /// to the record it is in.
    fn record(self: *SchemaParser, o: std.json.ObjectMap, ns: []const u8) Error!*const Schema {
        const name = try self.fullName(o, ns);
        const r = try self.arena.create(Record);
        r.* = .{ .name = name.full };
        const n = try self.node(.{ .record = r });
        if (name.full.len > 0) try self.names.put(self.arena, name.full, n);
        const fs = o.get("fields") orelse return Error.AvroBadSchema;
        if (fs != .array) return Error.AvroBadSchema;
        const fields = try self.arena.alloc(Field, fs.array.items.len);
        for (fields, fs.array.items) |*f, x| {
            if (x != .object) return Error.AvroBadSchema;
            const fname = x.object.get("name") orelse return Error.AvroBadSchema;
            if (fname != .string) return Error.AvroBadSchema;
            const id: ?i64 = if (x.object.get("field-id")) |i| (if (i == .integer) i.integer else null) else null;
            f.* = .{ .name = fname.string, .id = id, .ty = try self.parse(x.object.get("type") orelse return Error.AvroBadSchema, name.ns) };
        }
        r.fields = fields;
        return n;
    }
};

const testwrite = @import("testwrite.zig");

const entry_schema =
    \\{"type":"record","name":"manifest_entry","namespace":"org.x","fields":[
    \\ {"name":"status","field-id":0,"type":"int"},
    \\ {"name":"snapshot_id","field-id":1,"type":["null","long"],"default":null},
    \\ {"name":"data_file","field-id":2,"type":{"type":"record","name":"r2","fields":[
    \\   {"name":"file_path","field-id":100,"type":"string"},
    \\   {"name":"day","type":["null",{"type":"int","logicalType":"date"}]},
    \\   {"name":"kind","type":{"type":"enum","name":"Kind","symbols":["DATA","DELETES"]}},
    \\   {"name":"key","type":["null",{"type":"fixed","name":"k16","size":2}]},
    \\   {"name":"sizes","type":["null",{"type":"array","items":{"type":"record","name":"k117_v118","fields":[{"name":"key","type":"int"},{"name":"value","type":"long"}]},"logicalType":"map"}]},
    \\   {"name":"props","type":{"type":"map","values":"string"}},
    \\   {"name":"ratio","type":"double"},{"name":"f","type":"float"},{"name":"ok","type":"boolean"},
    \\   {"name":"again","type":["null","k16","org.x.Kind"]}]}}]}
;

fn encodeEntry(e: *testwrite.Encoder, i: i64, negative: bool) !void {
    try e.long(1);
    try e.long(1);
    try e.long(-7_000_000_000_000_000_000 + i);
    try e.string("s3a://b/t/data/f.parquet");
    try e.long(1);
    try e.long(20454);
    try e.long(1);
    try e.long(1);
    try e.fixed("\x01\x02");
    try e.long(1);
    var items = testwrite.Encoder.init(e.out.allocator);
    for (0..3) |k| {
        try items.long(@intCast(k));
        try items.long(i * 100 + @as(i64, @intCast(k)));
    }
    try e.block(3, items.out.items, negative);
    try e.long(0);
    try e.block(1, "\x02a\x02b", false);
    try e.long(0);
    try e.double(0.5);
    try e.float(1.25);
    try e.boolean(true);
    try e.long(2);
    try e.long(1);
}

fn expectEntry(v: Value, i: i64) !void {
    try testing.expectEqual(@as(i64, 1), v.field("status").?.long);
    try testing.expectEqual(@as(i64, -7_000_000_000_000_000_000 + i), v.field("snapshot_id").?.long);
    const df = v.field("data_file").?;
    try testing.expectEqualStrings("s3a://b/t/data/f.parquet", df.field("file_path").?.text().?);
    try testing.expectEqual(@as(i64, 20454), df.field("day").?.long);
    try testing.expectEqualStrings("DELETES", df.field("kind").?.@"enum");
    try testing.expectEqualStrings("\x01\x02", df.field("key").?.bytes);
    const sizes = df.field("sizes").?.array;
    try testing.expectEqual(@as(usize, 3), sizes.len);
    try testing.expectEqual(i * 100 + 2, sizes[2].field("value").?.long);
    try testing.expectEqualStrings("b", df.field("props").?.map[0].value.bytes);
    try testing.expectEqual(@as(f64, 0.5), df.field("ratio").?.double);
    try testing.expectEqual(@as(f64, 1.25), df.field("f").?.double);
    try testing.expect(df.field("ok").?.bool);
    try testing.expectEqualStrings("DELETES", df.field("again").?.@"enum");
    try testing.expect(df.field("nosuch") == null);
}

test "a container round-trips under every codec: named types, unions, logical types, blocks of either sign" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_]testwrite.Codec{ .null, .deflate, .snappy, .zstandard }) |codec| {
        var recs = std.array_list.Managed([]const u8).init(a);
        for (0..5) |i| {
            var e = testwrite.Encoder.init(a);
            try encodeEntry(&e, @intCast(i), i % 2 == 1);
            try recs.append(e.out.items);
        }
        const file = try testwrite.container(a, entry_schema, codec, recs.items, 2, &.{.{ "format-version", "2" }});
        const c = try Container.open(a, file);
        try testing.expectEqualStrings(@tagName(codec), c.codec);
        try testing.expectEqualStrings("2", c.metaValue("format-version").?);
        const got = try c.records(a);
        try testing.expectEqual(@as(usize, 5), got.len);
        for (got, 0..) |v, i| try expectEntry(v, @intCast(i));
        try testing.expectEqual(@as(?i64, 100), c.schema.record.fields[2].ty.record.fields[0].id);
    }
}

test "corruption is caught: a snappy CRC, a sync marker, a codec basalt does not read, a truncated file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var e = testwrite.Encoder.init(a);
    try encodeEntry(&e, 1, false);
    const recs = [_][]const u8{e.out.items};

    const sn = try a.dupe(u8, try testwrite.container(a, entry_schema, .snappy, &recs, 1, &.{}));
    sn[sn.len - 17] ^= 0x40;
    try testing.expectError(Error.AvroCorrupt, (try Container.open(a, sn)).records(a));

    const plain = try a.dupe(u8, try testwrite.container(a, entry_schema, .null, &recs, 1, &.{}));
    plain[plain.len - 1] ^= 0xff;
    try testing.expectError(Error.AvroCorrupt, (try Container.open(a, plain)).records(a));

    const bz = try testwrite.container(a, entry_schema, .null, &recs, 1, &.{.{ "avro.codec", "bzip2" }});
    const c = try Container.open(a, bz);
    try testing.expectEqualStrings("bzip2", c.codec);
    try testing.expectError(Error.AvroUnsupportedCodec, c.records(a));

    const ok = try testwrite.container(a, entry_schema, .deflate, &recs, 1, &.{});
    try testing.expectEqual(@as(usize, 1), (try (try Container.open(a, ok)).records(a)).len);
    for (0..ok.len) |n| {
        const cut = Container.open(a, ok[0..n]) catch continue;
        const got = cut.records(a) catch continue;
        try testing.expectEqual(@as(usize, 0), got.len);
    }
    try testing.expectError(Error.AvroBadSchema, parseSchema(a, "{\"type\":\"record\",\"fields\":[{\"name\":\"x\",\"type\":\"Nope\"}]}"));
    try testing.expectError(Error.AvroBadSchema, parseSchema(a, "[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[\"int\"]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]"));
}

test "a record of many null optional fields decodes, each a byte" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var fields = std.array_list.Managed(u8).init(a);
    for (0..70) |i| try fields.appendSlice(try std.fmt.allocPrint(a, "{s}{{\"name\":\"f{d}\",\"type\":[\"null\",\"long\"]}}", .{ if (i == 0) "" else ",", i }));
    const schema = try std.fmt.allocPrint(a, "{{\"type\":\"record\",\"name\":\"r\",\"fields\":[{s}]}}", .{fields.items});
    var e = testwrite.Encoder.init(a);
    for (0..70) |_| try e.long(0);
    const recs = [_][]const u8{e.out.items};
    const got = try (try Container.open(a, try testwrite.container(a, schema, .null, &recs, 1, &.{}))).records(a);
    try testing.expectEqual(@as(usize, 1), got.len);
    try testing.expect(got[0].field("f69").? == .null);
}

test "a record that refers to itself decodes, its nesting bounded" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const s = try parseSchema(a, "{\"type\":\"record\",\"name\":\"node\",\"fields\":[{\"name\":\"v\",\"type\":\"long\"},{\"name\":\"next\",\"type\":[\"null\",\"node\"]}]}");
    var d = Decoder{ .arena = a, .buf = "\x02\x02\x04\x00", .budget = 64 };
    const v = try d.value(s);
    try testing.expectEqual(@as(i64, 2), v.field("next").?.field("v").?.long);
    const deep = try a.alloc(u8, 400);
    @memset(deep, 0x02);
    var dd = Decoder{ .arena = a, .buf = deep, .budget = 10_000 };
    try testing.expectError(Error.AvroCorrupt, dd.value(s));
}

fn fuzzContainer(_: void, input: []const u8) anyerror!void {
    var mem: [1 << 20]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&mem);
    var arena = std.heap.ArenaAllocator.init(fba.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    const c = Container.open(a, input) catch return;
    _ = c.records(a) catch {};
}

test "fuzz: the container reader survives arbitrary bytes" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var e = testwrite.Encoder.init(a);
    try encodeEntry(&e, 3, true);
    const recs = [_][]const u8{ e.out.items, e.out.items };
    var corpus: [3][]const u8 = undefined;
    for (&corpus, [_]testwrite.Codec{ .null, .snappy, .deflate }) |*c, codec| c.* = try testwrite.container(a, entry_schema, codec, &recs, 1, &.{});
    try @import("../../testing/fuzzutil.zig").pound(fuzzContainer, &corpus);
}
