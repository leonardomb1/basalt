//! Avro object container files as rows — `FROM 'x.avro'`, a folder of them, stdin,
//! or any path a stream reaches (`stream.zig`: object stores, SFTP, HTTP, a `.gz` or
//! a zip member). The header's schema fixes the columns (`schema.zig`), and the file
//! is read front to back, one block at a time: each block's objects run through the
//! program `decode.zig` compiled from that schema, straight into column builders.
//!
//! A block is checked as the spec frames it: its object count and byte size, the
//! 16-byte sync marker the header chose after it, and, for `snappy`, the CRC-32 of
//! the bytes it decompresses to. `null`, `deflate`, `snappy` and `zstandard` blocks
//! are read; `bzip2` and `xz` are refused up front naming the codec. A value that
//! runs past its block, a block holding bytes past its last object, a union branch
//! or enum symbol out of range, or a decimal with more digits than its precision
//! fails the read rather than return rows.
//!
//! Every count and length comes from an untrusted file, so each is bounded by what
//! the block can hold: a block decompresses to at most 64 times its stored size, an
//! object count or array length past its bytes is corrupt (a type that takes no
//! bytes, such as `null`, is capped at 65,536), and a value nests at most 64 deep.
//! A corrupt file is an error, never a crash or a runaway allocation.
//!
//! A later file of a folder is read into the first one's columns by name, as a
//! folder of CSVs shares the first header: a column it lacks is NULL, one the first
//! lacks is dropped, and one of another type is widened when it can be (INT into
//! FLOAT or DECIMAL) and refused naming the column when it cannot.

const Batch = @import("../../exec/batch.zig").Batch;
const column = @import("../../exec/column.zig");
const codecs = @import("../compress/codecs.zig");
const conform = @import("../../lang/conform.zig");
const driver = @import("../../connect/driver.zig");
const decode = @import("decode.zig");
const schema_mod = @import("schema.zig");
const std = @import("std");
const stream = @import("../stream.zig");
const types = @import("../../lang/types.zig");
const varint = @import("../varint.zig");

pub const Node = schema_mod.Node;

pub const Error = error{ NotAvro, CorruptAvro, UnsupportedAvro, BadAvroSchema, OutOfMemory } || std.Io.Reader.ShortError;

pub const Codec = enum { null, deflate, snappy, zstandard };

pub const magic = "Obj\x01";

const max_block = 1 << 30;
const max_ratio = 64;
const max_meta = 64 << 20;
const max_empty = decode.max_empty;

pub const Options = struct {
    batch_rows: usize = 4096,
    schema: ?types.Schema = null,
    source_name: []const u8 = "",
};

/// The header of a container file: its schema's text, block codec and sync marker.
pub const Header = struct {
    schema_text: []const u8,
    codec: Codec,
    sync: [16]u8,
};

const corrupt = decode.corrupt;
const unsupported = decode.unsupported;

fn ended(e: std.Io.Reader.Error, comptime what: []const u8) Error {
    return switch (e) {
        error.EndOfStream => corrupt("the file ends inside " ++ what, .{}),
        error.ReadFailed => error.ReadFailed,
    };
}

/// The zigzag varint at the stream's position; null when the stream ends before its
/// first byte and `at_end` allows it.
fn takeLong(in: *std.Io.Reader, comptime what: []const u8, at_end: bool) Error!?i64 {
    var v: u64 = 0;
    var shift: u7 = 0;
    while (true) {
        const b = in.takeByte() catch |e| {
            if (at_end and shift == 0 and e == error.EndOfStream) return null;
            return ended(e, what);
        };
        v |= @as(u64, b & 0x7F) << @intCast(shift);
        if (b & 0x80 == 0) return varint.unzigzag(v);
        shift += 7;
        if (shift > 63) return corrupt("a number in " ++ what ++ " runs past ten bytes", .{});
    }
}

fn takeBytes(in: *std.Io.Reader, arena: std.mem.Allocator, comptime what: []const u8) Error![]u8 {
    const n = (try takeLong(in, what, false)).?;
    if (n < 0 or n > max_meta) return corrupt("a length of {d} in " ++ what, .{n});
    const out = try arena.alloc(u8, @intCast(n));
    in.readSliceAll(out) catch |e| return ended(e, what);
    return out;
}

/// The header at the stream's start; a stream not starting with `Obj\x01` is not Avro.
pub fn readHeader(arena: std.mem.Allocator, in: *std.Io.Reader) Error!Header {
    const m = in.takeArray(4) catch |e| return switch (e) {
        error.EndOfStream => error.NotAvro,
        error.ReadFailed => error.ReadFailed,
    };
    if (!std.mem.eql(u8, m, magic)) return error.NotAvro;
    var schema_text: ?[]const u8 = null;
    var codec_name: []const u8 = "null";
    while (true) {
        var count = (try takeLong(in, "the header", false)).?;
        if (count == 0) break;
        if (count < 0) {
            count = -count;
            _ = try takeLong(in, "the header", false);
        }
        for (0..@intCast(count)) |_| {
            const key = try takeBytes(in, arena, "the header");
            const val = try takeBytes(in, arena, "the header");
            if (std.mem.eql(u8, key, "avro.schema")) schema_text = val;
            if (std.mem.eql(u8, key, "avro.codec")) codec_name = val;
        }
    }
    const sync = (in.takeArray(16) catch |e| return ended(e, "the header")).*;
    const codec = std.meta.stringToEnum(Codec, codec_name) orelse
        return unsupported("blocks compressed with `{s}`; basalt reads null, deflate, snappy and zstandard", .{codec_name});
    return .{ .schema_text = schema_text orelse return corrupt("the header has no `avro.schema`", .{}), .codec = codec, .sync = sync };
}

pub const decimalOf = decode.decimalOf;

pub const Reader = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    in: *std.Io.Reader,
    opts: Options,
    header: Header,
    root: *const Node,
    own: types.Schema,
    schema: types.Schema,
    absent: []usize,
    m: decode.Machine,
    raw: std.ArrayListUnmanaged(u8) = .empty,
    dec: std.Io.Writer.Allocating,
    blk: std.heap.ArenaAllocator,
    left: u64 = 0,
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, in: *std.Io.Reader, opts: Options) Error!*Reader {
        const header = try readHeader(arena, in);
        const root = try schema_mod.parse(arena, header.schema_text);
        const p = try schema_mod.plan(arena, root);
        const table = opts.schema orelse p.schema;
        const routes = try arena.alloc(decode.Route, p.schema.fields.len);
        const absent = try route(arena, p.schema, table, routes, opts.source_name);
        const prog = try decode.compile(arena, p, routes);
        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .gpa = gpa,
            .in = in,
            .opts = opts,
            .header = header,
            .root = root,
            .own = p.schema,
            .schema = table,
            .absent = absent,
            .m = .init(gpa, prog, table, opts.source_name),
            .dec = .init(gpa),
            .blk = .init(gpa),
        };
        return self;
    }

    /// Each of this file's columns to the one of the table it fills, by name; the
    /// table's columns no file column fills.
    fn route(arena: std.mem.Allocator, own: types.Schema, out: types.Schema, routes: []decode.Route, source_name: []const u8) Error![]usize {
        const cols = switch (try conform.conform(arena, out, own, conform.Policy.avro_route, null)) {
            .cols => |c| c,
            .problem => |p| {
                const t = p.type;
                return unsupported("`{s}`: column `{s}` is {s} here but {s} in the first file", .{ source_name, t.name, try t.got.name(arena), try t.want.name(arena) });
            },
        };
        @memset(routes, .{});
        var absent = std.ArrayListUnmanaged(usize).empty;
        for (cols, 0..) |c, to| {
            const from = c.from orelse {
                try absent.append(arena, to);
                continue;
            };
            routes[from] = .{ .to = to, .cast = c.how == .cast };
        }
        return absent.items;
    }

    pub fn next(self: *Reader, arena: std.mem.Allocator) Error!?Batch {
        if (self.done) return null;
        const fields = self.schema.fields;
        const builders = try arena.alloc(column.Builder, fields.len);
        for (builders, fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, self.opts.batch_rows);
        self.m.out = builders;
        const cur = &self.m.cur;
        var n: usize = 0;
        while (n < self.opts.batch_rows) {
            if (self.left == 0) {
                if (!try self.nextBlock()) {
                    self.done = true;
                    break;
                }
                continue;
            }
            try self.m.row(arena);
            for (self.absent) |i| try builders[i].appendNull();
            self.left -= 1;
            n += 1;
            if (self.left == 0 and cur.pos != cur.buf.len)
                return corrupt("a block holds {d} bytes past its last object", .{cur.buf.len - cur.pos});
        }
        if (n == 0) return null;
        const cols = try arena.alloc(column.Column, fields.len);
        for (builders, cols) |*b, *c| c.* = try b.finish();
        return .{ .schema = &self.schema, .columns = cols, .len = n };
    }

    /// Reads the next block and decompresses it; false at the end of the file.
    fn nextBlock(self: *Reader) Error!bool {
        const count = (try takeLong(self.in, "a block", true)) orelse return false;
        const size = (try takeLong(self.in, "a block", false)).?;
        if (count < 0 or size < 0 or size > max_block) return corrupt("a block claims {d} objects in {d} bytes", .{ count, size });
        const room: i64 = if (schema_mod.zeroWidth(self.root)) max_empty else size *| max_ratio +| 64;
        if (count > room) return corrupt("a block claims {d} objects in {d} bytes", .{ count, size });
        self.raw.clearRetainingCapacity();
        try self.raw.resize(self.gpa, @intCast(size));
        self.in.readSliceAll(self.raw.items) catch |e| return ended(e, "a block");
        const sync = self.in.takeArray(16) catch |e| return ended(e, "a block");
        if (!std.mem.eql(u8, sync, &self.header.sync))
            return corrupt("a block's sync marker is not the header's: the file is corrupt or two files were joined", .{});
        _ = self.blk.reset(.retain_capacity);
        self.m.cur = .{ .buf = try self.decompress(self.raw.items) };
        self.left = @intCast(count);
        if (count == 0 and self.m.cur.buf.len != 0) return corrupt("a block of no objects holds {d} bytes", .{self.m.cur.buf.len});
        return true;
    }

    /// A block's bytes decompressed, to at most 64 times their stored size, as the
    /// most a real codec reaches on Avro data; past that it is a decompression bomb.
    fn decompress(self: *Reader, raw: []const u8) Error![]const u8 {
        const a = self.blk.allocator();
        const cap = @min(max_block, raw.len *| max_ratio +| 64);
        switch (self.header.codec) {
            .null => return raw,
            .deflate => {
                var src: std.Io.Reader = .fixed(raw);
                var d = std.compress.flate.Decompress.init(&src, .raw, try a.alloc(u8, std.compress.flate.max_window_len));
                return self.pump(&d.reader, "deflate", cap);
            },
            .zstandard => {
                var src: std.Io.Reader = .fixed(raw);
                var d = std.compress.zstd.Decompress.init(&src, try a.alloc(u8, std.compress.zstd.default_window_len), .{});
                return self.pump(&d.reader, "zstandard", cap);
            },
            .snappy => {
                if (raw.len < 4) return corrupt("a snappy block has no CRC", .{});
                const body = raw[0 .. raw.len - 4];
                const crc = std.mem.readInt(u32, raw[raw.len - 4 ..][0..4], .big);
                var at: usize = 0;
                const n = varint.read(body, &at) catch return corrupt("a snappy block is corrupt", .{});
                if (n > cap) return corrupt("a block decompresses to more than 64 times its size", .{});
                const out = codecs.decompress(a, .snappy, body, @intCast(n)) catch |e| switch (e) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return corrupt("a snappy block is corrupt", .{}),
                };
                if (std.hash.Crc32.hash(out) != crc) return corrupt("a snappy block fails its CRC", .{});
                return out;
            },
        }
    }

    fn pump(self: *Reader, r: *std.Io.Reader, comptime name: []const u8, cap: usize) Error![]const u8 {
        self.dec.clearRetainingCapacity();
        var room: usize = cap + 1;
        while (true) {
            const n = r.stream(&self.dec.writer, .limited(room)) catch |e| switch (e) {
                error.EndOfStream => return self.dec.written(),
                error.WriteFailed => return error.OutOfMemory,
                error.ReadFailed => return corrupt("a " ++ name ++ " block is corrupt", .{}),
            };
            room -= n;
            if (room == 0) return corrupt("a block decompresses to more than 64 times its size", .{});
        }
    }

    pub fn close(self: *Reader) void {
        self.raw.deinit(self.gpa);
        self.dec.deinit();
        self.m.deinit();
        self.blk.deinit();
    }

    fn schemaFn(ptr: *anyopaque) types.Schema {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.schema;
    }

    fn nextFn(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        return self.next(arena);
    }

    fn closeFn(ptr: *anyopaque) void {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        self.close();
    }

    const vtable = driver.Source.VTable{ .schema = schemaFn, .next = nextFn, .close = closeFn };

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

/// Avro files read as one table (`files.Files`).
pub const Files = @import("../files.zig").Files(Reader, Options);

const testing = std.testing;
const Value = @import("../../exec/value.zig").Value;
const eval = @import("../../exec/eval.zig");

fn readAll(a: std.mem.Allocator, bytes: []const u8) !struct { schema: types.Schema, rows: []const []const Value } {
    var in = std.Io.Reader.fixed(bytes);
    const r = try Reader.open(a, testing.allocator, &in, .{});
    defer r.close();
    var out = std.array_list.Managed([]const Value).init(a);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| {
            const row = try a.alloc(Value, b.columns.len);
            for (row, b.columns) |*v, c| v.* = c.getValue(i);
            try out.append(row);
        }
    }
    return .{ .schema = r.schema, .rows = out.items };
}

/// A container file of `schema_json` with one block of `count` objects in `body`.
fn file(a: std.mem.Allocator, schema_json: []const u8, codec: []const u8, count: i64, body: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(a);
    try out.appendSlice(magic);
    try varint.write(&out, varint.zigzag(2));
    for ([_][]const u8{ "avro.schema", schema_json, "avro.codec", codec }) |s| {
        try varint.write(&out, varint.zigzag(@intCast(s.len)));
        try out.appendSlice(s);
    }
    try out.append(0);
    try out.appendSlice("0123456789abcdef");
    try varint.write(&out, varint.zigzag(count));
    try varint.write(&out, varint.zigzag(@intCast(body.len)));
    try out.appendSlice(body);
    try out.appendSlice("0123456789abcdef");
    return out.items;
}

test "nested records flatten, nullable unions read NULL, arrays, maps and mixed unions read as JSON" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema_json =
        \\{"type":"record","name":"r","fields":[
        \\ {"name":"id","type":"int"},
        \\ {"name":"addr","type":["null",{"type":"record","name":"A","fields":[{"name":"city","type":"string"},{"name":"n","type":"long"}]}]},
        \\ {"name":"tags","type":{"type":"array","items":"string"}},
        \\ {"name":"m","type":{"type":"map","values":["null","double"]}},
        \\ {"name":"mix","type":["string","int","null"]},
        \\ {"name":"e","type":{"type":"enum","name":"E","symbols":["LOW","HIGH"]}},
        \\ {"name":"ts","type":{"type":"long","logicalType":"timestamp-millis"}},
        \\ {"name":"amt","type":{"type":"fixed","name":"F","size":4,"logicalType":"decimal","precision":9,"scale":2}}
        \\]}
    ;
    const body = [_]u8{
        0x02, 0x02, 0x06, 'L',  'i',  's',  0x0F, 0x04, 0x02, 'a',  0x02, 'b',  0x00, 0x02, 0x02, 'k', 0x02, 0, 0, 0, 0, 0, 0, 0xF8, 0x3F, 0x00, 0x00, 0x02, 'x', 0x02, 0xD0, 0x0F, 0x00, 0x00, 0x30, 0x39,
        0x04, 0x00, 0x00, 0x00, 0x02, 0x7F, 0x00, 0xCF, 0x0F, 0xFF, 0xFF, 0xFF, 0xFF,
    };
    const got = try readAll(a, try file(a, schema_json, "null", 2, &body));
    const names = [_][]const u8{ "id", "addr.city", "addr.n", "tags", "m", "mix", "e", "ts", "amt" };
    try testing.expectEqual(names.len, got.schema.fields.len);
    for (names, got.schema.fields) |n, f| try testing.expectEqualStrings(n, f.name);
    try testing.expectEqual(@as(usize, 2), got.rows.len);
    const r0 = got.rows[0];
    try testing.expectEqual(@as(i64, 1), r0[0].int);
    try testing.expectEqualStrings("Lis", r0[1].string);
    try testing.expectEqual(@as(i64, -8), r0[2].int);
    try testing.expectEqualStrings("[\"a\",\"b\"]", r0[3].string);
    try testing.expectEqualStrings("{\"k\":1.5}", r0[4].string);
    try testing.expectEqualStrings("\"x\"", r0[5].string);
    try testing.expectEqualStrings("HIGH", r0[6].string);
    try testing.expectEqual(@as(i64, 1000_000), r0[7].timestamp);
    try testing.expectEqual(@as(i128, 12345), r0[8].decimal.unscaled);
    const r1 = got.rows[1];
    try testing.expect(r1[1].isNull() and r1[2].isNull());
    try testing.expectEqualStrings("[]", r1[3].string);
    try testing.expectEqualStrings("{}", r1[4].string);
    try testing.expectEqualStrings("-64", r1[5].string);
    try testing.expectEqualStrings("LOW", r1[6].string);
    try testing.expectEqual(@as(i64, -1_000_000), r1[7].timestamp);
    try testing.expectEqual(@as(i128, -1), r1[8].decimal.unscaled);
}

test "not Avro, a bad sync marker, a short block, stray bytes and an unknown codec are refused" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try testing.expectError(error.NotAvro, readAll(a, "PAR1...."));
    try testing.expectError(error.NotAvro, readAll(a, ""));
    const s = "\"long\"";
    const good = try file(a, s, "null", 2, &.{ 0x02, 0x04 });
    try testing.expectEqual(@as(usize, 2), (try readAll(a, good)).rows.len);
    const bad_sync = try a.dupe(u8, good);
    bad_sync[bad_sync.len - 1] ^= 1;
    try testing.expectError(error.CorruptAvro, readAll(a, bad_sync));
    try testing.expectError(error.CorruptAvro, readAll(a, good[0 .. good.len - 3]));
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, s, "null", 3, &.{ 0x02, 0x04 })));
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, s, "null", 1, &.{ 0x02, 0x04 })));
    try testing.expectError(error.UnsupportedAvro, readAll(a, try file(a, s, "bzip2", 1, &.{0x02})));
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, "[\"null\",\"long\"]", "null", 1, &.{ 0x04, 0x02 })));
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":2}", "null", 1, &.{ 0x04, 0x01, 0x00 })));
}

test "a later file reads into the first one's columns by name, widening an INT to FLOAT" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "1.avro", .data = try file(a, "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"a\",\"type\":\"double\"},{\"name\":\"b\",\"type\":\"string\"}]}", "null", 1, &.{ 0, 0, 0, 0, 0, 0, 0xF0, 0x3F, 0x02, 'x' }) });
    try tmp.dir.writeFile(.{ .sub_path = "2.avro", .data = try file(a, "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"c\",\"type\":\"int\"},{\"name\":\"a\",\"type\":\"int\"}]}", "null", 1, &.{ 0x02, 0x06 }) });
    try tmp.dir.writeFile(.{ .sub_path = "3.avro", .data = try file(a, "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"b\",\"type\":\"int\"}]}", "null", 1, &.{0x02}) });
    const base = try tmp.dir.realpathAlloc(a, ".");
    const files = [_][]const u8{ try std.fs.path.join(a, &.{ base, "1.avro" }), try std.fs.path.join(a, &.{ base, "2.avro" }) };
    const f = try Files.open(a, testing.allocator, &files, .{});
    defer f.close();
    var rows: usize = 0;
    while (try f.next(a)) |b| {
        if (rows == 1) {
            try testing.expectEqual(@as(f64, 3), b.columns[0].getValue(0).float);
            try testing.expect(b.columns[1].getValue(0).isNull());
        }
        rows += b.len;
    }
    try testing.expectEqual(@as(usize, 2), rows);
    const clash = [_][]const u8{ files[0], try std.fs.path.join(a, &.{ base, "3.avro" }) };
    const g = try Files.open(a, testing.allocator, &clash, .{});
    defer g.close();
    _ = try g.next(a);
    try testing.expectError(error.UnsupportedAvro, g.next(a));
}

fn dump(a: std.mem.Allocator, bytes: []const u8) ![]const u8 {
    const got = try readAll(a, bytes);
    var out = std.Io.Writer.Allocating.init(a);
    for (got.rows) |row| {
        for (got.schema.fields, row) |f, v| try out.writer.print("{s}={s}\n", .{ f.name, if (v.isNull()) "NULL" else try eval.valueToString(a, v) });
        try out.writer.writeAll("--\n");
    }
    return out.written();
}

test "fastavro's files read alike through every codec it writes, several blocks each" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ts = "ts_ms=2026-10-24 12:30:15.123000\nts_us=2026-10-24 12:30:15.123456\nlts_us=2026-10-24 09:30:15.123456\n";
    const first = "id=1\nname=order 1 a\xc3\xa7\xc3\xa3o\nprice=19.99\nqty=3\nok=true\nratio=0.25\namount=12345678.90\n" ++
        "big=12345678901234567890123456789012.345678\nday=2026-10-24\nt_ms=01:02:03.004000\nt_us=01:02:03.004005\n" ++ ts ++
        "uid=00000000-0000-0000-1234-567890abcdef\nstatus=NEW\naddr.city=Lisboa\naddr.zip=1001\ntags=[\"a\",\"b \\\"q\\\"\"]\n" ++
        "attrs={\"k\":1,\"z\":-2}\nraw=\x00\x01\xff\nchoice=7\nparent=NULL\n--\n";
    const second = "id=2\nname=NULL\nprice=-0.5\nqty=-2147483648\nok=false\nratio=-1.5\namount=-0.01\nbig=-1.000001\nday=1969-12-31\n" ++
        "t_ms=00:00:00\nt_us=00:00:00\nts_ms=1969-12-31 23:59:59.999000\nts_us=1900-01-01 00:00:00\nlts_us=1970-01-01 00:00:00\n" ++
        "uid=00000000-0000-0000-2468-acf121579bde\nstatus=PAID\naddr.city=NULL\naddr.zip=NULL\ntags=[]\nattrs={}\nraw=\nchoice=NULL\nparent=NULL\n--\n";
    const parent = "{\"id\":1,\"name\":\"order 1 a\xc3\xa7\xc3\xa3o\",\"price\":19.99,\"qty\":3,\"ok\":true,\"ratio\":0.25,\"amount\":\"12345678.90\"," ++
        "\"big\":\"12345678901234567890123456789012.345678\",\"day\":\"2026-10-24\",\"t_ms\":\"01:02:03.004000\",\"t_us\":\"01:02:03.004005\"," ++
        "\"ts_ms\":\"2026-10-24 12:30:15.123000\",\"ts_us\":\"2026-10-24 12:30:15.123456\",\"lts_us\":\"2026-10-24 09:30:15.123456\"," ++
        "\"uid\":\"00000000-0000-0000-1234-567890abcdef\",\"status\":\"NEW\",\"addr\":{\"city\":\"Lisboa\",\"zip\":1001},\"tags\":[\"a\",\"b \\\"q\\\"\"]," ++
        "\"attrs\":{\"k\":1,\"z\":-2},\"raw\":\"AAH/\",\"choice\":7,\"parent\":null}";
    const third = "id=3\nname=order 3 a\xc3\xa7\xc3\xa3o\nprice=1" ++ "0" ** 300 ++ "\nqty=2147483647\nok=true\nratio=3\namount=0.00\nbig=0.000000\n" ++
        "day=0001-01-01\nt_ms=23:59:59.999000\nt_us=23:59:59.999999\nts_ms=1970-01-01 00:00:00\nts_us=2262-04-11 23:47:16.854775\n" ++
        "lts_us=2000-02-29 12:00:00\nuid=00000000-0000-0000-369d-0369b20369cd\nstatus=SHIPPED\naddr.city=S\xc3\xa3o Paulo\naddr.zip=NULL\n" ++
        "tags=[\"x\"]\nattrs={\"big\":9223372036854775807}\nraw=plain\nchoice=\"seven\"\nparent=" ++ parent ++ "\n--\n";
    for ([_][]const u8{ @embedFile("../testdata/avro/orders_null.avro"), @embedFile("../testdata/avro/orders_deflate.avro"), @embedFile("../testdata/avro/orders_snappy.avro"), @embedFile("../testdata/avro/orders_zstandard.avro") }) |bytes|
        try testing.expectEqualStrings(first ++ second ++ third, try dump(a, bytes));
}

test "counts past what a block holds, a decompression bomb and a value nested too deep are corrupt" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const arr = "{\"type\":\"array\",\"items\":\"long\"}";
    try testing.expectEqual(@as(usize, 1), (try readAll(a, try file(a, arr, "null", 1, &.{ 0x04, 0x02, 0x04, 0x00 }))).rows.len);
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, arr, "null", 1, &.{ 0xFE, 0xFF, 0xFF, 0xFF, 0x0F, 0x02 })));
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, "{\"type\":\"array\",\"items\":\"null\"}", "null", 1, &.{ 0xFE, 0xFF, 0xFF, 0xFF, 0x0F, 0x00 })));
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, "\"long\"", "null", 1 << 40, &.{0x02})));

    var big = std.array_list.Managed(u8).init(a);
    try big.appendNTimes(0, 1 << 20);
    var z = std.Io.Writer.Allocating.init(a);
    const d = try @import("../compress/deflate.zig").Deflate.init(testing.allocator, &z.writer, .{ .container = .raw });
    defer d.deinit(testing.allocator);
    try d.interface.writeAll(big.items);
    try d.finish();
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, "{\"type\":\"fixed\",\"name\":\"f\",\"size\":1048576}", "deflate", 1, z.written())));

    const list = "{\"type\":\"record\",\"name\":\"L\",\"fields\":[{\"name\":\"next\",\"type\":[\"null\",\"L\"]}]}";
    var deep = std.array_list.Managed(u8).init(a);
    try deep.appendNTimes(0x02, 80);
    try deep.append(0x00);
    try testing.expectError(error.CorruptAvro, readAll(a, try file(a, list, "null", 1, deep.items)));
    deep.items = deep.items[60..];
    const ok = try readAll(a, try file(a, list, "null", 1, deep.items));
    try testing.expect(std.mem.startsWith(u8, ok.rows[0][0].string, "{\"next\":{\"next\":"));
}

const tu = @import("testing_util.zig");

const fixtures = [_][]const u8{
    @embedFile("../testdata/avro/orders_null.avro"),
    @embedFile("../testdata/avro/orders_deflate.avro"),
    @embedFile("../testdata/avro/orders_snappy.avro"),
    @embedFile("../testdata/avro/orders_zstandard.avro"),
};

test "fuzz: random schemas and datums read every row, alike under every codec" {
    for (0..tu.iters(300)) |seed| {
        var ar = std.heap.ArenaAllocator.init(testing.allocator);
        defer ar.deinit();
        const a = ar.allocator();
        var prng = std.Random.DefaultPrng.init(0xa5_0000 + seed);
        var g = tu.Gen{ .r = prng.random(), .a = a };
        const rows = g.r.uintLessThan(usize, 24);
        const f = try tu.randomFile(&g, .null, rows);
        const want = dump(a, f.bytes) catch |e| {
            std.debug.print("seed {d}: {s} ({s})\nschema {s}\n", .{ seed, @errorName(e), eval.takeFailure(e) orelse "", f.schema_json });
            return e;
        };
        const got_rows = (try readAll(a, f.bytes)).rows.len;
        try testing.expectEqual(rows, got_rows);
        for ([_]Codec{ .deflate, .snappy }) |codec| {
            const other = try tu.file(a, f.schema_json, codec, f.blocks);
            try testing.expectEqualStrings(want, try dump(a, other));
        }
    }
}

fn readAny(bytes: []const u8) void {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    _ = readAll(ar.allocator(), bytes) catch return;
}

fn fuzzOne(_: void, input: []const u8) anyerror!void {
    readAny(input);
}

test "fuzz: corrupted blocks, headers and schemas never crash, leak or hang the reader" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xbad_a5);
    const r = prng.random();
    var corpus = std.array_list.Managed([]const u8).init(a);
    try corpus.appendSlice(&fixtures);
    var g = tu.Gen{ .r = r, .a = a };
    for (0..24) |i| try corpus.append((try tu.randomFile(&g, @enumFromInt(i % 3), 1 + r.uintLessThan(usize, 12))).bytes);
    for (0..tu.iters(2000)) |_| {
        var it = std.heap.ArenaAllocator.init(testing.allocator);
        defer it.deinit();
        const base = corpus.items[r.uintLessThan(usize, corpus.items.len)];
        readAny(try tu.mutateBody(r, it.allocator(), base, try tu.headerLen(it.allocator(), base)));
    }
    try @import("../../testing/fuzzutil.zig").pound(fuzzOne, corpus.items[0..4]);
    for (0..tu.iters(2000)) |_| {
        var it = std.heap.ArenaAllocator.init(testing.allocator);
        defer it.deinit();
        const ia = it.allocator();
        var text = std.array_list.Managed(u8).init(ia);
        try text.appendSlice(try g.top());
        for (0..1 + r.uintLessThan(usize, 3)) |_| {
            if (text.items.len == 0) break;
            const at = r.uintLessThan(usize, text.items.len);
            switch (r.uintLessThan(u8, 3)) {
                0 => text.items[at] = "{}[]\",:01-9aZ"[r.uintLessThan(usize, 14)],
                1 => _ = text.orderedRemove(at),
                else => text.items.len = at,
            }
        }
        const root = schema_mod.parse(ia, text.items) catch continue;
        _ = schema_mod.plan(ia, root) catch continue;
    }
}

test "fuzz: arbitrary bytes behind a valid header never crash the reader" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, input: []const u8) anyerror!void {
            var ar = std.heap.ArenaAllocator.init(testing.allocator);
            defer ar.deinit();
            const a = ar.allocator();
            const head = fixtures[0][0..try tu.headerLen(a, fixtures[0])];
            readAny(try std.mem.concat(a, u8, &.{ head, input }));
        }
    }.f, .{});
}

fn routeBefore(arena: std.mem.Allocator, own: types.Schema, out: types.Schema, routes: []decode.Route) !?[]usize {
    var covered = try arena.alloc(bool, out.fields.len);
    @memset(covered, false);
    for (own.fields, routes) |f, *r| {
        const to = out.indexOf(f.name) orelse {
            r.* = .{};
            continue;
        };
        const want = out.fields[to].ty;
        if (!types.Type.eql(f.ty, want) and !types.Type.canWiden(f.ty.kind, want.kind)) return null;
        r.* = .{ .to = to, .cast = !types.Type.eql(f.ty, want) };
        covered[to] = true;
    }
    var absent = std.ArrayListUnmanaged(usize).empty;
    for (covered, 0..) |c, i| if (!c) try absent.append(arena, i);
    return absent.items;
}

test "route on conform's avro_route equals the by-name walk it replaced, routes, absent columns and refusals" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const tys = [_]types.Type{ types.Type.init(.int), types.Type.init(.float), types.Type.init(.string), types.Type.init(.bool), types.Type.decimal(10, 2), types.Type.decimal(12, 4), types.Type.init(.date), types.Type.init(.timestamp), types.Type.nullColumn(true), types.Type.init(.int).asNullable() };
    const names = [_][]const u8{ "a", "b", "c", "d" };
    var prng = std.Random.DefaultPrng.init(0xa5_70);
    const r = prng.random();
    for (0..20_000) |_| {
        var two: [2]types.Schema = undefined;
        for (&two) |*s| {
            const n = r.uintLessThan(usize, names.len + 1);
            const fields = try a.alloc(types.Schema.Field, n);
            var pick = names;
            r.shuffle([]const u8, &pick);
            for (fields, 0..) |*f, i| f.* = .{ .name = pick[i], .ty = tys[r.uintLessThan(usize, tys.len)] };
            s.* = .{ .fields = fields };
        }
        const want_routes = try a.alloc(decode.Route, two[0].fields.len);
        const got_routes = try a.alloc(decode.Route, two[0].fields.len);
        const want = try routeBefore(a, two[0], two[1], want_routes);
        const got = Reader.route(a, two[0], two[1], got_routes, "f.avro");
        if (want) |absent| {
            try testing.expectEqualSlices(usize, absent, try got);
            for (want_routes, got_routes) |x, y| try testing.expectEqual(x, y);
        } else {
            try testing.expectError(error.UnsupportedAvro, got);
            _ = eval.takeFailure(error.UnsupportedAvro);
        }
    }
}

test "a union of INT and FLOAT branches reads every branch into its FLOAT column, a dropped column still checked" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const got = try readAll(a, try file(a, "{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"x\",\"type\":[\"int\",\"double\",\"null\"]}]}", "null", 3, &.{ 0x00, 0x06, 0x02, 0, 0, 0, 0, 0, 0, 0xF8, 0x3F, 0x04 }));
    try testing.expectEqual(types.TypeKind.float, got.schema.fields[0].ty.kind);
    try testing.expectEqual(@as(f64, 3), got.rows[0][0].float);
    try testing.expectEqual(@as(f64, 1.5), got.rows[1][0].float);
    try testing.expect(got.rows[2][0].isNull());
}
