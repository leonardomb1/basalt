//! Avro object container files as rows — `FROM 'x.avro'`, a folder of them, stdin,
//! or any path a stream reaches (`stream.zig`: object stores, SFTP, HTTP, a `.gz` or
//! a zip member). The header's schema fixes the columns (`schema.zig`), and the file
//! is read front to back, one block at a time: each block's objects are decoded
//! straight into column builders, never into a tree of values.
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
const Value = @import("../../exec/value.zig").Value;
const column = @import("../../exec/column.zig");
const codecs = @import("../compress/codecs.zig");
const driver = @import("../../connect/driver.zig");
const eval = @import("../../exec/eval.zig");
const jsonwrite = @import("../json/write.zig");
const schema_mod = @import("schema.zig");
const std = @import("std");
const stream = @import("../stream.zig");
const types = @import("../../lang/types.zig");
const pow10_i128 = @import("../../exec/value.zig").pow10_i128;
const varint = @import("../varint.zig");

pub const Node = schema_mod.Node;

pub const Error = error{ NotAvro, CorruptAvro, UnsupportedAvro, BadAvroSchema, OutOfMemory } || std.Io.Reader.ShortError;

pub const Codec = enum { null, deflate, snappy, zstandard };

pub const magic = "Obj\x01";

const max_block = 1 << 30;
const max_ratio = 64;
const max_meta = 64 << 20;
const max_empty = 1 << 16;

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

fn corrupt(comptime fmt: []const u8, args: anytype) Error {
    return @errorCast(stream.explainf(error.CorruptAvro, fmt, args));
}

fn unsupported(comptime fmt: []const u8, args: anytype) Error {
    return @errorCast(stream.explainf(error.UnsupportedAvro, fmt, args));
}

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

const Cursor = struct {
    buf: []const u8 = &.{},
    pos: usize = 0,

    fn short() Error {
        return corrupt("a value runs past the end of its block", .{});
    }

    fn long(c: *Cursor) Error!i64 {
        return varint.unzigzag(varint.read(c.buf, &c.pos) catch return short());
    }

    fn int(c: *Cursor) Error!i32 {
        const v = try c.long();
        return std.math.cast(i32, v) orelse corrupt("an int holds {d}, past 32 bits", .{v});
    }

    fn take(c: *Cursor, n: usize) Error![]const u8 {
        if (n > c.buf.len - c.pos) return short();
        defer c.pos += n;
        return c.buf[c.pos..][0..n];
    }

    fn bytes(c: *Cursor) Error![]const u8 {
        const n = try c.long();
        if (n < 0) return corrupt("a negative length {d}", .{n});
        return c.take(std.math.cast(usize, n) orelse return short());
    }

    fn boolean(c: *Cursor) Error!bool {
        const b = (try c.take(1))[0];
        if (b > 1) return corrupt("a boolean holds byte {d}", .{b});
        return b == 1;
    }

    fn float(c: *Cursor) Error!f64 {
        return @as(f32, @bitCast(std.mem.readInt(u32, (try c.take(4))[0..4], .little)));
    }

    fn double(c: *Cursor) Error!f64 {
        return @bitCast(std.mem.readInt(u64, (try c.take(8))[0..8], .little));
    }

    /// The item count of an array or map block, its byte size skipped when negative.
    /// Each item of a type that takes bytes takes at least one, so a count past the
    /// bytes left is corrupt; one of a type that may take none is capped.
    fn blockCount(c: *Cursor, items: *const Node) Error!usize {
        var n = try c.long();
        if (n < 0) {
            n = -%n;
            _ = try c.long();
        }
        const k = std.math.cast(usize, n) orelse return short();
        if (k > (if (schema_mod.zeroWidth(items)) max_empty else c.buf.len - c.pos)) return corrupt("an array or map claims {d} items its block cannot hold", .{k});
        return k;
    }
};

/// A two's complement big-endian integer as a decimal at `scale`; one with more
/// digits than `precision` is corrupt.
pub fn decimalOf(b: []const u8, precision: u8, scale: u8) Error!Value {
    if (b.len == 0) return .{ .decimal = .{ .unscaled = 0, .scale = scale } };
    const neg = b[0] & 0x80 != 0;
    var start: usize = 0;
    if (b.len > 16) {
        start = b.len - 16;
        for (b[0..start]) |x| if (x != (if (neg) @as(u8, 0xFF) else 0)) return corrupt("a decimal wider than its precision", .{});
        if ((b[start] & 0x80 != 0) != neg) return corrupt("a decimal wider than its precision", .{});
    }
    var acc: u128 = if (neg) std.math.maxInt(u128) else 0;
    for (b[start..]) |x| acc = (acc << 8) | x;
    const v: i128 = @bitCast(acc);
    const bound = pow10_i128[precision];
    if (v >= bound or v <= -bound) return corrupt("a decimal holds more digits than its precision {d}", .{precision});
    return .{ .decimal = .{ .unscaled = v, .scale = scale } };
}

const Route = struct { to: ?usize = null, cast: bool = false };

pub const Reader = struct {
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    in: *std.Io.Reader,
    opts: Options,
    header: Header,
    root: *const Node,
    step: schema_mod.Step,
    own: types.Schema,
    schema: types.Schema,
    routes: []Route,
    absent: []usize,
    raw: std.ArrayListUnmanaged(u8) = .empty,
    dec: std.Io.Writer.Allocating,
    text: std.Io.Writer.Allocating,
    blk: std.heap.ArenaAllocator,
    cur: Cursor = .{},
    left: u64 = 0,
    builders: []column.Builder = &.{},
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, in: *std.Io.Reader, opts: Options) Error!*Reader {
        const header = try readHeader(arena, in);
        const root = try schema_mod.parse(arena, header.schema_text);
        const p = try schema_mod.plan(arena, root);
        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .gpa = gpa,
            .in = in,
            .opts = opts,
            .header = header,
            .root = root,
            .step = p.step,
            .own = p.schema,
            .schema = opts.schema orelse p.schema,
            .routes = try arena.alloc(Route, p.schema.fields.len),
            .absent = &.{},
            .dec = .init(gpa),
            .text = .init(gpa),
            .blk = .init(gpa),
        };
        errdefer self.close();
        try self.route();
        return self;
    }

    /// Each of this file's columns to the one of the table it fills, by name.
    fn route(self: *Reader) Error!void {
        const out = self.schema;
        var covered = try self.arena.alloc(bool, out.fields.len);
        @memset(covered, false);
        for (self.own.fields, self.routes) |f, *r| {
            const to = out.indexOf(f.name) orelse {
                r.* = .{};
                continue;
            };
            const want = out.fields[to].ty;
            if (!types.Type.eql(f.ty, want) and !types.Type.canWiden(f.ty.kind, want.kind))
                return unsupported("`{s}`: column `{s}` is {s} here but {s} in the first file", .{ self.opts.source_name, f.name, try f.ty.name(self.arena), try want.name(self.arena) });
            r.* = .{ .to = to, .cast = !types.Type.eql(f.ty, want) };
            covered[to] = true;
        }
        var absent = std.ArrayListUnmanaged(usize).empty;
        for (covered, 0..) |c, i| if (!c) try absent.append(self.arena, i);
        self.absent = absent.items;
    }

    pub fn next(self: *Reader, arena: std.mem.Allocator) Error!?Batch {
        if (self.done) return null;
        const fields = self.schema.fields;
        self.builders = try arena.alloc(column.Builder, fields.len);
        for (self.builders, fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, self.opts.batch_rows);
        var n: usize = 0;
        while (n < self.opts.batch_rows) {
            if (self.left == 0) {
                if (!try self.nextBlock()) {
                    self.done = true;
                    break;
                }
                continue;
            }
            try self.run(arena, self.step);
            for (self.absent) |i| try self.builders[i].append(.null);
            self.left -= 1;
            n += 1;
            if (self.left == 0 and self.cur.pos != self.cur.buf.len)
                return corrupt("a block holds {d} bytes past its last object", .{self.cur.buf.len - self.cur.pos});
        }
        if (n == 0) return null;
        const cols = try arena.alloc(column.Column, fields.len);
        for (self.builders, cols) |*b, *c| c.* = try b.finish();
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
        self.cur = .{ .buf = try self.decompress(self.raw.items) };
        self.left = @intCast(count);
        if (count == 0 and self.cur.buf.len != 0) return corrupt("a block of no objects holds {d} bytes", .{self.cur.buf.len});
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

    fn emit(self: *Reader, arena: std.mem.Allocator, col: usize, v: Value) Error!void {
        const r = self.routes[col];
        const to = r.to orelse return;
        const val = if (r.cast and v != .null) eval.castValueTyped(arena, v, self.schema.fields[to].ty) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return unsupported("`{s}`: a value of column `{s}` does not fit the first file's {s}", .{ self.opts.source_name, self.own.fields[col].name, try self.schema.fields[to].ty.name(self.arena) }),
        } else v;
        try self.builders[to].append(val);
    }

    fn run(self: *Reader, arena: std.mem.Allocator, step: schema_mod.Step) Error!void {
        switch (step) {
            .leaf => |t| try self.emit(arena, t.col, try self.value(t.node)),
            .text => |t| {
                self.text.clearRetainingCapacity();
                try self.json(arena, t.node, &self.text.writer);
                try self.emit(arena, t.col, .{ .string = self.text.written() });
            },
            .record => |steps| for (steps) |s| try self.run(arena, s),
            .choice => |c| {
                const i = try self.cur.long();
                if (i < 0 or i >= c.branches.len) return corrupt("a union branch {d} of {d}", .{ i, c.branches.len });
                switch (c.branches[@intCast(i)]) {
                    .nulls => for (c.lo..c.hi) |col| try self.emit(arena, col, .null),
                    else => |b| try self.run(arena, b),
                }
            },
            .nulls => unreachable,
        }
    }

    fn value(self: *Reader, n: *const Node) Error!Value {
        const c = &self.cur;
        return switch (n.kind) {
            .null => .null,
            .boolean => .{ .bool = try c.boolean() },
            .int => blk: {
                const x = try c.int();
                break :blk switch (n.logical) {
                    .date => .{ .date = x },
                    .time_millis => .{ .time = @as(i64, x) * 1000 },
                    else => .{ .int = x },
                };
            },
            .long => blk: {
                const x = try c.long();
                break :blk switch (n.logical) {
                    .time_micros => .{ .time = x },
                    .timestamp_millis => .{ .timestamp = std.math.mul(i64, x, 1000) catch return corrupt("a timestamp-millis {d} past the range basalt holds", .{x}) },
                    .timestamp_micros => .{ .timestamp = x },
                    .timestamp_nanos => .{ .timestamp = @divFloor(x, 1000) },
                    else => .{ .int = x },
                };
            },
            .float => .{ .float = try c.float() },
            .double => .{ .float = try c.double() },
            .string => .{ .string = try c.bytes() },
            .@"enum" => blk: {
                const i = try c.long();
                if (i < 0 or i >= n.symbols.len) return corrupt("an enum symbol {d} of {d}", .{ i, n.symbols.len });
                break :blk .{ .string = n.symbols[@intCast(i)] };
            },
            .bytes, .fixed => blk: {
                const b = if (n.kind == .bytes) try c.bytes() else try c.take(n.size);
                break :blk if (n.logical == .decimal) try decimalOf(b, n.precision, n.scale) else .{ .bytes = b };
            },
            .record, .array, .map, .@"union" => unreachable,
        };
    }

    /// The value at the cursor as compact JSON, as basalt's JSON writer writes one.
    fn json(self: *Reader, arena: std.mem.Allocator, n: *const Node, w: *std.Io.Writer) Error!void {
        return self.jsonAt(arena, n, w, 0);
    }

    fn jsonAt(self: *Reader, arena: std.mem.Allocator, n: *const Node, w: *std.Io.Writer, depth: usize) Error!void {
        if (depth > schema_mod.max_depth) return corrupt("a value nests deeper than {d}", .{schema_mod.max_depth});
        const c = &self.cur;
        switch (n.kind) {
            .record => {
                try put(w, "{");
                for (n.fields, 0..) |f, i| {
                    if (i > 0) try put(w, ",");
                    try quoted(w, f.name);
                    try put(w, ":");
                    try self.jsonAt(arena, f.node, w, depth + 1);
                }
                try put(w, "}");
            },
            .array, .map => {
                try put(w, if (n.kind == .array) "[" else "{");
                var first = true;
                while (true) {
                    const k = try c.blockCount(n.items.?);
                    if (k == 0) break;
                    for (0..k) |_| {
                        if (!first) try put(w, ",");
                        first = false;
                        if (n.kind == .map) {
                            try quoted(w, try c.bytes());
                            try put(w, ":");
                        }
                        try self.jsonAt(arena, n.items.?, w, depth + 1);
                    }
                }
                try put(w, if (n.kind == .array) "]" else "}");
            },
            .@"union" => {
                const i = try c.long();
                if (i < 0 or i >= n.branches.len) return corrupt("a union branch {d} of {d}", .{ i, n.branches.len });
                try self.jsonAt(arena, n.branches[@intCast(i)], w, depth + 1);
            },
            else => switch (try self.value(n)) {
                .null => try put(w, "null"),
                .bool => |b| try put(w, if (b) "true" else "false"),
                .int => |x| w.print("{d}", .{x}) catch return error.OutOfMemory,
                .float => |x| if (std.math.isFinite(x)) (w.print("{d}", .{x}) catch return error.OutOfMemory) else try put(w, "null"),
                .string => |s| try quoted(w, s),
                .bytes => |s| {
                    try put(w, "\"");
                    std.base64.standard.Encoder.encodeWriter(w, s) catch return error.OutOfMemory;
                    try put(w, "\"");
                },
                else => |v| {
                    try put(w, "\"");
                    try put(w, eval.valueToString(arena, v) catch return error.OutOfMemory);
                    try put(w, "\"");
                },
            },
        }
    }

    pub fn close(self: *Reader) void {
        self.raw.deinit(self.gpa);
        self.dec.deinit();
        self.text.deinit();
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

fn put(w: *std.Io.Writer, s: []const u8) Error!void {
    w.writeAll(s) catch return error.OutOfMemory;
}

fn quoted(w: *std.Io.Writer, s: []const u8) Error!void {
    try put(w, "\"");
    jsonwrite.writeEscaped(w, s) catch return error.OutOfMemory;
    try put(w, "\"");
}

/// Avro files read as one table (`files.Files`).
pub const Files = @import("../files.zig").Files(Reader, Options);

const testing = std.testing;

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
