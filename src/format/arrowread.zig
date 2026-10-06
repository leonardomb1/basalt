//! Arrow IPC reader — `FROM 'x.arrow'` / `.feather` / `.ipc` / `.arrows`.
//!
//! A stream is read in order: a schema, then dictionary and record batches as
//! they come. A file (Feather v2: `ARROW1` magic, messages, footer) is read
//! through its footer, which holds the schema and the offset of every block —
//! writers do not agree on what sits between the magic and the first batch, and
//! polars puts its dictionaries after the batches that use them, so a file's
//! dictionaries are all loaded up front and kept for the reader's life.
//!
//! The file is memory-mapped and each record batch is copied out of the mapping
//! into the batch arena, decompressing LZ4-frame or ZSTD bodies as they are
//! met. The copy is the whole cost for the common columns — a fixed-width or
//! `utf8` buffer is already the engine's own layout — which is the point of the
//! format: handing a dataframe to basalt as IPC is close to a memcpy, where
//! Parquet is an encode and a decode. A record batch is decompressed and checked
//! once, then handed out in windows of `window_rows` (a multiple of eight, so
//! validity bits copy as bytes): a writer that puts a whole frame in one batch
//! (polars does) would otherwise be converted and held whole. Field names are
//! duped, since the file is unmapped on close and a schema can outlive its reader.
//!
//! The file is untrusted. `flatbuf.Table` trusts its input (it only reads what
//! this program wrote), so `Fb` checks every offset before following it, and a bad
//! one is `CorruptArrow`, not a trap. A compressed buffer's declared length may be
//! at most 1024 times its frame (plus 1 MiB), so a hostile length cannot become a
//! huge allocation before a byte is decoded.
//!
//! Types map as the engine can hold them: every integer width to `int` (a
//! UInt64 above 2^63-1 is an error, never a wrap), floats to `float`, the
//! three string layouts (`utf8`, `large_utf8`, `utf8_view`) to `string` and the
//! binary ones to `bytes`, `decimal128` to `decimal`, dates, times and
//! timestamps in any unit to basalt's day / microsecond types (a zoned
//! timestamp reads as its UTC wall-clock, as Parquet's does), a duration to its
//! count of microseconds as an `int` (the engine has no interval type),
//! dictionary encodings to their values, and the nested types — lists, structs,
//! maps — to JSON text, so `json_get` and `UNNEST(JSON_EACH(...))` reach inside
//! them. Every column is nullable: the bitmap is the truth. A null-typed column is
//! an all-null `string`. Unions, run-end and list-view encodings are refused by
//! name in `Reader.why`.
//!
//! A projection is a hint, as for parquet: an unknown name is ignored, and one
//! that names none of the columns still reads rows. Only the first stream of a
//! file is read: a `basalt run --format arrow` capture of several results holds
//! several, and which one a query meant is not something to guess.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const Batch = @import("../exec/batch.zig").Batch;
const Value = @import("../exec/value.zig").Value;
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const codec = @import("codec.zig");

pub const Error = error{
    NotArrow,
    CorruptArrow,
    UnsupportedArrow,
    ArrowIntOverflow,
} || codec.Error || std.mem.Allocator.Error;

const magic = "ARROW1";

pub fn isPath(path: []const u8) bool {
    inline for (.{ ".arrow", ".arrows", ".feather", ".ipc" }) |ext| {
        if (std.ascii.endsWithIgnoreCase(path, ext)) return true;
    }
    return false;
}

const Fb = struct {
    buf: []const u8,
    pos: usize,

    fn rd(comptime T: type, buf: []const u8, at: usize) Error!T {
        if (at > buf.len or buf.len - at < @sizeOf(T)) return Error.CorruptArrow;
        return std.mem.readInt(T, buf[at..][0..@sizeOf(T)], .little);
    }

    fn root(buf: []const u8) Error!Fb {
        const p = try rd(u32, buf, 0);
        if (p >= buf.len) return Error.CorruptArrow;
        return .{ .buf = buf, .pos = p };
    }

    fn fieldPos(self: Fb, slot: usize) Error!?usize {
        const soff = try rd(i32, self.buf, self.pos);
        const vt_i: i64 = @as(i64, @intCast(self.pos)) - soff;
        if (vt_i < 0 or vt_i >= self.buf.len) return Error.CorruptArrow;
        const vt: usize = @intCast(vt_i);
        const vsize = try rd(u16, self.buf, vt);
        const fo = 4 + slot * 2;
        if (fo + 2 > vsize) return null;
        const o = try rd(u16, self.buf, vt + fo);
        if (o == 0) return null;
        return self.pos + o;
    }

    fn int(self: Fb, comptime T: type, slot: usize, default: T) Error!T {
        const p = (try self.fieldPos(slot)) orelse return default;
        return rd(T, self.buf, p);
    }

    fn target(self: Fb, slot: usize) Error!?usize {
        const p = (try self.fieldPos(slot)) orelse return null;
        const t = p + try rd(u32, self.buf, p);
        if (t >= self.buf.len) return Error.CorruptArrow;
        return t;
    }

    fn table(self: Fb, slot: usize) Error!?Fb {
        const t = (try self.target(slot)) orelse return null;
        return .{ .buf = self.buf, .pos = t };
    }

    fn string(self: Fb, slot: usize) Error!?[]const u8 {
        const t = (try self.target(slot)) orelse return null;
        const n = try rd(u32, self.buf, t);
        if (n > self.buf.len - t - 4) return Error.CorruptArrow;
        return self.buf[t + 4 ..][0..n];
    }

    const Vec = struct { len: usize, at: usize };

    fn vector(self: Fb, slot: usize, elem: usize) Error!?Vec {
        const t = (try self.target(slot)) orelse return null;
        const n = try rd(u32, self.buf, t);
        if (n > (self.buf.len - t - 4) / elem) return Error.CorruptArrow;
        return .{ .len = n, .at = t + 4 };
    }

    fn tableAt(self: Fb, v: Vec, i: usize) Error!Fb {
        const p = v.at + i * 4;
        const t = p + try rd(u32, self.buf, p);
        if (t >= self.buf.len) return Error.CorruptArrow;
        return .{ .buf = self.buf, .pos = t };
    }
};

const Kind = enum {
    null_,
    int,
    float,
    bool_,
    decimal,
    date_day,
    date_ms,
    time,
    timestamp,
    duration,
    utf8,
    large_utf8,
    utf8_view,
    binary,
    large_binary,
    binary_view,
    fixed_binary,
    list,
    large_list,
    fixed_list,
    struct_,
    map,
};

const Field = struct {
    name: []const u8,
    kind: Kind,
    bit_width: u16 = 0,
    signed: bool = true,
    unit: Unit = .us,
    precision: u8 = 0,
    scale: u8 = 0,
    fixed_size: usize = 0,
    dict: ?Dict = null,
    children: []Field = &.{},

    const Dict = struct { id: i64, index_bits: u16, index_signed: bool };

    fn basaltType(self: Field) types.Type {
        const t: types.Type = switch (self.kind) {
            .int, .duration => types.Type.init(.int),
            .float => types.Type.init(.float),
            .bool_ => types.Type.init(.bool),
            .decimal => types.Type.decimal(self.precision, self.scale),
            .date_day, .date_ms => types.Type.init(.date),
            .time => types.Type.init(.time),
            .timestamp => types.Type.init(.timestamp),
            .utf8, .large_utf8, .utf8_view => types.Type.init(.string),
            .binary, .large_binary, .binary_view, .fixed_binary => types.Type.init(.bytes),
            .null_, .list, .large_list, .fixed_list, .struct_, .map => types.Type.init(.string),
        };
        return t.asNullable();
    }
};

const Unit = enum {
    s,
    ms,
    us,
    ns,

    fn toMicros(self: Unit, x: i64) i64 {
        return switch (self) {
            .s => std.math.mul(i64, x, 1_000_000) catch if (x < 0) std.math.minInt(i64) else std.math.maxInt(i64),
            .ms => std.math.mul(i64, x, 1_000) catch if (x < 0) std.math.minInt(i64) else std.math.maxInt(i64),
            .us => x,
            .ns => @divFloor(x, 1000),
        };
    }

    fn of(v: i16) Error!Unit {
        return switch (v) {
            0 => .s,
            1 => .ms,
            2 => .us,
            3 => .ns,
            else => Error.CorruptArrow,
        };
    }
};

const TypeTag = enum(u8) {
    null_ = 1,
    int = 2,
    floating_point = 3,
    binary = 4,
    utf8 = 5,
    bool_ = 6,
    decimal = 7,
    date = 8,
    time = 9,
    timestamp = 10,
    interval = 11,
    list = 12,
    struct_ = 13,
    union_ = 14,
    fixed_size_binary = 15,
    fixed_size_list = 16,
    map = 17,
    duration = 18,
    large_binary = 19,
    large_utf8 = 20,
    large_list = 21,
    run_end_encoded = 22,
    binary_view = 23,
    utf8_view = 24,
    list_view = 25,
    large_list_view = 26,
    _,
};

fn parseField(arena: std.mem.Allocator, f: Fb, why: *[]const u8) Error!Field {
    var out = Field{ .name = try arena.dupe(u8, (try f.string(0)) orelse ""), .kind = .null_ };
    const tag: TypeTag = @enumFromInt(try f.int(u8, 2, 0));
    const t = try f.table(3);
    switch (tag) {
        .null_ => out.kind = .null_,
        .int => {
            const tt = t orelse return Error.CorruptArrow;
            out.kind = .int;
            out.bit_width = @intCast(try tt.int(i32, 0, 0));
            out.signed = (try tt.int(u8, 1, 0)) != 0;
            switch (out.bit_width) {
                8, 16, 32, 64 => {},
                else => return Error.CorruptArrow,
            }
        },
        .floating_point => {
            const p = if (t) |tt| try tt.int(i16, 0, 0) else 0;
            out.kind = .float;
            out.bit_width = switch (p) {
                1 => 32,
                2 => 64,
                else => {
                    why.* = try std.fmt.allocPrint(arena, "column `{s}`: half-precision floats are not supported", .{out.name});
                    return Error.UnsupportedArrow;
                },
            };
        },
        .bool_ => out.kind = .bool_,
        .decimal => {
            const tt = t orelse return Error.CorruptArrow;
            const prec = try tt.int(i32, 0, 0);
            const scale = try tt.int(i32, 1, 0);
            const bw = try tt.int(i32, 2, 128);
            if (bw != 128 or prec < 1 or prec > 38 or scale < 0 or scale > prec) {
                why.* = try std.fmt.allocPrint(arena, "column `{s}`: decimal({d},{d}) at {d} bits does not fit the engine's 38-digit decimal", .{ out.name, prec, scale, bw });
                return Error.UnsupportedArrow;
            }
            out.kind = .decimal;
            out.precision = @intCast(prec);
            out.scale = @intCast(scale);
        },
        .date => {
            const u = if (t) |tt| try tt.int(i16, 0, 1) else 1;
            out.kind = if (u == 0) .date_day else .date_ms;
        },
        .time => {
            const tt = t orelse return Error.CorruptArrow;
            out.kind = .time;
            out.unit = try Unit.of(try tt.int(i16, 0, 1));
            out.bit_width = @intCast(try tt.int(i32, 1, 32));
            if (out.bit_width != 32 and out.bit_width != 64) return Error.CorruptArrow;
        },
        .timestamp => {
            const tt = t orelse return Error.CorruptArrow;
            out.kind = .timestamp;
            out.unit = try Unit.of(try tt.int(i16, 0, 0));
        },
        .duration => {
            out.kind = .duration;
            out.unit = if (t) |tt| try Unit.of(try tt.int(i16, 0, 1)) else .ms;
        },
        .utf8 => out.kind = .utf8,
        .large_utf8 => out.kind = .large_utf8,
        .utf8_view => out.kind = .utf8_view,
        .binary => out.kind = .binary,
        .large_binary => out.kind = .large_binary,
        .binary_view => out.kind = .binary_view,
        .fixed_size_binary => {
            const tt = t orelse return Error.CorruptArrow;
            const w = try tt.int(i32, 0, 0);
            if (w < 0) return Error.CorruptArrow;
            out.kind = .fixed_binary;
            out.fixed_size = @intCast(w);
        },
        .list => out.kind = .list,
        .large_list => out.kind = .large_list,
        .fixed_size_list => {
            const tt = t orelse return Error.CorruptArrow;
            const w = try tt.int(i32, 0, 0);
            if (w < 0) return Error.CorruptArrow;
            out.kind = .fixed_list;
            out.fixed_size = @intCast(w);
        },
        .struct_ => out.kind = .struct_,
        .map => out.kind = .map,
        else => {
            why.* = try std.fmt.allocPrint(arena, "column `{s}`: the Arrow {s} type is not supported", .{ out.name, tagName(tag) });
            return Error.UnsupportedArrow;
        },
    }
    if (try f.table(4)) |d| {
        const idx = try d.table(1);
        out.dict = .{
            .id = try d.int(i64, 0, 0),
            .index_bits = if (idx) |it| @intCast(try it.int(i32, 0, 32)) else 32,
            .index_signed = if (idx) |it| (try it.int(u8, 1, 0)) != 0 else true,
        };
        switch (out.dict.?.index_bits) {
            8, 16, 32, 64 => {},
            else => return Error.CorruptArrow,
        }
    }
    if (try f.vector(5, 4)) |kids| {
        out.children = try arena.alloc(Field, kids.len);
        for (out.children, 0..) |*c, i| c.* = try parseField(arena, try f.tableAt(kids, i), why);
    }
    const want_children: ?usize = switch (out.kind) {
        .list, .large_list, .fixed_list, .map => 1,
        else => null,
    };
    if (want_children) |n| if (out.children.len != n) return Error.CorruptArrow;
    return out;
}

fn tagName(t: TypeTag) []const u8 {
    return switch (t) {
        .interval => "Interval",
        .union_ => "Union",
        .run_end_encoded => "RunEndEncoded",
        .list_view => "ListView",
        .large_list_view => "LargeListView",
        else => "unknown",
    };
}

const Buf = struct {
    raw: []const u8,
    compressed: bool,
};

const Arr = struct {
    len: usize,
    null_count: usize,
    bufs: []Buf,
    variadic: []Buf = &.{},
    children: []Arr = &.{},
};

const Walk = struct {
    arena: std.mem.Allocator,
    body: []const u8,
    compressed: ?Codec,
    nodes: Fb.Vec,
    bufs: Fb.Vec,
    variadic_counts: ?Fb.Vec,
    meta: []const u8,
    ni: usize = 0,
    bi: usize = 0,
    vi: usize = 0,

    fn node(self: *Walk) Error!struct { len: usize, nulls: usize } {
        if (self.ni >= self.nodes.len) return Error.CorruptArrow;
        const at = self.nodes.at + self.ni * 16;
        self.ni += 1;
        const len = try Fb.rd(i64, self.meta, at);
        const nulls = try Fb.rd(i64, self.meta, at + 8);
        if (len < 0 or nulls < 0 or nulls > len) return Error.CorruptArrow;
        return .{ .len = @intCast(len), .nulls = @intCast(nulls) };
    }

    fn buf(self: *Walk) Error!Buf {
        if (self.bi >= self.bufs.len) return Error.CorruptArrow;
        const at = self.bufs.at + self.bi * 16;
        self.bi += 1;
        const off = try Fb.rd(i64, self.meta, at);
        const len = try Fb.rd(i64, self.meta, at + 8);
        if (off < 0 or len < 0 or off > self.body.len or len > self.body.len - @as(usize, @intCast(off)))
            return Error.CorruptArrow;
        return .{ .raw = self.body[@intCast(off)..][0..@intCast(len)], .compressed = self.compressed != null };
    }

    fn array(self: *Walk, f: Field) Error!Arr {
        const n = try self.node();
        var a = Arr{ .len = n.len, .null_count = n.nulls, .bufs = &.{} };
        if (f.dict != null) {
            a.bufs = try self.take(2);
            return a;
        }
        switch (f.kind) {
            .null_ => {},
            .int, .float, .bool_, .decimal, .date_day, .date_ms, .time, .timestamp, .duration, .fixed_binary => a.bufs = try self.take(2),
            .utf8, .large_utf8, .binary, .large_binary => a.bufs = try self.take(3),
            .utf8_view, .binary_view => {
                a.bufs = try self.take(2);
                const counts = self.variadic_counts orelse return Error.CorruptArrow;
                if (self.vi >= counts.len) return Error.CorruptArrow;
                const nv = try Fb.rd(i64, self.meta, counts.at + self.vi * 8);
                self.vi += 1;
                if (nv < 0 or nv > self.bufs.len) return Error.CorruptArrow;
                a.variadic = try self.take(@intCast(nv));
            },
            .list, .large_list, .map => {
                a.bufs = try self.take(2);
                a.children = try self.arena.alloc(Arr, 1);
                a.children[0] = try self.array(f.children[0]);
            },
            .fixed_list => {
                a.bufs = try self.take(1);
                a.children = try self.arena.alloc(Arr, 1);
                a.children[0] = try self.array(f.children[0]);
            },
            .struct_ => {
                a.bufs = try self.take(1);
                a.children = try self.arena.alloc(Arr, f.children.len);
                for (a.children, f.children) |*c, cf| c.* = try self.array(cf);
            },
        }
        return a;
    }

    fn take(self: *Walk, n: usize) Error![]Buf {
        const out = try self.arena.alloc(Buf, n);
        for (out) |*b| b.* = try self.buf();
        return out;
    }
};

const Codec = enum { lz4_frame, zstd };

/// A buffer's bytes, decompressed if the batch was: an i64 uncompressed length,
/// then the codec's frame, or the bytes as they are when the length is -1.
fn bytesOf(arena: std.mem.Allocator, b: Buf, c: ?Codec) Error![]const u8 {
    if (!b.compressed or b.raw.len == 0) return b.raw;
    if (b.raw.len < 8) return Error.CorruptArrow;
    const ulen = std.mem.readInt(i64, b.raw[0..8], .little);
    const rest = b.raw[8..];
    if (ulen == -1) return rest;
    if (ulen < 0) return Error.CorruptArrow;
    if (ulen > @as(i64, @intCast(rest.len)) * 1024 + (1 << 20)) return Error.CorruptArrow;
    const out = try arena.alloc(u8, @intCast(ulen));
    switch (c.?) {
        .lz4_frame => try codec.lz4Frame(rest, out),
        .zstd => try codec.zstdInto(arena, rest, out),
    }
    return out;
}

pub const Reader = struct {
    arena: std.mem.Allocator,
    map: []align(std.heap.page_size_min) const u8,
    data: []const u8 = &.{},
    end: usize,
    pos: usize,
    blocks: ?[]const Block = null,
    block_i: usize = 0,
    fields: []Field,
    keep: []usize,
    schema: types.Schema,
    dicts: std.AutoHashMap(i64, column.Column),
    why: []const u8 = "",
    done: bool = false,
    rb_arena: std.heap.ArenaAllocator = std.heap.ArenaAllocator.init(std.heap.page_allocator),
    cursors: []*Cursor = &.{},
    rb_rows: usize = 0,
    rb_off: usize = 0,

    const window_rows = 64 * 1024;

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Reader {
        return openProjected(arena, path, null);
    }

    /// `open`, converting only the named columns.
    pub fn openProjected(arena: std.mem.Allocator, path: []const u8, want: ?[]const []const u8) !*Reader {
        if (std.mem.indexOf(u8, path, "://") != null) return Error.UnsupportedArrow;
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();
        const size = (try file.stat()).size;
        if (size < 8) return Error.NotArrow;
        const map = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(map);
        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .map = map,
            .end = map.len,
            .pos = 0,
            .fields = &.{},
            .keep = &.{},
            .schema = .{ .fields = &.{} },
            .dicts = std.AutoHashMap(i64, column.Column).init(arena),
        };
        errdefer self.dicts.deinit();
        try self.init(want);
        return self;
    }

    /// Open over bytes already in memory, which must outlive the reader.
    pub fn openBytes(arena: std.mem.Allocator, bytes: []const u8, want: ?[]const []const u8) !*Reader {
        const self = try arena.create(Reader);
        self.* = .{
            .arena = arena,
            .map = &.{},
            .end = bytes.len,
            .pos = 0,
            .fields = &.{},
            .keep = &.{},
            .schema = .{ .fields = &.{} },
            .dicts = std.AutoHashMap(i64, column.Column).init(arena),
        };
        try self.initOver(bytes, want);
        return self;
    }

    fn init(self: *Reader, want: ?[]const []const u8) !void {
        return self.initOver(self.map, want);
    }

    fn initOver(self: *Reader, data: []const u8, want: ?[]const []const u8) !void {
        self.data = data;
        var sch: Fb = undefined;
        var dict_blocks: []const Block = &.{};
        if (data.len >= 8 and std.mem.eql(u8, data[0..6], magic)) {
            if (data.len < 8 + 10 or !std.mem.eql(u8, data[data.len - 6 ..], magic)) return Error.CorruptArrow;
            const flen = std.mem.readInt(i32, data[data.len - 10 ..][0..4], .little);
            if (flen < 0 or @as(usize, @intCast(flen)) > data.len - 18) return Error.CorruptArrow;
            const fstart = data.len - 10 - @as(usize, @intCast(flen));
            self.end = fstart;
            const footer = try Fb.root(data[fstart..][0..@intCast(flen)]);
            sch = (try footer.table(1)) orelse return Error.CorruptArrow;
            dict_blocks = try blocksOf(self.arena, footer, 2, fstart);
            self.blocks = try blocksOf(self.arena, footer, 3, fstart);
        } else if (std.mem.readInt(u32, data[0..4], .little) == 0xFFFFFFFF) {
            self.pos = 0;
            self.end = data.len;
            const first = (try self.nextMessage()) orelse return Error.CorruptArrow;
            if (first.header != 1) return Error.CorruptArrow;
            sch = first.table;
        } else return Error.NotArrow;

        if ((try sch.int(i16, 0, 0)) != 0) {
            self.why = "big-endian Arrow data is not supported";
            return Error.UnsupportedArrow;
        }
        const fv = (try sch.vector(1, 4)) orelse return Error.CorruptArrow;
        self.fields = try self.arena.alloc(Field, fv.len);
        for (self.fields, 0..) |*f, i| f.* = try parseField(self.arena, try sch.tableAt(fv, i), &self.why);

        var keep = std.array_list.Managed(usize).init(self.arena);
        for (self.fields, 0..) |f, i| {
            if (want) |w| {
                var hit = false;
                for (w) |n| if (std.mem.eql(u8, n, f.name)) {
                    hit = true;
                };
                if (!hit) continue;
            }
            try keep.append(i);
        }
        if (keep.items.len == 0 and self.fields.len > 0) try keep.append(0);
        self.keep = try keep.toOwnedSlice();
        const out = try self.arena.alloc(types.Schema.Field, self.keep.len);
        for (out, self.keep) |*o, i| o.* = .{ .name = self.fields[i].name, .ty = self.fields[i].basaltType() };
        self.schema = .{ .fields = out };

        for (dict_blocks) |b| {
            const m = try self.messageAt(b);
            if (m.header != 2) return Error.CorruptArrow;
            try self.readDictionary(m);
        }
    }

    const Block = struct { offset: usize, meta_len: usize, body_len: usize };

    fn blocksOf(arena: std.mem.Allocator, footer: Fb, slot: usize, limit: usize) Error![]const Block {
        const v = (try footer.vector(slot, 24)) orelse return &.{};
        const out = try arena.alloc(Block, v.len);
        for (out, 0..) |*b, i| {
            const at = v.at + i * 24;
            const off = try Fb.rd(i64, footer.buf, at);
            const ml = try Fb.rd(i32, footer.buf, at + 8);
            const bl = try Fb.rd(i64, footer.buf, at + 16);
            if (off < 8 or ml < 8 or bl < 0) return Error.CorruptArrow;
            const o: usize = @intCast(off);
            if (o > limit or @as(usize, @intCast(ml)) > limit - o or @as(usize, @intCast(bl)) > limit - o - @as(usize, @intCast(ml)))
                return Error.CorruptArrow;
            b.* = .{ .offset = o, .meta_len = @intCast(ml), .body_len = @intCast(bl) };
        }
        return out;
    }

    fn messageAt(self: *Reader, b: Block) Error!Message {
        const data = self.data;
        var at = b.offset;
        var len = std.mem.readInt(u32, data[at..][0..4], .little);
        at += 4;
        if (len == 0xFFFFFFFF) {
            len = std.mem.readInt(u32, data[at..][0..4], .little);
            at += 4;
        }
        if (len > b.offset + b.meta_len - at) return Error.CorruptArrow;
        const meta = data[at..][0..len];
        const msg = try Fb.root(meta);
        const body_at = b.offset + b.meta_len;
        return .{
            .header = try msg.int(u8, 1, 0),
            .table = (try msg.table(2)) orelse return Error.CorruptArrow,
            .body = data[body_at..][0..b.body_len],
            .meta = meta,
        };
    }

    const Message = struct { header: u8, table: Fb, body: []const u8, meta: []const u8 };

    fn nextMessage(self: *Reader) Error!?Message {
        const data = self.data;
        if (self.pos + 4 > self.end) return null;
        var len = std.mem.readInt(u32, data[self.pos..][0..4], .little);
        var at = self.pos + 4;
        if (len == 0xFFFFFFFF) {
            if (at + 4 > self.end) return Error.CorruptArrow;
            len = std.mem.readInt(u32, data[at..][0..4], .little);
            at += 4;
        }
        if (len == 0) return null;
        if (len > self.end - at) return Error.CorruptArrow;
        const meta = data[at..][0..len];
        const msg = try Fb.root(meta);
        const body_len = try msg.int(i64, 3, 0);
        const body_at = at + len;
        if (body_len < 0 or @as(usize, @intCast(body_len)) > self.end - body_at) return Error.CorruptArrow;
        self.pos = body_at + @as(usize, @intCast(body_len));
        return .{
            .header = try msg.int(u8, 1, 0),
            .table = (try msg.table(2)) orelse return Error.CorruptArrow,
            .body = data[body_at..][0..@intCast(body_len)],
            .meta = meta,
        };
    }

    pub fn next(self: *Reader, arena: std.mem.Allocator) !?Batch {
        while (self.rb_off >= self.rb_rows) {
            if (!try self.loadBatch()) return null;
        }
        const n = @min(window_rows, self.rb_rows - self.rb_off);
        const cols = try arena.alloc(column.Column, self.keep.len);
        for (cols, self.cursors) |*c, cur| c.* = try toColumn(arena, cur, self.rb_off, n);
        self.rb_off += n;
        return .{ .schema = &self.schema, .columns = cols, .len = n };
    }

    /// Move to the next record batch; false at the end. A second schema ends the
    /// stream; an empty batch (a writer's trailer) loads like any other and yields no window.
    fn loadBatch(self: *Reader) !bool {
        if (self.blocks) |blocks| {
            if (self.block_i >= blocks.len) return false;
            const m = try self.messageAt(blocks[self.block_i]);
            self.block_i += 1;
            if (m.header != 3) return Error.CorruptArrow;
            try self.openBatch(m.table, m.body, m.meta);
            return true;
        }
        while (!self.done) {
            const m = (try self.nextMessage()) orelse {
                self.done = true;
                break;
            };
            switch (m.header) {
                2 => try self.readDictionary(m),
                3 => {
                    try self.openBatch(m.table, m.body, m.meta);
                    return true;
                },
                1 => self.done = true,
                else => {},
            }
        }
        return false;
    }

    fn walker(arena: std.mem.Allocator, rb: Fb, body: []const u8, meta: []const u8) Error!Walk {
        var c: ?Codec = null;
        if (try rb.table(3)) |comp| {
            c = switch (try comp.int(i8, 0, 0)) {
                0 => .lz4_frame,
                1 => .zstd,
                else => return Error.CorruptArrow,
            };
            if ((try comp.int(i8, 1, 0)) != 0) return Error.CorruptArrow;
        }
        return .{
            .arena = arena,
            .body = body,
            .compressed = c,
            .nodes = (try rb.vector(1, 16)) orelse return Error.CorruptArrow,
            .bufs = (try rb.vector(2, 16)) orelse return Error.CorruptArrow,
            .variadic_counts = try rb.vector(4, 8),
            .meta = meta,
        };
    }

    fn openBatch(self: *Reader, rb: Fb, body: []const u8, meta: []const u8) !void {
        _ = self.rb_arena.reset(.retain_capacity);
        const a = self.rb_arena.allocator();
        const n = try rb.int(i64, 0, 0);
        if (n < 0) return Error.CorruptArrow;
        var w = try walker(a, rb, body, meta);
        const arrs = try a.alloc(Arr, self.fields.len);
        for (arrs, self.fields) |*arr, f| arr.* = try w.array(f);
        self.cursors = try a.alloc(*Cursor, self.keep.len);
        for (self.cursors, self.keep) |*cur, i| {
            if (arrs[i].len != n) return Error.CorruptArrow;
            cur.* = try self.cursor(a, self.fields[i], arrs[i], w.compressed);
        }
        self.rb_rows = @intCast(n);
        self.rb_off = 0;
    }

    fn readDictionary(self: *Reader, m: Message) !void {
        const id = try m.table.int(i64, 0, 0);
        if ((try m.table.int(u8, 2, 0)) != 0) {
            self.why = "delta dictionary batches are not supported";
            return Error.UnsupportedArrow;
        }
        const rb = (try m.table.table(1)) orelse return Error.CorruptArrow;
        var vf: ?Field = null;
        for (self.fields) |f| if (findDict(f, id)) |d| {
            vf = d;
            break;
        };
        var f = vf orelse return;
        f.dict = null;
        var w = try walker(self.arena, rb, m.body, m.meta);
        const a = try w.array(f);
        const cur = try self.cursor(self.arena, f, a, w.compressed);
        try self.dicts.put(id, try toColumn(self.arena, cur, 0, a.len));
    }

    fn findDict(f: Field, id: i64) ?Field {
        if (f.dict) |d| if (d.id == id) return f;
        for (f.children) |c| if (findDict(c, id)) |x| return x;
        return null;
    }

    pub fn close(self: *Reader) void {
        self.rb_arena.deinit();
        self.dicts.deinit();
        if (self.map.len > 0) std.posix.munmap(self.map);
    }

    pub fn source(self: *Reader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    /// Rows `off .. off + n` of a checked array as an engine column; `off` is a multiple of eight.
    fn toColumn(arena: std.mem.Allocator, cur: *const Cursor, off: usize, n: usize) !column.Column {
        const f = cur.f;
        const ty = f.basaltType();
        const fixed = cur.dict == null and switch (f.kind) {
            .int => f.bit_width == 64 and f.signed,
            .timestamp => f.unit == .us,
            .time => f.unit == .us and f.bit_width == 64,
            .float => f.bit_width == 64,
            .date_day, .utf8, .binary, .utf8_view, .binary_view, .large_utf8, .large_binary => true,
            else => false,
        };
        if (fixed) {
            std.debug.assert(off % 8 == 0);
            const valid: column.Bitmap = if (cur.a.null_count == 0 or cur.bits.len == 0)
                try column.Bitmap.initFull(arena, n)
            else
                .{ .bits = try arena.dupe(u8, cur.bits[off / 8 ..][0 .. (n + 7) / 8]), .len = n };
            const data: column.Column.Data = switch (f.kind) {
                .int, .timestamp, .time => .{ .i64 = try copyAs(i64, arena, cur.data[off * 8 ..], n) },
                .float => .{ .f64 = try copyAs(f64, arena, cur.data[off * 8 ..], n) },
                .date_day => .{ .i32 = try copyAs(i32, arena, cur.data[off * 4 ..], n) },
                .utf8, .binary => blk: {
                    const offs = try copyAs(i32, arena, cur.offs32[off * 4 ..], n + 1);
                    const base = offs[0];
                    for (offs) |*o| o.* -= base;
                    const lo: usize = @intCast(base);
                    break :blk .{ .bytes = .{ .offsets = offs, .values = try arena.dupe(u8, cur.data[lo..][0..@intCast(offs[n])]) } };
                },
                .utf8_view, .binary_view, .large_utf8, .large_binary => blk: {
                    var total: usize = 0;
                    for (off..off + n) |i| total += (try cur.slice(i)).len;
                    if (total > std.math.maxInt(i32)) return Error.UnsupportedArrow;
                    const vals = try arena.alloc(u8, total);
                    const offs = try arena.alloc(i32, n + 1);
                    var at: usize = 0;
                    offs[0] = 0;
                    for (off..off + n, 1..) |i, k| {
                        const v = try cur.slice(i);
                        @memcpy(vals[at..][0..v.len], v);
                        at += v.len;
                        offs[k] = @intCast(at);
                    }
                    break :blk .{ .bytes = .{ .offsets = offs, .values = vals } };
                },
                else => unreachable,
            };
            return .{ .ty = ty, .len = n, .validity = valid, .data = data };
        }
        var b = try column.Builder.initCapacity(arena, ty, n);
        for (off..off + n) |i| try b.append(try cur.value(arena, i));
        return b.finish();
    }

    fn cursor(self: *Reader, arena: std.mem.Allocator, f: Field, a: Arr, c: ?Codec) Error!*Cursor {
        const cur = try arena.create(Cursor);
        cur.* = .{ .f = f, .a = a, .reader = self };
        if (a.bufs.len > 0) cur.bits = try bytesOf(arena, a.bufs[0], c);
        if (f.kind != .null_ and a.null_count > 0 and cur.bits.len < (a.len + 7) / 8) return Error.CorruptArrow;
        if (f.dict) |d| {
            cur.data = try bytesOf(arena, a.bufs[1], c);
            if (cur.data.len < a.len * (d.index_bits / 8)) return Error.CorruptArrow;
            cur.dict = self.dicts.getPtr(d.id) orelse return Error.CorruptArrow;
            return cur;
        }
        switch (f.kind) {
            .null_ => {},
            .int, .float, .bool_, .decimal, .date_day, .date_ms, .time, .timestamp, .duration, .fixed_binary => {
                cur.data = try bytesOf(arena, a.bufs[1], c);
                const need: usize = switch (f.kind) {
                    .bool_ => (a.len + 7) / 8,
                    .decimal => a.len * 16,
                    .fixed_binary => a.len * f.fixed_size,
                    .date_day => a.len * 4,
                    .date_ms, .timestamp, .duration => a.len * 8,
                    else => a.len * (f.bit_width / 8),
                };
                if (cur.data.len < need) return Error.CorruptArrow;
            },
            .utf8, .binary, .list, .map => {
                const offs = try bytesOf(arena, a.bufs[1], c);
                if (a.len > 0 and offs.len < (a.len + 1) * 4) return Error.CorruptArrow;
                cur.offs32 = offs;
                if (f.kind == .utf8 or f.kind == .binary) cur.data = try bytesOf(arena, a.bufs[2], c);
                const limit = if (f.kind == .utf8 or f.kind == .binary) cur.data.len else a.children[0].len;
                for (0..if (a.len > 0) a.len + 1 else 0) |i| {
                    const o = std.mem.readInt(i32, offs[i * 4 ..][0..4], .little);
                    if (o < 0 or o > limit or (i > 0 and o < std.mem.readInt(i32, offs[(i - 1) * 4 ..][0..4], .little))) return Error.CorruptArrow;
                }
            },
            .large_utf8, .large_binary, .large_list => {
                const offs = try bytesOf(arena, a.bufs[1], c);
                if (a.len > 0 and offs.len < (a.len + 1) * 8) return Error.CorruptArrow;
                cur.offs64 = offs;
                if (f.kind != .large_list) cur.data = try bytesOf(arena, a.bufs[2], c);
                const limit = if (f.kind != .large_list) cur.data.len else a.children[0].len;
                for (0..if (a.len > 0) a.len + 1 else 0) |i| {
                    const o = std.mem.readInt(i64, offs[i * 8 ..][0..8], .little);
                    if (o < 0 or o > limit or (i > 0 and o < std.mem.readInt(i64, offs[(i - 1) * 8 ..][0..8], .little))) return Error.CorruptArrow;
                }
            },
            .utf8_view, .binary_view => {
                cur.data = try bytesOf(arena, a.bufs[1], c);
                if (cur.data.len < a.len * 16) return Error.CorruptArrow;
                const vs = try arena.alloc([]const u8, a.variadic.len);
                for (vs, a.variadic) |*v, vb| v.* = try bytesOf(arena, vb, c);
                cur.views = vs;
            },
            .fixed_list => {
                if (a.children[0].len < a.len * f.fixed_size) return Error.CorruptArrow;
            },
            .struct_ => {},
        }
        if (a.children.len > 0) {
            cur.kids = try arena.alloc(*Cursor, a.children.len);
            for (cur.kids, a.children, f.children) |*k, ca, cf| k.* = try self.cursor(arena, cf, ca, c);
            if (f.kind == .struct_) for (a.children) |ca| if (ca.len < a.len) return Error.CorruptArrow;
        }
        return cur;
    }
};

const Cursor = struct {
    f: Field,
    a: Arr,
    reader: *Reader,
    bits: []const u8 = &.{},
    data: []const u8 = &.{},
    offs32: []const u8 = &.{},
    offs64: []const u8 = &.{},
    views: []const []const u8 = &.{},
    kids: []*Cursor = &.{},
    dict: ?*const column.Column = null,

    fn isNull(self: *const Cursor, i: usize) bool {
        if (self.f.kind == .null_) return true;
        if (self.a.null_count == 0 or self.bits.len == 0) return false;
        return (self.bits[i >> 3] >> @intCast(i & 7)) & 1 == 0;
    }

    fn intAt(data: []const u8, i: usize, bits: u16, signed: bool) Error!i64 {
        return switch (bits) {
            8 => if (signed) @as(i8, @bitCast(data[i])) else data[i],
            16 => if (signed) std.mem.readInt(i16, data[i * 2 ..][0..2], .little) else std.mem.readInt(u16, data[i * 2 ..][0..2], .little),
            32 => if (signed) std.mem.readInt(i32, data[i * 4 ..][0..4], .little) else std.mem.readInt(u32, data[i * 4 ..][0..4], .little),
            64 => if (signed) std.mem.readInt(i64, data[i * 8 ..][0..8], .little) else blk: {
                const u = std.mem.readInt(u64, data[i * 8 ..][0..8], .little);
                break :blk std.math.cast(i64, u) orelse return Error.ArrowIntOverflow;
            },
            else => Error.CorruptArrow,
        };
    }

    fn span(self: *const Cursor, i: usize) Error!struct { lo: usize, hi: usize } {
        if (self.offs64.len > 0) {
            const lo = std.mem.readInt(i64, self.offs64[i * 8 ..][0..8], .little);
            const hi = std.mem.readInt(i64, self.offs64[(i + 1) * 8 ..][0..8], .little);
            return .{ .lo = @intCast(lo), .hi = @intCast(hi) };
        }
        const lo = std.mem.readInt(i32, self.offs32[i * 4 ..][0..4], .little);
        const hi = std.mem.readInt(i32, self.offs32[(i + 1) * 4 ..][0..4], .little);
        return .{ .lo = @intCast(lo), .hi = @intCast(hi) };
    }

    /// A null row's bytes are whatever the writer left (a view may hold garbage), so they read as empty.
    fn slice(self: *const Cursor, i: usize) Error![]const u8 {
        if (self.views.len > 0 or self.f.kind == .utf8_view or self.f.kind == .binary_view) {
            if (self.isNull(i)) return &.{};
            return self.viewAt(i);
        }
        const sp = try self.span(i);
        return self.data[sp.lo..sp.hi];
    }

    fn viewAt(self: *const Cursor, i: usize) Error![]const u8 {
        const v = self.data[i * 16 ..][0..16];
        const n = std.mem.readInt(i32, v[0..4], .little);
        if (n < 0) return Error.CorruptArrow;
        const len: usize = @intCast(n);
        if (len <= 12) return v[4..][0..len];
        const bi = std.mem.readInt(i32, v[8..12], .little);
        const off = std.mem.readInt(i32, v[12..16], .little);
        if (bi < 0 or off < 0 or @as(usize, @intCast(bi)) >= self.views.len) return Error.CorruptArrow;
        const b = self.views[@intCast(bi)];
        if (@as(usize, @intCast(off)) > b.len or len > b.len - @as(usize, @intCast(off))) return Error.CorruptArrow;
        return b[@intCast(off)..][0..len];
    }

    fn value(self: *const Cursor, arena: std.mem.Allocator, i: usize) anyerror!Value {
        if (self.isNull(i)) return .null;
        const f = self.f;
        if (self.dict) |d| {
            const ix = try intAt(self.data, i, f.dict.?.index_bits, f.dict.?.index_signed);
            if (ix < 0 or ix >= d.len) return Error.CorruptArrow;
            return d.getValue(@intCast(ix));
        }
        return switch (f.kind) {
            .null_ => .null,
            .int => .{ .int = try intAt(self.data, i, f.bit_width, f.signed) },
            .duration => .{ .int = f.unit.toMicros(std.mem.readInt(i64, self.data[i * 8 ..][0..8], .little)) },
            .float => .{ .float = if (f.bit_width == 32)
                @as(f32, @bitCast(std.mem.readInt(u32, self.data[i * 4 ..][0..4], .little)))
            else
                @as(f64, @bitCast(std.mem.readInt(u64, self.data[i * 8 ..][0..8], .little))) },
            .bool_ => .{ .bool = (self.data[i >> 3] >> @intCast(i & 7)) & 1 != 0 },
            .decimal => .{ .decimal = .{ .unscaled = std.mem.readInt(i128, self.data[i * 16 ..][0..16], .little), .scale = f.scale } },
            .date_day => .{ .date = std.mem.readInt(i32, self.data[i * 4 ..][0..4], .little) },
            .date_ms => .{ .date = std.math.cast(i32, @divFloor(std.mem.readInt(i64, self.data[i * 8 ..][0..8], .little), 86_400_000)) orelse return Error.CorruptArrow },
            .time => .{ .time = f.unit.toMicros(if (f.bit_width == 32)
                std.mem.readInt(i32, self.data[i * 4 ..][0..4], .little)
            else
                std.mem.readInt(i64, self.data[i * 8 ..][0..8], .little)) },
            .timestamp => .{ .timestamp = f.unit.toMicros(std.mem.readInt(i64, self.data[i * 8 ..][0..8], .little)) },
            .utf8, .large_utf8 => blk: {
                const s = try self.span(i);
                break :blk .{ .string = self.data[s.lo..s.hi] };
            },
            .binary, .large_binary => blk: {
                const s = try self.span(i);
                break :blk .{ .bytes = self.data[s.lo..s.hi] };
            },
            .utf8_view => .{ .string = try self.viewAt(i) },
            .binary_view => .{ .bytes = try self.viewAt(i) },
            .fixed_binary => .{ .bytes = self.data[i * f.fixed_size ..][0..f.fixed_size] },
            .list, .large_list, .fixed_list, .struct_, .map => blk: {
                var aw = std.Io.Writer.Allocating.init(arena);
                try self.json(arena, i, &aw.writer);
                break :blk .{ .string = aw.written() };
            },
        };
    }

    /// Row `i` as JSON. Numbers and booleans are bare, text and temporal values quoted
    /// as their SQL text, decimals as exact digits, a map as an object keyed by key text.
    fn json(self: *const Cursor, arena: std.mem.Allocator, i: usize, w: *std.Io.Writer) anyerror!void {
        if (self.isNull(i)) return w.writeAll("null");
        switch (self.f.kind) {
            .list, .large_list => {
                const s = try self.span(i);
                try w.writeByte('[');
                for (s.lo..s.hi, 0..) |k, n| {
                    if (n > 0) try w.writeByte(',');
                    try self.kids[0].json(arena, k, w);
                }
                try w.writeByte(']');
            },
            .fixed_list => {
                const n = self.f.fixed_size;
                try w.writeByte('[');
                for (0..n) |k| {
                    if (k > 0) try w.writeByte(',');
                    try self.kids[0].json(arena, i * n + k, w);
                }
                try w.writeByte(']');
            },
            .struct_ => {
                try w.writeByte('{');
                for (self.kids, self.f.children, 0..) |k, cf, n| {
                    if (n > 0) try w.writeByte(',');
                    try std.json.Stringify.encodeJsonString(cf.name, .{}, w);
                    try w.writeByte(':');
                    try k.json(arena, i, w);
                }
                try w.writeByte('}');
            },
            .map => {
                const s = try self.span(i);
                const entries = self.kids[0];
                if (entries.kids.len != 2) return Error.CorruptArrow;
                try w.writeByte('{');
                for (s.lo..s.hi, 0..) |k, n| {
                    if (n > 0) try w.writeByte(',');
                    const key = try entries.kids[0].value(arena, k);
                    try std.json.Stringify.encodeJsonString(try eval.valueToString(arena, key), .{}, w);
                    try w.writeByte(':');
                    try entries.kids[1].json(arena, k, w);
                }
                try w.writeByte('}');
            },
            else => {
                const v = try self.value(arena, i);
                switch (v) {
                    .null => try w.writeAll("null"),
                    .int => |x| try w.print("{d}", .{x}),
                    .bool => |b| try w.writeAll(if (b) "true" else "false"),
                    .float => |x| if (std.math.isFinite(x)) try w.print("{d}", .{x}) else try w.writeAll("null"),
                    .decimal => try w.writeAll(try eval.valueToString(arena, v)),
                    else => try std.json.Stringify.encodeJsonString(try eval.valueToString(arena, v), .{}, w),
                }
            },
        }
    }
};

fn copyAs(comptime T: type, arena: std.mem.Allocator, src: []const u8, n: usize) Error![]T {
    if (src.len < n * @sizeOf(T)) return Error.CorruptArrow;
    const out = try arena.alloc(T, n);
    @memcpy(std.mem.sliceAsBytes(out), src[0 .. n * @sizeOf(T)]);
    return out;
}

const source_vtable = driver.Source.VTable{
    .schema = srcSchema,
    .next = srcNext,
    .close = srcClose,
};

fn srcSchema(p: *anyopaque) types.Schema {
    return @as(*Reader, @ptrCast(@alignCast(p))).schema;
}
fn srcNext(p: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    return @as(*Reader, @ptrCast(@alignCast(p))).next(arena);
}
fn srcClose(p: *anyopaque) void {
    @as(*Reader, @ptrCast(@alignCast(p))).close();
}

fn dump(a: std.mem.Allocator, bytes: []const u8, want: ?[]const []const u8) ![]const u8 {
    const r = try Reader.openBytes(a, bytes, want);
    defer r.close();
    var out = std.Io.Writer.Allocating.init(a);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| {
            for (b.columns, r.schema.fields, 0..) |c, f, k| {
                if (k > 0) try out.writer.writeByte(' ');
                const v = c.getValue(i);
                try out.writer.print("{s}={s}", .{ f.name, if (v == .null) "null" else try eval.valueToString(a, v) });
            }
            try out.writer.writeByte('\n');
        }
    }
    return out.written();
}

const polars_want =
    \\i=1 s=a f=1.5 b=true d=2026-01-01 ts=2026-01-01 12:00:00 cat=x dec=1.50 lst=[1]
    \\i=2 s=null f=null b=false d=null ts=null cat=y dec=-2.25 lst=[2,3]
    \\i=null s=a string longer than twelve bytes f=-3 b=null d=1969-12-31 ts=1969-12-31 23:59:59.500000 cat=x dec=null lst=null
    \\
;

test "polars files read the same whether strings are views or large strings" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings(polars_want, try dump(a, @embedFile("testdata/polars.arrow"), null));
    try std.testing.expectEqualStrings(polars_want, try dump(a, @embedFile("testdata/polars_compat.arrow"), null));
}

const pyarrow_want =
    \\i8=-1 u64=0 f32=1.5 d64=2026-01-01 t32s=01:02:03 t64ns=01:02:03 ts_ns_utc=2026-01-01 12:00:00.123456 ts_ms=2026-01-01 12:00:00.123000 dec=12.345 ls=alpha fsb=abcd st={"a":1,"b":"x"} mp={"k":1} ll=[1.5,null] fl=[1,2] nul=null dict=p
    \\i8=null u64=null f32=null d64=null t32s=null t64ns=null ts_ns_utc=null ts_ms=null dec=null ls=null fsb=null st=null mp=null ll=null fl=null nul=null dict=q
    \\i8=127 u64=9223372036854775807 f32=-0.25 d64=1969-12-31 t32s=00:00:00 t64ns=00:00:00 ts_ns_utc=1969-12-31 23:59:59.999999 ts_ms=1970-01-01 00:00:00 dec=-0.001 ls=gamma fsb=wxyz st={"a":null,"b":"z"} mp={} ll=[] fl=[5,6] nul=null dict=null
    \\
;
const pyarrow_cols = [_][]const u8{ "i8", "u64", "f32", "d64", "t32s", "t64ns", "ts_ns_utc", "ts_ms", "dec", "ls", "fsb", "st", "mp", "ll", "fl", "nul", "dict" };

test "pyarrow files: LZ4 and ZSTD bodies, a chunked stream, every unit and nested type" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings(pyarrow_want, try dump(a, @embedFile("testdata/pyarrow_lz4.feather"), &pyarrow_cols));
    try std.testing.expectEqualStrings(pyarrow_want, try dump(a, @embedFile("testdata/pyarrow_zstd.feather"), &pyarrow_cols));
    try std.testing.expectEqualStrings(pyarrow_want, try dump(a, @embedFile("testdata/pyarrow_stream.arrows"), &pyarrow_cols));
}

test "a projection reads only the columns asked for, in the file's order" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const got = try dump(a, @embedFile("testdata/polars.arrow"), &.{ "dec", "i", "nosuch" });
    try std.testing.expectEqualStrings("i=1 dec=1.50\ni=2 dec=-2.25\ni=null dec=null\n", got);
}

test "types map onto the engine's, every column nullable" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const r = try Reader.openBytes(ar.allocator(), @embedFile("testdata/pyarrow_lz4.feather"), null);
    defer r.close();
    const Want = struct { name: []const u8, kind: types.TypeKind };
    const want = [_]Want{
        .{ .name = "i8", .kind = .int },      .{ .name = "u64", .kind = .int },
        .{ .name = "f32", .kind = .float },   .{ .name = "d64", .kind = .date },
        .{ .name = "t32s", .kind = .time },   .{ .name = "ts_ns_utc", .kind = .timestamp },
        .{ .name = "dec", .kind = .decimal }, .{ .name = "ls", .kind = .string },
        .{ .name = "bin", .kind = .bytes },   .{ .name = "fsb", .kind = .bytes },
        .{ .name = "st", .kind = .string },   .{ .name = "nul", .kind = .string },
        .{ .name = "dict", .kind = .string },
    };
    for (want) |w| {
        const f = r.schema.fields[r.schema.indexOf(w.name).?];
        try std.testing.expectEqual(w.kind, f.ty.kind);
        try std.testing.expect(f.ty.nullable);
    }
    const dec = r.schema.fields[r.schema.indexOf("dec").?].ty;
    try std.testing.expectEqual(@as(u8, 12), dec.precision);
    try std.testing.expectEqual(@as(u8, 3), dec.scale);
}

test "not arrow, and every truncation of an IPC file, is an error; a truncated stream never crashes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectError(Error.NotArrow, Reader.openBytes(a, "PAR1 and then some", null));
    inline for (.{ "testdata/polars.arrow", "testdata/pyarrow_lz4.feather" }) |name| {
        const full = @embedFile(name);
        var n: usize = 8;
        while (n < full.len) : (n += 7) {
            if (dump(a, full[0..n], null)) |_| {
                std.debug.print("{s}: a truncation to {d} of {d} bytes read without error\n", .{ name, n, full.len });
                return error.TestUnexpectedResult;
            } else |_| {}
        }
    }
    const stream = @embedFile("testdata/pyarrow_stream.arrows");
    var n: usize = 8;
    while (n < stream.len) : (n += 7) {
        _ = dump(a, stream[0..n], null) catch continue;
    }
}

test "fuzz: arbitrary bytes behind a valid magic never crash the reader" {
    try std.testing.fuzz({}, struct {
        fn f(_: void, input: []const u8) anyerror!void {
            var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer ar.deinit();
            var buf: [4096]u8 = undefined;
            const n = @min(input.len, buf.len - 8);
            @memcpy(buf[0..8], "\xff\xff\xff\xff\x00\x00\x00\x00"[0..8]);
            std.mem.writeInt(u32, buf[4..8], @intCast(@min(n, 4000)), .little);
            @memcpy(buf[8..][0..n], input[0..n]);
            _ = dump(ar.allocator(), buf[0 .. 8 + n], null) catch return;
        }
    }.f, .{});
}
