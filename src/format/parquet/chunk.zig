//! A column chunk's pages decoded into a column: dictionary and data pages (v1 and v2),
//! levels, and the bulk paths for flat columns.

const Decimal = @import("../../exec/value.zig").Decimal;
const Error = @import("read.zig").Error;
const Levels = @import("nested.zig").Levels;
const ListShape = @import("schema.zig").ListShape;
const PlainCursor = @import("encoding.zig").PlainCursor;
const TemporalScale = @import("logical.zig").TemporalScale;
const Value = @import("../../exec/value.zig").Value;
const assembleLists = @import("nested.zig").assembleLists;
const basaltType = @import("logical.zig").basaltType;
const bitWidth = @import("encoding.zig").bitWidth;
const coerce = @import("logical.zig").coerce;
const column = @import("../../exec/column.zig");
const decodeByteStreamSplit = @import("encoding.zig").decodeByteStreamSplit;
const decodeDeltaBinaryPacked = @import("encoding.zig").decodeDeltaBinaryPacked;
const decodeDeltaByteArray = @import("encoding.zig").decodeDeltaByteArray;
const decodeDeltaLengthByteArray = @import("encoding.zig").decodeDeltaLengthByteArray;
const decodeRleHybrid = @import("encoding.zig").decodeRleHybrid;
const parquet = @import("footer.zig");
const std = @import("std");
const temporalScale = @import("logical.zig").temporalScale;
const types = @import("../../lang/types.zig");
const Reader = @import("read.zig").Reader;
const fx = @import("testing_util.zig").fx;
const testing = std.testing;

/// A dictionary page, if present, fills the dictionary later data pages index
/// into. `base_offset` is where `file_bytes[0]` sits in the file.
pub fn readColumnChunk(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    rows: usize,
    max_def: u32,
    base_offset: u64,
) (Error || parquet.Error || @import("../codec.zig").Error)!column.Column {
    return readColumnChunkLevels(arena, file_bytes, meta, elem, rows, max_def, 0, null, base_offset);
}

/// With `list` set, the chunk's entries (one per level pair, not per row) are
/// assembled into a JSON array per row.
pub fn readColumnChunkLevels(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    rows: usize,
    max_def: u32,
    max_rep: u32,
    list: ?ListShape,
    base_offset: u64,
) (Error || parquet.Error || @import("../codec.zig").Error)!column.Column {
    const ty = (try basaltType(elem)).asNullable();
    const tscale = temporalScale(elem);

    const entries = if (list != null) std.math.cast(usize, meta.num_values) orelse return Error.CorruptParquetPage else rows;
    var levels: ?Levels = if (list != null) .{
        .reps = std.array_list.Managed(u32).init(arena),
        .defs = std.array_list.Managed(u32).init(arena),
    } else null;

    var b = try column.Builder.initCapacity(arena, ty, entries);
    var dict: ?[]Value = null;
    const at = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
    if (at < base_offset) return Error.CorruptParquetPage;
    var offset: usize = std.math.cast(usize, at - base_offset) orelse return Error.CorruptParquetPage;
    var produced: usize = 0;

    while (produced < entries) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try parquet.readPage(arena, file_bytes, offset, meta.compression);
        offset = pg.next_offset;

        switch (pg.header.ty) {
            .dictionary_page => {
                const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
                const vals = try arena.alloc(Value, n);
                var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, pg.data);
                for (vals) |*v| v.* = try cur.next();
                dict = vals;
            },
            .data_page, .data_page_v2 => {
                produced += try appendDataPage(arena, &b, pg, meta, elem, ty, max_def, dict, tscale, max_rep, if (levels) |*l| l else null);
            },
            .index_page => {},
            else => return Error.CorruptParquetPage,
        }
    }
    const col = try b.finish();
    if (list) |shape| return assembleLists(arena, col, levels.?.reps.items, levels.?.defs.items, max_def, shape, rows);
    return col;
}

pub const Entries = struct { vals: column.Column, reps: []const u32, defs: []const u32 };

pub fn readEntries(
    arena: std.mem.Allocator,
    file_bytes: []const u8,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    max_def: u32,
    max_rep: u32,
    base_offset: u64,
) (Error || parquet.Error || @import("../codec.zig").Error)!Entries {
    const ty = (try basaltType(elem)).asNullable();
    const tscale = temporalScale(elem);
    const entries = std.math.cast(usize, meta.num_values) orelse return Error.CorruptParquetPage;
    var levels = Levels{
        .reps = std.array_list.Managed(u32).init(arena),
        .defs = std.array_list.Managed(u32).init(arena),
    };
    var b = try column.Builder.initCapacity(arena, ty, entries);
    var dict: ?[]Value = null;
    const at = std.math.cast(u64, meta.startOffset()) orelse return Error.CorruptParquetPage;
    if (at < base_offset) return Error.CorruptParquetPage;
    var offset: usize = std.math.cast(usize, at - base_offset) orelse return Error.CorruptParquetPage;
    var produced: usize = 0;
    while (produced < entries) {
        if (offset >= file_bytes.len) return Error.CorruptParquetPage;
        const pg = try parquet.readPage(arena, file_bytes, offset, meta.compression);
        offset = pg.next_offset;
        switch (pg.header.ty) {
            .dictionary_page => {
                const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
                const vals = try arena.alloc(Value, n);
                var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, pg.data);
                for (vals) |*v| v.* = try cur.next();
                dict = vals;
            },
            .data_page, .data_page_v2 => {
                produced += try appendDataPage(arena, &b, pg, meta, elem, ty, max_def, dict, tscale, max_rep, &levels);
            },
            .index_page => {},
            else => return Error.CorruptParquetPage,
        }
    }
    const vals = try b.finish();
    if (vals.len != levels.reps.items.len or vals.len != levels.defs.items.len) return Error.CorruptParquetPage;
    return .{ .vals = vals, .reps = levels.reps.items, .defs = levels.defs.items };
}

/// v1 length-prefixes each level section; v2 keeps them unprefixed with lengths
/// in the header. The bulk path keys on `present == n`, not on absent levels:
/// most writers mark every column OPTIONAL even when nothing is null.
fn appendDataPage(
    arena: std.mem.Allocator,
    b: *column.Builder,
    pg: parquet.Page,
    meta: parquet.ColumnMetaData,
    elem: parquet.SchemaElement,
    ty: types.Type,
    max_def: u32,
    dict: ?[]Value,
    tscale: TemporalScale,
    max_rep: u32,
    levels: ?*Levels,
) Error!usize {
    const n = std.math.cast(usize, pg.header.num_values) orelse return Error.CorruptParquetPage;
    var body = pg.data;

    var reps: ?[]u32 = null;
    var defs: ?[]u32 = null;
    if (pg.header.ty == .data_page_v2) {
        const rl = pg.header.rep_levels_len;
        if (rl > body.len) return Error.CorruptParquetPage;
        if (max_rep > 0 and rl > 0) reps = try decodeRleHybrid(arena, body[0..rl], bitWidth(max_rep), n);
        body = body[rl..];
        const dl = pg.header.def_levels_len;
        if (dl > body.len) return Error.CorruptParquetPage;
        if (max_def > 0 and dl > 0) {
            defs = try decodeRleHybrid(arena, body[0..dl], bitWidth(max_def), n);
        }
        body = body[dl..];
    } else {
        if (max_rep > 0) {
            if (body.len < 4) return Error.CorruptParquetPage;
            const len: usize = std.mem.readInt(u32, body[0..4], .little);
            if (4 + len > body.len) return Error.CorruptParquetPage;
            reps = try decodeRleHybrid(arena, body[4..][0..len], bitWidth(max_rep), n);
            body = body[4 + len ..];
        }
        if (max_def > 0) {
            if (body.len < 4) return Error.CorruptParquetPage;
            const len: usize = std.mem.readInt(u32, body[0..4], .little);
            if (4 + len > body.len) return Error.CorruptParquetPage;
            defs = try decodeRleHybrid(arena, body[4..][0..len], bitWidth(max_def), n);
            body = body[4 + len ..];
        }
    }
    if (levels) |lv| {
        if (reps) |r| try lv.reps.appendSlice(r) else try lv.reps.appendNTimes(0, n);
        if (defs) |d| try lv.defs.appendSlice(d) else try lv.defs.appendNTimes(max_def, n);
    }

    var present: usize = n;
    if (defs) |d| {
        present = 0;
        for (d) |lvl| {
            if (lvl == max_def) present += 1;
        }
    }

    switch (pg.header.encoding) {
        .plain => {
            if (tscale.isIdentity() and try bulkPlain(arena, b, ty, meta.ty, body, present, defs, max_def)) {
                return n;
            }
            var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, body);
            try emit(b, ty, defs, max_def, n, &cur, null, null, tscale);
        },
        .byte_stream_split => {
            const width: usize = switch (meta.ty) {
                .float => 4,
                .double => 8,
                .int32 => 4,
                .int64 => 8,
                .fixed_len_byte_array => @intCast(@max(0, elem.type_length orelse 0)),
                else => return Error.UnsupportedParquetEncoding,
            };
            const flat = try decodeByteStreamSplit(arena, body, width, present);
            var cur = PlainCursor.init(meta.ty, elem.type_length orelse 0, flat);
            try emit(b, ty, defs, max_def, n, &cur, null, null, tscale);
        },
        .delta_binary_packed => {
            const vals = try decodeDeltaBinaryPacked(arena, body, present);
            try emitInts(b, ty, defs, max_def, n, vals, tscale);
        },
        .delta_length_byte_array => {
            const vals = try decodeDeltaLengthByteArray(arena, body, present);
            try emitBytes(b, ty, defs, max_def, n, vals, tscale);
        },
        .delta_byte_array => {
            const vals = try decodeDeltaByteArray(arena, body, present);
            try emitBytes(b, ty, defs, max_def, n, vals, tscale);
        },
        .rle => {
            const bits = try decodeRleHybrid(arena, body[@min(4, body.len)..], 1, present);
            const vals = try arena.alloc(i64, present);
            for (vals, bits) |*v, x| v.* = @intCast(x);
            try emitInts(b, ty, defs, max_def, n, vals, tscale);
        },
        .plain_dictionary, .rle_dictionary => {
            const d = dict orelse return Error.CorruptParquetPage;
            if (body.len < 1) return Error.CorruptParquetPage;
            if (body[0] > 32) return Error.CorruptParquetPage;
            const width: u6 = @intCast(body[0]);
            const idx = try decodeRleHybrid(arena, body[1..], width, present);
            if (try bulkDict(arena, b, ty, d, idx, defs, max_def, tscale)) return n;
            try emit(b, ty, defs, max_def, n, null, idx, d, tscale);
        },
        else => return Error.UnsupportedParquetEncoding,
    }
    return n;
}

/// Decodes a PLAIN page straight into the typed store, or returns false for a
/// shape it does not cover. Byte arrays are slices of the page body.
fn bulkPlain(
    arena: std.mem.Allocator,
    b: *column.Builder,
    ty: types.Type,
    phys: parquet.PhysicalType,
    body: []const u8,
    present: usize,
    defs: ?[]const u32,
    max_def: u32,
) Error!bool {
    const count = present;
    switch (phys) {
        .int64 => {
            if (body.len < count * 8) return Error.CorruptParquetPage;
            switch (ty.kind) {
                .int, .time, .timestamp => {
                    const out = try arena.alloc(i64, count);
                    for (out, 0..) |*o, i| o.* = std.mem.readInt(i64, body[i * 8 ..][0..8], .little);
                    if (defs) |d| {
                        b.appendBulkScattered(i64, out, d, max_def) catch return false;
                    } else b.appendBulk(i64, out) catch return false;
                    return true;
                },
                .decimal => {
                    const out = try arena.alloc(Decimal, count);
                    for (out, 0..) |*o, i| o.* = .{ .unscaled = std.mem.readInt(i64, body[i * 8 ..][0..8], .little), .scale = ty.scale };
                    if (defs) |d| {
                        b.appendBulkScattered(Decimal, out, d, max_def) catch return false;
                    } else b.appendBulk(Decimal, out) catch return false;
                    return true;
                },
                else => return false,
            }
        },
        .int32 => {
            if (body.len < count * 4) return Error.CorruptParquetPage;
            switch (ty.kind) {
                .int => {
                    const out = try arena.alloc(i64, count);
                    for (out, 0..) |*o, i| o.* = std.mem.readInt(i32, body[i * 4 ..][0..4], .little);
                    if (defs) |d| {
                        b.appendBulkScattered(i64, out, d, max_def) catch return false;
                    } else b.appendBulk(i64, out) catch return false;
                    return true;
                },
                .date => {
                    const out = try arena.alloc(i32, count);
                    for (out, 0..) |*o, i| o.* = std.mem.readInt(i32, body[i * 4 ..][0..4], .little);
                    if (defs) |d| {
                        b.appendBulkScattered(i32, out, d, max_def) catch return false;
                    } else b.appendBulk(i32, out) catch return false;
                    return true;
                },
                .decimal => {
                    const out = try arena.alloc(Decimal, count);
                    for (out, 0..) |*o, i| o.* = .{ .unscaled = std.mem.readInt(i32, body[i * 4 ..][0..4], .little), .scale = ty.scale };
                    if (defs) |d| {
                        b.appendBulkScattered(Decimal, out, d, max_def) catch return false;
                    } else b.appendBulk(Decimal, out) catch return false;
                    return true;
                },
                else => return false,
            }
        },
        .byte_array => {
            if (ty.kind != .string and ty.kind != .bytes) return false;
            const vals = try plainByteArrays(arena, body, count);
            b.appendBytesScattered(vals, defs, max_def) catch return false;
            return true;
        },
        .double => {
            if (ty.kind != .float) return false;
            if (body.len < count * 8) return Error.CorruptParquetPage;
            const out = try arena.alloc(f64, count);
            for (out, 0..) |*o, i| o.* = @bitCast(std.mem.readInt(u64, body[i * 8 ..][0..8], .little));
            if (defs) |d| {
                b.appendBulkScattered(f64, out, d, max_def) catch return false;
            } else b.appendBulk(f64, out) catch return false;
            return true;
        },
        .float => {
            if (ty.kind != .float) return false;
            if (body.len < count * 4) return Error.CorruptParquetPage;
            const out = try arena.alloc(f64, count);
            for (out, 0..) |*o, i| {
                const raw = std.mem.readInt(u32, body[i * 4 ..][0..4], .little);
                o.* = @floatCast(@as(f32, @bitCast(raw)));
            }
            if (defs) |d| {
                b.appendBulkScattered(f64, out, d, max_def) catch return false;
            } else b.appendBulk(f64, out) catch return false;
            return true;
        },
        else => return false,
    }
}

fn plainByteArrays(arena: std.mem.Allocator, body: []const u8, count: usize) Error![]const []const u8 {
    const out = try arena.alloc([]const u8, count);
    var pos: usize = 0;
    for (out) |*o| {
        if (pos + 4 > body.len) return Error.CorruptParquetPage;
        const len: usize = std.mem.readInt(u32, body[pos..][0..4], .little);
        pos += 4;
        if (pos + len > body.len) return Error.CorruptParquetPage;
        o.* = body[pos..][0..len];
        pos += len;
    }
    return out;
}

/// Turns the dictionary into a typed array once and gathers indices through it,
/// rather than boxing every row into a `Value`. False for a shape not covered.
fn bulkDict(
    arena: std.mem.Allocator,
    b: *column.Builder,
    ty: types.Type,
    dict: []const Value,
    idx: []const u32,
    defs: ?[]const u32,
    max_def: u32,
    tscale: TemporalScale,
) Error!bool {
    for (idx) |ix| if (ix >= dict.len) return Error.CorruptParquetPage;
    switch (ty.kind) {
        .string, .bytes => {
            for (dict) |v| if (v != .bytes) return false;
            const vals = try arena.alloc([]const u8, idx.len);
            for (vals, idx) |*o, ix| o.* = dict[ix].bytes;
            b.appendBytesScattered(vals, defs, max_def) catch return false;
            const entries = try arena.alloc([]const u8, dict.len);
            for (entries, dict) |*o, v| o.* = v.bytes;
            try b.noteDict(@intFromPtr(dict.ptr), entries, idx, defs, max_def);
            return true;
        },
        .int => {
            for (dict) |v| if (v != .int) return false;
            const vals = try arena.alloc(i64, idx.len);
            for (vals, idx) |*o, ix| o.* = dict[ix].int;
            if (defs) |d| {
                b.appendBulkScattered(i64, vals, d, max_def) catch return false;
            } else b.appendBulk(i64, vals) catch return false;
            return true;
        },
        .float => {
            for (dict) |v| if (v != .float) return false;
            const vals = try arena.alloc(f64, idx.len);
            for (vals, idx) |*o, ix| o.* = dict[ix].float;
            if (defs) |d| {
                b.appendBulkScattered(f64, vals, d, max_def) catch return false;
            } else b.appendBulk(f64, vals) catch return false;
            return true;
        },
        .date => {
            if (!tscale.isIdentity()) return false;
            for (dict) |v| if (v != .int) return false;
            const vals = try arena.alloc(i32, idx.len);
            for (vals, idx) |*o, ix| o.* = std.math.cast(i32, dict[ix].int) orelse return false;
            if (defs) |d| {
                b.appendBulkScattered(i32, vals, d, max_def) catch return false;
            } else b.appendBulk(i32, vals) catch return false;
            return true;
        },
        else => return false,
    }
}

fn emitInts(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    vals: []const i64,
    tscale: TemporalScale,
) Error!void {
    var j: usize = 0;
    for (0..n) |i| {
        if (if (defs) |d| d[i] != max_def else false) {
            try b.append(.null);
            continue;
        }
        if (j >= vals.len) return Error.CorruptParquetPage;
        const v: Value = if (ty.kind == .bool) .{ .bool = vals[j] != 0 } else .{ .int = vals[j] };
        j += 1;
        try b.append(coerce(ty, v, tscale));
    }
}

fn emitBytes(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    vals: []const []const u8,
    tscale: TemporalScale,
) Error!void {
    if (ty.kind == .string or ty.kind == .bytes) {
        b.appendBytesScattered(vals, defs, max_def) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return Error.CorruptParquetPage,
        };
        return;
    }
    var j: usize = 0;
    for (0..n) |i| {
        if (if (defs) |d| d[i] != max_def else false) {
            try b.append(.null);
            continue;
        }
        if (j >= vals.len) return Error.CorruptParquetPage;
        const v = Value{ .bytes = vals[j] };
        j += 1;
        try b.append(coerce(ty, v, tscale));
    }
}

/// A row whose definition level is below the maximum has no stored value, so the
/// value stream does not advance for it.
fn emit(
    b: *column.Builder,
    ty: types.Type,
    defs: ?[]const u32,
    max_def: u32,
    n: usize,
    cur: ?*PlainCursor,
    idx: ?[]const u32,
    dict: ?[]const Value,
    tscale: TemporalScale,
) Error!void {
    var j: usize = 0;
    for (0..n) |i| {
        const is_null = if (defs) |d| d[i] != max_def else false;
        if (is_null) {
            try b.append(.null);
            continue;
        }
        const v: Value = if (cur) |c|
            try c.next()
        else blk: {
            const ix = idx.?;
            const dv = dict.?;
            if (j >= ix.len or ix[j] >= dv.len) return Error.CorruptParquetPage;
            break :blk dv[ix[j]];
        };
        j += 1;
        try b.append(coerce(ty, v, tscale));
    }
}

test "decodes real column values from a DuckDB-written file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const md = try parquet.parseFile(a, fx);
    const g = md.row_groups[0];
    const rows: usize = @intCast(g.num_rows);

    const id = try readColumnChunk(a, fx, g.columns[0].meta.?, md.schema[1], rows, 1, 0);
    try testing.expectEqual(@as(usize, 60), id.len);
    try testing.expectEqual(@as(i64, 0), id.getValue(0).int);
    try testing.expectEqual(@as(i64, 59), id.getValue(59).int);

    const name = try readColumnChunk(a, fx, g.columns[1].meta.?, md.schema[2], rows, 1, 0);
    try testing.expectEqualStrings("row-0", name.getValue(0).string);
    try testing.expectEqualStrings("row-59", name.getValue(59).string);

    const amt = try readColumnChunk(a, fx, g.columns[2].meta.?, md.schema[3], rows, 1, 0);
    try testing.expectEqual(@as(f64, 0.0), amt.getValue(0).float);
    try testing.expectEqual(@as(f64, 88.5), amt.getValue(59).float);

    const flag = try readColumnChunk(a, fx, g.columns[3].meta.?, md.schema[4], rows, 1, 0);
    try testing.expectEqual(true, flag.getValue(0).bool);
    try testing.expectEqual(false, flag.getValue(1).bool);
    try testing.expectEqual(false, flag.getValue(59).bool);
}

test "ranged reads return the same values as an in-memory file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "r.parquet", .data = fx });
    const dir = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ dir, "r.parquet" });

    const r = try Reader.open(a, path);
    defer r.close();
    try testing.expect(r.src == .file);
    const got = (try r.next(a)).?;
    try testing.expectEqual(@as(usize, 60), got.len);
    try testing.expect((try r.next(a)) == null);

    const md = try parquet.parseFile(a, fx);
    const g = md.row_groups[0];
    try testing.expectEqual(g.columns.len, got.columns.len);
    for (g.columns, 0..) |c, ci| {
        const want = try readColumnChunk(a, fx, c.meta.?, md.schema[ci + 1], @intCast(g.num_rows), 1, 0);
        try testing.expectEqual(want.len, got.columns[ci].len);
        for (0..want.len) |i| try testing.expectEqualDeep(want.getValue(i), got.columns[ci].getValue(i));
    }
}
