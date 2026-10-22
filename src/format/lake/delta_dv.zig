//! A Delta deletion vector: the rows of one data file a table deletes, which a
//! read must not return. A file's `add` describes it (`Descriptor`): stored inline
//! in the log (`i`, Z85 text), in a file under the table named by a Z85 UUID
//! after an optional sub-folder prefix (`u`), or in a file at an absolute path
//! (`p`). A stored one is read through the same storage as the table: a version
//! byte (1) at the file's start, then at the descriptor's offset a big-endian
//! size, that many bytes (a little-endian magic and the bitmap), and a big-endian
//! CRC-32 of them. Only the portable bitmap format the protocol names is read
//! (`roaring.zig`); the native one Spark's own readers accept is refused, as is any
//! storage type the protocol does not define (`r`, a preview feature's).
//!
//! Row positions count across the whole data file, so a deleted row is skipped by
//! its file-wide index whichever row groups a read prunes (`read.zig`).

const std = @import("std");
const Bytes = @import("../bytes.zig").Bytes;
const Deletes = @import("../parquet/fit.zig").Deletes;
const roaring = @import("roaring.zig");
const table = @import("table.zig");
const testing = std.testing;

const refuse = table.refuse;

pub const portable_magic: u32 = 1681511377;
const native_magic: u32 = 1681511376;
const uuid_len = 20;

/// An `add`'s `deletionVector`, checked: `where` is the inline Z85 text, the
/// prefixed Z85 UUID, or the absolute path.
pub const Descriptor = struct {
    storage: u8,
    where: []const u8,
    offset: ?u64 = null,
    size: u32,
    cardinality: u64,
};

/// One file's deletion vector as its scan loads it: the table (`name`, `root`)
/// and the file as messages name it.
pub const Ref = struct {
    name: []const u8,
    root: []const u8,
    file: []const u8,
    d: Descriptor,

    /// How the file's reader loads the deletion vector.
    pub fn deletes(self: *const Ref) Deletes {
        return .{ .ctx = self, .load = loadFn };
    }

    fn loadFn(ctx: *const anyopaque, arena: std.mem.Allocator, rows: u64) anyerror!roaring.Bitmap {
        const self: *const Ref = @ptrCast(@alignCast(ctx));
        return load(arena, self, rows);
    }
};

fn int(v: ?std.json.Value) ?i64 {
    const x = v orelse return null;
    return switch (x) {
        .integer => |i| i,
        else => null,
    };
}

/// `v` (an `add`'s `deletionVector`) checked as the protocol describes one.
pub fn descriptor(name: []const u8, file: []const u8, v: std.json.Value) !Descriptor {
    if (v != .object) return malformed(name, file, "it is not an object");
    const o = v.object;
    const st = o.get("storageType") orelse return malformed(name, file, "it has no storageType");
    if (st != .string or st.string.len != 1) return malformed(name, file, "its storageType is not one letter");
    const kind = st.string[0];
    if (kind == 'r')
        return refuse("{s} has file `{s}` whose deletion vector is stored as `r` (an unencoded relative path, from a preview feature the Delta protocol does not define); basalt does not read it, and reading the file without it would return deleted rows", .{ name, file });
    if (kind != 'u' and kind != 'i' and kind != 'p')
        return refuse("{s} has file `{s}` whose deletion vector is stored as `{c}`, a storage type the Delta protocol does not define; basalt does not read it, and reading the file without it would return deleted rows", .{ name, file, kind });
    const where = o.get("pathOrInlineDv") orelse return malformed(name, file, "it has no pathOrInlineDv");
    if (where != .string) return malformed(name, file, "its pathOrInlineDv is not text");
    const size = int(o.get("sizeInBytes")) orelse return malformed(name, file, "it has no sizeInBytes");
    const card = int(o.get("cardinality")) orelse return malformed(name, file, "it has no cardinality");
    if (size < 4 or size > std.math.maxInt(u32)) return malformed(name, file, "its sizeInBytes is out of range");
    if (card < 0) return malformed(name, file, "its cardinality is negative");
    var d = Descriptor{ .storage = kind, .where = where.string, .size = @intCast(size), .cardinality = @intCast(card) };
    if (o.get("offset")) |off| if (off != .null) {
        const x = int(off) orelse return malformed(name, file, "its offset is not a number");
        if (x < 1) return malformed(name, file, "its offset is before the file's first deletion vector");
        if (kind == 'i') return malformed(name, file, "it is inline but has an offset");
        d.offset = @intCast(x);
    };
    switch (kind) {
        'i' => if (d.where.len % 5 != 0 or d.where.len / 5 * 4 < d.size or !isZ85(d.where))
            return malformed(name, file, "its inline bytes are not Z85 text of its size"),
        'u' => if (d.where.len < uuid_len or !isZ85(d.where[d.where.len - uuid_len ..]))
            return malformed(name, file, "its path does not end in a Z85 UUID"),
        else => {},
    }
    return d;
}

fn malformed(name: []const u8, file: []const u8, comptime why: []const u8) anyerror {
    return refuse("{s} has file `{s}` whose deletion vector descriptor is malformed (" ++ why ++ "); basalt does not read the file, as reading it without its deletion vector would return deleted rows", .{ name, file });
}

const z85_chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

const z85_values: [256]u8 = blk: {
    var t = [_]u8{0xFF} ** 256;
    for (z85_chars, 0..) |c, i| t[c] = i;
    break :blk t;
};

/// Whether `s` is Z85 text that decodes: a multiple of 5 characters of its
/// alphabet, each group at most 2^32-1.
fn isZ85(s: []const u8) bool {
    if (s.len % 5 != 0) return false;
    var i: usize = 0;
    while (i < s.len) : (i += 5) {
        var v: u64 = 0;
        for (s[i..][0..5]) |c| {
            if (z85_values[c] == 0xFF) return false;
            v = v * 85 + z85_values[c];
        }
        if (v > std.math.maxInt(u32)) return false;
    }
    return true;
}

/// Z85 text (a multiple of 5 characters) as its bytes, four per five characters,
/// each group a big-endian u32; null when the text is not Z85.
pub fn z85(arena: std.mem.Allocator, text: []const u8) !?[]u8 {
    if (text.len % 5 != 0) return null;
    const out = try arena.alloc(u8, text.len / 5 * 4);
    var i: usize = 0;
    while (i < text.len) : (i += 5) {
        var v: u64 = 0;
        for (text[i..][0..5]) |c| {
            const d = z85_values[c];
            if (d == 0xFF) return null;
            v = v * 85 + d;
        }
        if (v > std.math.maxInt(u32)) return null;
        std.mem.writeInt(u32, out[i / 5 * 4 ..][0..4], @intCast(v), .big);
    }
    return out;
}

/// Where a stored deletion vector is: `<root><prefix>/deletion_vector_<uuid>.bin`
/// for `u`, the path itself for `p`.
pub fn location(arena: std.mem.Allocator, root: []const u8, d: Descriptor) ![]const u8 {
    if (d.storage == 'p') return table.mapPath(arena, root, try table.percentDecode(arena, d.where));
    const prefix = d.where[0 .. d.where.len - uuid_len];
    const u = (try z85(arena, d.where[d.where.len - uuid_len ..])) orelse
        return refuse("deletion vector path `{s}` does not end in a Z85 UUID", .{d.where});
    const hex = std.fmt.bytesToHex(u[0..16].*, .lower);
    return std.fmt.allocPrint(arena, "{s}{s}{s}deletion_vector_{s}-{s}-{s}-{s}-{s}.bin", .{
        root, prefix, if (prefix.len > 0) "/" else "", hex[0..8], hex[8..12], hex[12..16], hex[16..20], hex[20..32],
    });
}

/// The deletion vector `r` names, checked whole and against a file of `rows` rows.
pub fn load(arena: std.mem.Allocator, r: *const Ref, rows: u64) !roaring.Bitmap {
    if (r.d.cardinality == 0) return .{};
    const data = if (r.d.storage == 'i') blk: {
        const all = (try z85(arena, r.d.where)) orelse return malformed(r.name, r.file, "its inline bytes are not Z85 text");
        break :blk all[0..r.d.size];
    } else try stored(arena, r);
    const magic = std.mem.readInt(u32, data[0..4], .little);
    if (magic == native_magic)
        return bad(r, "is in the native RoaringBitmapArray format, not the portable one the Delta protocol specifies", .{});
    if (magic != portable_magic) return bad(r, "does not start with the portable RoaringBitmapArray magic", .{});
    const bm = roaring.parse(arena, data[4..]) catch |e| {
        if (e == error.OutOfMemory) return e;
        return bad(r, "holds a malformed bitmap", .{});
    };
    if (bm.cardinality != r.d.cardinality)
        return bad(r, "deletes {d} rows where the log says {d}", .{ bm.cardinality, r.d.cardinality });
    if (bm.end > rows) return bad(r, "deletes row {d} of a file of {d} rows", .{ bm.end - 1, rows });
    return bm;
}

/// A stored deletion vector's bytes (magic and bitmap), its framing checked.
fn stored(arena: std.mem.Allocator, r: *const Ref) ![]const u8 {
    const path = try location(arena, r.root, r.d);
    const src = Bytes.open(arena, path) catch |e| {
        if (e == error.OutOfMemory) return e;
        return bad(r, "file `{s}` could not be opened ({s})", .{ below(path, r.root), @errorName(e) });
    };
    defer src.close();
    const off = r.d.offset orelse 1;
    const len = @as(u64, r.d.size) + 8;
    if (off + len > src.size()) return bad(r, "file `{s}` is shorter than the log says", .{below(path, r.root)});
    const head = off <= 4096;
    const bytes = if (head) try src.range(arena, 0, @intCast(off + len)) else try src.range(arena, off, @intCast(len));
    const version = if (head) bytes[0] else (try src.range(arena, 0, 1))[0];
    if (version != 1) return bad(r, "file `{s}` is format version {d}; basalt reads version 1", .{ below(path, r.root), version });
    const frame = if (head) bytes[@intCast(off)..] else bytes;
    const size = std.mem.readInt(u32, frame[0..4], .big);
    if (size != r.d.size) return bad(r, "in file `{s}` holds {d} bytes where the log says {d}", .{ below(path, r.root), size, r.d.size });
    const data = frame[4..][0..size];
    if (std.hash.Crc32.hash(data) != std.mem.readInt(u32, frame[4 + size ..][0..4], .big))
        return bad(r, "in file `{s}` fails its CRC-32 check: it is corrupt", .{below(path, r.root)});
    return data;
}

fn bad(r: *const Ref, comptime what: []const u8, args: anytype) anyerror {
    return refuse("{s} has file `{s}` whose deletion vector " ++ what ++ "; basalt does not read the file, as reading it without its deletion vector would return deleted rows", .{ r.name, r.file } ++ args);
}

fn below(path: []const u8, root: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, path, root)) path[root.len..] else path;
}

test "Z85: the protocol's UUID example and its path, the kernel-rs inline vector's rows" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const d = Descriptor{ .storage = 'u', .where = "ab^-aqEH.-t@S}K{vb[*k^", .offset = 1, .size = 36, .cardinality = 2 };
    try testing.expectEqualStrings("/t/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", try location(a, "/t/", d));
    const plain = Descriptor{ .storage = 'u', .where = "vBn[lx{q8@P<9BNH/isA", .offset = 1, .size = 36, .cardinality = 2 };
    try testing.expectEqualStrings("/t/deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin", try location(a, "/t/", plain));
    try testing.expectEqualStrings("s3://b/dv/x.bin", try location(a, "/t/", .{ .storage = 'p', .where = "s3a://b/dv/x.bin", .size = 4, .cardinality = 1 }));
    try testing.expect(try z85(a, "abcd") == null);
    try testing.expect(try z85(a, "abc\"d") == null);
    try testing.expect(try z85(a, "#####") == null);
    try testing.expectError(table.Error.LakeTableRefused, location(a, "/t/", .{ .storage = 'u', .where = "#####aaaaaaaaaaaaaaa", .size = 4, .cardinality = 1 }));
    _ = @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused);
    try testing.expectEqualSlices(u8, &.{ 0x86, 0x4F, 0xD2, 0x6F }, (try z85(a, "HelloWorld")).?[0..4]);

    const inline_dv = std.json.Value{ .object = blk: {
        var o = std.json.ObjectMap.init(a);
        try o.put("storageType", .{ .string = "i" });
        try o.put("pathOrInlineDv", .{ .string = "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L" });
        try o.put("sizeInBytes", .{ .integer = 44 });
        try o.put("cardinality", .{ .integer = 6 });
        break :blk o;
    } };
    const r = Ref{ .name = "T", .root = "/t/", .file = "f.parquet", .d = try descriptor("T", "f.parquet", inline_dv) };
    const bm = try load(a, &r, 30);
    var keep: [30]bool = undefined;
    @memset(&keep, true);
    try testing.expectEqual(@as(usize, 6), bm.clear(0, &keep));
    for (keep, 0..) |k, i| try testing.expectEqual(std.mem.indexOfScalar(u64, &.{ 3, 4, 7, 11, 18, 29 }, i) == null, k);
    try testing.expectError(table.Error.LakeTableRefused, load(a, &r, 29));
    try testing.expect(std.mem.indexOf(u8, @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?, "deletes row 29 of a file of 29 rows") != null);
}

/// A deletion vector file holding `rows`, framed as Spark writes one, at offset 1.
fn dvFile(a: std.mem.Allocator, rows: []const u16) ![]u8 {
    var body = std.ArrayListUnmanaged(u8).empty;
    var x: [8]u8 = undefined;
    std.mem.writeInt(u32, x[0..4], portable_magic, .little);
    try body.appendSlice(a, x[0..4]);
    std.mem.writeInt(u64, &x, 1, .little);
    try body.appendSlice(a, &x);
    try body.appendSlice(a, &.{ 0, 0, 0, 0, 0x3a, 0x30, 0, 0, 1, 0, 0, 0, 0, 0 });
    std.mem.writeInt(u16, x[0..2], @intCast(rows.len - 1), .little);
    try body.appendSlice(a, x[0..2]);
    try body.appendSlice(a, &.{ 16, 0, 0, 0 });
    for (rows) |v| {
        std.mem.writeInt(u16, x[0..2], v, .little);
        try body.appendSlice(a, x[0..2]);
    }
    var out = std.ArrayListUnmanaged(u8).empty;
    try out.append(a, 1);
    std.mem.writeInt(u32, x[0..4], @intCast(body.items.len), .big);
    try out.appendSlice(a, x[0..4]);
    try out.appendSlice(a, body.items);
    std.mem.writeInt(u32, x[0..4], std.hash.Crc32.hash(body.items), .big);
    try out.appendSlice(a, x[0..4]);
    return out.items;
}

test "a stored deletion vector: framing, size, CRC-32, magic and version are each checked" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.mem.concat(a, u8, &.{ try tmp.dir.realpathAlloc(a, "."), "/" });
    const good = try dvFile(a, &.{ 0, 9 });
    const name = "deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin";
    const r = Ref{ .name = "T", .root = root, .file = "f.parquet", .d = .{ .storage = 'u', .where = "vBn[lx{q8@P<9BNH/isA", .offset = 1, .size = @intCast(good.len - 9), .cardinality = 2 } };
    try tmp.dir.writeFile(.{ .sub_path = name, .data = good });
    const bm = try load(a, &r, 10);
    try testing.expectEqual(@as(u64, 2), bm.cardinality);
    try testing.expectEqual(@as(u64, 10), bm.end);

    const cases = [_]struct { at: usize, v: u8, want: []const u8 }{
        .{ .at = 0, .v = 2, .want = "is format version 2; basalt reads version 1" },
        .{ .at = 4, .v = 0x30, .want = "holds 48 bytes where the log says" },
        .{ .at = 5, .v = 0xd0, .want = "fails its CRC-32 check" },
        .{ .at = good.len - 1, .v = 0, .want = "fails its CRC-32 check" },
    };
    for (cases) |c| {
        const b = try a.dupe(u8, good);
        b[c.at] = c.v;
        try tmp.dir.writeFile(.{ .sub_path = name, .data = b });
        try testing.expectError(table.Error.LakeTableRefused, load(a, &r, 10));
        const msg = @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?;
        if (std.mem.indexOf(u8, msg, c.want) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ c.want, msg });
            return error.TestUnexpectedResult;
        }
    }
    const native = try a.dupe(u8, good);
    std.mem.writeInt(u32, native[5..9], native_magic, .little);
    std.mem.writeInt(u32, native[native.len - 4 ..][0..4], std.hash.Crc32.hash(native[5 .. native.len - 4]), .big);
    try tmp.dir.writeFile(.{ .sub_path = name, .data = native });
    try testing.expectError(table.Error.LakeTableRefused, load(a, &r, 10));
    try testing.expect(std.mem.indexOf(u8, @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?, "native RoaringBitmapArray format") != null);
    const other = try a.dupe(u8, native);
    std.mem.writeInt(u32, other[5..9], 0x12345678, .little);
    std.mem.writeInt(u32, other[other.len - 4 ..][0..4], std.hash.Crc32.hash(other[5 .. other.len - 4]), .big);
    try tmp.dir.writeFile(.{ .sub_path = name, .data = other });
    try testing.expectError(table.Error.LakeTableRefused, load(a, &r, 10));
    try testing.expect(std.mem.indexOf(u8, @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?, "does not start with the portable RoaringBitmapArray magic") != null);
    try tmp.dir.writeFile(.{ .sub_path = name, .data = good[0..20] });
    try testing.expectError(table.Error.LakeTableRefused, load(a, &r, 10));
    try testing.expect(std.mem.indexOf(u8, @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?, "is shorter than the log says") != null);
    try tmp.dir.deleteFile(name);
    try testing.expectError(table.Error.LakeTableRefused, load(a, &r, 10));
    try testing.expect(std.mem.indexOf(u8, @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?, "could not be opened (FileNotFound)") != null);
}

test "descriptors: storage types, offsets and sizes are checked before any read" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    for ([_][2][]const u8{
        .{ "{\"storageType\":\"r\",\"pathOrInlineDv\":\"x.bin\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":2}", "stored as `r`" },
        .{ "{\"storageType\":\"z\",\"pathOrInlineDv\":\"x\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":2}", "stored as `z`, a storage type the Delta protocol does not define" },
        .{ "{\"storageType\":\"u\",\"pathOrInlineDv\":\"#####aaaaaaaaaaaaaaa\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":2}", "its path does not end in a Z85 UUID" },
        .{ "{\"storageType\":\"u\",\"pathOrInlineDv\":\"short\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":2}", "its path does not end in a Z85 UUID" },
        .{ "{\"storageType\":\"u\",\"pathOrInlineDv\":\"vBn[lx{q8@P<9BNH/isA\",\"offset\":0,\"sizeInBytes\":36,\"cardinality\":2}", "its offset is before" },
        .{ "{\"storageType\":\"i\",\"pathOrInlineDv\":\"HelloWorld\",\"offset\":1,\"sizeInBytes\":8,\"cardinality\":2}", "it is inline but has an offset" },
        .{ "{\"storageType\":\"i\",\"pathOrInlineDv\":\"HelloWorld\",\"sizeInBytes\":12,\"cardinality\":2}", "not Z85 text of its size" },
        .{ "{\"storageType\":\"u\",\"pathOrInlineDv\":\"vBn[lx{q8@P<9BNH/isA\",\"offset\":1,\"cardinality\":2}", "it has no sizeInBytes" },
        .{ "{\"storageType\":\"u\",\"pathOrInlineDv\":\"vBn[lx{q8@P<9BNH/isA\",\"offset\":1,\"sizeInBytes\":36,\"cardinality\":-1}", "its cardinality is negative" },
    }) |c| {
        const v = try std.json.parseFromSliceLeaky(std.json.Value, a, c[0], .{});
        try testing.expectError(table.Error.LakeTableRefused, descriptor("T", "f.parquet", v));
        const msg = @import("../../exec/eval.zig").takeFailure(table.Error.LakeTableRefused).?;
        if (std.mem.indexOf(u8, msg, c[1]) == null) {
            std.debug.print("want `{s}` in `{s}`\n", .{ c[1], msg });
            return error.TestUnexpectedResult;
        }
    }
}
