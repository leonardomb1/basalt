//! Columnar storage: a typed, struct-of-arrays column with an out-of-band
//! validity bitmap (Arrow convention: bit set = valid/non-null). This is the
//! hot-path data layout — one contiguous buffer per column, indexed by row.

const std = @import("std");
const types = @import("../lang/types.zig");
const value = @import("value.zig");

const Value = value.Value;

/// Bit-packed validity: 1 = valid (non-null), 0 = null.
pub const Bitmap = struct {
    bits: []u8,
    len: usize,

    pub fn initFull(alloc: std.mem.Allocator, n: usize) !Bitmap {
        const nbytes = (n + 7) / 8;
        const bits = try alloc.alloc(u8, nbytes);
        @memset(bits, 0xFF);
        return .{ .bits = bits, .len = n };
    }

    pub fn get(self: Bitmap, i: usize) bool {
        const shift: u3 = @intCast(i & 7);
        return (self.bits[i >> 3] >> shift) & 1 != 0;
    }

    pub fn setValid(self: *Bitmap, i: usize, valid: bool) void {
        const shift: u3 = @intCast(i & 7);
        const mask = @as(u8, 1) << shift;
        if (valid) {
            self.bits[i >> 3] |= mask;
        } else {
            self.bits[i >> 3] &= ~mask;
        }
    }

    /// True if the first `n` bits are all set (no nulls) — lets kernels take a
    /// branch-free fast path. Whole bytes are checked at once.
    pub fn allSet(self: Bitmap, n: usize) bool {
        if (n == 0) return true;
        const full = n >> 3;
        var i: usize = 0;
        while (i < full) : (i += 1) {
            if (self.bits[i] != 0xFF) return false;
        }
        const rem: u3 = @intCast(n & 7);
        if (rem != 0) {
            const mask = (@as(u8, 1) << rem) - 1;
            if ((self.bits[full] & mask) != mask) return false;
        }
        return true;
    }
};

/// Copy a typed backing slice, keeping only rows where `keep[i]` is true.
fn gatherSlice(comptime T: type, arena: std.mem.Allocator, src: []const T, keep: []const bool, kept: usize) ![]T {
    const out = try arena.alloc(T, kept);
    var w: usize = 0;
    for (keep, 0..) |k, i| {
        if (k) {
            out[w] = src[i];
            w += 1;
        }
    }
    return out;
}

fn gatherValidity(arena: std.mem.Allocator, v: Bitmap, keep: []const bool, kept: usize) !Bitmap {
    var bm = try Bitmap.initFull(arena, kept);
    var w: usize = 0;
    for (keep, 0..) |k, i| {
        if (k) {
            if (!v.get(i)) bm.setValid(w, false);
            w += 1;
        }
    }
    return bm;
}

/// Select the `kept` rows of `c` flagged in `keep` into a fresh column, copying
/// the typed buffer directly (no per-row `Value` boxing). The hot filter path.
pub fn gather(arena: std.mem.Allocator, c: Column, keep: []const bool, kept: usize) !Column {
    const bm = try gatherValidity(arena, c.validity, keep, kept);
    const data: Column.Data = switch (c.data) {
        .b => |s| .{ .b = try gatherSlice(bool, arena, s, keep, kept) },
        .i32 => |s| .{ .i32 = try gatherSlice(i32, arena, s, keep, kept) },
        .i64 => |s| .{ .i64 = try gatherSlice(i64, arena, s, keep, kept) },
        .f64 => |s| .{ .f64 = try gatherSlice(f64, arena, s, keep, kept) },
        .dec => |s| .{ .dec = try gatherSlice(value.Decimal, arena, s, keep, kept) },
        .bytes => |s| .{ .bytes = try gatherSlice([]const u8, arena, s, keep, kept) },
    };
    return .{ .ty = c.ty, .len = kept, .validity = bm, .data = data };
}

/// Concatenate same-typed column chunks into one column of `total` rows by
/// copying the typed backing slices directly — no per-row `Value` boxing. Byte
/// slices are NOT re-duped: the caller guarantees the chunks' bytes live at
/// least as long as the output (e.g. all in the same arena).
pub fn concat(arena: std.mem.Allocator, chunks: []const Column, total: usize) !Column {
    std.debug.assert(chunks.len > 0);
    var bm = try Bitmap.initFull(arena, total);
    {
        var off: usize = 0;
        for (chunks) |c| {
            if (!c.validity.allSet(c.len)) {
                var i: usize = 0;
                while (i < c.len) : (i += 1) {
                    if (!c.validity.get(i)) bm.setValid(off + i, false);
                }
            }
            off += c.len;
        }
    }
    const data: Column.Data = switch (chunks[0].data) {
        .b => .{ .b = try concatSlices("b", bool, arena, chunks, total) },
        .i32 => .{ .i32 = try concatSlices("i32", i32, arena, chunks, total) },
        .i64 => .{ .i64 = try concatSlices("i64", i64, arena, chunks, total) },
        .f64 => .{ .f64 = try concatSlices("f64", f64, arena, chunks, total) },
        .dec => .{ .dec = try concatSlices("dec", value.Decimal, arena, chunks, total) },
        .bytes => .{ .bytes = try concatSlices("bytes", []const u8, arena, chunks, total) },
    };
    return .{ .ty = chunks[0].ty, .len = total, .validity = bm, .data = data };
}

fn concatSlices(comptime tag: []const u8, comptime T: type, arena: std.mem.Allocator, chunks: []const Column, total: usize) ![]T {
    const out = try arena.alloc(T, total);
    var off: usize = 0;
    for (chunks) |c| {
        @memcpy(out[off..][0..c.len], @field(c.data, tag)[0..c.len]);
        off += c.len;
    }
    return out;
}

/// Reorder a column by `idx` (`out[i] = c[idx[i]]`) into a fresh column, copying
/// the typed buffer directly (no per-row `Value` boxing). The sort output path.
pub fn permute(arena: std.mem.Allocator, c: Column, idx: []const usize) !Column {
    var bm = try Bitmap.initFull(arena, idx.len);
    if (!c.validity.allSet(c.len)) {
        for (idx, 0..) |r, i| {
            if (!c.validity.get(r)) bm.setValid(i, false);
        }
    }
    const data: Column.Data = switch (c.data) {
        .b => |s| .{ .b = try permuteSlice(bool, arena, s, idx) },
        .i32 => |s| .{ .i32 = try permuteSlice(i32, arena, s, idx) },
        .i64 => |s| .{ .i64 = try permuteSlice(i64, arena, s, idx) },
        .f64 => |s| .{ .f64 = try permuteSlice(f64, arena, s, idx) },
        .dec => |s| .{ .dec = try permuteSlice(value.Decimal, arena, s, idx) },
        .bytes => |s| .{ .bytes = try permuteSlice([]const u8, arena, s, idx) },
    };
    return .{ .ty = c.ty, .len = idx.len, .validity = bm, .data = data };
}

fn permuteSlice(comptime T: type, arena: std.mem.Allocator, src: []const T, idx: []const usize) ![]T {
    const out = try arena.alloc(T, idx.len);
    for (idx, 0..) |r, i| out[i] = src[r];
    return out;
}

pub const Column = struct {
    ty: types.Type,
    len: usize,
    validity: Bitmap,
    data: Data,

    /// Physical backing store, keyed by storage width rather than logical kind
    /// (e.g. int/time/timestamp all share `i64`, string/bytes share `bytes`).
    pub const Data = union(enum) {
        b: []bool,
        i32: []i32, // date
        i64: []i64, // int, time, timestamp
        f64: []f64, // float
        dec: []value.Decimal,
        bytes: [][]const u8, // string (utf-8), bytes
    };

    /// Boxed read of row `i`, honoring the validity bitmap.
    pub fn getValue(self: Column, i: usize) Value {
        if (!self.validity.get(i)) return .null;
        return switch (self.ty.kind) {
            .bool => .{ .bool = self.data.b[i] },
            .int => .{ .int = self.data.i64[i] },
            .float => .{ .float = self.data.f64[i] },
            .decimal => .{ .decimal = self.data.dec[i] },
            .string => .{ .string = self.data.bytes[i] },
            .bytes => .{ .bytes = self.data.bytes[i] },
            .date => .{ .date = self.data.i32[i] },
            .time => .{ .time = self.data.i64[i] },
            .timestamp => .{ .timestamp = self.data.i64[i] },
            .array, .@"struct" => .null, // composite columns are deferred past M0
        };
    }
};

/// Accumulates values one row at a time into the correct physical store, then
/// `finish()`es into an immutable `Column`. Strings are duped into the arena.
pub const Builder = struct {
    arena: std.mem.Allocator,
    ty: types.Type,
    valid: std.array_list.Managed(bool),
    store: Store,

    pub const Store = union(enum) {
        b: std.array_list.Managed(bool),
        i32: std.array_list.Managed(i32),
        i64: std.array_list.Managed(i64),
        f64: std.array_list.Managed(f64),
        dec: std.array_list.Managed(value.Decimal),
        bytes: std.array_list.Managed([]const u8),
    };

    pub fn init(arena: std.mem.Allocator, ty: types.Type) Builder {
        const store: Store = switch (ty.kind) {
            .bool => .{ .b = std.array_list.Managed(bool).init(arena) },
            .date => .{ .i32 = std.array_list.Managed(i32).init(arena) },
            .int, .time, .timestamp => .{ .i64 = std.array_list.Managed(i64).init(arena) },
            .float => .{ .f64 = std.array_list.Managed(f64).init(arena) },
            .decimal => .{ .dec = std.array_list.Managed(value.Decimal).init(arena) },
            .string, .bytes => .{ .bytes = std.array_list.Managed([]const u8).init(arena) },
            .array, .@"struct" => .{ .bytes = std.array_list.Managed([]const u8).init(arena) }, // unsupported; placeholder
        };
        return .{ .arena = arena, .ty = ty, .valid = std.array_list.Managed(bool).init(arena), .store = store };
    }

    pub fn append(self: *Builder, v: Value) !void {
        const ok = !v.isNull();
        try self.valid.append(ok);
        switch (self.store) {
            .b => |*l| try l.append(if (ok) v.bool else false),
            .i32 => |*l| try l.append(if (ok) v.date else 0),
            .i64 => |*l| try l.append(if (ok) self.asI64(v) else 0),
            .f64 => |*l| try l.append(if (ok) self.asF64(v) else 0),
            .dec => |*l| try l.append(if (ok) v.decimal else .{ .unscaled = 0, .scale = self.ty.scale }),
            .bytes => |*l| try l.append(if (ok) try self.arena.dupe(u8, self.asBytes(v)) else ""),
        }
    }

    fn asI64(self: *Builder, v: Value) i64 {
        return switch (self.ty.kind) {
            .time => v.time,
            .timestamp => v.timestamp,
            else => switch (v) {
                .int => |x| x,
                .float => |x| @intFromFloat(x),
                else => 0,
            },
        };
    }
    fn asF64(_: *Builder, v: Value) f64 {
        return switch (v) {
            .float => |x| x,
            .int => |x| @floatFromInt(x),
            else => 0,
        };
    }
    fn asBytes(self: *Builder, v: Value) []const u8 {
        return switch (self.ty.kind) {
            .bytes => v.bytes,
            else => switch (v) {
                .string => |s| s,
                .bytes => |s| s,
                else => "",
            },
        };
    }

    pub fn finish(self: *Builder) !Column {
        const n = self.valid.items.len;
        var bm = try Bitmap.initFull(self.arena, n);
        for (self.valid.items, 0..) |is_valid, i| {
            if (!is_valid) bm.setValid(i, false);
        }
        const data: Column.Data = switch (self.store) {
            .b => |*l| .{ .b = try l.toOwnedSlice() },
            .i32 => |*l| .{ .i32 = try l.toOwnedSlice() },
            .i64 => |*l| .{ .i64 = try l.toOwnedSlice() },
            .f64 => |*l| .{ .f64 = try l.toOwnedSlice() },
            .dec => |*l| .{ .dec = try l.toOwnedSlice() },
            .bytes => |*l| .{ .bytes = try l.toOwnedSlice() },
        };
        return .{ .ty = self.ty, .len = n, .validity = bm, .data = data };
    }
};

/// Build an `int` column from optional values (null where `null`). Test/helper.
pub fn intColumn(alloc: std.mem.Allocator, vals: []const ?i64) !Column {
    var validity = try Bitmap.initFull(alloc, vals.len);
    const store = try alloc.alloc(i64, vals.len);
    for (vals, 0..) |v, i| {
        if (v) |x| {
            store[i] = x;
        } else {
            store[i] = 0;
            validity.setValid(i, false);
        }
    }
    return .{
        .ty = types.Type.init(.int),
        .len = vals.len,
        .validity = validity,
        .data = .{ .i64 = store },
    };
}

test "int column round-trips values and nulls" {
    const alloc = std.testing.allocator;
    const c = try intColumn(alloc, &.{ 1, null, 3 });
    defer {
        alloc.free(c.validity.bits);
        alloc.free(c.data.i64);
    }

    try std.testing.expectEqual(@as(i64, 1), c.getValue(0).int);
    try std.testing.expect(c.getValue(1).isNull());
    try std.testing.expectEqual(@as(i64, 3), c.getValue(2).int);
}

test "builder assembles a nullable string column" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var b = Builder.init(arena.allocator(), types.Type.init(.string).asNullable());
    try b.append(.{ .string = "a" });
    try b.append(.null);
    try b.append(.{ .string = "c" });
    const c = try b.finish();
    try std.testing.expectEqual(@as(usize, 3), c.len);
    try std.testing.expectEqualStrings("a", c.getValue(0).string);
    try std.testing.expect(c.getValue(1).isNull());
    try std.testing.expectEqualStrings("c", c.getValue(2).string);
}

test "concat joins typed chunks and carries nulls across offsets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c1 = try intColumn(a, &.{ 1, null, 3 });
    const c2 = try intColumn(a, &.{ null, 5 });
    const out = try concat(a, &.{ c1, c2 }, 5);
    try std.testing.expectEqual(@as(usize, 5), out.len);
    try std.testing.expectEqual(@as(i64, 1), out.getValue(0).int);
    try std.testing.expect(out.getValue(1).isNull());
    try std.testing.expectEqual(@as(i64, 3), out.getValue(2).int);
    try std.testing.expect(out.getValue(3).isNull());
    try std.testing.expectEqual(@as(i64, 5), out.getValue(4).int);
}

test "permute reorders values and validity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = try intColumn(a, &.{ 10, null, 30 });
    const out = try permute(a, c, &.{ 2, 0, 1 });
    try std.testing.expectEqual(@as(i64, 30), out.getValue(0).int);
    try std.testing.expectEqual(@as(i64, 10), out.getValue(1).int);
    try std.testing.expect(out.getValue(2).isNull());
}

test "bitmap set/get across byte boundaries" {
    const alloc = std.testing.allocator;
    var bm = try Bitmap.initFull(alloc, 20);
    defer alloc.free(bm.bits);

    bm.setValid(0, false);
    bm.setValid(9, false);
    bm.setValid(19, false);
    try std.testing.expect(!bm.get(0));
    try std.testing.expect(bm.get(1));
    try std.testing.expect(!bm.get(9));
    try std.testing.expect(bm.get(10));
    try std.testing.expect(!bm.get(19));
}
