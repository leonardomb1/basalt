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
};

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
    valid: std.ArrayList(bool),
    store: Store,

    pub const Store = union(enum) {
        b: std.ArrayList(bool),
        i32: std.ArrayList(i32),
        i64: std.ArrayList(i64),
        f64: std.ArrayList(f64),
        dec: std.ArrayList(value.Decimal),
        bytes: std.ArrayList([]const u8),
    };

    pub fn init(arena: std.mem.Allocator, ty: types.Type) Builder {
        const store: Store = switch (ty.kind) {
            .bool => .{ .b = std.ArrayList(bool).init(arena) },
            .date => .{ .i32 = std.ArrayList(i32).init(arena) },
            .int, .time, .timestamp => .{ .i64 = std.ArrayList(i64).init(arena) },
            .float => .{ .f64 = std.ArrayList(f64).init(arena) },
            .decimal => .{ .dec = std.ArrayList(value.Decimal).init(arena) },
            .string, .bytes => .{ .bytes = std.ArrayList([]const u8).init(arena) },
            .array, .@"struct" => .{ .bytes = std.ArrayList([]const u8).init(arena) }, // unsupported; placeholder
        };
        return .{ .arena = arena, .ty = ty, .valid = std.ArrayList(bool).init(arena), .store = store };
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
