//! Columnar storage: a typed, struct-of-arrays column with an out-of-band
//! validity bitmap (Arrow convention: bit set = valid/non-null). This is the
//! hot-path data layout, one contiguous buffer per column indexed by row.
//!
//! `Data` is keyed by storage width, not logical kind (int/time/timestamp share
//! `i64`, string/bytes share `bytes`). Byte payloads use the Arrow
//! variable-size binary layout: one `values` buffer plus `len + 1` offsets, which
//! costs 4 bytes per row instead of a 16-byte slice and one allocation per column;
//! a null row is an empty slot. `gather`, `concat` and `permute` copy typed
//! buffers directly with no per-row `Value` boxing, and their output never borrows
//! from the input. A string column read from a dictionary keeps it (`dict`, row
//! `i` is `values[codes[i]]`, a null row's code is 0) so filters and GROUP BY can
//! work on the entries; anything that makes a new column leaves it behind.
//!
//! `Builder` writes validity directly in its final bit-packed form, appended
//! pre-set to all-valid, so a null-free column never touches it beyond growing it
//! (it was once a `[]bool` shadow array folded down in `finish`). The bulk and
//! typed appends exist because boxing every value into a `Value` measured several
//! times slower than the typed path, and any single null used to force it.

const std = @import("std");
const types = @import("../lang/types.zig");
const value = @import("value.zig");

const Value = value.Value;

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

    /// True if the first `n` bits are all set (no nulls). Checked eight bytes at a
    /// time: it runs ahead of nearly every kernel, so the scan width is the whole cost.
    pub fn allSet(self: Bitmap, n: usize) bool {
        if (n == 0) return true;
        const full = n >> 3;
        var i: usize = 0;
        while (i + @sizeOf(u64) <= full) : (i += @sizeOf(u64)) {
            const word = std.mem.readInt(u64, self.bits[i..][0..@sizeOf(u64)], .little);
            if (word != std.math.maxInt(u64)) return false;
        }
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

pub const Bytes = struct {
    offsets: []i32,
    values: []u8,

    pub fn at(self: Bytes, i: usize) []const u8 {
        const lo: usize = @intCast(self.offsets[i]);
        const hi: usize = @intCast(self.offsets[i + 1]);
        return self.values[lo..hi];
    }

    fn spanOf(self: Bytes, rows_idx: []const usize) usize {
        var total: usize = 0;
        for (rows_idx) |r| total += self.at(r).len;
        return total;
    }

    fn take(self: Bytes, arena: std.mem.Allocator, rows_idx: []const usize) !Bytes {
        const values = try arena.alloc(u8, self.spanOf(rows_idx));
        const offsets = try arena.alloc(i32, rows_idx.len + 1);
        offsets[0] = 0;
        var off: usize = 0;
        for (rows_idx, 0..) |r, w| {
            const s = self.at(r);
            @memcpy(values[off..][0..s.len], s);
            off += s.len;
            offsets[w + 1] = @intCast(off);
        }
        return .{ .offsets = offsets, .values = values };
    }
};

pub const BytesAppender = struct {
    values: std.array_list.Managed(u8),
    offsets: std.array_list.Managed(i32),

    pub fn init(arena: std.mem.Allocator, n: usize) !BytesAppender {
        var offsets = std.array_list.Managed(i32).init(arena);
        try offsets.ensureTotalCapacity(n + 1);
        offsets.appendAssumeCapacity(0);
        return .{ .values = std.array_list.Managed(u8).init(arena), .offsets = offsets };
    }

    pub fn append(self: *BytesAppender, s: []const u8) !void {
        try self.values.appendSlice(s);
    }

    pub fn endRow(self: *BytesAppender) !void {
        try self.offsets.append(@intCast(self.values.items.len));
    }

    pub fn push(self: *BytesAppender, s: []const u8) !void {
        try self.append(s);
        try self.endRow();
    }

    pub fn pushNull(self: *BytesAppender) !void {
        try self.endRow();
    }

    /// Appends `s` as a whole row and returns the just-written region, so a kernel
    /// can transform it in place instead of duping first.
    pub fn pushMutable(self: *BytesAppender, s: []const u8) ![]u8 {
        const start = self.values.items.len;
        try self.push(s);
        return self.values.items[start..];
    }

    pub fn finish(self: *BytesAppender) !Bytes {
        return .{ .offsets = try self.offsets.toOwnedSlice(), .values = try self.values.toOwnedSlice() };
    }
};

fn keptIndices(arena: std.mem.Allocator, keep: []const bool, kept: usize) ![]usize {
    const idx = try arena.alloc(usize, kept);
    var w: usize = 0;
    for (keep, 0..) |k, i| {
        if (k) {
            idx[w] = i;
            w += 1;
        }
    }
    return idx;
}

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

/// Returns the all-valid bitmap untouched when the source has no nulls, as
/// `concat` and `permute` do: this backs the filter hot path.
fn gatherValidity(arena: std.mem.Allocator, v: Bitmap, keep: []const bool, kept: usize) !Bitmap {
    var bm = try Bitmap.initFull(arena, kept);
    if (v.allSet(v.len)) return bm;
    var w: usize = 0;
    for (keep, 0..) |k, i| {
        if (k) {
            if (!v.get(i)) bm.setValid(w, false);
            w += 1;
        }
    }
    return bm;
}

pub fn gather(arena: std.mem.Allocator, c: Column, keep: []const bool, kept: usize) !Column {
    const bm = try gatherValidity(arena, c.validity, keep, kept);
    const data: Column.Data = switch (c.data) {
        .b => |s| .{ .b = try gatherSlice(bool, arena, s, keep, kept) },
        .i32 => |s| .{ .i32 = try gatherSlice(i32, arena, s, keep, kept) },
        .i64 => |s| .{ .i64 = try gatherSlice(i64, arena, s, keep, kept) },
        .f64 => |s| .{ .f64 = try gatherSlice(f64, arena, s, keep, kept) },
        .dec => |s| .{ .dec = try gatherSlice(value.Decimal, arena, s, keep, kept) },
        .bytes => |s| .{ .bytes = try s.take(arena, try keptIndices(arena, keep, kept)) },
    };
    return .{ .ty = c.ty, .len = kept, .validity = bm, .data = data };
}

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
        .bytes => .{ .bytes = try concatBytes(arena, chunks, total) },
    };
    return .{ .ty = chunks[0].ty, .len = total, .validity = bm, .data = data };
}

fn concatBytes(arena: std.mem.Allocator, chunks: []const Column, total: usize) !Bytes {
    var span: usize = 0;
    for (chunks) |c| {
        const b = c.data.bytes;
        span += @as(usize, @intCast(b.offsets[c.len])) - @as(usize, @intCast(b.offsets[0]));
    }
    const values = try arena.alloc(u8, span);
    const offsets = try arena.alloc(i32, total + 1);
    offsets[0] = 0;

    var off: usize = 0;
    var w: usize = 0;
    for (chunks) |c| {
        const b = c.data.bytes;
        const lo: usize = @intCast(b.offsets[0]);
        const hi: usize = @intCast(b.offsets[c.len]);
        @memcpy(values[off..][0 .. hi - lo], b.values[lo..hi]);
        var i: usize = 0;
        while (i < c.len) : (i += 1) {
            w += 1;
            offsets[w] = @intCast(off + @as(usize, @intCast(b.offsets[i + 1])) - lo);
        }
        off += hi - lo;
    }
    return .{ .offsets = offsets, .values = values };
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
        .bytes => |s| .{ .bytes = try s.take(arena, idx) },
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
    dict: ?*const Dict = null,

    pub const Dict = struct { values: []const []const u8, codes: []const u32 };

    pub const Data = union(enum) {
        b: []bool,
        i32: []i32,
        i64: []i64,
        f64: []f64,
        dec: []value.Decimal,
        bytes: Bytes,
    };

    pub fn getValue(self: Column, i: usize) Value {
        if (!self.validity.get(i)) return .null;
        return switch (self.ty.kind) {
            .bool => .{ .bool = self.data.b[i] },
            .int => .{ .int = self.data.i64[i] },
            .float => .{ .float = self.data.f64[i] },
            .decimal => .{ .decimal = self.data.dec[i] },
            .string => .{ .string = self.data.bytes.at(i) },
            .bytes => .{ .bytes = self.data.bytes.at(i) },
            .date => .{ .date = self.data.i32[i] },
            .time => .{ .time = self.data.i64[i] },
            .timestamp => .{ .timestamp = self.data.i64[i] },
            .array, .@"struct" => .null,
        };
    }
};

pub const Builder = struct {
    arena: std.mem.Allocator,
    ty: types.Type,
    dict_src: ?usize = null,
    dict_vals: ?[]const []const u8 = null,
    dict_codes: std.ArrayListUnmanaged(u32) = .empty,
    bits: std.array_list.Managed(u8),
    rows: usize = 0,
    store: Store,

    pub const BytesStore = struct {
        values: std.array_list.Managed(u8),
        ends: std.array_list.Managed(i32),
    };

    pub const Store = union(enum) {
        b: std.array_list.Managed(bool),
        i32: std.array_list.Managed(i32),
        i64: std.array_list.Managed(i64),
        f64: std.array_list.Managed(f64),
        dec: std.array_list.Managed(value.Decimal),
        bytes: BytesStore,
    };

    pub fn init(arena: std.mem.Allocator, ty: types.Type) Builder {
        const bytes_store: Store = .{ .bytes = .{
            .values = std.array_list.Managed(u8).init(arena),
            .ends = std.array_list.Managed(i32).init(arena),
        } };
        const store: Store = switch (ty.kind) {
            .bool => .{ .b = std.array_list.Managed(bool).init(arena) },
            .date => .{ .i32 = std.array_list.Managed(i32).init(arena) },
            .int, .time, .timestamp => .{ .i64 = std.array_list.Managed(i64).init(arena) },
            .float => .{ .f64 = std.array_list.Managed(f64).init(arena) },
            .decimal => .{ .dec = std.array_list.Managed(value.Decimal).init(arena) },
            .string, .bytes, .array, .@"struct" => bytes_store,
        };
        return .{ .arena = arena, .ty = ty, .bits = std.array_list.Managed(u8).init(arena), .store = store };
    }

    fn pushValid(self: *Builder, ok: bool) !void {
        if (self.rows & 7 == 0) try self.bits.append(0xFF);
        if (!ok) self.bits.items[self.rows >> 3] &= ~(@as(u8, 1) << @intCast(self.rows & 7));
        self.rows += 1;
    }

    /// Records `count` valid rows, a whole byte at a time; bits of the current
    /// partial byte are set, since nothing clears one except an actual null.
    fn pushValidRun(self: *Builder, count: usize) !void {
        const end = self.rows + count;
        const need = (end + 7) / 8;
        try self.bits.ensureTotalCapacity(need);
        while (self.bits.items.len < need) self.bits.appendAssumeCapacity(0xFF);
        self.rows = end;
    }

    const BYTES_PER_ROW_HINT = 24;

    /// `init` with the buffers pre-sized for `rows`; otherwise the values buffer
    /// grows by doubling and each growth copies everything appended so far.
    pub fn initCapacity(arena: std.mem.Allocator, ty: types.Type, rows: usize) !Builder {
        var b = init(arena, ty);
        try b.bits.ensureTotalCapacity((rows + 7) / 8);
        switch (b.store) {
            .bytes => |*l| {
                try l.ends.ensureTotalCapacity(rows);
                try l.values.ensureTotalCapacity(rows * BYTES_PER_ROW_HINT);
            },
            inline else => |*l| try l.ensureTotalCapacity(rows),
        }
        return b;
    }

    pub fn appendBulk(self: *Builder, comptime T: type, vals: []const T) !void {
        switch (self.store) {
            inline .b, .i32, .i64, .f64, .dec => |*l| {
                if (@TypeOf(l.items) != []T) return error.BulkTypeMismatch;
                try l.appendSlice(vals);
            },
            .bytes => return error.BulkTypeMismatch,
        }
        try self.pushValidRun(vals.len);
    }

    pub fn appendBytesScattered(self: *Builder, vals: []const []const u8, defs: ?[]const u32, max_def: u32) !void {
        if (self.store != .bytes) return error.BulkTypeMismatch;
        const l = &self.store.bytes;
        const d = defs orelse {
            try l.ends.ensureUnusedCapacity(vals.len);
            for (vals) |s| {
                try l.values.appendSlice(s);
                l.ends.appendAssumeCapacity(@intCast(l.values.items.len));
            }
            try self.pushValidRun(vals.len);
            return;
        };
        try l.ends.ensureUnusedCapacity(d.len);
        var j: usize = 0;
        for (d) |lvl| {
            const present = lvl == max_def;
            if (present) {
                if (j >= vals.len) return error.BulkTypeMismatch;
                try l.values.appendSlice(vals[j]);
                j += 1;
            }
            l.ends.appendAssumeCapacity(@intCast(l.values.items.len));
            try self.pushValid(present);
        }
    }

    /// Bulk append where only present rows supply a value: `defs[i] == max_def`
    /// marks a row present, and a null consumes a slot without one.
    pub fn appendBulkScattered(
        self: *Builder,
        comptime T: type,
        vals: []const T,
        defs: []const u32,
        max_def: u32,
    ) !void {
        switch (self.store) {
            inline .b, .i32, .i64, .f64, .dec => |*l| {
                if (@TypeOf(l.items) != []T) return error.BulkTypeMismatch;
                try l.ensureUnusedCapacity(defs.len);
                try self.bits.ensureTotalCapacity((self.rows + defs.len + 7) / 8);
                var j: usize = 0;
                for (defs) |d| {
                    const present = d == max_def;
                    if (present) {
                        if (j >= vals.len) return error.BulkTypeMismatch;
                        l.appendAssumeCapacity(vals[j]);
                        j += 1;
                    } else {
                        l.appendAssumeCapacity(std.mem.zeroes(T));
                    }
                    try self.pushValid(present);
                }
            },
            .bytes => return error.BulkTypeMismatch,
        }
    }

    /// Typed appends for a producer that already knows the cell's kind (the CSV
    /// reader). The caller guarantees the store matches.
    pub fn appendInt(self: *Builder, x: i64) !void {
        try self.pushValid(true);
        try self.store.i64.append(x);
    }
    pub fn appendFloat(self: *Builder, x: f64) !void {
        try self.pushValid(true);
        try self.store.f64.append(x);
    }
    pub fn appendStr(self: *Builder, s: []const u8) !void {
        try self.pushValid(true);
        const l = &self.store.bytes;
        try l.values.appendSlice(s);
        try l.ends.append(@intCast(l.values.items.len));
    }

    pub fn append(self: *Builder, v: Value) !void {
        const ok = !v.isNull();
        try self.pushValid(ok);
        switch (self.store) {
            .b => |*l| try l.append(if (ok) v.bool else false),
            .i32 => |*l| try l.append(if (ok) v.date else 0),
            .i64 => |*l| try l.append(if (ok) self.asI64(v) else 0),
            .f64 => |*l| try l.append(if (ok) self.asF64(v) else 0),
            .dec => |*l| try l.append(if (ok) v.decimal else .{ .unscaled = 0, .scale = self.ty.scale }),
            .bytes => |*l| {
                if (ok) try l.values.appendSlice(self.asBytes(v));
                try l.ends.append(@intCast(l.values.items.len));
            },
        }
    }

    /// `else => 0` once swallowed decimals appended to a numeric column; a kind with
    /// no numeric reading (string, date) still becomes 0 rather than widening `append`'s error set.
    fn asI64(self: *Builder, v: Value) i64 {
        return switch (self.ty.kind) {
            .time => v.time,
            .timestamp => v.timestamp,
            else => switch (v) {
                .int => |x| x,
                .float => |x| @intFromFloat(x),
                .bool => |x| @intFromBool(x),
                .decimal => |d| @intFromFloat(d.toF64()),
                else => 0,
            },
        };
    }

    /// The missing `.decimal` arm once made `ROUND(AVG(CAST(x AS DECIMAL(p,s))), n)`
    /// answer 0, since AVG materializes its argument as float; keep every numeric kind here.
    fn asF64(_: *Builder, v: Value) f64 {
        return switch (v) {
            .float => |x| x,
            .int => |x| @floatFromInt(x),
            .bool => |x| @floatFromInt(@intFromBool(x)),
            .decimal => |d| d.toF64(),
            else => 0,
        };
    }
    /// Takes either `.string` or `.bytes`: a text-protocol driver decodes a binary
    /// column into `.string`, and keying off the column's kind panicked on that.
    fn asBytes(self: *Builder, v: Value) []const u8 {
        _ = self;
        return switch (v) {
            .string => |s| s,
            .bytes => |s| s,
            else => "",
        };
    }

    pub fn finish(self: *Builder) !Column {
        const n = self.rows;
        const bm = Bitmap{ .bits = try self.bits.toOwnedSlice(), .len = n };
        const data: Column.Data = switch (self.store) {
            .b => |*l| .{ .b = try l.toOwnedSlice() },
            .i32 => |*l| .{ .i32 = try l.toOwnedSlice() },
            .i64 => |*l| .{ .i64 = try l.toOwnedSlice() },
            .f64 => |*l| .{ .f64 = try l.toOwnedSlice() },
            .dec => |*l| .{ .dec = try l.toOwnedSlice() },
            .bytes => |*l| blk: {
                const offsets = try self.arena.alloc(i32, n + 1);
                offsets[0] = 0;
                @memcpy(offsets[1..], l.ends.items);
                break :blk .{ .bytes = .{ .offsets = offsets, .values = try l.values.toOwnedSlice() } };
            },
        };
        var dict: ?*const Column.Dict = null;
        if (self.dict_vals) |vals| if (self.dict_codes.items.len == n) {
            const d = try self.arena.create(Column.Dict);
            d.* = .{ .values = vals, .codes = self.dict_codes.items };
            dict = d;
        };
        return .{ .ty = self.ty, .len = n, .validity = bm, .data = data, .dict = dict };
    }

    /// Records the dictionary the rows just appended came from (`codes` for present
    /// rows, a null taking 0). Rows appended otherwise leave the count short and `finish` attaches none.
    pub fn noteDict(self: *Builder, src: usize, vals: []const []const u8, codes: []const u32, defs: ?[]const u32, max_def: u32) !void {
        if (self.dict_src) |prev| if (prev != src) {
            self.dict_vals = null;
            return;
        };
        if (self.dict_src == null and self.rows != codes.len + (if (defs) |d| d.len - codes.len else 0)) return;
        self.dict_src = src;
        self.dict_vals = vals;
        if (defs) |d| {
            var j: usize = 0;
            for (d) |lvl| {
                if (lvl == max_def) {
                    try self.dict_codes.append(self.arena, codes[j]);
                    j += 1;
                } else try self.dict_codes.append(self.arena, 0);
            }
        } else try self.dict_codes.appendSlice(self.arena, codes);
    }
};

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

test "gather selects flagged rows preserving validity; all-false keep yields empty" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const c = try intColumn(a, &.{ 1, null, 3, 4 });
    const out = try gather(a, c, &.{ true, true, false, true }, 3);
    try std.testing.expectEqual(@as(usize, 3), out.len);
    try std.testing.expectEqual(@as(i64, 1), out.getValue(0).int);
    try std.testing.expect(out.getValue(1).isNull());
    try std.testing.expectEqual(@as(i64, 4), out.getValue(2).int);

    const none = try gather(a, c, &.{ false, false, false, false }, 0);
    try std.testing.expectEqual(@as(usize, 0), none.len);
}

test "builder finish with zero appended rows yields an empty column" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var b = Builder.init(arena.allocator(), types.Type.init(.int));
    const c = try b.finish();
    try std.testing.expectEqual(@as(usize, 0), c.len);
    try std.testing.expectEqual(@as(usize, 0), c.data.i64.len);
}

test "bitmap allSet: empty prefix, partial byte, exact byte multiple" {
    const alloc = std.testing.allocator;
    var bm = try Bitmap.initFull(alloc, 16);
    defer alloc.free(bm.bits);
    try std.testing.expect(bm.allSet(0));
    try std.testing.expect(bm.allSet(16));
    bm.setValid(11, false);
    try std.testing.expect(bm.allSet(11));
    try std.testing.expect(!bm.allSet(12));
    try std.testing.expect(!bm.allSet(16));
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

test "bitmap allSet: word-wise scan agrees with a bit-by-bit one at every length" {
    const alloc = std.testing.allocator;
    const n = 200;
    var bm = try Bitmap.initFull(alloc, n);
    defer alloc.free(bm.bits);

    for (0..n + 1) |k| try std.testing.expect(bm.allSet(k));

    for ([_]usize{ 0, 1, 7, 8, 63, 64, 65, 127, 128, 191, 192, 199 }) |i| {
        bm.setValid(i, false);
        for (0..n + 1) |k| {
            const expect = k <= i;
            try std.testing.expectEqual(expect, bm.allSet(k));
        }
        bm.setValid(i, true);
    }
}

test "builder validity: nulls, bulk runs and partial bytes interleave correctly" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var b = Builder.init(a, types.Type.init(.int).asNullable());
    try b.append(.{ .int = 1 });
    try b.append(.null);
    try b.append(.{ .int = 3 });
    try b.appendBulk(i64, &.{ 4, 5, 6, 7, 8, 9, 10, 11, 12 });
    try b.append(.null);
    try b.append(.{ .int = 14 });

    const c = try b.finish();
    try std.testing.expectEqual(@as(usize, 14), c.len);
    for (0..14) |i| {
        const is_null = i == 1 or i == 12;
        try std.testing.expectEqual(is_null, c.getValue(i).isNull());
        if (!is_null) try std.testing.expectEqual(@as(i64, @intCast(i + 1)), c.getValue(i).int);
    }
    try std.testing.expect(!c.validity.allSet(14));
    try std.testing.expect(c.validity.allSet(1));

    var b2 = Builder.init(a, types.Type.init(.int));
    try b2.appendBulk(i64, &.{ 1, 2, 3 });
    try b2.append(.{ .int = 4 });
    try b2.appendBulk(i64, &.{ 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 });
    const c2 = try b2.finish();
    try std.testing.expectEqual(@as(usize, 17), c2.len);
    for (0..18) |k| try std.testing.expect(c2.validity.allSet(k));
    try std.testing.expectEqual(@as(usize, 3), c2.validity.bits.len);
}
