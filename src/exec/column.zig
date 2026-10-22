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
const par = @import("par.zig");

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

/// The most value bytes a string column holds: its offsets are `i32`. A copy,
/// merge or build that would pass it fails as out of memory for the column, never
/// with offsets that wrap negative and index outside it.
pub const max_span: usize = std.math.maxInt(i32);

pub fn checkSpan(n: usize) error{OutOfMemory}!usize {
    if (n > max_span) return error.OutOfMemory;
    return n;
}

pub const Bytes = struct {
    offsets: []i32,
    values: []u8,

    pub fn at(self: Bytes, i: usize) []const u8 {
        const lo: usize = @intCast(self.offsets[i]);
        const hi: usize = @intCast(self.offsets[i + 1]);
        return self.values[lo..hi];
    }

    /// The value bytes of the first `n` rows, which may start past 0 and end before
    /// `values` does when the column views a larger buffer.
    pub fn span(self: Bytes, n: usize) usize {
        if (self.offsets.len == 0) return 0;
        return @as(usize, @intCast(self.offsets[n])) - @as(usize, @intCast(self.offsets[0]));
    }

    fn spanOf(self: Bytes, rows_idx: []const usize) usize {
        var total: usize = 0;
        for (rows_idx) |r| total += self.at(r).len;
        return total;
    }

    fn take(self: Bytes, arena: std.mem.Allocator, rows_idx: []const usize) !Bytes {
        const values = try arena.alloc(u8, try checkSpan(self.spanOf(rows_idx)));
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
        try self.offsets.append(@intCast(try checkSpan(self.values.items.len)));
    }

    pub fn push(self: *BytesAppender, s: []const u8) !void {
        try self.append(s);
        try self.endRow();
    }

    pub fn pushNull(self: *BytesAppender) !void {
        try self.endRow();
    }

    pub fn finish(self: *BytesAppender) !Bytes {
        return .{ .offsets = try self.offsets.toOwnedSlice(), .values = try self.values.toOwnedSlice() };
    }
};

/// The rows a `[]bool` mask keeps, worked out once for every column gathered by
/// it, 64 rows at a time: consecutive all-kept words merge into `runs`, each copied
/// as one block, and every other kept row is gathered by `idx`, the source row of
/// each output row outside the runs. An all-dropped word costs nothing per column.
pub const Selection = struct {
    idx: []const u32,
    runs: []const Run,
    kept: usize,

    pub const Run = struct { at: usize, src: usize, len: usize };

    const full = std.math.maxInt(u64);

    pub fn init(arena: std.mem.Allocator, keep: []const bool, kept: usize) !Selection {
        const idx = try arena.alloc(u32, kept + 8);
        var runs = std.ArrayListUnmanaged(Run).empty;
        var w: usize = 0;
        var lo: usize = 0;
        var in_run = false;
        while (lo < keep.len) : (lo += 64) {
            const m = maskWord(keep, lo);
            const was_run = in_run;
            in_run = m == full;
            if (in_run) {
                if (was_run) {
                    runs.items[runs.items.len - 1].len += 64;
                } else try runs.append(arena, .{ .at = w, .src = lo, .len = 64 });
                w += 64;
            } else if (@popCount(m) < sparse_word) {
                var x = m;
                while (x != 0) : (x &= x - 1) {
                    idx[w] = @intCast(lo + @ctz(x));
                    w += 1;
                }
            } else {
                var j: usize = 0;
                while (j < 64) : (j += 8) {
                    const b: u8 = @truncate(m >> @intCast(j));
                    const at: @Vector(8, u32) = byte_rows[b];
                    idx[w..][0..8].* = at + @as(@Vector(8, u32), @splat(@intCast(lo + j)));
                    w += @popCount(b);
                }
            }
        }
        return .{ .idx = idx[0..kept], .runs = runs.items, .kept = kept };
    }

    /// Bit `j` set when row `lo + j` is kept; rows past the mask are not.
    fn maskWord(keep: []const bool, lo: usize) u64 {
        if (lo + 64 <= keep.len) {
            const v: @Vector(64, u8) = @as(*const [64]u8, @ptrCast(keep[lo..].ptr)).*;
            return @bitCast(v != @as(@Vector(64, u8), @splat(0)));
        }
        var m: u64 = 0;
        for (keep[lo..], 0..) |k, j| m |= @as(u64, @intFromBool(k)) << @intCast(j);
        return m;
    }
};

/// A mixed word with fewer kept rows than this has its indices found bit by bit; a
/// denser one eight rows at a time, each byte of its mask writing the positions
/// it sets (`byte_rows`) in one store and advancing by their count.
const sparse_word = 8;

/// For each byte value, the positions of its set bits, lowest first.
const byte_rows: [256][8]u32 = blk: {
    @setEvalBranchQuota(10000);
    var t: [256][8]u32 = undefined;
    for (&t, 0..) |*row, b| {
        row.* = @splat(0);
        var n: usize = 0;
        for (0..8) |bit| if ((b >> bit) & 1 != 0) {
            row[n] = bit;
            n += 1;
        };
    }
    break :blk t;
};

fn selectSlice(comptime T: type, arena: std.mem.Allocator, src: []const T, sel: Selection) ![]T {
    const out = try arena.alloc(T, sel.kept);
    var w: usize = 0;
    for (sel.runs) |r| {
        for (sel.idx[w..r.at], out[w..r.at]) |i, *o| o.* = src[i];
        @memcpy(out[r.at..][0..r.len], src[r.src..][0..r.len]);
        w = r.at + r.len;
    }
    for (sel.idx[w..], out[w..]) |i, *o| o.* = src[i];
    return out;
}

/// Returns the all-valid bitmap untouched when the source has no nulls, as
/// `concat` and `permute` do: this backs the filter hot path.
fn selectValidity(arena: std.mem.Allocator, v: Bitmap, n: usize, sel: Selection) !Bitmap {
    var bm = try Bitmap.initFull(arena, sel.kept);
    if (v.allSet(n)) return bm;
    var w: usize = 0;
    for (sel.runs) |r| {
        for (sel.idx[w..r.at], w..) |i, at| if (!v.get(i)) bm.setValid(at, false);
        for (0..r.len) |j| if (!v.get(r.src + j)) bm.setValid(r.at + j, false);
        w = r.at + r.len;
    }
    for (sel.idx[w..], w..) |i, at| if (!v.get(i)) bm.setValid(at, false);
    return bm;
}

fn selectBytes(arena: std.mem.Allocator, b: Bytes, sel: Selection) !Bytes {
    var span: usize = 0;
    var w: usize = 0;
    for (sel.runs) |r| {
        for (sel.idx[w..r.at]) |i| span += b.at(i).len;
        span += @intCast(b.offsets[r.src + r.len] - b.offsets[r.src]);
        w = r.at + r.len;
    }
    for (sel.idx[w..]) |i| span += b.at(i).len;
    const values = try arena.alloc(u8, try checkSpan(span));
    const offsets = try arena.alloc(i32, sel.kept + 1);
    offsets[0] = 0;
    var off: usize = 0;
    w = 0;
    for (sel.runs) |r| {
        off = takeRows(b, sel.idx[w..r.at], values, offsets[w + 1 ..], off);
        const lo: usize = @intCast(b.offsets[r.src]);
        const hi: usize = @intCast(b.offsets[r.src + r.len]);
        @memcpy(values[off..][0 .. hi - lo], b.values[lo..hi]);
        const shift: i32 = @as(i32, @intCast(off)) - @as(i32, @intCast(lo));
        for (b.offsets[r.src + 1 ..][0..r.len], offsets[r.at + 1 ..][0..r.len]) |x, *dst| dst.* = x + shift;
        off += hi - lo;
        w = r.at + r.len;
    }
    _ = takeRows(b, sel.idx[w..], values, offsets[w + 1 ..], off);
    return .{ .offsets = offsets, .values = values };
}

/// Appends `b`'s rows `idx` at `off` of `values`, each one's end offset into
/// `ends`; the offset past them.
fn takeRows(b: Bytes, idx: []const u32, values: []u8, ends: []i32, off: usize) usize {
    var o = off;
    for (idx, ends[0..idx.len]) |i, *e| {
        const s = b.at(i);
        @memcpy(values[o..][0..s.len], s);
        o += s.len;
        e.* = @intCast(o);
    }
    return o;
}

/// The rows `sel` keeps of `c`, its dictionary view carried along (`carryDict`).
pub fn select(arena: std.mem.Allocator, c: Column, sel: Selection) !Column {
    const bm = try selectValidity(arena, c.validity, c.len, sel);
    const Op = struct {
        arena: std.mem.Allocator,
        c: Column,
        sel: Selection,
        fn fixed(o: @This(), comptime t: Tag) ![]Elem(t) {
            return selectSlice(Elem(t), o.arena, @field(o.c.data, @tagName(t)), o.sel);
        }
        fn bytes(o: @This()) !Bytes {
            return selectBytes(o.arena, o.c.data.bytes, o.sel);
        }
    };
    var out = Column.derived(c.ty, sel.kept, bm, try mapTag(c.data, Op{ .arena = arena, .c = c, .sel = sel }));
    if (c.dict) |d| if (d.values.len <= dict_carry_max) {
        out.dict = try carryDict(arena, d, try selectSlice(u32, arena, d.codes, sel));
    };
    return out;
}

pub fn gather(arena: std.mem.Allocator, c: Column, keep: []const bool, kept: usize) !Column {
    return select(arena, c, try Selection.init(arena, keep, kept));
}

/// The rows `keep` marks of every column of `src`, into `out`, by one selection.
pub fn gatherColumns(arena: std.mem.Allocator, src: []const Column, out: []Column, keep: []const bool, kept: usize) !void {
    const sel = try Selection.init(arena, keep, kept);
    for (src, out) |c, *o| o.* = try select(arena, c, sel);
}

/// Past this many entries a dictionary view is dropped rather than copied along.
const dict_carry_max = 1024;

/// A dictionary view for a column derived from one that had `d`, over `codes`: the
/// values are copied into `arena`, since they may live in the arena the source
/// column came from, so only a small dictionary is carried; null past
/// `dict_carry_max` entries, the consumer then reading the values themselves.
fn carryDict(arena: std.mem.Allocator, d: *const Column.Dict, codes: []const u32) !?*const Column.Dict {
    if (d.values.len > dict_carry_max) return null;
    const vals = try arena.alloc([]const u8, d.values.len);
    for (vals, d.values) |*o, v| o.* = try arena.dupe(u8, v);
    const out = try arena.create(Column.Dict);
    out.* = .{ .values = vals, .codes = codes, .id = d.id };
    return out;
}

/// The validity of `chunks` laid end to end.
fn concatValidity(arena: std.mem.Allocator, chunks: []const Column, total: usize) !Bitmap {
    var bm = try Bitmap.initFull(arena, total);
    var off: usize = 0;
    for (chunks) |c| {
        if (!c.validity.allSet(c.len)) {
            for (0..c.len) |i| if (!c.validity.get(i)) bm.setValid(off + i, false);
        }
        off += c.len;
    }
    return bm;
}

pub fn concat(arena: std.mem.Allocator, chunks: []const Column, total: usize) !Column {
    std.debug.assert(chunks.len > 0);
    const bm = try concatValidity(arena, chunks, total);
    const Op = struct {
        arena: std.mem.Allocator,
        chunks: []const Column,
        total: usize,
        fn fixed(o: @This(), comptime t: Tag) ![]Elem(t) {
            const out = try o.arena.alloc(Elem(t), o.total);
            var off: usize = 0;
            for (o.chunks) |c| {
                @memcpy(out[off..][0..c.len], @field(c.data, @tagName(t))[0..c.len]);
                off += c.len;
            }
            return out;
        }
        fn bytes(o: @This()) !Bytes {
            return concatBytes(o.arena, o.chunks, o.total);
        }
    };
    return Column.derived(chunks[0].ty, total, bm, try mapTag(chunks[0].data, Op{ .arena = arena, .chunks = chunks, .total = total }));
}

/// `concat` copying the chunks on up to `threads` threads, each chunk to its place;
/// the validity, a chunk's bits landing mid-byte, is copied first on this thread.
pub fn concatThreads(arena: std.mem.Allocator, chunks: []const Column, total: usize, threads: usize) !Column {
    if (threads < 2 or chunks.len < 2 or total < 1 << 17) return concat(arena, chunks, total);
    const bm = try concatValidity(arena, chunks, total);
    const rows = try arena.alloc(usize, chunks.len);
    const bytes = try arena.alloc(usize, chunks.len);
    var off: usize = 0;
    var span: usize = 0;
    for (chunks, rows, bytes) |c, *r, *b| {
        r.* = off;
        b.* = span;
        off += c.len;
        if (c.data == .bytes) span += c.data.bytes.span(c.len);
    }
    const out = try allocData(arena, chunks[0].data, total, span);
    const Copy = struct {
        chunks: []const Column,
        rows: []const usize,
        bytes: []const usize,
        out: Column.Data,
        fn f(x: *const @This(), i: usize) void {
            const c = x.chunks[i];
            const at = x.rows[i];
            switch (c.data) {
                .bytes => |b| {
                    const o = x.out.bytes;
                    const lo: usize = @intCast(b.offsets[0]);
                    const hi: usize = @intCast(b.offsets[c.len]);
                    const base = x.bytes[i];
                    @memcpy(o.values[base..][0 .. hi - lo], b.values[lo..hi]);
                    for (b.offsets[1 .. c.len + 1], o.offsets[at + 1 ..][0..c.len]) |src, *dst| dst.* = @intCast(base + @as(usize, @intCast(src)) - lo);
                },
                inline else => |s, tag| @memcpy(@field(x.out, @tagName(tag))[at..][0..c.len], s[0..c.len]),
            }
        }
    };
    par.each(threads, chunks.len, &Copy{ .chunks = chunks, .rows = rows, .bytes = bytes, .out = out }, Copy.f);
    return Column.derived(chunks[0].ty, total, bm, out);
}

fn concatBytes(arena: std.mem.Allocator, chunks: []const Column, total: usize) !Bytes {
    var span: usize = 0;
    for (chunks) |c| span += c.data.bytes.span(c.len);
    const values = try arena.alloc(u8, try checkSpan(span));
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

pub fn permute(arena: std.mem.Allocator, c: Column, idx: []const usize) !Column {
    const bm = try permuteValidity(arena, c.validity, c.len, idx);
    const Op = struct {
        arena: std.mem.Allocator,
        c: Column,
        idx: []const usize,
        fn fixed(o: @This(), comptime t: Tag) ![]Elem(t) {
            const src = @field(o.c.data, @tagName(t));
            const out = try o.arena.alloc(Elem(t), o.idx.len);
            for (o.idx, 0..) |r, i| out[i] = src[r];
            return out;
        }
        fn bytes(o: @This()) !Bytes {
            return o.c.data.bytes.take(o.arena, o.idx);
        }
    };
    var out = Column.derived(c.ty, idx.len, bm, try mapTag(c.data, Op{ .arena = arena, .c = c, .idx = idx }));
    if (c.dict) |d| if (d.values.len <= dict_carry_max) {
        const codes = try arena.alloc(u32, idx.len);
        for (codes, idx) |*o, r| o.* = d.codes[r];
        out.dict = try carryDict(arena, d, codes);
    };
    return out;
}

/// Rows `start..start + n` of `c` as a view on its buffers, nothing copied: `start` is
/// a multiple of 8 so the validity stays byte-aligned, byte offsets keep indexing the
/// same values buffer, and a dictionary keeps its entries over a run of its codes.
pub fn slice(arena: std.mem.Allocator, c: Column, start: usize, n: usize) !Column {
    std.debug.assert(start % 8 == 0 and start + n <= c.len);
    var out = c;
    out.len = n;
    out.validity = .{ .bits = c.validity.bits[start / 8 ..][0 .. (n + 7) / 8], .len = n };
    out.data = switch (c.data) {
        .bytes => |b| .{ .bytes = .{ .offsets = b.offsets[start..][0 .. n + 1], .values = b.values } },
        inline else => |sl, tag| @unionInit(Column.Data, @tagName(tag), sl[start..][0..n]),
    };
    if (c.dict) |d| {
        const v = try arena.create(Column.Dict);
        v.* = .{ .values = d.values, .codes = d.codes[start..][0..n], .id = d.id };
        out.dict = v;
    }
    return out;
}

/// The validity of rows `idx` of a column of `n` rows. A few rows of a long column (a
/// probe batch's matches in a join's build side) are checked one by one: scanning the
/// whole bitmap for a null first cost more than the rows themselves.
fn permuteValidity(arena: std.mem.Allocator, v: Bitmap, n: usize, idx: []const usize) !Bitmap {
    var bm = try Bitmap.initFull(arena, idx.len);
    if (idx.len * 64 >= n and v.allSet(n)) return bm;
    for (idx, 0..) |r, i| {
        if (!v.get(r)) bm.setValid(i, false);
    }
    return bm;
}

/// `c`'s rows copied into `arena`, one memcpy per buffer: the gather of every row in
/// order, without the gather. Only the rows' own span is copied, as `c` may view a
/// larger buffer; a small dictionary view comes along (`carryDict`).
pub fn copy(arena: std.mem.Allocator, c: Column) !Column {
    var out = c;
    out.dict = if (c.dict) |d| try carryDict(arena, d, try arena.dupe(u32, d.codes[0..c.len])) else null;
    out.validity = try Bitmap.initFull(arena, c.len);
    @memcpy(out.validity.bits, c.validity.bits[0..out.validity.bits.len]);
    out.data = switch (c.data) {
        .bytes => |b| blk: {
            if (b.offsets.len == 0) break :blk .{ .bytes = .{ .offsets = try arena.alloc(i32, 0), .values = &.{} } };
            const lo: usize = @intCast(b.offsets[0]);
            const hi: usize = @intCast(b.offsets[c.len]);
            const offs = try arena.alloc(i32, c.len + 1);
            for (offs, b.offsets[0 .. c.len + 1]) |*o, x| o.* = x - b.offsets[0];
            break :blk .{ .bytes = .{ .offsets = offs, .values = try arena.dupe(u8, b.values[lo..hi]) } };
        },
        inline else => |sl, tag| @unionInit(Column.Data, @tagName(tag), try arena.dupe(@typeInfo(@TypeOf(sl)).pointer.child, sl[0..c.len])),
    };
    return out;
}

/// `permute` on up to `threads` threads for a large `idx`, in chunks of rows: each
/// gathers its own, a string column's chunks first summing their bytes so each knows
/// where its values start.
pub fn permuteThreads(arena: std.mem.Allocator, c: Column, idx: []const usize, threads: usize) !Column {
    const n = idx.len;
    const nc = par.chunks(n, 1 << 16);
    if (threads < 2 or nc < 2) return permute(arena, c, idx);
    var bm = try Bitmap.initFull(arena, n);
    const starts = try arena.alloc(usize, nc);
    var span: usize = 0;
    if (c.data == .bytes) {
        const Span = struct {
            b: Bytes,
            idx: []const usize,
            starts: []usize,
            fn f(x: *const @This(), i: usize) void {
                const r = par.bounds(x.idx.len, x.starts.len, i);
                x.starts[i] = x.b.spanOf(x.idx[r[0]..r[1]]);
            }
        };
        par.each(threads, nc, &Span{ .b = c.data.bytes, .idx = idx, .starts = starts }, Span.f);
        for (starts) |*st| {
            const len = st.*;
            st.* = span;
            span += len;
        }
    }
    const out = try allocData(arena, c.data, n, span);
    const Gather = struct {
        c: Column,
        idx: []const usize,
        out: Column.Data,
        bm: *Bitmap,
        starts: []const usize,
        nulls: bool,
        fn f(x: *const @This(), i: usize) void {
            const r = par.bounds(x.idx.len, x.starts.len, i);
            const rows = x.idx[r[0]..r[1]];
            if (x.nulls) {
                for (rows, r[0]..) |row, at| if (!x.c.validity.get(row)) x.bm.setValid(at, false);
            }
            switch (x.c.data) {
                .bytes => |b| {
                    const o = x.out.bytes;
                    var off = x.starts[i];
                    for (rows, r[0]..) |row, w| {
                        const v = b.at(row);
                        @memcpy(o.values[off..][0..v.len], v);
                        off += v.len;
                        o.offsets[w + 1] = @intCast(off);
                    }
                },
                inline else => |s, tag| {
                    const o = @field(x.out, @tagName(tag));
                    for (rows, r[0]..) |row, at| o[at] = s[row];
                },
            }
        }
    };
    const g = Gather{ .c = c, .idx = idx, .out = out, .bm = &bm, .starts = starts, .nulls = !c.validity.allSet(c.len) };
    par.each(threads, nc, &g, Gather.f);
    return Column.derived(c.ty, n, bm, out);
}

pub const Tag = std.meta.Tag(Column.Data);

/// The storage a column of `kind` holds: the one kind-to-storage mapping, which
/// `Builder`, `Column.getValue` and the aggregate kernels derive theirs from.
pub fn storageOf(kind: types.TypeKind) Tag {
    return switch (kind) {
        .bool => .b,
        .date => .i32,
        .int, .time, .timestamp => .i64,
        .float => .f64,
        .decimal => .dec,
        .string, .bytes, .array, .@"struct" => .bytes,
    };
}

/// The element type of a fixed-width storage.
pub fn Elem(comptime t: Tag) type {
    return std.meta.Elem(@FieldType(Column.Data, @tagName(t)));
}

/// A `Data` of storage `tag`, from `op.fixed(t)` for a fixed-width one and
/// `op.bytes()` for bytes: the per-storage dispatch every column copy, merge and
/// spill read goes through, so none can miss a storage.
pub fn mapTag(tag: Tag, op: anytype) !Column.Data {
    return switch (tag) {
        .bytes => .{ .bytes = try op.bytes() },
        inline else => |t| @unionInit(Column.Data, @tagName(t), try op.fixed(t)),
    };
}

/// Unfilled buffers for `n` rows of storage `tag`; a bytes one gets `span` value
/// bytes and `n + 1` offsets, the first set to 0.
pub fn allocData(arena: std.mem.Allocator, tag: Tag, n: usize, span: usize) !Column.Data {
    const Op = struct {
        arena: std.mem.Allocator,
        n: usize,
        span: usize,
        fn fixed(o: @This(), comptime t: Tag) ![]Elem(t) {
            return o.arena.alloc(Elem(t), o.n);
        }
        fn bytes(o: @This()) !Bytes {
            const offsets = try o.arena.alloc(i32, o.n + 1);
            offsets[0] = 0;
            return .{ .offsets = offsets, .values = try o.arena.alloc(u8, try checkSpan(o.span)) };
        }
    };
    return mapTag(tag, Op{ .arena = arena, .n = n, .span = span });
}

pub const Column = struct {
    ty: types.Type,
    len: usize,
    validity: Bitmap,
    data: Data,
    dict: ?*const Dict = null,

    /// A column's values as codes into `values`; `id`, when not 0, names the
    /// dictionary for the process, so a consumer may remember what it made of it.
    pub const Dict = struct { values: []const []const u8, codes: []const u32, id: u64 = 0 };

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
            .array, .@"struct" => .null,
            inline else => |k| blk: {
                const t = comptime storageOf(k);
                break :blk @unionInit(Value, @tagName(k), if (t == .bytes) self.data.bytes.at(i) else @field(self.data, @tagName(t))[i]);
            },
        };
    }

    /// A column built from other columns' rows: it owns its buffers, so it carries
    /// no dictionary, whatever its sources had.
    pub fn derived(ty: types.Type, len: usize, validity: Bitmap, data: Data) Column {
        return .{ .ty = ty, .len = len, .validity = validity, .data = data, .dict = null };
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

    pub const Store = union(Tag) {
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
        const store: Store = switch (storageOf(ty.kind)) {
            .bytes => bytes_store,
            inline else => |t| @unionInit(Store, @tagName(t), std.array_list.Managed(Elem(t)).init(arena)),
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
            inline else => |*l| {
                if (@TypeOf(l.items) != []T) return error.BulkTypeMismatch;
                try l.appendSlice(vals);
            },
            .bytes => return error.BulkTypeMismatch,
        }
        try self.pushValidRun(vals.len);
    }

    /// Room for `n` more values of a fixed-width store, written in place and then
    /// counted, all valid, by `commitSlots`; skips the copy a temporary would cost.
    pub fn reserveSlots(self: *Builder, comptime T: type, n: usize) ![]T {
        switch (self.store) {
            inline else => |*l| {
                if (@TypeOf(l.items) != []T) return error.BulkTypeMismatch;
                try l.ensureUnusedCapacity(n);
                return l.unusedCapacitySlice()[0..n];
            },
            .bytes => return error.BulkTypeMismatch,
        }
    }

    pub fn commitSlots(self: *Builder, comptime T: type, n: usize) !void {
        switch (self.store) {
            inline else => |*l| {
                if (@TypeOf(l.items) != []T) return error.BulkTypeMismatch;
                l.items.len += n;
            },
            .bytes => return error.BulkTypeMismatch,
        }
        try self.pushValidRun(n);
    }

    pub fn appendBytesScattered(self: *Builder, vals: []const []const u8, defs: ?[]const u32, max_def: u32) !void {
        if (self.store != .bytes) return error.BulkTypeMismatch;
        const l = &self.store.bytes;
        const all_present = if (defs) |d| d.len == vals.len else true;
        if (all_present) {
            var total: usize = 0;
            for (vals) |s| total += s.len;
            _ = try checkSpan(l.values.items.len + total);
            try l.values.ensureUnusedCapacity(total);
            try l.ends.ensureUnusedCapacity(vals.len);
            for (vals) |s| {
                l.values.appendSliceAssumeCapacity(s);
                l.ends.appendAssumeCapacity(@intCast(l.values.items.len));
            }
            try self.pushValidRun(vals.len);
            return;
        }
        const d = defs.?;
        try l.ends.ensureUnusedCapacity(d.len);
        var j: usize = 0;
        for (d) |lvl| {
            const present = lvl == max_def;
            if (present) {
                if (j >= vals.len) return error.BulkTypeMismatch;
                try l.values.appendSlice(vals[j]);
                j += 1;
            }
            l.ends.appendAssumeCapacity(@intCast(try checkSpan(l.values.items.len)));
            try self.pushValid(present);
        }
    }

    /// Rows given as codes into a dictionary page's values, laid end to end in
    /// `flat` at `offs` with at least 16 bytes to spare past the last: `codes` are the
    /// present rows' in order, `defs` (when given) places them among the nulls. An
    /// entry of 16 bytes or fewer moves in one fixed copy, never a call, and neither
    /// side is read or written past what it holds.
    pub fn appendDictBytes(self: *Builder, flat: []const u8, offs: []const i32, codes: []const u32, defs: ?[]const u32, max_def: u32) !void {
        if (self.store != .bytes) return error.BulkTypeMismatch;
        const l = &self.store.bytes;
        var total: usize = 0;
        for (codes) |c| total += @intCast(offs[c + 1] - offs[c]);
        _ = try checkSpan(l.values.items.len + total);
        try l.values.ensureUnusedCapacity(total + 16);
        const rows = if (defs) |d| d.len else codes.len;
        try l.ends.ensureUnusedCapacity(rows);
        const out = l.values.items.ptr;
        var w = l.values.items.len;
        if (defs == null or defs.?.len == codes.len) {
            for (codes) |c| {
                w = moveEntry(out, w, flat, offs[c], offs[c + 1]);
                l.ends.appendAssumeCapacity(@intCast(w));
            }
            l.values.items.len = w;
            return self.pushValidRun(codes.len);
        }
        var j: usize = 0;
        for (defs.?) |lvl| {
            const present = lvl == max_def;
            if (present) {
                if (j >= codes.len) return error.BulkTypeMismatch;
                w = moveEntry(out, w, flat, offs[codes[j]], offs[codes[j] + 1]);
                j += 1;
            }
            l.ends.appendAssumeCapacity(@intCast(w));
            l.values.items.len = w;
            try self.pushValid(present);
        }
    }

    inline fn moveEntry(out: [*]u8, w: usize, flat: []const u8, lo_i: i32, hi_i: i32) usize {
        const lo: usize = @intCast(lo_i);
        const n: usize = @intCast(hi_i - lo_i);
        if (n <= 16) {
            out[w..][0..16].* = flat[lo..][0..16].*;
        } else @memcpy(out[w..][0..n], flat[lo..][0..n]);
        return w + n;
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
        if (defs.len == vals.len) return self.appendBulk(T, vals);
        switch (self.store) {
            inline else => |*l| {
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
        try l.ends.append(@intCast(try checkSpan(l.values.items.len)));
    }

    /// Room for `n` more bytes of a bytes column's next value, which a producer
    /// fills in place and then records with `commitBytes`.
    pub fn reserveBytes(self: *Builder, n: usize) ![]u8 {
        const l = &self.store.bytes.values;
        try l.ensureUnusedCapacity(n);
        return l.unusedCapacitySlice()[0..n];
    }

    pub fn commitBytes(self: *Builder, n: usize) !void {
        try self.pushValid(true);
        const l = &self.store.bytes;
        l.values.items.len += n;
        try l.ends.append(@intCast(try checkSpan(l.values.items.len)));
    }

    pub fn append(self: *Builder, v: Value) !void {
        const ok = !v.isNull();
        try self.pushValid(ok);
        switch (self.store) {
            .b => |*l| try l.append(if (ok) v.bool else false),
            .i32 => |*l| try l.append(if (ok) v.date else 0),
            .i64 => |*l| try l.append(if (ok) self.asI64(v) else 0),
            .f64 => |*l| try l.append(if (ok) self.asF64(v) else 0),
            .dec => |*l| try l.append(self.asDec(v)),
            .bytes => |*l| {
                if (ok) try l.values.appendSlice(self.asBytes(v));
                try l.ends.append(@intCast(try checkSpan(l.values.items.len)));
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

    /// An INT read as a scale-0 decimal, as an INT arm beside a DECIMAL one types.
    fn asDec(self: *Builder, v: Value) value.Decimal {
        return switch (v) {
            .decimal => |d| d,
            .int => |x| .{ .unscaled = x, .scale = 0 },
            else => .{ .unscaled = 0, .scale = self.ty.scale },
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
            .bytes => |*l| blk: {
                const offsets = try self.arena.alloc(i32, n + 1);
                offsets[0] = 0;
                @memcpy(offsets[1..], l.ends.items);
                break :blk .{ .bytes = .{ .offsets = offsets, .values = try l.values.toOwnedSlice() } };
            },
            inline else => |*l, t| @unionInit(Column.Data, @tagName(t), try l.toOwnedSlice()),
        };
        var dict: ?*const Column.Dict = null;
        if (self.dict_vals) |vals| if (vals.len > 0 and self.dict_codes.items.len == n) {
            const d = try self.arena.create(Column.Dict);
            d.* = .{ .values = vals, .codes = self.dict_codes.items, .id = self.dict_src.? };
            dict = d;
        };
        return .{ .ty = self.ty, .len = n, .validity = bm, .data = data, .dict = dict };
    }

    /// Records the dictionary the rows just appended came from (`src`, its process-wide
    /// id; `codes` for present rows, a null taking 0). Rows appended otherwise leave the
    /// count short and `finish` attaches none.
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

test "concat joins typed chunks and carries nulls across offsets" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const v1 = [_]?i64{ 1, null, 3, 4, null };
    const v2 = [_]?i64{ 6, 7, null, null, 10, null };
    const c1 = try intColumn(a, &v1);
    const c2 = try intColumn(a, &v2);
    const out = try concat(a, &.{ c1, c2 }, 11);
    try std.testing.expectEqual(@as(usize, 11), out.len);
    const want = v1 ++ v2;
    for (want, 0..) |w, i| {
        if (w) |x| {
            try std.testing.expectEqual(x, out.getValue(i).int);
        } else {
            try std.testing.expect(out.getValue(i).isNull());
        }
    }
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

test "gather by 64-row words agrees with a row-by-row pick at every block boundary and density" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var prng = std.Random.DefaultPrng.init(42);
    const rnd = prng.random();
    const vals = [_][]const u8{ "", "MAIL", "TRUCK AND SHIP", "x" };
    for ([_]usize{ 1, 63, 64, 65, 129, 200, 300 }) |n| {
        var ib = Builder.init(a, types.Type.init(.int).asNullable());
        var sb = Builder.init(a, types.Type.init(.string).asNullable());
        var db = Builder.init(a, types.Type.init(.string));
        const codes = try a.alloc(u32, n);
        for (0..n) |i| {
            const nul = rnd.uintLessThan(u8, 7) == 0;
            try ib.append(if (nul) .null else .{ .int = @intCast(i * 3) });
            try sb.append(if (nul) .null else .{ .string = vals[i % vals.len] });
            codes[i] = rnd.uintLessThan(u32, vals.len);
            try db.append(.{ .string = vals[codes[i]] });
        }
        var dc = try db.finish();
        const d = Column.Dict{ .values = &vals, .codes = codes, .id = 3 };
        dc.dict = &d;
        const srcs = [_]Column{ try ib.finish(), try sb.finish(), dc };
        const keep = try a.alloc(bool, n);
        for (0..6) |pattern| {
            var kept: usize = 0;
            for (keep, 0..) |*k, i| {
                k.* = switch (pattern) {
                    0 => true,
                    1 => false,
                    2 => rnd.uintLessThan(u8, 50) == 0,
                    3 => rnd.uintLessThan(u8, 50) != 0,
                    4 => i < 3 or i >= 70,
                    else => rnd.boolean(),
                };
                kept += @intFromBool(k.*);
            }
            const outs = try a.alloc(Column, srcs.len);
            try gatherColumns(a, &srcs, outs, keep, kept);
            for (srcs, outs) |src, got| {
                try std.testing.expectEqual(kept, got.len);
                var w: usize = 0;
                for (keep, 0..) |k, i| if (k) {
                    const want = src.getValue(i);
                    const have = got.getValue(w);
                    try std.testing.expectEqual(want.isNull(), have.isNull());
                    if (!want.isNull()) switch (want) {
                        .int => |x| try std.testing.expectEqual(x, have.int),
                        .string => |s| try std.testing.expectEqualStrings(s, have.string),
                        else => unreachable,
                    };
                    if (got.dict) |gd| try std.testing.expectEqual(codes[i], gd.codes[w]);
                    w += 1;
                };
            }
            try std.testing.expect(outs[2].dict != null);
        }
    }
}

test "builder finish with zero appended rows yields an empty column" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var b = Builder.init(arena.allocator(), types.Type.init(.int));
    const c = try b.finish();
    try std.testing.expectEqual(@as(usize, 0), c.len);
    try std.testing.expectEqual(@as(usize, 0), c.data.i64.len);
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

test "a small dictionary view survives gather, permute and copy, each row's code naming its own value" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const vals = [_][]const u8{ "MAIL", "SHIP", "AIR" };
    const codes = [_]u32{ 2, 0, 1, 1, 0, 2 };
    var b = Builder.init(a, types.Type.init(.string));
    for (codes) |cd| try b.append(.{ .string = vals[cd] });
    var c = try b.finish();
    const d = Column.Dict{ .values = &vals, .codes = &codes, .id = 7 };
    c.dict = &d;
    const keep = [_]bool{ true, false, true, true, false, true };
    const idx = [_]usize{ 5, 0, 3 };
    for ([_]Column{ try gather(a, c, &keep, 4), try permute(a, c, &idx), try copy(a, c) }) |got| {
        const gd = got.dict orelse return error.TestExpectedDict;
        try std.testing.expectEqual(@as(u64, 7), gd.id);
        for (0..got.len) |r| try std.testing.expectEqualStrings(got.data.bytes.at(r), gd.values[gd.codes[r]]);
    }
    var big_vals: [dict_carry_max + 1][]const u8 = undefined;
    for (&big_vals) |*v| v.* = "x";
    const big = Column.Dict{ .values = &big_vals, .codes = &codes, .id = 8 };
    c.dict = &big;
    try std.testing.expect((try copy(a, c)).dict == null);
}

test "a slice views its rows: nulls, strings past a nonzero first offset, and dictionary codes, through gather, permute, copy and concat" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const vals = [_][]const u8{ "MAIL", "SHIP", "AIR" };
    var codes: [40]u32 = undefined;
    var b = Builder.init(a, types.Type.init(.string).asNullable());
    for (&codes, 0..) |*cd, i| {
        cd.* = @intCast(i % 3);
        if (i % 7 == 3) {
            cd.* = 0;
            try b.append(.null);
        } else try b.append(.{ .string = vals[cd.*] });
    }
    var c = try b.finish();
    const d = Column.Dict{ .values = &vals, .codes = &codes, .id = 9 };
    c.dict = &d;
    const s = try slice(a, c, 16, 20);
    try std.testing.expect(s.data.bytes.offsets[0] != 0);
    for (0..s.len) |r| {
        try std.testing.expectEqual(c.validity.get(16 + r), s.validity.get(r));
        try std.testing.expectEqualStrings(c.data.bytes.at(16 + r), s.data.bytes.at(r));
        try std.testing.expectEqual(codes[16 + r], s.dict.?.codes[r]);
    }
    var keep: [20]bool = undefined;
    for (&keep, 0..) |*k, i| k.* = i % 2 == 0;
    var idx: [20]usize = undefined;
    for (&idx, 0..) |*x, i| x.* = 19 - i;
    const head = try slice(a, c, 0, 16);
    const parts = [_]Column{ head, s };
    const cat = try concat(a, &parts, 36);
    for ([_]Column{ try gather(a, s, &keep, 10), try permute(a, s, &idx), try copy(a, s), cat }, [_][]const usize{ &.{ 0, 2, 4, 6, 8, 10, 12, 14, 16, 18 }, &.{ 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 }, &.{}, &.{} }) |got, rows| {
        for (0..got.len) |r| {
            const src: usize = if (got.len == 36) r else 16 + (if (rows.len > 0) rows[r] else r);
            try std.testing.expectEqual(c.validity.get(src), got.validity.get(r));
            try std.testing.expectEqualStrings(c.data.bytes.at(src), got.data.bytes.at(r));
        }
    }
}

test "a slice of fixed-width rows views the same values" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var b = Builder.init(a, types.Type.init(.int).asNullable());
    for (0..30) |i| try b.append(if (i % 5 == 2) .null else .{ .int = @intCast(i) });
    const c = try b.finish();
    const s = try slice(a, c, 8, 22);
    try std.testing.expectEqual(@as(usize, 22), s.data.i64.len);
    for (0..s.len) |r| {
        try std.testing.expectEqual(c.validity.get(8 + r), s.validity.get(r));
        if (s.validity.get(r)) try std.testing.expectEqual(@as(i64, @intCast(8 + r)), s.data.i64[r]);
    }
    try std.testing.expect(!s.validity.allSet(s.len));
    try std.testing.expect((try slice(a, c, 24, 2)).validity.allSet(2));
}

test "permute of a few rows of a long column checks only those rows' validity" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var b = Builder.init(a, types.Type.init(.int).asNullable());
    for (0..1000) |i| try b.append(if (i % 100 == 37) .null else .{ .int = @intCast(i) });
    const c = try b.finish();
    const out = try permute(a, c, &.{ 5, 437, 999, 36 });
    try std.testing.expect(out.validity.get(0) and !out.validity.get(1) and out.validity.get(2) and out.validity.get(3));
    try std.testing.expectEqual(@as(i64, 999), out.data.i64[2]);
    try std.testing.expect((try permute(a, c, &.{ 1, 38, 500 })).validity.allSet(3));
}

test "a string column's value bytes stop at the i32 offsets' reach" {
    try std.testing.expectEqual(max_span, try checkSpan(max_span));
    try std.testing.expectError(error.OutOfMemory, checkSpan(max_span + 1));
}
