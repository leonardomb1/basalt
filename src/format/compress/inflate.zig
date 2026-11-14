//! A DEFLATE decoder (RFC 1951) in a raw, gzip (RFC 1952) or zlib (RFC 1950)
//! container, read as a `std.Io.Reader`, for `.gz` files, gzip Parquet pages and zip
//! members. `std.compress.flate.Decompress` pulled its input a few bits at a time
//! through the input reader and checked the CRC a byte at a time: 270 MB/s, against
//! 1.5 GB/s for libdeflate on a gzipped TPC-H `lineitem.csv`.
//!
//! This decodes as libdeflate does: a 64-bit bit buffer refilled with one unaligned
//! 8-byte load, the bytes it takes past the bits it needs being the input's own
//! next bytes, so a later refill ORs in the same values; a literal/length table of
//! 11 bits and a distance table of 8, a longer code reaching a subtable; a match of
//! distance 8 or more copied 8 bytes at a time, which the output's slack covers.
//! The output is the reader's own buffer, its last 32 KiB kept across a rebase as
//! the window a match reaches into, and the checksum runs over each decoded run at
//! once (`base/crc32.zig`).
//!
//! The bits past `bitsleft` are the input's next bytes only while the input advances
//! by refills; a stored block copied straight from the input clears them first.
//!
//! Whatever the bit buffer holds past the stream's end goes back to the input: the
//! input is advanced only past the bytes the bits have consumed, so the bytes of a
//! next gzip member are still there for the next decoder.
//!
//! Malformed input is an error, never a wrong read (std's decoder reads a gzip or
//! zlib trailer without comparing it, so a damaged `.gz` read back wrong bytes): a
//! header or checksum that does
//! not match, an oversubscribed code, an incomplete one (but a single code of one
//! bit, as zlib allows), a length or distance symbol past the table, a distance past
//! the output so far, and an input that ends inside the stream.

const std = @import("std");
const Crc32 = @import("../../base/crc32.zig").Crc32;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;

pub const Container = enum { raw, gzip, zlib };

pub const history_len = 32 << 10;

/// The output buffer a decoder is given: the window plus room to decode ahead, the
/// more of it the fewer rebases.
pub const buffer_len = history_len + (256 << 10);

const max_match = 258;
const out_slack = max_match + 16;

pub const Error = error{
    BadGzipHeader,
    BadZlibHeader,
    WrongChecksum,
    WrongSize,
    InvalidBlockType,
    WrongStoredBlockNlen,
    InvalidCode,
    InvalidMatch,
    BadTree,
    EndOfStream,
    ReadFailed,
};

const litlen_bits = 11;
const dist_bits = 8;
const pre_bits = 7;
const litlen_len = 2342;
const dist_len = 402;

/// A table entry: `n` the bits a code takes (the main table's bits for a subtable
/// link), `extra` the extra bits a length or distance reads (a subtable's bits for a
/// link), `kind`, and `val` the literal, base or subtable offset.
const Entry = packed struct(u32) {
    n: u4 = 0,
    extra: u4 = 0,
    kind: Kind = .invalid,
    _pad: u5 = 0,
    val: u16 = 0,

    const Kind = enum(u3) { literal, length, end, link, invalid, sym };
};

const length_base = [29]u16{ 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131, 163, 195, 227, 258 };
const length_extra = [29]u4{ 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0 };
const dist_base = [30]u16{ 1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537, 2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577 };
const dist_extra = [30]u4{ 0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13 };
const pre_order = [19]u8{ 16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15 };

fn litlenEntry(sym: usize) Entry {
    if (sym < 256) return .{ .kind = .literal, .val = @intCast(sym) };
    if (sym == 256) return .{ .kind = .end };
    if (sym - 257 < length_base.len) return .{ .kind = .length, .extra = length_extra[sym - 257], .val = length_base[sym - 257] };
    return .{ .kind = .invalid };
}

fn distEntry(sym: usize) Entry {
    if (sym < dist_base.len) return .{ .kind = .length, .extra = dist_extra[sym], .val = dist_base[sym] };
    return .{ .kind = .invalid };
}

fn preEntry(sym: usize) Entry {
    return .{ .kind = .sym, .val = @intCast(sym) };
}

/// The canonical code of `lens` laid into `table` with a main table of `bits`;
/// false when the code is oversubscribed, or incomplete past a single one-bit code.
fn build(table: []Entry, lens: []const u8, bits: u4, comptime entryOf: fn (usize) Entry) bool {
    var count = [_]u16{0} ** 16;
    for (lens) |l| count[l] += 1;
    count[0] = 0;
    var left: i32 = 1;
    var max: u8 = 0;
    for (1..16) |l| {
        left = (left << 1) - count[l];
        if (left < 0) return false;
        if (count[l] != 0) max = @intCast(l);
    }
    const main_len = @as(usize, 1) << bits;
    @memset(table[0..main_len], .{});
    if (max == 0) return true;
    if (left > 0 and max != 1) return false;

    var next = [_]u16{0} ** 16;
    var code: u16 = 0;
    for (1..16) |l| {
        code = (code + count[l - 1]) << 1;
        next[l] = code;
    }
    var codes: [320]u16 = undefined;
    for (lens, 0..) |l, s| if (l != 0) {
        codes[s] = next[l];
        next[l] += 1;
    };

    var sub_bits = [_]u4{0} ** (1 << litlen_bits);
    for (lens, 0..) |l, s| if (l > bits) {
        const r = reverse(codes[s], l);
        const p = r & (main_len - 1);
        sub_bits[p] = @max(sub_bits[p], @as(u4, @intCast(l - bits)));
    };
    var at = main_len;
    for (0..main_len) |p| if (sub_bits[p] != 0) {
        const size = @as(usize, 1) << sub_bits[p];
        if (at + size > table.len) return false;
        table[p] = .{ .kind = .link, .n = bits, .extra = sub_bits[p], .val = @intCast(at) };
        @memset(table[at .. at + size], .{});
        at += size;
    };

    for (lens, 0..) |l, s| if (l != 0) {
        var e = entryOf(s);
        e.n = @intCast(l);
        const r = reverse(codes[s], l);
        if (l <= bits) {
            var i: usize = r;
            while (i < main_len) : (i += @as(usize, 1) << @intCast(l)) table[i] = e;
        } else {
            const link = table[r & (main_len - 1)];
            const sb: u5 = link.extra;
            const step = @as(usize, 1) << @intCast(l - bits);
            var i: usize = r >> bits;
            while (i < (@as(usize, 1) << sb)) : (i += step) table[link.val + i] = e;
        }
    };
    return true;
}

fn reverse(code: u16, len: u8) usize {
    return @as(usize, @bitReverse(code)) >> @intCast(16 - len);
}

pub const Inflate = struct {
    input: *Reader,
    reader: Reader,
    container: Container,
    state: State = .header,
    bitbuf: u64 = 0,
    bitsleft: u7 = 0,
    ip: usize = 0,
    final: bool = false,
    stored_left: u32 = 0,
    crc: Crc32 = .{},
    adler: std.hash.Adler32 = .{},
    size: u32 = 0,
    err: ?Error = null,
    litlen: [litlen_len]Entry = undefined,
    dist: [dist_len]Entry = undefined,

    const State = enum { header, block, stored, huffman, trailer, end };

    const vtable: Reader.VTable = .{
        .stream = streamFn,
        .rebase = rebaseFn,
        .discard = discardFn,
        .readVec = readVecFn,
    };

    /// `buffer` is the output, at least `history_len` plus 64 KiB; `buffer_len`
    /// rebases less often.
    pub fn init(input: *Reader, container: Container, buffer: []u8) Inflate {
        std.debug.assert(buffer.len >= history_len + (64 << 10));
        return .{
            .input = input,
            .container = container,
            .reader = .{ .vtable = &vtable, .buffer = buffer, .seek = 0, .end = 0 },
        };
    }

    fn streamFn(r: *Reader, w: *Writer, limit: std.Io.Limit) Reader.StreamError!usize {
        _ = w;
        _ = limit;
        try fillFn(r);
        return 0;
    }

    fn readVecFn(r: *Reader, data: [][]u8) Reader.Error!usize {
        _ = data;
        try fillFn(r);
        return 0;
    }

    fn discardFn(r: *Reader, limit: std.Io.Limit) Reader.Error!usize {
        if (r.end == r.seek) try fillFn(r);
        const n = limit.minInt(r.end - r.seek);
        r.seek += n;
        return n;
    }

    fn rebaseFn(r: *Reader, capacity: usize) Reader.RebaseError!void {
        rebase(r, capacity);
    }

    fn rebase(r: *Reader, capacity: usize) void {
        _ = capacity;
        if (r.end <= history_len) return;
        const discard_n = @min(r.seek, r.end - history_len);
        if (discard_n == 0) return;
        const keep = r.buffer[discard_n..r.end];
        @memmove(r.buffer[0..keep.len], keep);
        r.end = keep.len;
        r.seek -= discard_n;
    }

    /// Decodes into the buffer past `end`; EndOfStream once the stream is done.
    fn fillFn(r: *Reader) Reader.Error!void {
        const self: *Inflate = @alignCast(@fieldParentPtr("reader", r));
        if (self.state == .end) return error.EndOfStream;
        if (r.buffer.len - r.end < out_slack + (16 << 10)) rebase(r, 0);
        const start = r.end;
        self.run() catch |e| {
            self.err = e;
            return error.ReadFailed;
        };
        if (r.end == start and self.state == .end) return error.EndOfStream;
    }

    fn run(self: *Inflate) Error!void {
        const r = &self.reader;
        var summed = r.end;
        defer self.sum(r.buffer[summed..r.end]);
        while (true) switch (self.state) {
            .header => {
                try self.header();
                self.state = .block;
            },
            .block => {
                if (self.final) {
                    self.state = .trailer;
                    continue;
                }
                try self.blockHeader();
            },
            .stored => {
                if (self.stored_left == 0) {
                    self.state = .block;
                    continue;
                }
                const room = r.buffer.len - r.end;
                if (room == 0) return;
                try self.storedCopy(room);
            },
            .huffman => {
                if (r.buffer.len - r.end < out_slack) return;
                try self.huffman();
                if (self.state == .huffman) return;
            },
            .trailer => {
                self.sum(r.buffer[summed..r.end]);
                summed = r.end;
                try self.trailer();
                self.state = .end;
                self.sync();
                return;
            },
            .end => return,
        };
    }

    /// The checksum taken over bytes just decoded, once each.
    fn sum(self: *Inflate, bytes: []const u8) void {
        switch (self.container) {
            .gzip => {
                self.crc.update(bytes);
                self.size +%= @truncate(bytes.len);
            },
            .zlib => self.adler.update(bytes),
            .raw => {},
        }
    }

    // Input.

    fn avail(self: *Inflate) []const u8 {
        return self.input.buffered()[self.ip..];
    }

    /// Advances the input past what the bits consumed and reads at least one more
    /// byte (`fillMore` may read none), keeping the bytes the bit buffer holds, so
    /// they can still go back.
    fn more(self: *Inflate) Error!bool {
        self.sync();
        self.input.fill(self.input.bufferedLen() + 1) catch |e| switch (e) {
            error.EndOfStream => return false,
            error.ReadFailed => return error.ReadFailed,
        };
        return true;
    }

    fn sync(self: *Inflate) void {
        const held: usize = self.bitsleft >> 3;
        const k = self.ip - held;
        self.input.toss(k);
        self.ip = held;
    }

    fn refill(self: *Inflate) Error!void {
        if (self.bitsleft > 56) return;
        var a = self.avail();
        if (a.len < 8) {
            _ = try self.more();
            a = self.avail();
            if (a.len < 8) {
                while (self.bitsleft <= 56 and a.len > 0) {
                    self.bitbuf |= @as(u64, a[0]) << @intCast(self.bitsleft);
                    self.bitsleft += 8;
                    self.ip += 1;
                    a = a[1..];
                }
                return;
            }
        }
        const word = std.mem.readInt(u64, a[0..8], .little);
        self.bitbuf |= word << @intCast(self.bitsleft);
        const n: u7 = (63 - self.bitsleft) >> 3;
        self.ip += n;
        self.bitsleft += n * 8;
    }

    fn need(self: *Inflate, n: u7) Error!void {
        if (self.bitsleft < n) try self.refill();
        if (self.bitsleft < n) return error.EndOfStream;
    }

    fn bits(self: *Inflate, n: u7) Error!u32 {
        try self.need(n);
        const v: u32 = @truncate(self.bitbuf & ((@as(u64, 1) << @intCast(n)) - 1));
        self.drop(n);
        return v;
    }

    inline fn drop(self: *Inflate, n: u7) void {
        self.bitbuf = if (n == 64) 0 else self.bitbuf >> @intCast(n);
        self.bitsleft -= n;
    }

    /// The next whole byte once the bits are aligned: from the bit buffer first,
    /// then the input.
    fn alignedByte(self: *Inflate) Error!u8 {
        self.drop(self.bitsleft & 7);
        if (self.bitsleft >= 8) {
            const b: u8 = @truncate(self.bitbuf);
            self.drop(8);
            return b;
        }
        self.bitbuf = 0;
        if (self.avail().len == 0 and !try self.more()) return error.EndOfStream;
        const b = self.avail()[0];
        self.ip += 1;
        return b;
    }

    fn header(self: *Inflate) Error!void {
        switch (self.container) {
            .raw => {},
            .zlib => {
                const cmf = try self.alignedByte();
                const flg = try self.alignedByte();
                if (cmf & 0x0f != 8 or cmf >> 4 > 7 or (@as(u16, cmf) * 256 + flg) % 31 != 0 or flg & 0x20 != 0) return error.BadZlibHeader;
            },
            .gzip => {
                if (try self.alignedByte() != 0x1f or try self.alignedByte() != 0x8b or try self.alignedByte() != 8) return error.BadGzipHeader;
                const flg = try self.alignedByte();
                if (flg & 0xe0 != 0) return error.BadGzipHeader;
                for (0..6) |_| _ = try self.alignedByte();
                if (flg & 0x04 != 0) {
                    const xlen = @as(u16, try self.alignedByte()) | @as(u16, try self.alignedByte()) << 8;
                    for (0..xlen) |_| _ = try self.alignedByte();
                }
                if (flg & 0x08 != 0) while (try self.alignedByte() != 0) {};
                if (flg & 0x10 != 0) while (try self.alignedByte() != 0) {};
                if (flg & 0x02 != 0) for (0..2) |_| {
                    _ = try self.alignedByte();
                };
            },
        }
    }

    fn trailer(self: *Inflate) Error!void {
        switch (self.container) {
            .raw => {},
            .zlib => {
                var want: u32 = 0;
                for (0..4) |_| want = want << 8 | try self.alignedByte();
                if (want != self.adler.adler) return error.WrongChecksum;
            },
            .gzip => {
                var crc: u32 = 0;
                var size: u32 = 0;
                for (0..4) |i| crc |= @as(u32, try self.alignedByte()) << @intCast(i * 8);
                for (0..4) |i| size |= @as(u32, try self.alignedByte()) << @intCast(i * 8);
                if (crc != self.crc.final()) return error.WrongChecksum;
                if (size != self.size) return error.WrongSize;
            },
        }
    }

    fn blockHeader(self: *Inflate) Error!void {
        const h = try self.bits(3);
        self.final = h & 1 != 0;
        switch (h >> 1) {
            0 => {
                const lo = try self.alignedByte();
                const hi = try self.alignedByte();
                const nlo = try self.alignedByte();
                const nhi = try self.alignedByte();
                const len = @as(u16, lo) | @as(u16, hi) << 8;
                const nlen = @as(u16, nlo) | @as(u16, nhi) << 8;
                if (len != ~nlen) return error.WrongStoredBlockNlen;
                self.stored_left = len;
                self.state = .stored;
            },
            1 => {
                var lens: [320]u8 = undefined;
                @memset(lens[0..144], 8);
                @memset(lens[144..256], 9);
                @memset(lens[256..280], 7);
                @memset(lens[280..288], 8);
                @memset(lens[288..320], 5);
                _ = build(&self.litlen, lens[0..288], litlen_bits, litlenEntry);
                _ = build(&self.dist, lens[288..320], dist_bits, distEntry);
                self.state = .huffman;
            },
            2 => {
                try self.dynamic();
                self.state = .huffman;
            },
            else => return error.InvalidBlockType,
        }
    }

    fn dynamic(self: *Inflate) Error!void {
        const nlen = try self.bits(5) + 257;
        const ndist = try self.bits(5) + 1;
        const ncode = try self.bits(4) + 4;
        if (nlen > 286 or ndist > 30) return error.BadTree;
        var pre_lens = [_]u8{0} ** 19;
        for (pre_order[0..ncode]) |s| pre_lens[s] = @intCast(try self.bits(3));
        var pre: [1 << pre_bits]Entry = undefined;
        var pre_lens_ok = true;
        {
            var count = [_]u16{0} ** 8;
            for (pre_lens) |l| count[l] += 1;
            var left: i32 = 1;
            for (1..8) |l| {
                left = (left << 1) - count[l];
                if (left < 0) pre_lens_ok = false;
            }
            if (left != 0) pre_lens_ok = false;
        }
        if (!pre_lens_ok or !build(&pre, &pre_lens, pre_bits, preEntry)) return error.BadTree;

        var lens: [320]u8 = undefined;
        const total = nlen + ndist;
        var i: usize = 0;
        while (i < total) {
            try self.need(pre_bits);
            const e = pre[self.bitbuf & ((1 << pre_bits) - 1)];
            if (e.kind != .sym or e.n > self.bitsleft) return error.InvalidCode;
            self.drop(e.n);
            const sym = e.val;
            if (sym < 16) {
                lens[i] = @intCast(sym);
                i += 1;
                continue;
            }
            const rep: struct { n: usize, v: u8 } = switch (sym) {
                16 => blk: {
                    if (i == 0) return error.BadTree;
                    break :blk .{ .n = 3 + try self.bits(2), .v = lens[i - 1] };
                },
                17 => .{ .n = 3 + try self.bits(3), .v = 0 },
                else => .{ .n = 11 + try self.bits(7), .v = 0 },
            };
            if (i + rep.n > total) return error.BadTree;
            @memset(lens[i .. i + rep.n], rep.v);
            i += rep.n;
        }
        if (lens[256] == 0) return error.BadTree;
        if (!build(&self.litlen, lens[0..nlen], litlen_bits, litlenEntry)) return error.BadTree;
        if (!build(&self.dist, lens[nlen..total], dist_bits, distEntry)) return error.BadTree;
    }

    fn storedCopy(self: *Inflate, room: usize) Error!void {
        const r = &self.reader;
        var want: usize = @min(room, self.stored_left);
        while (want > 0 and self.bitsleft >= 8) {
            r.buffer[r.end] = try self.alignedByte();
            r.end += 1;
            self.stored_left -= 1;
            want -= 1;
        }
        if (want > 0) self.bitbuf = 0;
        while (want > 0) {
            var a = self.avail();
            if (a.len == 0) {
                if (!try self.more()) return error.EndOfStream;
                a = self.avail();
            }
            const n = @min(a.len, want);
            @memcpy(r.buffer[r.end..][0..n], a[0..n]);
            r.end += n;
            self.ip += n;
            self.stored_left -= @intCast(n);
            want -= n;
        }
    }

    inline fn lookup(table: []const Entry, bb: u64, comptime tb: u4) Entry {
        const e = table[bb & ((1 << tb) - 1)];
        if (e.kind != .link) return e;
        return table[e.val + ((bb >> tb) & ((@as(u64, 1) << e.extra) - 1))];
    }

    /// Symbols until the block ends or the output nears full. The bit buffer, its
    /// count and the input position live in locals: held in `self`, every byte
    /// stored to the output could alias them, and each symbol reloaded them. A
    /// refill comes when a literal or length and its extra bits (20) might not fit,
    /// and again before a distance and its extra bits (28), so literals run several
    /// to a refill.
    fn huffman(self: *Inflate) Error!void {
        const r = &self.reader;
        const out = r.buffer;
        var op = r.end;
        const op_end = out.len - out_slack;
        var bb = self.bitbuf;
        var bl = self.bitsleft;
        var ip = self.ip;
        var in = self.input.buffered();
        defer {
            r.end = op;
            self.bitbuf = bb;
            self.bitsleft = bl;
            self.ip = ip;
        }
        {
            // The fast loop: 16 bytes of input left cover both refills, so no symbol
            // checks for the input's end; an invalid entry is neither a literal nor a
            // length, so it still ends in an error.
            var fb: u64 = bb;
            var fl: u64 = bl;
            defer {
                bb = fb;
                bl = @intCast(fl);
            }
            while (op < op_end and in.len - ip >= 16) {
                fb |= std.mem.readInt(u64, in[ip..][0..8], .little) << @intCast(fl);
                ip += (63 - fl) >> 3;
                fl |= 56;
                var e = lookup(&self.litlen, fb, litlen_bits);
                fb >>= e.n;
                fl -= e.n;
                if (e.kind == .literal) {
                    out[op] = @truncate(e.val);
                    op += 1;
                    e = lookup(&self.litlen, fb, litlen_bits);
                    fb >>= e.n;
                    fl -= e.n;
                    if (e.kind == .literal) {
                        out[op] = @truncate(e.val);
                        op += 1;
                        continue;
                    }
                }
                if (e.kind != .length) {
                    if (e.kind != .end) return error.InvalidCode;
                    self.state = .block;
                    return;
                }
                const len: usize = e.val + @as(u32, @truncate(fb & ((@as(u64, 1) << e.extra) - 1)));
                fb >>= e.extra;
                fl -= e.extra;
                fb |= std.mem.readInt(u64, in[ip..][0..8], .little) << @intCast(fl);
                ip += (63 - fl) >> 3;
                fl |= 56;
                e = lookup(&self.dist, fb, dist_bits);
                if (e.kind != .length) return error.InvalidCode;
                fb >>= e.n;
                fl -= e.n;
                const d: usize = e.val + @as(u32, @truncate(fb & ((@as(u64, 1) << e.extra) - 1)));
                fb >>= e.extra;
                fl -= e.extra;
                if (d > op) return error.InvalidMatch;
                copyMatch(out, op, d, len);
                op += len;
            }
        }
        while (op < op_end) {
            if (bl < 20) {
                if (in.len - ip >= 8) {
                    bb |= std.mem.readInt(u64, in[ip..][0..8], .little) << @intCast(bl);
                    const n: u7 = (63 - bl) >> 3;
                    ip += n;
                    bl += n * 8;
                } else {
                    self.bitbuf = bb;
                    self.bitsleft = bl;
                    self.ip = ip;
                    try self.refill();
                    bb = self.bitbuf;
                    bl = self.bitsleft;
                    ip = self.ip;
                    in = self.input.buffered();
                }
            }
            var e = lookup(&self.litlen, bb, litlen_bits);
            if (e.n > bl or e.n == 0) return if (e.n == 0) error.InvalidCode else error.EndOfStream;
            bb >>= e.n;
            bl -= e.n;
            if (e.kind == .literal) {
                out[op] = @intCast(e.val);
                op += 1;
                continue;
            }
            if (e.kind != .length) {
                if (e.kind != .end) return error.InvalidCode;
                self.state = .block;
                return;
            }
            if (e.extra > bl) return error.EndOfStream;
            const len: usize = e.val + @as(u32, @truncate(bb & ((@as(u64, 1) << e.extra) - 1)));
            bb >>= e.extra;
            bl -= e.extra;
            if (bl < 28) {
                if (in.len - ip >= 8) {
                    bb |= std.mem.readInt(u64, in[ip..][0..8], .little) << @intCast(bl);
                    const n: u7 = (63 - bl) >> 3;
                    ip += n;
                    bl += n * 8;
                } else {
                    self.bitbuf = bb;
                    self.bitsleft = bl;
                    self.ip = ip;
                    try self.refill();
                    bb = self.bitbuf;
                    bl = self.bitsleft;
                    ip = self.ip;
                    in = self.input.buffered();
                }
            }
            e = lookup(&self.dist, bb, dist_bits);
            if (e.kind != .length or e.n == 0) return error.InvalidCode;
            if (e.n + @as(u7, e.extra) > bl) return error.EndOfStream;
            bb >>= e.n;
            bl -= e.n;
            const d: usize = e.val + @as(u32, @truncate(bb & ((@as(u64, 1) << e.extra) - 1)));
            bb >>= e.extra;
            bl -= e.extra;
            if (d > op) return error.InvalidMatch;
            copyMatch(out, op, d, len);
            op += len;
        }
    }

    /// A match of distance 8 or more takes its first 16 bytes in two words whatever
    /// its length, the output's slack covering the bytes past it, and the rest a word
    /// at a time; a word read from `s` never reaches past what is already written.
    inline fn copyMatch(out: []u8, op: usize, d: usize, len: usize) void {
        var s = op - d;
        var t = op;
        const end = op + len;
        if (d >= 8) {
            std.mem.writeInt(u64, out[t..][0..8], std.mem.readInt(u64, out[s..][0..8], .little), .little);
            std.mem.writeInt(u64, out[t + 8 ..][0..8], std.mem.readInt(u64, out[s + 8 ..][0..8], .little), .little);
            s += 16;
            t += 16;
            while (t < end) : ({
                s += 8;
                t += 8;
            }) std.mem.writeInt(u64, out[t..][0..8], std.mem.readInt(u64, out[s..][0..8], .little), .little);
        } else if (d == 1) {
            @memset(out[t..end], out[s]);
        } else while (t < end) : ({
            s += 1;
            t += 1;
        }) out[t] = out[s];
    }
};

/// A gzip stream of one or more members read as one, as `gzip -d` reads
/// concatenated members (`pigz`, `bgzip`, appends): a member's end starts the next
/// while input remains.
pub const Gunzip = struct {
    src: *std.Io.Reader,
    dec: Inflate,
    window: []u8,
    interface: std.Io.Reader,

    pub fn init(self: *Gunzip, src: *std.Io.Reader, window: []u8, buf: []u8) void {
        self.* = .{
            .src = src,
            .dec = .init(src, .gzip, window),
            .window = window,
            .interface = .{ .vtable = &.{ .stream = streamFn }, .buffer = buf, .seek = 0, .end = 0 },
        };
    }

    fn streamFn(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Gunzip = @fieldParentPtr("interface", r);
        while (true) {
            return self.dec.reader.stream(w, limit) catch |e| switch (e) {
                error.EndOfStream => {
                    _ = self.src.peekByte() catch |e2| return switch (e2) {
                        error.EndOfStream => error.EndOfStream,
                        error.ReadFailed => error.ReadFailed,
                    };
                    self.dec = .init(self.src, .gzip, self.window);
                    continue;
                },
                else => |x| return x,
            };
        }
    }
};

/// `comp` decoded whole into `out`, which it must fill exactly.
pub fn decompress(comp: []const u8, out: []u8, container: Container, scratch: []u8) Error!void {
    var in: Reader = .fixed(comp);
    var d = Inflate.init(&in, container, scratch);
    var at: usize = 0;
    while (true) {
        const got = d.reader.readSliceShort(out[at..]) catch return d.err orelse error.ReadFailed;
        at += got;
        if (at == out.len) break;
        if (got == 0) return error.WrongSize;
    }
    var one: [1]u8 = undefined;
    const extra = d.reader.readSliceShort(&one) catch return d.err orelse error.ReadFailed;
    if (extra != 0) return error.WrongSize;
}

const testing = std.testing;
const Deflate = @import("deflate.zig").Deflate;

fn inflateAll(a: std.mem.Allocator, comp: []const u8, container: Container) ![]u8 {
    var in: Reader = .fixed(comp);
    const buf = try a.alloc(u8, buffer_len);
    var d = Inflate.init(&in, container, buf);
    var out: std.Io.Writer.Allocating = .init(a);
    _ = d.reader.streamRemaining(&out.writer) catch return d.err orelse error.ReadFailed;
    return out.written();
}

fn stdInflate(a: std.mem.Allocator, comp: []const u8, container: Container) ![]u8 {
    var in: Reader = .fixed(comp);
    const win = try a.alloc(u8, std.compress.flate.max_window_len);
    var d = std.compress.flate.Decompress.init(&in, switch (container) {
        .raw => .raw,
        .gzip => .gzip,
        .zlib => .zlib,
    }, win);
    var out: std.Io.Writer.Allocating = .init(a);
    _ = try d.reader.streamRemaining(&out.writer);
    return out.written();
}

fn compress(a: std.mem.Allocator, data: []const u8, container: Container, fast: bool) ![]u8 {
    var sink: std.Io.Writer.Allocating = .init(a);
    const z = try Deflate.init(a, &sink.writer, .{ .level = if (fast) .fast else .default, .container = switch (container) {
        .raw => .raw,
        .gzip => .gzip,
        .zlib => .zlib,
    } });
    try z.interface.writeAll(data);
    try z.finish();
    return sink.written();
}

fn sample(a: std.mem.Allocator, r: std.Random, n: usize) ![]u8 {
    const out = try a.alloc(u8, n);
    var i: usize = 0;
    while (i < n) {
        switch (r.uintLessThan(u8, 4)) {
            0 => {
                out[i] = r.int(u8);
                i += 1;
            },
            1 => {
                const len = @min(n - i, 1 + r.uintLessThan(usize, 300));
                if (i > 0) {
                    const d = 1 + r.uintLessThan(usize, @min(i, 40000));
                    for (0..len) |k| out[i + k] = out[i + k - d];
                } else @memset(out[i..][0..len], 'a');
                i += len;
            },
            2 => {
                const words = [_][]const u8{ "DELIVER IN PERSON", "1996-03-13", ",", "\n", "TRUCK", "0.04", "|" };
                const w = words[r.uintLessThan(usize, words.len)];
                const len = @min(n - i, w.len);
                @memcpy(out[i..][0..len], w[0..len]);
                i += len;
            },
            else => {
                out[i] = 'a' + r.uintLessThan(u8, 3);
                i += 1;
            },
        }
    }
    return out;
}

test "inflate reads back what deflate wrote, in every container and level, at sizes across the window and the buffer" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0x1f8b);
    const r = prng.random();
    for ([_]usize{ 0, 1, 7, 100, 4096, 40_000, 70_000, 300_000, 1_000_000 }) |n| {
        const data = try sample(a, r, n);
        for ([_]Container{ .raw, .gzip, .zlib }) |c| for ([_]bool{ false, true }) |fast| {
            const comp = try compress(a, data, c, fast);
            try testing.expectEqualSlices(u8, data, try inflateAll(a, comp, c));
        };
    }
    const noise = try a.alloc(u8, 200_000);
    r.bytes(noise);
    try testing.expectEqualSlices(u8, noise, try inflateAll(a, try compress(a, noise, .gzip, false), .gzip));
}

test "inflate takes concatenated gzip members one at a time, leaving the next member's bytes in the input" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const one = try compress(a, "first member\n", .gzip, false);
    const two = try compress(a, "second\n", .gzip, true);
    const both = try std.mem.concat(a, u8, &.{ one, two });
    var in: Reader = .fixed(both);
    const buf = try a.alloc(u8, buffer_len);
    var d = Inflate.init(&in, .gzip, buf);
    var out: std.Io.Writer.Allocating = .init(a);
    _ = try d.reader.streamRemaining(&out.writer);
    try testing.expectEqualStrings("first member\n", out.written());
    var d2 = Inflate.init(&in, .gzip, buf);
    out.clearRetainingCapacity();
    _ = try d2.reader.streamRemaining(&out.writer);
    try testing.expectEqualStrings("second\n", out.written());
}

test "inflate agrees with std.compress.flate on damaged streams, but refuses a header, checksum or size std does not check" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var prng = std.Random.DefaultPrng.init(0xdeaf);
    const r = prng.random();
    var refused: usize = 0;
    for (0..3000) |round| {
        const data = try sample(a, r, r.uintLessThan(usize, 3000));
        const c: Container = @enumFromInt(round % 3);
        const comp = try a.dupe(u8, try compress(a, data, c, round % 2 == 0));
        if (comp.len > 0) switch (r.uintLessThan(u8, 3)) {
            0 => comp[r.uintLessThan(usize, comp.len)] ^= @as(u8, 1) << @intCast(r.uintLessThan(u3, 7)),
            1 => comp[r.uintLessThan(usize, comp.len)] = r.int(u8),
            else => {},
        };
        const cut = if (r.uintLessThan(u8, 4) == 0) r.uintLessThan(usize, comp.len + 1) else comp.len;
        const theirs = stdInflate(a, comp[0..cut], c) catch null;
        var container_refused = false;
        const mine = inflateAll(a, comp[0..cut], c) catch |e| blk: {
            container_refused = switch (e) {
                error.BadZlibHeader, error.BadGzipHeader, error.WrongChecksum, error.WrongSize => true,
                else => false,
            };
            break :blk null;
        };
        if (theirs) |t| {
            if (c != .raw and !std.mem.eql(u8, t, data)) {
                if (mine != null) {
                    std.debug.print("round {d}: a stream its checksum disowns was read\n", .{round});
                    return error.TestUnexpectedResult;
                }
                refused += 1;
                continue;
            }
            if (container_refused) {
                refused += 1;
                continue;
            }
            const m = mine orelse {
                std.debug.print("round {d}: std read {d} bytes, inflate refused\n", .{ round, t.len });
                return error.TestUnexpectedResult;
            };
            try testing.expectEqualSlices(u8, t, m);
        } else {
            refused += 1;
            if (mine != null and c != .raw) {
                std.debug.print("round {d}: std refused, inflate read {d} bytes\n", .{ round, mine.?.len });
                return error.TestUnexpectedResult;
            }
        }
    }
    try testing.expect(refused > 500);
}
