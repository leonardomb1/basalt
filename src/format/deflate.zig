//! A DEFLATE writer (RFC 1951) in a gzip (RFC 1952), zlib (RFC 1950) or raw
//! container, for `LOAD INTO 'x.csv.gz'`, gzip stream-load bodies and gzip
//! Parquet pages. Zig 0.15's std ships only the decompressor.
//!
//! LZ77 over a 32 KiB window and a dynamic Huffman code per block of input, at
//! one of two levels. `default` follows hash chains with one step of lazy
//! matching, as zlib's default level does. `fast` probes a single slot of a
//! 4-byte hash per position, takes the first match greedily and strides faster
//! through a run of misses, as LZ4 does: several times the speed for a slightly
//! larger output. `data[0..cur]` is the history matches may reach into and
//! `data[cur..fill]` the input not yet compressed; after each block the last
//! window is kept as history. A block that would come out larger than its input
//! is stored instead, so an incompressible stream grows by a few bytes per
//! 64 KiB, not by a fraction.

const std = @import("std");

const window = 1 << 15;
const block_in = 1 << 17;
const min_match = 3;
const max_match = 258;
const hash_bits = 15;
const fast_hash_bits = 16;
const max_chain = 48;
const nice_len = 128;
const lazy_len = 32;

const Token = packed struct(u32) {
    len: u16,
    val: u16,
};

pub const Level = enum { default, fast };
pub const Container = enum { raw, gzip, zlib };
pub const Options = struct { level: Level = .default, container: Container = .gzip };

pub const Deflate = struct {
    out: *std.Io.Writer,
    interface: std.Io.Writer,
    opts: Options,
    data: []u8,
    cur: usize = 0,
    fill: usize = 0,
    head: []i32,
    prev: []i32,
    tokens: []Token,
    crc: u32 = 0xffff_ffff,
    adler: std.hash.Adler32 = .{},
    size: u32 = 0,
    bits: u64 = 0,
    nbits: u6 = 0,
    obuf: [8192]u8 = undefined,
    olen: usize = 0,
    done: bool = false,

    /// Writes the container's header to `out` at once; the fast level keeps no
    /// hash chains, so it allocates no `prev`.
    pub fn init(gpa: std.mem.Allocator, out: *std.Io.Writer, opts: Options) !*Deflate {
        const self = try gpa.create(Deflate);
        errdefer gpa.destroy(self);
        const data = try gpa.alloc(u8, window + block_in);
        errdefer gpa.free(data);
        const head = try gpa.alloc(i32, @as(usize, 1) << if (opts.level == .fast) fast_hash_bits else hash_bits);
        errdefer gpa.free(head);
        const prev = try gpa.alloc(i32, if (opts.level == .fast) 0 else data.len);
        errdefer gpa.free(prev);
        const tokens = try gpa.alloc(Token, data.len);
        errdefer gpa.free(tokens);
        const buf = try gpa.alloc(u8, 64 * 1024);
        errdefer gpa.free(buf);
        @memset(head, -1);
        self.* = .{
            .out = out,
            .interface = .{ .buffer = buf, .vtable = &.{ .drain = drainFn } },
            .opts = opts,
            .data = data,
            .head = head,
            .prev = prev,
            .tokens = tokens,
        };
        try self.writeHeader();
        return self;
    }

    pub fn deinit(self: *Deflate, gpa: std.mem.Allocator) void {
        gpa.free(self.interface.buffer);
        gpa.free(self.tokens);
        gpa.free(self.prev);
        gpa.free(self.head);
        gpa.free(self.data);
        gpa.destroy(self);
    }

    /// Starts a new stream on the same `out`, header first, reusing the buffers;
    /// input written since the last `finish` is dropped.
    pub fn reset(self: *Deflate) !void {
        @memset(self.head, -1);
        self.interface.end = 0;
        self.cur = 0;
        self.fill = 0;
        self.crc = 0xffff_ffff;
        self.adler = .{};
        self.size = 0;
        self.bits = 0;
        self.nbits = 0;
        self.olen = 0;
        self.done = false;
        try self.writeHeader();
    }

    fn writeHeader(self: *Deflate) !void {
        const fast = self.opts.level == .fast;
        switch (self.opts.container) {
            .raw => {},
            .gzip => try self.out.writeAll(&.{ 0x1f, 0x8b, 8, 0, 0, 0, 0, 0, if (fast) 4 else 0, 255 }),
            .zlib => try self.out.writeAll(&.{ 0x78, if (fast) 0x01 else 0x9c }),
        }
    }

    fn drainFn(w: *std.Io.Writer, chunks: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Deflate = @fieldParentPtr("interface", w);
        self.feed(w.buffered()) catch return error.WriteFailed;
        _ = w.consumeAll();
        var total: usize = 0;
        for (chunks[0 .. chunks.len - 1]) |c| {
            self.feed(c) catch return error.WriteFailed;
            total += c.len;
        }
        const last = chunks[chunks.len - 1];
        for (0..splat) |_| {
            self.feed(last) catch return error.WriteFailed;
            total += last.len;
        }
        return total;
    }

    fn feed(self: *Deflate, bytes: []const u8) !void {
        switch (self.opts.container) {
            .raw => {},
            .gzip => self.crc = crc32Update(self.crc, bytes),
            .zlib => self.adler.update(bytes),
        }
        self.size +%= @truncate(bytes.len);
        var rest = bytes;
        while (rest.len > 0) {
            const n = @min(rest.len, self.data.len - self.fill);
            @memcpy(self.data[self.fill..][0..n], rest[0..n]);
            self.fill += n;
            rest = rest[n..];
            if (self.fill == self.data.len) try self.compressBlock(false);
        }
    }

    /// Compresses the buffer as the last block and writes the trailer; flushing the
    /// underlying writer is left to its owner.
    pub fn finish(self: *Deflate) !void {
        if (self.done) return;
        self.done = true;
        try self.interface.flush();
        try self.compressBlock(true);
        try self.alignByte();
        var tail: [8]u8 = undefined;
        const trailer: []const u8 = switch (self.opts.container) {
            .raw => &.{},
            .gzip => blk: {
                std.mem.writeInt(u32, tail[0..4], ~self.crc, .little);
                std.mem.writeInt(u32, tail[4..8], self.size, .little);
                break :blk &tail;
            },
            .zlib => blk: {
                std.mem.writeInt(u32, tail[0..4], self.adler.adler, .big);
                break :blk tail[0..4];
            },
        };
        try self.emitBytes(trailer);
        try self.flushOut();
    }

    fn hashAt(self: *const Deflate, i: usize) usize {
        const v = @as(u32, self.data[i]) | @as(u32, self.data[i + 1]) << 8 | @as(u32, self.data[i + 2]) << 16;
        return (v *% 2654435761) >> (32 - hash_bits);
    }

    fn insert(self: *Deflate, i: usize) void {
        if (i + min_match > self.fill) return;
        const h = self.hashAt(i);
        self.prev[i] = self.head[h];
        self.head[h] = @intCast(i);
    }

    const Match = struct { len: usize = 0, dist: usize = 0 };

    /// How far `d[c..]` and `d[p..]` agree, from `from` up to `max_len`, eight bytes a step.
    fn extend(d: []const u8, c: usize, p: usize, from: usize, max_len: usize) usize {
        var n = from;
        while (n + 8 <= max_len) {
            const a = std.mem.readInt(u64, d[c + n ..][0..8], .little);
            const b = std.mem.readInt(u64, d[p + n ..][0..8], .little);
            if (a != b) return n + @ctz(a ^ b) / 8;
            n += 8;
        }
        while (n < max_len and d[c + n] == d[p + n]) n += 1;
        return n;
    }

    fn findMatch(self: *const Deflate, p: usize, end: usize, prev_len: usize) Match {
        const max_len = @min(max_match, end - p);
        if (max_len < min_match or prev_len >= max_len) return .{};
        var best = Match{ .len = @max(prev_len, min_match - 1) };
        var chain: usize = if (prev_len >= lazy_len) max_chain / 4 else max_chain;
        var cand = self.prev[p];
        const lowest: isize = @as(isize, @intCast(p)) - window;
        const d = self.data;
        while (cand >= 0 and cand > lowest and chain > 0) : (chain -= 1) {
            const c: usize = @intCast(cand);
            if (d[c + best.len] == d[p + best.len] and d[c] == d[p] and d[c + 1] == d[p + 1]) {
                const n = extend(d, c, p, 2, max_len);
                if (n > best.len) {
                    best = .{ .len = n, .dist = p - c };
                    if (n >= nice_len or n == max_len) break;
                }
            }
            cand = self.prev[c];
        }
        return if (best.dist == 0) .{} else best;
    }

    /// The default level's tokens for `data[cur..end]`: hash chains, one step lazy.
    fn tokenizeLazy(self: *Deflate, end: usize) usize {
        var nt: usize = 0;
        var p = self.cur;
        var pend: ?Match = null;
        while (p < end) {
            self.insert(p);
            const prev_len = if (pend) |m| m.len else 0;
            const here = if (prev_len < nice_len) self.findMatch(p, end, prev_len) else Match{};
            if (pend) |m| {
                if (m.len >= min_match and here.len <= m.len) {
                    self.tokens[nt] = .{ .len = @intCast(m.len), .val = @intCast(m.dist) };
                    nt += 1;
                    const stop = p - 1 + m.len;
                    var k = p + 1;
                    while (k < stop) : (k += 1) self.insert(k);
                    p = stop;
                    pend = null;
                    continue;
                }
                self.tokens[nt] = .{ .len = 0, .val = self.data[p - 1] };
                nt += 1;
            }
            pend = here;
            p += 1;
        }
        if (pend != null) {
            self.tokens[nt] = .{ .len = 0, .val = self.data[p - 1] };
            nt += 1;
        }
        return nt;
    }

    fn fastHash(v: u32) usize {
        return (v *% 2654435761) >> (32 - fast_hash_bits);
    }

    /// The fast level's tokens for `data[cur..end]`: one 4-byte hash slot per
    /// probed position, the first match taken whole, and a probe stride that grows
    /// by one every 32 misses in a row, so incompressible input is crossed quickly.
    fn tokenizeFast(self: *Deflate, end: usize) usize {
        const d = self.data;
        const tokens = self.tokens;
        var nt: usize = 0;
        var p = self.cur;
        var lit = p;
        var miss: usize = 0;
        while (p + 4 <= end) {
            const v = std.mem.readInt(u32, d[p..][0..4], .little);
            const h = fastHash(v);
            const cand = self.head[h];
            self.head[h] = @intCast(p);
            if (cand >= 0) {
                const c: usize = @intCast(cand);
                if (p - c <= window and std.mem.readInt(u32, d[c..][0..4], .little) == v) {
                    const n = extend(d, c, p, 4, @min(max_match, end - p));
                    while (lit < p) : (lit += 1) {
                        tokens[nt] = .{ .len = 0, .val = d[lit] };
                        nt += 1;
                    }
                    tokens[nt] = .{ .len = @intCast(n), .val = @intCast(p - c) };
                    nt += 1;
                    p += n;
                    lit = p;
                    miss = 0;
                    if (p + 2 <= end) self.head[fastHash(std.mem.readInt(u32, d[p - 2 ..][0..4], .little))] = @intCast(p - 2);
                    continue;
                }
            }
            miss += 1;
            p += 1 + (miss >> 5);
        }
        while (lit < end) : (lit += 1) {
            tokens[nt] = .{ .len = 0, .val = d[lit] };
            nt += 1;
        }
        return nt;
    }

    fn compressBlock(self: *Deflate, final: bool) !void {
        const end = self.fill;
        const nt = switch (self.opts.level) {
            .default => self.tokenizeLazy(end),
            .fast => self.tokenizeFast(end),
        };
        try self.writeBlock(self.tokens[0..nt], self.data[self.cur..end], final);

        self.cur = end;
        if (self.fill > window) {
            const shift = self.fill - window;
            std.mem.copyForwards(u8, self.data[0..window], self.data[shift..self.fill]);
            const s: i32 = @intCast(shift);
            for (self.head) |*h| h.* = if (h.* >= s) h.* - s else -1;
            if (self.prev.len > 0) {
                std.mem.copyForwards(i32, self.prev[0..window], self.prev[shift..self.fill]);
                for (self.prev[0..window]) |*h| h.* = if (h.* >= s) h.* - s else -1;
            }
            self.cur = window;
            self.fill = window;
        }
    }
    fn writeBlock(self: *Deflate, tokens: []const Token, raw: []const u8, final: bool) !void {
        var lfreq = [_]u32{0} ** 286;
        var dfreq = [_]u32{0} ** 30;
        for (tokens) |t| {
            if (t.len == 0) {
                lfreq[t.val] += 1;
            } else {
                lfreq[lenCode(t.len).code] += 1;
                dfreq[distCode(t.val).code] += 1;
            }
        }
        lfreq[256] = 1;
        var llen = [_]u8{0} ** 286;
        var dlen = [_]u8{0} ** 30;
        buildLengths(&lfreq, &llen, 15);
        buildLengths(&dfreq, &dlen, 15);

        var hlit: usize = 286;
        while (hlit > 257 and llen[hlit - 1] == 0) hlit -= 1;
        var hdist: usize = 30;
        while (hdist > 1 and dlen[hdist - 1] == 0) hdist -= 1;

        var all: [286 + 30]u8 = undefined;
        @memcpy(all[0..hlit], llen[0..hlit]);
        @memcpy(all[hlit..][0..hdist], dlen[0..hdist]);
        var rle: [286 + 30]struct { sym: u8, extra: u8 } = undefined;
        const nrle = runLengths(all[0 .. hlit + hdist], &rle);
        var cfreq = [_]u32{0} ** 19;
        for (rle[0..nrle]) |r| cfreq[r.sym] += 1;
        var clen = [_]u8{0} ** 19;
        buildLengths(&cfreq, &clen, 7);
        const order = [19]u8{ 16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15 };
        var hclen: usize = 19;
        while (hclen > 4 and clen[order[hclen - 1]] == 0) hclen -= 1;

        var cost: u64 = 3 + 5 + 5 + 4 + 3 * hclen;
        for (rle[0..nrle]) |r| cost += clen[r.sym] + @as(u64, switch (r.sym) {
            16 => 2,
            17 => 3,
            18 => 7,
            else => 0,
        });
        for (lfreq, llen, 0..) |f, l, sym| cost += @as(u64, f) * (l + (if (sym >= 257) lenExtraBits(sym) else 0));
        for (dfreq, dlen, 0..) |f, l, sym| cost += @as(u64, f) * (l + distExtraBits(sym));
        const stored_cost: u64 = (raw.len + 5 * (raw.len / 65535 + 1)) * 8 + 8;
        if (cost >= stored_cost) return self.writeStored(raw, final);

        var lcode: [286]u16 = undefined;
        var dcode: [30]u16 = undefined;
        var ccode: [19]u16 = undefined;
        canonical(&llen, &lcode);
        canonical(&dlen, &dcode);
        canonical(&clen, &ccode);

        try self.put(@intFromBool(final), 1);
        try self.put(2, 2);
        try self.put(@intCast(hlit - 257), 5);
        try self.put(@intCast(hdist - 1), 5);
        try self.put(@intCast(hclen - 4), 4);
        for (order[0..hclen]) |o| try self.put(clen[o], 3);
        for (rle[0..nrle]) |r| {
            try self.put(ccode[r.sym], @intCast(clen[r.sym]));
            switch (r.sym) {
                16 => try self.put(r.extra, 2),
                17 => try self.put(r.extra, 3),
                18 => try self.put(r.extra, 7),
                else => {},
            }
        }
        for (tokens) |t| {
            if (t.len == 0) {
                try self.put(lcode[t.val], @intCast(llen[t.val]));
            } else {
                const lc = lenCode(t.len);
                try self.put(lcode[lc.code], @intCast(llen[lc.code]));
                if (lc.nbits > 0) try self.put(lc.extra, lc.nbits);
                const dc = distCode(t.val);
                try self.put(dcode[dc.code], @intCast(dlen[dc.code]));
                if (dc.nbits > 0) try self.put(dc.extra, dc.nbits);
            }
        }
        try self.put(lcode[256], @intCast(llen[256]));
    }

    fn writeStored(self: *Deflate, raw: []const u8, final: bool) !void {
        var rest = raw;
        while (true) {
            const n = @min(rest.len, 65535);
            const last = final and n == rest.len;
            try self.put(@intFromBool(last), 1);
            try self.put(0, 2);
            try self.alignByte();
            var hdr: [4]u8 = undefined;
            std.mem.writeInt(u16, hdr[0..2], @intCast(n), .little);
            std.mem.writeInt(u16, hdr[2..4], ~@as(u16, @intCast(n)), .little);
            try self.emitBytes(&hdr);
            try self.emitBytes(rest[0..n]);
            rest = rest[n..];
            if (rest.len == 0) break;
        }
    }

    fn put(self: *Deflate, v: u32, n: u6) !void {
        self.bits |= @as(u64, v) << self.nbits;
        self.nbits += n;
        if (self.nbits >= 32) {
            if (self.olen + 4 > self.obuf.len) try self.flushOut();
            std.mem.writeInt(u32, self.obuf[self.olen..][0..4], @truncate(self.bits), .little);
            self.olen += 4;
            self.bits >>= 32;
            self.nbits -= 32;
        }
    }

    fn alignByte(self: *Deflate) !void {
        while (self.nbits > 0) {
            if (self.olen == self.obuf.len) try self.flushOut();
            self.obuf[self.olen] = @truncate(self.bits);
            self.olen += 1;
            self.bits >>= 8;
            self.nbits = if (self.nbits > 8) self.nbits - 8 else 0;
        }
        self.bits = 0;
    }

    fn emitBytes(self: *Deflate, bytes: []const u8) !void {
        try self.flushOut();
        try self.out.writeAll(bytes);
    }

    fn flushOut(self: *Deflate) !void {
        if (self.olen == 0) return;
        try self.out.writeAll(self.obuf[0..self.olen]);
        self.olen = 0;
    }
};

const crc_tables = blk: {
    @setEvalBranchQuota(50_000);
    var t: [8][256]u32 = undefined;
    for (0..256) |i| {
        var c: u32 = i;
        for (0..8) |_| c = if (c & 1 != 0) 0xEDB88320 ^ (c >> 1) else c >> 1;
        t[0][i] = c;
    }
    for (1..8) |k| {
        for (0..256) |i| t[k][i] = (t[k - 1][i] >> 8) ^ t[0][t[k - 1][i] & 0xff];
    }
    break :blk t;
};

/// gzip's CRC-32 register (pre- and post-inverted by the caller), eight bytes a
/// step: std's goes a byte at a time, slow enough to cost the fast level a third
/// of its speed.
fn crc32Update(crc: u32, bytes: []const u8) u32 {
    const t = &crc_tables;
    var c = crc;
    var i: usize = 0;
    while (i + 8 <= bytes.len) : (i += 8) {
        const lo = std.mem.readInt(u32, bytes[i..][0..4], .little) ^ c;
        const hi = std.mem.readInt(u32, bytes[i + 4 ..][0..4], .little);
        c = t[7][lo & 0xff] ^ t[6][(lo >> 8) & 0xff] ^ t[5][(lo >> 16) & 0xff] ^ t[4][lo >> 24] ^
            t[3][hi & 0xff] ^ t[2][(hi >> 8) & 0xff] ^ t[1][(hi >> 16) & 0xff] ^ t[0][hi >> 24];
    }
    for (bytes[i..]) |b| c = t[0][(c ^ b) & 0xff] ^ (c >> 8);
    return c;
}

const Coded = struct { code: u16, extra: u32, nbits: u6 };

fn lenCode(len: u16) Coded {
    if (len == 258) return .{ .code = 285, .extra = 0, .nbits = 0 };
    const l: u32 = len - 3;
    if (l < 8) return .{ .code = @intCast(257 + l), .extra = 0, .nbits = 0 };
    const n: u5 = @intCast(31 - @clz(l));
    const nb: u5 = n - 2;
    return .{ .code = @intCast(257 + 4 * (@as(u32, n) - 1) + ((l >> nb) & 3)), .extra = l & ((@as(u32, 1) << nb) - 1), .nbits = nb };
}

fn distCode(dist: u16) Coded {
    const d: u32 = @as(u32, dist) - 1;
    if (d < 4) return .{ .code = @intCast(d), .extra = 0, .nbits = 0 };
    const n: u5 = @intCast(31 - @clz(d));
    const nb: u5 = n - 1;
    return .{ .code = @intCast(2 * @as(u32, n) + ((d >> nb) & 1)), .extra = d & ((@as(u32, 1) << nb) - 1), .nbits = nb };
}

fn lenExtraBits(sym: usize) u32 {
    if (sym < 265 or sym == 285) return 0;
    return @intCast((sym - 261) / 4);
}

fn distExtraBits(sym: usize) u32 {
    return if (sym < 4) 0 else @intCast(sym / 2 - 1);
}

/// Code-length run-length coding (RFC 1951 3.2.7): 16 repeats the previous
/// length 3–6 times, 17 and 18 write 3–10 and 11–138 zeros.
fn runLengths(lens: []const u8, out: anytype) usize {
    var n: usize = 0;
    var i: usize = 0;
    while (i < lens.len) {
        const l = lens[i];
        var run: usize = 1;
        while (i + run < lens.len and lens[i + run] == l) run += 1;
        i += run;
        if (l == 0) {
            while (run >= 11) {
                const r = @min(run, 138);
                out[n] = .{ .sym = 18, .extra = @intCast(r - 11) };
                n += 1;
                run -= r;
            }
            if (run >= 3) {
                out[n] = .{ .sym = 17, .extra = @intCast(run - 3) };
                n += 1;
                run = 0;
            }
        } else {
            out[n] = .{ .sym = l, .extra = 0 };
            n += 1;
            run -= 1;
            while (run >= 3) {
                const r = @min(run, 6);
                out[n] = .{ .sym = 16, .extra = @intCast(r - 3) };
                n += 1;
                run -= r;
            }
        }
        while (run > 0) : (run -= 1) {
            out[n] = .{ .sym = l, .extra = 0 };
            n += 1;
        }
    }
    return n;
}

/// Minimum-redundancy lengths (Moffat and Katajainen), cut to `max_bits` and made
/// whole again as miniz does. Fewer than two symbols get two: a one-symbol code is
/// incomplete and some decoders refuse it.
fn buildLengths(freq: []u32, lens: []u8, max_bits: u5) void {
    var syms: [286]u16 = undefined;
    var n: usize = 0;
    for (freq, 0..) |f, i| if (f > 0) {
        syms[n] = @intCast(i);
        n += 1;
    };
    while (n < 2) {
        const add: u16 = if (n == 1 and syms[0] == 0) 1 else 0;
        freq[add] = 1;
        syms[n] = add;
        n += 1;
    }
    const s = syms[0..n];
    std.mem.sort(u16, s, freq, struct {
        fn lt(f: []u32, a: u16, b: u16) bool {
            return f[a] < f[b] or (f[a] == f[b] and a < b);
        }
    }.lt);
    var a: [286]u32 = undefined;
    for (s, 0..) |sym, i| a[i] = freq[sym];
    minimumRedundancy(a[0..n]);

    var count = [_]u32{0} ** 32;
    for (a[0..n]) |l| count[@min(l, 31)] += 1;
    var over = false;
    var i: usize = max_bits + 1;
    while (i < 32) : (i += 1) if (count[i] > 0) {
        over = true;
        count[max_bits] += count[i];
        count[i] = 0;
    };
    if (over) {
        var total: u32 = 0;
        var b: usize = max_bits;
        while (b > 0) : (b -= 1) total += count[b] << @intCast(max_bits - b);
        const full = @as(u32, 1) << max_bits;
        while (total != full) {
            count[max_bits] -= 1;
            var k: usize = max_bits - 1;
            while (k > 0) : (k -= 1) if (count[k] > 0) {
                count[k] -= 1;
                count[k + 1] += 2;
                break;
            };
            total -= 1;
        }
    }
    @memset(lens, 0);
    var j: usize = 0;
    var len: usize = max_bits;
    while (len > 0) : (len -= 1) {
        var c = count[len];
        while (c > 0) : (c -= 1) {
            lens[s[j]] = @intCast(len);
            j += 1;
        }
    }
}

/// In place: `a` holds frequencies in ascending order, and leaves holding each
/// one's code length.
fn minimumRedundancy(a: []u32) void {
    const n = a.len;
    if (n == 1) {
        a[0] = 1;
        return;
    }
    a[0] += a[1];
    var root: usize = 0;
    var leaf: usize = 2;
    var next: usize = 1;
    while (next < n - 1) : (next += 1) {
        if (leaf >= n or a[root] < a[leaf]) {
            a[next] = a[root];
            a[root] = @intCast(next);
            root += 1;
        } else {
            a[next] = a[leaf];
            leaf += 1;
        }
        if (leaf >= n or (root < next and a[root] < a[leaf])) {
            a[next] += a[root];
            a[root] = @intCast(next);
            root += 1;
        } else {
            a[next] += a[leaf];
            leaf += 1;
        }
    }
    a[n - 2] = 0;
    if (n >= 3) {
        var k: usize = n - 2;
        while (k > 0) {
            k -= 1;
            a[k] = a[a[k]] + 1;
        }
    }
    var avbl: usize = 1;
    var used: usize = 0;
    var dpth: u32 = 0;
    var r: isize = @as(isize, @intCast(n)) - 2;
    var nx: isize = @as(isize, @intCast(n)) - 1;
    while (avbl > 0) {
        while (r >= 0 and a[@intCast(r)] == dpth) {
            used += 1;
            r -= 1;
        }
        while (avbl > used) {
            a[@intCast(nx)] = dpth;
            nx -= 1;
            avbl -= 1;
        }
        avbl = 2 * used;
        dpth += 1;
        used = 0;
    }
}

/// Canonical codes, bit-reversed for least-significant-first output.
fn canonical(lens: []const u8, codes: []u16) void {
    var count = [_]u16{0} ** 16;
    for (lens) |l| count[l] += 1;
    count[0] = 0;
    var next = [_]u16{0} ** 16;
    var code: u16 = 0;
    for (1..16) |b| {
        code = (code + count[b - 1]) << 1;
        next[b] = code;
    }
    for (lens, 0..) |l, i| {
        if (l == 0) continue;
        const c = next[l];
        next[l] += 1;
        codes[i] = @bitReverse(c) >> @intCast(16 - @as(u5, @intCast(l)));
    }
}

fn roundTrip(input: []const u8, opts: Options) !void {
    const gpa = std.testing.allocator;
    var sink: std.Io.Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const z = try Deflate.init(gpa, &sink.writer, opts);
    defer z.deinit(gpa);
    var i: usize = 0;
    var step: usize = 1;
    while (i < input.len) : (step = step * 7 % 9973 + 1) {
        const n = @min(step, input.len - i);
        try z.interface.writeAll(input[i..][0..n]);
        i += n;
    }
    try z.finish();
    const packed_bytes = sink.written();

    var in: std.Io.Reader = .fixed(packed_bytes);
    var dbuf: [std.compress.flate.max_window_len]u8 = undefined;
    const container: std.compress.flate.Container = switch (opts.container) {
        .raw => .raw,
        .gzip => .gzip,
        .zlib => .zlib,
    };
    var dec: std.compress.flate.Decompress = .init(&in, container, &dbuf);
    var got: std.Io.Writer.Allocating = .init(gpa);
    defer got.deinit();
    _ = try dec.reader.streamRemaining(&got.writer);
    try std.testing.expectEqualSlices(u8, input, got.written());
}

fn roundTripAll(input: []const u8) !void {
    for ([_]Level{ .default, .fast }) |level| {
        for ([_]Container{ .gzip, .zlib, .raw }) |container| try roundTrip(input, .{ .level = level, .container = container });
    }
}

test "deflate: both levels round trip through std's decompressor in every container" {
    const gpa = std.testing.allocator;
    try roundTripAll("");
    try roundTripAll("a");
    try roundTripAll("abcd");
    try roundTripAll("abcabcabcabcabcabcabc");
    try roundTripAll("a" ** 1000);

    var text: std.Io.Writer.Allocating = .init(gpa);
    defer text.deinit();
    for (0..60_000) |r| try text.writer.print("{d},name_{d},{d}.{d},2026-10-{d:0>2}\n", .{ r, r % 977, r * 7 % 1000, r % 100, r % 28 + 1 });
    try text.writer.writeAll("z" ** 70_000);
    var noise: [300_000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(7);
    prng.random().bytes(&noise);
    try text.writer.writeAll(&noise);
    try roundTripAll(text.written());
}

test "deflate: matches at the window's full distance and across block shifts" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(11);
    const block = try gpa.alloc(u8, window);
    defer gpa.free(block);
    prng.random().bytes(block);
    var buf: std.Io.Writer.Allocating = .init(gpa);
    defer buf.deinit();
    for (0..3) |_| try buf.writer.writeAll(block);
    try roundTripAll(buf.written());

    buf.clearRetainingCapacity();
    const period = try gpa.alloc(u8, 30_011);
    defer gpa.free(period);
    prng.random().bytes(period);
    while (buf.written().len < 3 * (window + block_in)) try buf.writer.writeAll(period);
    try roundTripAll(buf.written());
}

test "deflate: reset starts a fresh stream that decodes on its own" {
    const gpa = std.testing.allocator;
    var sink: std.Io.Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const z = try Deflate.init(gpa, &sink.writer, .{ .level = .fast });
    defer z.deinit(gpa);
    try z.interface.writeAll("first stream, first stream, first stream\n" ** 50);
    try z.finish();
    sink.clearRetainingCapacity();
    try z.reset();
    const second = "second, second, second\n" ** 40;
    try z.interface.writeAll(second);
    try z.finish();
    var in: std.Io.Reader = .fixed(sink.written());
    var dbuf: [std.compress.flate.max_window_len]u8 = undefined;
    var dec: std.compress.flate.Decompress = .init(&in, .gzip, &dbuf);
    var got: std.Io.Writer.Allocating = .init(gpa);
    defer got.deinit();
    _ = try dec.reader.streamRemaining(&got.writer);
    try std.testing.expectEqualStrings(second, got.written());
}

test "deflate: the fast level still compresses CSV-like text" {
    const gpa = std.testing.allocator;
    var text: std.Io.Writer.Allocating = .init(gpa);
    defer text.deinit();
    for (0..20_000) |r| try text.writer.print("{d},name_{d},{d}.{d},2026-10-{d:0>2}\n", .{ r, r % 977, r * 7 % 1000, r % 100, r % 28 + 1 });
    var sink: std.Io.Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const z = try Deflate.init(gpa, &sink.writer, .{ .level = .fast });
    defer z.deinit(gpa);
    try z.interface.writeAll(text.written());
    try z.finish();
    try std.testing.expect(sink.written().len * 2 < text.written().len);
}

test "deflate: length and distance codes match RFC 1951's tables" {
    try std.testing.expectEqual(@as(u16, 257), lenCode(3).code);
    try std.testing.expectEqual(@as(u16, 265), lenCode(11).code);
    try std.testing.expectEqual(@as(u16, 266), lenCode(13).code);
    try std.testing.expectEqual(@as(u16, 284), lenCode(257).code);
    try std.testing.expectEqual(@as(u32, 30), lenCode(257).extra);
    try std.testing.expectEqual(@as(u16, 285), lenCode(258).code);
    try std.testing.expectEqual(@as(u16, 4), distCode(5).code);
    try std.testing.expectEqual(@as(u16, 5), distCode(7).code);
    try std.testing.expectEqual(@as(u16, 29), distCode(32768).code);
    try std.testing.expectEqual(@as(u32, 8191), distCode(32768).extra);
}

test "deflate: the sliced CRC-32 agrees with std's" {
    var buf: [1000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(3);
    prng.random().bytes(&buf);
    for ([_]usize{ 0, 1, 7, 8, 9, 63, 1000 }) |n| {
        try std.testing.expectEqual(std.hash.Crc32.hash(buf[0..n]), ~crc32Update(0xffff_ffff, buf[0..n]));
    }
    try std.testing.expectEqual(std.hash.Crc32.hash(buf[0..777]), ~crc32Update(crc32Update(0xffff_ffff, buf[0..13]), buf[13..777]));
}
