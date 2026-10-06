//! A gzip writer: DEFLATE (RFC 1951) in a gzip container (RFC 1952), for
//! `LOAD INTO 'x.csv.gz'`. Zig 0.15's std ships only the decompressor.
//!
//! LZ77 over a 32 KiB window — hash chains, one step of lazy matching, as
//! zlib's default level does — and a dynamic Huffman code per block of input.
//! A block that would come out larger than its input is stored instead, so an
//! incompressible stream grows by a few bytes per 64 KiB, not by a fraction.

const std = @import("std");

const window = 1 << 15;
const block_in = 1 << 17;
const min_match = 3;
const max_match = 258;
const hash_bits = 15;
const max_chain = 48;
/// A match this long is taken without looking further, or lazily past.
const nice_len = 128;
const lazy_len = 32;

const Token = packed struct(u32) {
    /// 0 for a literal; else the match length.
    len: u16,
    /// The literal byte, or the match distance.
    val: u16,
};

pub const Gzip = struct {
    out: *std.Io.Writer,
    interface: std.Io.Writer,
    /// `data[0..cur]` is history matches may reach back into; `data[cur..fill]`
    /// is input not yet compressed.
    data: []u8,
    cur: usize = 0,
    fill: usize = 0,
    head: []i32,
    prev: []i32,
    tokens: []Token,
    crc: std.hash.Crc32 = .init(),
    size: u32 = 0,
    bits: u64 = 0,
    nbits: u6 = 0,
    obuf: [8192]u8 = undefined,
    olen: usize = 0,
    done: bool = false,

    pub fn init(gpa: std.mem.Allocator, out: *std.Io.Writer) !*Gzip {
        const self = try gpa.create(Gzip);
        errdefer gpa.destroy(self);
        const data = try gpa.alloc(u8, window + block_in);
        errdefer gpa.free(data);
        const head = try gpa.alloc(i32, 1 << hash_bits);
        errdefer gpa.free(head);
        const prev = try gpa.alloc(i32, data.len);
        errdefer gpa.free(prev);
        // a block is at most the whole buffer, history included the first time
        const tokens = try gpa.alloc(Token, data.len);
        errdefer gpa.free(tokens);
        const buf = try gpa.alloc(u8, 64 * 1024);
        @memset(head, -1);
        self.* = .{
            .out = out,
            .interface = .{ .buffer = buf, .vtable = &.{ .drain = drainFn } },
            .data = data,
            .head = head,
            .prev = prev,
            .tokens = tokens,
        };
        // magic, deflate, no flags, no mtime, no extra flags, OS unknown
        try out.writeAll(&.{ 0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255 });
        return self;
    }

    pub fn deinit(self: *Gzip, gpa: std.mem.Allocator) void {
        gpa.free(self.interface.buffer);
        gpa.free(self.tokens);
        gpa.free(self.prev);
        gpa.free(self.head);
        gpa.free(self.data);
        gpa.destroy(self);
    }

    fn drainFn(w: *std.Io.Writer, chunks: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Gzip = @fieldParentPtr("interface", w);
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

    fn feed(self: *Gzip, bytes: []const u8) !void {
        self.crc.update(bytes);
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

    /// Compress what is buffered as the last block and write the gzip trailer.
    /// The underlying writer is left to its owner to flush.
    pub fn finish(self: *Gzip) !void {
        if (self.done) return;
        self.done = true;
        try self.interface.flush();
        try self.compressBlock(true);
        try self.alignByte();
        var tail: [8]u8 = undefined;
        std.mem.writeInt(u32, tail[0..4], self.crc.final(), .little);
        std.mem.writeInt(u32, tail[4..8], self.size, .little);
        try self.emitBytes(&tail);
        try self.flushOut();
    }

    // --- LZ77 ------------------------------------------------------------------

    fn hashAt(self: *const Gzip, i: usize) usize {
        const v = @as(u32, self.data[i]) | @as(u32, self.data[i + 1]) << 8 | @as(u32, self.data[i + 2]) << 16;
        return (v *% 2654435761) >> (32 - hash_bits);
    }

    fn insert(self: *Gzip, i: usize) void {
        if (i + min_match > self.fill) return;
        const h = self.hashAt(i);
        self.prev[i] = self.head[h];
        self.head[h] = @intCast(i);
    }

    const Match = struct { len: usize = 0, dist: usize = 0 };

    /// The longest match for position `p` (already inserted) along its chain.
    fn findMatch(self: *const Gzip, p: usize, end: usize, prev_len: usize) Match {
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
                var n: usize = 2;
                while (n + 8 <= max_len) {
                    const a = std.mem.readInt(u64, d[c + n ..][0..8], .little);
                    const b = std.mem.readInt(u64, d[p + n ..][0..8], .little);
                    if (a != b) {
                        n += @ctz(a ^ b) / 8;
                        break;
                    }
                    n += 8;
                } else while (n < max_len and d[c + n] == d[p + n]) n += 1;
                if (n > max_len) n = max_len;
                if (n > best.len) {
                    best = .{ .len = n, .dist = p - c };
                    if (n >= nice_len or n == max_len) break;
                }
            }
            cand = self.prev[c];
        }
        return if (best.dist == 0) .{} else best;
    }

    fn compressBlock(self: *Gzip, final: bool) !void {
        const end = self.fill;
        var nt: usize = 0;
        var p = self.cur;
        // zlib's lazy evaluation: a match found at p is emitted only if p+1 has none longer
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
        try self.writeBlock(self.tokens[0..nt], self.data[self.cur..end], final);

        // keep the last window as history
        self.cur = end;
        if (self.fill > window) {
            const shift = self.fill - window;
            std.mem.copyForwards(u8, self.data[0..window], self.data[shift..self.fill]);
            std.mem.copyForwards(i32, self.prev[0..window], self.prev[shift..self.fill]);
            const s: i32 = @intCast(shift);
            for (self.head) |*h| h.* = if (h.* >= s) h.* - s else -1;
            for (self.prev[0..window]) |*h| h.* = if (h.* >= s) h.* - s else -1;
            self.cur = window;
            self.fill = window;
        }
    }

    // --- blocks ----------------------------------------------------------------

    fn writeBlock(self: *Gzip, tokens: []const Token, raw: []const u8, final: bool) !void {
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

        // the code lengths, run-length coded (symbols 16, 17, 18)
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

        // stored when the code would not pay for itself
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

    fn writeStored(self: *Gzip, raw: []const u8, final: bool) !void {
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

    // --- bits ------------------------------------------------------------------

    /// `n` bits of `v`, least significant first, as DEFLATE packs them.
    fn put(self: *Gzip, v: u32, n: u6) !void {
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

    fn alignByte(self: *Gzip) !void {
        while (self.nbits > 0) {
            if (self.olen == self.obuf.len) try self.flushOut();
            self.obuf[self.olen] = @truncate(self.bits);
            self.olen += 1;
            self.bits >>= 8;
            self.nbits = if (self.nbits > 8) self.nbits - 8 else 0;
        }
        self.bits = 0;
    }

    /// Bytes on a byte boundary (stored data, the trailer).
    fn emitBytes(self: *Gzip, bytes: []const u8) !void {
        try self.flushOut();
        try self.out.writeAll(bytes);
    }

    fn flushOut(self: *Gzip) !void {
        if (self.olen == 0) return;
        try self.out.writeAll(self.obuf[0..self.olen]);
        self.olen = 0;
    }
};

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

/// Code-length run-length coding: 16 repeats the previous length 3–6 times,
/// 17 and 18 write 3–10 and 11–138 zeros.
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

/// Huffman code lengths for `freq`, none longer than `max_bits` — the
/// minimum-redundancy lengths (Moffat and Katajainen's in-place method), then
/// cut to the limit and the code made whole again, as miniz does. A tree of
/// fewer than two symbols gets two, as a code of one length-1 symbol is not
/// complete and some decoders refuse it.
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
    // the rarest symbols take the longest codes
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

/// Canonical codes for `lens`, bit-reversed for least-significant-first output.
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

fn roundTrip(input: []const u8) !void {
    const gpa = std.testing.allocator;
    var sink: std.Io.Writer.Allocating = .init(gpa);
    defer sink.deinit();
    const gz = try Gzip.init(gpa, &sink.writer);
    defer gz.deinit(gpa);
    // in uneven pieces, as rows arrive
    var i: usize = 0;
    var step: usize = 1;
    while (i < input.len) : (step = step * 7 % 9973 + 1) {
        const n = @min(step, input.len - i);
        try gz.interface.writeAll(input[i..][0..n]);
        i += n;
    }
    try gz.finish();
    const packed_bytes = sink.written();

    var in: std.Io.Reader = .fixed(packed_bytes);
    var dbuf: [std.compress.flate.max_window_len]u8 = undefined;
    var dec: std.compress.flate.Decompress = .init(&in, .gzip, &dbuf);
    var got: std.Io.Writer.Allocating = .init(gpa);
    defer got.deinit();
    _ = try dec.reader.streamRemaining(&got.writer);
    try std.testing.expectEqualSlices(u8, input, got.written());
}

test "gzip: round trips through std's decompressor" {
    const gpa = std.testing.allocator;
    try roundTrip("");
    try roundTrip("a");
    try roundTrip("abcabcabcabcabcabcabc");
    try roundTrip("a" ** 1000);

    // CSV-like text over several blocks, long runs, and incompressible noise
    var text: std.Io.Writer.Allocating = .init(gpa);
    defer text.deinit();
    for (0..60_000) |r| try text.writer.print("{d},name_{d},{d}.{d},2026-10-{d:0>2}\n", .{ r, r % 977, r * 7 % 1000, r % 100, r % 28 + 1 });
    try text.writer.writeAll("z" ** 70_000);
    var noise: [300_000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(7);
    prng.random().bytes(&noise);
    try text.writer.writeAll(&noise);
    try roundTrip(text.written());
}

test "gzip: length and distance codes match RFC 1951's tables" {
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
