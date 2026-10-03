//! An SSH-2 client — just enough transport for SFTP.
//!
//! Key exchange is curve25519-sha256 (RFC 8731), with OpenSSH's strict-kex
//! extension: sequence numbers restart at each NEWKEYS and the first exchange
//! admits no other message, which is what closes the Terrapin prefix-truncation
//! attack (CVE-2023-48795) on chacha20-poly1305. The server's host key is checked
//! twice — its signature over the exchange hash, and the key itself against
//! `known_hosts` or a pinned fingerprint — and an unknown or changed key is
//! refused, never trusted on first use: a file transfer to the wrong host is a
//! leak, not a hiccup.
//!
//! Ciphers: chacha20-poly1305@openssh.com and AES-GCM, and AES-CTR with
//! HMAC-SHA2 (plain or encrypt-then-MAC) for the file-transfer appliances that
//! offer nothing newer. Host keys: Ed25519, ECDSA P-256, RSA with SHA-2. Login:
//! password, keyboard-interactive, or an Ed25519 key (passphrase-protected too).
//! The server may rekey at any moment; that is handled where it is met.

const std = @import("std");
const crypto = std.crypto;
const Sha256 = crypto.hash.sha2.Sha256;
const Sha512 = crypto.hash.sha2.Sha512;
const X25519 = crypto.dh.X25519;
const Ed25519 = crypto.sign.Ed25519;
const EcdsaP256 = crypto.sign.ecdsa.EcdsaP256Sha256;
const rsa = crypto.Certificate.rsa;
const ChaCha = crypto.stream.chacha.ChaCha20With64BitNonce;
const Poly1305 = crypto.onetimeauth.Poly1305;
const Aes128Gcm = crypto.aead.aes_gcm.Aes128Gcm;
const Aes256Gcm = crypto.aead.aes_gcm.Aes256Gcm;
const HmacSha256 = crypto.auth.hmac.sha2.HmacSha256;
const HmacSha512 = crypto.auth.hmac.sha2.HmacSha512;
const HmacSha1 = crypto.auth.hmac.HmacSha1;

pub const Error = error{
    SshProtocol,
    SshDisconnected,
    SshNoCommonAlgorithm,
    SshBadMac,
    SshHostKeyUnknown,
    SshHostKeyChanged,
    SshHostKeyRevoked,
    SshBadHostSignature,
    SshAuthFailed,
    SshKeyUnsupported,
    SshKeyPassphrase,
    SshChannelRefused,
    OutOfMemory,
};

pub const Config = struct {
    host: []const u8,
    port: u16 = 22,
    user: []const u8,
    password: ?[]const u8 = null,
    /// An OpenSSH private key file (`openssh-key-v1`): Ed25519.
    key_file: ?[]const u8 = null,
    key_passphrase: ?[]const u8 = null,
    /// `known_hosts` to check the server against; `~/.ssh/known_hosts` when null.
    known_hosts: ?[]const u8 = null,
    /// A pinned host key — `SHA256:…` as the error for an unknown host prints
    /// it, or a public key line `ssh-ed25519 AAAA…` — instead of `known_hosts`.
    host_key: ?[]const u8 = null,
};

const client_version = "SSH-2.0-basalt";

/// Why the last session on this thread failed, kept past the session: a failed
/// `connect` frees it before the caller could ask.
threadlocal var failure_buf: [512]u8 = undefined;
threadlocal var failure_len: usize = 0;

pub fn lastFailure() []const u8 {
    return failure_buf[0..failure_len];
}

pub fn clearFailure() void {
    failure_len = 0;
}

// message numbers (RFC 4250 §4.1)
const msg = struct {
    const disconnect = 1;
    const ignore = 2;
    const unimplemented = 3;
    const debug = 4;
    const service_request = 5;
    const service_accept = 6;
    const ext_info = 7;
    const kexinit = 20;
    const newkeys = 21;
    const kex_ecdh_init = 30;
    const kex_ecdh_reply = 31;
    const userauth_request = 50;
    const userauth_failure = 51;
    const userauth_success = 52;
    const userauth_banner = 53;
    const userauth_info_request = 60;
    const userauth_info_response = 61;
    const global_request = 80;
    const request_failure = 82;
    const channel_open = 90;
    const channel_open_confirmation = 91;
    const channel_open_failure = 92;
    const channel_window_adjust = 93;
    const channel_data = 94;
    const channel_extended_data = 95;
    const channel_eof = 96;
    const channel_close = 97;
    const channel_request = 98;
    const channel_success = 99;
    const channel_failure = 100;
};

// --- wire encoding ----------------------------------------------------------

pub const Buf = struct {
    list: std.array_list.Managed(u8),

    pub fn init(gpa: std.mem.Allocator) Buf {
        return .{ .list = .init(gpa) };
    }
    pub fn deinit(self: *Buf) void {
        self.list.deinit();
    }
    pub fn byte(self: *Buf, b: u8) !void {
        try self.list.append(b);
    }
    pub fn u32be(self: *Buf, v: u32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .big);
        try self.list.appendSlice(&b);
    }
    pub fn u64be(self: *Buf, v: u64) !void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, v, .big);
        try self.list.appendSlice(&b);
    }
    pub fn str(self: *Buf, s: []const u8) !void {
        try self.u32be(@intCast(s.len));
        try self.list.appendSlice(s);
    }
    pub fn boolean(self: *Buf, v: bool) !void {
        try self.byte(@intFromBool(v));
    }
    /// `bytes` as a positive mpint: leading zeros dropped, a zero byte added when
    /// the top bit is set (RFC 4251 §5).
    pub fn mpint(self: *Buf, bytes: []const u8) !void {
        var b = bytes;
        while (b.len > 0 and b[0] == 0) b = b[1..];
        const pad = b.len > 0 and b[0] & 0x80 != 0;
        try self.u32be(@intCast(b.len + @intFromBool(pad)));
        if (pad) try self.byte(0);
        try self.list.appendSlice(b);
    }
};

pub const Cursor = struct {
    s: []const u8,
    i: usize = 0,

    pub fn byte(self: *Cursor) !u8 {
        if (self.i >= self.s.len) return error.SshProtocol;
        defer self.i += 1;
        return self.s[self.i];
    }
    pub fn u32be(self: *Cursor) !u32 {
        if (self.i + 4 > self.s.len) return error.SshProtocol;
        defer self.i += 4;
        return std.mem.readInt(u32, self.s[self.i..][0..4], .big);
    }
    pub fn u64be(self: *Cursor) !u64 {
        if (self.i + 8 > self.s.len) return error.SshProtocol;
        defer self.i += 8;
        return std.mem.readInt(u64, self.s[self.i..][0..8], .big);
    }
    pub fn str(self: *Cursor) ![]const u8 {
        const n = try self.u32be();
        if (n > self.s.len - self.i) return error.SshProtocol;
        defer self.i += n;
        return self.s[self.i..][0..n];
    }
    pub fn boolean(self: *Cursor) !bool {
        return (try self.byte()) != 0;
    }
    pub fn rest(self: *Cursor) []const u8 {
        return self.s[self.i..];
    }
};

/// `name` in a comma-separated name-list.
fn listHas(list: []const u8, name: []const u8) bool {
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |n| if (std.mem.eql(u8, n, name)) return true;
    return false;
}

/// The first of `ours` the server also lists — RFC 4253 §7.1's rule.
fn pick(ours: []const []const u8, theirs: []const u8) ?[]const u8 {
    for (ours) |n| if (listHas(theirs, n)) return n;
    return null;
}

// --- ciphers ----------------------------------------------------------------

const CipherKind = enum {
    none,
    chacha,
    gcm128,
    gcm256,
    ctr128,
    ctr256,

    fn name(self: CipherKind) []const u8 {
        return switch (self) {
            .none => "none",
            .chacha => "chacha20-poly1305@openssh.com",
            .gcm128 => "aes128-gcm@openssh.com",
            .gcm256 => "aes256-gcm@openssh.com",
            .ctr128 => "aes128-ctr",
            .ctr256 => "aes256-ctr",
        };
    }
    fn keyLen(self: CipherKind) usize {
        return switch (self) {
            .none => 0,
            .chacha => 64,
            .gcm128, .ctr128 => 16,
            .gcm256, .ctr256 => 32,
        };
    }
    fn ivLen(self: CipherKind) usize {
        return switch (self) {
            .none, .chacha => 0,
            .gcm128, .gcm256 => 12,
            .ctr128, .ctr256 => 16,
        };
    }
    fn block(self: CipherKind) usize {
        return switch (self) {
            .none, .chacha => 8,
            else => 16,
        };
    }
    /// The cipher authenticates the packet itself; no separate MAC is chosen.
    fn aead(self: CipherKind) bool {
        return self == .chacha or self == .gcm128 or self == .gcm256;
    }
};

const MacKind = enum {
    none,
    sha256,
    sha512,
    sha256_etm,
    sha512_etm,

    fn name(self: MacKind) []const u8 {
        return switch (self) {
            .none => "none",
            .sha256 => "hmac-sha2-256",
            .sha512 => "hmac-sha2-512",
            .sha256_etm => "hmac-sha2-256-etm@openssh.com",
            .sha512_etm => "hmac-sha2-512-etm@openssh.com",
        };
    }
    fn len(self: MacKind) usize {
        return switch (self) {
            .none => 0,
            .sha256, .sha256_etm => 32,
            .sha512, .sha512_etm => 64,
        };
    }
    fn etm(self: MacKind) bool {
        return self == .sha256_etm or self == .sha512_etm;
    }
};

const cipher_prefs = [_]CipherKind{ .chacha, .gcm256, .gcm128, .ctr256, .ctr128 };
const mac_prefs = [_]MacKind{ .sha256_etm, .sha512_etm, .sha256, .sha512 };

/// One direction's keys and counters.
const Dir = struct {
    cipher: CipherKind = .none,
    mac: MacKind = .none,
    key: [64]u8 = undefined,
    iv: [16]u8 = undefined,
    mac_key: [64]u8 = undefined,
    seq: u32 = 0,

    fn macOf(self: *const Dir, out: []u8, parts: []const []const u8) void {
        var seq: [4]u8 = undefined;
        std.mem.writeInt(u32, &seq, self.seq, .big);
        switch (self.mac) {
            .none => {},
            .sha256, .sha256_etm => {
                var h = HmacSha256.init(self.mac_key[0..32]);
                h.update(&seq);
                for (parts) |p| h.update(p);
                h.final(out[0..32]);
            },
            .sha512, .sha512_etm => {
                var h = HmacSha512.init(self.mac_key[0..64]);
                h.update(&seq);
                for (parts) |p| h.update(p);
                h.final(out[0..64]);
            },
        }
    }

    /// AES-CTR over `buf` in place, the counter carried from packet to packet.
    fn ctrXor(self: *Dir, buf: []u8) void {
        switch (self.cipher) {
            .ctr128 => ctrRun(crypto.core.aes.Aes128, self.key[0..16].*, &self.iv, buf),
            .ctr256 => ctrRun(crypto.core.aes.Aes256, self.key[0..32].*, &self.iv, buf),
            else => unreachable,
        }
    }

    /// The GCM nonce: four fixed bytes and a 64-bit invocation counter that
    /// steps once per packet (RFC 5647 §7.1).
    fn gcmNonce(self: *Dir) [12]u8 {
        const n: [12]u8 = self.iv[0..12].*;
        const ctr = std.mem.readInt(u64, self.iv[4..12], .big);
        std.mem.writeInt(u64, self.iv[4..12], ctr +% 1, .big);
        return n;
    }
};

fn ctrRun(comptime Aes: type, key: [Aes.key_bits / 8]u8, ctr: *[16]u8, buf: []u8) void {
    const ctx = Aes.initEnc(key);
    var i: usize = 0;
    while (i < buf.len) : (i += 16) {
        var ks: [16]u8 = undefined;
        ctx.encrypt(&ks, ctr);
        const n = @min(16, buf.len - i);
        for (buf[i..][0..n], ks[0..n]) |*b, k| b.* ^= k;
        // the whole 128-bit block is one big-endian counter
        var c = std.mem.readInt(u128, ctr, .big);
        c +%= 1;
        std.mem.writeInt(u128, ctr, c, .big);
    }
}

// --- the session ------------------------------------------------------------

pub const Session = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    sr: std.net.Stream.Reader,
    sw: std.net.Stream.Writer,
    /// Holds a whole packet: the reader can only `take` what fits its buffer.
    rbuf: [max_packet + 1024]u8 = undefined,
    wbuf: [64 * 1024]u8 = undefined,
    cfg: Config,
    server_version: []const u8 = "",
    session_id: [32]u8 = undefined,
    have_session_id: bool = false,
    strict: bool = false,
    tx: Dir = .{},
    rx: Dir = .{},
    /// The last payload read; valid until the next read.
    packet: std.array_list.Managed(u8),
    out: std.array_list.Managed(u8),
    /// Why the last call failed, in words — the server's disconnect message, a
    /// host key's fingerprint. Never a secret.
    why: std.array_list.Managed(u8),

    // the one channel this client opens
    chan_local: u32 = 0,
    chan_remote: u32 = 0,
    remote_window: u64 = 0,
    remote_max_packet: u32 = 0,
    local_window: u64 = 0,
    chan_open: bool = false,
    eof: bool = false,
    /// Channel data that arrived while a send waited, kept for the next `recv`.
    held: std.array_list.Managed(u8),

    const local_window_size: u32 = 2 * 1024 * 1024;
    /// OpenSSH's own channel packet size.
    const local_max_packet: u32 = 32 * 1024;

    pub fn connect(gpa: std.mem.Allocator, cfg: Config) !*Session {
        const self = try gpa.create(Session);
        errdefer gpa.destroy(self);
        const stream = std.net.tcpConnectToHost(gpa, cfg.host, cfg.port) catch |e| return e;
        self.* = .{
            .gpa = gpa,
            .stream = stream,
            .sr = undefined,
            .sw = undefined,
            .cfg = cfg,
            .packet = .init(gpa),
            .out = .init(gpa),
            .why = .init(gpa),
            .held = .init(gpa),
        };
        self.sr = stream.reader(&self.rbuf);
        self.sw = stream.writer(&self.wbuf);
        errdefer self.release();
        try self.handshake();
        try self.authenticate();
        return self;
    }

    fn release(self: *Session) void {
        self.stream.close();
        self.packet.deinit();
        self.out.deinit();
        self.why.deinit();
        self.held.deinit();
        if (self.server_version.len > 0) self.gpa.free(self.server_version);
    }

    pub fn close(self: *Session) void {
        const gpa = self.gpa;
        self.release();
        gpa.destroy(self);
    }

    pub fn lastError(self: *const Session) []const u8 {
        return self.why.items;
    }

    fn fail(self: *Session, e: Error, comptime fmt: []const u8, args: anytype) Error {
        self.why.clearRetainingCapacity();
        self.why.writer().print(fmt, args) catch {};
        const n = @min(self.why.items.len, failure_buf.len);
        @memcpy(failure_buf[0..n], self.why.items[0..n]);
        failure_len = n;
        return e;
    }

    fn reader(self: *Session) *std.Io.Reader {
        return self.sr.interface();
    }

    // --- packets ---------------------------------------------------------------

    fn sendPacket(self: *Session, payload: []const u8) !void {
        const d = &self.tx;
        const blk = d.cipher.block();
        const len_outside = d.cipher.aead() or d.mac.etm();
        const covered = 1 + payload.len + @as(usize, if (len_outside) 0 else 4);
        var pad = blk - covered % blk;
        if (pad < 4) pad += blk;
        const plen: u32 = @intCast(1 + payload.len + pad);

        const o = &self.out;
        o.clearRetainingCapacity();
        try o.ensureTotalCapacity(4 + plen + 64);
        var lb: [4]u8 = undefined;
        std.mem.writeInt(u32, &lb, plen, .big);
        o.appendSliceAssumeCapacity(&lb);
        o.appendAssumeCapacity(@intCast(pad));
        o.appendSliceAssumeCapacity(payload);
        const pad_at = o.items.len;
        o.items.len += pad;
        crypto.random.bytes(o.items[pad_at..]);
        const pkt = o.items;

        switch (d.cipher) {
            .none => {},
            .chacha => {
                var nonce: [8]u8 = undefined;
                std.mem.writeInt(u64, &nonce, d.seq, .big);
                ChaCha.xor(pkt[0..4], pkt[0..4], 0, d.key[32..64].*, nonce);
                var poly_key: [64]u8 = @splat(0);
                ChaCha.xor(&poly_key, &poly_key, 0, d.key[0..32].*, nonce);
                ChaCha.xor(pkt[4..], pkt[4..], 1, d.key[0..32].*, nonce);
                var tag: [16]u8 = undefined;
                Poly1305.create(&tag, pkt, poly_key[0..32]);
                try o.appendSlice(&tag);
            },
            .gcm128, .gcm256 => {
                const nonce = d.gcmNonce();
                var tag: [16]u8 = undefined;
                const body = pkt[4..];
                if (d.cipher == .gcm128)
                    Aes128Gcm.encrypt(body, &tag, body, pkt[0..4], nonce, d.key[0..16].*)
                else
                    Aes256Gcm.encrypt(body, &tag, body, pkt[0..4], nonce, d.key[0..32].*);
                try o.appendSlice(&tag);
            },
            .ctr128, .ctr256 => {
                var mac: [64]u8 = undefined;
                if (d.mac.etm()) {
                    d.ctrXor(o.items[4..]);
                    d.macOf(&mac, &.{o.items});
                } else {
                    d.macOf(&mac, &.{o.items});
                    d.ctrXor(o.items);
                }
                try o.appendSlice(mac[0..d.mac.len()]);
            },
        }
        d.seq +%= 1;
        self.sw.interface.writeAll(o.items) catch return error.SshDisconnected;
        self.sw.interface.flush() catch return error.SshDisconnected;
    }

    fn take(self: *Session, n: usize) ![]const u8 {
        return self.reader().take(n) catch error.SshDisconnected;
    }

    /// The next packet's payload, decrypted and verified, into `self.packet`.
    fn readPacket(self: *Session) ![]const u8 {
        const d = &self.rx;
        const p = &self.packet;
        p.clearRetainingCapacity();
        var plen: u32 = 0;
        switch (d.cipher) {
            .none => {
                plen = std.mem.readInt(u32, (try self.take(4))[0..4], .big);
                try checkLen(plen);
                try p.appendSlice(try self.take(plen));
            },
            .chacha => {
                var nonce: [8]u8 = undefined;
                std.mem.writeInt(u64, &nonce, d.seq, .big);
                var enc_len: [4]u8 = (try self.take(4))[0..4].*;
                var len_b: [4]u8 = undefined;
                ChaCha.xor(&len_b, &enc_len, 0, d.key[32..64].*, nonce);
                plen = std.mem.readInt(u32, &len_b, .big);
                try checkLen(plen);
                const body = try self.take(plen + 16);
                var poly_key: [64]u8 = @splat(0);
                ChaCha.xor(&poly_key, &poly_key, 0, d.key[0..32].*, nonce);
                var st = Poly1305.init(poly_key[0..32]);
                st.update(&enc_len);
                st.update(body[0..plen]);
                var tag: [16]u8 = undefined;
                st.final(&tag);
                if (!crypto.timing_safe.eql([16]u8, tag, body[plen..][0..16].*)) return error.SshBadMac;
                try p.resize(plen);
                ChaCha.xor(p.items, body[0..plen], 1, d.key[0..32].*, nonce);
            },
            .gcm128, .gcm256 => {
                const lb: [4]u8 = (try self.take(4))[0..4].*;
                plen = std.mem.readInt(u32, &lb, .big);
                try checkLen(plen);
                const body = try self.take(plen + 16);
                try p.resize(plen);
                const nonce = d.gcmNonce();
                const tag: [16]u8 = body[plen..][0..16].*;
                const r = if (d.cipher == .gcm128)
                    Aes128Gcm.decrypt(p.items, body[0..plen], tag, &lb, nonce, d.key[0..16].*)
                else
                    Aes256Gcm.decrypt(p.items, body[0..plen], tag, &lb, nonce, d.key[0..32].*);
                r catch return error.SshBadMac;
            },
            .ctr128, .ctr256 => {
                const ml = d.mac.len();
                var mac: [64]u8 = undefined;
                if (d.mac.etm()) {
                    const lb: [4]u8 = (try self.take(4))[0..4].*;
                    plen = std.mem.readInt(u32, &lb, .big);
                    try checkLen(plen);
                    const body = try self.take(plen + ml);
                    d.macOf(&mac, &.{ &lb, body[0..plen] });
                    if (!macEq(mac[0..ml], body[plen..][0..ml])) return error.SshBadMac;
                    try p.appendSlice(body[0..plen]);
                    d.ctrXor(p.items);
                } else {
                    var first: [16]u8 = (try self.take(16))[0..16].*;
                    d.ctrXor(&first);
                    plen = std.mem.readInt(u32, first[0..4], .big);
                    try checkLen(plen);
                    if (plen + 4 < 16) return error.SshProtocol;
                    try p.appendSlice(first[4..]);
                    const rest = try self.take(plen + 4 - 16 + ml);
                    const at = p.items.len;
                    try p.appendSlice(rest[0 .. plen + 4 - 16]);
                    d.ctrXor(p.items[at..]);
                    var lb: [4]u8 = undefined;
                    std.mem.writeInt(u32, &lb, plen, .big);
                    d.macOf(&mac, &.{ &lb, p.items });
                    if (!macEq(mac[0..ml], rest[plen + 4 - 16 ..][0..ml])) return error.SshBadMac;
                }
            },
        }
        d.seq +%= 1;
        // padding_length || payload || padding
        if (p.items.len < 1) return error.SshProtocol;
        const padn = p.items[0];
        if (padn + 1 > p.items.len) return error.SshProtocol;
        return p.items[1 .. p.items.len - padn];
    }

    // --- key exchange ----------------------------------------------------------

    fn handshake(self: *Session) !void {
        // version exchange: the server may send lines before its identification
        self.sw.interface.writeAll(client_version ++ "\r\n") catch return error.SshDisconnected;
        self.sw.interface.flush() catch return error.SshDisconnected;
        var tries: usize = 0;
        while (tries < 64) : (tries += 1) {
            // inclusive: the exclusive form leaves the `\n` unread, a byte ahead of
            // the first binary packet
            const line = self.reader().takeDelimiterInclusive('\n') catch return error.SshDisconnected;
            const l = std.mem.trimRight(u8, line, "\r\n");
            if (std.mem.startsWith(u8, l, "SSH-")) {
                if (!std.mem.startsWith(u8, l, "SSH-2.0-") and !std.mem.startsWith(u8, l, "SSH-1.99-"))
                    return self.fail(error.SshProtocol, "the server speaks {s}, not SSH-2", .{l});
                self.server_version = try self.gpa.dupe(u8, l);
                break;
            }
        } else return self.fail(error.SshProtocol, "no SSH identification from the server", .{});
        try self.kex(null);
    }

    fn kexinitPayload(self: *Session, out: *Buf) !void {
        _ = self;
        try out.byte(msg.kexinit);
        var cookie: [16]u8 = undefined;
        crypto.random.bytes(&cookie);
        try out.list.appendSlice(&cookie);
        try out.str("curve25519-sha256,curve25519-sha256@libssh.org,ext-info-c,kex-strict-c-v00@openssh.com");
        try out.str("ssh-ed25519,ecdsa-sha2-nistp256,rsa-sha2-512,rsa-sha2-256");
        var ciphers: [256]u8 = undefined;
        var cw = std.Io.Writer.fixed(&ciphers);
        for (cipher_prefs, 0..) |c, i| cw.print("{s}{s}", .{ if (i > 0) "," else "", c.name() }) catch unreachable;
        try out.str(cw.buffered());
        try out.str(cw.buffered());
        var macs: [256]u8 = undefined;
        var mw = std.Io.Writer.fixed(&macs);
        for (mac_prefs, 0..) |m, i| mw.print("{s}{s}", .{ if (i > 0) "," else "", m.name() }) catch unreachable;
        try out.str(mw.buffered());
        try out.str(mw.buffered());
        try out.str("none");
        try out.str("none");
        try out.str("");
        try out.str("");
        try out.boolean(false);
        try out.u32be(0);
    }

    /// A key exchange: the first, or a rekey the server started with the
    /// KEXINIT it sent (`their`).
    fn kex(self: *Session, their: ?[]const u8) !void {
        const first = !self.have_session_id;
        var ic = Buf.init(self.gpa);
        defer ic.deinit();
        try self.kexinitPayload(&ic);
        try self.sendPacket(ic.list.items);

        var is_owned = std.array_list.Managed(u8).init(self.gpa);
        defer is_owned.deinit();
        if (their) |t| {
            try is_owned.appendSlice(t);
        } else {
            const p = try self.nextKex(first);
            if (p.len == 0 or p[0] != msg.kexinit) return self.fail(error.SshProtocol, "expected the server's KEXINIT", .{});
            try is_owned.appendSlice(p);
        }
        const is = is_owned.items;

        var c = Cursor{ .s = is[17..] };
        const kex_algs = try c.str();
        const hostkey_algs = try c.str();
        const enc_cs = try c.str();
        const enc_sc = try c.str();
        const mac_cs = try c.str();
        const mac_sc = try c.str();
        const comp_cs = try c.str();
        const comp_sc = try c.str();
        if (first and listHas(kex_algs, "kex-strict-s-v00@openssh.com")) self.strict = true;
        _ = pick(&.{ "curve25519-sha256", "curve25519-sha256@libssh.org" }, kex_algs) orelse
            return self.fail(error.SshNoCommonAlgorithm, "the server offers no curve25519 key exchange (it offers {s})", .{kex_algs});
        const hk_alg = pick(&.{ "ssh-ed25519", "ecdsa-sha2-nistp256", "rsa-sha2-512", "rsa-sha2-256" }, hostkey_algs) orelse
            return self.fail(error.SshNoCommonAlgorithm, "no host key algorithm in common (the server offers {s})", .{hostkey_algs});
        if (!listHas(comp_cs, "none") or !listHas(comp_sc, "none")) return self.fail(error.SshNoCommonAlgorithm, "the server requires compression", .{});
        var next_tx = Dir{};
        var next_rx = Dir{};
        try self.chooseCiphers(&next_tx, enc_cs, mac_cs);
        try self.chooseCiphers(&next_rx, enc_sc, mac_sc);

        // ECDH
        const kp = X25519.KeyPair.generate();
        var ecdh = Buf.init(self.gpa);
        defer ecdh.deinit();
        try ecdh.byte(msg.kex_ecdh_init);
        try ecdh.str(&kp.public_key);
        try self.sendPacket(ecdh.list.items);

        const reply = try self.nextKex(first);
        if (reply.len == 0 or reply[0] != msg.kex_ecdh_reply) return self.fail(error.SshProtocol, "expected KEX_ECDH_REPLY", .{});
        var rc = Cursor{ .s = reply[1..] };
        const k_s = try self.gpa.dupe(u8, try rc.str());
        defer self.gpa.free(k_s);
        const q_s = try rc.str();
        if (q_s.len != 32) return error.SshProtocol;
        const sig = try self.gpa.dupe(u8, try rc.str());
        defer self.gpa.free(sig);
        const shared = X25519.scalarmult(kp.secret_key, q_s[0..32].*) catch return error.SshProtocol;

        var hb = Buf.init(self.gpa);
        defer hb.deinit();
        try hb.str(client_version);
        try hb.str(self.server_version);
        try hb.str(ic.list.items);
        try hb.str(is);
        try hb.str(k_s);
        try hb.str(&kp.public_key);
        try hb.str(q_s);
        try hb.mpint(&shared);
        var h: [32]u8 = undefined;
        Sha256.hash(hb.list.items, &h, .{});

        try self.checkHostKey(k_s, hk_alg);
        try verifySignature(self, k_s, hk_alg, sig, &h);
        if (first) {
            self.session_id = h;
            self.have_session_id = true;
        }

        var kmp = Buf.init(self.gpa);
        defer kmp.deinit();
        try kmp.mpint(&shared);
        self.deriveKey(kmp.list.items, &h, 'A', next_tx.iv[0..next_tx.cipher.ivLen()]);
        self.deriveKey(kmp.list.items, &h, 'B', next_rx.iv[0..next_rx.cipher.ivLen()]);
        self.deriveKey(kmp.list.items, &h, 'C', next_tx.key[0..next_tx.cipher.keyLen()]);
        self.deriveKey(kmp.list.items, &h, 'D', next_rx.key[0..next_rx.cipher.keyLen()]);
        self.deriveKey(kmp.list.items, &h, 'E', next_tx.mac_key[0..macKeyLen(next_tx.mac)]);
        self.deriveKey(kmp.list.items, &h, 'F', next_rx.mac_key[0..macKeyLen(next_rx.mac)]);

        try self.sendPacket(&.{msg.newkeys});
        next_tx.seq = if (self.strict) 0 else self.tx.seq;
        self.tx = next_tx;
        const nk = try self.nextKex(first);
        if (nk.len == 0 or nk[0] != msg.newkeys) return self.fail(error.SshProtocol, "expected NEWKEYS", .{});
        next_rx.seq = if (self.strict) 0 else self.rx.seq;
        self.rx = next_rx;
    }

    fn macKeyLen(m: MacKind) usize {
        return switch (m) {
            .none => 0,
            .sha256, .sha256_etm => 32,
            .sha512, .sha512_etm => 64,
        };
    }

    fn chooseCiphers(self: *Session, d: *Dir, enc: []const u8, macs: []const u8) !void {
        for (cipher_prefs) |c| {
            if (listHas(enc, c.name())) {
                d.cipher = c;
                break;
            }
        } else return self.fail(error.SshNoCommonAlgorithm, "no cipher in common (the server offers {s})", .{enc});
        if (d.cipher.aead()) return;
        for (mac_prefs) |m| if (listHas(macs, m.name())) {
            d.mac = m;
            return;
        };
        return self.fail(error.SshNoCommonAlgorithm, "no MAC in common (the server offers {s})", .{macs});
    }

    /// RFC 4253 §7.2: HASH(K || H || X || session_id), extended by
    /// HASH(K || H || key so far) until long enough.
    fn deriveKey(self: *Session, k: []const u8, h: *const [32]u8, x: u8, out: []u8) void {
        if (out.len == 0) return;
        var acc: [128]u8 = undefined;
        var have: usize = 0;
        var s = Sha256.init(.{});
        s.update(k);
        s.update(h);
        s.update(&.{x});
        s.update(if (self.have_session_id) &self.session_id else h);
        s.final(acc[0..32]);
        have = 32;
        while (have < out.len) {
            var t = Sha256.init(.{});
            t.update(k);
            t.update(h);
            t.update(acc[0..have]);
            t.final(acc[have..][0..32]);
            have += 32;
        }
        @memcpy(out, acc[0..out.len]);
    }

    /// The next packet during a key exchange. In a strict first exchange any
    /// other message is fatal; otherwise the ignorable ones are skipped.
    fn nextKex(self: *Session, first: bool) ![]const u8 {
        while (true) {
            const p = try self.readPacket();
            if (p.len == 0) return error.SshProtocol;
            switch (p[0]) {
                msg.kexinit, msg.kex_ecdh_reply, msg.newkeys => return p,
                msg.disconnect => return self.disconnected(p),
                msg.ignore, msg.debug, msg.unimplemented => if (first and self.strict)
                    return self.fail(error.SshProtocol, "message {d} during a strict key exchange", .{p[0]}),
                else => return self.fail(error.SshProtocol, "unexpected message {d} during key exchange", .{p[0]}),
            }
        }
    }

    fn disconnected(self: *Session, p: []const u8) Error {
        var c = Cursor{ .s = p[1..] };
        _ = c.u32be() catch {};
        const why = c.str() catch "";
        return self.fail(error.SshDisconnected, "the server disconnected: {s}", .{why});
    }

    /// The next connection-level message: ignorable ones skipped, a rekey run
    /// where it is met, a global request refused.
    fn next(self: *Session) ![]const u8 {
        while (true) {
            const p = try self.readPacket();
            if (p.len == 0) return error.SshProtocol;
            switch (p[0]) {
                msg.ignore, msg.debug, msg.unimplemented, msg.ext_info, msg.userauth_banner => continue,
                msg.disconnect => return self.disconnected(p),
                msg.kexinit => {
                    const theirs = try self.gpa.dupe(u8, p);
                    defer self.gpa.free(theirs);
                    try self.kex(theirs);
                },
                msg.global_request => {
                    var c = Cursor{ .s = p[1..] };
                    _ = try c.str();
                    if (try c.boolean()) try self.sendPacket(&.{msg.request_failure});
                },
                else => return p,
            }
        }
    }

    // --- host keys ---------------------------------------------------------------

    fn checkHostKey(self: *Session, blob: []const u8, alg: []const u8) !void {
        var fp_buf: [64]u8 = undefined;
        const fp = fingerprint(blob, &fp_buf);
        const key_type = keyTypeOfAlg(alg);
        const pin_hint = "pin it with host_key";
        if (self.cfg.host_key) |pin| {
            const p = std.mem.trim(u8, pin, " \t");
            if (std.mem.startsWith(u8, p, "SHA256:")) {
                if (std.mem.eql(u8, p, fp)) return;
            } else if (keyLineMatches(self.gpa, p, blob)) return;
            return self.fail(error.SshHostKeyChanged, "the host key of {s} is {s} {s}, not the pinned one — someone may be intercepting the connection, or the key was replaced", .{ self.cfg.host, key_type, fp });
        }
        const path = self.cfg.known_hosts orelse blk: {
            const home = std.posix.getenv("HOME") orelse break :blk null;
            break :blk std.fmt.allocPrint(self.gpa, "{s}/.ssh/known_hosts", .{home}) catch return error.OutOfMemory;
        };
        defer if (self.cfg.known_hosts == null) if (path) |p| self.gpa.free(p);
        const text: []const u8 = if (path) |p|
            std.fs.cwd().readFileAlloc(self.gpa, p, 16 << 20) catch ""
        else
            "";
        defer if (text.len > 0) self.gpa.free(text);
        switch (try knownHostsVerdict(self.gpa, text, self.cfg.host, self.cfg.port, key_type, blob)) {
            .ok => return,
            .revoked => return self.fail(error.SshHostKeyRevoked, "the host key of {s} ({s} {s}) is marked @revoked in known_hosts", .{ self.cfg.host, key_type, fp }),
            .changed => return self.fail(error.SshHostKeyChanged, "the host key of {s} changed: it is now {s} {s}, not the one known_hosts has — someone may be intercepting the connection, or the key was replaced", .{ self.cfg.host, key_type, fp }),
            .unknown => return self.fail(error.SshHostKeyUnknown, "the host key of {s} is not in known_hosts: {s} {s} — check it with the server's owner, then add it to known_hosts or {s} = '{s}'", .{ self.cfg.host, key_type, fp, pin_hint, fp }),
        }
    }

    // --- authentication ----------------------------------------------------------

    fn authenticate(self: *Session) !void {
        var b = Buf.init(self.gpa);
        defer b.deinit();
        try b.byte(msg.service_request);
        try b.str("ssh-userauth");
        try self.sendPacket(b.list.items);
        const acc = try self.next();
        if (acc[0] != msg.service_accept) return self.fail(error.SshProtocol, "the server refused the userauth service", .{});

        if (self.cfg.key_file) |kf| {
            if (try self.tryKey(kf)) return;
        }
        if (self.cfg.password) |pw| {
            if (try self.tryPassword(pw)) return;
            if (try self.tryKeyboard(pw)) return;
        }
        if (self.cfg.key_file == null and self.cfg.password == null)
            return self.fail(error.SshAuthFailed, "no password or key_file to log in to {s} as {s}", .{ self.cfg.host, self.cfg.user });
        return self.fail(error.SshAuthFailed, "the server refused the login for {s}@{s}", .{ self.cfg.user, self.cfg.host });
    }

    /// The answer to a userauth request: true on success, false on failure.
    fn authReply(self: *Session) !bool {
        while (true) {
            const p = try self.next();
            switch (p[0]) {
                msg.userauth_success => return true,
                msg.userauth_failure => return false,
                else => return p[0] == msg.userauth_success,
            }
        }
    }

    fn authHeader(self: *Session, b: *Buf, method: []const u8) !void {
        try b.byte(msg.userauth_request);
        try b.str(self.cfg.user);
        try b.str("ssh-connection");
        try b.str(method);
    }

    fn tryPassword(self: *Session, pw: []const u8) !bool {
        var b = Buf.init(self.gpa);
        defer {
            crypto.secureZero(u8, b.list.items);
            b.deinit();
        }
        try self.authHeader(&b, "password");
        try b.boolean(false);
        try b.str(pw);
        try self.sendPacket(b.list.items);
        return self.authReply();
    }

    /// RFC 4256: every prompt answered with the password, a prompt-less round
    /// answered with nothing.
    fn tryKeyboard(self: *Session, pw: []const u8) !bool {
        var b = Buf.init(self.gpa);
        defer b.deinit();
        try self.authHeader(&b, "keyboard-interactive");
        try b.str("");
        try b.str("");
        try self.sendPacket(b.list.items);
        var rounds: usize = 0;
        while (rounds < 8) : (rounds += 1) {
            const p = try self.next();
            switch (p[0]) {
                msg.userauth_success => return true,
                msg.userauth_failure => return false,
                msg.userauth_info_request => {
                    var c = Cursor{ .s = p[1..] };
                    _ = try c.str();
                    _ = try c.str();
                    _ = try c.str();
                    const n = try c.u32be();
                    if (n > 16) return error.SshProtocol;
                    var r = Buf.init(self.gpa);
                    defer {
                        crypto.secureZero(u8, r.list.items);
                        r.deinit();
                    }
                    try r.byte(msg.userauth_info_response);
                    try r.u32be(n);
                    for (0..n) |_| try r.str(pw);
                    try self.sendPacket(r.list.items);
                },
                else => return error.SshProtocol,
            }
        }
        return false;
    }

    fn tryKey(self: *Session, path: []const u8) !bool {
        const text = std.fs.cwd().readFileAlloc(self.gpa, path, 1 << 20) catch |e|
            return self.fail(error.SshKeyUnsupported, "cannot read key_file {s} ({s})", .{ path, @errorName(e) });
        defer {
            crypto.secureZero(u8, text);
            self.gpa.free(text);
        }
        var key = parsePrivateKey(self.gpa, text, self.cfg.key_passphrase) catch |e| switch (e) {
            error.SshKeyPassphrase => return self.fail(error.SshKeyPassphrase, "key_file {s} is protected by a passphrase; give key_passphrase (or the right one)", .{path}),
            error.SshKeyUnsupported => return self.fail(error.SshKeyUnsupported, "key_file {s} is not an OpenSSH Ed25519 key; use one (ssh-keygen -t ed25519) or a password", .{path}),
            else => return e,
        };
        defer crypto.secureZero(u8, &key.secret);

        var pubblob = Buf.init(self.gpa);
        defer pubblob.deinit();
        try pubblob.str("ssh-ed25519");
        try pubblob.str(&key.public);

        var signed = Buf.init(self.gpa);
        defer signed.deinit();
        try signed.str(&self.session_id);
        try self.authHeader(&signed, "publickey");
        try signed.boolean(true);
        try signed.str("ssh-ed25519");
        try signed.str(pubblob.list.items);
        const kp = Ed25519.KeyPair.fromSecretKey(Ed25519.SecretKey.fromBytes(key.secret) catch return error.SshKeyUnsupported) catch return error.SshKeyUnsupported;
        const s = kp.sign(signed.list.items, null) catch return error.SshKeyUnsupported;

        var b = Buf.init(self.gpa);
        defer b.deinit();
        try self.authHeader(&b, "publickey");
        try b.boolean(true);
        try b.str("ssh-ed25519");
        try b.str(pubblob.list.items);
        var sigblob = Buf.init(self.gpa);
        defer sigblob.deinit();
        try sigblob.str("ssh-ed25519");
        try sigblob.str(&s.toBytes());
        try b.str(sigblob.list.items);
        try self.sendPacket(b.list.items);
        return self.authReply();
    }

    // --- the channel ---------------------------------------------------------------

    /// Open a session channel and start `name` on it (`sftp`).
    pub fn openSubsystem(self: *Session, name: []const u8) !void {
        var b = Buf.init(self.gpa);
        defer b.deinit();
        try b.byte(msg.channel_open);
        try b.str("session");
        try b.u32be(self.chan_local);
        try b.u32be(local_window_size);
        try b.u32be(local_max_packet);
        try self.sendPacket(b.list.items);
        while (true) {
            const p = try self.next();
            var c = Cursor{ .s = p[1..] };
            switch (p[0]) {
                msg.channel_open_confirmation => {
                    _ = try c.u32be();
                    self.chan_remote = try c.u32be();
                    self.remote_window = try c.u32be();
                    self.remote_max_packet = try c.u32be();
                    self.local_window = local_window_size;
                    self.chan_open = true;
                    break;
                },
                msg.channel_open_failure => return self.fail(error.SshChannelRefused, "the server refused a session channel", .{}),
                else => return error.SshProtocol,
            }
        }
        b.list.clearRetainingCapacity();
        try b.byte(msg.channel_request);
        try b.u32be(self.chan_remote);
        try b.str("subsystem");
        try b.boolean(true);
        try b.str(name);
        try self.sendPacket(b.list.items);
        while (true) {
            const p = try self.next();
            switch (p[0]) {
                msg.channel_success => return,
                msg.channel_failure => return self.fail(error.SshChannelRefused, "the server has no {s} subsystem", .{name}),
                msg.channel_window_adjust => try self.onWindow(p),
                else => return error.SshProtocol,
            }
        }
    }

    fn onWindow(self: *Session, p: []const u8) !void {
        var c = Cursor{ .s = p[1..] };
        _ = try c.u32be();
        self.remote_window += try c.u32be();
    }

    /// Send `data` on the channel, cut to the peer's packet size and waiting on
    /// its window.
    pub fn send(self: *Session, data: []const u8) !void {
        var rest = data;
        var b = Buf.init(self.gpa);
        defer b.deinit();
        while (rest.len > 0) {
            while (self.remote_window == 0) {
                const p = try self.next();
                switch (p[0]) {
                    msg.channel_window_adjust => try self.onWindow(p),
                    msg.channel_eof, msg.channel_close => return self.fail(error.SshDisconnected, "the server closed the channel", .{}),
                    // data arriving while we wait is held for `recv`
                    msg.channel_data => try self.stash(p),
                    else => {},
                }
            }
            const n: usize = @intCast(@min(@as(u64, rest.len), @min(self.remote_window, self.remote_max_packet - 64)));
            b.list.clearRetainingCapacity();
            try b.byte(msg.channel_data);
            try b.u32be(self.chan_remote);
            try b.str(rest[0..n]);
            try self.sendPacket(b.list.items);
            self.remote_window -= n;
            rest = rest[n..];
        }
    }

    fn stash(self: *Session, p: []const u8) !void {
        var c = Cursor{ .s = p[1..] };
        _ = try c.u32be();
        const d = try c.str();
        try self.held.appendSlice(d);
        try self.consumed(d.len);
    }

    /// Append the next channel data to `out`, at least one byte; error at EOF.
    pub fn recv(self: *Session, out: *std.array_list.Managed(u8)) !void {
        if (self.held.items.len > 0) {
            try out.appendSlice(self.held.items);
            self.held.clearRetainingCapacity();
            return;
        }
        while (true) {
            const p = try self.next();
            var c = Cursor{ .s = p[1..] };
            switch (p[0]) {
                msg.channel_data => {
                    _ = try c.u32be();
                    const d = try c.str();
                    try out.appendSlice(d);
                    try self.consumed(d.len);
                    if (d.len > 0) return;
                },
                msg.channel_extended_data => {
                    _ = try c.u32be();
                    _ = try c.u32be();
                    try self.consumed((try c.str()).len);
                },
                msg.channel_window_adjust => try self.onWindow(p),
                msg.channel_eof, msg.channel_close => {
                    self.eof = true;
                    return self.fail(error.SshDisconnected, "the server closed the channel", .{});
                },
                msg.channel_request => {
                    // exit-status and the like: answer a want-reply with failure
                    _ = try c.u32be();
                    _ = try c.str();
                    if (try c.boolean()) {
                        var b = Buf.init(self.gpa);
                        defer b.deinit();
                        try b.byte(msg.channel_failure);
                        try b.u32be(self.chan_remote);
                        try self.sendPacket(b.list.items);
                    }
                },
                else => {},
            }
        }
    }

    /// Give back window once half of it is used.
    fn consumed(self: *Session, n: usize) !void {
        self.local_window -|= n;
        if (self.local_window < local_window_size / 2) {
            const add: u32 = @intCast(local_window_size - self.local_window);
            var b = Buf.init(self.gpa);
            defer b.deinit();
            try b.byte(msg.channel_window_adjust);
            try b.u32be(self.chan_remote);
            try b.u32be(add);
            try self.sendPacket(b.list.items);
            self.local_window += add;
        }
    }
};

/// The largest packet read, past which a length is refused rather than
/// buffered. RFC 4253 §6.1 asks for 35000 at least; channel data comes in at
/// most `local_max_packet`.
const max_packet = 256 * 1024;

fn checkLen(n: u32) !void {
    if (n < 5 or n > max_packet) return error.SshProtocol;
}

fn macEq(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var d: u8 = 0;
    for (a, b) |x, y| d |= x ^ y;
    return d == 0;
}

/// `ssh-rsa` for an `rsa-sha2-*` signature algorithm; the key type otherwise.
fn keyTypeOfAlg(alg: []const u8) []const u8 {
    if (std.mem.startsWith(u8, alg, "rsa-sha2-")) return "ssh-rsa";
    return alg;
}

/// `SHA256:` and the unpadded base64 of the key blob's SHA-256, as OpenSSH
/// prints it.
pub fn fingerprint(blob: []const u8, out: *[64]u8) []const u8 {
    var h: [32]u8 = undefined;
    Sha256.hash(blob, &h, .{});
    @memcpy(out[0..7], "SHA256:");
    const enc = std.base64.standard_no_pad.Encoder;
    return out[0 .. 7 + enc.encode(out[7..], &h).len];
}

fn keyLineMatches(gpa: std.mem.Allocator, line: []const u8, blob: []const u8) bool {
    var it = std.mem.tokenizeAny(u8, line, " \t");
    _ = it.next() orelse return false;
    const b64 = it.next() orelse return false;
    const dec = std.base64.standard.Decoder;
    const n = dec.calcSizeForSlice(b64) catch return false;
    const buf = gpa.alloc(u8, n) catch return false;
    defer gpa.free(buf);
    dec.decode(buf, b64) catch return false;
    return std.mem.eql(u8, buf, blob);
}

const Verdict = enum { ok, unknown, changed, revoked };

/// What `known_hosts` says of `blob` for `host`:`port`. Patterns are `host`,
/// `[host]:port` off port 22, comma lists, `*`/`?` wildcards, `!negations`, and
/// hashed `|1|salt|hash` (HMAC-SHA1 of the name). `@revoked` refuses the key;
/// `@cert-authority` is not supported and skipped. Only entries of the
/// negotiated key type count, as OpenSSH counts them.
pub fn knownHostsVerdict(gpa: std.mem.Allocator, text: []const u8, host: []const u8, port: u16, key_type: []const u8, blob: []const u8) !Verdict {
    var name_buf: [300]u8 = undefined;
    const name = if (port == 22) host else (std.fmt.bufPrint(&name_buf, "[{s}]:{d}", .{ host, port }) catch return .unknown);
    var seen_type = false;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        const line = std.mem.trim(u8, raw, " \t\r");
        if (line.len == 0 or line[0] == '#') continue;
        var it = std.mem.tokenizeAny(u8, line, " \t");
        var first = it.next() orelse continue;
        var marker: enum { none, revoked, ca } = .none;
        if (first[0] == '@') {
            marker = if (std.mem.eql(u8, first, "@revoked")) .revoked else .ca;
            first = it.next() orelse continue;
        }
        if (marker == .ca) continue;
        const ktype = it.next() orelse continue;
        const b64 = it.next() orelse continue;
        if (!hostsMatch(first, name)) continue;
        const dec = std.base64.standard.Decoder;
        const n = dec.calcSizeForSlice(b64) catch continue;
        const kb = try gpa.alloc(u8, n);
        defer gpa.free(kb);
        dec.decode(kb, b64) catch continue;
        const same = std.mem.eql(u8, kb, blob);
        if (marker == .revoked) {
            if (same) return .revoked;
            continue;
        }
        if (!std.mem.eql(u8, ktype, key_type)) continue;
        if (same) return .ok;
        seen_type = true;
    }
    return if (seen_type) .changed else .unknown;
}

fn hostsMatch(field: []const u8, name: []const u8) bool {
    if (std.mem.startsWith(u8, field, "|1|")) return hashedMatch(field, name);
    var matched = false;
    var it = std.mem.splitScalar(u8, field, ',');
    while (it.next()) |pat| {
        if (pat.len > 0 and pat[0] == '!') {
            if (glob(pat[1..], name)) return false;
        } else if (glob(pat, name)) matched = true;
    }
    return matched;
}

fn hashedMatch(field: []const u8, name: []const u8) bool {
    var it = std.mem.splitScalar(u8, field[3..], '|');
    const salt64 = it.next() orelse return false;
    const hash64 = it.next() orelse return false;
    const dec = std.base64.standard.Decoder;
    var salt: [64]u8 = undefined;
    var want: [64]u8 = undefined;
    const sl = dec.calcSizeForSlice(salt64) catch return false;
    const hl = dec.calcSizeForSlice(hash64) catch return false;
    if (sl > 64 or hl != HmacSha1.mac_length) return false;
    dec.decode(salt[0..sl], salt64) catch return false;
    dec.decode(want[0..hl], hash64) catch return false;
    var got: [HmacSha1.mac_length]u8 = undefined;
    HmacSha1.create(&got, name, salt[0..sl]);
    return std.mem.eql(u8, &got, want[0..hl]);
}

/// `*` and `?` over a host name, case-insensitively.
fn glob(pat: []const u8, s: []const u8) bool {
    if (pat.len == 0) return s.len == 0;
    if (pat[0] == '*') {
        var i: usize = 0;
        while (i <= s.len) : (i += 1) if (glob(pat[1..], s[i..])) return true;
        return false;
    }
    if (s.len == 0) return false;
    if (pat[0] != '?' and std.ascii.toLower(pat[0]) != std.ascii.toLower(s[0])) return false;
    return glob(pat[1..], s[1..]);
}

/// Check `sig` over the exchange hash `h` with host key `blob`.
fn verifySignature(self: *Session, blob: []const u8, alg: []const u8, sig: []const u8, h: *const [32]u8) !void {
    var kc = Cursor{ .s = blob };
    const ktype = try kc.str();
    var sc = Cursor{ .s = sig };
    const salg = try sc.str();
    const sbytes = try sc.str();
    if (!std.mem.eql(u8, salg, alg)) return self.fail(error.SshBadHostSignature, "the server signed with {s}, not the agreed {s}", .{ salg, alg });
    const bad = error.SshBadHostSignature;
    if (std.mem.eql(u8, ktype, "ssh-ed25519")) {
        const pk = try kc.str();
        if (pk.len != 32 or sbytes.len != 64) return bad;
        const pub_key = Ed25519.PublicKey.fromBytes(pk[0..32].*) catch return bad;
        const s = Ed25519.Signature.fromBytes(sbytes[0..64].*);
        s.verify(h, pub_key) catch return self.fail(bad, "the server's Ed25519 signature does not verify", .{});
        return;
    }
    if (std.mem.eql(u8, ktype, "ecdsa-sha2-nistp256")) {
        _ = try kc.str();
        const q = try kc.str();
        const pub_key = EcdsaP256.PublicKey.fromSec1(q) catch return bad;
        var rs = Cursor{ .s = sbytes };
        const r = try rs.str();
        const s = try rs.str();
        var raw: [64]u8 = @splat(0);
        if (!fitInt(r, raw[0..32]) or !fitInt(s, raw[32..64])) return bad;
        const sg = EcdsaP256.Signature.fromBytes(raw);
        sg.verify(h, pub_key) catch return self.fail(bad, "the server's ECDSA signature does not verify", .{});
        return;
    }
    if (std.mem.eql(u8, ktype, "ssh-rsa")) {
        const e = try kc.str();
        const n = try kc.str();
        var nb = n;
        while (nb.len > 0 and nb[0] == 0) nb = nb[1..];
        const pk = rsa.PublicKey.fromBytes(e, nb) catch return bad;
        const ok = switch (nb.len) {
            inline 128, 256, 384, 512 => |ml| blk: {
                if (sbytes.len != ml) break :blk false;
                if (std.mem.eql(u8, alg, "rsa-sha2-512")) {
                    rsa.PKCS1v1_5Signature.verify(ml, sbytes[0..ml].*, h, pk, Sha512) catch break :blk false;
                } else {
                    rsa.PKCS1v1_5Signature.verify(ml, sbytes[0..ml].*, h, pk, Sha256) catch break :blk false;
                }
                break :blk true;
            },
            else => return self.fail(bad, "an RSA host key of {d} bits is not supported", .{nb.len * 8}),
        };
        if (!ok) return self.fail(bad, "the server's RSA signature does not verify", .{});
        return;
    }
    return self.fail(bad, "host key type {s} is not supported", .{ktype});
}

/// An mpint's magnitude right-aligned in `out`; false when it does not fit.
fn fitInt(m: []const u8, out: []u8) bool {
    var b = m;
    while (b.len > 0 and b[0] == 0) b = b[1..];
    if (b.len > out.len) return false;
    @memcpy(out[out.len - b.len ..], b);
    return true;
}

// --- private keys -------------------------------------------------------------

const PrivateKey = struct { public: [32]u8, secret: [64]u8 };

/// An `openssh-key-v1` Ed25519 private key (OpenSSH's PROTOCOL.key), plain or
/// encrypted with aes256-ctr under bcrypt-pbkdf as `ssh-keygen` writes it.
fn parsePrivateKey(gpa: std.mem.Allocator, text: []const u8, passphrase: ?[]const u8) !PrivateKey {
    const begin = "-----BEGIN OPENSSH PRIVATE KEY-----";
    const end = "-----END OPENSSH PRIVATE KEY-----";
    const b = std.mem.indexOf(u8, text, begin) orelse return error.SshKeyUnsupported;
    const e = std.mem.indexOfPos(u8, text, b, end) orelse return error.SshKeyUnsupported;
    var b64 = std.array_list.Managed(u8).init(gpa);
    defer b64.deinit();
    for (text[b + begin.len .. e]) |c| if (!std.ascii.isWhitespace(c)) try b64.append(c);
    const dec = std.base64.standard.Decoder;
    const raw = try gpa.alloc(u8, dec.calcSizeForSlice(b64.items) catch return error.SshKeyUnsupported);
    defer {
        crypto.secureZero(u8, raw);
        gpa.free(raw);
    }
    dec.decode(raw, b64.items) catch return error.SshKeyUnsupported;
    const magic = "openssh-key-v1\x00";
    if (!std.mem.startsWith(u8, raw, magic)) return error.SshKeyUnsupported;
    var c = Cursor{ .s = raw[magic.len..] };
    const cipher = try c.str();
    const kdf = try c.str();
    const kdf_opts = try c.str();
    if (try c.u32be() != 1) return error.SshKeyUnsupported;
    _ = try c.str(); // public key
    const sec_in = try c.str();
    const sec = try gpa.dupe(u8, sec_in);
    defer {
        crypto.secureZero(u8, sec);
        gpa.free(sec);
    }
    if (!std.mem.eql(u8, cipher, "none")) {
        if (!std.mem.eql(u8, cipher, "aes256-ctr") or !std.mem.eql(u8, kdf, "bcrypt")) return error.SshKeyUnsupported;
        const pass = passphrase orelse return error.SshKeyPassphrase;
        var oc = Cursor{ .s = kdf_opts };
        const salt = try oc.str();
        const rounds = try oc.u32be();
        var km: [48]u8 = undefined;
        defer crypto.secureZero(u8, &km);
        crypto.pwhash.bcrypt.opensshKdf(pass, salt, &km, rounds) catch return error.SshKeyUnsupported;
        var iv: [16]u8 = km[32..48].*;
        ctrRun(crypto.core.aes.Aes256, km[0..32].*, &iv, sec);
    }
    var pc = Cursor{ .s = sec };
    const c1 = try pc.u32be();
    const c2 = try pc.u32be();
    // the check words match only when the passphrase was right
    if (c1 != c2) return error.SshKeyPassphrase;
    const ktype = try pc.str();
    if (!std.mem.eql(u8, ktype, "ssh-ed25519")) return error.SshKeyUnsupported;
    const pk = try pc.str();
    const sk = try pc.str();
    if (pk.len != 32 or sk.len != 64) return error.SshKeyUnsupported;
    return .{ .public = pk[0..32].*, .secret = sk[0..64].* };
}

test "wire: mpint sign byte, name-list choice" {
    var b = Buf.init(std.testing.allocator);
    defer b.deinit();
    try b.mpint(&.{ 0x00, 0x00, 0x80, 0x01 });
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 3, 0, 0x80, 0x01 }, b.list.items);
    b.list.clearRetainingCapacity();
    try b.mpint(&.{ 0x00, 0x7f });
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 1, 0x7f }, b.list.items);
    try std.testing.expectEqualStrings("b", pick(&.{ "a", "b", "c" }, "c,b").?);
    try std.testing.expect(pick(&.{"a"}, "b,c") == null);
}

test "known_hosts: plain, bracketed port, lists, wildcards, hashed, revoked, changed" {
    const a = std.testing.allocator;
    const blob = "KEYBLOB";
    const other = "OTHERKEY";
    const enc = std.base64.standard.Encoder;
    var kb: [32]u8 = undefined;
    var ob: [32]u8 = undefined;
    const k64 = enc.encode(&kb, blob);
    const o64 = enc.encode(&ob, other);
    var buf: [1024]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "# comment\nsftp.example.com,10.0.0.5 ssh-ed25519 {s}\n[files.example.com]:2222 ssh-ed25519 {s}\n*.bank.example ssh-ed25519 {s}\n@revoked bad.example ssh-ed25519 {s}\nold.example ssh-ed25519 {s}\n", .{ k64, k64, k64, k64, o64 });
    try std.testing.expectEqual(Verdict.ok, try knownHostsVerdict(a, text, "10.0.0.5", 22, "ssh-ed25519", blob));
    try std.testing.expectEqual(Verdict.ok, try knownHostsVerdict(a, text, "files.example.com", 2222, "ssh-ed25519", blob));
    try std.testing.expectEqual(Verdict.unknown, try knownHostsVerdict(a, text, "files.example.com", 22, "ssh-ed25519", blob));
    try std.testing.expectEqual(Verdict.ok, try knownHostsVerdict(a, text, "SFTP.Bank.Example", 22, "ssh-ed25519", blob));
    try std.testing.expectEqual(Verdict.revoked, try knownHostsVerdict(a, text, "bad.example", 22, "ssh-ed25519", blob));
    try std.testing.expectEqual(Verdict.changed, try knownHostsVerdict(a, text, "old.example", 22, "ssh-ed25519", blob));
    // another key type for the host is not a change
    try std.testing.expectEqual(Verdict.unknown, try knownHostsVerdict(a, text, "old.example", 22, "ssh-rsa", blob));

    // hashed: |1|base64(salt)|base64(HMAC-SHA1(salt, name))
    const salt = "0123456789abcdef0123";
    var mac: [20]u8 = undefined;
    HmacSha1.create(&mac, "hidden.example", salt);
    var s64: [32]u8 = undefined;
    var m64: [32]u8 = undefined;
    const ht = try std.fmt.bufPrint(&buf, "|1|{s}|{s} ssh-ed25519 {s}\n", .{ enc.encode(&s64, salt), enc.encode(&m64, &mac), k64 });
    try std.testing.expectEqual(Verdict.ok, try knownHostsVerdict(a, ht, "hidden.example", 22, "ssh-ed25519", blob));
    try std.testing.expectEqual(Verdict.unknown, try knownHostsVerdict(a, ht, "other.example", 22, "ssh-ed25519", blob));
}

test "fingerprint as OpenSSH prints it" {
    var out: [64]u8 = undefined;
    const fp = fingerprint("", &out);
    // SHA-256 of nothing, base64 without padding
    try std.testing.expectEqualStrings("SHA256:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU", fp);
}

test "live: log in and open sftp (BASALT_SSH_TEST_PORT)" {
    const port_s = std.posix.getenv("BASALT_SSH_TEST_PORT") orelse return error.SkipZigTest;
    const dir = std.posix.getenv("BASALT_SSH_TEST_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    const kh = try std.fmt.allocPrint(a, "{s}/known_hosts", .{dir});
    defer a.free(kh);
    const key = try std.fmt.allocPrint(a, "{s}/id_ed25519", .{dir});
    defer a.free(key);
    const port = try std.fmt.parseInt(u16, port_s, 10);
    for ([_]Config{
        .{ .host = "127.0.0.1", .port = port, .user = "basalt", .password = "pw", .known_hosts = kh },
        .{ .host = "127.0.0.1", .port = port, .user = "basalt", .key_file = key, .known_hosts = kh },
    }) |cfg| {
        const s = Session.connect(a, cfg) catch |e| {
            std.debug.print("connect failed: {s}\n", .{@errorName(e)});
            return e;
        };
        defer s.close();
        try s.openSubsystem("sftp");
        std.debug.print("ok: tx {s}/{s} strict={}\n", .{ s.tx.cipher.name(), s.tx.mac.name(), s.strict });
    }
    // an unknown host and a wrong pin are refused; the right pin is enough alone
    try std.testing.expectError(error.SshHostKeyUnknown, Session.connect(a, .{ .host = "127.0.0.1", .port = port, .user = "basalt", .password = "pw", .known_hosts = "/dev/null" }));
    try std.testing.expectError(error.SshHostKeyChanged, Session.connect(a, .{ .host = "127.0.0.1", .port = port, .user = "basalt", .password = "pw", .host_key = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" }));
    const pin = std.posix.getenv("BASALT_SSH_TEST_PIN") orelse return;
    const s = try Session.connect(a, .{ .host = "127.0.0.1", .port = port, .user = "basalt", .password = "pw", .host_key = pin });
    s.close();
}
