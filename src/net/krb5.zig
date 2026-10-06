//! A Kerberos 5 client, enough to log in to a Windows file share: a password
//! turned into an AES key (RFC 3962), a ticket-granting ticket from the KDC with
//! encrypted-timestamp pre-authentication (RFC 4120; Active Directory requires
//! it), a service ticket for `cifs/<host>`, and the AP-REQ a server accepts
//! inside SPNEGO (GSS-API, RFC 4121), its AP-REP checked in turn so the server
//! proves it holds the service key (mutual authentication). What it yields is the
//! session key SMB signs and seals with.
//!
//! AES only: aes256-cts-hmac-sha1-96 and aes128-cts-hmac-sha1-96. A KDC that
//! offers an account nothing but RC4 is refused by name: RC4 is off by default
//! on current Windows domains and weak where it is not.
//!
//! The KDC is the connection's `kdc`, else what DNS lists for
//! `_kerberos._tcp.<realm>`, else the realm's own name (an AD domain's name
//! resolves to its domain controllers). The realm is kept upper-cased, as AD
//! writes it. Tickets are kept in-process per principal and service until they
//! expire, so a parallel read's lanes do not each go back to the KDC.
//! `lastError` says why the last call on this thread failed, never with a secret.
//! The live test against a real domain never tries a wrong password: each one
//! counts toward the account's lockout.

const std = @import("std");

pub const Error = error{
    KrbProtocol,
    KrbPreauthFailed,
    KrbPrincipalUnknown,
    KrbServiceUnknown,
    KrbClockSkew,
    KrbAccountRestricted,
    KrbEtypeUnsupported,
    KrbIntegrity,
    KrbKdcUnreachable,
    KrbFailure,
};

pub const Credential = struct {
    user: []const u8,
    password: []const u8,
    realm: []const u8,
    kdc: []const u8 = "",
};

/// A credential from a login as people write it: `me@CORP.LOCAL` names the realm
/// inline, `DOMAIN\\me` is taken as `me`, and an explicit `realm` wins.
pub fn credential(user_in: []const u8, password: []const u8, realm_in: []const u8, kdc: []const u8, buf: []u8) ?Credential {
    var user = user_in;
    var realm = realm_in;
    if (std.mem.indexOfScalar(u8, user, '\\')) |i| user = user[i + 1 ..];
    if (std.mem.lastIndexOfScalar(u8, user, '@')) |i| {
        if (realm.len == 0) realm = user[i + 1 ..];
        user = user[0..i];
    }
    if (realm.len == 0 or realm.len > buf.len) return null;
    return .{ .user = user, .password = password, .realm = std.ascii.upperString(buf[0..realm.len], realm), .kdc = kdc };
}

threadlocal var why_buf: [512]u8 = undefined;
threadlocal var why_len: usize = 0;

pub fn lastError() []const u8 {
    return why_buf[0..why_len];
}

fn fail(e: Error, comptime fmt: []const u8, args: anytype) Error {
    const s = std.fmt.bufPrint(&why_buf, fmt, args) catch why_buf[0..];
    why_len = s.len;
    return e;
}

pub const etype_aes128: i32 = 17;
pub const etype_aes256: i32 = 18;
const etype_rc4: i32 = 23;
const cksum_aes128: i32 = 15;
const cksum_aes256: i32 = 16;

pub const Key = struct {
    etype: i32,
    bytes: [32]u8 = undefined,
    len: usize,

    pub fn slice(self: *const Key) []const u8 {
        return self.bytes[0..self.len];
    }
};

fn keyLen(etype: i32) usize {
    return if (etype == etype_aes256) 32 else 16;
}

/// The n-fold of `in` into `out` (RFC 3961 5.1): the input repeated to the lcm of
/// the two lengths, each copy rotated 13 bits right, summed in one's complement.
pub fn nfold(out: []u8, in: []const u8) void {
    const n = out.len;
    const k = in.len;
    const lcm = n / std.math.gcd(n, k) * k;
    @memset(out, 0);
    var carry: u32 = 0;
    var i: usize = lcm;
    while (i > 0) {
        i -= 1;
        const msbit = ((k << 3) - 1 + (((k << 3) + 13) * (i / k)) + ((k - (i % k)) << 3)) % (k << 3);
        const b: u32 = ((@as(u32, in[((k - 1) - (msbit >> 3)) % k]) << 8) | in[((k) - (msbit >> 3)) % k]);
        const byte: u32 = (b >> @intCast((msbit & 7) + 1)) & 0xff;
        carry += byte + out[i % n];
        out[i % n] = @truncate(carry);
        carry >>= 8;
    }
    if (carry != 0) {
        var j: usize = n;
        while (j > 0) {
            j -= 1;
            carry += out[j];
            out[j] = @truncate(carry);
            carry >>= 8;
            if (carry == 0) break;
        }
    }
}

fn aesEncryptBlock(key: []const u8, out: *[16]u8, in: *const [16]u8) void {
    if (key.len == 32) {
        std.crypto.core.aes.Aes256.initEnc(key[0..32].*).encrypt(out, in);
    } else {
        std.crypto.core.aes.Aes128.initEnc(key[0..16].*).encrypt(out, in);
    }
}

fn aesDecryptBlock(key: []const u8, out: *[16]u8, in: *const [16]u8) void {
    if (key.len == 32) {
        std.crypto.core.aes.Aes256.initDec(key[0..32].*).decrypt(out, in);
    } else {
        std.crypto.core.aes.Aes128.initDec(key[0..16].*).decrypt(out, in);
    }
}

/// AES-CBC with ciphertext stealing (RFC 3962 5), IV zero: the input zero-padded
/// and CBC-encrypted, the last two blocks swapped, the output cut to the input's length.
pub fn ctsEncrypt(key: []const u8, out: []u8, in: []const u8) void {
    std.debug.assert(in.len >= 16 and out.len == in.len);
    var prev: [16]u8 = [_]u8{0} ** 16;
    const nblocks = (in.len + 15) / 16;
    var last_two: [32]u8 = undefined;
    var i: usize = 0;
    while (i < nblocks) : (i += 1) {
        var blk: [16]u8 = [_]u8{0} ** 16;
        const n = @min(16, in.len - i * 16);
        @memcpy(blk[0..n], in[i * 16 ..][0..n]);
        for (&blk, prev) |*b, p| b.* ^= p;
        aesEncryptBlock(key, &prev, &blk);
        if (nblocks == 1) {
            @memcpy(out, &prev);
            return;
        }
        if (i + 2 < nblocks) {
            @memcpy(out[i * 16 ..][0..16], &prev);
        } else if (i + 2 == nblocks) {
            @memcpy(last_two[16..32], &prev);
        } else {
            @memcpy(last_two[0..16], &prev);
        }
    }
    const tail = in.len - (nblocks - 2) * 16;
    @memcpy(out[(nblocks - 2) * 16 ..][0..tail], last_two[0..tail]);
}

pub fn ctsDecrypt(key: []const u8, out: []u8, in: []const u8) void {
    std.debug.assert(in.len >= 16 and out.len == in.len);
    const nblocks = (in.len + 15) / 16;
    if (nblocks == 1) {
        aesDecryptBlock(key, out[0..16], in[0..16]);
        return;
    }
    var prev: [16]u8 = [_]u8{0} ** 16;
    var i: usize = 0;
    while (i + 2 < nblocks) : (i += 1) {
        var d: [16]u8 = undefined;
        aesDecryptBlock(key, &d, in[i * 16 ..][0..16]);
        for (out[i * 16 ..][0..16], d, prev) |*o, x, p| o.* = x ^ p;
        @memcpy(&prev, in[i * 16 ..][0..16]);
    }
    const base = (nblocks - 2) * 16;
    const r = in.len - base - 16;
    var x: [16]u8 = undefined;
    aesDecryptBlock(key, &x, in[base..][0..16]);
    var full: [16]u8 = undefined;
    @memcpy(full[0..r], in[base + 16 ..][0..r]);
    @memcpy(full[r..16], x[r..16]);
    for (out[base + 16 ..][0..r], x[0..r], full[0..r]) |*o, a, b| o.* = a ^ b;
    var d: [16]u8 = undefined;
    aesDecryptBlock(key, &d, &full);
    for (out[base..][0..16], d, prev) |*o, a, p| o.* = a ^ p;
}

/// DK(key, constant) (RFC 3961 5.3): the n-folded constant encrypted, then each
/// output encrypted again, until a key's worth.
pub fn derive(key: []const u8, constant: []const u8) Key {
    var k = Key{ .etype = if (key.len == 32) etype_aes256 else etype_aes128, .len = key.len };
    var blk: [16]u8 = undefined;
    nfold(&blk, constant);
    var at: usize = 0;
    while (at < key.len) : (at += 16) {
        var o: [16]u8 = undefined;
        aesEncryptBlock(key, &o, &blk);
        const n = @min(16, key.len - at);
        @memcpy(k.bytes[at..][0..n], o[0..n]);
        blk = o;
    }
    return k;
}

/// The key of a password (RFC 3962 4): PBKDF2-HMAC-SHA1 over the salt, then DK
/// with "kerberos". `iterations` defaults to 4096 when the KDC sends no params.
pub fn stringToKey(etype: i32, password: []const u8, salt: []const u8, iterations: u32) !Key {
    var tkey: [32]u8 = undefined;
    const n = keyLen(etype);
    try std.crypto.pwhash.pbkdf2(tkey[0..n], password, salt, iterations, std.crypto.auth.hmac.HmacSha1);
    var k = derive(tkey[0..n], "kerberos");
    k.etype = etype;
    return k;
}

fn usageKey(base: *const Key, usage: u32, suffix: u8) Key {
    var c: [5]u8 = undefined;
    std.mem.writeInt(u32, c[0..4], usage, .big);
    c[4] = suffix;
    return derive(base.slice(), &c);
}

const HmacSha1 = std.crypto.auth.hmac.HmacSha1;

/// A random confounder and the message, CTS-encrypted with Ke, then 96 bits of
/// HMAC-SHA1 with Ki over the plaintext.
pub fn encrypt(gpa: std.mem.Allocator, key: *const Key, usage: u32, msg: []const u8) ![]u8 {
    const ke = usageKey(key, usage, 0xAA);
    const ki = usageKey(key, usage, 0x55);
    const plain = try gpa.alloc(u8, 16 + msg.len);
    defer gpa.free(plain);
    std.crypto.random.bytes(plain[0..16]);
    @memcpy(plain[16..], msg);
    const out = try gpa.alloc(u8, plain.len + 12);
    ctsEncrypt(ke.slice(), out[0..plain.len], plain);
    var mac: [20]u8 = undefined;
    HmacSha1.create(&mac, plain, ki.slice());
    @memcpy(out[plain.len..], mac[0..12]);
    return out;
}

pub fn decrypt(gpa: std.mem.Allocator, key: *const Key, usage: u32, ct: []const u8) ![]u8 {
    if (ct.len < 16 + 12) return fail(error.KrbProtocol, "an encrypted part too short to hold anything", .{});
    const ke = usageKey(key, usage, 0xAA);
    const ki = usageKey(key, usage, 0x55);
    const body = ct[0 .. ct.len - 12];
    const plain = try gpa.alloc(u8, body.len);
    defer gpa.free(plain);
    ctsDecrypt(ke.slice(), plain, body);
    var mac: [20]u8 = undefined;
    HmacSha1.create(&mac, plain, ki.slice());
    if (!std.crypto.timing_safe.eql([12]u8, mac[0..12].*, ct[ct.len - 12 ..][0..12].*))
        return error.KrbIntegrity;
    return gpa.dupe(u8, plain[16..]);
}

fn checksum(key: *const Key, usage: u32, msg: []const u8) [12]u8 {
    const kc = usageKey(key, usage, 0x99);
    var mac: [20]u8 = undefined;
    HmacSha1.create(&mac, msg, kc.slice());
    return mac[0..12].*;
}

const Der = struct {
    out: std.array_list.Managed(u8),

    fn init(gpa: std.mem.Allocator) Der {
        return .{ .out = .init(gpa) };
    }

    fn len(self: *Der, n: usize) !void {
        if (n < 0x80) return self.out.append(@intCast(n));
        var tmp: [4]u8 = undefined;
        var k: usize = 0;
        var v = n;
        while (v > 0) : (v >>= 8) {
            tmp[k] = @truncate(v);
            k += 1;
        }
        try self.out.append(@intCast(0x80 | k));
        while (k > 0) {
            k -= 1;
            try self.out.append(tmp[k]);
        }
    }

    fn wrap(self: *Der, tag: u8, body: []const u8) !void {
        try self.out.append(tag);
        try self.len(body.len);
        try self.out.appendSlice(body);
    }
};

fn derInt(gpa: std.mem.Allocator, v: i64) ![]u8 {
    var tmp: [9]u8 = undefined;
    var n: usize = 0;
    var x = v;
    while (true) {
        tmp[8 - n] = @truncate(@as(u64, @bitCast(x)));
        n += 1;
        x >>= 8;
        const top = tmp[9 - n];
        if ((x == 0 and top & 0x80 == 0) or (x == -1 and top & 0x80 != 0)) break;
    }
    var d = Der.init(gpa);
    try d.wrap(0x02, tmp[9 - n ..]);
    return d.out.toOwnedSlice();
}

fn derOf(gpa: std.mem.Allocator, tag: u8, parts: []const []const u8) ![]u8 {
    const body = try std.mem.concat(gpa, u8, parts);
    defer gpa.free(body);
    var d = Der.init(gpa);
    try d.wrap(tag, body);
    return d.out.toOwnedSlice();
}

const B = struct {
    gpa: std.mem.Allocator,
    made: std.array_list.Managed([]u8),

    fn init(gpa: std.mem.Allocator) B {
        return .{ .gpa = gpa, .made = .init(gpa) };
    }

    fn deinit(self: *B) void {
        for (self.made.items) |m| self.gpa.free(m);
        self.made.deinit();
    }

    fn keep(self: *B, m: []u8) ![]const u8 {
        try self.made.append(m);
        return m;
    }

    fn tag(self: *B, t: u8, parts: []const []const u8) ![]const u8 {
        return self.keep(try derOf(self.gpa, t, parts));
    }

    fn seq(self: *B, parts: []const []const u8) ![]const u8 {
        return self.tag(0x30, parts);
    }

    fn ctx(self: *B, n: u8, inner: []const u8) ![]const u8 {
        return self.tag(0xa0 | n, &.{inner});
    }

    fn int(self: *B, v: i64) ![]const u8 {
        return self.keep(try derInt(self.gpa, v));
    }

    fn str(self: *B, s: []const u8) ![]const u8 {
        return self.tag(0x1b, &.{s});
    }

    fn octets(self: *B, s: []const u8) ![]const u8 {
        return self.tag(0x04, &.{s});
    }

    fn bits32(self: *B, v: u32) ![]const u8 {
        var b: [5]u8 = undefined;
        b[0] = 0;
        std.mem.writeInt(u32, b[1..5], v, .big);
        return self.tag(0x03, &.{&b});
    }

    fn time(self: *B, unix: i64) ![]const u8 {
        var buf: [15]u8 = undefined;
        return self.tag(0x18, &.{kerberosTime(&buf, unix)});
    }

    fn principal(self: *B, name_type: i32, parts: []const []const u8) ![]const u8 {
        var names = std.array_list.Managed([]const u8).init(self.gpa);
        defer names.deinit();
        for (parts) |p| try names.append(try self.str(p));
        return self.seq(&.{ try self.ctx(0, try self.int(name_type)), try self.ctx(1, try self.seq(names.items)) });
    }

    fn encrypted(self: *B, etype: i32, kvno: ?i64, cipher: []const u8) ![]const u8 {
        if (kvno) |v| return self.seq(&.{ try self.ctx(0, try self.int(etype)), try self.ctx(1, try self.int(v)), try self.ctx(2, try self.octets(cipher)) });
        return self.seq(&.{ try self.ctx(0, try self.int(etype)), try self.ctx(2, try self.octets(cipher)) });
    }
};

fn kerberosTime(buf: *[15]u8, unix: i64) []const u8 {
    const days = @divFloor(unix, 86400);
    const secs: u64 = @intCast(unix - days * 86400);
    const c = civil(days);
    return std.fmt.bufPrint(buf, "{d:0>4}{d:0>2}{d:0>2}{d:0>2}{d:0>2}{d:0>2}Z", .{ @as(u64, @intCast(c.y)), c.m, c.d, secs / 3600, (secs / 60) % 60, secs % 60 }) catch unreachable;
}

fn civil(z0: i64) struct { y: i64, m: u32, d: u32 } {
    const z = z0 + 719468;
    const era = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const y = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d: u32 = @intCast(doy - @divFloor(153 * mp + 2, 5) + 1);
    const m: u32 = @intCast(if (mp < 10) mp + 3 else mp - 9);
    return .{ .y = y + (if (m <= 2) @as(i64, 1) else 0), .m = m, .d = d };
}

fn parseTime(s: []const u8) i64 {
    if (s.len < 15) return 0;
    const n = struct {
        fn f(t: []const u8) i64 {
            return std.fmt.parseInt(i64, t, 10) catch 0;
        }
    }.f;
    const y = n(s[0..4]);
    const mo = n(s[4..6]);
    const d = n(s[6..8]);
    const yy = if (mo <= 2) y - 1 else y;
    const era = @divFloor(if (yy >= 0) yy else yy - 399, 400);
    const yoe = yy - era * 400;
    const mp = if (mo > 2) mo - 3 else mo + 9;
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    const days = era * 146097 + doe - 719468;
    return days * 86400 + n(s[8..10]) * 3600 + n(s[10..12]) * 60 + n(s[12..14]);
}

const El = struct {
    tag: u8,
    body: []const u8,
    end: usize,
    raw: []const u8,

    fn at(b: []const u8, i: usize) ?El {
        if (i + 2 > b.len) return null;
        var j = i + 1;
        var n: usize = b[j];
        j += 1;
        if (n & 0x80 != 0) {
            const k = n & 0x7f;
            if (k == 0 or k > 4 or j + k > b.len) return null;
            n = 0;
            for (b[j..][0..k]) |c| n = (n << 8) | c;
            j += k;
        }
        if (j + n > b.len) return null;
        return .{ .tag = b[i], .body = b[j..][0..n], .end = j + n, .raw = b[i .. j + n] };
    }

    fn child(self: El, t: u8) ?El {
        var i: usize = 0;
        while (El.at(self.body, i)) |e| : (i = e.end) {
            if (e.tag == t) return e;
        }
        return null;
    }

    fn field(self: El, n: u8) ?El {
        const c = self.child(0xa0 | n) orelse return null;
        return El.at(c.body, 0);
    }

    fn int(self: El) i64 {
        var v: i64 = if (self.body.len > 0 and self.body[0] & 0x80 != 0) -1 else 0;
        for (self.body) |c| v = (v << 8) | c;
        return v;
    }

    fn inner(self: El) ?El {
        return El.at(self.body, 0);
    }
};

const pvno = 5;
const msg_as_req = 10;
const msg_as_rep = 11;
const msg_tgs_req = 12;
const msg_tgs_rep = 13;
const msg_ap_req = 14;
const msg_error = 30;
const nt_principal = 1;
const nt_srv_inst = 2;
const pa_tgs_req = 1;
const pa_enc_timestamp = 2;
const pa_etype_info2 = 19;
const kdc_options: u32 = 0x4081_0010;

const err_principal_unknown = 6;
const err_service_unknown = 7;
const err_etype_nosupp = 14;
const err_client_revoked = 18;
const err_key_expired = 23;
const err_preauth_failed = 24;
const err_preauth_required = 25;
const err_skew = 37;

fn exchange(gpa: std.mem.Allocator, cred: Credential, req: []const u8) ![]u8 {
    var hosts_buf: [16][]const u8 = undefined;
    var hosts = std.array_list.Managed([]const u8).init(gpa);
    defer {
        for (hosts.items) |h| gpa.free(h);
        hosts.deinit();
    }
    if (cred.kdc.len > 0) {
        try hosts.append(try gpa.dupe(u8, cred.kdc));
    } else {
        const n = srvLookup(gpa, cred.realm, &hosts_buf) catch 0;
        for (hosts_buf[0..n]) |h| try hosts.append(h);
        try hosts.append(try std.ascii.allocLowerString(gpa, cred.realm));
    }
    var last: anyerror = error.KrbKdcUnreachable;
    for (hosts.items) |hp| {
        var host = hp;
        var port: u16 = 88;
        if (std.mem.lastIndexOfScalar(u8, hp, ':')) |c| {
            host = hp[0..c];
            port = std.fmt.parseInt(u16, hp[c + 1 ..], 10) catch 88;
        }
        const stream = std.net.tcpConnectToHost(gpa, host, port) catch |e| {
            last = e;
            continue;
        };
        defer stream.close();
        var lenb: [4]u8 = undefined;
        std.mem.writeInt(u32, &lenb, @intCast(req.len), .big);
        stream.writeAll(&lenb) catch |e| {
            last = e;
            continue;
        };
        stream.writeAll(req) catch |e| {
            last = e;
            continue;
        };
        var got: [4]u8 = undefined;
        if (!readFull(stream, &got)) continue;
        const n = std.mem.readInt(u32, &got, .big);
        if (n > 1 << 20) return fail(error.KrbProtocol, "the KDC at {s} sent a {d}-byte reply", .{ hp, n });
        const buf = try gpa.alloc(u8, n);
        if (!readFull(stream, buf)) {
            gpa.free(buf);
            continue;
        }
        return buf;
    }
    return fail(error.KrbKdcUnreachable, "no KDC for {s} answered ({s}) — set `kdc` on the connection", .{ cred.realm, @errorName(last) });
}

fn readFull(stream: std.net.Stream, buf: []u8) bool {
    var at: usize = 0;
    while (at < buf.len) {
        const n = stream.read(buf[at..]) catch return false;
        if (n == 0) return false;
        at += n;
    }
    return true;
}

fn kdcError(e: El, cred: Credential, what: []const u8) Error {
    const code = if (e.field(6)) |c| c.int() else -1;
    const stime = if (e.field(4)) |t| parseTime(t.body) else 0;
    return switch (code) {
        err_principal_unknown => fail(error.KrbPrincipalUnknown, "the KDC knows no user {s}@{s}", .{ cred.user, cred.realm }),
        err_service_unknown => fail(error.KrbServiceUnknown, "the KDC knows no service {s} in {s} — set `spn` to the name the server is registered under", .{ what, cred.realm }),
        err_preauth_failed => fail(error.KrbPreauthFailed, "the KDC refused the password for {s}@{s}", .{ cred.user, cred.realm }),
        err_client_revoked => fail(error.KrbAccountRestricted, "the account {s}@{s} is disabled or locked out", .{ cred.user, cred.realm }),
        err_key_expired => fail(error.KrbAccountRestricted, "the password of {s}@{s} has expired", .{ cred.user, cred.realm }),
        err_etype_nosupp => fail(error.KrbEtypeUnsupported, "the KDC offers {s} no AES key; basalt does not use RC4 — enable AES on the account", .{cred.user}),
        err_skew => fail(error.KrbClockSkew, "this machine's clock is {d} seconds off the KDC's; Kerberos allows five minutes", .{std.time.timestamp() - stime}),
        else => fail(error.KrbFailure, "the KDC refused {s} (error {d}{s}{s})", .{ what, code, if (e.field(11) != null) ": " else "", if (e.field(11)) |t| t.body else "" }),
    };
}

pub const Ticket = struct {
    der: []u8,
    key: Key,
    end: i64,
    realm: []const u8,
};

/// A ticket-granting ticket for `cred`. The first request learns the
/// pre-authentication, salt and key type the KDC expects.
pub fn asExchange(gpa: std.mem.Allocator, cred: Credential) !Ticket {
    var salt_buf: [512]u8 = undefined;
    var salt: []const u8 = std.fmt.bufPrint(&salt_buf, "{s}{s}", .{ cred.realm, cred.user }) catch return error.KrbFailure;
    var etype: i32 = etype_aes256;
    var iterations: u32 = 4096;
    const first = try asRequest(gpa, cred, null);
    defer gpa.free(first);
    const r1 = try exchange(gpa, cred, first);
    defer gpa.free(r1);
    const e1 = El.at(r1, 0) orelse return fail(error.KrbProtocol, "the KDC's reply is not DER", .{});
    if (e1.tag != 0x7e) return fail(error.KrbProtocol, "the KDC answered a request without pre-authentication with a ticket; refusing", .{});
    const err = e1.inner() orelse return error.KrbProtocol;
    const code = if (err.field(6)) |c| c.int() else -1;
    if (code != err_preauth_required) return kdcError(err, cred, "the login");
    if (err.field(12)) |edata| {
        const methods = El.at(edata.body, 0) orelse return error.KrbProtocol;
        var i: usize = 0;
        var found = false;
        var only_rc4 = false;
        while (El.at(methods.body, i)) |pa| : (i = pa.end) {
            if ((pa.field(1) orelse continue).int() != pa_etype_info2) continue;
            const v = pa.field(2) orelse continue;
            const entries = El.at(v.body, 0) orelse continue;
            var j: usize = 0;
            only_rc4 = true;
            while (El.at(entries.body, j)) |ent| : (j = ent.end) {
                const et: i32 = @intCast((ent.field(0) orelse continue).int());
                if (et != etype_aes256 and et != etype_aes128) continue;
                only_rc4 = false;
                if (found and et != etype_aes256) continue;
                etype = et;
                found = true;
                if (ent.field(1)) |s| {
                    const n = @min(s.body.len, salt_buf.len);
                    @memcpy(salt_buf[0..n], s.body[0..n]);
                    salt = salt_buf[0..n];
                }
                if (ent.field(2)) |p| if (p.body.len == 4) {
                    iterations = std.mem.readInt(u32, p.body[0..4], .big);
                };
            }
        }
        if (only_rc4) return fail(error.KrbEtypeUnsupported, "the KDC offers {s} only RC4, which basalt does not use — enable AES on the account", .{cred.user});
    }
    const key = stringToKey(etype, cred.password, salt, iterations) catch return error.KrbFailure;

    const second = try asRequest(gpa, cred, &key);
    defer gpa.free(second);
    const r2 = try exchange(gpa, cred, second);
    defer gpa.free(r2);
    return kdcReply(gpa, cred, r2, &key, 3, "the login");
}

fn asRequest(gpa: std.mem.Allocator, cred: Credential, key: ?*const Key) ![]u8 {
    var b = B.init(gpa);
    defer b.deinit();
    var padata = std.array_list.Managed([]const u8).init(gpa);
    defer padata.deinit();
    if (key) |k| {
        const now = std.time.microTimestamp();
        const ts = try b.seq(&.{ try b.ctx(0, try b.time(@divFloor(now, 1_000_000))), try b.ctx(1, try b.int(@mod(now, 1_000_000))) });
        const enc = try b.keep(try encrypt(gpa, k, 1, ts));
        const value = try b.encrypted(k.etype, null, enc);
        try padata.append(try b.seq(&.{ try b.ctx(1, try b.int(pa_enc_timestamp)), try b.ctx(2, try b.octets(value)) }));
    }
    const body = try reqBody(&b, cred.realm, try b.principal(nt_principal, &.{cred.user}), try b.principal(nt_srv_inst, &.{ "krbtgt", cred.realm }));
    var fields = std.array_list.Managed([]const u8).init(gpa);
    defer fields.deinit();
    try fields.append(try b.ctx(1, try b.int(pvno)));
    try fields.append(try b.ctx(2, try b.int(msg_as_req)));
    if (padata.items.len > 0) try fields.append(try b.ctx(3, try b.seq(padata.items)));
    try fields.append(try b.ctx(4, body));
    return derOf(gpa, 0x6a, &.{try b.seq(fields.items)});
}

fn reqBody(b: *B, realm: []const u8, cname: ?[]const u8, sname: []const u8) ![]const u8 {
    var nonce: u32 = undefined;
    std.crypto.random.bytes(std.mem.asBytes(&nonce));
    var etypes = [_][]const u8{ try b.int(etype_aes256), try b.int(etype_aes128) };
    var fields = std.array_list.Managed([]const u8).init(b.gpa);
    defer fields.deinit();
    try fields.append(try b.ctx(0, try b.bits32(kdc_options)));
    if (cname) |c| try fields.append(try b.ctx(1, c));
    try fields.append(try b.ctx(2, try b.str(realm)));
    try fields.append(try b.ctx(3, sname));
    try fields.append(try b.ctx(5, try b.time(std.time.timestamp() + 10 * 3600)));
    try fields.append(try b.ctx(7, try b.int(nonce & 0x7fff_ffff)));
    try fields.append(try b.ctx(8, try b.seq(&etypes)));
    return b.seq(fields.items);
}

fn kdcReply(gpa: std.mem.Allocator, cred: Credential, r: []const u8, key: *const Key, usage: u32, what: []const u8) !Ticket {
    const top = El.at(r, 0) orelse return fail(error.KrbProtocol, "the KDC's reply is not DER", .{});
    const rep = top.inner() orelse return error.KrbProtocol;
    if (top.tag == 0x7e) return kdcError(rep, cred, what);
    if (top.tag != 0x6b and top.tag != 0x6d) return fail(error.KrbProtocol, "the KDC sent an unexpected message (tag 0x{x})", .{top.tag});
    const ticket = rep.child(0xa5) orelse return error.KrbProtocol;
    const tdr = El.at(ticket.body, 0) orelse return error.KrbProtocol;
    const enc = rep.field(6) orelse return error.KrbProtocol;
    const cipher = enc.field(2) orelse return error.KrbProtocol;
    const plain = decrypt(gpa, key, usage, cipher.body) catch |e| switch (e) {
        error.KrbIntegrity => return fail(error.KrbPreauthFailed, "the KDC's reply does not open with the key of the password for {s}@{s}", .{ cred.user, cred.realm }),
        else => return e,
    };
    defer gpa.free(plain);
    const part = (El.at(plain, 0) orelse return error.KrbProtocol).inner() orelse return error.KrbProtocol;
    const ek = part.field(0) orelse return error.KrbProtocol;
    const kt: i32 = @intCast((ek.field(0) orelse return error.KrbProtocol).int());
    const kv = ek.field(1) orelse return error.KrbProtocol;
    if (kt != etype_aes256 and kt != etype_aes128) return fail(error.KrbEtypeUnsupported, "the KDC issued a key of type {d}; basalt uses AES only", .{kt});
    var sk = Key{ .etype = kt, .len = kv.body.len };
    if (kv.body.len != keyLen(kt)) return error.KrbProtocol;
    @memcpy(sk.bytes[0..sk.len], kv.body);
    const end = if (part.field(7)) |t| parseTime(t.body) else std.time.timestamp() + 3600;
    const srealm = if (part.field(9)) |s| s.body else cred.realm;
    return .{ .der = try gpa.dupe(u8, tdr.raw), .key = sk, .end = end, .realm = try gpa.dupe(u8, srealm) };
}

/// An AP-REQ carrying `t`, its authenticator under the session key for `usage`:
/// 7 inside a TGS-REQ (body checksum), 11 for the service (GSS checksum and subkey).
fn apReq(b: *B, cred: Credential, t: *const Ticket, usage: u32, cksum: []const u8, subkey: ?*const Key, mutual: bool) ![]const u8 {
    const now = std.time.microTimestamp();
    var fields = std.array_list.Managed([]const u8).init(b.gpa);
    defer fields.deinit();
    try fields.append(try b.ctx(0, try b.int(pvno)));
    try fields.append(try b.ctx(1, try b.str(cred.realm)));
    try fields.append(try b.ctx(2, try b.principal(nt_principal, &.{cred.user})));
    try fields.append(try b.ctx(3, cksum));
    try fields.append(try b.ctx(4, try b.int(@mod(now, 1_000_000))));
    try fields.append(try b.ctx(5, try b.time(@divFloor(now, 1_000_000))));
    if (subkey) |k| try fields.append(try b.ctx(6, try b.seq(&.{ try b.ctx(0, try b.int(k.etype)), try b.ctx(1, try b.octets(k.slice())) })));
    var seqn: u32 = undefined;
    std.crypto.random.bytes(std.mem.asBytes(&seqn));
    try fields.append(try b.ctx(7, try b.int(seqn & 0x7fff_ffff)));
    const auth = try b.tag(0x62, &.{try b.seq(fields.items)});
    const enc_auth = try b.keep(try encrypt(b.gpa, &t.key, usage, auth));
    return b.tag(0x6e, &.{try b.seq(&.{
        try b.ctx(0, try b.int(pvno)),
        try b.ctx(1, try b.int(msg_ap_req)),
        try b.ctx(2, try b.bits32(if (mutual) 0x2000_0000 else 0)),
        try b.ctx(3, t.der),
        try b.ctx(4, try b.encrypted(t.key.etype, null, enc_auth)),
    })});
}

pub fn tgsExchange(gpa: std.mem.Allocator, cred: Credential, tgt: *const Ticket, spn: []const u8) !Ticket {
    var b = B.init(gpa);
    defer b.deinit();
    var parts = std.array_list.Managed([]const u8).init(gpa);
    defer parts.deinit();
    var it = std.mem.splitScalar(u8, spn, '/');
    while (it.next()) |p| try parts.append(p);
    const body = try reqBody(&b, cred.realm, null, try b.principal(nt_srv_inst, parts.items));
    const ck = checksum(&tgt.key, 6, body);
    const cksum = try b.seq(&.{ try b.ctx(0, try b.int(if (tgt.key.etype == etype_aes256) cksum_aes256 else cksum_aes128)), try b.ctx(1, try b.octets(&ck)) });
    const ap = try apReq(&b, cred, tgt, 7, cksum, null, false);
    const pa = try b.seq(&.{ try b.ctx(1, try b.int(pa_tgs_req)), try b.ctx(2, try b.octets(ap)) });
    const req = try derOf(gpa, 0x6c, &.{try b.seq(&.{
        try b.ctx(1, try b.int(pvno)),
        try b.ctx(2, try b.int(msg_tgs_req)),
        try b.ctx(3, try b.seq(&.{pa})),
        try b.ctx(4, body),
    })});
    defer gpa.free(req);
    const r = try exchange(gpa, cred, req);
    defer gpa.free(r);
    return kdcReply(gpa, cred, r, &tgt.key, 8, spn);
}

const oid_krb5 = [_]u8{ 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02 };

pub const Context = struct {
    token: []u8,
    ticket_key: Key,
    subkey: Key,
};

/// The GSS InitialContextToken for the service of `t`, asking for mutual
/// authentication (GSS checksum per RFC 4121 4.1.1, no channel bindings).
pub fn initContext(gpa: std.mem.Allocator, cred: Credential, t: *const Ticket) !Context {
    var b = B.init(gpa);
    defer b.deinit();
    var gss: [24]u8 = [_]u8{0} ** 24;
    std.mem.writeInt(u32, gss[0..4], 16, .little);
    std.mem.writeInt(u32, gss[20..24], 0x3e, .little);
    const cksum = try b.seq(&.{ try b.ctx(0, try b.int(0x8003)), try b.ctx(1, try b.octets(&gss)) });
    var sub = Key{ .etype = t.key.etype, .len = t.key.len };
    std.crypto.random.bytes(sub.bytes[0..sub.len]);
    const ap = try apReq(&b, cred, t, 11, cksum, &sub, true);
    const token = try derOf(gpa, 0x60, &.{ &oid_krb5, &[_]u8{ 0x01, 0x00 }, ap });
    return .{ .token = token, .ticket_key = t.key, .subkey = sub };
}

/// Check the service's AP-REP inside its GSS token and return the context's key:
/// the service's subkey when it sent one, else ours.
pub fn acceptReply(gpa: std.mem.Allocator, ctx: *const Context, token: []const u8) !Key {
    const g = El.at(token, 0) orelse return fail(error.KrbProtocol, "the server's Kerberos reply is not DER", .{});
    var rep_der: []const u8 = token;
    if (g.tag == 0x60) {
        const oid = El.at(g.body, 0) orelse return error.KrbProtocol;
        if (oid.end + 2 > g.body.len) return error.KrbProtocol;
        if (g.body[oid.end] == 0x03) return fail(error.KrbFailure, "the server answered with a Kerberos error", .{});
        rep_der = g.body[oid.end + 2 ..];
    }
    const top = El.at(rep_der, 0) orelse return error.KrbProtocol;
    if (top.tag == 0x7e) return fail(error.KrbFailure, "the server refused the ticket (error {d})", .{if ((top.inner() orelse return error.KrbProtocol).field(6)) |c| c.int() else -1});
    if (top.tag != 0x6f) return fail(error.KrbProtocol, "the server's Kerberos reply is not an AP-REP", .{});
    const rep = top.inner() orelse return error.KrbProtocol;
    const enc = rep.field(2) orelse return error.KrbProtocol;
    const cipher = enc.field(2) orelse return error.KrbProtocol;
    const plain = decrypt(gpa, &ctx.ticket_key, 12, cipher.body) catch |e| switch (e) {
        error.KrbIntegrity => return fail(error.KrbIntegrity, "the server's AP-REP does not open with the ticket's key — it is not the service the ticket was issued for", .{}),
        else => return e,
    };
    defer gpa.free(plain);
    const part = (El.at(plain, 0) orelse return error.KrbProtocol).inner() orelse return error.KrbProtocol;
    if (part.field(2)) |sk| {
        const kt: i32 = @intCast((sk.field(0) orelse return error.KrbProtocol).int());
        const kv = sk.field(1) orelse return error.KrbProtocol;
        if (kv.body.len > 32) return error.KrbProtocol;
        var k = Key{ .etype = kt, .len = kv.body.len };
        @memcpy(k.bytes[0..k.len], kv.body);
        return k;
    }
    return ctx.subkey;
}

const Cached = struct { key: []const u8, t: Ticket };
var cache_mtx: std.Thread.Mutex = .{};
var cache: std.ArrayListUnmanaged(Cached) = .empty;

fn cacheKey(buf: []u8, cred: Credential, spn: []const u8) []const u8 {
    var h = std.hash.Wyhash.init(0);
    h.update(cred.password);
    return std.fmt.bufPrint(buf, "{s}@{s}#{x}>{s}", .{ cred.user, cred.realm, h.final(), spn }) catch spn;
}

fn cached(key: []const u8) ?Ticket {
    cache_mtx.lock();
    defer cache_mtx.unlock();
    const now = std.time.timestamp();
    for (cache.items) |c| if (std.mem.eql(u8, c.key, key) and c.t.end > now + 60) return c.t;
    return null;
}

fn remember(key: []const u8, t: Ticket) void {
    const gpa = std.heap.page_allocator;
    cache_mtx.lock();
    defer cache_mtx.unlock();
    const k = gpa.dupe(u8, key) catch return;
    const der = gpa.dupe(u8, t.der) catch return;
    const realm = gpa.dupe(u8, t.realm) catch return;
    var copy = t;
    copy.der = der;
    copy.realm = realm;
    cache.append(gpa, .{ .key = k, .t = copy }) catch {};
}

pub fn serviceTicket(gpa: std.mem.Allocator, cred: Credential, spn: []const u8) !Ticket {
    why_len = 0;
    var kb: [512]u8 = undefined;
    const skey = cacheKey(&kb, cred, spn);
    if (cached(skey)) |t| return t;
    var tb: [512]u8 = undefined;
    const tkey = cacheKey(&tb, cred, "krbtgt");
    const tgt = cached(tkey) orelse blk: {
        const fresh = try asExchange(gpa, cred);
        defer {
            gpa.free(fresh.der);
            gpa.free(fresh.realm);
        }
        remember(tkey, fresh);
        break :blk cached(tkey) orelse return error.KrbFailure;
    };
    const st = try tgsExchange(gpa, cred, &tgt, spn);
    defer {
        gpa.free(st.der);
        gpa.free(st.realm);
    }
    remember(skey, st);
    return cached(skey) orelse error.KrbFailure;
}

fn srvLookup(gpa: std.mem.Allocator, realm: []const u8, out: [][]const u8) !usize {
    const ns = nameserver() orelse return 0;
    var q = std.array_list.Managed(u8).init(gpa);
    defer q.deinit();
    var id: u16 = undefined;
    std.crypto.random.bytes(std.mem.asBytes(&id));
    var hdr: [12]u8 = [_]u8{0} ** 12;
    std.mem.writeInt(u16, hdr[0..2], id, .big);
    hdr[2] = 0x01;
    std.mem.writeInt(u16, hdr[4..6], 1, .big);
    try q.appendSlice(&hdr);
    const name = try std.fmt.allocPrint(gpa, "_kerberos._tcp.{s}", .{realm});
    defer gpa.free(name);
    var labels = std.mem.splitScalar(u8, name, '.');
    while (labels.next()) |l| {
        if (l.len == 0 or l.len > 63) continue;
        try q.append(@intCast(l.len));
        for (l) |c| try q.append(std.ascii.toLower(c));
    }
    try q.appendSlice(&[_]u8{ 0, 0, 33, 0, 1 });

    const sock = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.DGRAM, 0);
    defer std.posix.close(sock);
    const tv = std.posix.timeval{ .sec = 3, .usec = 0 };
    std.posix.setsockopt(sock, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
    const addr = std.net.Address.initIp4(ns, 53);
    _ = try std.posix.sendto(sock, q.items, 0, &addr.any, addr.getOsSockLen());
    var buf: [4096]u8 = undefined;
    const n = try std.posix.recv(sock, &buf, 0);
    const r = buf[0..n];
    if (n < 12 or std.mem.readInt(u16, r[0..2], .big) != id) return 0;
    const an = std.mem.readInt(u16, r[6..8], .big);
    var at: usize = 12;
    at = skipName(r, at) orelse return 0;
    at += 4;
    var k: usize = 0;
    var i: usize = 0;
    while (i < an and k < out.len) : (i += 1) {
        at = skipName(r, at) orelse return k;
        if (at + 10 > r.len) return k;
        const ty = std.mem.readInt(u16, r[at..][0..2], .big);
        const rdlen = std.mem.readInt(u16, r[at + 8 ..][0..2], .big);
        at += 10;
        if (ty == 33 and at + 6 <= r.len) {
            const port = std.mem.readInt(u16, r[at + 4 ..][0..2], .big);
            var host = std.array_list.Managed(u8).init(gpa);
            defer host.deinit();
            readName(r, at + 6, &host) catch {};
            if (host.items.len > 0) {
                out[k] = try std.fmt.allocPrint(gpa, "{s}:{d}", .{ host.items, port });
                k += 1;
            }
        }
        at += rdlen;
    }
    return k;
}

fn nameserver() ?[4]u8 {
    const f = std.fs.cwd().openFile("/etc/resolv.conf", .{}) catch return null;
    defer f.close();
    var buf: [4096]u8 = undefined;
    const n = f.readAll(&buf) catch return null;
    var lines = std.mem.splitScalar(u8, buf[0..n], '\n');
    while (lines.next()) |line| {
        const t = std.mem.trim(u8, line, " \t\r");
        if (!std.mem.startsWith(u8, t, "nameserver")) continue;
        const ip = std.mem.trim(u8, t["nameserver".len..], " \t");
        const a = std.net.Address.parseIp4(ip, 53) catch continue;
        return @bitCast(a.in.sa.addr);
    }
    return null;
}

fn skipName(r: []const u8, start: usize) ?usize {
    var at = start;
    while (at < r.len) {
        const l = r[at];
        if (l == 0) return at + 1;
        if (l & 0xc0 == 0xc0) return at + 2;
        at += 1 + l;
    }
    return null;
}

fn readName(r: []const u8, start: usize, out: *std.array_list.Managed(u8)) !void {
    var at = start;
    var hops: usize = 0;
    while (at < r.len and hops < 16) {
        const l = r[at];
        if (l == 0) return;
        if (l & 0xc0 == 0xc0) {
            if (at + 1 >= r.len) return;
            at = (@as(usize, l & 0x3f) << 8) | r[at + 1];
            hops += 1;
            continue;
        }
        if (at + 1 + l > r.len) return;
        if (out.items.len > 0) try out.append('.');
        try out.appendSlice(r[at + 1 ..][0..l]);
        at += 1 + l;
    }
}

fn hex(comptime s: []const u8) [s.len / 2]u8 {
    var out: [s.len / 2]u8 = undefined;
    _ = std.fmt.hexToBytes(&out, s) catch unreachable;
    return out;
}

test "krb5: a login names its realm by `realm` or after the user's @, upper-cased" {
    var buf: [256]u8 = undefined;
    const a = credential("me@corp.local", "p", "", "", &buf).?;
    try std.testing.expectEqualStrings("me", a.user);
    try std.testing.expectEqualStrings("CORP.LOCAL", a.realm);
    const b = credential("me@corp.local", "p", "OTHER.LOCAL", "dc1:88", &buf).?;
    try std.testing.expectEqualStrings("me", b.user);
    try std.testing.expectEqualStrings("OTHER.LOCAL", b.realm);
    try std.testing.expectEqualStrings("dc1:88", b.kdc);
    const c = credential("CORP\\me", "p", "corp.local", "", &buf).?;
    try std.testing.expectEqualStrings("me", c.user);
    try std.testing.expectEqualStrings("CORP.LOCAL", c.realm);
    try std.testing.expect(credential("me", "p", "", "", &buf) == null);
}

test "krb5: n-fold reproduces RFC 3961 A.1" {
    const cases = [_]struct { bits: usize, in: []const u8, out: []const u8 }{
        .{ .bits = 64, .in = "012345", .out = "be072631276b1955" },
        .{ .bits = 56, .in = "password", .out = "78a07b6caf85fa" },
        .{ .bits = 64, .in = "Rough Consensus, and Running Code", .out = "bb6ed30870b7f0e0" },
        .{ .bits = 168, .in = "password", .out = "59e4a8ca7c0385c3c37b3f6d2000247cb6e6bd5b3e" },
        .{ .bits = 192, .in = "MASSACHVSETTS INSTITVTE OF TECHNOLOGY", .out = "db3b0d8f0b061e603282b308a50841229ad798fab9540c1b" },
        .{ .bits = 168, .in = "Q", .out = "518a54a215a8452a518a54a215a8452a518a54a215" },
        .{ .bits = 168, .in = "ba", .out = "fb25d531ae8974499f52fd92ea9857c4ba24cf297e" },
    };
    for (cases) |c| {
        var out: [32]u8 = undefined;
        nfold(out[0 .. c.bits / 8], c.in);
        var want: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(want[0 .. c.out.len / 2], c.out);
        try std.testing.expectEqualSlices(u8, want[0 .. c.out.len / 2], out[0 .. c.bits / 8]);
    }
}

test "krb5: string-to-key reproduces RFC 3962 B" {
    const cases = [_]struct { it: u32, pw: []const u8, salt: []const u8, k128: []const u8, k256: []const u8 }{
        .{ .it = 1, .pw = "password", .salt = "ATHENA.MIT.EDUraeburn", .k128 = "42263c6e89f4fc28b8df68ee09799f15", .k256 = "fe697b52bc0d3ce14432ba036a92e65bbb52280990a2fa27883998d72af30161" },
        .{ .it = 2, .pw = "password", .salt = "ATHENA.MIT.EDUraeburn", .k128 = "c651bf29e2300ac27fa469d693bdda13", .k256 = "a2e16d16b36069c135d5e9d2e25f896102685618b95914b467c67622225824ff" },
        .{ .it = 1200, .pw = "password", .salt = "ATHENA.MIT.EDUraeburn", .k128 = "4c01cd46d632d01e6dbe230a01ed642a", .k256 = "55a6ac740ad17b4846941051e1e8b0a7548d93b0ab30a8bc3ff16280382b8c2a" },
        .{ .it = 5, .pw = "password", .salt = &hex("1234567878563412"), .k128 = "e9b23d52273747dd5c35cb55be619d8e", .k256 = "97a4e786be20d81a382d5ebc96d5909cabcdadc87ca48f574504159f16c36e31" },
        .{ .it = 1200, .pw = "X" ** 64, .salt = "pass phrase equals block size", .k128 = "59d1bb789a828b1aa54ef9c2883f69ed", .k256 = "89adee3608db8bc71f1bfbfe459486b05618b70cbae22092534e56c553ba4b34" },
        .{ .it = 1200, .pw = "X" ** 65, .salt = "pass phrase exceeds block size", .k128 = "cb8005dc5f90179a7f02104c0018751d", .k256 = "d78c5c9cb872a8c9dad4697f0bb5b2d21496c82beb2caeda2112fceea057401b" },
        .{ .it = 50, .pw = "\xf0\x9d\x84\x9e", .salt = "EXAMPLE.COMpianist", .k128 = "f149c1f2e154a73452d43e7fe62a56e5", .k256 = "4b6d9839f84406df1f09cc166db4b83c571848b784a3d6bdc346589a3e393f9e" },
    };
    for (cases) |c| {
        var w128: [16]u8 = undefined;
        var w256: [32]u8 = undefined;
        _ = try std.fmt.hexToBytes(&w128, c.k128);
        _ = try std.fmt.hexToBytes(&w256, c.k256);
        try std.testing.expectEqualSlices(u8, &w128, (try stringToKey(etype_aes128, c.pw, c.salt, c.it)).slice());
        try std.testing.expectEqualSlices(u8, &w256, (try stringToKey(etype_aes256, c.pw, c.salt, c.it)).slice());
    }
}

test "krb5: AES-CTS reproduces RFC 3962 B, and opens what it sealed" {
    const key = "chicken teriyaki";
    const plain = "I would like the General Gau's Chicken, please, and wonton soup.";
    const cases = [_]struct { n: usize, out: []const u8 }{
        .{ .n = 17, .out = "c6353568f2bf8cb4d8a580362da7ff7f97" },
        .{ .n = 31, .out = "fc00783e0efdb2c1d445d4c8eff7ed2297687268d6ecccc0c07b25e25ecfe5" },
        .{ .n = 32, .out = "39312523a78662d5be7fcbcc98ebf5a897687268d6ecccc0c07b25e25ecfe584" },
        .{ .n = 47, .out = "97687268d6ecccc0c07b25e25ecfe584b3fffd940c16a18c1b5549d2f838029e39312523a78662d5be7fcbcc98ebf5" },
        .{ .n = 48, .out = "97687268d6ecccc0c07b25e25ecfe5849dad8bbb96c4cdc03bc103e1a194bbd839312523a78662d5be7fcbcc98ebf5a8" },
        .{ .n = 64, .out = "97687268d6ecccc0c07b25e25ecfe58439312523a78662d5be7fcbcc98ebf5a84807efe836ee89a526730dbc2f7bc8409dad8bbb96c4cdc03bc103e1a194bbd8" },
    };
    for (cases) |c| {
        var got: [64]u8 = undefined;
        ctsEncrypt(key, got[0..c.n], plain[0..c.n]);
        var want: [64]u8 = undefined;
        _ = try std.fmt.hexToBytes(want[0..c.n], c.out);
        try std.testing.expectEqualSlices(u8, want[0..c.n], got[0..c.n]);
        var back: [64]u8 = undefined;
        ctsDecrypt(key, back[0..c.n], got[0..c.n]);
        try std.testing.expectEqualStrings(plain[0..c.n], back[0..c.n]);
    }
}

test "krb5: encrypt and decrypt round-trip per usage, and a wrong key or usage fails" {
    const gpa = std.testing.allocator;
    const k = try stringToKey(etype_aes256, "secret", "EXAMPLE.COMuser", 4096);
    for ([_]usize{ 0, 1, 16, 33, 200 }) |n| {
        const msg = try gpa.alloc(u8, n);
        defer gpa.free(msg);
        @memset(msg, 'm');
        const ct = try encrypt(gpa, &k, 3, msg);
        defer gpa.free(ct);
        const back = try decrypt(gpa, &k, 3, ct);
        defer gpa.free(back);
        try std.testing.expectEqualSlices(u8, msg, back);
        try std.testing.expectError(error.KrbIntegrity, decrypt(gpa, &k, 8, ct));
    }
}

test "krb5: decrypt opens MIT krb5's known AES-CTS-HMAC-SHA1 ciphertexts, and a flipped bit fails" {
    const gpa = std.testing.allocator;
    const cases = [_]struct { usage: u32, plain: []const u8, key: []const u8, ct: []const u8 }{
        .{ .usage = 0, .plain = "", .key = "5a5c0f0ba54f3828b2195e66ca24a289", .ct = "49ff8e11c173d9583a3254fbe7b1f1df36c538e8416784a1672e6676" },
        .{ .usage = 1, .plain = "1", .key = "98450e3f3baa13f5c99beb936981b06f", .ct = "f86742f537b35dc2174a4dbaa920faf9042090b065e1ebb1cad9a65394" },
        .{ .usage = 2, .plain = "9 bytesss", .key = "9062430c8cda3388922e6d6a509f5b7a", .ct = "68fb9679601f45c78857b2bf820fd6e53eca8d42fd4b1d7024a09205abb7cd2ec26c355d2f" },
        .{ .usage = 3, .plain = "13 bytes byte", .key = "033ee6502c54fd23e27791e987983827", .ct = "ec366d0327a933bf49330e650e49bc6b974637fe80bf532fe51795b4809718e6194724db948d1fd637" },
        .{ .usage = 4, .plain = "30 bytes bytes bytes bytes byt", .key = "dceeb70b3de76562e689226c76429148", .ct = "c96081032d5d8eeb7e32b4089f789d0faa481dea74c0f97cbf3146ddfcf8e800156ecb532fc203e30ff600b63b350939fece510f02d7ff1e7bac" },
        .{ .usage = 0, .plain = "", .key = "17f275f2954f2ed1f90c377ba7f4d6a369aa0136e0bf0c927ad6133c693759a9", .ct = "e5094c55ee7b38262e2b044280b069379a95bf95bd8376fb3281b435" },
        .{ .usage = 1, .plain = "1", .key = "b9477e1ff0329c0050e20ce6c72d2dff27e8fe541ab0954429a9cb5b4f7b1e2a", .ct = "406150b97aeb76d43b36b62cc1ecdfbe6f40e95755e0beb5c27825f3a4" },
        .{ .usage = 2, .plain = "9 bytesss", .key = "b1ae4cd8462aff1677053cc9279aac30b796fb81ce21474dd3ddbcfea4ec76d7", .ct = "09957aa25fcaf88f7b39e4406e633012d5fea21853f6478da7065caef41fd454a40824eec5" },
        .{ .usage = 3, .plain = "13 bytes byte", .key = "e5a72be9b7926c1225bafef9c1872e7ba4cdb2b17893d84abd90acdd8764d966", .ct = "d8f1aafeec84587cc3e700a774e56651a6d693e174ec4473b5e6d96f80297a653fb818ad893e719f96" },
        .{ .usage = 4, .plain = "30 bytes bytes bytes bytes byt", .key = "f1c795e9248a09338d82c3f8d5b567040b0110736845041347235b1404231398", .ct = "d1137a4d634cfece924dbc3bf6790648bd5cff7de0e7b99460211d0daef3d79a295c688858f3b34b9cbd6eebae81daf6b734d4d498b6714f1c1d" },
    };
    for (cases) |c| {
        var k = Key{ .etype = if (c.key.len == 64) etype_aes256 else etype_aes128, .len = c.key.len / 2 };
        _ = try std.fmt.hexToBytes(k.bytes[0..k.len], c.key);
        var ct: [64]u8 = undefined;
        const n = c.ct.len / 2;
        _ = try std.fmt.hexToBytes(ct[0..n], c.ct);
        const back = try decrypt(gpa, &k, c.usage, ct[0..n]);
        defer gpa.free(back);
        try std.testing.expectEqualStrings(c.plain, back);
        ct[0] ^= 1;
        try std.testing.expectError(error.KrbIntegrity, decrypt(gpa, &k, c.usage, ct[0..n]));
    }
}

test "krb5: DER integers and times" {
    const gpa = std.testing.allocator;
    for ([_]struct { v: i64, h: []const u8 }{ .{ .v = 0, .h = "020100" }, .{ .v = 5, .h = "020105" }, .{ .v = 128, .h = "02020080" }, .{ .v = 0x8003, .h = "0203008003" }, .{ .v = -1, .h = "0201ff" } }) |c| {
        const d = try derInt(gpa, c.v);
        defer gpa.free(d);
        var want: [8]u8 = undefined;
        _ = try std.fmt.hexToBytes(want[0 .. c.h.len / 2], c.h);
        try std.testing.expectEqualSlices(u8, want[0 .. c.h.len / 2], d);
        try std.testing.expectEqual(c.v, (El.at(d, 0) orelse return error.TestUnexpectedResult).int());
    }
    var tb: [15]u8 = undefined;
    try std.testing.expectEqualStrings("20261005170729Z", kerberosTime(&tb, 1791220049));
    try std.testing.expectEqual(@as(i64, 1791220049), parseTime("20261005170729Z"));
}

test "krb5: a live login and service ticket (BASALT_KRB_REALM, _USER, _PASS, _SPN; _KDC optional)" {
    const realm = std.posix.getenv("BASALT_KRB_REALM") orelse return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const cred = Credential{
        .realm = realm,
        .user = std.posix.getenv("BASALT_KRB_USER") orelse return error.SkipZigTest,
        .password = std.posix.getenv("BASALT_KRB_PASS") orelse return error.SkipZigTest,
        .kdc = std.posix.getenv("BASALT_KRB_KDC") orelse "",
    };
    const spn = std.posix.getenv("BASALT_KRB_SPN") orelse return error.SkipZigTest;
    const tgt = asExchange(gpa, cred) catch |e| {
        std.debug.print("AS exchange: {s}: {s}\n", .{ @errorName(e), lastError() });
        return e;
    };
    defer {
        gpa.free(tgt.der);
        gpa.free(tgt.realm);
    }
    const st = tgsExchange(gpa, cred, &tgt, spn) catch |e| {
        std.debug.print("TGS exchange: {s}: {s}\n", .{ @errorName(e), lastError() });
        return e;
    };
    defer {
        gpa.free(st.der);
        gpa.free(st.realm);
    }
    try std.testing.expect(st.end > std.time.timestamp());
}
