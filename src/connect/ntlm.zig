//! NTLMv2 message construction, per [MS-NLMP]. Builds the Type 1
//! NEGOTIATE_MESSAGE and, from a server's Type 2 CHALLENGE_MESSAGE, the Type 3
//! AUTHENTICATE_MESSAGE. Pure: no sockets, no clock, no randomness — the
//! timestamp and the client challenge are injected by the caller, so every
//! message this module produces is a deterministic function of its inputs and
//! can be checked against the spec's own worked example.
//!
//! Deliberately omitted: NTLMv1 and LMv1, key exchange
//! (NTLMSSP_NEGOTIATE_KEY_EXCH), signing and sealing — a client that negotiates
//! none of them never needs RC4, which is the other primitive std does not
//! ship. Also omitted: the MIC (section 3.1.5.1.2), an HMAC-MD5 over the
//! NEGOTIATE, CHALLENGE and AUTHENTICATE messages concatenated. Emitting one
//! would mean taking the NEGOTIATE_MESSAGE bytes back in here and reserving a
//! 16-byte MIC field in the Type 3; instead the MsvAvFlags 0x2 bit is left
//! clear, which is exactly how a client tells the server there is no MIC to
//! check. What that costs: the negotiated flags are not integrity-protected
//! against a downgrade, and a server hardened to require a MIC refuses the
//! login. The Type 3 this builds otherwise has the same shape as the one in
//! the spec's own example (section 4.2.4.3), which carries no MIC either.
//!
//! MD4 lives here because the NT hash is defined in terms of it and std omits
//! it as a broken primitive; it is not exported and must not be used elsewhere.

const std = @import("std");

pub const Error = error{ NtlmBadMessage, NtlmUnsupported } || std.mem.Allocator.Error;

/// A Windows credential. `domain` may be empty for a local account.
pub const Credential = struct {
    domain: []const u8,
    user: []const u8,
    password: []const u8,
    workstation: []const u8 = "",
};

const signature = "NTLMSSP\x00";

// MS-NLMP 2.2.2.5. The spec draws the flag word MSB-first, so bit "A"
// (NTLMSSP_NEGOTIATE_UNICODE) is the least significant bit of the u32.
const negotiate_unicode: u32 = 0x0000_0001;
const request_target: u32 = 0x0000_0004;
const negotiate_ntlm: u32 = 0x0000_0200;
const negotiate_always_sign: u32 = 0x0000_8000;
const negotiate_extended_session_security: u32 = 0x0008_0000;
const negotiate_target_info: u32 = 0x0080_0000;

/// Sent unchanged in both messages: Unicode strings, extended session security
/// (which is what makes the response NTLMv2 rather than NTLMv1), and target
/// info. Signing, sealing and key exchange are left clear on purpose — this
/// module cannot honour them, and promising them to the server would strand the
/// connection after login. NTLMSSP_NEGOTIATE_ALWAYS_SIGN is set because
/// MS-NLMP 2.2.2.5 requires it in the NEGOTIATE_MESSAGE; it only asks that a
/// session key be derived, not that traffic be signed.
const client_flags = negotiate_unicode | request_target | negotiate_ntlm |
    negotiate_always_sign | negotiate_extended_session_security | negotiate_target_info;

const av_eol: u16 = 0;
const av_timestamp: u16 = 7;

/// Type 1 NEGOTIATE_MESSAGE. Caller owns the returned slice.
///
/// Carries neither DomainName nor Workstation: MS-NLMP 2.2.1.1 requires both to
/// be OEM-encoded here, and servers ignore them — the authoritative, Unicode
/// copies travel in the Type 3 message.
pub fn negotiate(gpa: std.mem.Allocator, cred: Credential) Error![]u8 {
    _ = cred;
    const msg = try gpa.alloc(u8, 32);
    @memset(msg, 0);
    @memcpy(msg[0..8], signature);
    std.mem.writeInt(u32, msg[8..12], 1, .little);
    std.mem.writeInt(u32, msg[12..16], client_flags, .little);
    putFields(msg, 16, 0, msg.len);
    putFields(msg, 24, 0, msg.len);
    return msg;
}

/// Parse a Type 2 CHALLENGE_MESSAGE and build the Type 3 AUTHENTICATE_MESSAGE.
/// `time` is a Windows FILETIME (100ns ticks since 1601-01-01 UTC) and `nonce`
/// the 8-byte client challenge — both injected so the result is deterministic
/// and testable. Caller owns the returned slice.
///
/// `time` is only used when the challenge carries no MsvAvTimestamp AV pair;
/// when it does, MS-NLMP 3.1.5.1.2 requires the server's clock to be echoed
/// instead, which this does.
pub fn authenticate(
    gpa: std.mem.Allocator,
    cred: Credential,
    challenge: []const u8,
    time: u64,
    nonce: [8]u8,
) Error![]u8 {
    const chal = try parseChallenge(challenge);
    const key = try ntowfv2(gpa, cred);

    // NTLMv2_CLIENT_CHALLENGE (MS-NLMP 2.2.2.7), which 3.3.2 calls `temp`:
    // RespType, HiRespType, Z(6), TimeStamp, ChallengeFromClient, Z(4),
    // the target info copied verbatim, then a trailing Z(4).
    const temp = try gpa.alloc(u8, 28 + chal.target_info.len + 4);
    defer gpa.free(temp);
    @memset(temp, 0);
    temp[0] = 1;
    temp[1] = 1;
    std.mem.writeInt(u64, temp[8..16], chal.timestamp orelse time, .little);
    @memcpy(temp[16..24], &nonce);
    @memcpy(temp[28..][0..chal.target_info.len], chal.target_info);

    var proof: [16]u8 = undefined;
    var nt_mac = std.crypto.auth.hmac.HmacMd5.init(&key);
    nt_mac.update(&chal.server_challenge);
    nt_mac.update(temp);
    nt_mac.final(&proof);

    // LMv2_RESPONSE (MS-NLMP 2.2.2.4). MS-NLMP 3.1.5.1.2 says a client SHOULD
    // send Z(24) here once the challenge carries a timestamp; a correct LMv2
    // response is accepted in either case, so it is always computed.
    var lm: [24]u8 = undefined;
    var lm_mac = std.crypto.auth.hmac.HmacMd5.init(&key);
    lm_mac.update(&chal.server_challenge);
    lm_mac.update(&nonce);
    lm_mac.final(lm[0..16]);
    @memcpy(lm[16..24], &nonce);

    const dom = try utf16le(gpa, cred.domain);
    defer gpa.free(dom);
    const usr = try utf16le(gpa, cred.user);
    defer gpa.free(usr);
    const wks = try utf16le(gpa, cred.workstation);
    defer gpa.free(wks);

    // Header through the Version field; no MIC field, matching the layout of
    // the AUTHENTICATE_MESSAGE in the spec's own example (MS-NLMP 4.2.4.3).
    const header = 72;
    const nt_len = proof.len + temp.len;
    if (nt_len > 0xffff or dom.len > 0xffff or usr.len > 0xffff or wks.len > 0xffff) {
        return error.NtlmUnsupported;
    }

    const dom_at = header;
    const usr_at = dom_at + dom.len;
    const wks_at = usr_at + usr.len;
    const lm_at = wks_at + wks.len;
    const nt_at = lm_at + lm.len;
    const total = nt_at + nt_len;

    const msg = try gpa.alloc(u8, total);
    @memset(msg, 0);
    @memcpy(msg[0..8], signature);
    std.mem.writeInt(u32, msg[8..12], 3, .little);
    putFields(msg, 12, lm.len, lm_at);
    putFields(msg, 20, nt_len, nt_at);
    putFields(msg, 28, dom.len, dom_at);
    putFields(msg, 36, usr.len, usr_at);
    putFields(msg, 44, wks.len, wks_at);
    putFields(msg, 52, 0, total);
    std.mem.writeInt(u32, msg[60..64], client_flags, .little);

    @memcpy(msg[dom_at..][0..dom.len], dom);
    @memcpy(msg[usr_at..][0..usr.len], usr);
    @memcpy(msg[wks_at..][0..wks.len], wks);
    @memcpy(msg[lm_at..][0..lm.len], &lm);
    @memcpy(msg[nt_at..][0..proof.len], &proof);
    @memcpy(msg[nt_at + proof.len ..][0..temp.len], temp);
    return msg;
}

/// The current time as a Windows FILETIME, for callers with nothing better to
/// pass to `authenticate`.
pub fn filetimeNow() u64 {
    // 11644473600 seconds separate the FILETIME epoch (1601-01-01) from the
    // Unix epoch, at 10^7 ticks per second.
    const ticks = @divFloor(std.time.nanoTimestamp(), 100) + 116_444_736_000_000_000;
    return if (ticks <= 0) 0 else @intCast(ticks);
}

const Challenge = struct {
    server_challenge: [8]u8,
    /// Borrowed from the caller's message.
    target_info: []const u8,
    timestamp: ?u64,
};

fn parseChallenge(msg: []const u8) Error!Challenge {
    if (msg.len < 32) return error.NtlmBadMessage;
    if (!std.mem.eql(u8, msg[0..8], signature)) return error.NtlmBadMessage;
    if (std.mem.readInt(u32, msg[8..12], .little) != 2) return error.NtlmBadMessage;

    var chal: Challenge = .{
        .server_challenge = msg[24..32].*,
        .target_info = &.{},
        .timestamp = null,
    };
    // TargetInfoFields sit at offset 40; a pre-NTLMv2 server may not send them
    // at all, in which case the NTLMv2 blob simply carries no server naming
    // context (MS-NLMP 2.2.1.2).
    if (msg.len >= 48) {
        const len = std.mem.readInt(u16, msg[40..42], .little);
        const off = std.mem.readInt(u32, msg[44..48], .little);
        if (len != 0) {
            if (off > msg.len or len > msg.len - off) return error.NtlmBadMessage;
            chal.target_info = msg[off..][0..len];
            chal.timestamp = avTimestamp(chal.target_info);
        }
    }
    return chal;
}

/// Walks the AV_PAIR list (MS-NLMP 2.2.2.1) for MsvAvTimestamp. A truncated
/// list is not an error: the bytes are copied verbatim either way, and only the
/// timestamp is being looked up.
fn avTimestamp(info: []const u8) ?u64 {
    var i: usize = 0;
    while (i + 4 <= info.len) {
        const id = std.mem.readInt(u16, info[i..][0..2], .little);
        const len = std.mem.readInt(u16, info[i + 2 ..][0..2], .little);
        i += 4;
        if (id == av_eol) return null;
        if (len > info.len - i) return null;
        if (id == av_timestamp and len == 8) return std.mem.readInt(u64, info[i..][0..8], .little);
        i += len;
    }
    return null;
}

/// NTOWFv2 (MS-NLMP 3.3.2): HMAC_MD5(MD4(UTF-16LE(password)),
/// UTF-16LE(uppercase(user) ++ domain)). The user is uppercased; the domain is
/// taken as given. Case folding is ASCII-only — Windows folds the full Unicode
/// range, so a non-ASCII user name whose case differs from the account's will
/// hash differently.
fn ntowfv2(gpa: std.mem.Allocator, cred: Credential) Error![16]u8 {
    const pass = try utf16le(gpa, cred.password);
    defer gpa.free(pass);
    const nt_hash = md4(pass);

    const upper = try std.ascii.allocUpperString(gpa, cred.user);
    defer gpa.free(upper);
    const ident = try std.mem.concat(gpa, u8, &.{ upper, cred.domain });
    defer gpa.free(ident);
    const ident16 = try utf16le(gpa, ident);
    defer gpa.free(ident16);

    var out: [16]u8 = undefined;
    std.crypto.auth.hmac.HmacMd5.create(&out, ident16, &nt_hash);
    return out;
}

fn utf16le(gpa: std.mem.Allocator, s: []const u8) Error![]u8 {
    const wide = std.unicode.utf8ToUtf16LeAlloc(gpa, s) catch |e| switch (e) {
        error.OutOfMemory => return error.OutOfMemory,
        error.InvalidUtf8 => return error.NtlmUnsupported,
    };
    defer gpa.free(wide);
    const out = try gpa.alloc(u8, wide.len * 2);
    for (wide, 0..) |c, i| std.mem.writeInt(u16, out[i * 2 ..][0..2], c, .little);
    return out;
}

/// One of the Len/MaxLen/BufferOffset triples every variable-length field in an
/// NTLM message is described by.
fn putFields(msg: []u8, at: usize, len: usize, off: usize) void {
    std.mem.writeInt(u16, msg[at..][0..2], @intCast(len), .little);
    std.mem.writeInt(u16, msg[at + 2 ..][0..2], @intCast(len), .little);
    std.mem.writeInt(u32, msg[at + 4 ..][0..4], @intCast(off), .little);
}

/// MD4 (RFC 1320), one-shot. Only reason it exists: the NT hash is MD4 of the
/// UTF-16LE password.
fn md4(msg: []const u8) [16]u8 {
    var state = [4]u32{ 0x67452301, 0xefcdab89, 0x98badcfe, 0x10325476 };

    var i: usize = 0;
    while (msg.len - i >= 64) : (i += 64) md4Block(&state, msg[i..][0..64]);

    var block: [64]u8 = undefined;
    @memset(&block, 0);
    const rest = msg.len - i;
    @memcpy(block[0..rest], msg[i..]);
    block[rest] = 0x80;
    if (rest >= 56) {
        md4Block(&state, &block);
        @memset(&block, 0);
    }
    std.mem.writeInt(u64, block[56..64], @as(u64, @intCast(msg.len)) *% 8, .little);
    md4Block(&state, &block);

    var out: [16]u8 = undefined;
    for (state, 0..) |w, k| std.mem.writeInt(u32, out[k * 4 ..][0..4], w, .little);
    return out;
}

fn md4Block(state: *[4]u32, block: *const [64]u8) void {
    var x: [16]u32 = undefined;
    for (&x, 0..) |*w, k| w.* = std.mem.readInt(u32, block[k * 4 ..][0..4], .little);

    var a = state[0];
    var b = state[1];
    var c = state[2];
    var d = state[3];

    // Each step assigns one register and rotates the roles, which is what the
    // RFC's [ABCD]/[DABC]/[CDAB]/[BCDA] operand ordering describes.
    const shift1 = [4]u5{ 3, 7, 11, 19 };
    for (0..16) |j| {
        const f = (b & c) | (~b & d);
        a = std.math.rotl(u32, a +% f +% x[j], shift1[j % 4]);
        const t = d;
        d = c;
        c = b;
        b = a;
        a = t;
    }

    const shift2 = [4]u5{ 3, 5, 9, 13 };
    const order2 = [16]u4{ 0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11, 15 };
    for (0..16) |j| {
        const g = (b & c) | (b & d) | (c & d);
        a = std.math.rotl(u32, a +% g +% x[order2[j]] +% 0x5a827999, shift2[j % 4]);
        const t = d;
        d = c;
        c = b;
        b = a;
        a = t;
    }

    const shift3 = [4]u5{ 3, 9, 11, 15 };
    const order3 = [16]u4{ 0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15 };
    for (0..16) |j| {
        const h = b ^ c ^ d;
        a = std.math.rotl(u32, a +% h +% x[order3[j]] +% 0x6ed9eba1, shift3[j % 4]);
        const t = d;
        d = c;
        c = b;
        b = a;
        a = t;
    }

    state[0] +%= a;
    state[1] +%= b;
    state[2] +%= c;
    state[3] +%= d;
}

fn expectHex(expected: []const u8, actual: []const u8) !void {
    var buf: [512]u8 = undefined;
    const want = try std.fmt.hexToBytes(buf[0 .. expected.len / 2], expected);
    try std.testing.expectEqualSlices(u8, want, actual);
}

fn expectMd4(expected: []const u8, msg: []const u8) !void {
    const got = md4(msg);
    try expectHex(expected, &got);
}

test "md4 matches the RFC 1320 A.5 test suite" {
    try expectMd4("31d6cfe0d16ae931b73c59d7e0c089c0", "");
    try expectMd4("bde52cb31de33e46245e05fbdbd6fb24", "a");
    try expectMd4("a448017aaf21d8525fc10ae87aa6729d", "abc");
    try expectMd4("d9130a8164549fe818874806e1c7014b", "message digest");
    try expectMd4("d79e1c308aa5bbcdeea8ed63df412da9", "abcdefghijklmnopqrstuvwxyz");
    try expectMd4(
        "043f8582f241db351ce627e153e7f0e4",
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
    );
    try expectMd4("e33b4ddc9c38f2199c3e7b164fcc0536", "1234567890" ** 8);
}

test "NTOWFv2 matches the MS-NLMP 4.2.4.1.1 worked example" {
    const cred = Credential{ .domain = "Domain", .user = "User", .password = "Password" };
    // MS-NLMP 4.2.2.1.2: NTOWFv1 is the bare NT hash, i.e. MD4 of the password.
    const pass = try utf16le(std.testing.allocator, cred.password);
    defer std.testing.allocator.free(pass);
    try expectMd4("a4f49c406510bdcab6824ee7c30fd852", pass);

    const key = try ntowfv2(std.testing.allocator, cred);
    try expectHex("0c868a403bfd7a93a3001ef22ef02e3f", &key);

    // Uppercasing applies to the user only; lower-casing the domain must change
    // the key, lower-casing the user must not.
    const same = try ntowfv2(std.testing.allocator, .{ .domain = "Domain", .user = "user", .password = "Password" });
    try std.testing.expectEqualSlices(u8, &key, &same);
    const other = try ntowfv2(std.testing.allocator, .{ .domain = "domain", .user = "User", .password = "Password" });
    try std.testing.expect(!std.mem.eql(u8, &key, &other));
}

/// A CHALLENGE_MESSAGE carrying the server challenge, target name and target
/// info of MS-NLMP 4.2.4: NetBIOS domain "Domain", NetBIOS computer "Server".
const example_challenge = "4e544c4d53535000" ++ // NTLMSSP\0
    "02000000" ++ // MessageType
    "0c000c0038000000" ++ // TargetNameFields: 12 bytes at 56
    "33828ae2" ++ // NegotiateFlags, as in 4.2.4
    "0123456789abcdef" ++ // ServerChallenge
    "0000000000000000" ++ // Reserved
    "2400240044000000" ++ // TargetInfoFields: 36 bytes at 68
    "0000000000000000" ++ // Version
    "530065007200760065007200" ++ // "Server"
    "02000c0044006f006d00610069006e00" ++ // MsvAvNbDomainName
    "01000c00530065007200760065007200" ++ // MsvAvNbComputerName
    "00000000"; // MsvAvEOL

test "authenticate reproduces the MS-NLMP 4.2.4 NTLMv2 and LMv2 responses" {
    const gpa = std.testing.allocator;
    var chal: [104]u8 = undefined;
    _ = try std.fmt.hexToBytes(&chal, example_challenge);

    const cred = Credential{
        .domain = "Domain",
        .user = "User",
        .password = "Password",
        .workstation = "COMPUTER",
    };
    const msg = try authenticate(gpa, cred, &chal, 0, [_]u8{0xaa} ** 8);
    defer gpa.free(msg);

    try std.testing.expectEqualStrings("NTLMSSP\x00", msg[0..8]);
    try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, msg[8..12], .little));

    const lm = field(msg, 12);
    const nt = field(msg, 20);
    try expectHex("86c35097ac9cec102554764a57cccc19" ++ "aaaaaaaaaaaaaaaa", lm);
    try expectHex(
        "68cd0ab851e51c96aabc927bebef6a1c" ++ // NTProofStr, 4.2.4.2.2
            "0101000000000000" ++ // RespType, HiRespType, Z(6)
            "0000000000000000" ++ // TimeStamp
            "aaaaaaaaaaaaaaaa" ++ // ChallengeFromClient
            "00000000" ++ // Z(4)
            "02000c0044006f006d00610069006e00" ++ // target info, verbatim
            "01000c00530065007200760065007200" ++
            "00000000" ++
            "00000000", // trailing Z(4)
        nt,
    );

    // Payload layout: the three names precede the two responses, and the
    // unused EncryptedRandomSessionKey points just past the message.
    try expectHex("44006f006d00610069006e00", field(msg, 28));
    try expectHex("5500730065007200", field(msg, 36));
    try expectHex("43004f004d0050005500540045005200", field(msg, 44));
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, msg[52..54], .little));
    try std.testing.expectEqual(@as(u32, @intCast(msg.len)), std.mem.readInt(u32, msg[56..60], .little));
    try std.testing.expectEqual(client_flags, std.mem.readInt(u32, msg[60..64], .little));
    try expectHex("0000000000000000", msg[64..72]);
    try std.testing.expectEqual(@as(usize, 72 + 12 + 8 + 16 + 24 + 84), msg.len);
}

test "authenticate: MsvAvTimestamp overrides the caller's clock" {
    const gpa = std.testing.allocator;
    // Same challenge, with an MsvAvTimestamp AV pair spliced in front of EOL.
    var chal: [116]u8 = undefined;
    _ = try std.fmt.hexToBytes(&chal, "4e544c4d53535000" ++ "02000000" ++
        "0c000c0038000000" ++ "33828ae2" ++ "0123456789abcdef" ++ "0000000000000000" ++
        "3000300044000000" ++ // TargetInfo is now 48 bytes
        "0000000000000000" ++ "530065007200760065007200" ++
        "02000c0044006f006d00610069006e00" ++
        "01000c00530065007200760065007200" ++
        "070008000102030405060708" ++ // MsvAvTimestamp
        "00000000");

    const cred = Credential{ .domain = "D", .user = "U", .password = "P" };
    const msg = try authenticate(gpa, cred, &chal, 0xdead, [_]u8{0xaa} ** 8);
    defer gpa.free(msg);
    // The blob's TimeStamp is at temp+8, i.e. 16+8 bytes into the response.
    try expectHex("0102030405060708", field(msg, 20)[24..32]);
}

test "negotiate is a 32-byte Type 1 with empty name fields" {
    const gpa = std.testing.allocator;
    const msg = try negotiate(gpa, .{ .domain = "Domain", .user = "User", .password = "Password" });
    defer gpa.free(msg);

    try std.testing.expectEqual(@as(usize, 32), msg.len);
    try std.testing.expectEqualStrings("NTLMSSP\x00", msg[0..8]);
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, msg[8..12], .little));
    try std.testing.expectEqual(client_flags, std.mem.readInt(u32, msg[12..16], .little));
    try std.testing.expectEqual(@as(u32, 0x00888205), client_flags);
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, msg[16..18], .little));
    try std.testing.expectEqual(@as(u32, 32), std.mem.readInt(u32, msg[20..24], .little));
    try std.testing.expectEqual(@as(u16, 0), std.mem.readInt(u16, msg[24..26], .little));
    try std.testing.expectEqual(@as(u32, 32), std.mem.readInt(u32, msg[28..32], .little));
}

test "a challenge that is not one is rejected, not misread" {
    const gpa = std.testing.allocator;
    const cred = Credential{ .domain = "D", .user = "U", .password = "P" };
    const nonce = [_]u8{0xaa} ** 8;

    var chal: [104]u8 = undefined;
    _ = try std.fmt.hexToBytes(&chal, example_challenge);

    try std.testing.expectError(Error.NtlmBadMessage, authenticate(gpa, cred, chal[0..31], 0, nonce));
    var bad_sig = chal;
    bad_sig[0] = 'X';
    try std.testing.expectError(Error.NtlmBadMessage, authenticate(gpa, cred, &bad_sig, 0, nonce));
    var bad_type = chal;
    bad_type[8] = 3;
    try std.testing.expectError(Error.NtlmBadMessage, authenticate(gpa, cred, &bad_type, 0, nonce));
    var past_end = chal;
    std.mem.writeInt(u32, past_end[44..48], 100, .little);
    try std.testing.expectError(Error.NtlmBadMessage, authenticate(gpa, cred, &past_end, 0, nonce));

    // No target info at all: still a valid NTLMv2 blob, just an empty one.
    var no_info = chal;
    std.mem.writeInt(u16, no_info[40..42], 0, .little);
    const msg = try authenticate(gpa, cred, &no_info, 0, nonce);
    defer gpa.free(msg);
    try std.testing.expectEqual(@as(usize, 16 + 32), field(msg, 20).len);
}

/// Resolve the payload a Len/MaxLen/BufferOffset triple at `at` points to.
fn field(msg: []const u8, at: usize) []const u8 {
    const len = std.mem.readInt(u16, msg[at..][0..2], .little);
    const off = std.mem.readInt(u32, msg[at + 4 ..][0..4], .little);
    return msg[off..][0..len];
}
