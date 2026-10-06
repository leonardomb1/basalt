//! SPNEGO (RFC 4178), just what a client sends and reads: a NegTokenInit
//! offering NTLM or Kerberos with the first mechanism's token sent
//! optimistically, a NegTokenResp carrying the next NTLM leg, and the token
//! out of a server's reply. Shared by the SMB and SQL Server logins.
//!
//! `oid_ms_krb5` is the legacy Kerberos 5 OID that Windows lists beside the
//! standard one.

const std = @import("std");

const oid_spnego = [_]u8{ 0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02 };
pub const oid_ntlmssp = [_]u8{ 0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a };

fn derLen(out: *std.array_list.Managed(u8), n: usize) !void {
    if (n < 0x80) return out.append(@intCast(n));
    if (n < 0x100) return out.appendSlice(&[_]u8{ 0x81, @intCast(n) });
    return out.appendSlice(&[_]u8{ 0x82, @intCast(n >> 8), @intCast(n & 0xff) });
}

fn derWrap(gpa: std.mem.Allocator, tag: u8, inner: []const u8) ![]u8 {
    var out = std.array_list.Managed(u8).init(gpa);
    try out.append(tag);
    try derLen(&out, inner.len);
    try out.appendSlice(inner);
    return out.toOwnedSlice();
}

pub const oid_krb5 = [_]u8{ 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x02 };
pub const oid_ms_krb5 = [_]u8{ 0x06, 0x09, 0x2a, 0x86, 0x48, 0x82, 0xf7, 0x12, 0x01, 0x02, 0x02 };

/// GSS-API InitialContextToken with a NegTokenInit offering NTLMSSP only and
/// carrying its NEGOTIATE_MESSAGE.
pub fn init(gpa: std.mem.Allocator, ntlm_negotiate: []const u8) ![]u8 {
    return initWith(gpa, &.{&oid_ntlmssp}, ntlm_negotiate);
}

/// A NegTokenInit offering `mechs`, the first one's token optimistically sent.
pub fn initWith(gpa: std.mem.Allocator, mechs: []const []const u8, optimistic: []const u8) ![]u8 {
    const oids = try std.mem.concat(gpa, u8, mechs);
    defer gpa.free(oids);
    const mech_list = try derWrap(gpa, 0x30, oids);
    defer gpa.free(mech_list);
    const mech_types = try derWrap(gpa, 0xa0, mech_list);
    defer gpa.free(mech_types);
    const octets = try derWrap(gpa, 0x04, optimistic);
    defer gpa.free(octets);
    const mech_token = try derWrap(gpa, 0xa2, octets);
    defer gpa.free(mech_token);
    const seq_inner = try std.mem.concat(gpa, u8, &.{ mech_types, mech_token });
    defer gpa.free(seq_inner);
    const seq = try derWrap(gpa, 0x30, seq_inner);
    defer gpa.free(seq);
    const init_tok = try derWrap(gpa, 0xa0, seq);
    defer gpa.free(init_tok);
    const app_inner = try std.mem.concat(gpa, u8, &.{ &oid_spnego, init_tok });
    defer gpa.free(app_inner);
    return derWrap(gpa, 0x60, app_inner);
}

/// NegTokenResp carrying the AUTHENTICATE_MESSAGE.
pub fn response(gpa: std.mem.Allocator, ntlm_authenticate: []const u8) ![]u8 {
    const octets = try derWrap(gpa, 0x04, ntlm_authenticate);
    defer gpa.free(octets);
    const tok = try derWrap(gpa, 0xa2, octets);
    defer gpa.free(tok);
    const seq = try derWrap(gpa, 0x30, tok);
    defer gpa.free(seq);
    return derWrap(gpa, 0xa1, seq);
}

fn derAt(b: []const u8, at: usize) ?struct { tag: u8, body: []const u8, end: usize } {
    if (at + 2 > b.len) return null;
    var i = at + 1;
    var n: usize = b[i];
    i += 1;
    if (n & 0x80 != 0) {
        const k = n & 0x7f;
        if (k == 0 or k > 3 or i + k > b.len) return null;
        n = 0;
        for (b[i..][0..k]) |c| n = (n << 8) | c;
        i += k;
    }
    if (i + n > b.len) return null;
    return .{ .tag = b[at], .body = b[i..][0..n], .end = i + n };
}

/// The responseToken of a server's NegTokenResp: an NTLM CHALLENGE_MESSAGE or a
/// Kerberos AP-REP.
pub fn token(blob: []const u8) ?[]const u8 {
    const outer = derAt(blob, 0) orelse return null;
    if (outer.tag != 0xa1) return null;
    const seq = derAt(outer.body, 0) orelse return null;
    if (seq.tag != 0x30) return null;
    var at: usize = 0;
    while (derAt(seq.body, at)) |el| : (at = el.end) {
        if (el.tag == 0xa2) {
            const oct = derAt(el.body, 0) orelse return null;
            if (oct.tag != 0x04) return null;
            return oct.body;
        }
    }
    return null;
}

test "spnego: wraps an NTLM token and finds one in a reply" {
    const gpa = std.testing.allocator;
    const t = try response(gpa, "NTLMSSP\x00\x03");
    defer gpa.free(t);
    try std.testing.expectEqualStrings("NTLMSSP\x00\x03", token(t).?);
    const init_tok = try init(gpa, "NTLMSSP\x00\x01");
    defer gpa.free(init_tok);
    try std.testing.expectEqual(@as(u8, 0x60), init_tok[0]);
    try std.testing.expect(std.mem.indexOf(u8, init_tok, &oid_ntlmssp) != null);
    const big = try gpa.alloc(u8, 600);
    defer gpa.free(big);
    @memset(big, 'x');
    const tb = try response(gpa, big);
    defer gpa.free(tb);
    try std.testing.expectEqual(@as(usize, 600), token(tb).?.len);
}
