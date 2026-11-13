//! Certificate trust rules std's verification leaves out. `Certificate.Parsed.verify`
//! checks only names, dates and the signature, so any publicly trusted leaf could
//! sign a forged certificate for another host: every issuer a chain uses must
//! assert cA in its basicConstraints.

const std = @import("std");
const Certificate = std.crypto.Certificate;
const der = Certificate.der;

/// The basicConstraints extension, 2.5.29.19.
const basic_constraints_oid = [_]u8{ 0x55, 0x1D, 0x13 };

pub const Error = error{CertificateIssuerNotCa};

/// `subject` signed by `issuer`, which a chain may use only when it is a CA.
pub fn verifyIssuer(subject: Certificate.Parsed, issuer: Certificate.Parsed, now_sec: i64) (Error || Certificate.Parsed.VerifyError)!void {
    if (!isCa(issuer)) return error.CertificateIssuerNotCa;
    return subject.verify(issuer, now_sec);
}

/// Whether `p` asserts cA in a basicConstraints extension; a v1 certificate, one
/// without the extension, or one that cannot be read is no CA.
pub fn isCa(p: Certificate.Parsed) bool {
    return caFlag(p) catch false;
}

fn caFlag(p: Certificate.Parsed) !bool {
    if (p.version != .v3) return false;
    const bytes = p.certificate.buffer;
    const tbs_end = p.message_slice.end;
    var i = p.pub_key_slice.end;
    while (i < tbs_end) {
        const outer = try element(bytes, i, tbs_end);
        i = outer.slice.end;
        if (outer.identifier.class != .context_specific or outer.identifier.tag != .bitstring) continue;
        const exts = try element(bytes, outer.slice.start, outer.slice.end);
        var e = exts.slice.start;
        while (e < exts.slice.end) {
            const ext = try element(bytes, e, exts.slice.end);
            e = ext.slice.end;
            const oid = try element(bytes, ext.slice.start, ext.slice.end);
            if (!std.mem.eql(u8, bytes[oid.slice.start..oid.slice.end], &basic_constraints_oid)) continue;
            var value = try element(bytes, oid.slice.end, ext.slice.end);
            if (value.identifier.tag == .boolean) value = try element(bytes, value.slice.end, ext.slice.end);
            const seq = try element(bytes, value.slice.start, value.slice.end);
            if (seq.slice.start == seq.slice.end) return false;
            const ca = try element(bytes, seq.slice.start, seq.slice.end);
            return ca.identifier.tag == .boolean and ca.slice.end - ca.slice.start == 1 and bytes[ca.slice.start] != 0;
        }
        return false;
    }
    return false;
}

/// A DER element at `i` whose header and contents both end by `limit`.
fn element(bytes: []const u8, i: u32, limit: u32) !der.Element {
    if (limit > bytes.len or i + 2 > limit) return error.CertificateFieldHasInvalidLength;
    const size = bytes[i + 1];
    if (size >> 7 != 0 and i + 2 + (size & 0x7F) > limit) return error.CertificateFieldHasInvalidLength;
    const e = try der.Element.parse(bytes, i);
    if (e.slice.end < e.slice.start or e.slice.end > limit) return error.CertificateFieldHasInvalidLength;
    return e;
}

/// Adds to `bundle` each of `ders` that is a CA and verifies against a certificate
/// already there, repeating to a fixpoint so order does not matter; true if the
/// bundle grew. A leaf is never trusted as an anchor, however it chains.
pub fn grow(gpa: std.mem.Allocator, bundle: *Certificate.Bundle, ders: []const []const u8, now_sec: i64) bool {
    var used = std.bit_set.IntegerBitSet(64).initEmpty();
    var added = false;
    var progress = true;
    while (progress) {
        progress = false;
        for (ders[0..@min(ders.len, 64)], 0..) |d, i| {
            if (used.isSet(i)) continue;
            const parsed = (Certificate{ .buffer = d, .index = 0 }).parse() catch {
                used.set(i);
                continue;
            };
            if (!isCa(parsed)) {
                used.set(i);
                continue;
            }
            bundle.verify(parsed, now_sec) catch continue;
            const start: u32 = @intCast(bundle.bytes.items.len);
            bundle.bytes.appendSlice(gpa, d) catch return added;
            bundle.parseCert(gpa, start, now_sec) catch return added;
            used.set(i);
            progress = true;
            added = true;
        }
    }
    return added;
}

/// The roots a verifying connection trusts: the system's, and the PEM file
/// `extra` names (BASALT_CA_BUNDLE), a private CA's, added to them.
pub fn roots(gpa: std.mem.Allocator, bundle: *Certificate.Bundle, extra: ?[]const u8) !void {
    try bundle.rescan(gpa);
    if (extra) |path| try bundle.addCertsFromFilePath(gpa, std.fs.cwd(), path);
}

/// The PEM file BASALT_CA_BUNDLE names, or null when it is unset.
pub fn envBundle(gpa: std.mem.Allocator) ?[]const u8 {
    return std.process.getEnvVarOwned(gpa, "BASALT_CA_BUNDLE") catch null;
}

const testing = std.testing;
const fixtures = struct {
    const root = @embedFile("testdata/root.der");
    const inter = @embedFile("testdata/inter.der");
    const server = @embedFile("testdata/server.der");
    const leaf = @embedFile("testdata/leaf.der");
    const forged = @embedFile("testdata/forged.der");
};

fn parseDer(d: []const u8) !Certificate.Parsed {
    return (Certificate{ .buffer = d, .index = 0 }).parse();
}

fn bundleOf(gpa: std.mem.Allocator, d: []const u8) !Certificate.Bundle {
    var b: Certificate.Bundle = .{};
    try b.bytes.appendSlice(gpa, d);
    try b.parseCert(gpa, 0, std.time.timestamp());
    return b;
}

test "isCa reads basicConstraints: roots and intermediates are CAs, leaves are not" {
    try testing.expect(isCa(try parseDer(fixtures.root)));
    try testing.expect(isCa(try parseDer(fixtures.inter)));
    try testing.expect(!isCa(try parseDer(fixtures.server)));
    try testing.expect(!isCa(try parseDer(fixtures.leaf)));
    try testing.expect(!isCa(try parseDer(fixtures.forged)));
}

test "a leaf signed by a trusted leaf is refused as the chain's issuer, one signed by an intermediate CA is not" {
    const now = std.time.timestamp();
    const forged = try parseDer(fixtures.forged);
    const leaf = try parseDer(fixtures.leaf);
    try forged.verify(leaf, now);
    try testing.expectError(error.CertificateIssuerNotCa, verifyIssuer(forged, leaf, now));
    try verifyIssuer(try parseDer(fixtures.server), try parseDer(fixtures.inter), now);
}

test "grow trusts an intermediate CA that chains to the bundle, never a leaf" {
    const gpa = testing.allocator;
    var b = try bundleOf(gpa, fixtures.root);
    defer b.deinit(gpa);
    try testing.expect(!grow(gpa, &b, &.{ fixtures.forged, fixtures.leaf }, std.time.timestamp()));
    try testing.expectError(error.CertificateIssuerNotFound, b.verify(try parseDer(fixtures.forged), std.time.timestamp()));
    try testing.expect(grow(gpa, &b, &.{ fixtures.server, fixtures.inter }, std.time.timestamp()));
    try b.verify(try parseDer(fixtures.server), std.time.timestamp());
}

test "isCa refuses a basicConstraints whose lengths run past the certificate" {
    var bad = fixtures.inter.*;
    const at = std.mem.indexOf(u8, &bad, &basic_constraints_oid).?;
    bad[at + basic_constraints_oid.len + 4] = 0x7F;
    const p = parseDer(&bad) catch return;
    try testing.expect(!isCa(p));
}

test "roots adds a private CA's PEM to the system's, and an unreadable one is an error" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var pem = std.array_list.Managed(u8).init(testing.allocator);
    defer pem.deinit();
    try pem.appendSlice("-----BEGIN CERTIFICATE-----\n");
    const enc = std.base64.standard.Encoder;
    const b64 = try testing.allocator.alloc(u8, enc.calcSize(fixtures.root.len));
    defer testing.allocator.free(b64);
    _ = enc.encode(b64, fixtures.root);
    try pem.appendSlice(b64);
    try pem.appendSlice("\n-----END CERTIFICATE-----\n");
    try tmp.dir.writeFile(.{ .sub_path = "ca.pem", .data = pem.items });
    const path = try tmp.dir.realpathAlloc(testing.allocator, "ca.pem");
    defer testing.allocator.free(path);

    var system: Certificate.Bundle = .{};
    defer system.deinit(testing.allocator);
    try roots(testing.allocator, &system, null);
    var with: Certificate.Bundle = .{};
    defer with.deinit(testing.allocator);
    try roots(testing.allocator, &with, path);
    try testing.expectEqual(system.map.count() + 1, with.map.count());

    var bad: Certificate.Bundle = .{};
    defer bad.deinit(testing.allocator);
    try testing.expectError(error.FileNotFound, roots(testing.allocator, &bad, "/nonexistent/ca.pem"));
}
