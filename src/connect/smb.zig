//! An SMB2/3 client, per [MS-SMB2]: enough of the protocol to read, list and
//! write files on a Windows share or a Samba server.
//!
//! Dialects 2.1 through 3.1.1 over direct TCP (445). Login is NTLMv2 inside
//! SPNEGO (`ntlm.zig`). Every request after login is signed and every signed
//! response checked — Windows 11 24H2 and Server 2025 require signing by
//! default, and a server that does not still accepts it: HMAC-SHA256 on 2.1,
//! AES-CMAC on 3.x with keys derived by the SP800-108 KDF, on 3.1.1 over the
//! SHA-512 hash of the negotiate and session setup exchange (pre-authentication
//! integrity). Reads and writes are pipelined within the credits the server
//! grants, matched back to their requests by message id.
//!
//! A session or share that requires encryption gets it: each message sealed in
//! an SMB2 TRANSFORM_HEADER with AES-128-GCM (3.1.1) or AES-128-CCM (3.0, and
//! 3.1.1 when the server picks it), keys derived as the signing key is.
//!
//! Not here: Kerberos, DFS referrals, oplocks and leases, multichannel.

const std = @import("std");
const ntlm = @import("ntlm.zig");

pub const Error = error{
    SmbProtocol,
    SmbLogonFailure,
    SmbAccessDenied,
    SmbNoSuchFile,
    SmbBadShare,
    SmbSharingViolation,
    SmbEncryptionRequired,
    SmbSignature,
    SmbUnsupported,
    SmbFailure,
    SmbDisconnected,
    NotSmbUrl,
};

pub const Config = struct {
    host: []const u8,
    port: u16 = 445,
    user: []const u8,
    password: []const u8,
    /// The account's domain; empty for a local account on the server.
    domain: []const u8 = "",
    /// The newest dialect to offer — 0x0311 unless a server mishandles it.
    max_dialect: u16 = dialect_311,
};

/// Why the last call on this thread failed, in words; never a secret.
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

// --- protocol constants (MS-SMB2 2.2) -----------------------------------------

const Command = enum(u16) {
    negotiate = 0,
    session_setup = 1,
    logoff = 2,
    tree_connect = 3,
    tree_disconnect = 4,
    create = 5,
    close = 6,
    read = 8,
    write = 9,
    echo = 0x0D,
    query_directory = 0x0E,
    set_info = 0x11,
};

const flag_server_to_redir: u32 = 0x1;
const flag_async: u32 = 0x2;
const flag_signed: u32 = 0x8;

const Status = struct {
    const success: u32 = 0;
    const pending: u32 = 0x0000_0103;
    const more_processing: u32 = 0xC000_0016;
    const no_more_files: u32 = 0x8000_0006;
    const end_of_file: u32 = 0xC000_0011;
    const logon_failure: u32 = 0xC000_006D;
    const account_restriction: u32 = 0xC000_006E;
    const password_expired: u32 = 0xC000_0071;
    const account_disabled: u32 = 0xC000_0072;
    const account_locked: u32 = 0xC000_0234;
    const access_denied: u32 = 0xC000_0022;
    const name_not_found: u32 = 0xC000_0034;
    const name_collision: u32 = 0xC000_0035;
    const path_not_found: u32 = 0xC000_003A;
    const sharing_violation: u32 = 0xC000_0043;
    const delete_pending: u32 = 0xC000_0056;
    const bad_network_name: u32 = 0xC000_00CC;
    const file_is_directory: u32 = 0xC000_00BA;
    const not_a_directory: u32 = 0xC000_0103;
    const disk_full: u32 = 0xC000_007F;
};

const dialect_210: u16 = 0x0210;
const dialect_300: u16 = 0x0300;
const dialect_302: u16 = 0x0302;
const dialect_311: u16 = 0x0311;

const signing_enabled: u16 = 0x1;
const signing_required: u16 = 0x2;
const cap_large_mtu: u32 = 0x4;
const cap_encryption: u32 = 0x40;

const share_flag_encrypt: u32 = 0x8000;
const cipher_ccm: u16 = 1;
const cipher_gcm: u16 = 2;

// access masks, share access, dispositions, options (MS-SMB2 2.2.13)
pub const Access = struct {
    pub const read_data: u32 = 0x1;
    pub const write_data: u32 = 0x2;
    pub const read_attributes: u32 = 0x80;
    pub const write_attributes: u32 = 0x100;
    pub const delete: u32 = 0x10000;
    pub const synchronize: u32 = 0x100000;
    pub const generic_read: u32 = 0x8000_0000;
    pub const generic_write: u32 = 0x4000_0000;
};
const share_all: u32 = 0x7; // read | write | delete
const disposition_open: u32 = 1;
const disposition_overwrite_if: u32 = 5;
const option_directory: u32 = 0x1;
const option_non_directory: u32 = 0x40;
const attr_directory: u32 = 0x10;

const header_len = 64;
/// The largest single read or write asked for: a megabyte, 16 credits.
const io_unit: u32 = 1 << 20;
/// Requests kept in flight on one handle.
const max_in_flight = 16;

// --- the session --------------------------------------------------------------

/// A response: its header fields and the whole message, owned.
const Msg = struct {
    status: u32,
    command: u16,
    flags: u32,
    session_id: u64,
    tree_id: u32,
    bytes: []u8,

    fn body(self: Msg) []const u8 {
        return self.bytes[header_len..];
    }
};

pub const Session = struct {
    gpa: std.mem.Allocator,
    stream: std.net.Stream,
    sr: std.net.Stream.Reader,
    sw: std.net.Stream.Writer,
    rbuf: [64 * 1024]u8 = undefined,
    wbuf: [64 * 1024]u8 = undefined,
    cfg: Config,

    dialect: u16 = 0,
    server_signing_required: bool = false,
    large_mtu: bool = false,
    max_read: u32 = 65536,
    max_write: u32 = 65536,
    /// The cipher 3.1.1 negotiated, 0 when none (for when encryption lands).
    cipher: u16 = 0,

    next_id: u64 = 0,
    credits: u32 = 1,
    session_id: u64 = 0,
    signing_key: [16]u8 = undefined,
    signing: bool = false,
    /// SHA-512 over the negotiate and session setup exchange (3.1.1).
    preauth: [64]u8 = [_]u8{0} ** 64,

    /// Sealing: the client-to-server and server-to-client keys, and whether the
    /// whole session is sealed or only the trees in `sealed_trees`.
    enc_key: [16]u8 = undefined,
    dec_key: [16]u8 = undefined,
    have_keys: bool = false,
    seal_all: bool = false,
    sealed_trees: [16]u32 = undefined,
    n_sealed: usize = 0,
    /// Nonces are a random prefix and a counter: never reused under one key.
    nonce_prefix: [4]u8 = undefined,
    nonce_ctr: u64 = 0,

    /// Responses read while waiting for another, by message id.
    held: std.AutoHashMap(u64, Msg),
    /// Trees connected on this session, by share name (lowercased).
    trees: std.StringHashMap(u32),

    pub fn connect(gpa: std.mem.Allocator, cfg: Config) !*Session {
        const self = try gpa.create(Session);
        errdefer gpa.destroy(self);
        const stream = std.net.tcpConnectToHost(gpa, cfg.host, cfg.port) catch |e|
            return fail(error.SmbDisconnected, "cannot reach {s}:{d}: {s}", .{ cfg.host, cfg.port, @errorName(e) });
        self.* = .{
            .gpa = gpa,
            .stream = stream,
            .sr = undefined,
            .sw = undefined,
            .cfg = cfg,
            .held = .init(gpa),
            .trees = .init(gpa),
        };
        self.sr = stream.reader(&self.rbuf);
        self.sw = stream.writer(&self.wbuf);
        errdefer self.release();
        try self.negotiate();
        try self.login();
        return self;
    }

    fn release(self: *Session) void {
        self.stream.close();
        var it = self.held.valueIterator();
        while (it.next()) |m| self.gpa.free(m.bytes);
        self.held.deinit();
        var tk = self.trees.keyIterator();
        while (tk.next()) |k| self.gpa.free(k.*);
        self.trees.deinit();
    }

    pub fn close(self: *Session) void {
        // a polite logoff; the server cleans up a dropped connection all the same
        if (self.session_id != 0) {
            var b: [4]u8 = undefined;
            std.mem.writeInt(u16, b[0..2], 4, .little);
            std.mem.writeInt(u16, b[2..4], 0, .little);
            if (self.send(.logoff, 0, &b, 1)) |id| {
                if (self.wait(id)) |m| self.gpa.free(m.bytes) else |_| {}
            } else |_| {}
        }
        const gpa = self.gpa;
        self.release();
        gpa.destroy(self);
    }

    // --- framing -----------------------------------------------------------------

    /// Send one request; returns its message id. `charge` is its credit charge.
    fn send(self: *Session, cmd: Command, tree_id: u32, body: []const u8, credit_charge: u16) Error!u64 {
        const msg = self.gpa.alloc(u8, header_len + body.len) catch return error.SmbFailure;
        defer self.gpa.free(msg);
        @memset(msg[0..header_len], 0);
        @memcpy(msg[0..4], "\xfeSMB");
        std.mem.writeInt(u16, msg[4..6], 64, .little);
        std.mem.writeInt(u16, msg[6..8], if (self.dialect == 0) 0 else credit_charge, .little);
        std.mem.writeInt(u16, msg[12..14], @intFromEnum(cmd), .little);
        // ask for plenty: credits bound how many requests may be in flight
        std.mem.writeInt(u16, msg[14..16], 256, .little);
        const id = self.next_id;
        self.next_id += @max(credit_charge, 1);
        std.mem.writeInt(u64, msg[24..32], id, .little);
        std.mem.writeInt(u32, msg[36..40], tree_id, .little);
        std.mem.writeInt(u64, msg[40..48], self.session_id, .little);
        @memcpy(msg[header_len..], body);
        const seal = self.sealed(cmd, tree_id);
        // a sealed message is authenticated by its cipher, and not also signed
        if (self.signing and cmd != .negotiate and !seal) {
            std.mem.writeInt(u32, msg[16..20], flag_signed, .little);
            const sig = self.sign(msg);
            @memcpy(msg[48..64], &sig);
        }
        if (cmd == .negotiate or cmd == .session_setup) self.preauthAdd(msg);
        self.credits -|= @max(credit_charge, 1);
        const w = &self.sw.interface;
        var nb: [4]u8 = undefined;
        if (seal) {
            var th: [52]u8 = undefined;
            const ct = self.gpa.alloc(u8, msg.len) catch return error.SmbFailure;
            defer self.gpa.free(ct);
            self.encrypt(&th, ct, msg);
            std.mem.writeInt(u32, &nb, @intCast(th.len + ct.len), .big);
            w.writeAll(&nb) catch return error.SmbDisconnected;
            w.writeAll(&th) catch return error.SmbDisconnected;
            w.writeAll(ct) catch return error.SmbDisconnected;
        } else {
            std.mem.writeInt(u32, &nb, @intCast(msg.len), .big);
            w.writeAll(&nb) catch return error.SmbDisconnected;
            w.writeAll(msg) catch return error.SmbDisconnected;
        }
        w.flush() catch return error.SmbDisconnected;
        return id;
    }

    /// Read frames until the response to `id` arrives; others are held for their
    /// own `wait`. An interim STATUS_PENDING is skipped: the final one follows.
    fn wait(self: *Session, id: u64) Error!Msg {
        if (self.held.fetchRemove(id)) |kv| return kv.value;
        while (true) {
            const r = self.sr.interface();
            const nb = r.takeArray(4) catch return error.SmbDisconnected;
            const len = std.mem.readInt(u32, nb, .big) & 0x00ff_ffff;
            if (len < header_len) return fail(error.SmbProtocol, "short frame from the server", .{});
            const frame = self.gpa.alloc(u8, len) catch return error.SmbFailure;
            r.readSliceAll(frame) catch {
                self.gpa.free(frame);
                return error.SmbDisconnected;
            };
            var was_sealed = false;
            const bytes = if (std.mem.eql(u8, frame[0..4], "\xfdSMB")) blk: {
                defer self.gpa.free(frame);
                was_sealed = true;
                break :blk try self.decrypt(frame);
            } else frame;
            errdefer self.gpa.free(bytes);
            if (bytes.len < header_len or !std.mem.eql(u8, bytes[0..4], "\xfeSMB"))
                return fail(error.SmbProtocol, "not an SMB2 message", .{});
            const m = Msg{
                .status = std.mem.readInt(u32, bytes[8..12], .little),
                .command = std.mem.readInt(u16, bytes[12..14], .little),
                .flags = std.mem.readInt(u32, bytes[16..20], .little),
                .session_id = std.mem.readInt(u64, bytes[40..48], .little),
                .tree_id = std.mem.readInt(u32, bytes[36..40], .little),
                .bytes = bytes,
            };
            self.credits += std.mem.readInt(u16, bytes[14..16], .little);
            const mid = std.mem.readInt(u64, bytes[24..32], .little);
            if (m.flags & flag_async != 0 and m.status == Status.pending) {
                self.gpa.free(bytes);
                continue;
            }
            if (m.flags & flag_signed != 0 and self.signing and !was_sealed) {
                var sig: [16]u8 = undefined;
                @memcpy(&sig, bytes[48..64]);
                @memset(bytes[48..64], 0);
                const want = self.sign(bytes);
                if (!std.crypto.timing_safe.eql([16]u8, sig, want))
                    return fail(error.SmbSignature, "a response's signature does not match — the session key is wrong, or the message was altered", .{});
                @memcpy(bytes[48..64], &sig);
            }
            if (mid == id) return m;
            self.held.put(mid, m) catch return error.SmbFailure;
        }
    }

    /// Send and wait for the response; its status is the caller's to judge.
    fn call(self: *Session, cmd: Command, tree_id: u32, body: []const u8, credit_charge: u16) Error!Msg {
        return self.wait(try self.send(cmd, tree_id, body, credit_charge));
    }

    fn sign(self: *Session, msg: []const u8) [16]u8 {
        var out: [16]u8 = undefined;
        if (self.dialect >= dialect_300) {
            std.crypto.auth.cmac.CmacAes128.create(&out, msg, &self.signing_key);
        } else {
            var full: [32]u8 = undefined;
            std.crypto.auth.hmac.sha2.HmacSha256.create(&full, msg, &self.signing_key);
            @memcpy(&out, full[0..16]);
        }
        return out;
    }

    fn preauthAdd(self: *Session, msg: []const u8) void {
        if (self.dialect != 0 and self.dialect != dialect_311) return;
        var h = std.crypto.hash.sha2.Sha512.init(.{});
        h.update(&self.preauth);
        h.update(msg);
        h.final(&self.preauth);
    }

    // --- negotiate and login -----------------------------------------------------

    fn negotiate(self: *Session) Error!void {
        const all = [_]u16{ dialect_210, dialect_300, dialect_302, dialect_311 };
        var n_dialects: usize = 0;
        for (all) |d| {
            if (d <= self.cfg.max_dialect) n_dialects += 1;
        }
        const dialects = all[0..n_dialects];
        const with_contexts = self.cfg.max_dialect >= dialect_311;
        var b = std.array_list.Managed(u8).init(self.gpa);
        defer b.deinit();
        var guid: [16]u8 = undefined;
        std.crypto.random.bytes(&guid);
        // StructureSize 36, DialectCount, SecurityMode, Reserved, Capabilities, ClientGuid
        put16(&b, 36);
        put16(&b, dialects.len);
        put16(&b, signing_enabled);
        put16(&b, 0);
        put32(&b, cap_large_mtu | cap_encryption);
        b.appendSlice(&guid) catch return error.SmbFailure;
        const ctx_offset_at = b.items.len;
        put32(&b, 0); // NegotiateContextOffset, filled below (ClientStartTime before 3.1.1)
        put16(&b, if (with_contexts) 2 else 0); // NegotiateContextCount
        put16(&b, 0);
        for (dialects) |d| put16(&b, d);
        if (!with_contexts) {
            const m = try self.call(.negotiate, 0, b.items, 0);
            return self.negotiated(m);
        }
        // negotiate contexts start 8-aligned, measured from the SMB2 header
        while ((header_len + b.items.len) % 8 != 0) b.append(0) catch return error.SmbFailure;
        std.mem.writeInt(u32, b.items[ctx_offset_at..][0..4], @intCast(header_len + b.items.len), .little);
        // SMB2_PREAUTH_INTEGRITY_CAPABILITIES: SHA-512 and a 32-byte salt
        var salt: [32]u8 = undefined;
        std.crypto.random.bytes(&salt);
        put16(&b, 1);
        put16(&b, 38);
        put32(&b, 0);
        put16(&b, 1);
        put16(&b, 32);
        put16(&b, 1);
        b.appendSlice(&salt) catch return error.SmbFailure;
        while ((header_len + b.items.len) % 8 != 0) b.append(0) catch return error.SmbFailure;
        // SMB2_ENCRYPTION_CAPABILITIES: AES-128-GCM, AES-128-CCM
        put16(&b, 2);
        put16(&b, 6);
        put32(&b, 0);
        put16(&b, 2);
        put16(&b, 2);
        put16(&b, 1);

        return self.negotiated(try self.call(.negotiate, 0, b.items, 0));
    }

    fn negotiated(self: *Session, m: Msg) Error!void {
        defer self.gpa.free(m.bytes);
        if (m.status != Status.success)
            return fail(error.SmbProtocol, "the server refused to negotiate (status 0x{x:0>8})", .{m.status});
        const r = m.body();
        if (r.len < 64) return fail(error.SmbProtocol, "short NEGOTIATE response", .{});
        const mode = std.mem.readInt(u16, r[2..4], .little);
        self.dialect = std.mem.readInt(u16, r[4..6], .little);
        const ctx_count = std.mem.readInt(u16, r[6..8], .little);
        const caps = std.mem.readInt(u32, r[24..28], .little);
        self.max_read = std.mem.readInt(u32, r[32..36], .little);
        self.max_write = std.mem.readInt(u32, r[36..40], .little);
        self.server_signing_required = mode & signing_required != 0;
        self.large_mtu = caps & cap_large_mtu != 0;
        switch (self.dialect) {
            dialect_210, dialect_300, dialect_302, dialect_311 => {},
            else => return fail(error.SmbUnsupported, "the server chose SMB dialect 0x{x:0>4}; basalt speaks 2.1 to 3.1.1", .{self.dialect}),
        }
        if (self.dialect == dialect_311) {
            // the hash so far covered our request; it covers the response too
            self.preauthAdd(m.bytes);
            const off = std.mem.readInt(u32, r[60..64], .little);
            var at: usize = off;
            var k: usize = 0;
            while (k < ctx_count and at + 8 <= m.bytes.len) : (k += 1) {
                const ty = std.mem.readInt(u16, m.bytes[at..][0..2], .little);
                const n = std.mem.readInt(u16, m.bytes[at + 2 ..][0..2], .little);
                const data = m.bytes[at + 8 ..][0..n];
                if (ty == 2 and n >= 4) self.cipher = std.mem.readInt(u16, data[2..4], .little);
                at = std.mem.alignForward(usize, at + 8 + n, 8);
            }
        }
    }

    fn login(self: *Session) Error!void {
        const cred = ntlm.Credential{ .domain = self.cfg.domain, .user = self.cfg.user, .password = self.cfg.password };
        const t1 = ntlm.negotiate(self.gpa, cred) catch return error.SmbFailure;
        defer self.gpa.free(t1);
        const init_tok = spnegoInit(self.gpa, t1) catch return error.SmbFailure;
        defer self.gpa.free(init_tok);

        const m1 = try self.sessionSetup(init_tok, null);
        defer self.gpa.free(m1.bytes);
        if (m1.status != Status.more_processing) return self.logonFailed(m1.status);
        self.session_id = m1.session_id;
        // only a response that is not the final one joins the hash
        self.preauthAdd(m1.bytes);
        const chal = spnegoToken(sessionBlob(m1)) orelse
            return fail(error.SmbProtocol, "the server's SPNEGO reply carries no NTLM challenge", .{});

        var nonce: [8]u8 = undefined;
        std.crypto.random.bytes(&nonce);
        const auth = ntlm.authenticateKeyed(self.gpa, cred, chal, ntlm.filetimeNow(), nonce) catch |e| switch (e) {
            error.OutOfMemory => return error.SmbFailure,
            else => return fail(error.SmbProtocol, "the server's NTLM challenge is malformed", .{}),
        };
        defer self.gpa.free(auth.msg);
        const resp_tok = spnegoResponse(self.gpa, auth.msg) catch return error.SmbFailure;
        defer self.gpa.free(resp_tok);

        const m2 = try self.sessionSetup(resp_tok, auth.session_key);
        defer self.gpa.free(m2.bytes);
        if (m2.status != Status.success) return self.logonFailed(m2.status);
        const flags = std.mem.readInt(u16, m2.body()[2..4], .little);
        if (flags & 0x3 != 0)
            return fail(error.SmbLogonFailure, "the server logged {s} in as a guest — check the user name and password", .{self.cfg.user});
        if (flags & 0x4 != 0) {
            if (!self.have_keys) return fail(error.SmbEncryptionRequired, "the server requires an encrypted session, which needs SMB 3 with a cipher it shares with basalt", .{});
            self.seal_all = true;
        }
    }

    /// One SESSION_SETUP round. With `key`, this is the last one: its response
    /// is signed, with a key that on 3.1.1 hashes this very request — so the key
    /// is derived between sending it and reading the answer.
    fn sessionSetup(self: *Session, token: []const u8, key: ?[16]u8) Error!Msg {
        var b = std.array_list.Managed(u8).init(self.gpa);
        defer b.deinit();
        put16(&b, 25);
        b.append(0) catch return error.SmbFailure; // Flags
        b.append(@intCast(signing_enabled)) catch return error.SmbFailure; // SecurityMode
        put32(&b, 0); // Capabilities
        put32(&b, 0); // Channel
        put16(&b, header_len + 24); // SecurityBufferOffset
        put16(&b, @intCast(token.len));
        put64(&b, 0); // PreviousSessionId
        b.appendSlice(token) catch return error.SmbFailure;
        self.signing = false;
        const id = try self.send(.session_setup, 0, b.items, 1);
        if (key) |k| {
            self.deriveSigningKey(k);
            self.signing = true;
        }
        const m = self.wait(id) catch |e| {
            self.signing = false;
            return e;
        };
        if (m.status != Status.success) self.signing = false;
        return m;
    }

    fn sessionBlob(m: Msg) []const u8 {
        const r = m.body();
        if (r.len < 8) return &.{};
        const off = std.mem.readInt(u16, r[4..6], .little);
        const n = std.mem.readInt(u16, r[6..8], .little);
        if (off + n > m.bytes.len) return &.{};
        return m.bytes[off..][0..n];
    }

    fn logonFailed(self: *Session, status: u32) Error {
        return switch (status) {
            Status.logon_failure => fail(error.SmbLogonFailure, "the server refused the user name or password for {s}", .{self.cfg.user}),
            Status.account_disabled => fail(error.SmbLogonFailure, "the account {s} is disabled", .{self.cfg.user}),
            Status.account_locked => fail(error.SmbLogonFailure, "the account {s} is locked out", .{self.cfg.user}),
            Status.password_expired => fail(error.SmbLogonFailure, "the password of {s} has expired", .{self.cfg.user}),
            Status.account_restriction => fail(error.SmbLogonFailure, "an account restriction refused {s} (an empty password, or a logon-hours or workstation rule)", .{self.cfg.user}),
            Status.access_denied => fail(error.SmbAccessDenied, "the server refused the session for {s}", .{self.cfg.user}),
            else => fail(error.SmbLogonFailure, "login failed (status 0x{x:0>8})", .{status}),
        };
    }

    fn deriveSigningKey(self: *Session, session_key: [16]u8) void {
        std.crypto.random.bytes(&self.nonce_prefix);
        switch (self.dialect) {
            dialect_210 => self.signing_key = session_key,
            dialect_300, dialect_302 => {
                self.signing_key = kdf(&session_key, "SMB2AESCMAC\x00", "SmbSign\x00");
                self.enc_key = kdf(&session_key, "SMB2AESCCM\x00", "ServerIn \x00");
                self.dec_key = kdf(&session_key, "SMB2AESCCM\x00", "ServerOut\x00");
                self.cipher = cipher_ccm;
                self.have_keys = true;
            },
            else => {
                self.signing_key = kdf(&session_key, "SMBSigningKey\x00", &self.preauth);
                self.enc_key = kdf(&session_key, "SMBC2SCipherKey\x00", &self.preauth);
                self.dec_key = kdf(&session_key, "SMBS2CCipherKey\x00", &self.preauth);
                self.have_keys = self.cipher == cipher_ccm or self.cipher == cipher_gcm;
            },
        }
    }

    /// Whether a request goes sealed: all of them once the session is, else
    /// those on a share that asked for it.
    fn sealed(self: *Session, cmd: Command, tree_id: u32) bool {
        if (cmd == .negotiate or cmd == .session_setup) return false;
        if (self.seal_all) return true;
        for (self.sealed_trees[0..self.n_sealed]) |t| if (t == tree_id) return true;
        return false;
    }

    /// Seal `msg` into `ct`, writing the TRANSFORM_HEADER to `th` (MS-SMB2 2.2.41).
    fn encrypt(self: *Session, th: *[52]u8, ct: []u8, msg: []const u8) void {
        @memset(th, 0);
        @memcpy(th[0..4], "\xfdSMB");
        @memcpy(th[20..24], &self.nonce_prefix);
        std.mem.writeInt(u64, th[24..32], self.nonce_ctr, .little);
        self.nonce_ctr += 1;
        std.mem.writeInt(u32, th[36..40], @intCast(msg.len), .little);
        std.mem.writeInt(u16, th[42..44], 1, .little); // Flags: encrypted
        std.mem.writeInt(u64, th[44..52], self.session_id, .little);
        const aad = th[20..52];
        if (self.cipher == cipher_gcm) {
            std.crypto.aead.aes_gcm.Aes128Gcm.encrypt(ct, th[4..20], msg, aad, th[20..32].*, self.enc_key);
        } else {
            ccmEncrypt(ct, th[4..20], msg, aad, th[20..31].*, self.enc_key);
        }
    }

    /// The SMB2 message inside a sealed frame, owned by the caller.
    fn decrypt(self: *Session, frame: []const u8) Error![]u8 {
        if (frame.len < 52 or !self.have_keys) return fail(error.SmbProtocol, "a sealed message the session has no key for", .{});
        const size = std.mem.readInt(u32, frame[36..40], .little);
        if (size != frame.len - 52) return fail(error.SmbProtocol, "a sealed message's size does not match its frame", .{});
        const out = self.gpa.alloc(u8, size) catch return error.SmbFailure;
        errdefer self.gpa.free(out);
        const aad = frame[20..52];
        const ok = if (self.cipher == cipher_gcm)
            std.crypto.aead.aes_gcm.Aes128Gcm.decrypt(out, frame[52..], frame[4..20].*, aad, frame[20..32].*, self.dec_key)
        else
            ccmDecrypt(out, frame[52..], frame[4..20].*, aad, frame[20..31].*, self.dec_key);
        ok catch return fail(error.SmbSignature, "a sealed response does not authenticate — the session key is wrong, or the message was altered", .{});
        return out;
    }

    // --- trees and files -----------------------------------------------------------

    /// The tree id of `share`, connecting it on first use.
    pub fn tree(self: *Session, share: []const u8) Error!u32 {
        var low_buf: [256]u8 = undefined;
        if (share.len > low_buf.len) return fail(error.SmbBadShare, "share name too long", .{});
        const low = std.ascii.lowerString(&low_buf, share);
        if (self.trees.get(low)) |t| return t;

        const unc = std.fmt.allocPrint(self.gpa, "\\\\{s}\\{s}", .{ self.cfg.host, share }) catch return error.SmbFailure;
        defer self.gpa.free(unc);
        const path = utf16(self.gpa, unc) catch return error.SmbFailure;
        defer self.gpa.free(path);
        var b = std.array_list.Managed(u8).init(self.gpa);
        defer b.deinit();
        put16(&b, 9);
        put16(&b, 0);
        put16(&b, header_len + 8);
        put16(&b, @intCast(path.len));
        b.appendSlice(path) catch return error.SmbFailure;
        const m = try self.call(.tree_connect, 0, b.items, 1);
        defer self.gpa.free(m.bytes);
        switch (m.status) {
            Status.success => {},
            Status.bad_network_name => return fail(error.SmbBadShare, "no share named {s} on {s}", .{ share, self.cfg.host }),
            // a share that requires encryption is denied outright to a session that
            // cannot seal, before its flags could say why
            Status.access_denied => return if (self.have_keys)
                fail(error.SmbAccessDenied, "{s} may not open the share {s}", .{ self.cfg.user, share })
            else
                fail(error.SmbAccessDenied, "{s} may not open the share {s} — or it requires encryption, which SMB {s} cannot do", .{ self.cfg.user, share, dialectName(self.dialect) }),
            else => return fail(error.SmbFailure, "opening the share {s} failed (status 0x{x:0>8})", .{ share, m.status }),
        }
        const flags = std.mem.readInt(u32, m.body()[4..8], .little);
        if (flags & share_flag_encrypt != 0 and !self.seal_all) {
            if (!self.have_keys)
                return fail(error.SmbEncryptionRequired, "the share {s} requires encryption, which needs SMB 3 with a cipher the server shares with basalt", .{share});
            if (self.n_sealed == self.sealed_trees.len) return fail(error.SmbFailure, "too many encrypted shares open on one session", .{});
            self.sealed_trees[self.n_sealed] = m.tree_id;
            self.n_sealed += 1;
        }
        self.trees.put(self.gpa.dupe(u8, low) catch return error.SmbFailure, m.tree_id) catch return error.SmbFailure;
        return m.tree_id;
    }

    pub const Handle = struct { tree: u32, id: [16]u8, size: u64, directory: bool };

    /// Open `path` (backslashes, relative to the share's root) in `share`.
    pub fn open(self: *Session, share: []const u8, path: []const u8, access: u32, disposition: u32, options: u32) Error!Handle {
        const t = try self.tree(share);
        const name = utf16(self.gpa, path) catch return error.SmbFailure;
        defer self.gpa.free(name);
        var b = std.array_list.Managed(u8).init(self.gpa);
        defer b.deinit();
        put16(&b, 57);
        b.append(0) catch return error.SmbFailure; // SecurityFlags
        b.append(0) catch return error.SmbFailure; // RequestedOplockLevel: none
        put32(&b, 2); // ImpersonationLevel: Impersonation
        put64(&b, 0); // SmbCreateFlags
        put64(&b, 0); // Reserved
        put32(&b, access);
        put32(&b, 0); // FileAttributes
        put32(&b, share_all);
        put32(&b, disposition);
        put32(&b, options);
        put16(&b, header_len + 56); // NameOffset
        put16(&b, @intCast(name.len));
        put32(&b, 0); // CreateContextsOffset
        put32(&b, 0);
        b.appendSlice(name) catch return error.SmbFailure;
        if (name.len == 0) b.append(0) catch return error.SmbFailure; // the buffer is never empty
        const m = try self.call(.create, t, b.items, 1);
        defer self.gpa.free(m.bytes);
        if (m.status != Status.success) return self.fileFailed(m.status, share, path);
        const r = m.body();
        var h = Handle{ .tree = t, .id = undefined, .size = std.mem.readInt(u64, r[48..56], .little), .directory = std.mem.readInt(u32, r[56..60], .little) & attr_directory != 0 };
        @memcpy(&h.id, r[64..80]);
        return h;
    }

    pub fn closeHandle(self: *Session, h: Handle) void {
        var b: [24]u8 = [_]u8{0} ** 24;
        std.mem.writeInt(u16, b[0..2], 24, .little);
        @memcpy(b[8..24], &h.id);
        const m = self.call(.close, h.tree, &b, 1) catch return;
        self.gpa.free(m.bytes);
    }

    fn fileFailed(self: *Session, status: u32, share: []const u8, path: []const u8) Error {
        _ = self;
        return switch (status) {
            Status.name_not_found, Status.path_not_found => fail(error.SmbNoSuchFile, "no file {s} in the share {s}", .{ path, share }),
            Status.access_denied => fail(error.SmbAccessDenied, "access to {s} in the share {s} is denied", .{ path, share }),
            Status.sharing_violation => fail(error.SmbSharingViolation, "{s} is open elsewhere in a way that excludes this access", .{path}),
            Status.delete_pending => fail(error.SmbNoSuchFile, "{s} is being deleted", .{path}),
            Status.file_is_directory => fail(error.SmbFailure, "{s} is a folder, not a file", .{path}),
            Status.not_a_directory => fail(error.SmbFailure, "a part of {s} is a file, not a folder", .{path}),
            Status.name_collision => fail(error.SmbFailure, "{s} already exists", .{path}),
            Status.disk_full => fail(error.SmbFailure, "the share {s} is full", .{share}),
            else => fail(error.SmbFailure, "{s} in the share {s}: status 0x{x:0>8}", .{ path, share, status }),
        };
    }

    /// The credit charge of an I/O of `n` bytes: one per 64 KiB, on 2.1 and up.
    fn charge(self: *Session, n: usize) u16 {
        if (!self.large_mtu) return 1;
        return @intCast(@max(1, (n + 65535) / 65536));
    }

    fn unit(self: *Session) u32 {
        const cap = if (self.large_mtu) io_unit else 65536;
        return @min(cap, @min(self.max_read, self.max_write));
    }

    fn sendRead(self: *Session, h: Handle, off: u64, len: u32) Error!u64 {
        var b: [49]u8 = [_]u8{0} ** 49;
        std.mem.writeInt(u16, b[0..2], 49, .little);
        b[2] = 0x50; // Padding: where the data lands in the response
        std.mem.writeInt(u32, b[4..8], len, .little);
        std.mem.writeInt(u64, b[8..16], off, .little);
        @memcpy(b[16..32], &h.id);
        return self.send(.read, h.tree, &b, self.charge(len));
    }

    /// `len` bytes of `h` from `off` into `out`, as many reads in flight as the
    /// credits allow. Returns how many bytes there were (fewer only at the end).
    pub fn readAt(self: *Session, h: Handle, off: u64, out: []u8) Error!usize {
        const u = self.unit();
        var ids: [max_in_flight]u64 = undefined;
        var at: [max_in_flight]usize = undefined;
        var sent: usize = 0;
        var head: usize = 0;
        var n_in: usize = 0;
        var got: usize = 0;
        var short = false;
        while (sent < out.len or n_in > 0) {
            while (!short and sent < out.len and n_in < max_in_flight and (n_in == 0 or self.credits >= self.charge(u))) {
                const len: u32 = @intCast(@min(u, out.len - sent));
                ids[(head + n_in) % max_in_flight] = try self.sendRead(h, off + sent, len);
                at[(head + n_in) % max_in_flight] = sent;
                sent += len;
                n_in += 1;
            }
            const m = try self.wait(ids[head]);
            defer self.gpa.free(m.bytes);
            const pos = at[head];
            head = (head + 1) % max_in_flight;
            n_in -= 1;
            if (m.status == Status.end_of_file) {
                short = true;
                continue;
            }
            if (m.status != Status.success) return fail(error.SmbFailure, "a read failed (status 0x{x:0>8})", .{m.status});
            const r = m.body();
            const data_off = r[2];
            const n = std.mem.readInt(u32, r[4..8], .little);
            if (data_off + n > m.bytes.len) return fail(error.SmbProtocol, "a read response overruns its message", .{});
            @memcpy(out[pos..][0..n], m.bytes[data_off..][0..n]);
            got = @max(got, pos + n);
            if (pos + n < out.len and n < @min(u, out.len - pos)) short = true;
        }
        return got;
    }

    /// Write `data` at `off`, as many writes in flight as the credits allow.
    pub fn writeAt(self: *Session, h: Handle, off: u64, data: []const u8) Error!void {
        const u = self.unit();
        var ids: [max_in_flight]u64 = undefined;
        var sent: usize = 0;
        var head: usize = 0;
        var n_in: usize = 0;
        var body = std.array_list.Managed(u8).init(self.gpa);
        defer body.deinit();
        while (sent < data.len or n_in > 0) {
            while (sent < data.len and n_in < max_in_flight and (n_in == 0 or self.credits >= self.charge(u))) {
                const len: u32 = @intCast(@min(u, data.len - sent));
                body.clearRetainingCapacity();
                put16(&body, 49);
                put16(&body, header_len + 48); // DataOffset
                put32(&body, len);
                put64(&body, off + sent);
                body.appendSlice(&h.id) catch return error.SmbFailure;
                put32(&body, 0); // Channel
                put32(&body, 0); // RemainingBytes
                put16(&body, 0);
                put16(&body, 0);
                put32(&body, 0); // Flags
                body.appendSlice(data[sent..][0..len]) catch return error.SmbFailure;
                ids[(head + n_in) % max_in_flight] = try self.send(.write, h.tree, body.items, self.charge(len));
                sent += len;
                n_in += 1;
            }
            const m = try self.wait(ids[head]);
            defer self.gpa.free(m.bytes);
            head = (head + 1) % max_in_flight;
            n_in -= 1;
            if (m.status != Status.success) {
                if (m.status == Status.disk_full) return fail(error.SmbFailure, "the share is full", .{});
                return fail(error.SmbFailure, "a write failed (status 0x{x:0>8})", .{m.status});
            }
        }
    }

    pub const Entry = struct { name: []const u8, directory: bool, size: u64 };

    /// The entries of the folder `h`, `.` and `..` left out.
    pub fn list(self: *Session, arena: std.mem.Allocator, h: Handle) Error![]Entry {
        var out = std.array_list.Managed(Entry).init(arena);
        var first = true;
        while (true) {
            var b: [34]u8 = [_]u8{0} ** 34;
            std.mem.writeInt(u16, b[0..2], 33, .little);
            b[2] = 0x01; // FileDirectoryInformation
            b[3] = if (first) 0x01 else 0; // RESTART_SCANS
            @memcpy(b[8..24], &h.id);
            std.mem.writeInt(u16, b[24..26], header_len + 32, .little);
            std.mem.writeInt(u16, b[26..28], 2, .little);
            std.mem.writeInt(u32, b[28..32], 65536, .little);
            b[32] = '*';
            first = false;
            const m = try self.call(.query_directory, h.tree, &b, 1);
            defer self.gpa.free(m.bytes);
            if (m.status == Status.no_more_files) break;
            if (m.status != Status.success) return fail(error.SmbFailure, "listing a folder failed (status 0x{x:0>8})", .{m.status});
            const r = m.body();
            const off = std.mem.readInt(u16, r[2..4], .little);
            const n = std.mem.readInt(u32, r[4..8], .little);
            if (off + n > m.bytes.len) return fail(error.SmbProtocol, "a folder listing overruns its message", .{});
            const buf = m.bytes[off..][0..n];
            var at: usize = 0;
            while (at + 64 <= buf.len) {
                const next = std.mem.readInt(u32, buf[at..][0..4], .little);
                const size = std.mem.readInt(u64, buf[at + 40 ..][0..8], .little);
                const attrs = std.mem.readInt(u32, buf[at + 56 ..][0..4], .little);
                const nlen = std.mem.readInt(u32, buf[at + 60 ..][0..4], .little);
                if (at + 64 + nlen > buf.len) break;
                const name = utf8(arena, buf[at + 64 ..][0..nlen]) catch return error.SmbFailure;
                if (!std.mem.eql(u8, name, ".") and !std.mem.eql(u8, name, ".."))
                    out.append(.{ .name = name, .directory = attrs & attr_directory != 0, .size = size }) catch return error.SmbFailure;
                if (next == 0) break;
                at += next;
            }
        }
        return out.toOwnedSlice() catch error.SmbFailure;
    }

    fn setInfo(self: *Session, h: Handle, class: u8, info: []const u8) Error!u32 {
        var b = std.array_list.Managed(u8).init(self.gpa);
        defer b.deinit();
        put16(&b, 33);
        b.append(1) catch return error.SmbFailure; // InfoType: file
        b.append(class) catch return error.SmbFailure;
        put32(&b, @intCast(info.len));
        put16(&b, header_len + 32);
        put16(&b, 0);
        put32(&b, 0);
        b.appendSlice(&h.id) catch return error.SmbFailure;
        b.appendSlice(info) catch return error.SmbFailure;
        const m = try self.call(.set_info, h.tree, b.items, 1);
        defer self.gpa.free(m.bytes);
        return m.status;
    }

    /// Rename the open file `h` to `to` (relative to the share's root), replacing
    /// any file there.
    pub fn rename(self: *Session, h: Handle, to: []const u8) Error!void {
        const name = utf16(self.gpa, to) catch return error.SmbFailure;
        defer self.gpa.free(name);
        var b = std.array_list.Managed(u8).init(self.gpa);
        defer b.deinit();
        b.append(1) catch return error.SmbFailure; // ReplaceIfExists
        b.appendNTimes(0, 7) catch return error.SmbFailure;
        put64(&b, 0); // RootDirectory
        put32(&b, @intCast(name.len));
        b.appendSlice(name) catch return error.SmbFailure;
        const st = try self.setInfo(h, 10, b.items); // FileRenameInformation
        if (st != Status.success) return fail(error.SmbFailure, "renaming to {s} failed (status 0x{x:0>8})", .{ to, st });
    }

    /// Whether the session still answers — a pooled one may have been dropped.
    pub fn echo(self: *Session) bool {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u16, b[0..2], 4, .little);
        std.mem.writeInt(u16, b[2..4], 0, .little);
        const m = self.call(.echo, 0, &b, 1) catch return false;
        defer self.gpa.free(m.bytes);
        return m.status == Status.success;
    }

    /// Mark the open file `h` for deletion when it is closed.
    pub fn deleteOnClose(self: *Session, h: Handle) Error!void {
        const st = try self.setInfo(h, 13, &[_]u8{1}); // FileDispositionInformation
        if (st != Status.success) return fail(error.SmbFailure, "deleting a file failed (status 0x{x:0>8})", .{st});
    }
};

// --- URLs, connections and the pool -------------------------------------------------

pub fn isUrl(path: []const u8) bool {
    return std.ascii.startsWithIgnoreCase(path, "smb://");
}

/// What a `CREATE CONNECTION … TYPE smb` resolved to, registered by the runtime
/// so a path's host part can name it.
pub const Conn = struct {
    host: []const u8,
    port: u16 = 445,
    user: ?[]const u8 = null,
    password: ?[]const u8 = null,
    domain: ?[]const u8 = null,
    /// When set, a path under the connection is inside this share:
    /// `smb://name/dir/x.csv` rather than `smb://name/share/dir/x.csv`.
    share: ?[]const u8 = null,
};

var registry_mtx: std.Thread.Mutex = .{};
var registry: std.StringHashMapUnmanaged(Conn) = .empty;

/// Make `smb://name/…` reach the server `c` describes. Process-wide, as the
/// pool is; a later registration of a name replaces the earlier one. Strings
/// are copied.
pub fn register(name: []const u8, c: Conn) !void {
    const gpa = std.heap.page_allocator;
    registry_mtx.lock();
    defer registry_mtx.unlock();
    var owned = c;
    owned.host = try gpa.dupe(u8, c.host);
    inline for (.{ "user", "password", "domain", "share" }) |f| {
        if (@field(c, f)) |v| @field(owned, f) = try gpa.dupe(u8, v);
    }
    try registry.put(gpa, try std.ascii.allocLowerString(gpa, name), owned);
}

/// A parsed `smb://` path: the session it needs, the share, and the file in it
/// (backslashes, from the share's root).
pub const Target = struct {
    cfg: Config,
    share: []const u8,
    path: []const u8,
};

/// `smb://[[domain;]user@]host[:port]/share/path`. The host may name a
/// registered connection, whose share — when it fixes one — the path is then
/// inside. Otherwise the user is the URL's, `SMB_USER` or `$USER`, the password
/// `SMB_PASSWORD`, the domain `SMB_DOMAIN`.
pub fn resolve(arena: std.mem.Allocator, url: []const u8) !Target {
    if (!isUrl(url)) return error.NotSmbUrl;
    const rest = url["smb://".len..];
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.NotSmbUrl;
    var auth = rest[0..slash];
    var path = std.mem.trimLeft(u8, rest[slash..], "/");
    var user: ?[]const u8 = null;
    var domain: ?[]const u8 = null;
    if (std.mem.lastIndexOfScalar(u8, auth, '@')) |at| {
        var u = auth[0..at];
        if (std.mem.indexOfScalar(u8, u, ';')) |semi| {
            domain = u[0..semi];
            u = u[semi + 1 ..];
        }
        user = u;
        auth = auth[at + 1 ..];
    }
    var host = auth;
    var port: ?u16 = null;
    if (std.mem.lastIndexOfScalar(u8, auth, ':')) |c| {
        host = auth[0..c];
        port = std.fmt.parseInt(u16, auth[c + 1 ..], 10) catch return error.NotSmbUrl;
    }
    if (host.len == 0) return error.NotSmbUrl;

    var low_buf: [256]u8 = undefined;
    const low = if (host.len <= low_buf.len) std.ascii.lowerString(&low_buf, host) else host;
    registry_mtx.lock();
    const named = registry.get(low);
    registry_mtx.unlock();

    var cfg: Config = undefined;
    var share: []const u8 = undefined;
    if (named) |c| {
        cfg = .{
            .host = c.host,
            .port = port orelse c.port,
            .user = user orelse c.user orelse std.posix.getenv("USER") orelse "",
            .password = c.password orelse "",
            .domain = domain orelse c.domain orelse "",
        };
        if (c.share) |sh| {
            share = sh;
        } else {
            share = try splitShare(&path);
        }
    } else {
        cfg = .{
            .host = host,
            .port = port orelse 445,
            .user = user orelse std.posix.getenv("SMB_USER") orelse std.posix.getenv("USER") orelse "",
            .password = std.posix.getenv("SMB_PASSWORD") orelse "",
            .domain = domain orelse std.posix.getenv("SMB_DOMAIN") orelse "",
        };
        share = try splitShare(&path);
    }
    const win = try arena.dupe(u8, path);
    std.mem.replaceScalar(u8, win, '/', '\\');
    return .{ .cfg = cfg, .share = share, .path = win };
}

fn splitShare(path: *[]const u8) ![]const u8 {
    const p = path.*;
    const e = std.mem.indexOfScalar(u8, p, '/') orelse p.len;
    if (e == 0) return error.NotSmbUrl;
    path.* = if (e < p.len) p[e + 1 ..] else "";
    return p[0..e];
}

const Pooled = struct { s: *Session, key: []const u8, idle_since: i64 };

var pool_mtx: std.Thread.Mutex = .{};
var pool: std.ArrayListUnmanaged(Pooled) = .empty;

/// A session is reused only for the same login: another password must not
/// inherit a session the first one opened.
fn poolKey(buf: []u8, cfg: Config) []const u8 {
    var h = std.hash.Wyhash.init(0);
    h.update(cfg.password);
    h.update(&[_]u8{0});
    h.update(cfg.domain);
    return std.fmt.bufPrint(buf, "{s}@{s}:{d}#{x}", .{ cfg.user, cfg.host, cfg.port, h.final() }) catch cfg.host;
}

/// A session to the server `cfg` names, the caller's alone until `checkin`.
pub fn checkout(cfg: Config) !*Session {
    why_len = 0;
    var kb: [512]u8 = undefined;
    const key = poolKey(&kb, cfg);
    while (takePooled(key)) |p| {
        // one idle a while may have been dropped by the server or a NAT
        if (std.time.milliTimestamp() - p.idle_since < 15_000 or p.s.echo()) {
            std.heap.page_allocator.free(p.key);
            return p.s;
        }
        std.heap.page_allocator.free(p.key);
        p.s.close();
    }
    return Session.connect(std.heap.page_allocator, cfg);
}

fn takePooled(key: []const u8) ?Pooled {
    pool_mtx.lock();
    defer pool_mtx.unlock();
    var i = pool.items.len;
    while (i > 0) {
        i -= 1;
        if (std.mem.eql(u8, pool.items[i].key, key)) return pool.swapRemove(i);
    }
    return null;
}

/// Hand a session back for the next opener; a broken one is closed instead.
pub fn checkin(s: *Session, healthy: bool) void {
    if (!healthy) {
        s.close();
        return;
    }
    var kb: [512]u8 = undefined;
    const key = std.heap.page_allocator.dupe(u8, poolKey(&kb, s.cfg)) catch {
        s.close();
        return;
    };
    pool_mtx.lock();
    defer pool_mtx.unlock();
    pool.append(std.heap.page_allocator, .{ .s = s, .key = key, .idle_since = std.time.milliTimestamp() }) catch {
        std.heap.page_allocator.free(key);
        s.close();
    };
}

/// A file or folder error keeps the session usable; a transport error does not.
fn isProtocolOk(e: anyerror) bool {
    return switch (e) {
        error.SmbNoSuchFile, error.SmbAccessDenied, error.SmbSharingViolation, error.SmbBadShare, error.SmbFailure, error.EndOfStream => true,
        else => false,
    };
}

// --- files ------------------------------------------------------------------------

const read_access = Access.read_data | Access.read_attributes | Access.synchronize;

/// An open remote file, its session checked out for as long as it is open.
pub const File = struct {
    session: *Session,
    handle: Session.Handle,
    size: u64,
    healthy: bool = true,

    pub fn open(arena: std.mem.Allocator, url: []const u8) !*File {
        const t = try resolve(arena, url);
        const s = try checkout(t.cfg);
        const h = s.open(t.share, t.path, read_access, disposition_open, option_non_directory) catch |e| {
            checkin(s, isProtocolOk(e));
            return e;
        };
        const self = try arena.create(File);
        self.* = .{ .session = s, .handle = h, .size = h.size };
        return self;
    }

    /// `len` bytes at `off`, owned by `arena`.
    pub fn read(self: *File, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        const buf = try arena.alloc(u8, len);
        const n = self.session.readAt(self.handle, off, buf) catch |e| {
            self.healthy = isProtocolOk(e);
            return e;
        };
        if (n != len) return error.EndOfStream;
        return buf;
    }

    pub fn close(self: *File) void {
        if (self.healthy) self.session.closeHandle(self.handle);
        checkin(self.session, self.healthy);
    }
};

/// A file read front to back as a `std.Io.Reader`, a megabyte-sized chunk at a
/// time with the reads behind it in flight together.
pub const Stream = struct {
    file: *File,
    at: u64 = 0,
    /// Where the stream stops: the file's end, or a window's.
    end: u64,
    /// Opened by `open`, closed with the stream; a window borrows its file.
    owns_file: bool,
    /// Fetched ahead, `chunk[pos..len]` not yet handed out.
    chunk: []u8,
    pos: usize = 0,
    len: usize = 0,
    interface: std.Io.Reader,

    const chunk_size = 4 << 20;

    pub fn open(arena: std.mem.Allocator, url: []const u8) !*Stream {
        const f = try File.open(arena, url);
        errdefer f.close();
        return make(arena, f, 0, f.size, true);
    }

    /// `len` bytes of an open file from `off` — a zip member inside a remote
    /// archive.
    pub fn window(arena: std.mem.Allocator, f: *File, off: u64, len: u64) !*Stream {
        return make(arena, f, off, off + len, false);
    }

    fn make(arena: std.mem.Allocator, f: *File, start: u64, end: u64, owns: bool) !*Stream {
        const self = try arena.create(Stream);
        self.* = .{
            .file = f,
            .at = start,
            .end = end,
            .owns_file = owns,
            .chunk = try arena.alloc(u8, chunk_size),
            .interface = .{
                .buffer = try arena.alloc(u8, 64 * 1024),
                .vtable = &.{ .stream = streamFn },
                .seek = 0,
                .end = 0,
            },
        };
        return self;
    }

    pub fn close(self: *Stream) void {
        if (self.owns_file) self.file.close();
    }

    fn streamFn(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Stream = @fieldParentPtr("interface", r);
        if (self.pos == self.len) {
            if (self.at >= self.end) return error.EndOfStream;
            const want: usize = @intCast(@min(@as(u64, self.chunk.len), self.end - self.at));
            const n = self.file.session.readAt(self.file.handle, self.at, self.chunk[0..want]) catch |e| {
                self.file.healthy = isProtocolOk(e);
                return error.ReadFailed;
            };
            if (n == 0) return error.EndOfStream;
            self.at += n;
            self.pos = 0;
            self.len = n;
        }
        const n = limit.minInt(self.len - self.pos);
        w.writeAll(self.chunk[self.pos..][0..n]) catch return error.WriteFailed;
        self.pos += n;
        return n;
    }
};

/// A file written front to back to `path.part`, renamed over `path` by `finish`
/// and deleted by `abort`.
pub const Upload = struct {
    session: *Session,
    handle: Session.Handle,
    path: []const u8,
    at: u64 = 0,
    interface: std.Io.Writer,
    last_status: ?anyerror = null,
    last_error: []const u8 = "",
    err_buf: [256]u8 = undefined,
    done: bool = false,

    pub fn open(arena: std.mem.Allocator, url: []const u8) !*Upload {
        const t = try resolve(arena, url);
        const s = try checkout(t.cfg);
        const part = try std.fmt.allocPrint(arena, "{s}.part", .{t.path});
        // DELETE: the handle that wrote the `.part` also renames it or deletes it
        const h = s.open(t.share, part, Access.write_data | Access.read_data | Access.delete | Access.synchronize, disposition_overwrite_if, option_non_directory) catch |e| {
            checkin(s, isProtocolOk(e));
            return e;
        };
        const self = try arena.create(Upload);
        self.* = .{
            .session = s,
            .handle = h,
            .path = t.path,
            .interface = .{ .buffer = try arena.alloc(u8, 4 << 20), .vtable = &.{ .drain = drainFn } },
        };
        return self;
    }

    fn put(self: *Upload, bytes: []const u8) std.Io.Writer.Error!usize {
        if (bytes.len == 0) return 0;
        self.session.writeAt(self.handle, self.at, bytes) catch |e| {
            self.fail(e);
            return error.WriteFailed;
        };
        self.at += bytes.len;
        return bytes.len;
    }

    fn drainFn(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *Upload = @fieldParentPtr("interface", w);
        var total: usize = 0;
        total += try self.put(w.buffered());
        for (data[0 .. data.len - 1]) |d| total += try self.put(d);
        const last = data[data.len - 1];
        for (0..splat) |_| total += try self.put(last);
        return w.consume(total);
    }

    fn fail(self: *Upload, e: anyerror) void {
        self.last_status = e;
        const why = lastError();
        const n = @min(why.len, self.err_buf.len);
        @memcpy(self.err_buf[0..n], why[0..n]);
        self.last_error = self.err_buf[0..n];
    }

    /// Write what is buffered and rename `path.part` over `path`. On any failure
    /// the `.part` is deleted, as by `abort`.
    pub fn finish(self: *Upload) !void {
        if (self.done) return;
        self.interface.flush() catch |e| {
            const err = self.last_status orelse e;
            self.abort();
            return err;
        };
        self.session.rename(self.handle, self.path) catch |e| {
            self.fail(e);
            self.abort();
            return e;
        };
        self.session.closeHandle(self.handle);
        self.release();
    }

    /// Drop the upload: the `.part` is deleted, `path` untouched.
    pub fn abort(self: *Upload) void {
        if (self.done) return;
        self.session.deleteOnClose(self.handle) catch {};
        self.session.closeHandle(self.handle);
        self.release();
    }

    fn release(self: *Upload) void {
        self.done = true;
        checkin(self.session, if (self.last_status) |e| isProtocolOk(e) else true);
    }
};

/// The regular files under `url` (ending in `/`), subfolders included but not
/// those starting `_` or `.`, as `smb://` URLs sorted by name.
pub fn listPrefix(arena: std.mem.Allocator, url: []const u8) ![]const []const u8 {
    const t = try resolve(arena, url);
    const s = try checkout(t.cfg);
    var healthy = true;
    defer checkin(s, healthy);
    const top = std.mem.trimRight(u8, t.path, "\\");
    var names = std.array_list.Managed([]const u8).init(arena);
    // folders still to list, relative to `top` with `/`; "" is `top` itself
    var todo = std.array_list.Managed([]const u8).init(arena);
    try todo.append("");
    while (todo.pop()) |rel| {
        const dir = if (rel.len == 0) top else if (top.len == 0) try winPath(arena, rel) else try std.fmt.allocPrint(arena, "{s}\\{s}", .{ top, try winPath(arena, rel) });
        const h = s.open(t.share, dir, read_access, disposition_open, option_directory) catch |e| {
            healthy = isProtocolOk(e);
            return e;
        };
        const entries = s.list(arena, h) catch |e| {
            healthy = isProtocolOk(e);
            s.closeHandle(h);
            return e;
        };
        s.closeHandle(h);
        for (entries) |en| {
            const sub = if (rel.len == 0) en.name else try std.fmt.allocPrint(arena, "{s}/{s}", .{ rel, en.name });
            if (en.directory) {
                if (en.name[0] != '_' and en.name[0] != '.') try todo.append(sub);
            } else try names.append(sub);
        }
        if (names.items.len > 1_000_000) return error.FolderTooLarge;
    }
    std.mem.sort([]const u8, names.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);
    const base = url[0 .. url.len - (if (std.mem.endsWith(u8, url, "/")) @as(usize, 1) else 0)];
    const out = try arena.alloc([]const u8, names.items.len);
    for (names.items, out) |n, *o| o.* = try std.fmt.allocPrint(arena, "{s}/{s}", .{ base, n });
    return out;
}

fn winPath(arena: std.mem.Allocator, p: []const u8) ![]const u8 {
    const w = try arena.dupe(u8, p);
    std.mem.replaceScalar(u8, w, '/', '\\');
    return w;
}

// --- AES-128-CCM (RFC 3610), the cipher of SMB 3.0 ------------------------------------
// std has GCM but no CCM. SMB's: an 11-byte nonce, so a 4-byte length field, and a
// 16-byte tag.

const Aes = std.crypto.core.aes.Aes128;

fn ccmCtrBlock(nonce: [11]u8, i: u32) [16]u8 {
    var a: [16]u8 = undefined;
    a[0] = 3; // L - 1
    @memcpy(a[1..12], &nonce);
    std.mem.writeInt(u32, a[12..16], i, .big);
    return a;
}

fn ccmMac(ctx: anytype, msg: []const u8, aad: []const u8, nonce: [11]u8) [16]u8 {
    var x: [16]u8 = undefined;
    var b0: [16]u8 = undefined;
    b0[0] = 0x40 | (((16 - 2) / 2) << 3) | 3; // Adata, M = 16, L = 4
    @memcpy(b0[1..12], &nonce);
    std.mem.writeInt(u32, b0[12..16], @intCast(msg.len), .big);
    ctx.encrypt(&x, &b0);
    // the associated data, its 2-byte length first, zero-padded to blocks
    var blk: [16]u8 = [_]u8{0} ** 16;
    std.mem.writeInt(u16, blk[0..2], @intCast(aad.len), .big);
    var fill: usize = 2;
    for (aad) |c| {
        blk[fill] = c;
        fill += 1;
        if (fill == 16) {
            for (&x, blk) |*d, s| d.* ^= s;
            ctx.encrypt(&x, &x);
            blk = [_]u8{0} ** 16;
            fill = 0;
        }
    }
    if (fill > 0) {
        for (&x, blk) |*d, s| d.* ^= s;
        ctx.encrypt(&x, &x);
    }
    var at: usize = 0;
    while (at < msg.len) : (at += 16) {
        const n = @min(16, msg.len - at);
        for (x[0..n], msg[at..][0..n]) |*d, s| d.* ^= s;
        ctx.encrypt(&x, &x);
    }
    return x;
}

fn ccmCtr(ctx: anytype, out: []u8, in: []const u8, nonce: [11]u8) void {
    var at: usize = 0;
    var i: u32 = 1;
    while (at < in.len) : ({
        at += 16;
        i += 1;
    }) {
        var ks: [16]u8 = undefined;
        ctx.encrypt(&ks, &ccmCtrBlock(nonce, i));
        const n = @min(16, in.len - at);
        for (out[at..][0..n], in[at..][0..n], ks[0..n]) |*o, p, k| o.* = p ^ k;
    }
}

pub fn ccmEncrypt(ct: []u8, tag: *[16]u8, msg: []const u8, aad: []const u8, nonce: [11]u8, key: [16]u8) void {
    const ctx = Aes.initEnc(key);
    const mac = ccmMac(ctx, msg, aad, nonce);
    var s0: [16]u8 = undefined;
    ctx.encrypt(&s0, &ccmCtrBlock(nonce, 0));
    for (tag, mac, s0) |*t, m, k| t.* = m ^ k;
    ccmCtr(ctx, ct, msg, nonce);
}

pub fn ccmDecrypt(msg: []u8, ct: []const u8, tag: [16]u8, aad: []const u8, nonce: [11]u8, key: [16]u8) error{AuthenticationFailed}!void {
    const ctx = Aes.initEnc(key);
    ccmCtr(ctx, msg, ct, nonce);
    const mac = ccmMac(ctx, msg, aad, nonce);
    var s0: [16]u8 = undefined;
    ctx.encrypt(&s0, &ccmCtrBlock(nonce, 0));
    var want: [16]u8 = undefined;
    for (&want, mac, s0) |*t, m, k| t.* = m ^ k;
    if (!std.crypto.timing_safe.eql([16]u8, want, tag)) {
        @memset(msg, 0);
        return error.AuthenticationFailed;
    }
}

fn dialectName(d: u16) []const u8 {
    return switch (d) {
        dialect_210 => "2.1",
        dialect_300 => "3.0",
        dialect_302 => "3.0.2",
        dialect_311 => "3.1.1",
        else => "?",
    };
}

// --- SP800-108 KDF (MS-SMB2 3.1.4.2) ------------------------------------------------

/// HMAC-SHA256 in counter mode, one block, 128 bits out.
pub fn kdf(key: []const u8, label: []const u8, context: []const u8) [16]u8 {
    var mac = std.crypto.auth.hmac.sha2.HmacSha256.init(key);
    mac.update(&[_]u8{ 0, 0, 0, 1 });
    mac.update(label);
    mac.update(&[_]u8{0});
    mac.update(context);
    mac.update(&[_]u8{ 0, 0, 0, 128 });
    var full: [32]u8 = undefined;
    mac.final(&full);
    return full[0..16].*;
}

// --- SPNEGO (RFC 4178), just what an NTLM-only client sends and reads -------------------

const oid_spnego = [_]u8{ 0x06, 0x06, 0x2b, 0x06, 0x01, 0x05, 0x05, 0x02 };
const oid_ntlmssp = [_]u8{ 0x06, 0x0a, 0x2b, 0x06, 0x01, 0x04, 0x01, 0x82, 0x37, 0x02, 0x02, 0x0a };

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

/// GSS-API InitialContextToken with a NegTokenInit offering NTLMSSP only and
/// carrying its NEGOTIATE_MESSAGE.
pub fn spnegoInit(gpa: std.mem.Allocator, ntlm_negotiate: []const u8) ![]u8 {
    const mech_list = try derWrap(gpa, 0x30, &oid_ntlmssp);
    defer gpa.free(mech_list);
    const mech_types = try derWrap(gpa, 0xa0, mech_list);
    defer gpa.free(mech_types);
    const octets = try derWrap(gpa, 0x04, ntlm_negotiate);
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
pub fn spnegoResponse(gpa: std.mem.Allocator, ntlm_authenticate: []const u8) ![]u8 {
    const octets = try derWrap(gpa, 0x04, ntlm_authenticate);
    defer gpa.free(octets);
    const tok = try derWrap(gpa, 0xa2, octets);
    defer gpa.free(tok);
    const seq = try derWrap(gpa, 0x30, tok);
    defer gpa.free(seq);
    return derWrap(gpa, 0xa1, seq);
}

/// A DER element at `at`: its tag, and its contents as a slice.
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

/// The responseToken of a server's NegTokenResp: the NTLM CHALLENGE_MESSAGE.
pub fn spnegoToken(blob: []const u8) ?[]const u8 {
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

// --- little helpers ---------------------------------------------------------------

fn put16(b: *std.array_list.Managed(u8), v: u64) void {
    var x: [2]u8 = undefined;
    std.mem.writeInt(u16, &x, @intCast(v), .little);
    b.appendSlice(&x) catch {};
}

fn put32(b: *std.array_list.Managed(u8), v: u64) void {
    var x: [4]u8 = undefined;
    std.mem.writeInt(u32, &x, @intCast(v), .little);
    b.appendSlice(&x) catch {};
}

fn put64(b: *std.array_list.Managed(u8), v: u64) void {
    var x: [8]u8 = undefined;
    std.mem.writeInt(u64, &x, @intCast(v), .little);
    b.appendSlice(&x) catch {};
}

fn utf16(gpa: std.mem.Allocator, s: []const u8) ![]u8 {
    const w = try std.unicode.utf8ToUtf16LeAlloc(gpa, s);
    defer gpa.free(w);
    const out = try gpa.alloc(u8, w.len * 2);
    for (w, 0..) |c, i| std.mem.writeInt(u16, out[i * 2 ..][0..2], c, .little);
    return out;
}

fn utf8(arena: std.mem.Allocator, le: []const u8) ![]const u8 {
    const w = try arena.alloc(u16, le.len / 2);
    for (w, 0..) |*c, i| c.* = std.mem.readInt(u16, le[i * 2 ..][0..2], .little);
    return std.unicode.utf16LeToUtf8Alloc(arena, w);
}

// --- tests ------------------------------------------------------------------------

test "smb: SPNEGO wraps an NTLM token and finds one in a reply" {
    const gpa = std.testing.allocator;
    const t = try spnegoResponse(gpa, "NTLMSSP\x00\x03");
    defer gpa.free(t);
    // a client NegTokenResp has the same shape as a server's: the token comes back out
    try std.testing.expectEqualStrings("NTLMSSP\x00\x03", spnegoToken(t).?);
    const init_tok = try spnegoInit(gpa, "NTLMSSP\x00\x01");
    defer gpa.free(init_tok);
    try std.testing.expectEqual(@as(u8, 0x60), init_tok[0]);
    try std.testing.expect(std.mem.indexOf(u8, init_tok, &oid_ntlmssp) != null);
    // long lengths: a token over 255 bytes takes the two-byte form
    const big = try gpa.alloc(u8, 600);
    defer gpa.free(big);
    @memset(big, 'x');
    const tb = try spnegoResponse(gpa, big);
    defer gpa.free(tb);
    try std.testing.expectEqual(@as(usize, 600), spnegoToken(tb).?.len);
}

test "smb: AES-128-CCM seals and opens, and refuses a tampered message" {
    const key = [_]u8{0x40} ** 16;
    const nonce = [_]u8{0x10} ** 11;
    const aad = "transform header bytes 20 to 52";
    // lengths around the block size, and one with a partial last block
    for ([_]usize{ 0, 1, 15, 16, 17, 100, 4096 + 3 }) |n| {
        const gpa = std.testing.allocator;
        const msg = try gpa.alloc(u8, n);
        defer gpa.free(msg);
        for (msg, 0..) |*c, i| c.* = @truncate(i * 7 + 3);
        const ct = try gpa.alloc(u8, n);
        defer gpa.free(ct);
        var tag: [16]u8 = undefined;
        ccmEncrypt(ct, &tag, msg, aad, nonce, key);
        const back = try gpa.alloc(u8, n);
        defer gpa.free(back);
        try ccmDecrypt(back, ct, tag, aad, nonce, key);
        try std.testing.expectEqualSlices(u8, msg, back);
        if (n > 0) {
            ct[n / 2] ^= 1;
            try std.testing.expectError(error.AuthenticationFailed, ccmDecrypt(back, ct, tag, aad, nonce, key));
            ct[n / 2] ^= 1;
        }
        var bad_tag = tag;
        bad_tag[0] ^= 1;
        try std.testing.expectError(error.AuthenticationFailed, ccmDecrypt(back, ct, bad_tag, aad, nonce, key));
    }
}

test "smb URLs: share, domain, port, registered names" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const t = try resolve(a, "smb://CORP;ana@files.example.com:4445/finance/in/x.csv");
    try std.testing.expectEqualStrings("files.example.com", t.cfg.host);
    try std.testing.expectEqual(@as(u16, 4445), t.cfg.port);
    try std.testing.expectEqualStrings("ana", t.cfg.user);
    try std.testing.expectEqualStrings("CORP", t.cfg.domain);
    try std.testing.expectEqualStrings("finance", t.share);
    try std.testing.expectEqualStrings("in\\x.csv", t.path);
    try std.testing.expectEqualStrings("", (try resolve(a, "smb://h/share/")).path);
    try std.testing.expectError(error.NotSmbUrl, resolve(a, "smb://h/"));
    try std.testing.expectError(error.NotSmbUrl, resolve(a, "smb://nopath"));
    // a registered name, matched without regard to case; with a fixed share the
    // whole path is inside it
    try register("FS_T", .{ .host = "fs.corp.example", .user = "svc", .password = "p", .domain = "CORP", .share = "data" });
    const n = try resolve(a, "smb://fs_t/2026/out.parquet");
    try std.testing.expectEqualStrings("fs.corp.example", n.cfg.host);
    try std.testing.expectEqualStrings("svc", n.cfg.user);
    try std.testing.expectEqualStrings("data", n.share);
    try std.testing.expectEqualStrings("2026\\out.parquet", n.path);
}

test "smb: a live login, share, listing and read (BASALT_SMB_TEST_PORT)" {
    const port_s = std.posix.getenv("BASALT_SMB_TEST_PORT") orelse return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    // every dialect, so each signing scheme is checked against a server that
    // refuses a bad signature: HMAC-SHA256 (2.1), CMAC with the 3.0 key
    // derivation (3.0.2), and CMAC over the pre-authentication hash (3.1.1)
    for ([_]u16{ dialect_210, dialect_302, dialect_311 }) |d| {
        const s = try Session.connect(gpa, .{ .host = "127.0.0.1", .port = try std.fmt.parseInt(u16, port_s, 10), .user = "basalt", .password = "it", .max_dialect = d });
        defer s.close();
        try std.testing.expectEqual(d, s.dialect);
        try liveRoundTrip(s, ar.allocator());
    }
    // the `secure` share requires encryption: sealed with CCM on 3.0.2 and GCM on
    // 3.1.1 — Samba refuses any request on it that is not — and refused by name on
    // 2.1, which has no encryption
    for ([_]u16{ dialect_302, dialect_311 }) |d| {
        const s = try Session.connect(gpa, .{ .host = "127.0.0.1", .port = try std.fmt.parseInt(u16, port_s, 10), .user = "basalt", .password = "it", .max_dialect = d });
        defer s.close();
        try liveRoundTripOn(s, ar.allocator(), "secure");
        try std.testing.expectEqual(if (d == dialect_311) cipher_gcm else cipher_ccm, s.cipher);
    }
    {
        const s = try Session.connect(gpa, .{ .host = "127.0.0.1", .port = try std.fmt.parseInt(u16, port_s, 10), .user = "basalt", .password = "it", .max_dialect = dialect_210 });
        defer s.close();
        try std.testing.expectError(error.SmbAccessDenied, s.tree("secure"));
        try std.testing.expect(std.mem.indexOf(u8, lastError(), "requires encryption") != null);
    }
    // a wrong password is refused by name, not as a protocol error
    try std.testing.expectError(error.SmbLogonFailure, Session.connect(gpa, .{ .host = "127.0.0.1", .port = try std.fmt.parseInt(u16, port_s, 10), .user = "basalt", .password = "wrong" }));
}

fn liveRoundTrip(s: *Session, arena: std.mem.Allocator) !void {
    return liveRoundTripOn(s, arena, "data");
}

fn liveRoundTripOn(s: *Session, arena: std.mem.Allocator, share: []const u8) !void {
    const gpa = std.testing.allocator;
    const root = try s.open(share, "", Access.read_data | Access.read_attributes | Access.synchronize, disposition_open, option_directory);
    const entries = try s.list(arena, root);
    s.closeHandle(root);
    _ = entries;
    // write a file, read it back, rename it, delete it
    const payload = try gpa.alloc(u8, 3 * io_unit + 12345);
    defer gpa.free(payload);
    for (payload, 0..) |*c, i| c.* = @truncate(i *% 31);
    const w = try s.open(share, "basalt_live.bin.part", Access.write_data | Access.read_data | Access.delete | Access.synchronize, disposition_overwrite_if, option_non_directory);
    try s.writeAt(w, 0, payload);
    try s.rename(w, "basalt_live.bin");
    s.closeHandle(w);
    const r = try s.open(share, "basalt_live.bin", Access.read_data | Access.delete | Access.synchronize, disposition_open, option_non_directory);
    try std.testing.expectEqual(@as(u64, payload.len), r.size);
    const back = try gpa.alloc(u8, payload.len + 100);
    defer gpa.free(back);
    try std.testing.expectEqual(payload.len, try s.readAt(r, 0, back));
    try std.testing.expectEqualSlices(u8, payload, back[0..payload.len]);
    try s.deleteOnClose(r);
    s.closeHandle(r);
    try std.testing.expectError(error.SmbNoSuchFile, s.open(share, "basalt_live.bin", Access.read_data, disposition_open, option_non_directory));
    try std.testing.expectError(error.SmbBadShare, s.tree("nope"));
}
