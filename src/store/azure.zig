//! Azure Blob Storage over the Blob REST API, authenticated with Shared Key.
//!
//! ADLS Gen2 data is reachable through this endpoint: Gen2 is layered on Blob
//! storage, and the DFS endpoint only adds hierarchical-namespace operations
//! (atomic rename, POSIX ACLs) a file pipeline never issues. Azurite implements
//! Blob only, so going through Blob makes the local integration suite possible
//! while staying valid against a real Gen2 account.
//!
//! URLs are `az://<account>/<container>/<path>`; a trailing slash addresses every
//! blob under a prefix. The endpoint defaults to
//! `https://<account>.blob.core.windows.net`; AZURE_BLOB_ENDPOINT points at Azurite,
//! which is path-style (`<endpoint>/<account>/<container>/<path>`). The key comes
//! from AZURE_STORAGE_KEY. `api_version` only needs to be recent enough for the
//! operations used; Shared Key signing is stable across versions.
//!
//! Shared Key signs the verb, the `x-ms-*` headers (sorted), the Range and the
//! canonical resource, so none of them can be changed after signing. StringToSign
//! is the verb, then Content-Encoding, Content-Language, Content-Length (empty, not
//! "0", when zero), Content-MD5, Content-Type, Date (empty: x-ms-date is used),
//! If-Modified-Since, If-Match, If-None-Match, If-Unmodified-Since and Range, one
//! per line. Any mistake there is an opaque 403.
//!
//! Writes stream as a block blob: one `block_size` buffer is staged per Put Block
//! and `finish` commits the ordered list with Put Block List, so memory stays at
//! one block (4 MiB blocks under Azure's 50,000-block limit cap an object at
//! ~190 GiB). There is no abort: uncommitted blocks are invisible and Azure
//! garbage-collects them, so dropping the writer without `finish` is the abort.
//! Staging runs under `std.Io.Writer`, whose only error is `WriteFailed`, so the
//! writer keeps the typed error (`last_status`) and Azure's message for the sink
//! to re-raise.
//!
//! `AzureEmptyPrefix` is kept distinct from an empty object because its usual
//! cause is a mistyped prefix in an otherwise full container.
//!
//! Everything not specific to Shared Key (error bodies, retry policy, the listing
//! loop, the consumer-facing interface) lives in `objstore.zig`.

const std = @import("std");
const objstore = @import("objstore.zig");
const civilFromDays = @import("../exec/eval/time.zig").civilFromDays;

pub const api_version = "2021-08-06";

pub const env_key = "AZURE_STORAGE_KEY";
pub const env_endpoint = "AZURE_BLOB_ENDPOINT";

pub const Error = error{
    AzureBadUrl,
    AzureMissingKey,
    AzureBadKey,
    AzureRequestFailed,
    AzureContainerMissing,
    AzureBlobNotFound,
    AzureAuthFailed,
    AzureThrottled,
    AzureEmptyPrefix,
};

pub const Blob = struct {
    account: []const u8,
    container: []const u8,
    /// The blob's path as its URL carries it, percent-encoded, as Shared Key signs it.
    path: []const u8,
    url: []const u8,
    canonical_resource: []const u8,
};

pub fn isUrl(s: []const u8) bool {
    return std.mem.startsWith(u8, s, "az://");
}

/// Splits `az://account/container/path...`; the container is the first segment.
/// A non-null `endpoint` selects a path-style emulator, which changes the signed
/// resource, not just the URL (see `canonicalResource`).
pub fn parseUrl(arena: std.mem.Allocator, url: []const u8, endpoint: ?[]const u8) !Blob {
    if (!isUrl(url)) return Error.AzureBadUrl;
    const rest = url["az://".len..];

    const a_end = std.mem.indexOfScalar(u8, rest, '/') orelse return Error.AzureBadUrl;
    const account = rest[0..a_end];
    const after_account = rest[a_end + 1 ..];

    const c_end = std.mem.indexOfScalar(u8, after_account, '/') orelse return Error.AzureBadUrl;
    const container = after_account[0..c_end];
    const path = after_account[c_end + 1 ..];
    if (account.len == 0 or container.len == 0 or path.len == 0) return Error.AzureBadUrl;

    const base = try containerBase(arena, account, container, endpoint);
    const enc = try objstore.uriEncode(arena, path, .keep_slash);
    const url_path = try std.fmt.allocPrint(arena, "{s}/{s}", .{ base.path, enc });
    return .{
        .account = account,
        .container = container,
        .path = enc,
        .url = try std.fmt.allocPrint(arena, "{s}/{s}", .{ base.url, enc }),
        .canonical_resource = try canonicalResource(arena, account, url_path),
    };
}

/// The container's URL and URL path: `/<container>` against the real service,
/// `/<account>/<container>` against a path-style `endpoint`.
fn containerBase(arena: std.mem.Allocator, account: []const u8, container: []const u8, endpoint: ?[]const u8) !struct { url: []const u8, path: []const u8 } {
    const path = if (endpoint == null)
        try std.fmt.allocPrint(arena, "/{s}", .{container})
    else
        try std.fmt.allocPrint(arena, "/{s}/{s}", .{ account, container });
    const url = if (endpoint) |ep|
        try std.fmt.allocPrint(arena, "{s}{s}", .{ std.mem.trimRight(u8, ep, "/"), path })
    else
        try std.fmt.allocPrint(arena, "https://{s}.blob.core.windows.net{s}", .{ account, path });
    return .{ .url = url, .path = path };
}

/// `/<account>` followed by the request URL's path. Against a path-style emulator
/// that path already begins with the account, so it legitimately appears twice
/// (`/devstoreaccount1/devstoreaccount1/lake/f.csv`); signing it once is a bare 403.
pub fn canonicalResource(arena: std.mem.Allocator, account: []const u8, url_path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "/{s}{s}", .{ account, url_path });
}

pub fn endpointFromEnv(arena: std.mem.Allocator) ?[]const u8 {
    return std.process.getEnvVarOwned(arena, env_endpoint) catch null;
}

pub fn keyFromEnv(arena: std.mem.Allocator) ![]const u8 {
    return std.process.getEnvVarOwned(arena, env_key) catch return Error.AzureMissingKey;
}

/// `Sun, 06 Nov 1994 08:49:37 GMT`, the only date format Shared Key accepts.
pub fn rfc1123(arena: std.mem.Allocator, epoch_secs: i64) ![]const u8 {
    const days = @divFloor(epoch_secs, 86400);
    const secs_of_day = @as(u32, @intCast(epoch_secs - days * 86400));
    const c = civilFromDays(days);
    const dow: usize = @intCast(@mod(days + 4, 7));
    const day_names = [_][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat" };
    const mon_names = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    return std.fmt.allocPrint(arena, "{s}, {d:0>2} {s} {d:0>4} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
        day_names[dow],
        c.d,
        mon_names[c.m - 1],
        @as(u32, @intCast(c.y)),
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    });
}

pub const MsHeader = struct { name: []const u8, value: []const u8 };

pub const SignParams = struct {
    method: []const u8,
    canonical_resource: []const u8,
    ms_headers: []const MsHeader = &.{},
    content_length: usize = 0,
    content_type: []const u8 = "",
    range: []const u8 = "",
    if_none_match: []const u8 = "",
    query: []const []const u8 = &.{},
};

/// Builds the `Authorization: SharedKey ...` value. The StringToSign layout is
/// written out one line per field on purpose (see the module header).
pub fn authHeader(arena: std.mem.Allocator, account: []const u8, key_b64: []const u8, p: SignParams) ![]const u8 {
    var sts = std.array_list.Managed(u8).init(arena);
    const w = sts.writer();

    try w.print("{s}\n", .{p.method});
    try w.writeAll("\n");
    try w.writeAll("\n");
    if (p.content_length > 0) try w.print("{d}", .{p.content_length});
    try w.writeAll("\n");
    try w.writeAll("\n");
    try w.print("{s}\n", .{p.content_type});
    try w.writeAll("\n");
    try w.writeAll("\n");
    try w.writeAll("\n");
    try w.print("{s}\n", .{p.if_none_match});
    try w.writeAll("\n");
    try w.print("{s}\n", .{p.range});

    const sorted = try arena.dupe(MsHeader, p.ms_headers);
    std.mem.sort(MsHeader, sorted, {}, struct {
        fn lt(_: void, a: MsHeader, b: MsHeader) bool {
            return std.mem.lessThan(u8, a.name, b.name);
        }
    }.lt);
    for (sorted) |h| try w.print("{s}:{s}\n", .{ h.name, h.value });

    try w.writeAll(p.canonical_resource);
    for (p.query) |q| try w.print("\n{s}", .{q});

    const dec = std.base64.standard.Decoder;
    const key_len = dec.calcSizeForSlice(key_b64) catch return Error.AzureBadKey;
    const key = try arena.alloc(u8, key_len);
    dec.decode(key, key_b64) catch return Error.AzureBadKey;

    const H = std.crypto.auth.hmac.sha2.HmacSha256;
    var mac: [H.mac_length]u8 = undefined;
    H.create(&mac, sts.items, key);

    const enc = std.base64.standard.Encoder;
    const sig = try arena.alloc(u8, enc.calcSize(mac.len));
    _ = enc.encode(sig, &mac);
    return std.fmt.allocPrint(arena, "SharedKey {s}:{s}", .{ account, sig });
}

/// Signed headers for any verb; `range` is `bytes=a-b` or empty for the whole object.
pub fn requestHeaders(
    arena: std.mem.Allocator,
    b: Blob,
    method: []const u8,
    range: []const u8,
) ![]const std.http.Header {
    return signedHeaders(arena, b.account, try keyFromEnv(arena), .{
        .method = method,
        .canonical_resource = b.canonical_resource,
        .range = range,
    });
}

/// Signs `p` with the date and version added to its `x-ms-*` headers, and returns
/// every header to send.
fn signedHeaders(arena: std.mem.Allocator, account: []const u8, key: []const u8, p: SignParams) ![]const std.http.Header {
    const date = try rfc1123(arena, std.time.timestamp());
    var ms = std.array_list.Managed(MsHeader).init(arena);
    try ms.appendSlice(p.ms_headers);
    try ms.append(.{ .name = "x-ms-date", .value = date });
    try ms.append(.{ .name = "x-ms-version", .value = api_version });
    var signed = p;
    signed.ms_headers = ms.items;
    const auth = try authHeader(arena, account, key, signed);

    var out = std.array_list.Managed(std.http.Header).init(arena);
    try out.append(.{ .name = "x-ms-date", .value = date });
    try out.append(.{ .name = "x-ms-version", .value = api_version });
    for (p.ms_headers) |h| try out.append(.{ .name = h.name, .value = h.value });
    try out.append(.{ .name = "Authorization", .value = auth });
    if (p.range.len > 0) try out.append(.{ .name = "Range", .value = p.range });
    if (p.if_none_match.len > 0) try out.append(.{ .name = "If-None-Match", .value = p.if_none_match });
    return out.toOwnedSlice();
}

/// Maps a status plus Azure's error code onto a distinct Zig error.
pub fn statusToError(code: u16, body: []const u8) Error {
    if (objstore.parseError(body)) |e| {
        if (std.mem.eql(u8, e.code, "AuthenticationFailed")) return Error.AzureAuthFailed;
        if (std.mem.eql(u8, e.code, "ContainerNotFound")) return Error.AzureContainerMissing;
        if (std.mem.eql(u8, e.code, "BlobNotFound")) return Error.AzureBlobNotFound;
        if (std.mem.eql(u8, e.code, "AuthorizationFailure")) return Error.AzureAuthFailed;
    }
    return switch (code) {
        401, 403 => Error.AzureAuthFailed,
        404 => Error.AzureContainerMissing,
        429, 503 => Error.AzureThrottled,
        else => Error.AzureRequestFailed,
    };
}

pub const block_size = 4 * 1024 * 1024;

pub const BlockBlobWriter = struct {
    interface: std.Io.Writer,
    arena: std.mem.Allocator,
    client: *std.http.Client,
    blob: Blob,
    key: []const u8,
    block_ids: std.array_list.Managed([]const u8),
    content_type: []const u8,
    last_error: []const u8 = "",
    last_status: ?Error = null,
    rand: std.Random.DefaultPrng,
    sleep: objstore.Sleep = std.Thread.sleep,

    const vtable = std.Io.Writer.VTable{ .drain = objstore.drain(BlockBlobWriter) };

    pub fn init(
        arena: std.mem.Allocator,
        client: *std.http.Client,
        blob: Blob,
        content_type: []const u8,
    ) !*BlockBlobWriter {
        const self = try arena.create(BlockBlobWriter);
        self.* = .{
            .interface = .{ .vtable = &vtable, .buffer = try arena.alloc(u8, block_size) },
            .arena = arena,
            .client = client,
            .blob = blob,
            .key = try keyFromEnv(arena),
            .block_ids = std.array_list.Managed([]const u8).init(arena),
            .content_type = content_type,
            .rand = objstore.jitterPrng(),
        };
        return self;
    }

    pub fn put(self: *BlockBlobWriter, bytes: []const u8) std.Io.Writer.Error!usize {
        if (bytes.len == 0) return 0;
        self.stageBlock(bytes) catch return error.WriteFailed;
        return bytes.len;
    }

    /// Block IDs must all be the same length and base64-encoded; the commit list
    /// defines order, so a counter is enough.
    fn blockId(self: *BlockBlobWriter, n: usize) ![]const u8 {
        var raw: [16]u8 = undefined;
        _ = try std.fmt.bufPrint(&raw, "blk{d:0>13}", .{n});
        const enc = std.base64.standard.Encoder;
        const out = try self.arena.alloc(u8, enc.calcSize(raw.len));
        _ = enc.encode(out, &raw);
        return out;
    }

    /// Put Block. A missing container is created once and the block retried, so a
    /// fresh destination needs no setup step. Query params sign sorted: blockid, comp.
    fn stageBlock(self: *BlockBlobWriter, bytes: []const u8) !void {
        const id = try self.blockId(self.block_ids.items.len);
        const id_enc = try objstore.uriEncode(self.arena, id, .encode_slash);
        const url = try std.fmt.allocPrint(self.arena, "{s}?comp=block&blockid={s}", .{ self.blob.url, id_enc });

        const sign = Sign{
            .content_length = bytes.len,
            .query = &.{
                try std.fmt.allocPrint(self.arena, "blockid:{s}", .{id}),
                "comp:block",
            },
        };

        self.send(url, sign, bytes) catch |e| switch (e) {
            Error.AzureContainerMissing => {
                try self.createContainer();
                try self.send(url, sign, bytes);
            },
            else => return e,
        };
        try self.block_ids.append(id);
    }

    /// Commits the staged blocks in order. Until this returns, the blob does not exist.
    /// A replay after a lost answer is harmless: `Latest` resolves an id against the
    /// committed list once the uncommitted one is gone, so the same list commits again.
    pub fn finish(self: *BlockBlobWriter) !void {
        try self.interface.flush();

        var body = std.array_list.Managed(u8).init(self.arena);
        try body.appendSlice("<?xml version=\"1.0\" encoding=\"utf-8\"?><BlockList>");
        for (self.block_ids.items) |id| {
            try body.appendSlice("<Latest>");
            try body.appendSlice(id);
            try body.appendSlice("</Latest>");
        }
        try body.appendSlice("</BlockList>");

        const url = try std.fmt.allocPrint(self.arena, "{s}?comp=blocklist", .{self.blob.url});
        const sign = Sign{
            .ms_headers = &.{.{ .name = "x-ms-blob-content-type", .value = self.content_type }},
            .content_length = body.items.len,
            .query = &.{"comp:blocklist"},
        };
        try self.send(url, sign, body.items);
    }

    /// Put Blob of `bytes` with `If-None-Match: *`: true when it made the blob,
    /// false when one was there (409 BlobAlreadyExists, or 412). A missing
    /// container is created once and the put retried.
    pub fn putIfAbsent(self: *BlockBlobWriter, bytes: []const u8) !bool {
        var created = false;
        const Ctx = struct { w: *BlockBlobWriter, bytes: []const u8, created: *bool, made_container: bool = false };
        var ctx = Ctx{ .w = self, .bytes = bytes, .created = &created };
        try objstore.retry(self.policy(), &ctx, struct {
            fn attempt(c: *Ctx) !objstore.Verdict {
                const w = c.w;
                const hdrs = try signedHeaders(w.arena, w.blob.account, w.key, .{
                    .method = "PUT",
                    .canonical_resource = w.blob.canonical_resource,
                    .ms_headers = &.{.{ .name = "x-ms-blob-type", .value = "BlockBlob" }},
                    .content_length = c.bytes.len,
                    .content_type = w.content_type,
                    .if_none_match = "*",
                });
                const r = try objstore.exchange(w.arena, w.client, .{ .method = .PUT, .url = w.blob.url, .headers = hdrs, .payload = c.bytes, .content_type = w.content_type });
                switch (r.code) {
                    200, 201 => c.created.* = true,
                    409, 412 => c.created.* = false,
                    else => {
                        if (!c.made_container and statusToError(r.code, r.body) == Error.AzureContainerMissing) {
                            c.made_container = true;
                            try w.createContainer();
                            return .again;
                        }
                        return w.failed(r);
                    },
                }
                w.last_status = null;
                return .done;
            }
        }.attempt);
        return created;
    }

    const Sign = struct { ms_headers: []const MsHeader = &.{}, content_length: usize, query: []const []const u8 };
    const Send = struct { w: *BlockBlobWriter, url: []const u8, sign: Sign, body: []const u8 };

    /// PUT with retry, signed afresh per attempt. `last_status` is cleared on success,
    /// since `stageBlock` retries a missing container and that first attempt's stale
    /// error must not be re-raised.
    fn send(self: *BlockBlobWriter, url: []const u8, sign: Sign, body: []const u8) !void {
        try objstore.retry(self.policy(), Send{ .w = self, .url = url, .sign = sign, .body = body }, struct {
            fn attempt(c: Send) !objstore.Verdict {
                const w = c.w;
                const hdrs = try signedHeaders(w.arena, w.blob.account, w.key, .{
                    .method = "PUT",
                    .canonical_resource = w.blob.canonical_resource,
                    .ms_headers = c.sign.ms_headers,
                    .content_length = c.sign.content_length,
                    .query = c.sign.query,
                });
                const r = try objstore.exchange(w.arena, w.client, .{ .method = .PUT, .url = c.url, .headers = hdrs, .payload = c.body });
                if (r.code == 201 or r.code == 200) {
                    w.last_status = null;
                    return .done;
                }
                return w.failed(r);
            }
        }.attempt);
    }

    /// Create the container; 409 (already exists) counts as success.
    fn createContainer(self: *BlockBlobWriter) !void {
        const base = self.blob.url[0 .. self.blob.url.len - self.blob.path.len - 1];
        const url = try std.fmt.allocPrint(self.arena, "{s}?restype=container", .{base});
        const canon = self.blob.canonical_resource[0 .. self.blob.canonical_resource.len - self.blob.path.len - 1];

        const hdrs = try signedHeaders(self.arena, self.blob.account, self.key, .{
            .method = "PUT",
            .canonical_resource = canon,
            .query = &.{"restype:container"},
        });

        const r = try objstore.exchange(self.arena, self.client, .{ .method = .PUT, .url = url, .headers = hdrs, .payload = "" });
        if (r.code != 201 and r.code != 409) return self.fail(r.code, r.body);
    }

    fn policy(self: *BlockBlobWriter) objstore.Policy {
        return .{ .rand = self.rand.random(), .sleep = self.sleep };
    }

    fn fail(self: *BlockBlobWriter, code: u16, body: []const u8) Error {
        self.last_error = objstore.describe(self.arena, code, body) catch "";
        self.last_status = statusToError(code, body);
        return self.last_status.?;
    }

    fn failed(self: *BlockBlobWriter, r: objstore.Response) objstore.Verdict {
        return r.failed(self.fail(r.code, r.body));
    }
};

test "block ids are the same width at every block number" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var client: std.http.Client = undefined;
    const blob = try parseUrl(a, "az://acct/c/f.csv", null);
    var w = BlockBlobWriter{
        .interface = .{ .vtable = undefined, .buffer = &.{} },
        .arena = a,
        .client = &client,
        .blob = blob,
        .key = "",
        .block_ids = std.array_list.Managed([]const u8).init(a),
        .content_type = "text/csv",
        .rand = std.Random.DefaultPrng.init(0),
    };
    const b0 = try w.blockId(0);
    try std.testing.expectEqual(b0.len, (try w.blockId(9)).len);
    try std.testing.expectEqual(b0.len, (try w.blockId(10)).len);
    try std.testing.expectEqual(b0.len, (try w.blockId(49_999)).len);
}

var test_slept_ms: u64 = 0;

fn testSleep(ns: u64) void {
    test_slept_ms += ns / std.time.ns_per_ms;
}

test "Put Block is re-put after a reset and waits out Retry-After; Put Block List is replayed after a 500" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const reply = objstore.FakeServer.reply;
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    var srv: objstore.FakeServer = undefined;
    try srv.start(&.{
        null,
        reply("503 Server Busy", "Retry-After: 2\r\n", "<Error><Code>ServerBusy</Code></Error>"),
        reply("201 Created", "", ""),
        reply("500 Internal Server Error", "", ""),
        reply("201 Created", "", ""),
    });
    defer srv.stop();
    var w = BlockBlobWriter{
        .interface = .{ .vtable = &BlockBlobWriter.vtable, .buffer = &.{} },
        .arena = a,
        .client = &client,
        .blob = try parseUrl(a, "az://devstoreaccount1/c/f.csv", try srv.url(a, "")),
        .key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        .block_ids = std.array_list.Managed([]const u8).init(a),
        .content_type = "text/csv",
        .rand = std.Random.DefaultPrng.init(0),
        .sleep = testSleep,
    };
    test_slept_ms = 0;
    try w.stageBlock("hello");
    try std.testing.expectEqual(@as(usize, 1), w.block_ids.items.len);
    try std.testing.expect(test_slept_ms >= 2000);
    try std.testing.expectEqualStrings(srv.line(0), srv.line(2));
    try w.finish();
    try std.testing.expectEqual(@as(usize, 5), srv.requests());
    try std.testing.expect(std.mem.indexOf(u8, srv.line(4), "comp=blocklist") != null);
    try std.testing.expectEqualStrings(srv.line(3), srv.line(4));
}

test "putIfAbsent: 201 creates, 409 and 412 find one there, a missing container is made once" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const reply = objstore.FakeServer.reply;
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    var srv: objstore.FakeServer = undefined;
    try srv.start(&.{
        reply("201 Created", "", ""),
        reply("409 Conflict", "", "<Error><Code>BlobAlreadyExists</Code></Error>"),
        reply("412 Precondition Failed", "", "<Error><Code>ConditionNotMet</Code></Error>"),
        reply("404 Not Found", "", "<Error><Code>ContainerNotFound</Code></Error>"),
        reply("201 Created", "", ""),
        reply("201 Created", "", ""),
    });
    defer srv.stop();
    var w = BlockBlobWriter{
        .interface = .{ .vtable = &BlockBlobWriter.vtable, .buffer = &.{} },
        .arena = a,
        .client = &client,
        .blob = try parseUrl(a, "az://devstoreaccount1/c/_delta_log/0.json", try srv.url(a, "")),
        .key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        .block_ids = std.array_list.Managed([]const u8).init(a),
        .content_type = "application/json",
        .rand = std.Random.DefaultPrng.init(0),
        .sleep = testSleep,
    };
    try std.testing.expect(try w.putIfAbsent("{}"));
    try std.testing.expect(!try w.putIfAbsent("{}"));
    try std.testing.expect(!try w.putIfAbsent("{}"));
    try std.testing.expect(try w.putIfAbsent("{}"));
    try std.testing.expectEqual(@as(usize, 6), srv.requests());
    try std.testing.expect(std.mem.indexOf(u8, srv.line(4), "restype=container") != null);
}

test "If-None-Match is signed in its own line of the string to sign" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const p = SignParams{ .method = "PUT", .canonical_resource = "/acct/c/f" };
    var q = p;
    q.if_none_match = "*";
    try std.testing.expect(!std.mem.eql(u8, try authHeader(a, "acct", key, p), try authHeader(a, "acct", key, q)));
}

test "query values escape the base64 characters that are not URL-safe" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("%2B", try objstore.uriEncode(a, "+", .encode_slash));
    try std.testing.expectEqualStrings("a%2Fb%3D%3D", try objstore.uriEncode(a, "a/b==", .encode_slash));
    try std.testing.expectEqualStrings("YmxrMDA", try objstore.uriEncode(a, "YmxrMDA", .encode_slash));
}

test "parseUrl: host-style URL and signature for the real service" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const b = try parseUrl(a, "az://acct/cont/dir/sub/file.csv", null);
    try std.testing.expectEqualStrings("acct", b.account);
    try std.testing.expectEqualStrings("cont", b.container);
    try std.testing.expectEqualStrings("dir/sub/file.csv", b.path);
    try std.testing.expectEqualStrings("https://acct.blob.core.windows.net/cont/dir/sub/file.csv", b.url);
    try std.testing.expectEqualStrings("/acct/cont/dir/sub/file.csv", b.canonical_resource);

    const odd = try parseUrl(a, "az://acct/cont/ts=2026-01-01 00%3A00%3A00/a+b.parquet", null);
    try std.testing.expectEqualStrings("https://acct.blob.core.windows.net/cont/ts%3D2026-01-01%2000%253A00%253A00/a%2Bb.parquet", odd.url);
    try std.testing.expectEqualStrings("/acct/cont/ts%3D2026-01-01%2000%253A00%253A00/a%2Bb.parquet", odd.canonical_resource);
    try std.testing.expectError(Error.AzureBadUrl, parseUrl(a, "az://acct/cont", null));
    try std.testing.expectError(Error.AzureBadUrl, parseUrl(a, "https://x/y/z", null));
    try std.testing.expect(!isUrl("s3://b/k"));
}

test "parseUrl: path-style emulator repeats the account in the signed resource" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const b = try parseUrl(a, "az://devstoreaccount1/lake/in.csv", "http://127.0.0.1:31000");
    try std.testing.expectEqualStrings("http://127.0.0.1:31000/devstoreaccount1/lake/in.csv", b.url);
    try std.testing.expectEqualStrings("/devstoreaccount1/devstoreaccount1/lake/in.csv", b.canonical_resource);

    const c = try parseUrl(a, "az://devstoreaccount1/lake/in.csv", "http://127.0.0.1:31000/");
    try std.testing.expectEqualStrings(b.url, c.url);
}

test "rfc1123 matches the reference date from the Azure signing docs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("Sun, 06 Nov 1994 08:49:37 GMT", try rfc1123(a, 784111777));
    try std.testing.expectEqualStrings("Thu, 01 Jan 1970 00:00:00 GMT", try rfc1123(a, 0));
}

test "authHeader: signatures match the SharedKey layout, header order does not matter, and Content-Length 0 signs as empty" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    const date = "Sun, 06 Nov 1994 08:49:37 GMT";
    const res = "/devstoreaccount1/c/f.csv";

    const h1 = try authHeader(a, "devstoreaccount1", key, .{
        .method = "GET",
        .canonical_resource = res,
        .ms_headers = &.{
            .{ .name = "x-ms-version", .value = api_version },
            .{ .name = "x-ms-date", .value = date },
        },
    });
    const h2 = try authHeader(a, "devstoreaccount1", key, .{
        .method = "GET",
        .canonical_resource = res,
        .ms_headers = &.{
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        },
    });
    try std.testing.expectEqualStrings("SharedKey devstoreaccount1:obRwfj83MnpKQKlq0x0hh52lTflQP09ZDntY2eAI7Hk=", h1);
    try std.testing.expectEqualStrings(h1, h2);

    const headers = &[_]MsHeader{
        .{ .name = "x-ms-date", .value = date },
        .{ .name = "x-ms-version", .value = api_version },
    };
    const omitted = try authHeader(a, "devstoreaccount1", key, .{ .method = "PUT", .canonical_resource = res, .ms_headers = headers, .content_type = "text/csv" });
    const zero = try authHeader(a, "devstoreaccount1", key, .{ .method = "PUT", .canonical_resource = res, .ms_headers = headers, .content_type = "text/csv", .content_length = 0 });
    const five = try authHeader(a, "devstoreaccount1", key, .{ .method = "PUT", .canonical_resource = res, .ms_headers = headers, .content_type = "text/csv", .content_length = 5 });
    try std.testing.expectEqualStrings("SharedKey devstoreaccount1:A3dhQWBT20oStAXoOomI0lCAMLRuq2OUX2fi9Zq0c78=", zero);
    try std.testing.expectEqualStrings(omitted, zero);
    try std.testing.expectEqualStrings("SharedKey devstoreaccount1:leEHE8BYEl9Yzf92qjOQxX5MlJKmy64LZXWbOKAA8AA=", five);
}

test "authHeader rejects a malformed key rather than signing with garbage" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectError(Error.AzureBadKey, authHeader(ar.allocator(), "acct", "not!base64!", .{
        .method = "GET",
        .canonical_resource = "/acct/c/f",
        .ms_headers = &.{},
    }));
}

const ListPage = struct {
    arena: std.mem.Allocator,
    client: *std.http.Client,
    account: []const u8,
    prefix: []const u8,
    base: []const u8,
    canon: []const u8,
    key: []const u8,
    sleep: objstore.Sleep = std.Thread.sleep,

    fn page(c: ListPage, marker: []const u8) ![]const u8 {
        const url = if (marker.len == 0)
            try std.fmt.allocPrint(c.arena, "{s}?restype=container&comp=list&prefix={s}", .{ c.base, try objstore.uriEncode(c.arena, c.prefix, .encode_slash) })
        else
            try std.fmt.allocPrint(c.arena, "{s}?restype=container&comp=list&prefix={s}&marker={s}", .{ c.base, try objstore.uriEncode(c.arena, c.prefix, .encode_slash), try objstore.uriEncode(c.arena, marker, .encode_slash) });

        var q = std.array_list.Managed([]const u8).init(c.arena);
        try q.append("comp:list");
        if (marker.len > 0) try q.append(try std.fmt.allocPrint(c.arena, "marker:{s}", .{marker}));
        try q.append(try std.fmt.allocPrint(c.arena, "prefix:{s}", .{c.prefix}));
        try q.append("restype:container");

        var body: []const u8 = "";
        var prng = objstore.jitterPrng();
        const Ctx = struct { c: ListPage, q: []const []const u8, url: []const u8, out: *[]const u8 };
        try objstore.retry(.{ .rand = prng.random(), .sleep = c.sleep }, Ctx{ .c = c, .q = q.items, .url = url, .out = &body }, struct {
            fn attempt(x: Ctx) !objstore.Verdict {
                const hdrs = try signedHeaders(x.c.arena, x.c.account, x.c.key, .{
                    .method = "GET",
                    .canonical_resource = x.c.canon,
                    .query = x.q,
                });
                const r = try objstore.exchange(x.c.arena, x.c.client, .{ .method = .GET, .url = x.url, .headers = hdrs });
                if (r.code != 200) return r.failed(statusToError(r.code, r.body));
                x.out.* = r.body;
                return .done;
            }
        }.attempt);
        return body;
    }
};

/// Lists blob names under `prefix`, container-relative, following continuation
/// markers, in Azure's lexicographic order. A flat listing returns only blobs.
pub fn listPrefix(
    arena: std.mem.Allocator,
    client: *std.http.Client,
    account: []const u8,
    container: []const u8,
    prefix: []const u8,
    endpoint: ?[]const u8,
) ![][]const u8 {
    const base = try containerBase(arena, account, container, endpoint);
    const ctx = ListPage{
        .arena = arena,
        .client = client,
        .account = account,
        .prefix = prefix,
        .base = base.url,
        .canon = try canonicalResource(arena, account, base.path),
        .key = try keyFromEnv(arena),
    };
    return objstore.listPages(arena, .{ .item_tag = "Name", .next_tag = "NextMarker" }, ctx, ListPage.page);
}

test "statusToError distinguishes causes instead of one catch-all" {
    try std.testing.expectEqual(Error.AzureAuthFailed, statusToError(403, "<Error><Code>AuthorizationFailure</Code></Error>"));
    try std.testing.expectEqual(Error.AzureContainerMissing, statusToError(404, "<Error><Code>ContainerNotFound</Code></Error>"));
    try std.testing.expectEqual(Error.AzureBlobNotFound, statusToError(404, "<Error><Code>BlobNotFound</Code></Error>"));
    try std.testing.expectEqual(Error.AzureThrottled, statusToError(503, ""));
    try std.testing.expectEqual(Error.AzureRequestFailed, statusToError(418, ""));
    try std.testing.expectEqual(Error.AzureAuthFailed, statusToError(400, "<Error><Code>AuthenticationFailed</Code></Error>"));
    try std.testing.expectEqual(Error.AzureBlobNotFound, statusToError(400, "<Error><Code>BlobNotFound</Code></Error>"));
    try std.testing.expectEqual(Error.AzureContainerMissing, statusToError(403, "<Error><Code>ContainerNotFound</Code></Error>"));
    try std.testing.expectEqual(Error.AzureRequestFailed, statusToError(400, "<Error><Code>InvalidHeaderValue</Code></Error>"));
}

/// Splits a prefix URL for listing. Unlike `parseUrl`, the prefix may be empty.
pub fn parsePrefix(url: []const u8) !struct { account: []const u8, container: []const u8, prefix: []const u8 } {
    if (!isUrl(url)) return Error.AzureBadUrl;
    const rest = url["az://".len..];
    const a_end = std.mem.indexOfScalar(u8, rest, '/') orelse return Error.AzureBadUrl;
    const account = rest[0..a_end];
    const after = rest[a_end + 1 ..];
    const c_end = std.mem.indexOfScalar(u8, after, '/') orelse return Error.AzureBadUrl;
    const container = after[0..c_end];
    if (account.len == 0 or container.len == 0) return Error.AzureBadUrl;
    return .{ .account = account, .container = container, .prefix = after[c_end + 1 ..] };
}

test "prefix URLs may have an empty prefix" {
    const p = try parsePrefix("az://acct/cont/year=2026/");
    try std.testing.expectEqualStrings("acct", p.account);
    try std.testing.expectEqualStrings("cont", p.container);
    try std.testing.expectEqualStrings("year=2026/", p.prefix);

    const bare = try parsePrefix("az://acct/cont/");
    try std.testing.expectEqualStrings("", bare.prefix);
}

pub const provider = objstore.Provider{
    .scheme = "az://",
    .empty_prefix = Error.AzureEmptyPrefix,
    .vtable = &.{
        .parse = vtParse,
        .list_prefix = vtListPrefix,
        .request_headers = vtRequestHeaders,
        .status_to_error = vtStatusToError,
        .open_writer = vtOpenWriter,
        .put_if_absent = vtPutIfAbsent,
    },
};

fn vtParse(arena: std.mem.Allocator, url: []const u8) anyerror!objstore.Object {
    const b = try arena.create(Blob);
    b.* = try parseUrl(arena, url, endpointFromEnv(arena));
    return .{ .provider = &provider, .url = b.url, .ptr = b };
}

fn vtListPrefix(arena: std.mem.Allocator, client: *std.http.Client, url: []const u8) anyerror![]const []const u8 {
    const p = try parsePrefix(url);
    const names = try listPrefix(arena, client, p.account, p.container, p.prefix, endpointFromEnv(arena));
    const urls = try arena.alloc([]const u8, names.len);
    for (names, urls) |n, *u| u.* = try std.fmt.allocPrint(arena, "az://{s}/{s}/{s}", .{ p.account, p.container, n });
    return urls;
}

fn vtRequestHeaders(ptr: *const anyopaque, arena: std.mem.Allocator, method: []const u8, range: []const u8) anyerror![]const std.http.Header {
    return requestHeaders(arena, objstore.cast(Blob, ptr).*, method, range);
}

fn vtStatusToError(code: u16, body: []const u8) anyerror {
    return statusToError(code, body);
}

fn vtOpenWriter(ptr: *const anyopaque, arena: std.mem.Allocator, client: *std.http.Client, content_type: []const u8) anyerror!objstore.Writer {
    return objstore.writer(try BlockBlobWriter.init(arena, client, objstore.cast(Blob, ptr).*, content_type));
}

fn vtPutIfAbsent(ptr: *const anyopaque, arena: std.mem.Allocator, client: *std.http.Client, bytes: []const u8, content_type: []const u8, why: *[]const u8) anyerror!bool {
    var w = BlockBlobWriter{
        .interface = .{ .vtable = &BlockBlobWriter.vtable, .buffer = &.{} },
        .arena = arena,
        .client = client,
        .blob = objstore.cast(Blob, ptr).*,
        .key = try keyFromEnv(arena),
        .block_ids = std.array_list.Managed([]const u8).init(arena),
        .content_type = content_type,
        .rand = objstore.jitterPrng(),
    };
    return w.putIfAbsent(bytes) catch |e| {
        why.* = w.last_error;
        return e;
    };
}
