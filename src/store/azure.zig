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
const http_client = @import("../net/http_client.zig");
const objstore = @import("objstore.zig");

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

    const url_path = if (endpoint == null)
        try std.fmt.allocPrint(arena, "/{s}/{s}", .{ container, path })
    else
        try std.fmt.allocPrint(arena, "/{s}/{s}/{s}", .{ account, container, path });

    const full = if (endpoint) |ep|
        try std.fmt.allocPrint(arena, "{s}{s}", .{ std.mem.trimRight(u8, ep, "/"), url_path })
    else
        try std.fmt.allocPrint(arena, "https://{s}.blob.core.windows.net{s}", .{ account, url_path });

    return .{
        .account = account,
        .container = container,
        .path = path,
        .url = full,
        .canonical_resource = try canonicalResource(arena, account, url_path),
    };
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
    const c = objstore.civilFromDays(days);
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
    ms_headers: []const MsHeader,
    content_length: usize = 0,
    content_type: []const u8 = "",
    range: []const u8 = "",
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
    try w.writeAll("\n");
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

pub fn getHeaders(arena: std.mem.Allocator, b: Blob, range: []const u8) ![]const std.http.Header {
    return requestHeaders(arena, b, "GET", range);
}

/// Signed headers for any verb; `range` is `bytes=a-b` or empty for the whole object.
pub fn requestHeaders(
    arena: std.mem.Allocator,
    b: Blob,
    method: []const u8,
    range: []const u8,
) ![]const std.http.Header {
    const key = try keyFromEnv(arena);
    const date = try rfc1123(arena, std.time.timestamp());
    const ms = [_]MsHeader{
        .{ .name = "x-ms-date", .value = date },
        .{ .name = "x-ms-version", .value = api_version },
    };
    const auth = try authHeader(arena, b.account, key, .{
        .method = method,
        .canonical_resource = b.canonical_resource,
        .ms_headers = &ms,
        .range = range,
    });

    var out = std.array_list.Managed(std.http.Header).init(arena);
    try out.append(.{ .name = "x-ms-date", .value = date });
    try out.append(.{ .name = "x-ms-version", .value = api_version });
    try out.append(.{ .name = "Authorization", .value = auth });
    if (range.len > 0) try out.append(.{ .name = "Range", .value = range });
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
        const id_enc = try urlEncode(self.arena, id);
        const url = try std.fmt.allocPrint(self.arena, "{s}?comp=block&blockid={s}", .{ self.blob.url, id_enc });

        const date = try rfc1123(self.arena, std.time.timestamp());
        const ms = [_]MsHeader{
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        };
        const auth = try authHeader(self.arena, self.blob.account, self.key, .{
            .method = "PUT",
            .canonical_resource = self.blob.canonical_resource,
            .ms_headers = &ms,
            .content_length = bytes.len,
            .query = &.{
                try std.fmt.allocPrint(self.arena, "blockid:{s}", .{id}),
                "comp:block",
            },
        });

        self.send(url, date, auth, bytes, &.{}) catch |e| switch (e) {
            Error.AzureContainerMissing => {
                try self.createContainer();
                try self.send(url, date, auth, bytes, &.{});
            },
            else => return e,
        };
        try self.block_ids.append(id);
    }

    /// Commits the staged blocks in order. Until this returns, the blob does not exist.
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
        const date = try rfc1123(self.arena, std.time.timestamp());
        const ms = [_]MsHeader{
            .{ .name = "x-ms-blob-content-type", .value = self.content_type },
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        };
        const auth = try authHeader(self.arena, self.blob.account, self.key, .{
            .method = "PUT",
            .canonical_resource = self.blob.canonical_resource,
            .ms_headers = &ms,
            .content_length = body.items.len,
            .query = &.{"comp:blocklist"},
        });
        try self.send(url, date, auth, body.items, &.{
            .{ .name = "x-ms-blob-content-type", .value = self.content_type },
        });
    }

    const Send = struct { w: *BlockBlobWriter, url: []const u8, hdrs: []const std.http.Header, body: []const u8 };

    /// PUT with retry. `last_status` is cleared on success, since `stageBlock` retries a
    /// missing container and that first attempt's stale error must not be re-raised.
    fn send(
        self: *BlockBlobWriter,
        url: []const u8,
        date: []const u8,
        auth: []const u8,
        body: []const u8,
        extra: []const std.http.Header,
    ) !void {
        var hdrs = std.array_list.Managed(std.http.Header).init(self.arena);
        try hdrs.append(.{ .name = "x-ms-date", .value = date });
        try hdrs.append(.{ .name = "x-ms-version", .value = api_version });
        for (extra) |h| try hdrs.append(h);
        try hdrs.append(.{ .name = "Authorization", .value = auth });

        try objstore.retry(self.policy(), Send{ .w = self, .url = url, .hdrs = hdrs.items, .body = body }, struct {
            fn attempt(c: Send) !objstore.Verdict {
                var aw = std.Io.Writer.Allocating.init(c.w.arena);
                const res = try c.w.client.fetch(.{
                    .method = .PUT,
                    .location = .{ .url = c.url },
                    .extra_headers = c.hdrs,
                    .payload = c.body,
                    .decompress_buffer = http_client.decompress_direct,
                    .response_writer = &aw.writer,
                });
                const code = @intFromEnum(res.status);
                if (code == 201 or code == 200) {
                    c.w.last_status = null;
                    return .done;
                }
                return c.w.failed(code, aw.writer.buffered());
            }
        }.attempt);
    }

    /// Create the container; 409 (already exists) counts as success.
    fn createContainer(self: *BlockBlobWriter) !void {
        const base = self.blob.url[0 .. self.blob.url.len - self.blob.path.len - 1];
        const url = try std.fmt.allocPrint(self.arena, "{s}?restype=container", .{base});
        const canon = self.blob.canonical_resource[0 .. self.blob.canonical_resource.len - self.blob.path.len - 1];

        const date = try rfc1123(self.arena, std.time.timestamp());
        const ms = [_]MsHeader{
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        };
        const auth = try authHeader(self.arena, self.blob.account, self.key, .{
            .method = "PUT",
            .canonical_resource = canon,
            .ms_headers = &ms,
            .query = &.{"restype:container"},
        });

        var hdrs = std.array_list.Managed(std.http.Header).init(self.arena);
        try hdrs.append(.{ .name = "x-ms-date", .value = date });
        try hdrs.append(.{ .name = "x-ms-version", .value = api_version });
        try hdrs.append(.{ .name = "Authorization", .value = auth });

        var aw = std.Io.Writer.Allocating.init(self.arena);
        const res = try self.client.fetch(.{
            .method = .PUT,
            .location = .{ .url = url },
            .extra_headers = hdrs.items,
            .payload = "",
            .decompress_buffer = http_client.decompress_direct,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        if (code != 201 and code != 409) return self.fail(code, aw.writer.buffered());
    }

    fn policy(self: *BlockBlobWriter) objstore.Policy {
        return .{ .rand = self.rand.random() };
    }

    fn fail(self: *BlockBlobWriter, code: u16, body: []const u8) Error {
        self.last_error = objstore.describe(self.arena, code, body) catch "";
        self.last_status = statusToError(code, body);
        return self.last_status.?;
    }

    fn failed(self: *BlockBlobWriter, code: u16, body: []const u8) objstore.Verdict {
        return .{ .failed = .{ .code = code, .err = self.fail(code, body) } };
    }
};

/// Percent-encodes the characters base64 produces that are not URL-safe.
fn urlEncode(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out = std.array_list.Managed(u8).init(arena);
    for (s) |c| switch (c) {
        '+' => try out.appendSlice("%2B"),
        '/' => try out.appendSlice("%2F"),
        '=' => try out.appendSlice("%3D"),
        else => try out.append(c),
    };
    return out.toOwnedSlice();
}

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

test "urlEncode escapes the base64 characters that are not URL-safe" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("%2B", try urlEncode(a, "+"));
    try std.testing.expectEqualStrings("a%2Fb%3D%3D", try urlEncode(a, "a/b=="));
    try std.testing.expectEqualStrings("YmxrMDA", try urlEncode(a, "YmxrMDA"));
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

    fn page(c: ListPage, marker: []const u8) ![]const u8 {
        const url = if (marker.len == 0)
            try std.fmt.allocPrint(c.arena, "{s}?restype=container&comp=list&prefix={s}", .{ c.base, try urlEncode(c.arena, c.prefix) })
        else
            try std.fmt.allocPrint(c.arena, "{s}?restype=container&comp=list&prefix={s}&marker={s}", .{ c.base, try urlEncode(c.arena, c.prefix), try urlEncode(c.arena, marker) });

        const date = try rfc1123(c.arena, std.time.timestamp());
        const ms = [_]MsHeader{
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        };
        var q = std.array_list.Managed([]const u8).init(c.arena);
        try q.append("comp:list");
        if (marker.len > 0) try q.append(try std.fmt.allocPrint(c.arena, "marker:{s}", .{marker}));
        try q.append(try std.fmt.allocPrint(c.arena, "prefix:{s}", .{c.prefix}));
        try q.append("restype:container");

        const auth = try authHeader(c.arena, c.account, c.key, .{
            .method = "GET",
            .canonical_resource = c.canon,
            .ms_headers = &ms,
            .query = q.items,
        });

        var hdrs = std.array_list.Managed(std.http.Header).init(c.arena);
        try hdrs.append(.{ .name = "x-ms-date", .value = date });
        try hdrs.append(.{ .name = "x-ms-version", .value = api_version });
        try hdrs.append(.{ .name = "Authorization", .value = auth });

        var aw = std.Io.Writer.Allocating.init(c.arena);
        const res = try c.client.fetch(.{
            .method = .GET,
            .location = .{ .url = url },
            .extra_headers = hdrs.items,
            .decompress_buffer = http_client.decompress_direct,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        const body = aw.writer.buffered();
        if (code != 200) return statusToError(code, body);
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
    const url_path = if (endpoint == null)
        try std.fmt.allocPrint(arena, "/{s}", .{container})
    else
        try std.fmt.allocPrint(arena, "/{s}/{s}", .{ account, container });
    const base = if (endpoint) |ep|
        try std.fmt.allocPrint(arena, "{s}{s}", .{ std.mem.trimRight(u8, ep, "/"), url_path })
    else
        try std.fmt.allocPrint(arena, "https://{s}.blob.core.windows.net{s}", .{ account, url_path });
    const ctx = ListPage{
        .arena = arena,
        .client = client,
        .account = account,
        .prefix = prefix,
        .base = base,
        .canon = try canonicalResource(arena, account, url_path),
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

pub fn isPrefix(url: []const u8) bool {
    return isUrl(url) and std.mem.endsWith(u8, url, "/");
}

test "prefix URLs are distinguished from blob URLs and may have an empty prefix" {
    try std.testing.expect(isPrefix("az://a/c/dir/"));
    try std.testing.expect(isPrefix("az://a/c/"));
    try std.testing.expect(!isPrefix("az://a/c/f.csv"));

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
