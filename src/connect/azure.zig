//! Azure Blob Storage over the Blob REST API, authenticated with Shared Key.
//!
//! ADLS Gen2 data is reachable through this endpoint: Gen2 is layered on Blob
//! storage, and the separate DFS endpoint only adds hierarchical-namespace
//! operations (atomic directory rename, POSIX ACLs) that a read/write file
//! pipeline never issues. Azurite implements Blob only — no DFS, no HNS — so
//! going through Blob is what makes the local integration suite possible while
//! staying valid against a real Gen2 account.
//!
//! URLs are `az://<account>/<container>/<path>`. The endpoint defaults to
//! `https://<account>.blob.core.windows.net`; set AZURE_BLOB_ENDPOINT to point
//! at Azurite (which is path-style: `<endpoint>/<account>/<container>/<path>`).
//! The key comes from AZURE_STORAGE_KEY.

const std = @import("std");
const httpx = @import("http.zig");

/// Civil date from a day count since the epoch (Howard Hinnant's algorithm).
/// Duplicated from `exec/eval.zig` rather than imported so this module depends
/// on nothing but std — it is the one piece reachable before any pipeline exists.
fn civilFromDays(z0: i64) struct { y: i64, m: u32, d: u32 } {
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

/// x-ms-version sent on every request. Shared Key signing is stable across
/// versions; this only needs to be recent enough for the operations used.
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
    /// The container exists and is readable; nothing is stored under the prefix.
    /// Distinct from a genuinely empty object, because the overwhelmingly common
    /// cause is a mistyped prefix in an otherwise full lake.
    AzureEmptyPrefix,
};

pub const Blob = struct {
    account: []const u8,
    container: []const u8,
    /// Blob path within the container, no leading slash.
    path: []const u8,
    /// Absolute request URL, endpoint style already applied.
    url: []const u8,
    /// `/<account>/<container>/<path>` — what Shared Key signs, identical for
    /// both host-style and path-style endpoints.
    canonical_resource: []const u8,
};

pub fn isUrl(s: []const u8) bool {
    return std.mem.startsWith(u8, s, "az://");
}

/// Splits `az://account/container/path...`. The path may contain slashes; the
/// container is only the first segment after the account.
///
/// `endpoint` null selects the real service (host-style,
/// `https://<account>.blob.core.windows.net/<container>/<path>`); non-null
/// selects a path-style emulator such as Azurite, where the account is itself a
/// path segment. That distinction changes the *signature*, not just the URL —
/// see `canonicalResource`.
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

/// `/<account>` followed by the request URL's path.
///
/// The subtlety: against a path-style emulator the URL path *already begins with
/// the account*, so the account legitimately appears twice
/// (`/devstoreaccount1/devstoreaccount1/lake/f.csv`). Getting this wrong is an
/// opaque 403 with no hint, which is why it is a named function with a test.
pub fn canonicalResource(arena: std.mem.Allocator, account: []const u8, url_path: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "/{s}{s}", .{ account, url_path });
}

pub fn endpointFromEnv(arena: std.mem.Allocator) ?[]const u8 {
    return std.process.getEnvVarOwned(arena, env_endpoint) catch null;
}

pub fn keyFromEnv(arena: std.mem.Allocator) ![]const u8 {
    return std.process.getEnvVarOwned(arena, env_key) catch return Error.AzureMissingKey;
}

/// `Sun, 06 Nov 1994 08:49:37 GMT` — the only date format Shared Key accepts.
pub fn rfc1123(arena: std.mem.Allocator, epoch_secs: i64) ![]const u8 {
    const days = @divFloor(epoch_secs, 86400);
    const secs_of_day = @as(u32, @intCast(epoch_secs - days * 86400));
    const c = civilFromDays(days);
    // 1970-01-01 was a Thursday; shift so 0 = Sunday.
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

/// One `x-ms-*` header. They participate in the signature, so they are kept
/// together with the request rather than rebuilt at send time.
pub const MsHeader = struct { name: []const u8, value: []const u8 };

pub const SignParams = struct {
    method: []const u8,
    canonical_resource: []const u8,
    /// Sorted lexicographically by the caller-independent path below.
    ms_headers: []const MsHeader,
    content_length: usize = 0,
    content_type: []const u8 = "",
    /// `bytes=a-b`, signed verbatim when present.
    range: []const u8 = "",
    /// Query params, `name=value`, lowercase names, sorted by name.
    query: []const []const u8 = &.{},
};

/// Builds the `Authorization: SharedKey ...` value.
///
/// StringToSign is a fixed 12-line preamble (most lines empty for the requests
/// this driver makes) followed by the canonicalized `x-ms-*` headers and the
/// canonicalized resource. Getting a single newline wrong yields an opaque 403,
/// so the layout below is written out one line per field deliberately.
pub fn authHeader(arena: std.mem.Allocator, account: []const u8, key_b64: []const u8, p: SignParams) ![]const u8 {
    var sts = std.array_list.Managed(u8).init(arena);
    const w = sts.writer();

    try w.print("{s}\n", .{p.method});
    try w.writeAll("\n"); // Content-Encoding
    try w.writeAll("\n"); // Content-Language
    // Content-Length is the empty string when zero, not "0".
    if (p.content_length > 0) try w.print("{d}", .{p.content_length});
    try w.writeAll("\n");
    try w.writeAll("\n"); // Content-MD5
    try w.print("{s}\n", .{p.content_type});
    try w.writeAll("\n"); // Date — empty, x-ms-date is used instead
    try w.writeAll("\n"); // If-Modified-Since
    try w.writeAll("\n"); // If-Match
    try w.writeAll("\n"); // If-None-Match
    try w.writeAll("\n"); // If-Unmodified-Since
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

/// Signed headers for a plain GET of a whole blob. `range` is `bytes=a-b` for a
/// partial read, or empty for the whole object; it participates in the
/// signature, so it cannot be added to the request afterwards.
pub fn getHeaders(arena: std.mem.Allocator, b: Blob, range: []const u8) ![]const std.http.Header {
    const key = try keyFromEnv(arena);
    const date = try rfc1123(arena, std.time.timestamp());
    const ms = [_]MsHeader{
        .{ .name = "x-ms-date", .value = date },
        .{ .name = "x-ms-version", .value = api_version },
    };
    const auth = try authHeader(arena, b.account, key, .{
        .method = "GET",
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

/// Pulls `<Code>` and `<Message>` out of an Azure error body. Every failure
/// response carries them; without this a caller sees only a bare status and the
/// cause (expired key? clock skew? wrong container?) is guesswork.
pub fn parseError(body: []const u8) ?struct { code: []const u8, message: []const u8 } {
    const code = extractTag(body, "Code") orelse return null;
    const msg = extractTag(body, "Message") orelse "";
    // The message carries RequestId/Time on following lines; the first is enough.
    const first = std.mem.sliceTo(msg, '\n');
    return .{ .code = code, .message = first };
}

fn extractTag(xml: []const u8, name: []const u8) ?[]const u8 {
    var open_buf: [64]u8 = undefined;
    var close_buf: [64]u8 = undefined;
    const open = std.fmt.bufPrint(&open_buf, "<{s}>", .{name}) catch return null;
    const close = std.fmt.bufPrint(&close_buf, "</{s}>", .{name}) catch return null;
    const s = std.mem.indexOf(u8, xml, open) orelse return null;
    const from = s + open.len;
    const e = std.mem.indexOfPos(u8, xml, from, close) orelse return null;
    return xml[from..e];
}

/// Maps a status plus Azure's error code onto a distinct Zig error, so callers
/// can react (and users can read a failure) instead of seeing one catch-all.
pub fn statusToError(code: u16, body: []const u8) Error {
    if (parseError(body)) |e| {
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

/// Azure throttles with 429/503 under load and returns 500 on transient
/// internal faults. Every request this module makes is idempotent — Put Block
/// is keyed by block id, Put Block List is a full replace, GET is a read — so
/// retrying is always safe.
pub fn retriable(code: u16) bool {
    return code == 429 or code == 500 or code == 503;
}

pub const max_attempts = 5;

/// Exponential backoff with jitter. Jitter matters here specifically: N parallel
/// lanes throttled at the same instant would otherwise retry in lockstep and
/// re-throttle each other.
///
/// Azure also sends `Retry-After` on 503, which would beat guessing — but
/// `std.http.Client.fetch` does not surface response headers, and dropping to
/// the lower-level request API for one hint is not worth it until throttling is
/// observed in practice.
pub fn backoffMs(attempt: usize, rand: std.Random) u64 {
    const base = @as(u64, 200) << @intCast(@min(attempt, 5));
    return base + rand.uintLessThan(u64, base / 2 + 1);
}

/// Staging block size. Azure allows up to 50,000 blocks per blob, so 4 MiB
/// blocks cap a single object at ~190 GiB — far past anything this writes — while
/// keeping resident memory to one block.
pub const block_size = 4 * 1024 * 1024;

/// Streams a block blob: content accumulates into one `block_size` buffer, each
/// full buffer is staged with Put Block, and `finish` commits the ordered list
/// with Put Block List. Memory stays at one block regardless of object size,
/// which is what keeps the pipeline's constant-RSS property intact — buffering
/// the whole object for a single Put Blob would not.
///
/// There is no abort call to make: uncommitted blocks are invisible (the blob
/// does not exist until the block list is committed) and Azure garbage-collects
/// them after a week. Dropping the writer without `finish` is the abort.
pub const BlockBlobWriter = struct {
    interface: std.Io.Writer,
    arena: std.mem.Allocator,
    client: *std.http.Client,
    blob: Blob,
    key: []const u8,
    block_ids: std.array_list.Managed([]const u8),
    content_type: []const u8,
    /// Azure's `<Code>: <Message>` from the last failure, for the caller to log.
    last_error: []const u8 = "",
    /// The typed error behind that failure. Block staging runs under
    /// `std.Io.Writer`, whose error set is only `WriteFailed`; keeping the real
    /// one here lets the sink re-raise it instead of reporting a generic write
    /// failure for what was really a 403 or a missing container.
    last_status: ?Error = null,
    rand: std.Random.DefaultPrng,

    const vtable = std.Io.Writer.VTable{ .drain = drainFn };

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
            // Jitter only needs to decorrelate lanes, not be unpredictable.
            .rand = std.Random.DefaultPrng.init(@bitCast(std.time.milliTimestamp())),
        };
        return self;
    }

    fn drainFn(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *BlockBlobWriter = @fieldParentPtr("interface", w);
        var total: usize = 0;
        total += try self.put(w.buffered());
        for (data[0 .. data.len - 1]) |d| total += try self.put(d);
        const last = data[data.len - 1];
        for (0..splat) |_| total += try self.put(last);
        return w.consume(total);
    }

    fn put(self: *BlockBlobWriter, bytes: []const u8) std.Io.Writer.Error!usize {
        if (bytes.len == 0) return 0;
        self.stageBlock(bytes) catch return error.WriteFailed;
        return bytes.len;
    }

    /// Block IDs must all be the same length and base64-encoded; the commit list
    /// is what defines order, so a simple counter is enough.
    fn blockId(self: *BlockBlobWriter, n: usize) ![]const u8 {
        var raw: [16]u8 = undefined;
        _ = try std.fmt.bufPrint(&raw, "blk{d:0>13}", .{n});
        const enc = std.base64.standard.Encoder;
        const out = try self.arena.alloc(u8, enc.calcSize(raw.len));
        _ = enc.encode(out, &raw);
        return out;
    }

    fn stageBlock(self: *BlockBlobWriter, bytes: []const u8) !void {
        const id = try self.blockId(self.block_ids.items.len);
        const id_enc = try urlEncode(self.arena, id);
        const url = try std.fmt.allocPrint(self.arena, "{s}?comp=block&blockid={s}", .{ self.blob.url, id_enc });

        const date = try rfc1123(self.arena, std.time.timestamp());
        const ms = [_]MsHeader{
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        };
        // Canonicalized query params are sorted by name: blockid before comp.
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
            // Fresh destination: create the container, then retry this block once.
            Error.AzureContainerMissing => {
                try self.createContainer();
                try self.send(url, date, auth, bytes, &.{});
            },
            else => return e,
        };
        try self.block_ids.append(id);
    }

    /// Commits the staged blocks in order. Until this returns, the blob does not
    /// exist as far as any reader is concerned.
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

        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            var aw = std.Io.Writer.Allocating.init(self.arena);
            const res = try self.client.fetch(.{
                .method = .PUT,
                .location = .{ .url = url },
                .extra_headers = hdrs.items,
                .payload = body,
                .decompress_buffer = httpx.decompress_direct,
                .response_writer = &aw.writer,
            });
            const code = @intFromEnum(res.status);
            // Cleared on success: `stageBlock` retries a missing container, and a
            // stale code from that first attempt must not be re-raised later.
            if (code == 201 or code == 200) {
                self.last_status = null;
                return;
            }

            const resp = aw.writer.buffered();
            if (retriable(code) and attempt + 1 < max_attempts) {
                std.Thread.sleep(backoffMs(attempt, self.rand.random()) * std.time.ns_per_ms);
                continue;
            }
            self.last_error = describe(self.arena, code, resp) catch "";
            self.last_status = statusToError(code, resp);
            return self.last_status.?;
        }
    }

    /// Create the container, ignoring "already exists". Called once after a 404
    /// so writing to a fresh destination works without a separate setup step.
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
            .decompress_buffer = httpx.decompress_direct,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        // 409 = already there, which is success for our purposes.
        if (code != 201 and code != 409) {
            self.last_error = describe(self.arena, code, aw.writer.buffered()) catch "";
            self.last_status = statusToError(code, aw.writer.buffered());
            return self.last_status.?;
        }
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

test "block ids are fixed-width base64 so ordering is stable past 10 blocks" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var client: std.http.Client = undefined;
    const blob = try parseUrl(a, "az://acct/c/f.csv", null);
    // key must be valid base64 for init to succeed via keyFromEnv; call blockId
    // on a hand-built struct instead of touching the environment.
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
    const b9 = try w.blockId(9);
    const b10 = try w.blockId(10);
    try std.testing.expectEqual(b0.len, b10.len);
    try std.testing.expect(std.mem.lessThan(u8, b9, b10));
    try std.testing.expectEqualStrings("%2B", try urlEncode(a, "+"));
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

    // Regression guard: Azurite's URL path already starts with the account, and
    // the canonicalized resource is "/" + account + that path — so the account
    // appears twice. Signing it once yields a bare 403 with no diagnostic.
    const b = try parseUrl(a, "az://devstoreaccount1/lake/in.csv", "http://127.0.0.1:31000");
    try std.testing.expectEqualStrings("http://127.0.0.1:31000/devstoreaccount1/lake/in.csv", b.url);
    try std.testing.expectEqualStrings("/devstoreaccount1/devstoreaccount1/lake/in.csv", b.canonical_resource);

    // a trailing slash on the endpoint must not double up
    const c = try parseUrl(a, "az://devstoreaccount1/lake/in.csv", "http://127.0.0.1:31000/");
    try std.testing.expectEqualStrings(b.url, c.url);
}

test "rfc1123 matches the reference date from the Azure signing docs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // 1994-11-06T08:49:37Z, a Sunday.
    try std.testing.expectEqualStrings("Sun, 06 Nov 1994 08:49:37 GMT", try rfc1123(a, 784111777));
    try std.testing.expectEqualStrings("Thu, 01 Jan 1970 00:00:00 GMT", try rfc1123(a, 0));
}

test "authHeader: canonical headers are sorted and Content-Length 0 signs as empty" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    // headers deliberately out of order: the signature must not depend on it
    const h1 = try authHeader(a, "devstoreaccount1", key, .{
        .method = "GET",
        .canonical_resource = "/devstoreaccount1/c/f.csv",
        .ms_headers = &.{
            .{ .name = "x-ms-version", .value = api_version },
            .{ .name = "x-ms-date", .value = "Sun, 06 Nov 1994 08:49:37 GMT" },
        },
    });
    const h2 = try authHeader(a, "devstoreaccount1", key, .{
        .method = "GET",
        .canonical_resource = "/devstoreaccount1/c/f.csv",
        .ms_headers = &.{
            .{ .name = "x-ms-date", .value = "Sun, 06 Nov 1994 08:49:37 GMT" },
            .{ .name = "x-ms-version", .value = api_version },
        },
    });
    try std.testing.expectEqualStrings(h1, h2);
    try std.testing.expect(std.mem.startsWith(u8, h1, "SharedKey devstoreaccount1:"));

    // a different verb must produce a different signature
    const put = try authHeader(a, "devstoreaccount1", key, .{
        .method = "PUT",
        .canonical_resource = "/devstoreaccount1/c/f.csv",
        .ms_headers = &.{.{ .name = "x-ms-date", .value = "Sun, 06 Nov 1994 08:49:37 GMT" }},
    });
    try std.testing.expect(!std.mem.eql(u8, h1, put));
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

/// `Code: Message (HTTP nnn)` — what a user needs to see instead of a status.
pub fn describe(arena: std.mem.Allocator, code: u16, body: []const u8) ![]const u8 {
    if (parseError(body)) |e|
        return std.fmt.allocPrint(arena, "{s}: {s} (HTTP {d})", .{ e.code, e.message, code });
    return std.fmt.allocPrint(arena, "HTTP {d}", .{code});
}

/// Lists blob names under `prefix`, following continuation markers to the end.
/// Names come back container-relative, in the lexicographic order Azure returns
/// them, so a caller reading them in order gets a deterministic result.
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
    const canon = try canonicalResource(arena, account, url_path);
    const key = try keyFromEnv(arena);

    var out = std.array_list.Managed([]const u8).init(arena);
    var marker: []const u8 = "";
    while (true) {
        const url = if (marker.len == 0)
            try std.fmt.allocPrint(arena, "{s}?restype=container&comp=list&prefix={s}", .{ base, try urlEncode(arena, prefix) })
        else
            try std.fmt.allocPrint(arena, "{s}?restype=container&comp=list&prefix={s}&marker={s}", .{ base, try urlEncode(arena, prefix), try urlEncode(arena, marker) });

        const date = try rfc1123(arena, std.time.timestamp());
        const ms = [_]MsHeader{
            .{ .name = "x-ms-date", .value = date },
            .{ .name = "x-ms-version", .value = api_version },
        };
        // Canonicalized query params sort by name: comp, marker, prefix, restype.
        var q = std.array_list.Managed([]const u8).init(arena);
        try q.append("comp:list");
        if (marker.len > 0) try q.append(try std.fmt.allocPrint(arena, "marker:{s}", .{marker}));
        try q.append(try std.fmt.allocPrint(arena, "prefix:{s}", .{prefix}));
        try q.append("restype:container");

        const auth = try authHeader(arena, account, key, .{
            .method = "GET",
            .canonical_resource = canon,
            .ms_headers = &ms,
            .query = q.items,
        });

        var hdrs = std.array_list.Managed(std.http.Header).init(arena);
        try hdrs.append(.{ .name = "x-ms-date", .value = date });
        try hdrs.append(.{ .name = "x-ms-version", .value = api_version });
        try hdrs.append(.{ .name = "Authorization", .value = auth });

        var aw = std.Io.Writer.Allocating.init(arena);
        const res = try client.fetch(.{
            .method = .GET,
            .location = .{ .url = url },
            .extra_headers = hdrs.items,
            .decompress_buffer = httpx.decompress_direct,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        const body = aw.writer.buffered();
        if (code != 200) return statusToError(code, body);

        try collectNames(arena, body, &out);
        const next = extractTag(body, "NextMarker") orelse "";
        if (next.len == 0) break;
        marker = try arena.dupe(u8, next);
    }
    return out.toOwnedSlice();
}

/// Every `<Name>` inside the `<Blobs>` element. A flat listing (no delimiter)
/// returns only blobs, so no BlobPrefix entries can be confused for one.
fn collectNames(arena: std.mem.Allocator, xml: []const u8, out: *std.array_list.Managed([]const u8)) !void {
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Name>")) |s| {
        const from = s + "<Name>".len;
        const e = std.mem.indexOfPos(u8, xml, from, "</Name>") orelse break;
        try out.append(try arena.dupe(u8, xml[from..e]));
        pos = e + "</Name>".len;
    }
}

test "parseError pulls the code and first message line out of an Azure fault" {
    const body =
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<Error>
        \\  <Code>AuthorizationFailure</Code>
        \\  <Message>Server failed to authenticate the request.
        \\RequestId:abc
        \\Time:2026-07-25T17:09:10.903Z</Message>
        \\</Error>
    ;
    const e = parseError(body).?;
    try std.testing.expectEqualStrings("AuthorizationFailure", e.code);
    try std.testing.expectEqualStrings("Server failed to authenticate the request.", e.message);
    try std.testing.expect(parseError("not xml") == null);
}

test "statusToError distinguishes causes instead of one catch-all" {
    try std.testing.expectEqual(Error.AzureAuthFailed, statusToError(403, "<Error><Code>AuthorizationFailure</Code></Error>"));
    try std.testing.expectEqual(Error.AzureContainerMissing, statusToError(404, "<Error><Code>ContainerNotFound</Code></Error>"));
    try std.testing.expectEqual(Error.AzureBlobNotFound, statusToError(404, "<Error><Code>BlobNotFound</Code></Error>"));
    try std.testing.expectEqual(Error.AzureThrottled, statusToError(503, ""));
    try std.testing.expectEqual(Error.AzureRequestFailed, statusToError(418, ""));
}

test "retry policy: only transient statuses, and jitter never collapses to zero spread" {
    try std.testing.expect(retriable(429) and retriable(500) and retriable(503));
    try std.testing.expect(!retriable(403) and !retriable(404) and !retriable(201));

    var prng = std.Random.DefaultPrng.init(1);
    const r = prng.random();
    // backoff grows with the attempt and stays within [base, base*1.5]
    var prev: u64 = 0;
    for (0..5) |i| {
        const base = @as(u64, 200) << @intCast(i);
        const ms = backoffMs(i, r);
        try std.testing.expect(ms >= base and ms <= base + base / 2 + 1);
        try std.testing.expect(ms > prev);
        prev = base;
    }
}

test "collectNames reads every blob name from a listing page" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var out = std.array_list.Managed([]const u8).init(a);
    try collectNames(a,
        "<Blobs><Blob><Name>p/a.csv</Name></Blob><Blob><Name>p/b.csv</Name></Blob></Blobs><NextMarker/>",
        &out);
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqualStrings("p/a.csv", out.items[0]);
    try std.testing.expectEqualStrings("p/b.csv", out.items[1]);
}

/// Splits a prefix URL (`az://account/container/some/prefix/`) for listing. The
/// prefix may be empty (`az://account/container/`), unlike `parseUrl`, which
/// addresses one blob and requires a path.
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

/// A trailing slash means "every blob under this prefix", not one blob.
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
