//! Amazon S3 (and S3-compatible stores: MinIO, localstack) over the REST API,
//! authenticated with AWS Signature Version 4.
//!
//! URLs are `s3://<bucket>/<key>`. With no endpoint override the real service
//! is addressed virtual-host style (`https://<bucket>.s3.<region>.amazonaws.com/<key>`,
//! region from AWS_REGION, default us-east-1). Setting AWS_ENDPOINT_URL selects
//! path-style (`<endpoint>/<bucket>/<key>`), which is what MinIO and the other
//! emulators require. Credentials come from AWS_ACCESS_KEY_ID /
//! AWS_SECRET_ACCESS_KEY (+ optional AWS_SESSION_TOKEN), environment only.
//!
//! Every request signs the real payload hash (the empty-body SHA-256 for
//! bodyless verbs) — no UNSIGNED-PAYLOAD anywhere, so requests are integrity-
//! protected even over the plain-HTTP endpoints emulators use.

const std = @import("std");
const httpx = @import("http.zig");

/// Civil date from a day count since the epoch (Howard Hinnant's algorithm).
/// Duplicated from `exec/eval.zig` rather than imported so this module depends
/// on nothing but std — same reasoning as azure.zig.
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

pub const env_key_id = "AWS_ACCESS_KEY_ID";
pub const env_secret = "AWS_SECRET_ACCESS_KEY";
pub const env_token = "AWS_SESSION_TOKEN";
pub const env_region = "AWS_REGION";
pub const env_endpoint = "AWS_ENDPOINT_URL";

pub const default_region = "us-east-1";

pub const Error = error{
    S3BadUrl,
    S3MissingKey,
    S3RequestFailed,
    S3BucketMissing,
    S3KeyNotFound,
    S3AuthFailed,
    S3Throttled,
    /// The bucket exists and is readable; nothing is stored under the prefix.
    /// Distinct from a genuinely empty object, because the overwhelmingly common
    /// cause is a mistyped prefix in an otherwise full lake.
    S3EmptyPrefix,
};

pub const Obj = struct {
    bucket: []const u8,
    /// Object key within the bucket, no leading slash, as written (unencoded).
    key: []const u8,
    /// Absolute request URL, endpoint style already applied, path segments
    /// percent-encoded exactly as the canonical URI is — the two must match
    /// byte-for-byte or the signature check fails.
    url: []const u8,
    /// The Host header value std.http.Client will derive from `url` (authority
    /// including any explicit port). Host participates in the signature.
    host: []const u8,
    /// Canonical URI for SigV4: the URL's path component, single-encoded.
    uri_path: []const u8,
    region: []const u8,
    /// The bucket root (no key), for CreateBucket on a fresh destination.
    bucket_url: []const u8,
    bucket_uri_path: []const u8,
};

pub fn isUrl(s: []const u8) bool {
    return std.mem.startsWith(u8, s, "s3://");
}

/// A trailing slash means "every object under this prefix", not one object.
pub fn isPrefix(url: []const u8) bool {
    return isUrl(url) and std.mem.endsWith(u8, url, "/");
}

pub fn endpointFromEnv(arena: std.mem.Allocator) ?[]const u8 {
    return std.process.getEnvVarOwned(arena, env_endpoint) catch null;
}

pub fn regionFromEnv(arena: std.mem.Allocator) []const u8 {
    return std.process.getEnvVarOwned(arena, env_region) catch default_region;
}

pub const Creds = struct {
    access: []const u8,
    secret: []const u8,
    /// AWS_SESSION_TOKEN when present (STS / role credentials); sent and signed
    /// as x-amz-security-token.
    token: ?[]const u8 = null,
};

pub fn credsFromEnv(arena: std.mem.Allocator) !Creds {
    const access = std.process.getEnvVarOwned(arena, env_key_id) catch return Error.S3MissingKey;
    const secret = std.process.getEnvVarOwned(arena, env_secret) catch return Error.S3MissingKey;
    const token = std.process.getEnvVarOwned(arena, env_token) catch null;
    return .{ .access = access, .secret = secret, .token = token };
}

/// The authority (host[:port]) of an endpoint URL. std.http.Client emits the
/// Host header as the URI's authority including any explicit port, so this is
/// exactly the string that must be signed when a MinIO endpoint carries a port.
fn authorityOf(endpoint: []const u8) ![]const u8 {
    const after = if (std.mem.startsWith(u8, endpoint, "https://"))
        endpoint["https://".len..]
    else if (std.mem.startsWith(u8, endpoint, "http://"))
        endpoint["http://".len..]
    else
        return Error.S3BadUrl;
    const host = std.mem.sliceTo(after, '/');
    if (host.len == 0) return Error.S3BadUrl;
    return host;
}

/// Splits `s3://bucket/key...`. The key may contain slashes; the bucket is only
/// the first segment.
///
/// `endpoint` null selects the real service (virtual-host style); non-null
/// selects path-style against that endpoint (MinIO, localstack). That
/// distinction changes both the signed Host and the canonical URI.
pub fn parseUrl(arena: std.mem.Allocator, url: []const u8, endpoint: ?[]const u8) !Obj {
    if (!isUrl(url)) return Error.S3BadUrl;
    const rest = url["s3://".len..];
    const b_end = std.mem.indexOfScalar(u8, rest, '/') orelse return Error.S3BadUrl;
    const bucket = rest[0..b_end];
    const key = rest[b_end + 1 ..];
    if (bucket.len == 0 or key.len == 0) return Error.S3BadUrl;

    const region = regionFromEnv(arena);
    const enc_key = try uriEncode(arena, key, .keep_slash);
    const base = try bucketBase(arena, bucket, region, endpoint);
    // Path style: bucket base has no trailing slash, the key needs a separator.
    // Virtual-host style: the base URL is `https://host/` and the base URI `/`.
    const sep: []const u8 = if (std.mem.endsWith(u8, base.uri_path, "/")) "" else "/";
    return .{
        .bucket = bucket,
        .key = key,
        .url = try std.fmt.allocPrint(arena, "{s}{s}{s}", .{ base.url, sep, enc_key }),
        .host = base.host,
        .uri_path = try std.fmt.allocPrint(arena, "{s}{s}{s}", .{ base.uri_path, sep, enc_key }),
        .region = region,
        .bucket_url = base.url,
        .bucket_uri_path = base.uri_path,
    };
}

/// Splits a prefix URL (`s3://bucket/some/prefix/`) for listing. The prefix may
/// be empty (`s3://bucket/`), unlike `parseUrl`, which addresses one object and
/// requires a key.
pub fn parsePrefix(url: []const u8) !struct { bucket: []const u8, prefix: []const u8 } {
    if (!isUrl(url)) return Error.S3BadUrl;
    const rest = url["s3://".len..];
    const b_end = std.mem.indexOfScalar(u8, rest, '/') orelse return Error.S3BadUrl;
    const bucket = rest[0..b_end];
    if (bucket.len == 0) return Error.S3BadUrl;
    return .{ .bucket = bucket, .prefix = rest[b_end + 1 ..] };
}

/// `20130524T000000Z` — the ISO-basic instant SigV4 signs (x-amz-date). The
/// first 8 characters are the credential-scope date.
pub fn amzDate(arena: std.mem.Allocator, epoch_secs: i64) ![]const u8 {
    const days = @divFloor(epoch_secs, 86400);
    const secs: u32 = @intCast(epoch_secs - days * 86400);
    const c = civilFromDays(days);
    return std.fmt.allocPrint(arena, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
        @as(u32, @intCast(c.y)), c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
    });
}

const EncodeSlash = enum { keep_slash, encode_slash };

/// SigV4 URI encoding: unreserved characters pass, everything else becomes
/// %XX (uppercase hex). S3 canonical URIs are single-encoded with '/' kept;
/// query values encode '/' too.
fn uriEncode(arena: std.mem.Allocator, s: []const u8, slash: EncodeSlash) ![]const u8 {
    const hex = "0123456789ABCDEF";
    var out = std.array_list.Managed(u8).init(arena);
    for (s) |c| {
        const unreserved = std.ascii.isAlphanumeric(c) or c == '-' or c == '.' or c == '_' or c == '~';
        if (unreserved or (slash == .keep_slash and c == '/')) {
            try out.append(c);
        } else {
            try out.append('%');
            try out.append(hex[c >> 4]);
            try out.append(hex[c & 0xF]);
        }
    }
    return out.toOwnedSlice();
}

/// SHA-256 of the request payload, lowercase hex — the x-amz-content-sha256
/// value and the last line of the canonical request.
pub fn payloadHash(arena: std.mem.Allocator, payload: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    return arena.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}

/// SHA-256 of the empty string: the payload hash of every bodyless request.
pub const empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// One header participating in the signature. Names must be lowercase; values
/// are signed verbatim (ours are all machine-built, never needing whitespace
/// canonicalization).
pub const SignHeader = struct { name: []const u8, value: []const u8 };

pub const SignParams = struct {
    method: []const u8,
    /// Canonical URI: the URL's path, single-encoded, starting with '/'.
    uri_path: []const u8,
    /// Query params as pre-encoded `name=value` strings; sorted here. A
    /// valueless param signs as `name=`.
    query: []const []const u8 = &.{},
    /// Signed headers (must include host); sorted here.
    headers: []const SignHeader,
    payload_hash: []const u8,
    /// `20130524T000000Z` — must equal the x-amz-date header value.
    timestamp: []const u8,
    region: []const u8,
    service: []const u8 = "s3",
};

/// Canonical request text plus the `;`-joined signed-headers list. A named
/// function because a single byte of drift here yields SignatureDoesNotMatch
/// with no further hint; the tests pin it to AWS's published examples.
fn canonicalRequest(arena: std.mem.Allocator, p: SignParams) !struct { text: []const u8, signed: []const u8 } {
    const hdrs = try arena.dupe(SignHeader, p.headers);
    std.mem.sort(SignHeader, hdrs, {}, struct {
        fn lt(_: void, x: SignHeader, y: SignHeader) bool {
            return std.mem.lessThan(u8, x.name, y.name);
        }
    }.lt);
    const q = try arena.dupe([]const u8, p.query);
    std.mem.sort([]const u8, q, {}, struct {
        fn lt(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.lessThan(u8, x, y);
        }
    }.lt);

    var signed = std.array_list.Managed(u8).init(arena);
    for (hdrs, 0..) |h, i| {
        if (i > 0) try signed.append(';');
        try signed.appendSlice(h.name);
    }

    var buf = std.array_list.Managed(u8).init(arena);
    const w = buf.writer();
    try w.print("{s}\n{s}\n", .{ p.method, p.uri_path });
    for (q, 0..) |s, i| {
        if (i > 0) try w.writeAll("&");
        try w.writeAll(s);
    }
    try w.writeAll("\n");
    for (hdrs) |h| try w.print("{s}:{s}\n", .{ h.name, h.value });
    try w.writeAll("\n");
    try w.print("{s}\n{s}", .{ signed.items, p.payload_hash });
    return .{ .text = try buf.toOwnedSlice(), .signed = try signed.toOwnedSlice() };
}

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// The derived SigV4 signing key: HMAC chain over date, region, service,
/// "aws4_request", rooted at "AWS4" + secret.
pub fn signingKey(arena: std.mem.Allocator, secret: []const u8, date: []const u8, region: []const u8, service: []const u8) ![32]u8 {
    const seed = try std.fmt.allocPrint(arena, "AWS4{s}", .{secret});
    var k: [32]u8 = undefined;
    HmacSha256.create(&k, date, seed);
    HmacSha256.create(&k, region, &k);
    HmacSha256.create(&k, service, &k);
    HmacSha256.create(&k, "aws4_request", &k);
    return k;
}

/// Builds the `Authorization: AWS4-HMAC-SHA256 ...` value: canonical request →
/// string to sign → derived key → signature.
pub fn authHeader(arena: std.mem.Allocator, access: []const u8, secret: []const u8, p: SignParams) ![]const u8 {
    const cr = try canonicalRequest(arena, p);
    var cr_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(cr.text, &cr_hash, .{});

    const date = p.timestamp[0..8];
    const sts = try std.fmt.allocPrint(arena, "AWS4-HMAC-SHA256\n{s}\n{s}/{s}/{s}/aws4_request\n{s}", .{
        p.timestamp, date, p.region, p.service, &std.fmt.bytesToHex(cr_hash, .lower),
    });

    const key = try signingKey(arena, secret, date, p.region, p.service);
    var mac: [32]u8 = undefined;
    HmacSha256.create(&mac, sts, &key);

    return std.fmt.allocPrint(
        arena,
        "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/{s}/aws4_request,SignedHeaders={s},Signature={s}",
        .{ access, date, p.region, p.service, cr.signed, &std.fmt.bytesToHex(mac, .lower) },
    );
}

/// Signed headers for a plain GET of a whole object. `range` is `bytes=a-b`
/// for a partial read, or empty for the whole object; it participates in the
/// signature, so it cannot be added to the request afterwards.
pub fn getHeaders(arena: std.mem.Allocator, o: Obj, range: []const u8) ![]const std.http.Header {
    return requestHeaders(arena, o, "GET", range);
}

/// `getHeaders` for any verb. HEAD is the one other verb a reader needs — it
/// answers "how big is this object?" without a body — and SigV4 signs the verb,
/// so it cannot reuse the GET signature. Host is signed but NOT returned:
/// std.http.Client derives it from the URL, and `Obj.host` is that same
/// authority by construction.
pub fn requestHeaders(
    arena: std.mem.Allocator,
    o: Obj,
    method: []const u8,
    range: []const u8,
) ![]const std.http.Header {
    const creds = try credsFromEnv(arena);
    const ts = try amzDate(arena, std.time.timestamp());

    var sh = std.array_list.Managed(SignHeader).init(arena);
    try sh.append(.{ .name = "host", .value = o.host });
    if (range.len > 0) try sh.append(.{ .name = "range", .value = range });
    try sh.append(.{ .name = "x-amz-content-sha256", .value = empty_payload_hash });
    try sh.append(.{ .name = "x-amz-date", .value = ts });
    if (creds.token) |t| try sh.append(.{ .name = "x-amz-security-token", .value = t });

    const auth = try authHeader(arena, creds.access, creds.secret, .{
        .method = method,
        .uri_path = o.uri_path,
        .headers = sh.items,
        .payload_hash = empty_payload_hash,
        .timestamp = ts,
        .region = o.region,
    });

    var out = std.array_list.Managed(std.http.Header).init(arena);
    try out.append(.{ .name = "x-amz-date", .value = ts });
    try out.append(.{ .name = "x-amz-content-sha256", .value = empty_payload_hash });
    if (creds.token) |t| try out.append(.{ .name = "x-amz-security-token", .value = t });
    try out.append(.{ .name = "Authorization", .value = auth });
    if (range.len > 0) try out.append(.{ .name = "Range", .value = range });
    return out.toOwnedSlice();
}

/// Pulls `<Code>` and `<Message>` out of an S3 error body. Every failure
/// response carries them; without this a caller sees only a bare status and the
/// cause (bad key? clock skew? wrong bucket?) is guesswork.
pub fn parseError(body: []const u8) ?struct { code: []const u8, message: []const u8 } {
    const code = extractTag(body, "Code") orelse return null;
    const msg = extractTag(body, "Message") orelse "";
    return .{ .code = code, .message = std.mem.sliceTo(msg, '\n') };
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

/// Maps a status plus S3's error code onto a distinct Zig error, so callers can
/// react (and users can read a failure) instead of seeing one catch-all.
pub fn statusToError(code: u16, body: []const u8) Error {
    if (parseError(body)) |e| {
        if (std.mem.eql(u8, e.code, "NoSuchBucket")) return Error.S3BucketMissing;
        if (std.mem.eql(u8, e.code, "NoSuchKey")) return Error.S3KeyNotFound;
        if (std.mem.eql(u8, e.code, "AccessDenied")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "SignatureDoesNotMatch")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "InvalidAccessKeyId")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "ExpiredToken")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "SlowDown")) return Error.S3Throttled;
    }
    return switch (code) {
        401, 403 => Error.S3AuthFailed,
        404 => Error.S3BucketMissing,
        429, 503 => Error.S3Throttled,
        else => Error.S3RequestFailed,
    };
}

/// S3 throttles with 503 SlowDown (and 429 on some compatibles) and returns 500
/// on transient internal faults. Every request this module makes is idempotent —
/// UploadPart is keyed by part number, CompleteMultipartUpload is a full
/// replace, GET is a read — so retrying is always safe.
pub fn retriable(code: u16) bool {
    return code == 429 or code == 500 or code == 503;
}

pub const max_attempts = 5;

/// Exponential backoff with jitter, same policy as azure.zig: jitter matters
/// because N parallel lanes throttled at the same instant would otherwise retry
/// in lockstep and re-throttle each other.
pub fn backoffMs(attempt: usize, rand: std.Random) u64 {
    const base = @as(u64, 200) << @intCast(@min(attempt, 5));
    return base + rand.uintLessThan(u64, base / 2 + 1);
}

/// `Code: Message (HTTP nnn)` — what a user needs to see instead of a status.
pub fn describe(arena: std.mem.Allocator, code: u16, body: []const u8) ![]const u8 {
    if (parseError(body)) |e|
        return std.fmt.allocPrint(arena, "{s}: {s} (HTTP {d})", .{ e.code, e.message, code });
    return std.fmt.allocPrint(arena, "HTTP {d}", .{code});
}

/// The bucket-level base for listing (and bucket creation): URL, signed host,
/// and canonical URI, in either endpoint style.
fn bucketBase(arena: std.mem.Allocator, bucket: []const u8, region: []const u8, endpoint: ?[]const u8) !struct {
    url: []const u8,
    host: []const u8,
    uri_path: []const u8,
} {
    if (endpoint) |ep| {
        const base = std.mem.trimRight(u8, ep, "/");
        const uri_path = try std.fmt.allocPrint(arena, "/{s}", .{bucket});
        return .{
            .url = try std.fmt.allocPrint(arena, "{s}{s}", .{ base, uri_path }),
            .host = try authorityOf(base),
            .uri_path = uri_path,
        };
    }
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, region });
    return .{
        .url = try std.fmt.allocPrint(arena, "https://{s}/", .{host}),
        .host = host,
        .uri_path = "/",
    };
}

/// Lists object keys under `prefix` (ListObjectsV2), following continuation
/// tokens to the end. Keys come back bucket-relative, in the lexicographic
/// order S3 returns them, so a caller reading them in order gets a
/// deterministic result.
pub fn listPrefix(
    arena: std.mem.Allocator,
    client: *std.http.Client,
    bucket: []const u8,
    prefix: []const u8,
    endpoint: ?[]const u8,
) ![][]const u8 {
    const region = regionFromEnv(arena);
    const creds = try credsFromEnv(arena);
    const base = try bucketBase(arena, bucket, region, endpoint);

    var out = std.array_list.Managed([]const u8).init(arena);
    var token: []const u8 = "";
    while (true) {
        // Query built pre-sorted (continuation-token < list-type < prefix), so
        // the request URL and the canonical query string are the same text.
        var q = std.array_list.Managed([]const u8).init(arena);
        if (token.len > 0)
            try q.append(try std.fmt.allocPrint(arena, "continuation-token={s}", .{try uriEncode(arena, token, .encode_slash)}));
        try q.append("list-type=2");
        try q.append(try std.fmt.allocPrint(arena, "prefix={s}", .{try uriEncode(arena, prefix, .encode_slash)}));

        const ts = try amzDate(arena, std.time.timestamp());
        var sh = std.array_list.Managed(SignHeader).init(arena);
        try sh.append(.{ .name = "host", .value = base.host });
        try sh.append(.{ .name = "x-amz-content-sha256", .value = empty_payload_hash });
        try sh.append(.{ .name = "x-amz-date", .value = ts });
        if (creds.token) |t| try sh.append(.{ .name = "x-amz-security-token", .value = t });
        const auth = try authHeader(arena, creds.access, creds.secret, .{
            .method = "GET",
            .uri_path = base.uri_path,
            .query = q.items,
            .headers = sh.items,
            .payload_hash = empty_payload_hash,
            .timestamp = ts,
            .region = region,
        });

        var hdrs = std.array_list.Managed(std.http.Header).init(arena);
        try hdrs.append(.{ .name = "x-amz-date", .value = ts });
        try hdrs.append(.{ .name = "x-amz-content-sha256", .value = empty_payload_hash });
        if (creds.token) |t| try hdrs.append(.{ .name = "x-amz-security-token", .value = t });
        try hdrs.append(.{ .name = "Authorization", .value = auth });

        var url = std.array_list.Managed(u8).init(arena);
        try url.appendSlice(base.url);
        for (q.items, 0..) |s, i| {
            try url.append(if (i == 0) '?' else '&');
            try url.appendSlice(s);
        }

        var aw = std.Io.Writer.Allocating.init(arena);
        const res = try client.fetch(.{
            .method = .GET,
            .location = .{ .url = url.items },
            .extra_headers = hdrs.items,
            .decompress_buffer = httpx.decompress_direct,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        const body = aw.writer.buffered();
        if (code != 200) return statusToError(code, body);

        try collectKeys(arena, body, &out);
        const next = extractTag(body, "NextContinuationToken") orelse "";
        if (next.len == 0) break;
        token = try arena.dupe(u8, next);
    }
    return out.toOwnedSlice();
}

/// Every `<Key>` in a ListObjectsV2 page. Keys appear only inside `<Contents>`
/// elements, and a flat listing (no delimiter) returns no CommonPrefixes, so a
/// plain tag scan cannot pick up anything else.
fn collectKeys(arena: std.mem.Allocator, xml: []const u8, out: *std.array_list.Managed([]const u8)) !void {
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, "<Key>")) |s| {
        const from = s + "<Key>".len;
        const e = std.mem.indexOfPos(u8, xml, from, "</Key>") orelse break;
        try out.append(try arena.dupe(u8, xml[from..e]));
        pos = e + "</Key>".len;
    }
}

/// Part accumulation size. Multipart parts must be at least 5 MiB except the
/// last; every part staged here is exactly this size, and S3 allows 10,000
/// parts per upload, so 8 MiB parts cap a single object at ~78 GiB — far past
/// anything this writes — while keeping resident memory to one part.
pub const part_size = 8 * 1024 * 1024;

/// Signed header list for one write-path request: signs host, payload hash,
/// date, session token and (when given) content-type, and returns the headers
/// to send. Content-type is signed but NOT returned — it is sent through
/// `std.http.Client`'s own content_type option so only one copy goes out.
fn signedWriteHeaders(
    arena: std.mem.Allocator,
    creds: Creds,
    method: []const u8,
    o: Obj,
    uri_path: []const u8,
    query: []const []const u8,
    payload_hash_hex: []const u8,
    content_type: ?[]const u8,
) ![]const std.http.Header {
    const ts = try amzDate(arena, std.time.timestamp());
    var sh = std.array_list.Managed(SignHeader).init(arena);
    if (content_type) |ct| try sh.append(.{ .name = "content-type", .value = ct });
    try sh.append(.{ .name = "host", .value = o.host });
    try sh.append(.{ .name = "x-amz-content-sha256", .value = payload_hash_hex });
    try sh.append(.{ .name = "x-amz-date", .value = ts });
    if (creds.token) |t| try sh.append(.{ .name = "x-amz-security-token", .value = t });
    const auth = try authHeader(arena, creds.access, creds.secret, .{
        .method = method,
        .uri_path = uri_path,
        .query = query,
        .headers = sh.items,
        .payload_hash = payload_hash_hex,
        .timestamp = ts,
        .region = o.region,
    });
    var out = std.array_list.Managed(std.http.Header).init(arena);
    try out.append(.{ .name = "x-amz-date", .value = ts });
    try out.append(.{ .name = "x-amz-content-sha256", .value = payload_hash_hex });
    if (creds.token) |t| try out.append(.{ .name = "x-amz-security-token", .value = t });
    try out.append(.{ .name = "Authorization", .value = auth });
    return out.toOwnedSlice();
}

/// The CompleteMultipartUpload request body: parts in staging order.
fn completeBody(arena: std.mem.Allocator, etags: []const []const u8) ![]const u8 {
    var body = std.array_list.Managed(u8).init(arena);
    try body.appendSlice("<CompleteMultipartUpload>");
    for (etags, 1..) |etag, n| {
        try body.writer().print("<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>", .{ n, etag });
    }
    try body.appendSlice("</CompleteMultipartUpload>");
    return body.toOwnedSlice();
}

/// Streams an object. Content accumulates into one `part_size` buffer; the
/// first time it fills, a multipart upload starts and each full buffer goes out
/// as one part, with `finish` completing the ordered list. An object that never
/// fills the buffer is written with a plain single PUT instead — no multipart
/// bookkeeping for the common small-file case. Memory stays at one part
/// regardless of object size, which is what keeps the pipeline's constant-RSS
/// property intact.
///
/// Parts accumulate in a private buffer rather than staging the writer's drain
/// chunks directly (azure.zig's approach): S3 rejects any non-final part under
/// 5 MiB at completion time, and drain chunk sizes are not under our control.
///
/// Until `finish` returns, the object does not exist as far as any reader is
/// concerned. Dropping the writer without `finish` leaves an invisible
/// uncommitted upload behind; unlike Azure, S3 only garbage-collects those
/// where the bucket has a lifecycle rule, so an abort call may become worth
/// adding if aborted runs prove common.
pub const MultipartWriter = struct {
    interface: std.Io.Writer,
    arena: std.mem.Allocator,
    client: *std.http.Client,
    obj: Obj,
    creds: Creds,
    content_type: []const u8,
    part_buf: []u8,
    part_len: usize = 0,
    upload_id: ?[]const u8 = null,
    /// URI-encoded once: the id rides in a query string on every part.
    upload_id_enc: []const u8 = "",
    etags: std.array_list.Managed([]const u8),
    created_bucket: bool = false,
    /// S3's `Code: Message (HTTP nnn)` from the last failure, for the caller to log.
    last_error: []const u8 = "",
    /// The typed error behind that failure. Part staging runs under
    /// `std.Io.Writer`, whose error set is only `WriteFailed`; keeping the real
    /// one here lets the sink re-raise it instead of reporting a generic write
    /// failure for what was really a 403 or a missing bucket.
    last_status: ?Error = null,
    rand: std.Random.DefaultPrng,

    const vtable = std.Io.Writer.VTable{ .drain = drainFn };

    pub fn init(
        arena: std.mem.Allocator,
        client: *std.http.Client,
        obj: Obj,
        content_type: []const u8,
    ) !*MultipartWriter {
        const self = try arena.create(MultipartWriter);
        self.* = .{
            .interface = .{ .vtable = &vtable, .buffer = try arena.alloc(u8, 64 * 1024) },
            .arena = arena,
            .client = client,
            .obj = obj,
            .creds = try credsFromEnv(arena),
            .content_type = content_type,
            .part_buf = try arena.alloc(u8, part_size),
            .etags = std.array_list.Managed([]const u8).init(arena),
            // Jitter only needs to decorrelate lanes, not be unpredictable.
            .rand = std.Random.DefaultPrng.init(@bitCast(std.time.milliTimestamp())),
        };
        return self;
    }

    fn drainFn(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *MultipartWriter = @fieldParentPtr("interface", w);
        var total: usize = 0;
        total += try self.put(w.buffered());
        for (data[0 .. data.len - 1]) |d| total += try self.put(d);
        const last = data[data.len - 1];
        for (0..splat) |_| total += try self.put(last);
        return w.consume(total);
    }

    /// Copies into the part buffer, shipping each full part. The copy is what
    /// guarantees every non-final part is exactly `part_size` — never under
    /// S3's 5 MiB minimum — regardless of how the writer machinery chunks.
    fn put(self: *MultipartWriter, bytes: []const u8) std.Io.Writer.Error!usize {
        var rest = bytes;
        while (rest.len > 0) {
            const n = @min(part_size - self.part_len, rest.len);
            @memcpy(self.part_buf[self.part_len..][0..n], rest[0..n]);
            self.part_len += n;
            rest = rest[n..];
            if (self.part_len == part_size) {
                self.uploadPart(self.part_buf) catch return error.WriteFailed;
                self.part_len = 0;
            }
        }
        return bytes.len;
    }

    /// CreateMultipartUpload, once. The URL says `?uploads` while the canonical
    /// query says `uploads=` — both sides canonicalize a valueless key that way.
    fn ensureUpload(self: *MultipartWriter) !void {
        if (self.upload_id != null) return;
        const url = try std.fmt.allocPrint(self.arena, "{s}?uploads", .{self.obj.url});
        const q = [_][]const u8{"uploads="};
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const hdrs = try signedWriteHeaders(self.arena, self.creds, "POST", self.obj, self.obj.uri_path, &q, empty_payload_hash, self.content_type);
            var aw = std.Io.Writer.Allocating.init(self.arena);
            const res = try self.client.fetch(.{
                .method = .POST,
                .location = .{ .url = url },
                .extra_headers = hdrs,
                .headers = .{ .content_type = .{ .override = self.content_type } },
                .payload = "",
                .decompress_buffer = httpx.decompress_direct,
                .response_writer = &aw.writer,
            });
            const code = @intFromEnum(res.status);
            const body = aw.writer.buffered();
            if (code == 200) {
                const id = extractTag(body, "UploadId") orelse return self.fail(code, body);
                self.upload_id = try self.arena.dupe(u8, id);
                self.upload_id_enc = try uriEncode(self.arena, self.upload_id.?, .encode_slash);
                return;
            }
            // Fresh destination: create the bucket, then retry the initiation.
            if (statusToError(code, body) == Error.S3BucketMissing and !self.created_bucket) {
                try self.createBucket();
                continue;
            }
            if (retriable(code) and attempt + 1 < max_attempts) {
                std.Thread.sleep(backoffMs(attempt, self.rand.random()) * std.time.ns_per_ms);
                continue;
            }
            return self.fail(code, body);
        }
    }

    /// UploadPart over the lower-level request API: the ETag needed by
    /// CompleteMultipartUpload only exists as a response header, which
    /// `Client.fetch` does not surface.
    fn uploadPart(self: *MultipartWriter, bytes: []const u8) !void {
        try self.ensureUpload();
        // Query pre-sorted: partNumber < uploadId.
        const q = [_][]const u8{
            try std.fmt.allocPrint(self.arena, "partNumber={d}", .{self.etags.items.len + 1}),
            try std.fmt.allocPrint(self.arena, "uploadId={s}", .{self.upload_id_enc}),
        };
        const url = try std.fmt.allocPrint(self.arena, "{s}?{s}&{s}", .{ self.obj.url, q[0], q[1] });
        const ph = try payloadHash(self.arena, bytes);
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const hdrs = try signedWriteHeaders(self.arena, self.creds, "PUT", self.obj, self.obj.uri_path, &q, ph, null);
            const r = try self.putWithEtag(url, hdrs, bytes);
            if (r.code == 200) {
                if (r.etag.len == 0) {
                    self.last_error = "UploadPart response carried no ETag";
                    self.last_status = Error.S3RequestFailed;
                    return self.last_status.?;
                }
                try self.etags.append(r.etag);
                return;
            }
            if (retriable(r.code) and attempt + 1 < max_attempts) {
                std.Thread.sleep(backoffMs(attempt, self.rand.random()) * std.time.ns_per_ms);
                continue;
            }
            return self.fail(r.code, r.body);
        }
    }

    const PutResult = struct { code: u16, etag: []const u8, body: []const u8 };

    fn putWithEtag(self: *MultipartWriter, url: []const u8, hdrs: []const std.http.Header, bytes: []const u8) !PutResult {
        const uri = std.Uri.parse(url) catch return Error.S3BadUrl;
        var req = try self.client.request(.PUT, uri, .{ .extra_headers = hdrs });
        defer req.deinit();
        req.transfer_encoding = .{ .content_length = bytes.len };
        var body = try req.sendBodyUnflushed(&.{});
        try body.writer.writeAll(bytes);
        try body.end();
        try req.connection.?.flush();

        var redirect_buf: [1024]u8 = undefined;
        var resp = try req.receiveHead(&redirect_buf);
        const code = @intFromEnum(resp.head.status);
        // The ETag must come out before the body: reading the body invalidates
        // the head's strings.
        var etag: []const u8 = "";
        var it = resp.head.iterateHeaders();
        while (it.next()) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, "etag")) etag = try self.arena.dupe(u8, h.value);
        }
        var aw = std.Io.Writer.Allocating.init(self.arena);
        var tbuf: [4096]u8 = undefined;
        _ = resp.reader(&tbuf).streamRemaining(&aw.writer) catch {};
        return .{ .code = code, .etag = etag, .body = aw.writer.buffered() };
    }

    /// Whole object in one PUT — the path taken when everything fit in the part
    /// buffer, so no multipart upload was ever started.
    fn singlePut(self: *MultipartWriter, bytes: []const u8) !void {
        const ph = try payloadHash(self.arena, bytes);
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const hdrs = try signedWriteHeaders(self.arena, self.creds, "PUT", self.obj, self.obj.uri_path, &.{}, ph, self.content_type);
            var aw = std.Io.Writer.Allocating.init(self.arena);
            const res = try self.client.fetch(.{
                .method = .PUT,
                .location = .{ .url = self.obj.url },
                .extra_headers = hdrs,
                .headers = .{ .content_type = .{ .override = self.content_type } },
                .payload = bytes,
                .decompress_buffer = httpx.decompress_direct,
                .response_writer = &aw.writer,
            });
            const code = @intFromEnum(res.status);
            const body = aw.writer.buffered();
            if (code == 200) {
                self.last_status = null;
                return;
            }
            if (statusToError(code, body) == Error.S3BucketMissing and !self.created_bucket) {
                try self.createBucket();
                continue;
            }
            if (retriable(code) and attempt + 1 < max_attempts) {
                std.Thread.sleep(backoffMs(attempt, self.rand.random()) * std.time.ns_per_ms);
                continue;
            }
            return self.fail(code, body);
        }
    }

    /// Commits the object. Single PUT when the part buffer never filled;
    /// otherwise the final (possibly short — allowed for the last) part goes
    /// out and the part list is completed.
    pub fn finish(self: *MultipartWriter) !void {
        try self.interface.flush();
        if (self.upload_id == null) return self.singlePut(self.part_buf[0..self.part_len]);
        if (self.part_len > 0) {
            try self.uploadPart(self.part_buf[0..self.part_len]);
            self.part_len = 0;
        }

        const body = try completeBody(self.arena, self.etags.items);
        const q = [_][]const u8{
            try std.fmt.allocPrint(self.arena, "uploadId={s}", .{self.upload_id_enc}),
        };
        const url = try std.fmt.allocPrint(self.arena, "{s}?{s}", .{ self.obj.url, q[0] });
        const ph = try payloadHash(self.arena, body);
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const hdrs = try signedWriteHeaders(self.arena, self.creds, "POST", self.obj, self.obj.uri_path, &q, ph, null);
            var aw = std.Io.Writer.Allocating.init(self.arena);
            const res = try self.client.fetch(.{
                .method = .POST,
                .location = .{ .url = url },
                .extra_headers = hdrs,
                .payload = body,
                .decompress_buffer = httpx.decompress_direct,
                .response_writer = &aw.writer,
            });
            var code = @intFromEnum(res.status);
            const resp = aw.writer.buffered();
            // CompleteMultipartUpload can answer 200 with an error body
            // (documented behavior under internal faults); that is a retriable
            // failure, not success.
            if (code == 200 and std.mem.indexOf(u8, resp, "<Error>") != null) code = 500;
            if (code == 200) {
                self.last_status = null;
                return;
            }
            if (retriable(code) and attempt + 1 < max_attempts) {
                std.Thread.sleep(backoffMs(attempt, self.rand.random()) * std.time.ns_per_ms);
                continue;
            }
            return self.fail(code, resp);
        }
    }

    /// Create the bucket, ignoring "already exists". Called once after a
    /// NoSuchBucket so writing to a fresh destination works without a separate
    /// setup step. Regions other than us-east-1 require the location in the body.
    fn createBucket(self: *MultipartWriter) !void {
        self.created_bucket = true;
        const body = if (std.mem.eql(u8, self.obj.region, default_region))
            ""
        else
            try std.fmt.allocPrint(
                self.arena,
                "<CreateBucketConfiguration><LocationConstraint>{s}</LocationConstraint></CreateBucketConfiguration>",
                .{self.obj.region},
            );
        const ph = try payloadHash(self.arena, body);
        const hdrs = try signedWriteHeaders(self.arena, self.creds, "PUT", self.obj, self.obj.bucket_uri_path, &.{}, ph, null);
        var aw = std.Io.Writer.Allocating.init(self.arena);
        const res = try self.client.fetch(.{
            .method = .PUT,
            .location = .{ .url = self.obj.bucket_url },
            .extra_headers = hdrs,
            .payload = body,
            .decompress_buffer = httpx.decompress_direct,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        // 409 = already there (owned or raced), which is success for our purposes.
        if (code != 200 and code != 409) return self.fail(code, aw.writer.buffered());
    }

    fn fail(self: *MultipartWriter, code: u16, body: []const u8) Error {
        self.last_error = describe(self.arena, code, body) catch "";
        self.last_status = statusToError(code, body);
        return self.last_status.?;
    }
};

test "amzDate matches the reference instant from the S3 signing docs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    // 2013-05-24T00:00:00Z, the timestamp of every example in the S3 SigV4 docs.
    try std.testing.expectEqualStrings("20130524T000000Z", try amzDate(a, 1369353600));
    try std.testing.expectEqualStrings("19700101T000000Z", try amzDate(a, 0));
    try std.testing.expectEqualStrings("20150830T123600Z", try amzDate(a, 1440938160));
}

test "signingKey matches the derivation example from the AWS SigV4 docs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    // "Deriving the signing key" example: secret/date/region/service → this key.
    const k = try signingKey(ar.allocator(), "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam");
    try std.testing.expectEqualStrings(
        "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9",
        &std.fmt.bytesToHex(k, .lower),
    );
}

// The next three tests are AWS's published S3 SigV4 examples ("Authenticating
// Requests: Using the Authorization Header"), signature values verbatim from
// the docs. They are the ground truth that the signing here is right without a
// server to test against.
const example_access = "AKIAIOSFODNN7EXAMPLE";
const example_secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

test "sigv4: AWS example 1 — GET object with a Range header" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const h = try authHeader(a, example_access, example_secret, .{
        .method = "GET",
        .uri_path = "/test.txt",
        .headers = &.{
            // deliberately unsorted: canonicalization must not depend on order
            .{ .name = "x-amz-date", .value = "20130524T000000Z" },
            .{ .name = "host", .value = "examplebucket.s3.amazonaws.com" },
            .{ .name = "range", .value = "bytes=0-9" },
            .{ .name = "x-amz-content-sha256", .value = empty_payload_hash },
        },
        .payload_hash = empty_payload_hash,
        .timestamp = "20130524T000000Z",
        .region = "us-east-1",
    });
    try std.testing.expectEqualStrings(
        "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request," ++
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date," ++
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
        h,
    );
}

test "sigv4: AWS example 2 — PUT object with payload hash and extra headers" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ph = try payloadHash(a, "Welcome to Amazon S3.");
    try std.testing.expectEqualStrings("44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072", ph);
    // the docs' key is `test$file.text`; the canonical URI single-encodes the $
    try std.testing.expectEqualStrings("test%24file.text", try uriEncode(a, "test$file.text", .keep_slash));
    const h = try authHeader(a, example_access, example_secret, .{
        .method = "PUT",
        .uri_path = "/test%24file.text",
        .headers = &.{
            .{ .name = "date", .value = "Fri, 24 May 2013 00:00:00 GMT" },
            .{ .name = "host", .value = "examplebucket.s3.amazonaws.com" },
            .{ .name = "x-amz-content-sha256", .value = ph },
            .{ .name = "x-amz-date", .value = "20130524T000000Z" },
            .{ .name = "x-amz-storage-class", .value = "REDUCED_REDUNDANCY" },
        },
        .payload_hash = ph,
        .timestamp = "20130524T000000Z",
        .region = "us-east-1",
    });
    try std.testing.expect(std.mem.endsWith(u8, h, "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"));
}

test "sigv4: AWS example 3 — GET bucket listing with query parameters" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const h = try authHeader(a, example_access, example_secret, .{
        .method = "GET",
        .uri_path = "/",
        // deliberately unsorted: the canonical query string must sort them
        .query = &.{ "prefix=J", "max-keys=2" },
        .headers = &.{
            .{ .name = "host", .value = "examplebucket.s3.amazonaws.com" },
            .{ .name = "x-amz-content-sha256", .value = empty_payload_hash },
            .{ .name = "x-amz-date", .value = "20130524T000000Z" },
        },
        .payload_hash = empty_payload_hash,
        .timestamp = "20130524T000000Z",
        .region = "us-east-1",
    });
    try std.testing.expect(std.mem.endsWith(u8, h, "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"));
}

test "parseUrl: path-style endpoint keeps the port in the signed host" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const o = try parseUrl(a, "s3://lake/dir/sub/file.csv", "http://127.0.0.1:9000");
    try std.testing.expectEqualStrings("lake", o.bucket);
    try std.testing.expectEqualStrings("dir/sub/file.csv", o.key);
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake/dir/sub/file.csv", o.url);
    try std.testing.expectEqualStrings("127.0.0.1:9000", o.host);
    try std.testing.expectEqualStrings("/lake/dir/sub/file.csv", o.uri_path);

    // a trailing slash on the endpoint must not double up
    const t = try parseUrl(a, "s3://lake/f.csv", "http://127.0.0.1:9000/");
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake/f.csv", t.url);

    // keys with characters needing encoding stay aligned between URL and canonical URI
    const e = try parseUrl(a, "s3://lake/a b$c.csv", "http://127.0.0.1:9000");
    try std.testing.expectEqualStrings("/lake/a%20b%24c.csv", e.uri_path);
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake/a%20b%24c.csv", e.url);

    try std.testing.expectError(Error.S3BadUrl, parseUrl(a, "s3://bucketonly", null));
    try std.testing.expectError(Error.S3BadUrl, parseUrl(a, "s3://b/", null));
    try std.testing.expectError(Error.S3BadUrl, parseUrl(a, "az://a/c/f.csv", null));
    try std.testing.expect(!isUrl("az://a/c/f"));
}

test "parseUrl: virtual-host style for the real service" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const o = try parseUrl(a, "s3://mybucket/dir/f.parquet", null);
    const host = try std.fmt.allocPrint(a, "mybucket.s3.{s}.amazonaws.com", .{regionFromEnv(a)});
    try std.testing.expectEqualStrings(host, o.host);
    try std.testing.expectEqualStrings("/dir/f.parquet", o.uri_path);
    const url = try std.fmt.allocPrint(a, "https://{s}/dir/f.parquet", .{host});
    try std.testing.expectEqualStrings(url, o.url);
}

test "prefix URLs are distinguished from object URLs and may have an empty prefix" {
    try std.testing.expect(isPrefix("s3://b/dir/"));
    try std.testing.expect(isPrefix("s3://b/"));
    try std.testing.expect(!isPrefix("s3://b/f.csv"));

    const p = try parsePrefix("s3://lake/year=2026/");
    try std.testing.expectEqualStrings("lake", p.bucket);
    try std.testing.expectEqualStrings("year=2026/", p.prefix);

    const bare = try parsePrefix("s3://lake/");
    try std.testing.expectEqualStrings("", bare.prefix);
    try std.testing.expectError(Error.S3BadUrl, parsePrefix("s3://noslash"));
}

test "parseError pulls the code and first message line out of an S3 fault" {
    const body =
        \\<?xml version="1.0" encoding="UTF-8"?>
        \\<Error>
        \\  <Code>SignatureDoesNotMatch</Code>
        \\  <Message>The request signature we calculated does not match the signature you provided.</Message>
        \\  <RequestId>4442587FB7D0A2F9</RequestId>
        \\</Error>
    ;
    const e = parseError(body).?;
    try std.testing.expectEqualStrings("SignatureDoesNotMatch", e.code);
    try std.testing.expectEqualStrings("The request signature we calculated does not match the signature you provided.", e.message);
    try std.testing.expect(parseError("not xml") == null);
}

test "statusToError distinguishes causes instead of one catch-all" {
    try std.testing.expectEqual(Error.S3BucketMissing, statusToError(404, "<Error><Code>NoSuchBucket</Code></Error>"));
    try std.testing.expectEqual(Error.S3KeyNotFound, statusToError(404, "<Error><Code>NoSuchKey</Code></Error>"));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(403, "<Error><Code>SignatureDoesNotMatch</Code></Error>"));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(403, "<Error><Code>AccessDenied</Code></Error>"));
    try std.testing.expectEqual(Error.S3Throttled, statusToError(503, "<Error><Code>SlowDown</Code></Error>"));
    try std.testing.expectEqual(Error.S3Throttled, statusToError(503, ""));
    try std.testing.expectEqual(Error.S3RequestFailed, statusToError(418, ""));
}

test "retry policy: only transient statuses, and jitter stays within its band" {
    try std.testing.expect(retriable(429) and retriable(500) and retriable(503));
    try std.testing.expect(!retriable(403) and !retriable(404) and !retriable(200));

    var prng = std.Random.DefaultPrng.init(1);
    const r = prng.random();
    for (0..5) |i| {
        const base = @as(u64, 200) << @intCast(i);
        const ms = backoffMs(i, r);
        try std.testing.expect(ms >= base and ms <= base + base / 2 + 1);
    }
}

test "collectKeys reads every object key from a ListObjectsV2 page" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var out = std.array_list.Managed([]const u8).init(a);
    try collectKeys(a,
        \\<ListBucketResult><Name>lake</Name><Prefix>p/</Prefix><KeyCount>2</KeyCount>
        \\<Contents><Key>p/a.csv</Key><Size>10</Size></Contents>
        \\<Contents><Key>p/b.csv</Key><Size>20</Size></Contents>
        \\</ListBucketResult>
    , &out);
    try std.testing.expectEqual(@as(usize, 2), out.items.len);
    try std.testing.expectEqualStrings("p/a.csv", out.items[0]);
    try std.testing.expectEqualStrings("p/b.csv", out.items[1]);
}

test "bucketBase covers both endpoint styles" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const p = try bucketBase(a, "lake", "us-east-1", "http://127.0.0.1:9000");
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake", p.url);
    try std.testing.expectEqualStrings("127.0.0.1:9000", p.host);
    try std.testing.expectEqualStrings("/lake", p.uri_path);

    const v = try bucketBase(a, "lake", "eu-west-1", null);
    try std.testing.expectEqualStrings("https://lake.s3.eu-west-1.amazonaws.com/", v.url);
    try std.testing.expectEqualStrings("lake.s3.eu-west-1.amazonaws.com", v.host);
    try std.testing.expectEqualStrings("/", v.uri_path);
}

test "completeBody lists parts in staging order, 1-based" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const body = try completeBody(ar.allocator(), &.{ "\"etag-a\"", "\"etag-b\"" });
    try std.testing.expectEqualStrings(
        "<CompleteMultipartUpload>" ++
            "<Part><PartNumber>1</PartNumber><ETag>\"etag-a\"</ETag></Part>" ++
            "<Part><PartNumber>2</PartNumber><ETag>\"etag-b\"</ETag></Part>" ++
            "</CompleteMultipartUpload>",
        body,
    );
}

test "parseUrl carries the bucket root for CreateBucket in both styles" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const p = try parseUrl(a, "s3://lake/dir/f.csv", "http://127.0.0.1:9000");
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake", p.bucket_url);
    try std.testing.expectEqualStrings("/lake", p.bucket_uri_path);

    const v = try parseUrl(a, "s3://mybucket/f.csv", null);
    try std.testing.expectEqualStrings("/", v.bucket_uri_path);
    try std.testing.expect(std.mem.startsWith(u8, v.bucket_url, "https://mybucket.s3."));
}

test "describe renders the code and message a user needs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings(
        "NoSuchKey: The specified key does not exist. (HTTP 404)",
        try describe(a, 404, "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>"),
    );
    try std.testing.expectEqualStrings("HTTP 500", try describe(a, 500, ""));
}
