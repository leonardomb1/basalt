//! Amazon S3 (and S3-compatible stores: MinIO, localstack) over the REST API,
//! authenticated with AWS Signature Version 4.
//!
//! URLs are `s3://<bucket>/<key>`; a trailing slash addresses every object under a
//! prefix. With no endpoint override the real service is addressed virtual-host
//! style (`https://<bucket>.s3.<region>.amazonaws.com/<key>`, region from AWS_REGION,
//! default us-east-1). AWS_ENDPOINT_URL selects path-style (`<endpoint>/<bucket>/<key>`),
//! which MinIO and the other emulators require; the style changes both the signed
//! Host and the canonical URI. Credentials come from AWS_ACCESS_KEY_ID /
//! AWS_SECRET_ACCESS_KEY (+ optional AWS_SESSION_TOKEN, signed as
//! x-amz-security-token), environment only.
//!
//! Signing invariants: the request URL's path is percent-encoded exactly as the
//! canonical URI (single-encoded, '/' kept), and query strings are built pre-sorted
//! so the URL and the canonical query are the same text; any byte of drift is a
//! SignatureDoesNotMatch with no hint. Host is signed but not sent by us:
//! std.http.Client derives it from the URL as the authority including any port,
//! and `Obj.host` is that same string by construction. Every request signs the real
//! payload hash (the empty-body SHA-256 for bodyless verbs), never UNSIGNED-PAYLOAD,
//! so requests are integrity-protected even over the plain-HTTP emulator endpoints.
//! The tests pin signing to AWS's published SigV4 examples.
//!
//! Writes go through `MultipartWriter`: content accumulates into one `part_size`
//! buffer, a multipart upload starts the first time it fills, and `finish`
//! completes the ordered part list; an object that never fills it is one plain PUT.
//! Parts are gathered whole (`objstore.Upload`) rather than staged from drain
//! chunks, as S3 rejects a non-final part under 5 MiB, and each request's memory
//! is freed before the next part. 8 MiB parts under the 10,000-part limit cap an
//! object at ~78 GiB. Until `finish` returns the object does not exist; `abort`,
//! or a `finish` that fails, sends AbortMultipartUpload once an upload was started
//! so its parts stop being billed, best effort: the abort's own failure is dropped
//! and never replaces the error that caused it, leaving at worst an invisible
//! upload for a bucket lifecycle rule. Staging runs under `std.Io.Writer`, whose
//! only error is `WriteFailed`, so the writer keeps the typed error (`last_status`) and S3's message for the
//! sink to re-raise.
//!
//! `S3EmptyPrefix` is kept distinct from an empty object because its usual cause is
//! a mistyped prefix in an otherwise full bucket.
//!
//! Everything not specific to SigV4 (error bodies, retry policy, the listing loop,
//! the consumer-facing interface) lives in `objstore.zig`.

const std = @import("std");
const msgfmt = @import("../format/msgfmt.zig");
const objstore = @import("objstore.zig");
const scheme = @import("scheme.zig");
const http_fake = @import("../testing/http_fake.zig");
const xml = @import("../format/xml/token.zig");
const civil = @import("../exec/civil.zig");

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
    S3EmptyPrefix,
    S3UploadMissing,
    S3ConditionalWriteUnsupported,
};

pub const Obj = struct {
    bucket: []const u8,
    key: []const u8,
    url: []const u8,
    host: []const u8,
    uri_path: []const u8,
    region: []const u8,
    bucket_url: []const u8,
    bucket_uri_path: []const u8,
};

pub fn isUrl(s: []const u8) bool {
    return scheme.of(s) == .s3;
}

/// Why the bucket in this `s3://` URL cannot name a bucket, or null. Checked at plan
/// time because the service answers a bare 400 (`s3://bt/x.csv` is too short); only
/// the DNS rules AWS and MinIO share, the rest left to the service's own message.
pub fn bucketNameError(url: []const u8) ?[]const u8 {
    if (!isUrl(url)) return null;
    const rest = scheme.Scheme.rest(url);
    const bucket = std.mem.sliceTo(rest, '/');
    if (bucket.len < 3 or bucket.len > 63)
        return "a bucket name is 3 to 63 characters";
    for (bucket) |c| {
        if (std.ascii.isUpper(c)) return "a bucket name cannot contain uppercase letters";
        const ok = std.ascii.isLower(c) or std.ascii.isDigit(c) or c == '-' or c == '.';
        if (!ok) return "a bucket name may hold only lowercase letters, digits, `-` and `.`";
    }
    const first = bucket[0];
    const last = bucket[bucket.len - 1];
    if (!(std.ascii.isLower(first) or std.ascii.isDigit(first)) or
        !(std.ascii.isLower(last) or std.ascii.isDigit(last)))
        return "a bucket name must start and end with a letter or digit";
    if (std.mem.indexOf(u8, bucket, "..") != null)
        return "a bucket name cannot contain two adjacent dots";
    return null;
}

test "bucketNameError accepts real names and explains the rejects" {
    try std.testing.expect(bucketNameError("s3://basalt-it/seed.csv") == null);
    try std.testing.expect(bucketNameError("s3://a.b.c/k") == null);
    try std.testing.expect(bucketNameError("s3://bucket/") == null);
    try std.testing.expect(bucketNameError("/tmp/x.csv") == null);
    try std.testing.expect(bucketNameError("az://acct/c/x.csv") == null);

    try std.testing.expect(bucketNameError("s3://bt/x.csv") != null);
    try std.testing.expect(bucketNameError("s3://Bucket/x.csv") != null);
    try std.testing.expect(bucketNameError("s3://my_bucket/x.csv") != null);
    try std.testing.expect(bucketNameError("s3://-lead/x.csv") != null);
    try std.testing.expect(bucketNameError("s3://trail-/x.csv") != null);
    try std.testing.expect(bucketNameError("s3://a..b/x.csv") != null);
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
    token: ?[]const u8 = null,
};

pub fn credsFromEnv(arena: std.mem.Allocator) !Creds {
    const access = std.process.getEnvVarOwned(arena, env_key_id) catch return Error.S3MissingKey;
    const secret = std.process.getEnvVarOwned(arena, env_secret) catch return Error.S3MissingKey;
    const token = std.process.getEnvVarOwned(arena, env_token) catch null;
    return .{ .access = access, .secret = secret, .token = token };
}

/// The authority (host[:port]) of an endpoint URL, which is the Host that
/// std.http.Client sends and so the one that must be signed.
fn authorityOf(endpoint: []const u8) ![]const u8 {
    if (!scheme.of(endpoint).has(.http)) return Error.S3BadUrl;
    const host = std.mem.sliceTo(scheme.Scheme.rest(endpoint), '/');
    if (host.len == 0) return Error.S3BadUrl;
    return host;
}

/// Splits `s3://bucket/key...`; the bucket is the first segment. A null `endpoint`
/// selects virtual-host style, non-null path-style against that endpoint.
pub fn parseUrl(arena: std.mem.Allocator, url: []const u8, endpoint: ?[]const u8) !Obj {
    if (!isUrl(url)) return Error.S3BadUrl;
    const rest = scheme.Scheme.rest(url);
    const b_end = std.mem.indexOfScalar(u8, rest, '/') orelse return Error.S3BadUrl;
    const bucket = rest[0..b_end];
    const key = rest[b_end + 1 ..];
    if (bucket.len == 0 or key.len == 0) return Error.S3BadUrl;

    const region = regionFromEnv(arena);
    const enc_key = try objstore.uriEncode(arena, key, .keep_slash);
    const base = try bucketBase(arena, bucket, region, endpoint);
    const sep: []const u8 = if (std.mem.endsWith(u8, base.uri_path, "/")) "" else "/";
    return .{
        .bucket = bucket,
        .key = key,
        .url = try msgfmt.allocPrint(arena, "{s}{s}{s}", .{ base.url, sep, enc_key }),
        .host = base.host,
        .uri_path = try msgfmt.allocPrint(arena, "{s}{s}{s}", .{ base.uri_path, sep, enc_key }),
        .region = region,
        .bucket_url = base.url,
        .bucket_uri_path = base.uri_path,
    };
}

/// Splits a prefix URL for listing. Unlike `parseUrl`, the prefix may be empty.
pub fn parsePrefix(url: []const u8) !struct { bucket: []const u8, prefix: []const u8 } {
    if (!isUrl(url)) return Error.S3BadUrl;
    const rest = scheme.Scheme.rest(url);
    const b_end = std.mem.indexOfScalar(u8, rest, '/') orelse return Error.S3BadUrl;
    const bucket = rest[0..b_end];
    if (bucket.len == 0) return Error.S3BadUrl;
    return .{ .bucket = bucket, .prefix = rest[b_end + 1 ..] };
}

/// `20130524T000000Z`, the x-amz-date instant; its first 8 characters are the
/// credential-scope date.
pub fn amzDate(arena: std.mem.Allocator, epoch_secs: i64) ![]const u8 {
    var aw: std.Io.Writer.Allocating = .init(arena);
    civil.writeLayout(&aw.writer, epoch_secs * civil.us_per_sec, .amz) catch return error.OutOfMemory;
    return aw.written();
}

pub fn payloadHash(arena: std.mem.Allocator, payload: []const u8) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    return arena.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
}

pub const empty_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

pub const SignHeader = struct { name: []const u8, value: []const u8 };

pub const SignParams = struct {
    method: []const u8,
    uri_path: []const u8,
    query: []const []const u8 = &.{},
    headers: []const SignHeader,
    payload_hash: []const u8,
    timestamp: []const u8,
    region: []const u8,
    service: []const u8 = "s3",
};

/// Canonical request text plus the `;`-joined signed-headers list. Header values
/// are signed verbatim (all machine-built); a valueless query param signs as `name=`.
fn canonicalRequest(arena: std.mem.Allocator, p: SignParams) !struct { text: []const u8, signed: []const u8 } {
    const hdrs = try arena.dupe(SignHeader, p.headers);
    std.sort.insertion(SignHeader, hdrs, {}, struct {
        fn lt(_: void, x: SignHeader, y: SignHeader) bool {
            return std.mem.lessThan(u8, x.name, y.name);
        }
    }.lt);
    const q = try arena.dupe([]const u8, p.query);
    std.sort.insertion([]const u8, q, {}, struct {
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

/// The derived SigV4 signing key: an HMAC chain over date, region, service and
/// `aws4_request`, rooted at `AWS4` + secret.
pub fn signingKey(arena: std.mem.Allocator, secret: []const u8, date: []const u8, region: []const u8, service: []const u8) ![32]u8 {
    const seed = try msgfmt.allocPrint(arena, "AWS4{s}", .{secret});
    var k: [32]u8 = undefined;
    HmacSha256.create(&k, date, seed);
    HmacSha256.create(&k, region, &k);
    HmacSha256.create(&k, service, &k);
    HmacSha256.create(&k, "aws4_request", &k);
    return k;
}

/// Builds the `Authorization: AWS4-HMAC-SHA256 ...` value: canonical request,
/// string to sign, derived key, signature.
pub fn authHeader(arena: std.mem.Allocator, access: []const u8, secret: []const u8, p: SignParams) ![]const u8 {
    const cr = try canonicalRequest(arena, p);
    var cr_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(cr.text, &cr_hash, .{});

    const date = p.timestamp[0..8];
    const sts = try msgfmt.allocPrint(arena, "AWS4-HMAC-SHA256\n{s}\n{s}/{s}/{s}/aws4_request\n{s}", .{
        p.timestamp, date, p.region, p.service, &std.fmt.bytesToHex(cr_hash, .lower),
    });

    const key = try signingKey(arena, secret, date, p.region, p.service);
    var mac: [32]u8 = undefined;
    HmacSha256.create(&mac, sts, &key);

    return msgfmt.allocPrint(
        arena,
        "AWS4-HMAC-SHA256 Credential={s}/{s}/{s}/{s}/aws4_request,SignedHeaders={s},Signature={s}",
        .{ access, date, p.region, p.service, cr.signed, &std.fmt.bytesToHex(mac, .lower) },
    );
}

/// Signed headers for any verb; `range` is `bytes=a-b` or empty for the whole
/// object.
pub fn requestHeaders(
    arena: std.mem.Allocator,
    o: Obj,
    method: []const u8,
    range: []const u8,
) ![]const std.http.Header {
    return signedHeaders(arena, try credsFromEnv(arena), .{
        .method = method,
        .host = o.host,
        .region = o.region,
        .uri_path = o.uri_path,
        .range = range,
    });
}

const Request = struct {
    method: []const u8,
    host: []const u8,
    region: []const u8,
    uri_path: []const u8,
    query: []const []const u8 = &.{},
    payload_hash: []const u8 = empty_payload_hash,
    content_type: ?[]const u8 = null,
    range: []const u8 = "",
};

/// Signs host, payload hash, date, session token and any range or content-type.
/// Host and content-type are signed but not returned: the client sends both itself
/// (see the module header).
fn signedHeaders(arena: std.mem.Allocator, creds: Creds, r: Request) ![]const std.http.Header {
    const ts = try amzDate(arena, std.time.timestamp());
    var sh = std.array_list.Managed(SignHeader).init(arena);
    if (r.content_type) |ct| try sh.append(.{ .name = "content-type", .value = ct });
    try sh.append(.{ .name = "host", .value = r.host });
    if (r.range.len > 0) try sh.append(.{ .name = "range", .value = r.range });
    try sh.append(.{ .name = "x-amz-content-sha256", .value = r.payload_hash });
    try sh.append(.{ .name = "x-amz-date", .value = ts });
    if (creds.token) |t| try sh.append(.{ .name = "x-amz-security-token", .value = t });
    const auth = try authHeader(arena, creds.access, creds.secret, .{
        .method = r.method,
        .uri_path = r.uri_path,
        .query = r.query,
        .headers = sh.items,
        .payload_hash = r.payload_hash,
        .timestamp = ts,
        .region = r.region,
    });
    var out = std.array_list.Managed(std.http.Header).init(arena);
    try out.append(.{ .name = "x-amz-date", .value = ts });
    try out.append(.{ .name = "x-amz-content-sha256", .value = r.payload_hash });
    if (creds.token) |t| try out.append(.{ .name = "x-amz-security-token", .value = t });
    try out.append(.{ .name = "Authorization", .value = auth });
    if (r.range.len > 0) try out.append(.{ .name = "Range", .value = r.range });
    return out.toOwnedSlice();
}

/// Maps a status plus S3's error code onto a distinct Zig error. Every status the
/// retry loop replays (`objstore.retriable`) is `S3Throttled`, so a run that
/// exhausts its retries on one exits as transient.
pub fn statusToError(code: u16, body: []const u8) Error {
    if (objstore.parseError(body)) |e| {
        if (std.mem.eql(u8, e.code, "NoSuchBucket")) return Error.S3BucketMissing;
        if (std.mem.eql(u8, e.code, "NoSuchKey")) return Error.S3KeyNotFound;
        if (std.mem.eql(u8, e.code, "AccessDenied")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "SignatureDoesNotMatch")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "InvalidAccessKeyId")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "ExpiredToken")) return Error.S3AuthFailed;
        if (std.mem.eql(u8, e.code, "SlowDown")) return Error.S3Throttled;
        if (std.mem.eql(u8, e.code, "NoSuchUpload")) return Error.S3UploadMissing;
    }
    if (objstore.retriable(code)) return Error.S3Throttled;
    return switch (code) {
        401, 403 => Error.S3AuthFailed,
        404 => Error.S3BucketMissing,
        else => Error.S3RequestFailed,
    };
}

/// The bucket-level URL, signed host and canonical URI, in either endpoint style,
/// for listing and bucket creation.
fn bucketBase(arena: std.mem.Allocator, bucket: []const u8, region: []const u8, endpoint: ?[]const u8) !struct {
    url: []const u8,
    host: []const u8,
    uri_path: []const u8,
} {
    if (endpoint) |ep| {
        const base = std.mem.trimRight(u8, ep, "/");
        const uri_path = try msgfmt.allocPrint(arena, "/{s}", .{bucket});
        return .{
            .url = try msgfmt.allocPrint(arena, "{s}{s}", .{ base, uri_path }),
            .host = try authorityOf(base),
            .uri_path = uri_path,
        };
    }
    const host = try msgfmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, region });
    return .{
        .url = try msgfmt.allocPrint(arena, "https://{s}/", .{host}),
        .host = host,
        .uri_path = "/",
    };
}

const ListPage = struct {
    arena: std.mem.Allocator,
    client: *std.http.Client,
    creds: Creds,
    region: []const u8,
    prefix: []const u8,
    base_url: []const u8,
    host: []const u8,
    uri_path: []const u8,
    sleep: objstore.Sleep = std.Thread.sleep,

    fn page(c: ListPage, token: []const u8) ![]const u8 {
        var q = std.array_list.Managed([]const u8).init(c.arena);
        if (token.len > 0)
            try q.append(try msgfmt.allocPrint(c.arena, "continuation-token={s}", .{try objstore.uriEncode(c.arena, token, .encode_slash)}));
        try q.append("list-type=2");
        try q.append(try msgfmt.allocPrint(c.arena, "prefix={s}", .{try objstore.uriEncode(c.arena, c.prefix, .encode_slash)}));

        var url = std.array_list.Managed(u8).init(c.arena);
        try url.appendSlice(c.base_url);
        for (q.items, 0..) |s, i| {
            try url.append(if (i == 0) '?' else '&');
            try url.appendSlice(s);
        }

        var body: []const u8 = "";
        var prng = objstore.jitterPrng();
        const Ctx = struct { c: ListPage, q: []const []const u8, url: []const u8, out: *[]const u8 };
        try objstore.retry(.{ .rand = prng.random(), .sleep = c.sleep }, Ctx{ .c = c, .q = q.items, .url = url.items, .out = &body }, struct {
            fn attempt(x: Ctx) !objstore.Verdict {
                const hdrs = try signedHeaders(x.c.arena, x.c.creds, .{
                    .method = "GET",
                    .host = x.c.host,
                    .region = x.c.region,
                    .uri_path = x.c.uri_path,
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

/// Lists object keys under `prefix` (ListObjectsV2), bucket-relative, following
/// continuation tokens, in S3's lexicographic order. Keys are read only from
/// `<Contents>`, and a flat listing has no CommonPrefixes.
pub fn listPrefix(
    arena: std.mem.Allocator,
    client: *std.http.Client,
    bucket: []const u8,
    prefix: []const u8,
    endpoint: ?[]const u8,
) ![][]const u8 {
    const region = regionFromEnv(arena);
    const base = try bucketBase(arena, bucket, region, endpoint);
    const ctx = ListPage{
        .arena = arena,
        .client = client,
        .creds = try credsFromEnv(arena),
        .region = region,
        .prefix = prefix,
        .base_url = base.url,
        .host = base.host,
        .uri_path = base.uri_path,
    };
    return objstore.listPages(arena, .{ .item_tag = "Key", .next_tag = "NextContinuationToken" }, ctx, ListPage.page);
}

pub const part_size = 8 * 1024 * 1024;

fn completeBody(arena: std.mem.Allocator, etags: []const []const u8) ![]const u8 {
    var body = std.array_list.Managed(u8).init(arena);
    try body.appendSlice("<CompleteMultipartUpload>");
    for (etags, 1..) |etag, n| {
        try body.writer().print("<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>", .{ n, etag });
    }
    try body.appendSlice("</CompleteMultipartUpload>");
    return body.toOwnedSlice();
}

pub const MultipartWriter = struct {
    interface: std.Io.Writer,
    arena: std.mem.Allocator,
    up: objstore.Upload,
    obj: Obj,
    creds: Creds,
    content_type: []const u8,
    upload_id: ?[]const u8 = null,
    upload_id_enc: []const u8 = "",
    etags: std.array_list.Managed([]const u8),
    created_bucket: bool = false,
    last_error: []const u8 = "",
    last_status: ?Error = null,
    rand: std.Random.DefaultPrng,
    sleep: objstore.Sleep = std.Thread.sleep,
    total: u64 = 0,
    completing: bool = false,

    const vtable = std.Io.Writer.VTable{ .drain = objstore.drain(MultipartWriter) };

    pub fn init(
        arena: std.mem.Allocator,
        client: ?*std.http.Client,
        obj: Obj,
        content_type: []const u8,
    ) !*MultipartWriter {
        const self = try arena.create(MultipartWriter);
        self.* = .{
            .interface = .{ .vtable = &vtable, .buffer = try arena.alloc(u8, 64 * 1024) },
            .arena = arena,
            .up = try objstore.Upload.init(arena, client, part_size),
            .obj = obj,
            .creds = try credsFromEnv(arena),
            .content_type = content_type,
            .etags = std.array_list.Managed([]const u8).init(arena),
            .rand = objstore.jitterPrng(),
        };
        return self;
    }

    pub fn put(self: *MultipartWriter, bytes: []const u8) std.Io.Writer.Error!usize {
        self.up.stage(bytes, self, uploadPart) catch return error.WriteFailed;
        return bytes.len;
    }

    /// Aborts the multipart upload, if one was started, then frees the client and
    /// request memory.
    pub fn abort(self: *MultipartWriter) void {
        self.abortUpload();
        self.up.release();
    }

    /// AbortMultipartUpload, best effort and before `release`: its failure is
    /// dropped without touching `last_error` or `last_status`, which hold the cause
    /// of the abort. NoSuchUpload counts as aborted.
    fn abortUpload(self: *MultipartWriter) void {
        if (self.upload_id == null or self.up.released) return;
        self.sendAbort() catch {};
    }

    fn sendAbort(self: *MultipartWriter) !void {
        const a = self.up.fresh();
        const q = [_][]const u8{
            try msgfmt.allocPrint(a, "uploadId={s}", .{self.upload_id_enc}),
        };
        const Ctx = struct { w: *MultipartWriter, url: []const u8, q: []const []const u8 };
        const ctx = Ctx{
            .w = self,
            .url = try msgfmt.allocPrint(a, "{s}?{s}", .{ self.obj.url, q[0] }),
            .q = try a.dupe([]const u8, &q),
        };
        try objstore.retry(self.policy(), ctx, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const w = c.w;
                const hdrs = try w.signed("DELETE", w.obj.uri_path, c.q, empty_payload_hash, null);
                const r = try objstore.exchange(w.up.alloc(), w.up.client, .{ .method = .DELETE, .url = c.url, .headers = hdrs });
                const e = statusToError(r.code, r.body);
                if (r.code == 204 or r.code == 200 or e == Error.S3UploadMissing) return .done;
                return r.failed(e);
            }
        }.attempt);
    }

    /// CreateMultipartUpload, once. The URL says `?uploads` and the canonical query
    /// `uploads=`; both sides canonicalize a valueless key that way.
    fn ensureUpload(self: *MultipartWriter) !void {
        if (self.upload_id != null) return;
        const url = try msgfmt.allocPrint(self.up.fresh(), "{s}?uploads", .{self.obj.url});
        const Ctx = struct { w: *MultipartWriter, url: []const u8 };
        try objstore.retry(self.policy(), Ctx{ .w = self, .url = url }, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const w = c.w;
                const q = [_][]const u8{"uploads="};
                const hdrs = try w.signed("POST", w.obj.uri_path, &q, empty_payload_hash, w.content_type);
                const r = try objstore.exchange(w.up.alloc(), w.up.client, .{
                    .method = .POST,
                    .url = c.url,
                    .headers = hdrs,
                    .payload = "",
                    .content_type = w.content_type,
                });
                if (r.code == 200) {
                    const ids = try xml.elementTexts(w.up.alloc(), r.body, "UploadId");
                    if (ids.len == 0) return w.failed(r);
                    w.upload_id = try w.arena.dupe(u8, ids[0]);
                    w.upload_id_enc = try objstore.uriEncode(w.arena, w.upload_id.?, .encode_slash);
                    return .done;
                }
                if (try w.createBucketIfMissing(r.code, r.body)) return .again;
                return w.failed(r);
            }
        }.attempt);
    }

    /// UploadPart; its response's ETag is what CompleteMultipartUpload lists.
    fn uploadPart(self: *MultipartWriter, bytes: []const u8) !void {
        try self.ensureUpload();
        const a = self.up.fresh();
        const q = [_][]const u8{
            try msgfmt.allocPrint(a, "partNumber={d}", .{self.etags.items.len + 1}),
            try msgfmt.allocPrint(a, "uploadId={s}", .{self.upload_id_enc}),
        };
        const Ctx = struct { w: *MultipartWriter, url: []const u8, q: []const []const u8, ph: []const u8, bytes: []const u8 };
        const ctx = Ctx{
            .w = self,
            .url = try msgfmt.allocPrint(a, "{s}?{s}&{s}", .{ self.obj.url, q[0], q[1] }),
            .q = try a.dupe([]const u8, &q),
            .ph = try payloadHash(a, bytes),
            .bytes = bytes,
        };
        try objstore.retry(self.policy(), ctx, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const w = c.w;
                const hdrs = try w.signed("PUT", w.obj.uri_path, c.q, c.ph, null);
                const r = try objstore.exchange(w.up.alloc(), w.up.client, .{ .method = .PUT, .url = c.url, .headers = hdrs, .payload = c.bytes });
                if (r.code == 200) {
                    if (r.etag.len == 0) {
                        w.last_error = "UploadPart response carried no ETag";
                        w.last_status = Error.S3RequestFailed;
                        return .{ .failed = .{ .code = r.code, .err = w.last_status.? } };
                    }
                    try w.etags.append(try w.arena.dupe(u8, r.etag));
                    w.total += c.bytes.len;
                    return .done;
                }
                return w.failed(r);
            }
        }.attempt);
    }

    fn singlePut(self: *MultipartWriter, bytes: []const u8) !void {
        const Ctx = struct { w: *MultipartWriter, ph: []const u8, bytes: []const u8 };
        const ctx = Ctx{ .w = self, .ph = try payloadHash(self.up.fresh(), bytes), .bytes = bytes };
        try objstore.retry(self.policy(), ctx, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const w = c.w;
                const hdrs = try w.signed("PUT", w.obj.uri_path, &.{}, c.ph, w.content_type);
                const r = try objstore.exchange(w.up.alloc(), w.up.client, .{
                    .method = .PUT,
                    .url = w.obj.url,
                    .headers = hdrs,
                    .payload = c.bytes,
                    .content_type = w.content_type,
                });
                if (r.code == 200) {
                    w.last_status = null;
                    return .done;
                }
                if (try w.createBucketIfMissing(r.code, r.body)) return .again;
                return w.failed(r);
            }
        }.attempt);
    }

    /// One PUT of `bytes` with `If-None-Match: *`: true when it made the object,
    /// false when one was there (412), or a concurrent conditional write held the
    /// key (409). A store answering 501 does not do conditional writes, and is
    /// refused rather than trusted to overwrite.
    pub fn putIfAbsent(self: *MultipartWriter, bytes: []const u8) !bool {
        var created = false;
        const Ctx = struct { w: *MultipartWriter, ph: []const u8, bytes: []const u8, created: *bool };
        const ctx = Ctx{ .w = self, .ph = try payloadHash(self.up.fresh(), bytes), .bytes = bytes, .created = &created };
        try objstore.retry(self.policy(), ctx, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const w = c.w;
                const signed_hdrs = try w.signed("PUT", w.obj.uri_path, &.{}, c.ph, w.content_type);
                const hdrs = try std.mem.concat(w.up.alloc(), std.http.Header, &.{ signed_hdrs, &.{.{ .name = "If-None-Match", .value = "*" }} });
                const r = try objstore.exchange(w.up.alloc(), w.up.client, .{
                    .method = .PUT,
                    .url = w.obj.url,
                    .headers = hdrs,
                    .payload = c.bytes,
                    .content_type = w.content_type,
                });
                switch (r.code) {
                    200 => c.created.* = true,
                    409, 412 => c.created.* = false,
                    501 => {
                        w.last_error = w.up.keepError(r.code, r.body);
                        w.last_status = Error.S3ConditionalWriteUnsupported;
                        return r.failed(Error.S3ConditionalWriteUnsupported);
                    },
                    else => {
                        if (try w.createBucketIfMissing(r.code, r.body)) return .again;
                        return w.failed(r);
                    },
                }
                w.last_status = null;
                return .done;
            }
        }.attempt);
        return created;
    }

    /// Commits the object: a single PUT when the buffer never filled, else the final
    /// part and CompleteMultipartUpload. A 200 carrying `<Error>` is documented S3
    /// behaviour under internal faults and is retried as a failure. A replay whose
    /// predecessor's answer was lost may meet NoSuchUpload because that one
    /// completed; it counts as done when the object is there (`landed`). A failure
    /// aborts the upload, as nothing can complete it after `release`.
    pub fn finish(self: *MultipartWriter) !void {
        defer self.up.release();
        errdefer self.abortUpload();
        try self.interface.flush();
        if (self.upload_id == null) return self.singlePut(self.up.pending());
        if (self.up.part_len > 0) {
            try self.uploadPart(self.up.pending());
            self.up.part_len = 0;
        }

        const a = self.up.fresh();
        const q = [_][]const u8{
            try msgfmt.allocPrint(a, "uploadId={s}", .{self.upload_id_enc}),
        };
        const Ctx = struct { w: *MultipartWriter, url: []const u8, q: []const []const u8, ph: []const u8, body: []const u8 };
        const body = try completeBody(a, self.etags.items);
        const ctx = Ctx{
            .w = self,
            .url = try msgfmt.allocPrint(a, "{s}?{s}", .{ self.obj.url, q[0] }),
            .q = try a.dupe([]const u8, &q),
            .ph = try payloadHash(a, body),
            .body = body,
        };
        try objstore.retry(self.policy(), ctx, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const w = c.w;
                const replay = w.completing;
                w.completing = true;
                const hdrs = try w.signed("POST", w.obj.uri_path, c.q, c.ph, null);
                var r = try objstore.exchange(w.up.alloc(), w.up.client, .{ .method = .POST, .url = c.url, .headers = hdrs, .payload = c.body });
                if (r.code == 200 and std.mem.indexOf(u8, r.body, "<Error>") != null) r.code = 500;
                const gone = statusToError(r.code, r.body) == Error.S3UploadMissing;
                if (r.code == 200 or (replay and gone and try w.landed())) {
                    w.last_status = null;
                    return .done;
                }
                return w.failed(r);
            }
        }.attempt);
    }

    /// Whether the object a lost CompleteMultipartUpload answer was about exists:
    /// HEAD finds the multipart ETag these parts make, or the uploaded size when a
    /// part ETag is not a plain MD5 (SSE-KMS, SSE-C).
    fn landed(self: *MultipartWriter) !bool {
        const hdrs = try self.signed("HEAD", self.obj.uri_path, &.{}, empty_payload_hash, null);
        const r = try objstore.exchange(self.up.alloc(), self.up.client, .{ .method = .HEAD, .url = self.obj.url, .headers = hdrs });
        if (r.code != 200) return false;
        if (try multipartEtag(self.up.alloc(), self.etags.items)) |want| return std.ascii.eqlIgnoreCase(r.etag, want);
        return r.content_length == self.total;
    }

    /// Creates the bucket on the first NoSuchBucket so the caller can retry. False
    /// for any other failure, or when the bucket was already created once.
    fn createBucketIfMissing(self: *MultipartWriter, code: u16, body: []const u8) !bool {
        if (statusToError(code, body) != Error.S3BucketMissing or self.created_bucket) return false;
        try self.createBucket();
        return true;
    }

    /// Create the bucket; 409 (already exists, owned or raced) counts as success.
    /// Regions other than us-east-1 need the location in the body.
    fn createBucket(self: *MultipartWriter) !void {
        self.created_bucket = true;
        const body = if (std.mem.eql(u8, self.obj.region, default_region))
            ""
        else
            try msgfmt.allocPrint(
                self.up.alloc(),
                "<CreateBucketConfiguration><LocationConstraint>{s}</LocationConstraint></CreateBucketConfiguration>",
                .{self.obj.region},
            );
        const ph = try payloadHash(self.up.alloc(), body);
        const hdrs = try self.signed("PUT", self.obj.bucket_uri_path, &.{}, ph, null);
        const r = try objstore.exchange(self.up.alloc(), self.up.client, .{ .method = .PUT, .url = self.obj.bucket_url, .headers = hdrs, .payload = body });
        if (r.code != 200 and r.code != 409) return self.fail(r.code, r.body);
    }

    fn signed(self: *MultipartWriter, method: []const u8, uri_path: []const u8, query: []const []const u8, payload_hash: []const u8, content_type: ?[]const u8) ![]const std.http.Header {
        return signedHeaders(self.up.alloc(), self.creds, .{
            .method = method,
            .host = self.obj.host,
            .region = self.obj.region,
            .uri_path = uri_path,
            .query = query,
            .payload_hash = payload_hash,
            .content_type = content_type,
        });
    }

    fn policy(self: *MultipartWriter) objstore.Policy {
        return .{ .rand = self.rand.random(), .sleep = self.sleep };
    }

    fn fail(self: *MultipartWriter, code: u16, body: []const u8) Error {
        self.last_error = self.up.keepError(code, body);
        self.last_status = statusToError(code, body);
        return self.last_status.?;
    }

    fn failed(self: *MultipartWriter, r: objstore.Response) objstore.Verdict {
        return r.failed(self.fail(r.code, r.body));
    }
};

/// S3's ETag for a completed multipart upload: the MD5 of the parts' binary MD5s,
/// then `-<part count>`, quoted. Null when a part ETag is not a plain MD5.
pub fn multipartEtag(arena: std.mem.Allocator, etags: []const []const u8) !?[]const u8 {
    var md5 = std.crypto.hash.Md5.init(.{});
    for (etags) |e| {
        const hex = std.mem.trim(u8, e, "\"");
        if (hex.len != 32) return null;
        var raw: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&raw, hex) catch return null;
        md5.update(&raw);
    }
    var d: [16]u8 = undefined;
    md5.final(&d);
    return try msgfmt.allocPrint(arena, "\"{s}-{d}\"", .{ &std.fmt.bytesToHex(d, .lower), etags.len });
}

test "amzDate matches the reference instant from the S3 signing docs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("20130524T000000Z", try amzDate(a, 1369353600));
    try std.testing.expectEqualStrings("19700101T000000Z", try amzDate(a, 0));
    try std.testing.expectEqualStrings("20150830T123600Z", try amzDate(a, 1440938160));
}

test "signingKey matches the derivation example from the AWS SigV4 docs" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const k = try signingKey(ar.allocator(), "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY", "20150830", "us-east-1", "iam");
    try std.testing.expectEqualStrings(
        "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9",
        &std.fmt.bytesToHex(k, .lower),
    );
}

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
    try std.testing.expectEqualStrings("test%24file.text", try objstore.uriEncode(a, "test$file.text", .keep_slash));
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
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake", o.bucket_url);
    try std.testing.expectEqualStrings("/lake", o.bucket_uri_path);

    const t = try parseUrl(a, "s3://lake/f.csv", "http://127.0.0.1:9000/");
    try std.testing.expectEqualStrings("http://127.0.0.1:9000/lake/f.csv", t.url);

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
    try std.testing.expectEqualStrings("/dir/f.parquet", o.uri_path);
    try std.testing.expectEqualStrings("/", o.bucket_uri_path);
    if (std.process.hasEnvVarConstant(env_region)) {
        const host = try msgfmt.allocPrint(a, "mybucket.s3.{s}.amazonaws.com", .{regionFromEnv(a)});
        try std.testing.expectEqualStrings(host, o.host);
    } else {
        try std.testing.expectEqualStrings("mybucket.s3.us-east-1.amazonaws.com", o.host);
        try std.testing.expectEqualStrings("https://mybucket.s3.us-east-1.amazonaws.com/dir/f.parquet", o.url);
        try std.testing.expectEqualStrings("https://mybucket.s3.us-east-1.amazonaws.com/", o.bucket_url);
    }
}

test "prefix URLs may have an empty prefix" {
    const p = try parsePrefix("s3://lake/year=2026/");
    try std.testing.expectEqualStrings("lake", p.bucket);
    try std.testing.expectEqualStrings("year=2026/", p.prefix);

    const bare = try parsePrefix("s3://lake/");
    try std.testing.expectEqualStrings("", bare.prefix);
    try std.testing.expectError(Error.S3BadUrl, parsePrefix("s3://noslash"));
}

test "statusToError distinguishes causes instead of one catch-all" {
    try std.testing.expectEqual(Error.S3BucketMissing, statusToError(404, "<Error><Code>NoSuchBucket</Code></Error>"));
    try std.testing.expectEqual(Error.S3KeyNotFound, statusToError(404, "<Error><Code>NoSuchKey</Code></Error>"));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(403, "<Error><Code>SignatureDoesNotMatch</Code></Error>"));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(403, "<Error><Code>AccessDenied</Code></Error>"));
    try std.testing.expectEqual(Error.S3Throttled, statusToError(503, "<Error><Code>SlowDown</Code></Error>"));
    try std.testing.expectEqual(Error.S3Throttled, statusToError(503, ""));
    try std.testing.expectEqual(Error.S3RequestFailed, statusToError(418, ""));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(400, "<Error><Code>ExpiredToken</Code></Error>"));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(400, "<Error><Code>InvalidAccessKeyId</Code></Error>"));
    try std.testing.expectEqual(Error.S3KeyNotFound, statusToError(403, "<Error><Code>NoSuchKey</Code></Error>"));
    try std.testing.expectEqual(Error.S3Throttled, statusToError(400, "<Error><Code>SlowDown</Code></Error>"));
    try std.testing.expectEqual(Error.S3RequestFailed, statusToError(400, "<Error><Code>InvalidArgument</Code></Error>"));
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

var test_slept_ms: u64 = 0;

fn testSleep(ns: u64) void {
    test_slept_ms += ns / std.time.ns_per_ms;
}

fn testWriter(a: std.mem.Allocator, client: *std.http.Client, srv: *http_fake.FakeServer) !MultipartWriter {
    return .{
        .interface = .{ .vtable = &MultipartWriter.vtable, .buffer = &.{} },
        .arena = a,
        .up = .{ .client = client },
        .obj = try parseUrl(a, "s3://lake/out.csv", try srv.url(a, "")),
        .creds = .{ .access = example_access, .secret = example_secret },
        .content_type = "text/csv",
        .upload_id = "up1",
        .upload_id_enc = "up1",
        .etags = std.array_list.Managed([]const u8).init(a),
        .rand = std.Random.DefaultPrng.init(0),
        .sleep = testSleep,
    };
}

const reply = http_fake.FakeServer.reply;
const reply_reset: ?[]const u8 = null;
const reply_no_upload = reply("404 Not Found", "", "<Error><Code>NoSuchUpload</Code><Message>gone</Message></Error>");

test "a listing page that meets SlowDown is fetched again" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var srv: http_fake.FakeServer = undefined;
    try srv.start(&.{
        reply("503 Slow Down", "", "<Error><Code>SlowDown</Code></Error>"),
        reply("200 OK", "", "<ListBucketResult><Contents><Key>p/a.csv</Key></Contents></ListBucketResult>"),
    });
    defer srv.stop();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();

    const base = try bucketBase(a, "lake", default_region, try srv.url(a, ""));
    const ctx = ListPage{
        .arena = a,
        .client = &client,
        .creds = .{ .access = example_access, .secret = example_secret },
        .region = default_region,
        .prefix = "p/",
        .base_url = base.url,
        .host = base.host,
        .uri_path = base.uri_path,
        .sleep = testSleep,
    };
    const keys = try objstore.listPages(a, .{ .item_tag = "Key", .next_tag = "NextContinuationToken" }, ctx, ListPage.page);
    try std.testing.expectEqual(@as(usize, 1), keys.len);
    try std.testing.expectEqualStrings("p/a.csv", keys[0]);
    try std.testing.expectEqual(@as(usize, 2), srv.requests());
}

test "an upload part is re-put under its part number after a reset and a 502; a 403 is not retried" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    {
        var srv: http_fake.FakeServer = undefined;
        try srv.start(&.{
            reply_reset,
            reply("502 Bad Gateway", "", ""),
            reply("200 OK", "ETag: \"e1\"\r\n", ""),
        });
        defer srv.stop();
        var w = try testWriter(a, &client, &srv);
        defer w.abort();
        try w.uploadPart("hello");
        try std.testing.expectEqual(@as(usize, 1), w.etags.items.len);
        try std.testing.expectEqualStrings("\"e1\"", w.etags.items[0]);
        try std.testing.expectEqual(@as(u64, 5), w.total);
        try std.testing.expectEqual(@as(usize, 3), srv.requests());
        try std.testing.expect(std.mem.indexOf(u8, srv.line(0), "partNumber=1&uploadId=up1") != null);
        try std.testing.expectEqualStrings(srv.line(0), srv.line(2));
    }
    {
        var srv: http_fake.FakeServer = undefined;
        try srv.start(&.{
            reply("403 Forbidden", "", "<Error><Code>AccessDenied</Code></Error>"),
        });
        defer srv.stop();
        var w = try testWriter(a, &client, &srv);
        defer w.abort();
        try std.testing.expectError(Error.S3AuthFailed, w.uploadPart("hello"));
        try std.testing.expectEqual(@as(usize, 1), srv.requests());
    }
}

test "a complete whose answer was lost counts as done only when the object it made is there" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    const part = "\"9dd4e461268c8034f5c8564e155c67a6\"";
    const etag = (try multipartEtag(a, &.{part})).?;
    const head_ok = try msgfmt.allocPrint(a, "HTTP/1.1 200 OK\r\nETag: {s}\r\ncontent-length: 1\r\nconnection: close\r\n\r\n", .{etag});
    const head_other = "HTTP/1.1 200 OK\r\nETag: \"0123456789abcdef0123456789abcdef-1\"\r\ncontent-length: 1\r\nconnection: close\r\n\r\n";
    const head_missing = reply("404 Not Found", "", "");

    const Case = struct { replies: []const ?[]const u8, ok: bool, requests: usize };
    const cases = [_]Case{
        .{ .replies = &.{ reply_reset, reply_no_upload, head_ok }, .ok = true, .requests = 3 },
        .{ .replies = &.{ reply_reset, reply_no_upload, head_missing, reply_no_upload }, .ok = false, .requests = 4 },
        .{ .replies = &.{ reply_reset, reply_no_upload, head_other, reply_no_upload }, .ok = false, .requests = 4 },
        .{ .replies = &.{ reply_no_upload, reply_no_upload }, .ok = false, .requests = 2 },
    };
    for (cases) |c| {
        var srv: http_fake.FakeServer = undefined;
        try srv.start(c.replies);
        defer srv.stop();
        var w = try testWriter(a, &client, &srv);
        try w.etags.append(part);
        if (c.ok) try w.finish() else try std.testing.expectError(Error.S3UploadMissing, w.finish());
        try std.testing.expectEqual(c.requests, srv.requests());
        try std.testing.expect(std.mem.startsWith(u8, srv.line(0), "POST /lake/out.csv?uploadId=up1 "));
        if (c.requests >= 3) try std.testing.expect(std.mem.startsWith(u8, srv.line(2), "HEAD /lake/out.csv "));
        if (!c.ok) try std.testing.expect(std.mem.startsWith(u8, srv.line(c.requests - 1), "DELETE /lake/out.csv?uploadId=up1 "));
    }
}

test "abort sends a signed AbortMultipartUpload for a started upload and keeps the error that caused it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    {
        var srv: http_fake.FakeServer = undefined;
        try srv.start(&.{
            reply("403 Forbidden", "", "<Error><Code>AccessDenied</Code><Message>denied</Message></Error>"),
            reply("500 Internal Server Error", "", "<Error><Code>InternalError</Code></Error>"),
            reply("204 No Content", "", ""),
        });
        defer srv.stop();
        var w = try testWriter(a, &client, &srv);
        try std.testing.expectError(Error.S3AuthFailed, w.uploadPart("hello"));
        w.abort();
        w.abort();
        try std.testing.expectEqual(@as(usize, 3), srv.requests());
        try std.testing.expectEqualStrings("DELETE /lake/out.csv?uploadId=up1 HTTP/1.1", srv.line(1));
        try std.testing.expectEqualStrings(srv.line(1), srv.line(2));
        try std.testing.expect(std.mem.indexOf(u8, srv.headOf(1), "Authorization: AWS4-HMAC-SHA256 ") != null);
        try std.testing.expectEqual(Error.S3AuthFailed, w.last_status.?);
        try std.testing.expect(std.mem.indexOf(u8, w.last_error, "AccessDenied") != null);
    }
    {
        var srv: http_fake.FakeServer = undefined;
        try srv.start(&.{
            reply("403 Forbidden", "", "<Error><Code>AccessDenied</Code></Error>"),
        });
        defer srv.stop();
        var w = try testWriter(a, &client, &srv);
        try std.testing.expectError(Error.S3AuthFailed, w.uploadPart("hello"));
        w.abort();
        try std.testing.expectEqual(@as(usize, 2), srv.requests());
        try std.testing.expectEqual(Error.S3AuthFailed, w.last_status.?);
    }
    {
        var srv: http_fake.FakeServer = undefined;
        try srv.start(&.{});
        defer srv.stop();
        var w = try testWriter(a, &client, &srv);
        w.upload_id = null;
        w.abort();
        try std.testing.expectEqual(@as(usize, 0), srv.requests());
    }
}

test "multipartEtag is the MD5 of the binary part MD5s plus the part count" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var raw: [32]u8 = undefined;
    _ = try std.fmt.hexToBytes(raw[0..16], "9dd4e461268c8034f5c8564e155c67a6");
    _ = try std.fmt.hexToBytes(raw[16..], "d41d8cd98f00b204e9800998ecf8427e");
    var d: [16]u8 = undefined;
    std.crypto.hash.Md5.hash(&raw, &d, .{});
    const want = try msgfmt.allocPrint(a, "\"{s}-2\"", .{&std.fmt.bytesToHex(d, .lower)});
    const got = (try multipartEtag(a, &.{ "\"9dd4e461268c8034f5c8564e155c67a6\"", "\"D41D8CD98F00B204E9800998ECF8427E\"" })).?;
    try std.testing.expectEqualStrings(want, got);
    try std.testing.expect((try multipartEtag(a, &.{"\"kms-opaque-etag\""})) == null);
}

pub const provider = objstore.Provider{
    .scheme = .s3,
    .empty_prefix = Error.S3EmptyPrefix,
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
    const o = try arena.create(Obj);
    o.* = try parseUrl(arena, url, endpointFromEnv(arena));
    return .{ .provider = &provider, .url = o.url, .ptr = o };
}

fn vtListPrefix(arena: std.mem.Allocator, client: *std.http.Client, url: []const u8) anyerror![]const []const u8 {
    const p = try parsePrefix(url);
    const keys = try listPrefix(arena, client, p.bucket, p.prefix, endpointFromEnv(arena));
    const urls = try arena.alloc([]const u8, keys.len);
    for (keys, urls) |k, *u| u.* = try msgfmt.allocPrint(arena, "s3://{s}/{s}", .{ p.bucket, k });
    return urls;
}

fn vtRequestHeaders(ptr: *const anyopaque, arena: std.mem.Allocator, method: []const u8, range: []const u8) anyerror![]const std.http.Header {
    return requestHeaders(arena, objstore.cast(Obj, ptr).*, method, range);
}

fn vtStatusToError(code: u16, body: []const u8) anyerror {
    return statusToError(code, body);
}

fn vtOpenWriter(ptr: *const anyopaque, arena: std.mem.Allocator, client: ?*std.http.Client, content_type: []const u8) anyerror!objstore.Writer {
    return objstore.writer(try MultipartWriter.init(arena, client, objstore.cast(Obj, ptr).*, content_type));
}

fn vtPutIfAbsent(ptr: *const anyopaque, arena: std.mem.Allocator, client: *std.http.Client, bytes: []const u8, content_type: []const u8, why: *[]const u8) anyerror!bool {
    var w = MultipartWriter{
        .interface = .{ .vtable = &MultipartWriter.vtable, .buffer = &.{} },
        .arena = arena,
        .up = .{ .client = client },
        .obj = objstore.cast(Obj, ptr).*,
        .creds = try credsFromEnv(arena),
        .content_type = content_type,
        .etags = std.array_list.Managed([]const u8).init(arena),
        .rand = objstore.jitterPrng(),
    };
    defer w.up.release();
    return w.putIfAbsent(bytes) catch |e| {
        why.* = arena.dupe(u8, w.last_error) catch "";
        return e;
    };
}

test "putIfAbsent: 200 creates, 412 and 409 find one there, a 503 is retried, a 501 is refused" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    var srv: http_fake.FakeServer = undefined;
    try srv.start(&.{
        reply("503 Slow Down", "", "<Error><Code>SlowDown</Code></Error>"),
        reply("200 OK", "", ""),
        reply("412 Precondition Failed", "", "<Error><Code>PreconditionFailed</Code></Error>"),
        reply("409 Conflict", "", "<Error><Code>ConditionalRequestConflict</Code></Error>"),
        reply("501 Not Implemented", "", "<Error><Code>NotImplemented</Code><Message>If-None-Match</Message></Error>"),
    });
    defer srv.stop();
    var w = try testWriter(a, &client, &srv);
    try std.testing.expect(try w.putIfAbsent("{}"));
    try std.testing.expect(!try w.putIfAbsent("{}"));
    try std.testing.expect(!try w.putIfAbsent("{}"));
    try std.testing.expectError(Error.S3ConditionalWriteUnsupported, w.putIfAbsent("{}"));
    try std.testing.expectEqualStrings("PUT /lake/out.csv HTTP/1.1", srv.line(0));
}

test "every retried status is S3Throttled, which a run exits on as transient; 501, 505 and 511 are RequestFailed" {
    const env = @import("../runtime/env.zig");
    for ([_]u16{ 408, 429, 500, 502, 503, 504, 507, 509, 599 }) |c| {
        try std.testing.expectEqual(Error.S3Throttled, statusToError(c, ""));
        try std.testing.expectEqual(Error.S3Throttled, statusToError(c, "<Error><Code>InternalError</Code></Error>"));
        try std.testing.expect(env.isTransient(statusToError(c, "")));
    }
    for ([_]u16{ 400, 405, 409, 412, 418, 501, 505, 511, 600 }) |c| {
        try std.testing.expectEqual(Error.S3RequestFailed, statusToError(c, ""));
        try std.testing.expect(!env.isTransient(statusToError(c, "")));
    }
    try std.testing.expectEqual(Error.S3Throttled, statusToError(400, "<Error><Code>SlowDown</Code></Error>"));
    try std.testing.expectEqual(Error.S3AuthFailed, statusToError(503, "<Error><Code>AccessDenied</Code></Error>"));
    for (0..1000) |c| {
        const code: u16 = @intCast(c);
        try std.testing.expectEqual(objstore.retriable(code), statusToError(code, "") == Error.S3Throttled);
    }
}

test "amzDate on civil's Layout writes exactly what the hand-rolled one did, years 0000 to 99999" {
    const Old = struct {
        fn amz(arena: std.mem.Allocator, epoch_secs: i64) ![]const u8 {
            const days = @divFloor(epoch_secs, 86400);
            const secs: u32 = @intCast(epoch_secs - days * 86400);
            const c = civil.civilFromDays(days);
            return msgfmt.allocPrint(arena, "{d:0>4}{d:0>2}{d:0>2}T{d:0>2}{d:0>2}{d:0>2}Z", .{
                @as(u32, @intCast(c.y)), c.m, c.d, secs / 3600, (secs % 3600) / 60, secs % 60,
            });
        }
    };
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const lo: i64 = civil.daysFromCivil(0, 1, 1) * 86400;
    const hi: i64 = civil.daysFromCivil(100000, 1, 1) * 86400 - 1;
    var prng = std.Random.DefaultPrng.init(7);
    const r = prng.random();
    const edges = [_]i64{ lo, -1, 0, 1, 86399, 951782400, 1369353600, 253402300799, 253402300800, hi };
    for (0..100_000 + edges.len) |i| {
        const t = if (i < edges.len) edges[i] else r.intRangeAtMost(i64, lo, hi);
        _ = ar.reset(.retain_capacity);
        try std.testing.expectEqualStrings(try Old.amz(a, t), try amzDate(a, t));
    }
}
