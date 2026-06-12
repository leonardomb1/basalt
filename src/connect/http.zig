//! `read http` — REST/JSON source. GETs a URL and yields one batch per page of
//! JSON objects. Auth and pagination ride on the read stage's hints:
//!
//!   read http "https://api.x/items" @[bearer_env = API_TOKEN, items = "data",
//!     paginate = cursor, cursor_field = "next"]
//!
//! Pagination modes:
//!   (none)             one GET, one batch.
//!   paginate = page    `?<page_param>=N` (N starts at `start_page`), optionally
//!                      `&<size_param>=<page_size>`; stops on an empty page.
//!   paginate = offset  like `page`, but the param advances by `page_size` from
//!                      `start_offset` (OData `$skip`/`$top` style).
//!   paginate = cursor  reads `<cursor_field>` (dotted path) from each response;
//!                      a value starting with http(s):// is the next URL, anything
//!                      else is sent as `?<cursor_param>=<value>`; stops when the
//!                      field is missing/null/empty or the page is empty.
//! `max_pages` caps every mode so a misbehaving API can't loop forever.
//!
//! Auth: `bearer_env`/`auth_env`/`user_env`+`pass_env` name environment variables
//! (keeps tokens out of script text); `bearer` prepends "Bearer ", `auth`/`auth_env`
//! send the Authorization value verbatim (session-token APIs). `bearer`/`auth`
//! take literals (useful with for-loop `${var}` interpolation), and
//! `header = "Name: value"` adds one extra raw header.
//!
//! Connection-level form: `connection itsm = http` + `read itsm "/path?query"`.
//! The path resolves against the connection's `base_url` (spaces auto-encoded,
//! so OData filters read naturally) and auth lives on the connection:
//!   auth = "bearer"      token = secret("TOK")               -> Bearer <token>
//!   auth = "basic"       user = ..., password = ...          -> Basic <b64>
//!   auth = "header"      header_name/header_value            -> any API-key header
//!   auth = "login_json"  login_url + body_* attrs            -> POST a JSON object
//!                        built from every `body_<field>` attr; the response token
//!                        (`token_path`, default the whole response string) is sent
//!                        as `token_header` (default Authorization) with
//!                        `token_prefix` (default none).
//!   auth = "oauth2"      login_url ("token_url" also accepted) + client_id/
//!                        client_secret [+ scope] -> client-credentials form POST;
//!                        token_path defaults to access_token, prefix to "Bearer ".
//! Session kinds (login_json, oauth2) re-login and retry the page once on a 401,
//! so server-side session expiry mid-pull heals itself.
//!
//! The schema is inferred from the first page (request.zig rules); later pages
//! coerce to it — missing keys become null, new keys are dropped. Each page is
//! fetched whole into the per-batch arena (pages are bounded; the stream as a
//! whole is not, so memory stays flat across pages).

const std = @import("std");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const batchmod = @import("../exec/batch.zig");
const driver = @import("driver.zig");
const request = @import("request.zig");
const tlsmod = @import("tls_client.zig");

const Batch = batchmod.Batch;
const json = std.json;

/// std's verifier does no path building: it walks the server's chain strictly
/// in presentation order, so a misordered chain (a depressingly common server
/// misconfiguration) fails with CertificateIssuerMismatch even though every
/// certificate is valid. This fixes that out-of-band: harvest the chain over an
/// unverified handshake, then add to the bundle each presented certificate that
/// itself verifies against a certificate already in the bundle, repeating until
/// a fixpoint so order doesn't matter. Nothing gets trusted that doesn't chain
/// to an existing root — this only reorders trust the server already earned.
/// Returns true if the bundle gained at least one certificate.
pub fn repairBundle(gpa: std.mem.Allocator, bundle: *std.crypto.Certificate.Bundle, host: []const u8, port: u16) bool {
    var cap = tlsmod.ChainCapture{};
    harvestChain(gpa, host, port, &cap) catch return false;
    const now = std.time.timestamp();
    var added_any = false;
    var progress = true;
    while (progress) {
        progress = false;
        for (0..cap.count) |i| {
            if (cap.used[i]) continue;
            const der = cap.bufs[i][0..cap.lens[i]];
            const cert = std.crypto.Certificate{ .buffer = der, .index = 0 };
            const parsed = cert.parse() catch {
                cap.used[i] = true;
                continue;
            };
            // Only admit certs whose issuer is already trusted.
            bundle.verify(parsed, now) catch continue;
            const start: u32 = @intCast(bundle.bytes.items.len);
            bundle.bytes.appendSlice(gpa, der) catch return added_any;
            bundle.parseCert(gpa, start, now) catch return added_any;
            cap.used[i] = true;
            progress = true;
            added_any = true;
        }
    }
    return added_any;
}

/// Capture the certificate chain a server presents, trusting nothing: the
/// hostname is still sent and checked (we need SNI to reach the right vhost)
/// but the CA path is not verified.
fn harvestChain(gpa: std.mem.Allocator, host: []const u8, port: u16, cap: *tlsmod.ChainCapture) !void {
    const stream = try std.net.tcpConnectToHost(gpa, host, port);
    defer stream.close();
    var in_buf: [std.crypto.tls.max_ciphertext_record_len]u8 = undefined;
    var out_buf: [std.crypto.tls.max_ciphertext_record_len]u8 = undefined;
    var sr = stream.reader(&in_buf);
    var sw = stream.writer(&out_buf);
    var wbuf: [std.crypto.tls.max_ciphertext_record_len]u8 = undefined;
    var rbuf: [std.crypto.tls.max_ciphertext_record_len * 2]u8 = undefined;
    _ = try tlsmod.init(sr.interface(), &sw.interface, .{
        .host = .{ .explicit = host },
        .ca = .no_verification,
        .chain = cap,
        .write_buffer = &wbuf,
        .read_buffer = &rbuf,
    });
}

pub fn uriHost(uri: std.Uri) ?[]const u8 {
    const c = uri.host orelse return null;
    return switch (c) {
        .raw => |s| s,
        .percent_encoded => |s| s,
    };
}

pub const AuthKind = enum { none, bearer, basic, header, login_json, oauth2 };

pub const KV = struct { key: []const u8, value: []const u8 };

/// Resolved configuration of a `connection <name> = http` block.
pub const ConnConfig = struct {
    base_url: []const u8 = "",
    auth: AuthKind = .none,
    token: []const u8 = "", // bearer
    user: []const u8 = "", // basic / oauth2 client_id
    password: []const u8 = "", // basic / oauth2 client_secret
    header_name: []const u8 = "", // header kind
    header_value: []const u8 = "",
    login_url: []const u8 = "", // login_json / oauth2 (absolute or base_url-relative)
    scope: []const u8 = "", // oauth2, optional
    body: []const KV = &.{}, // login_json: the POSTed JSON object's fields
    token_path: []const u8 = "", // dotted path to the token; "" = kind default
    token_header: []const u8 = "Authorization",
    token_prefix: ?[]const u8 = null, // null = kind default ("" / "Bearer ")
};

/// Build a ConnConfig from resolved (string) connection attrs. `body_<field>`
/// attrs pass through into the login JSON object — vendor-agnostic, so any
/// login body shape works without templating. Unknown keys are an error
/// (connections are explicit config, unlike advisory hints); `errmsg` says which.
pub fn connFromKvs(arena: std.mem.Allocator, kvs: []const KV, errmsg: *[]const u8) !ConnConfig {
    var cc = ConnConfig{};
    var body = std.array_list.Managed(KV).init(arena);
    for (kvs) |kv| {
        if (std.mem.startsWith(u8, kv.key, "body_")) {
            try body.append(.{ .key = kv.key["body_".len..], .value = kv.value });
        } else if (std.mem.eql(u8, kv.key, "base_url")) {
            cc.base_url = kv.value;
        } else if (std.mem.eql(u8, kv.key, "auth")) {
            cc.auth = std.meta.stringToEnum(AuthKind, kv.value) orelse {
                errmsg.* = try std.fmt.allocPrint(arena, "unknown auth kind `{s}`", .{kv.value});
                return error.BadHttpConn;
            };
        } else if (std.mem.eql(u8, kv.key, "token")) {
            cc.token = kv.value;
        } else if (std.mem.eql(u8, kv.key, "user") or std.mem.eql(u8, kv.key, "client_id")) {
            cc.user = kv.value;
        } else if (std.mem.eql(u8, kv.key, "password") or std.mem.eql(u8, kv.key, "client_secret")) {
            cc.password = kv.value;
        } else if (std.mem.eql(u8, kv.key, "header_name")) {
            cc.header_name = kv.value;
        } else if (std.mem.eql(u8, kv.key, "header_value")) {
            cc.header_value = kv.value;
        } else if (std.mem.eql(u8, kv.key, "login_url") or std.mem.eql(u8, kv.key, "login_path") or std.mem.eql(u8, kv.key, "token_url")) {
            cc.login_url = kv.value;
        } else if (std.mem.eql(u8, kv.key, "scope")) {
            cc.scope = kv.value;
        } else if (std.mem.eql(u8, kv.key, "token_path")) {
            cc.token_path = kv.value;
        } else if (std.mem.eql(u8, kv.key, "token_header")) {
            cc.token_header = kv.value;
        } else if (std.mem.eql(u8, kv.key, "token_prefix")) {
            cc.token_prefix = kv.value;
        } else {
            errmsg.* = try std.fmt.allocPrint(arena, "unknown attribute `{s}`", .{kv.key});
            return error.BadHttpConn;
        }
    }
    cc.body = try body.toOwnedSlice();
    if (cc.base_url.len == 0) {
        errmsg.* = "missing `base_url`";
        return error.BadHttpConn;
    }
    return cc;
}

/// Produces and refreshes the auth header for a connection. Static kinds
/// (bearer/basic/header) compute once; session kinds (login_json/oauth2) log in
/// lazily and can mint a fresh token after a 401.
pub const AuthState = struct {
    arena: std.mem.Allocator,
    cc: ConnConfig,
    header: ?std.http.Header = null,

    pub fn ensure(self: *AuthState, client: *std.http.Client) !?std.http.Header {
        if (self.header) |h| return h;
        switch (self.cc.auth) {
            .none => return null,
            .bearer => self.header = .{
                .name = "Authorization",
                .value = try std.fmt.allocPrint(self.arena, "Bearer {s}", .{self.cc.token}),
            },
            .basic => {
                const raw = try std.fmt.allocPrint(self.arena, "{s}:{s}", .{ self.cc.user, self.cc.password });
                const enc = std.base64.standard.Encoder;
                const b64 = try self.arena.alloc(u8, enc.calcSize(raw.len));
                _ = enc.encode(b64, raw);
                self.header = .{ .name = "Authorization", .value = try std.fmt.allocPrint(self.arena, "Basic {s}", .{b64}) };
            },
            .header => self.header = .{ .name = self.cc.header_name, .value = self.cc.header_value },
            .login_json, .oauth2 => try self.login(client),
        }
        return self.header;
    }

    /// Session kinds re-login (server-side expiry mid-pull); static kinds can't.
    pub fn refresh(self: *AuthState, client: *std.http.Client) bool {
        switch (self.cc.auth) {
            .login_json, .oauth2 => {
                self.header = null;
                self.login(client) catch return false;
                return true;
            },
            else => return false,
        }
    }

    fn login(self: *AuthState, client: *std.http.Client) !void {
        const arena = self.arena;
        const is_oauth = self.cc.auth == .oauth2;
        var content_type: []const u8 = undefined;
        var payload: []const u8 = undefined;
        if (is_oauth) {
            content_type = "application/x-www-form-urlencoded";
            var buf = std.array_list.Managed(u8).init(arena);
            try buf.appendSlice("grant_type=client_credentials");
            try appendForm(&buf, "client_id", self.cc.user);
            try appendForm(&buf, "client_secret", self.cc.password);
            if (self.cc.scope.len > 0) try appendForm(&buf, "scope", self.cc.scope);
            payload = try buf.toOwnedSlice();
        } else {
            content_type = "application/json";
            var map = std.json.ObjectMap.init(arena);
            for (self.cc.body) |kv| try map.put(kv.key, .{ .string = kv.value });
            payload = try std.json.Stringify.valueAlloc(arena, std.json.Value{ .object = map }, .{});
        }

        var aw = std.Io.Writer.Allocating.init(arena);
        const res = try client.fetch(.{
            .method = .POST,
            .location = .{ .url = self.cc.login_url },
            .headers = .{ .content_type = .{ .override = content_type } },
            .payload = payload,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        if (code != 200) {
            const b = aw.writer.buffered();
            std.debug.print("login http {d} from {s}: {s}\n", .{ code, self.cc.login_url, b[0..@min(b.len, 300)] });
            return statusError(code);
        }
        const root = try json.parseFromSliceLeaky(json.Value, arena, aw.writer.buffered(), .{});
        const path = if (self.cc.token_path.len > 0)
            self.cc.token_path
        else if (is_oauth) "access_token" else ".";
        const tok_val = if (std.mem.eql(u8, path, ".")) root else (jsonPath(root, path) orelse return error.LoginTokenMissing);
        const tok = switch (tok_val) {
            .string => |t| t,
            else => return error.LoginTokenMissing,
        };
        const prefix = self.cc.token_prefix orelse (if (is_oauth) "Bearer " else "");
        self.header = .{
            .name = self.cc.token_header,
            .value = try std.fmt.allocPrint(arena, "{s}{s}", .{ prefix, tok }),
        };
    }
};

fn appendForm(buf: *std.array_list.Managed(u8), key: []const u8, val: []const u8) !void {
    try buf.append('&');
    try buf.appendSlice(key);
    try buf.append('=');
    const hex = "0123456789ABCDEF";
    for (val) |c| {
        if (formUnreserved(c)) {
            try buf.append(c);
        } else {
            try buf.append('%');
            try buf.append(hex[c >> 4]);
            try buf.append(hex[c & 0xF]);
        }
    }
}

fn formUnreserved(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '-' or c == '_' or c == '.' or c == '~';
}

/// Resolve a read path against a base URL; absolute http(s) paths pass through.
pub fn joinUrl(arena: std.mem.Allocator, base: []const u8, path: []const u8) ![]const u8 {
    if (path.len == 0) return base;
    if (std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://")) return path;
    const b = std.mem.trimRight(u8, base, "/");
    const sep: []const u8 = if (path.len > 0 and path[0] == '/') "" else "/";
    return std.fmt.allocPrint(arena, "{s}{s}{s}", .{ b, sep, path });
}

/// Encode the one character that breaks URI parsing but appears constantly in
/// hand-written query strings (OData filters): the space. Everything else is
/// the script author's responsibility.
pub fn encodeSpaces(arena: std.mem.Allocator, url: []const u8) ![]const u8 {
    const n = std.mem.count(u8, url, " ");
    if (n == 0) return url;
    const out = try arena.alloc(u8, url.len + n * 2);
    var j: usize = 0;
    for (url) |c| {
        if (c == ' ') {
            @memcpy(out[j..][0..3], "%20");
            j += 3;
        } else {
            out[j] = c;
            j += 1;
        }
    }
    return out;
}

/// A std.http.Client with the CA store pre-loaded: system roots plus, when the
/// BASALT_CA_BUNDLE env var names a PEM file, every certificate in it. That's
/// the manual override for chains the automatic repair can't fix (e.g. a server
/// that omits its intermediate entirely — repair can only re-anchor what the
/// server actually sends).
pub fn initClient(gpa: std.mem.Allocator) std.http.Client {
    var client = std.http.Client{ .allocator = gpa };
    const path = std.process.getEnvVarOwned(gpa, "BASALT_CA_BUNDLE") catch return client;
    client.ca_bundle.rescan(gpa) catch return client;
    client.ca_bundle.addCertsFromFilePath(gpa, std.fs.cwd(), path) catch return client;
    client.next_https_rescan_certs = false;
    return client;
}

pub const Mode = enum { none, page, offset, cursor };

pub const Options = struct {
    bearer: ?[]const u8 = null, // literal token
    bearer_env: ?[]const u8 = null, // env var holding the token
    auth: ?[]const u8 = null, // literal raw Authorization header value
    auth_env: ?[]const u8 = null, // env var holding the raw Authorization value
    user_env: ?[]const u8 = null, // basic auth env vars
    pass_env: ?[]const u8 = null,
    header: ?[]const u8 = null, // one extra raw "Name: value" header
    items: ?[]const u8 = null, // dotted path to the row array in the response
    method: enum { get, post } = .get,
    /// POST body sent with every page. With page/offset pagination the page
    /// param is appended to the BODY (`&<page_param>=N`), not the URL — the
    /// form-style APIs that want POST paginate in the body.
    body: ?[]const u8 = null,
    body_type: []const u8 = "form", // form -> x-www-form-urlencoded, json -> application/json
    paginate: Mode = .none,
    page_param: []const u8 = "page", // page number (page mode) or offset (offset mode) param
    start_page: i64 = 1,
    start_offset: i64 = 0,
    size_param: ?[]const u8 = null,
    page_size: i64 = 100,
    cursor_param: []const u8 = "cursor",
    cursor_field: []const u8 = "next",
    max_pages: i64 = 10_000,
    /// page/offset modes: fetch up to N pages concurrently (1 = sequential).
    /// The win is server-side latency: slow APIs compute pages in parallel.
    /// Counterproductive on servers that serialize requests per session.
    prefetch: i64 = 1,
    /// Transient failures (429/5xx, connection resets) retry in place with
    /// exponential backoff + jitter, like any robust extraction client. Without
    /// this, one blip at page 400k of a long backfill restarts the whole run.
    retries: i64 = 2, // extra attempts per request (3 total, matching common practice)
    retry_base_ms: i64 = 500,
    /// Per-page deadline (ms) for page fetches (both sequential and prefetched
    /// paths); 0 disables. A request
    /// that black-holes (no reset, just silence) is abandoned at the deadline
    /// and surfaces as ConnectionTimedOut (transient). The blocked worker
    /// thread can't be interrupted — it frees itself if the server ever
    /// answers, and is leaked (bounded, counted) if it never does.
    timeout_ms: i64 = 300_000,
    /// Progress heartbeat to stderr every N ms during multi-page pulls
    /// (0 = silent). Long extractions are otherwise mute for an hour.
    progress_ms: i64 = 30_000,
    /// Extra status codes to treat as transient (comma-separated, e.g. "404,408").
    /// For vendors that lie: iFractal answers 404 with "Erro ao conectar no banco
    /// de dados" when ITS database is down. Listed codes retry with backoff and,
    /// if they outlive the retry budget, exit 75 so the control plane re-runs.
    retry_statuses: ?[]const u8 = null,
    /// page mode: dotted path to a "total pages" field in the response; when
    /// set, exactly that many pages are fetched. For APIs that never return an
    /// empty page (page-overrun keeps yielding data), where the empty-page
    /// detector can't terminate.
    total_field: ?[]const u8 = null,
    /// page/offset modes: a page shorter than page_size ends the stream,
    /// skipping the trailing empty-page request (a full server-side scan on
    /// slow OData backends). Opt-in: unsafe when the server caps page size
    /// below the requested one (the short page would lie).
    stop_short: bool = false,
};

/// Hint keys mirror the Options field names. Unknown hints are ignored, per the
/// engine-wide hint convention.
pub fn optsFromHints(hints: []const ast.Hint) Options {
    var o = Options{};
    for (hints) |h| {
        const sv: ?[]const u8 = switch (h.value) {
            .str => |s| s,
            .ident => |s| s,
            else => null,
        };
        const iv: ?i64 = if (h.value == .int) h.value.int else null;
        if (std.mem.eql(u8, h.key, "bearer")) {
            o.bearer = sv;
        } else if (std.mem.eql(u8, h.key, "bearer_env")) {
            o.bearer_env = sv;
        } else if (std.mem.eql(u8, h.key, "auth")) {
            o.auth = sv;
        } else if (std.mem.eql(u8, h.key, "auth_env")) {
            o.auth_env = sv;
        } else if (std.mem.eql(u8, h.key, "user_env")) {
            o.user_env = sv;
        } else if (std.mem.eql(u8, h.key, "pass_env")) {
            o.pass_env = sv;
        } else if (std.mem.eql(u8, h.key, "header")) {
            o.header = sv;
        } else if (std.mem.eql(u8, h.key, "items")) {
            o.items = sv;
        } else if (std.mem.eql(u8, h.key, "method")) {
            if (sv) |v| {
                if (std.mem.eql(u8, v, "post")) o.method = .post;
            }
        } else if (std.mem.eql(u8, h.key, "body")) {
            o.body = sv;
        } else if (std.mem.eql(u8, h.key, "body_type")) {
            if (sv) |v| o.body_type = v;
        } else if (std.mem.eql(u8, h.key, "paginate")) {
            if (sv) |s| o.paginate = std.meta.stringToEnum(Mode, s) orelse .none;
        } else if (std.mem.eql(u8, h.key, "page_param")) {
            if (sv) |s| o.page_param = s;
        } else if (std.mem.eql(u8, h.key, "start_page")) {
            if (iv) |n| o.start_page = n;
        } else if (std.mem.eql(u8, h.key, "start_offset")) {
            if (iv) |n| o.start_offset = n;
        } else if (std.mem.eql(u8, h.key, "size_param")) {
            o.size_param = sv;
        } else if (std.mem.eql(u8, h.key, "page_size")) {
            if (iv) |n| o.page_size = n;
        } else if (std.mem.eql(u8, h.key, "cursor_param")) {
            if (sv) |s| o.cursor_param = s;
        } else if (std.mem.eql(u8, h.key, "cursor_field")) {
            if (sv) |s| o.cursor_field = s;
        } else if (std.mem.eql(u8, h.key, "max_pages")) {
            if (iv) |n| o.max_pages = n;
        } else if (std.mem.eql(u8, h.key, "prefetch")) {
            if (iv) |n| o.prefetch = n;
        } else if (std.mem.eql(u8, h.key, "stop_short")) {
            o.stop_short = (h.value == .flag);
        } else if (std.mem.eql(u8, h.key, "total_field")) {
            o.total_field = sv;
        } else if (std.mem.eql(u8, h.key, "retries")) {
            if (iv) |n| o.retries = n;
        } else if (std.mem.eql(u8, h.key, "retry_base_ms")) {
            if (iv) |n| o.retry_base_ms = n;
        } else if (std.mem.eql(u8, h.key, "retry_statuses")) {
            o.retry_statuses = sv;
        } else if (std.mem.eql(u8, h.key, "progress_ms")) {
            if (iv) |n| o.progress_ms = n;
        } else if (std.mem.eql(u8, h.key, "timeout_ms")) {
            if (iv) |n| o.timeout_ms = n;
        }
    }
    return o;
}

pub const HttpSource = struct {
    arena: std.mem.Allocator, // run arena: self, schema, headers, first page
    gpa: std.mem.Allocator, // worker page bodies (freed after each batch)
    /// gpa-allocated (not in the run arena): an abandoned worker may still be
    /// using it after the run's arena is freed.
    client: *std.http.Client,
    zombies: usize = 0, // abandoned (timed-out) workers still in flight
    base_url: []const u8,
    opts: Options,
    headers: []const std.http.Header,
    schema: *types.Schema,
    first: ?Batch, // built at open() so the schema is known up front
    page_no: i64,
    next_url: ?[]const u8 = null, // cursor mode: resolved next request URL
    pages_fetched: i64 = 0,
    done: bool = false,
    repaired: bool = false, // one trust-repair attempt per source
    auth: ?*AuthState = null, // connection-level auth (read http URL form: null)
    slots: std.array_list.Managed(*Slot) = undefined, // in-flight prefetched pages, FIFO
    pages_issued: i64 = 0,
    issue_done: bool = false,
    auth_gen: u32 = 0, // bumped on re-login so stale-token slots retry without a second login
    total_pages: ?i64 = null, // from total_field on the first response
    rows_done: u64 = 0,
    pages_done: i64 = 0,
    last_progress_ms: i64 = 0,

    const SlotState = enum(u8) { running, done, abandoned };

    /// One prefetched page in flight — fully self-contained: every slice it
    /// reads lives in its own arena snapshot (never the run arena), so a worker
    /// abandoned at the timeout deadline can safely outlive the run. The state
    /// CAS decides who frees the slot: consumer (done) or worker (abandoned).
    const Slot = struct {
        state: std.atomic.Value(u8) = std.atomic.Value(u8).init(@intFromEnum(SlotState.running)),
        gpa: std.mem.Allocator,
        snap: std.heap.ArenaAllocator, // owns url/req_body/hdrs/body below
        client: *std.http.Client,
        method_post: bool,
        content_type: ?[]const u8, // static string
        url: []const u8,
        req_body: ?[]const u8 = null,
        hdrs: [12]std.http.Header = undefined,
        nh: usize = 0,
        gen: u32,
        retries: i64,
        retry_base_ms: i64,
        retry_statuses: ?[]const u8 = null,
        code: u16 = 0,
        body: []const u8 = &.{},
        err: ?anyerror = null,
    };

    fn freeSlot(slot: *Slot) void {
        const gpa = slot.gpa;
        slot.snap.deinit();
        gpa.destroy(slot);
    }

    pub fn open(arena: std.mem.Allocator, gpa: std.mem.Allocator, url: []const u8, opts: Options) !*HttpSource {
        return openConn(arena, gpa, .{ .base_url = url }, "", opts);
    }

    /// Open against a `connection ... = http`: `path` resolves on the
    /// connection's base_url, and the connection's auth kind applies (with
    /// mid-run re-login for session kinds).
    pub fn openConn(arena: std.mem.Allocator, gpa: std.mem.Allocator, cc: ConnConfig, path: []const u8, opts: Options) !*HttpSource {
        // A line break in the URL is always an accident (an editor or terminal
        // hard-wrapped the script line mid-string); fail with a name that says
        // so rather than sending a corrupt request.
        if (std.mem.indexOfAny(u8, path, "\r\n") != null) return error.UrlContainsLineBreak;
        const url = try encodeSpaces(arena, try joinUrl(arena, cc.base_url, path));
        const client = try gpa.create(std.http.Client);
        client.* = initClient(gpa);
        errdefer {
            client.deinit();
            gpa.destroy(client);
        }
        const self = try arena.create(HttpSource);
        self.* = .{
            .arena = arena,
            .gpa = gpa,
            // The client allocator must be thread-safe (prefetch workers share
            // the client and its connection pool); the run arena is not.
            .client = client,
            .base_url = url,
            .auth = null,
            .opts = opts,
            .headers = try buildHeaders(arena, opts),
            .schema = undefined,
            .first = null,
            .page_no = if (opts.paginate == .offset) opts.start_offset else opts.start_page,
        };
        self.slots = std.array_list.Managed(*Slot).init(gpa);
        if (cc.auth != .none) {
            var rcc = cc;
            // The login endpoint may be base_url-relative; resolve it once here.
            rcc.login_url = try joinUrl(arena, cc.base_url, cc.login_url);
            const a = try arena.create(AuthState);
            a.* = .{ .arena = arena, .cc = rcc };
            self.auth = a;
        }

        const first_req = try self.pageReq(arena);
        const page = try self.fetchParsed(arena, first_req);
        const items = page.items;
        self.schema = try request.inferSchema(arena, items);
        self.first = try request.batchFromJson(arena, self.schema, items);
        self.noteProgress(items.len);
        if (opts.total_field) |tf| {
            if (jsonPath(page.root, tf)) |tv| {
                if (tv == .integer) self.total_pages = tv.integer;
            }
        }
        self.advance(arena, page.root, items.len) catch |e| return e;
        return self;
    }

    /// Heartbeat so a 30-minute pull isn't silent: pages done (of total when
    /// known) and rows so far, to stderr like the other source diagnostics.
    fn noteProgress(self: *HttpSource, n_rows: usize) void {
        self.pages_done += 1;
        self.rows_done += n_rows;
        if (self.opts.progress_ms <= 0) return;
        const now = std.time.milliTimestamp();
        if (self.last_progress_ms == 0) {
            self.last_progress_ms = now;
            return;
        }
        if (now - self.last_progress_ms < self.opts.progress_ms) return;
        self.last_progress_ms = now;
        if (self.total_pages) |t| {
            std.debug.print("[http] pages {d}/{d}, rows {d}\n", .{ self.pages_done, t, self.rows_done });
        } else {
            std.debug.print("[http] pages {d}, rows {d}\n", .{ self.pages_done, self.rows_done });
        }
    }

    const Page = struct { root: json.Value, items: []const json.Value };

    /// Fetch one page and extract its rows. An empty body or a bare JSON `null`
    /// (end-of-dataset markers in the wild, alongside http 204) yields no items.
    fn fetchParsed(self: *HttpSource, arena: std.mem.Allocator, req: PageReq) !Page {
        const body = try self.fetchPage(arena, req);
        const trimmed = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed.len == 0) return .{ .root = .null, .items = &.{} };
        const root = try json.parseFromSliceLeaky(json.Value, arena, trimmed, .{});
        if (root == .null) return .{ .root = .null, .items = &.{} };
        return .{ .root = root, .items = try itemsOf(arena, root, self.opts.items) };
    }

    /// Per-page bookkeeping: decide whether another page exists and what its URL is.
    fn advance(self: *HttpSource, arena: std.mem.Allocator, root: json.Value, n_items: usize) !void {
        self.pages_fetched += 1;
        switch (self.opts.paginate) {
            .none => self.done = true,
            .page => {
                self.page_no += 1;
                if (self.pageEnds(n_items)) self.done = true;
            },
            .offset => {
                self.page_no += self.opts.page_size;
                if (self.pageEnds(n_items)) self.done = true;
            },
            .cursor => {
                self.next_url = null;
                if (n_items == 0 or self.pages_fetched >= self.opts.max_pages) {
                    self.done = true;
                    return;
                }
                const cur = jsonPath(root, self.opts.cursor_field) orelse {
                    self.done = true;
                    return;
                };
                const tok = switch (cur) {
                    .string => |s| s,
                    else => {
                        self.done = true;
                        return;
                    },
                };
                if (tok.len == 0) {
                    self.done = true;
                    return;
                }
                // Cursor values live in the page arena (freed after the batch), so
                // the next URL is duped into the run arena to survive the reset.
                if (std.mem.startsWith(u8, tok, "http://") or std.mem.startsWith(u8, tok, "https://")) {
                    self.next_url = try self.arena.dupe(u8, tok);
                } else {
                    self.next_url = try withParam(self.arena, self.base_url, self.opts.cursor_param, tok);
                }
                _ = arena;
            },
        }
    }

    fn pageEnds(self: *HttpSource, n_items: usize) bool {
        if (n_items == 0 or self.pages_fetched >= self.opts.max_pages) return true;
        if (self.total_pages) |t| {
            if (self.pages_fetched >= t) return true;
        }
        return self.opts.stop_short and n_items < self.opts.page_size;
    }

    const PageReq = struct { url: []const u8, body: ?[]const u8 };

    fn pageReq(self: *HttpSource, arena: std.mem.Allocator) !PageReq {
        switch (self.opts.paginate) {
            .none => return .{ .url = self.base_url, .body = self.opts.body },
            .page, .offset => {
                var buf: [32]u8 = undefined;
                const n = try std.fmt.bufPrint(&buf, "{d}", .{self.page_no});
                if (self.opts.method == .post) {
                    // POST APIs paginate in the form body, not the query string.
                    var body = try appendParam(arena, self.opts.body orelse "", self.opts.page_param, n);
                    if (self.opts.size_param) |sp| {
                        const sz = try std.fmt.bufPrint(&buf, "{d}", .{self.opts.page_size});
                        body = try appendParam(arena, body, sp, sz);
                    }
                    return .{ .url = self.base_url, .body = body };
                }
                var url = try withParam(arena, self.base_url, self.opts.page_param, n);
                if (self.opts.size_param) |sp| {
                    const sz = try std.fmt.bufPrint(&buf, "{d}", .{self.opts.page_size});
                    url = try withParam(arena, url, sp, sz);
                }
                return .{ .url = url, .body = self.opts.body };
            },
            .cursor => return .{ .url = self.next_url orelse self.base_url, .body = self.opts.body },
        }
    }

    fn contentType(self: *HttpSource) ?[]const u8 {
        if (self.opts.body == null) return null;
        return if (std.mem.eql(u8, self.opts.body_type, "json")) "application/json" else "application/x-www-form-urlencoded";
    }

    fn fetchPage(self: *HttpSource, arena: std.mem.Allocator, req: PageReq) ![]const u8 {
        var attempt: i64 = 0;
        while (true) {
            return self.fetchOnce(arena, req) catch |e| {
                attempt += 1;
                if (isRetryableNet(e) and attempt <= self.opts.retries and !driver.aborting()) {
                    std.Thread.sleep(retryDelayNs(self.opts.retry_base_ms, attempt));
                    continue;
                }
                // Exhausted: classify HERE, where we know the peer was a network
                // socket — the run-level classifier must not treat the ambient
                // std.Io names (WriteFailed et al) as transient globally.
                return mapTransport(e);
            };
        }
    }

    fn fetchOnce(self: *HttpSource, arena: std.mem.Allocator, req: PageReq) ![]const u8 {
        // Pages on slow APIs run tens of seconds; honoring an abort here caps
        // cancellation latency at one in-flight request instead of a whole run.
        if (driver.aborting()) return error.Aborted;
        return self.rawOrTimed(arena, req) catch |e| switch (e) {
            // Likely a misordered server chain: rebuild trust from the chain
            // itself (repairBundle) and retry once.
            error.TlsInitializationFailed => {
                if (!self.tryRepair(req.url)) return e;
                return self.rawOrTimed(arena, req);
            },
            // Session expired mid-pull: session auth kinds re-login and the
            // page is retried once; static kinds surface the 401. A 401 in
            // retry_statuses still maps to transient when auth declines it.
            error.HttpUnauthorized => {
                const a = self.auth orelse return self.listedFallback(401, e);
                if (!a.refresh(self.client)) return self.listedFallback(401, e);
                return self.rawOrTimed(arena, req);
            },
            else => e,
        };
    }

    /// One request attempt with the page deadline applied (timeout_ms > 0):
    /// runs through the same slot machinery as prefetch, window of one, so the
    /// sequential/cursor/default paths get the black-hole protection too. The
    /// slot carries retries=0 — fetchPage owns the sequential retry budget.
    fn rawOrTimed(self: *HttpSource, arena: std.mem.Allocator, req: PageReq) ![]const u8 {
        if (self.opts.timeout_ms <= 0) return self.fetchPageRaw(arena, req);
        const slot = try self.spawnFetch(req, 0);
        const url_copy = arena.dupe(u8, slot.url) catch "";
        self.awaitSlot(slot) catch |e| {
            std.debug.print("page fetch abandoned ({s}): {s}\n", .{ @errorName(e), url_copy });
            return e; // slot now belongs to its worker — do not free or touch
        };
        defer freeSlot(slot);
        if (slot.err) |e| return e;
        if (slot.code == 204) return "";
        if (slot.code != 200) return self.raiseStatus(slot.code, slot.url, slot.body);
        return try arena.dupe(u8, slot.body);
    }

    fn tryRepair(self: *HttpSource, url: []const u8) bool {
        if (self.repaired) return false;
        self.repaired = true;
        const uri = std.Uri.parse(url) catch return false;
        const h = uriHost(uri) orelse return false;
        if (!repairBundle(self.client.allocator, &self.client.ca_bundle, h, uri.port orelse 443)) return false;
        self.client.next_https_rescan_certs = false;
        return true;
    }

    fn prefetchOn(self: *HttpSource) bool {
        return self.opts.prefetch > 1 and
            (self.opts.paginate == .page or self.opts.paginate == .offset);
    }

    /// Spawn a worker for the next page; page bookkeeping happens at issue time
    /// (the sequential path does it at consume time via advance()).
    fn issueSlot(self: *HttpSource) !void {
        // total_field: page 1 came from open(); never issue past page `total`.
        if (self.total_pages) |t| {
            if (1 + self.pages_issued >= t) {
                self.issue_done = true;
                return;
            }
        }
        if (self.pages_issued + 1 >= self.opts.max_pages) self.issue_done = true;
        // Reserve the list space first: once the worker is spawned and detached,
        // no error path may free the slot (the state CAS owns that decision).
        try self.slots.ensureUnusedCapacity(1);
        const slot = try self.spawnSlot();
        self.slots.appendAssumeCapacity(slot);
        switch (self.opts.paginate) {
            .page => self.page_no += 1,
            .offset => self.page_no += self.opts.page_size,
            else => unreachable,
        }
        self.pages_issued += 1;
    }

    /// Build + spawn the next page's slot. Page strings are built directly in
    /// the slot's own arena — building them in the run arena would grow it by
    /// ~2x url bytes per page for the life of the run.
    fn spawnSlot(self: *HttpSource) !*Slot {
        const slot = try self.gpa.create(Slot);
        slot.* = .{
            .gpa = self.gpa,
            .snap = std.heap.ArenaAllocator.init(self.gpa),
            .client = self.client,
            .method_post = self.opts.method == .post,
            .content_type = self.contentType(),
            .url = undefined,
            .gen = self.auth_gen,
            .retries = self.opts.retries,
            .retry_base_ms = self.opts.retry_base_ms,
        };
        errdefer freeSlot(slot); // safe: fires only before the thread exists
        const sa = slot.snap.allocator();
        const req = try self.pageReq(sa);
        slot.url = req.url; // page/offset urls are allocated by pageReq in sa
        if (req.body) |b| slot.req_body = try sa.dupe(u8, b);
        try self.snapshotInto(slot);
        var th = try std.Thread.spawn(.{}, workerMain, .{slot});
        th.detach();
        return slot;
    }

    /// Same, for an explicit request (the sequential timed path).
    fn spawnFetch(self: *HttpSource, req: PageReq, retries: i64) !*Slot {
        const slot = try self.gpa.create(Slot);
        slot.* = .{
            .gpa = self.gpa,
            .snap = std.heap.ArenaAllocator.init(self.gpa),
            .client = self.client,
            .method_post = self.opts.method == .post,
            .content_type = self.contentType(),
            .url = undefined,
            .gen = self.auth_gen,
            .retries = retries,
            .retry_base_ms = self.opts.retry_base_ms,
        };
        errdefer freeSlot(slot); // safe: fires only before the thread exists
        const sa = slot.snap.allocator();
        slot.url = try sa.dupe(u8, req.url);
        if (req.body) |b| slot.req_body = try sa.dupe(u8, b);
        try self.snapshotInto(slot);
        var th = try std.Thread.spawn(.{}, workerMain, .{slot});
        th.detach();
        return slot;
    }

    fn snapshotInto(self: *HttpSource, slot: *Slot) !void {
        const sa = slot.snap.allocator();
        if (self.opts.retry_statuses) |rs| slot.retry_statuses = try sa.dupe(u8, rs);
        for (self.headers) |h| {
            slot.hdrs[slot.nh] = .{ .name = try sa.dupe(u8, h.name), .value = try sa.dupe(u8, h.value) };
            slot.nh += 1;
        }
        if (self.auth) |a| {
            if (try a.ensure(self.client)) |h| {
                slot.hdrs[slot.nh] = .{ .name = try sa.dupe(u8, h.name), .value = try sa.dupe(u8, h.value) };
                slot.nh += 1;
            }
        }
    }

    /// Worker thread (detached): GET one page. Reads ONLY the slot (never the
    /// HttpSource, which lives in the run arena) plus the heap-allocated client,
    /// whose pool is mutex-guarded. On completion the state CAS hands the slot
    /// to the consumer — unless the consumer already abandoned it (timeout), in
    /// which case the worker frees it.
    fn workerMain(slot: *Slot) void {
        const sa = slot.snap.allocator();
        var attempt: i64 = 0;
        while (true) {
            var aw = std.Io.Writer.Allocating.init(slot.gpa);
            defer aw.deinit();
            const res = slot.client.fetch(.{
                .method = if (slot.method_post) .POST else .GET,
                .location = .{ .url = slot.url },
                .extra_headers = slot.hdrs[0..slot.nh],
                .headers = if (slot.content_type) |ct| .{ .content_type = .{ .override = ct } } else .{},
                .payload = slot.req_body,
                .response_writer = &aw.writer,
            }) catch |e| {
                attempt += 1;
                if (isRetryableNet(e) and attempt <= slot.retries and !driver.aborting()) {
                    std.Thread.sleep(retryDelayNs(slot.retry_base_ms, attempt));
                    continue;
                }
                slot.err = e;
                break;
            };
            const code = @intFromEnum(res.status);
            // Retry transient statuses in-thread too (worker = its own backoff).
            if ((code == 429 or code >= 500 or (code != 401 and statusListed(slot.retry_statuses, code))) and attempt < slot.retries and !driver.aborting()) {
                attempt += 1;
                std.Thread.sleep(retryDelayNs(slot.retry_base_ms, attempt));
                continue;
            }
            slot.code = code;
            slot.body = sa.dupe(u8, aw.writer.buffered()) catch |e| {
                slot.err = e;
                break;
            };
            break;
        }
        if (slot.state.cmpxchgStrong(
            @intFromEnum(SlotState.running),
            @intFromEnum(SlotState.done),
            .acq_rel,
            .acquire,
        ) != null) {
            // Consumer abandoned us at the deadline: nobody else will.
            freeSlot(slot);
        }
    }

    /// Wait for a slot under the page deadline, polling so an abort (Ctrl+C)
    /// interrupts a blocked fetch within ~25ms. On timeout/abort the slot is
    /// abandoned to its worker and counted as a zombie.
    fn awaitSlot(self: *HttpSource, slot: *Slot) !void {
        const deadline: i64 = if (self.opts.timeout_ms > 0)
            std.time.milliTimestamp() +| self.opts.timeout_ms
        else
            std.math.maxInt(i64);
        return self.awaitSlotUntil(slot, deadline);
    }

    fn awaitSlotUntil(self: *HttpSource, slot: *Slot, deadline: i64) !void {
        while (true) {
            if (slot.state.load(.acquire) == @intFromEnum(SlotState.done)) return;
            if (driver.aborting() or std.time.milliTimestamp() >= deadline) {
                if (slot.state.cmpxchgStrong(
                    @intFromEnum(SlotState.running),
                    @intFromEnum(SlotState.abandoned),
                    .acq_rel,
                    .acquire,
                ) == null) {
                    self.zombies += 1;
                    return if (driver.aborting()) error.Aborted else error.ConnectionTimedOut;
                }
                return; // raced: the worker just finished — take the result
            }
            std.Thread.sleep(25 * std.time.ns_per_ms);
        }
    }

    /// Discard in-flight slots whose results are no longer wanted (end of data,
    /// stop_short, teardown). One short SHARED grace window — they are usually
    /// milliseconds from done — then abandon; nothing here waits a full page
    /// deadline per slot.
    fn drainSlots(self: *HttpSource) void {
        const grace_ms: i64 = if (self.opts.timeout_ms > 0) @min(self.opts.timeout_ms, 5_000) else 5_000;
        const deadline = std.time.milliTimestamp() +| grace_ms;
        for (self.slots.items) |slot| {
            if (self.awaitSlotUntil(slot, deadline)) |_| freeSlot(slot) else |_| {}
        }
        self.slots.clearRetainingCapacity();
    }

    /// End of dataset reached: requests already in flight past the end are
    /// joined and discarded (same waste profile as any prefetch window).
    fn finishEmpty(self: *HttpSource) ?Batch {
        self.issue_done = true;
        self.drainSlots();
        self.done = true;
        return null;
    }

    fn nextPrefetched(self: *HttpSource, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        if (driver.aborting()) {
            self.drainSlots();
            return error.Aborted;
        }
        while (!self.issue_done and self.slots.items.len < @as(usize, @intCast(self.opts.prefetch)))
            try self.issueSlot();
        if (self.slots.items.len == 0) {
            self.done = true;
            return null;
        }
        const slot = self.slots.orderedRemove(0);
        // After an abandon CAS the worker owns (and may free) the slot — only a
        // pre-made copy of the URL is safe to reference in error reporting.
        const url_copy = arena.dupe(u8, slot.url) catch "";
        self.awaitSlot(slot) catch |e| {
            std.debug.print("page fetch abandoned ({s}): {s}\n", .{ @errorName(e), url_copy });
            return e; // slot now belongs to its worker — do not free or touch
        };
        defer freeSlot(slot);
        if (slot.err) |e| {
            std.debug.print("page fetch failed ({s}): {s}\n", .{ @errorName(e), slot.url });
            return mapTransport(e);
        }
        if (slot.code == 401) {
            // Stale token (gen behind) just re-fetches; a current-gen 401 means
            // the session expired — session auths re-login once.
            const a = self.auth orelse return self.listedFallback(401, error.HttpUnauthorized);
            if (slot.gen >= self.auth_gen) {
                if (!a.refresh(self.client)) return self.listedFallback(401, error.HttpUnauthorized);
                self.auth_gen += 1;
            }
            const body = self.fetchPageRaw(arena, .{ .url = slot.url, .body = slot.req_body }) catch |e|
                return mapTransport(e);
            return self.consumeBody(arena, body);
        }
        if (slot.code == 204) return self.finishEmpty();
        if (slot.code != 200) return self.raiseStatus(slot.code, slot.url, slot.body);
        // Parsed JSON slices into the source text, so the body must move into
        // the batch arena before the gpa copy is freed.
        return self.consumeBody(arena, try arena.dupe(u8, slot.body));
    }

    fn consumeBody(self: *HttpSource, arena: std.mem.Allocator, body: []const u8) !?Batch {
        const trimmed = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed.len == 0) return self.finishEmpty();
        const root = try json.parseFromSliceLeaky(json.Value, arena, trimmed, .{});
        if (root == .null) return self.finishEmpty();
        const items = try itemsOf(arena, root, self.opts.items);
        if (items.len == 0) return self.finishEmpty();
        self.noteProgress(items.len);
        const batch = try request.batchFromJson(arena, self.schema, items);
        if (self.opts.stop_short and items.len < self.opts.page_size) {
            // Short page = end of data: stop issuing and discard in-flight pages
            // (they'd come back empty), but still yield this final batch.
            self.issue_done = true;
            self.drainSlots();
            self.done = true;
        }
        return batch;
    }

    fn fetchPageRaw(self: *HttpSource, arena: std.mem.Allocator, req: PageReq) ![]const u8 {
        var hdrs = try arena.alloc(std.http.Header, self.headers.len + 1);
        @memcpy(hdrs[0..self.headers.len], self.headers);
        var nh = self.headers.len;
        if (self.auth) |a| {
            if (try a.ensure(self.client)) |h| {
                hdrs[nh] = h;
                nh += 1;
            }
        }
        var aw = std.Io.Writer.Allocating.init(arena);
        const res = try self.client.fetch(.{
            .method = if (self.opts.method == .post) .POST else .GET,
            .location = .{ .url = req.url },
            .extra_headers = hdrs[0..nh],
            .headers = if (self.contentType()) |ct| .{ .content_type = .{ .override = ct } } else .{},
            .payload = req.body,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        // 204 = no content: some OData servers (e.g. Ivanti) signal "past the
        // end of the dataset" this way instead of an empty `value` array.
        if (code == 204) return "";
        if (code != 200) return self.raiseStatus(code, req.url, aw.writer.buffered());
        return aw.writer.buffered();
    }

    /// Single status-disposition policy for every fetch path. 401 outranks the
    /// retry_statuses mapping: session re-login gets first claim, and only when
    /// auth declines (no auth kind / refresh failed) does listedFallback apply.
    fn raiseStatus(self: *HttpSource, code: u16, url: []const u8, body: []const u8) anyerror {
        std.debug.print("http {d} from {s}: {s}\n", .{ code, url, body[0..@min(body.len, 300)] });
        if (code == 401) return error.HttpUnauthorized;
        if (statusListed(self.opts.retry_statuses, code)) return error.HttpServerBusy;
        return statusError(code);
    }

    fn listedFallback(self: *HttpSource, code: u16, e: anyerror) anyerror {
        return if (statusListed(self.opts.retry_statuses, code)) error.HttpServerBusy else e;
    }

    pub fn next(self: *HttpSource, arena: std.mem.Allocator) !?Batch {
        if (self.first) |b| {
            self.first = null;
            return b;
        }
        if (self.prefetchOn()) return self.nextPrefetched(arena);
        if (self.done) return null;
        const req = try self.pageReq(arena);
        const page = try self.fetchParsed(arena, req);
        try self.advance(arena, page.root, page.items.len);
        if (page.items.len == 0) return null;
        self.noteProgress(page.items.len);
        return try request.batchFromJson(arena, self.schema, page.items);
    }

    pub fn close(self: *HttpSource) void {
        self.drainSlots();
        self.slots.deinit();
        if (self.zombies == 0) {
            self.client.deinit();
            self.gpa.destroy(self.client);
        } else {
            // Abandoned workers may still touch the client's pool; leaking it
            // is the only safe option (the zombies die with the process).
            std.debug.print("[http] {d} timed-out request(s) abandoned; http client leaked intentionally\n", .{self.zombies});
        }
    }

    pub fn source(self: *HttpSource) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

fn buildHeaders(arena: std.mem.Allocator, opts: Options) ![]const std.http.Header {
    var hdrs = std.array_list.Managed(std.http.Header).init(arena);
    if (opts.bearer orelse try envOpt(arena, opts.bearer_env)) |tok| {
        try hdrs.append(.{ .name = "Authorization", .value = try std.fmt.allocPrint(arena, "Bearer {s}", .{tok}) });
    } else if (opts.auth orelse try envOpt(arena, opts.auth_env)) |val| {
        try hdrs.append(.{ .name = "Authorization", .value = try arena.dupe(u8, val) });
    } else if (opts.user_env != null and opts.pass_env != null) {
        const user = (try envOpt(arena, opts.user_env)) orelse return error.MissingAuthEnv;
        const pass = (try envOpt(arena, opts.pass_env)) orelse return error.MissingAuthEnv;
        const raw = try std.fmt.allocPrint(arena, "{s}:{s}", .{ user, pass });
        const enc = std.base64.standard.Encoder;
        const b64 = try arena.alloc(u8, enc.calcSize(raw.len));
        _ = enc.encode(b64, raw);
        try hdrs.append(.{ .name = "Authorization", .value = try std.fmt.allocPrint(arena, "Basic {s}", .{b64}) });
    }
    if (opts.header) |h| {
        const colon = std.mem.indexOfScalar(u8, h, ':') orelse return error.BadHeaderHint;
        try hdrs.append(.{
            .name = std.mem.trim(u8, h[0..colon], " "),
            .value = std.mem.trim(u8, h[colon + 1 ..], " "),
        });
    }
    try hdrs.append(.{ .name = "Accept", .value = "application/json" });
    return hdrs.toOwnedSlice();
}

fn envOpt(arena: std.mem.Allocator, name_opt: ?[]const u8) !?[]const u8 {
    const name = name_opt orelse return null;
    return std.process.getEnvVarOwned(arena, name) catch return error.MissingAuthEnv;
}

/// The row array of a response: a bare array, the (dotted) `items` path into an
/// object, or — with no path — a single object treated as one row.
fn itemsOf(arena: std.mem.Allocator, root: json.Value, items_path: ?[]const u8) ![]const json.Value {
    if (items_path) |p| {
        const v = jsonPath(root, p) orelse return error.ItemsFieldMissing;
        return switch (v) {
            .array => |a| a.items,
            else => error.ItemsFieldNotArray,
        };
    }
    return switch (root) {
        .array => |a| a.items,
        .object => blk: {
            const one = try arena.alloc(json.Value, 1);
            one[0] = root;
            break :blk one;
        },
        else => error.ExpectedJsonArrayOrObject,
    };
}

fn jsonPath(root: json.Value, dotted: []const u8) ?json.Value {
    var cur = root;
    var it = std.mem.splitScalar(u8, dotted, '.');
    while (it.next()) |key| {
        switch (cur) {
            .object => |o| cur = o.get(key) orelse return null,
            else => return null,
        }
    }
    if (cur == .null) return null;
    return cur;
}

/// `base&key=val` (no leading & when base is empty) — form-body composition.
fn appendParam(arena: std.mem.Allocator, base: []const u8, key: []const u8, val: []const u8) ![]const u8 {
    const sep: []const u8 = if (base.len == 0) "" else "&";
    return std.fmt.allocPrint(arena, "{s}{s}{s}={s}", .{ base, sep, key, val });
}

fn withParam(arena: std.mem.Allocator, base: []const u8, key: []const u8, val: []const u8) ![]const u8 {
    const sep: u8 = if (std.mem.indexOfScalar(u8, base, '?') != null) '&' else '?';
    return std.fmt.allocPrint(arena, "{s}{c}{s}={s}", .{ base, sep, key, val });
}

/// Map a non-200 status to a named error so failures are diagnosable from the
/// error name alone. 429/5xx are worth a control-plane retry (exit 75); the
/// 4xx family is config/auth — distinct names because each means a different
/// fix (401: token expired mid-run -> re-auth + retry the step; 400: the
/// server rejected the request, e.g. deep $skip paging).
pub fn statusError(code: u16) anyerror {
    if (code == 429 or code >= 500) return error.HttpServerBusy;
    return switch (code) {
        400 => error.HttpBadRequest,
        401 => error.HttpUnauthorized,
        403 => error.HttpForbidden,
        404 => error.HttpNotFound,
        else => error.HttpRequestFailed,
    };
}

/// Is `code` in a comma-separated status list ("404,408")?
fn statusListed(list_opt: ?[]const u8, code: u16) bool {
    const list = list_opt orelse return false;
    var it = std.mem.splitScalar(u8, list, ',');
    while (it.next()) |part| {
        const t = std.mem.trim(u8, part, " ");
        const n = std.fmt.parseInt(u16, t, 10) catch continue;
        if (n == code) return true;
    }
    return false;
}

/// Worth an in-place retry: server overload/restart or a dropped connection.
/// 4xx (other than 429, mapped to HttpServerBusy) is config — retrying lies.
/// The socket set is the shared driver.transientNet, so the retry layers
/// (sql reconnect, http backoff) can never drift apart.
fn isRetryableNet(e: anyerror) bool {
    return e == error.HttpServerBusy or driver.transientNet(e);
}

/// Site-level transient classification: a socket failure that survived the
/// retry budget surfaces as HttpTransportFailed — a name the run-level
/// classifier can safely treat as transient. The ambient std.Io names
/// (WriteFailed/ReadFailed/EndOfStream) must NOT be transient globally:
/// a CSV sink's disk-full is also WriteFailed.
fn mapTransport(e: anyerror) anyerror {
    if (e == error.HttpServerBusy) return e;
    return if (driver.transientNet(e)) error.HttpTransportFailed else e;
}

/// base * 2^(attempt-1), with +-30% time-seeded jitter so concurrent workers
/// don't re-hammer in lockstep.
fn retryDelayNs(base_ms: i64, attempt: i64) u64 {
    const shift: u6 = @intCast(@min(@max(attempt - 1, 0), 6));
    const base: u64 = @intCast(@max(base_ms, 1));
    const d = base * (@as(u64, 1) << shift);
    var x: u64 = @bitCast(std.time.microTimestamp());
    x = x *% 6364136223846793005 +% 1442695040888963407;
    const j = 70 + (x >> 33) % 61; // 70..130 percent
    return d * j / 100 * std.time.ns_per_ms;
}

const source_vtable = driver.Source.VTable{ .schema = srcSchema, .next = srcNext, .close = srcClose };
fn srcSchema(ptr: *anyopaque) types.Schema {
    const self: *HttpSource = @ptrCast(@alignCast(ptr));
    return self.schema.*;
}
fn srcNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *HttpSource = @ptrCast(@alignCast(ptr));
    return self.next(arena);
}
fn srcClose(ptr: *anyopaque) void {
    const self: *HttpSource = @ptrCast(@alignCast(ptr));
    self.close();
}

// --- tests ----------------------------------------------------------------

test "optsFromHints maps hint keys" {
    const hints = [_]ast.Hint{
        .{ .key = "bearer", .value = .{ .str = "tok" }, .pos = .{ .line = 1, .col = 1 } },
        .{ .key = "items", .value = .{ .str = "data" }, .pos = .{ .line = 1, .col = 1 } },
        .{ .key = "paginate", .value = .{ .ident = "cursor" }, .pos = .{ .line = 1, .col = 1 } },
        .{ .key = "cursor_field", .value = .{ .str = "meta.next" }, .pos = .{ .line = 1, .col = 1 } },
        .{ .key = "max_pages", .value = .{ .int = 5 }, .pos = .{ .line = 1, .col = 1 } },
    };
    const o = optsFromHints(&hints);
    try std.testing.expectEqualStrings("tok", o.bearer.?);
    try std.testing.expectEqualStrings("data", o.items.?);
    try std.testing.expectEqual(Mode.cursor, o.paginate);
    try std.testing.expectEqualStrings("meta.next", o.cursor_field);
    try std.testing.expectEqual(@as(i64, 5), o.max_pages);
}

/// Serve `responses` in order, one connection each; captures each request head.
const TestServer = struct {
    listener: std.net.Server,
    responses: []const []const u8,
    statuses: ?[]const []const u8 = null, // per-response status lines; null = all "200 OK"
    route_div: ?i64 = null, // route by $skip/div (prefetch arrives out of order)
    expected: ?usize = null, // connections to serve; default responses.len
    captured: [4][2048]u8 = undefined,
    captured_len: [4]usize = .{ 0, 0, 0, 0 },

    fn start(responses: []const []const u8) !*TestServer {
        const self = try std.testing.allocator.create(TestServer);
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        self.* = .{ .listener = try addr.listen(.{ .reuse_address = true }), .responses = responses };
        return self;
    }
    fn port(self: *TestServer) u16 {
        return self.listener.listen_address.getPort();
    }
    fn run(self: *TestServer) void {
        self.serve() catch {};
    }
    fn serve(self: *TestServer) !void {
        const total = self.expected orelse self.responses.len;
        var i: usize = 0;
        while (i < total) : (i += 1) {
            const conn = try self.listener.accept();
            defer conn.stream.close();
            var req_buf: [2048]u8 = undefined;
            const req_len = try conn.stream.read(&req_buf);
            if (i < self.captured.len) {
                @memcpy(self.captured[i][0..req_len], req_buf[0..req_len]);
                self.captured_len[i] = req_len;
            }
            var body: []const u8 = undefined;
            var status: []const u8 = "200 OK";
            if (self.route_div) |div| {
                // prefetch issues pages concurrently; route by the $skip value
                var skip: i64 = 0;
                if (std.mem.indexOf(u8, req_buf[0..req_len], "$skip=")) |at| {
                    var j = at + "$skip=".len;
                    while (j < req_len and req_buf[j] >= '0' and req_buf[j] <= '9') : (j += 1)
                        skip = skip * 10 + (req_buf[j] - '0');
                }
                const idx: usize = @intCast(@divTrunc(skip, div));
                body = if (idx < self.responses.len) self.responses[idx] else "[]";
            } else {
                body = self.responses[i];
                if (self.statuses) |st| status = st[i];
            }
            var wb: [256]u8 = undefined;
            const head = try std.fmt.bufPrint(&wb, "HTTP/1.1 {s}\r\ncontent-type: application/json\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n", .{ status, body.len });
            try conn.stream.writeAll(head);
            try conn.stream.writeAll(body);
        }
    }
    fn deinit(self: *TestServer) void {
        self.listener.deinit();
        std.testing.allocator.destroy(self);
    }
};

fn drain(src: *HttpSource, arena: std.mem.Allocator) !usize {
    var total: usize = 0;
    while (try src.next(arena)) |b| total += b.len;
    return total;
}

test "http source: bare array, single fetch, bearer header sent" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .bearer = "sek" });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), s.schema.fields.len);
    try std.testing.expectEqual(types.TypeKind.int, s.schema.fields[0].ty.kind);
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[0][0..srv.captured_len[0]], "Authorization: Bearer sek") != null);
}

test "http source: page pagination stops on empty page" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"data":[{"id":1},{"id":2}]}
        ,
        \\{"data":[{"id":3}]}
        ,
        \\{"data":[]}
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .items = "data", .paginate = .page });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
    // page numbers advanced in the query string
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[0][0..srv.captured_len[0]], "GET /items?page=1") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[1][0..srv.captured_len[1]], "GET /items?page=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[2][0..srv.captured_len[2]], "GET /items?page=3") != null);
}

test "http source: offset pagination advances $skip by page_size, raw auth sent" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"value":[{"RecId":"a"},{"RecId":"b"}]}
        ,
        \\{"value":[{"RecId":"c"}]}
        ,
        \\{"value":[]}
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/api/odata/businessobject/incidents", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .auth = "session-tok-xyz",
        .items = "value",
        .paginate = .offset,
        .page_param = "$skip",
        .size_param = "$top",
        .page_size = 2,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[0][0..srv.captured_len[0]], "$skip=0") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[0][0..srv.captured_len[0]], "$top=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[1][0..srv.captured_len[1]], "$skip=2") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[2][0..srv.captured_len[2]], "$skip=4") != null);
    // raw Authorization value, no "Bearer " prefix
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[0][0..srv.captured_len[0]], "Authorization: session-tok-xyz") != null);
}

test "http source: 204 past the end of the dataset ends the stream cleanly" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"value":[{"RecId":"a"},{"RecId":"b"}]}
        ,
        "",
    });
    srv.statuses = &.{ "200 OK", "204 No Content" };
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .items = "value",
        .paginate = .offset,
        .page_param = "$skip",
        .size_param = "$top",
        .page_size = 100,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));
}

test "connFromKvs maps attrs, collects body_*, rejects unknown keys" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var errmsg: []const u8 = "";

    const cc = try connFromKvs(a, &.{
        .{ .key = "base_url", .value = "https://x" },
        .{ .key = "auth", .value = "login_json" },
        .{ .key = "login_path", .value = "/login" },
        .{ .key = "body_tenant", .value = "t" },
        .{ .key = "body_username", .value = "u" },
    }, &errmsg);
    try std.testing.expectEqual(AuthKind.login_json, cc.auth);
    try std.testing.expectEqualStrings("/login", cc.login_url);
    try std.testing.expectEqual(@as(usize, 2), cc.body.len);
    try std.testing.expectEqualStrings("tenant", cc.body[0].key);

    try std.testing.expectError(error.BadHttpConn, connFromKvs(a, &.{
        .{ .key = "base_url", .value = "https://x" },
        .{ .key = "baseurl_typo", .value = "y" },
    }, &errmsg));
    try std.testing.expect(std.mem.indexOf(u8, errmsg, "baseurl_typo") != null);
}

test "http connection: login_json posts body, token rides later requests" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        "\"tok-abc\"",
        \\[{"id":1},{"id":2}]
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{srv.port()});
    const s = try HttpSource.openConn(a, std.testing.allocator, .{
        .base_url = base,
        .auth = .login_json,
        .login_url = "/api/login",
        .body = &.{ .{ .key = "tenant", .value = "t1" }, .{ .key = "username", .value = "u1" } },
    }, "/items", .{});
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));

    const login_req = srv.captured[0][0..srv.captured_len[0]];
    try std.testing.expect(std.mem.indexOf(u8, login_req, "POST /api/login") != null);
    try std.testing.expect(std.mem.indexOf(u8, login_req, "\"tenant\":\"t1\"") != null);
    const data_req = srv.captured[1][0..srv.captured_len[1]];
    try std.testing.expect(std.mem.indexOf(u8, data_req, "GET /items") != null);
    try std.testing.expect(std.mem.indexOf(u8, data_req, "Authorization: tok-abc") != null);
}

test "http connection: 401 mid-run triggers re-login and the page retries" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        "\"tok-A\"",
        "expired",
        "\"tok-B\"",
        \\[{"id":7}]
    });
    srv.statuses = &.{ "200 OK", "401 Unauthorized", "200 OK", "200 OK" };
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{srv.port()});
    const s = try HttpSource.openConn(a, std.testing.allocator, .{
        .base_url = base,
        .auth = .login_json,
        .login_url = "/login",
    }, "/items", .{});
    defer s.close();
    try std.testing.expectEqual(@as(usize, 1), try drain(s, a));
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[3][0..srv.captured_len[3]], "Authorization: tok-B") != null);
}

test "http connection: oauth2 client credentials form post -> Bearer" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"access_token":"xyz","expires_in":3600}
        ,
        \\[{"id":1}]
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{srv.port()});
    const s = try HttpSource.openConn(a, std.testing.allocator, .{
        .base_url = base,
        .auth = .oauth2,
        .login_url = "/oauth/token",
        .user = "cid",
        .password = "sec ret",
    }, "/items", .{});
    defer s.close();
    try std.testing.expectEqual(@as(usize, 1), try drain(s, a));

    const tok_req = srv.captured[0][0..srv.captured_len[0]];
    try std.testing.expect(std.mem.indexOf(u8, tok_req, "grant_type=client_credentials") != null);
    try std.testing.expect(std.mem.indexOf(u8, tok_req, "client_id=cid") != null);
    try std.testing.expect(std.mem.indexOf(u8, tok_req, "client_secret=sec%20ret") != null);
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[1][0..srv.captured_len[1]], "Authorization: Bearer xyz") != null);
}

test "http source: prefetch fetches pages concurrently and stops on empty" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\[{"id":1},{"id":2}]
        ,
        \\[{"id":3}]
        ,
        "[]",
    });
    srv.route_div = 2;
    srv.expected = 5; // open(skip0) + slots skip2,4,6 + top-up skip8
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .paginate = .offset,
        .page_param = "$skip",
        .size_param = "$top",
        .page_size = 2,
        .prefetch = 3,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
    var seen2 = false;
    var seen4 = false;
    for (0..4) |i| {
        const req = srv.captured[i][0..srv.captured_len[i]];
        if (std.mem.indexOf(u8, req, "$skip=2&") != null) seen2 = true;
        if (std.mem.indexOf(u8, req, "$skip=4&") != null) seen4 = true;
    }
    try std.testing.expect(seen2 and seen4);
}

test "http source: stop_short ends after a short page (no trailing empty fetch)" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // Only two responses served; without stop_short the source would issue a
    // third request and this test would hang on the closed listener.
    const srv = try TestServer.start(&.{
        \\[{"id":1},{"id":2}]
        ,
        \\[{"id":3}]
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .paginate = .offset,
        .page_param = "$skip",
        .size_param = "$top",
        .page_size = 2,
        .stop_short = true,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
}

test "abort flag stops pagination between page requests" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\[{"id":1}]
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .paginate = .page });
    defer s.close();
    _ = (try s.next(a)).?; // first page, fetched at open

    driver.requestAbort();
    defer driver.resetAbort();
    // would otherwise issue the page-2 request; the abort check fires first
    try std.testing.expectError(error.Aborted, s.next(a));
}

test "http source: POST form body carries pagination, content-type set" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"totalCount":2,"itens":[{"id":1}]}
        ,
        \\{"totalCount":2,"itens":[{"id":2}]}
        ,
        \\{"totalCount":2,"itens":[]}
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/rest", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .method = .post,
        .body = "user=u&token=t&pag=ponto&cmd=get",
        .items = "itens",
        .paginate = .page,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));

    const r1 = srv.captured[0][0..srv.captured_len[0]];
    try std.testing.expect(std.mem.indexOf(u8, r1, "POST /rest") != null);
    try std.testing.expect(std.mem.indexOf(u8, r1, "content-type: application/x-www-form-urlencoded") != null);
    try std.testing.expect(std.mem.indexOf(u8, r1, "user=u&token=t&pag=ponto&cmd=get&page=1") != null);
    const r2 = srv.captured[1][0..srv.captured_len[1]];
    try std.testing.expect(std.mem.indexOf(u8, r2, "&page=2") != null);
    // URL stays clean: pagination went to the body, not the query string
    try std.testing.expect(std.mem.indexOf(u8, r1, "/rest?") == null);
}

test "http source: total_field bounds the page count exactly" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // Only 2 responses served; the source must not request page 3 even though
    // page 2 is non-empty (this API style never returns an empty page).
    const srv = try TestServer.start(&.{
        \\{"totalCount":2,"itens":[{"id":1},{"id":2}]}
        ,
        \\{"totalCount":2,"itens":[{"id":3}]}
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/rest", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .method = .post,
        .body = "cmd=get",
        .items = "itens",
        .paginate = .page,
        .total_field = "totalCount",
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
}

test "http source: transient 503 retries in place and succeeds" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        "overloaded",
        \\[{"id":1},{"id":2}]
    });
    srv.statuses = &.{ "503 Service Unavailable", "200 OK" };
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .retries = 2, .retry_base_ms = 10 });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));
}

test "retry_statuses treats a lying 404 as transient and retries through it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        "{\"success\":false,\"error_html\":\"Erro ao conectar no banco de dados.\"}",
        \\[{"id":1}]
    });
    srv.statuses = &.{ "404 Not Found", "200 OK" };
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/rest", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .retries = 2,
        .retry_base_ms = 10,
        .retry_statuses = "404",
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 1), try drain(s, a));
}

test "statusListed parses comma lists with spaces" {
    try std.testing.expect(statusListed("404,408, 410", 404));
    try std.testing.expect(statusListed("404,408, 410", 408));
    try std.testing.expect(statusListed("404,408, 410", 410));
    try std.testing.expect(!statusListed("404,408, 410", 500));
    try std.testing.expect(!statusListed(null, 404));
    try std.testing.expect(!statusListed("garbage,abc", 404));
}

fn serveOneThenHang(listener: *std.net.Server) void {
    // page 1: normal response; page 2: accept, read, never answer (black hole)
    const body = "[{\"id\":1}]";
    {
        const conn = listener.accept() catch return;
        defer conn.stream.close();
        var rb: [2048]u8 = undefined;
        _ = conn.stream.read(&rb) catch return;
        var wb: [256]u8 = undefined;
        const head = std.fmt.bufPrint(&wb, "HTTP/1.1 200 OK\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n{s}", .{ body.len, body }) catch return;
        conn.stream.writeAll(head) catch return;
    }
    const conn = listener.accept() catch return;
    var rb: [2048]u8 = undefined;
    _ = conn.stream.read(&rb) catch return;
    std.Thread.sleep(10 * std.time.ns_per_s); // hold it open, silent
    conn.stream.close();
}

test "a black-holed page request times out instead of hanging the run" {
    var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var listener = try addr.listen(.{ .reuse_address = true });
    defer listener.deinit();
    const th = try std.Thread.spawn(.{}, serveOneThenHang, .{&listener});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{listener.listen_address.getPort()});
    // page_allocator (not testing.allocator): abandoning the hung worker leaks
    // its slot + client BY DESIGN; the leak checker would call that a failure.
    const s = try HttpSource.open(a, std.heap.page_allocator, url, .{
        .paginate = .page,
        .prefetch = 2,
        .retries = 0,
        .timeout_ms = 400,
    });
    defer s.close();
    _ = (try s.next(a)).?; // page 1, fetched at open
    const started = std.time.milliTimestamp();
    try std.testing.expectError(error.ConnectionTimedOut, s.next(a));
    const waited = std.time.milliTimestamp() - started;
    try std.testing.expect(waited >= 350 and waited < 5_000);
}

test "joinUrl and encodeSpaces" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("https://x/api/v1", try joinUrl(a, "https://x/", "/api/v1"));
    try std.testing.expectEqualStrings("https://x/api", try joinUrl(a, "https://x", "api"));
    try std.testing.expectEqualStrings("https://y/z", try joinUrl(a, "https://x", "https://y/z"));
    try std.testing.expectEqualStrings(
        "https://x/o?$filter=a%20eq%20'b'",
        try encodeSpaces(a, "https://x/o?$filter=a eq 'b'"),
    );
}

test "http source: cursor pagination follows token then stops" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"next":"t1","data":[{"id":1}]}
        ,
        \\{"next":null,"data":[{"id":2}]}
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .items = "data", .paginate = .cursor });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[1][0..srv.captured_len[1]], "GET /items?cursor=t1") != null);
}

test "http source: non-200 maps to permanent/transient errors" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var listener_addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var listener = try listener_addr.listen(.{ .reuse_address = true });
    defer listener.deinit();
    const th = try std.Thread.spawn(.{}, serve404Once, .{&listener});
    defer th.join();
    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/x", .{listener.listen_address.getPort()});
    try std.testing.expectError(error.HttpNotFound, HttpSource.open(a, std.testing.allocator, url, .{}));
}

fn serve404Once(listener: *std.net.Server) void {
    const conn = listener.accept() catch return;
    defer conn.stream.close();
    var rb: [2048]u8 = undefined;
    _ = conn.stream.read(&rb) catch return;
    conn.stream.writeAll("HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n") catch return;
}
