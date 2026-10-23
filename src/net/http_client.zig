//! The REST/JSON source. GETs a URL and yields one batch per page of JSON
//! objects. Auth and pagination ride on the read's hints — its `PAGINATE`,
//! `RETRY` and `WITH (...)` clauses — whose keys mirror the `Options` field names
//! (a key no REST read takes is refused at plan time):
//!
//!   SELECT * FROM HTTP('https://api.x/items')
//!     PAGINATE BY cursor (field = 'next')
//!     WITH (bearer_env = 'API_TOKEN', items = 'data');
//!
//! Pagination modes:
//!   (none)             one GET, one batch.
//!   paginate = page    `?<page_param>=N` (N starts at `start_page`), optionally
//!                      `&<size_param>=<page_size>`; stops on an empty page, or
//!                      after exactly `total_field` pages for APIs that never
//!                      return an empty one.
//!   paginate = offset  like `page`, but the param advances by `page_size` from
//!                      `start_offset` (OData `$skip`/`$top` style).
//!   paginate = cursor  reads `<cursor_field>` (dotted path) from each response;
//!                      a value starting with http(s):// is the next URL, anything
//!                      else is sent URL-encoded as `?<cursor_param>=<value>`; stops when the
//!                      field is missing/null/empty or the page is empty.
//! `max_pages` caps every mode so a misbehaving API can't loop forever. With
//! `body` (POST) the page param goes into the body, as form-style APIs expect.
//! `stop_short` ends on a page shorter than page_size; it is opt-in because a
//! server that caps page size below the requested one makes the short page lie.
//! An empty body or bare JSON `null` is an empty page.
//!
//! Auth: `bearer_env`/`auth_env`/`user_env`+`pass_env` name environment variables
//! (keeps tokens out of script text); `bearer` prepends "Bearer ", `auth`/`auth_env`
//! send the Authorization value verbatim (session-token APIs). `bearer`/`auth`
//! take literals (useful with for-loop `${var}` interpolation), and
//! `header = "Name: value"` adds one extra raw header; a User-Agent given that way
//! replaces the client's (`basalt/<version>`) instead of being sent twice, which
//! some WAFs reject.
//!
//! Connection-level form: `CREATE CONNECTION itsm TYPE http OPTIONS (base_url =
//! ...)` and `FROM itsm.GET('/path', k = v)`. The path resolves against the
//! connection's `base_url` (spaces auto-encoded, so OData filters read naturally)
//! and auth lives on the connection; unknown connection keys are an error
//! too:
//!   auth = 'bearer'      token = env('TOK')                  -> Bearer <token>
//!   auth = 'basic'       user = ..., password = ...          -> Basic <b64>
//!   auth = 'header'      header_name/header_value            -> any API-key header
//!   auth = 'login_json'  login_url + body_* attrs            -> POST a JSON object
//!                        built from every `body_<field>` attr; the response token
//!                        (`token_path`, default the whole response string) is sent
//!                        as `token_header` (default Authorization) with
//!                        `token_prefix` (default none).
//!   auth = 'oauth2'      login_url (`token_url` also accepted) + client_id/
//!                        client_secret [+ scope] -> client-credentials form POST;
//!                        token_path defaults to access_token, prefix to "Bearer ".
//! Session kinds (login_json, oauth2) re-login and retry the page once on a 401,
//! so server-side session expiry mid-pull heals itself. A 401 goes to re-login
//! before `retry_statuses` is consulted.
//!
//! Resilience: transient failures (429/5xx, connection resets) retry in place
//! `retries` times through `objstore.retry`, backing off from `retry_base_ms` with
//! jitter, stretched to a `Retry-After` of up to a minute, so one blip deep in a long
//! backfill does not restart the run. `retry_statuses` ("404,408") marks extra
//! codes transient for APIs that misreport an outage; if they outlive the budget
//! the run exits 75 so the control plane re-runs it. A socket failure that
//! survives the budget surfaces as HttpTransportFailed, never as a std.Io
//! WriteFailed/ReadFailed, which must not count as transient globally (a CSV
//! sink's disk-full is WriteFailed too). `progress_ms` emits a heartbeat.
//!
//! Deadlines and prefetch: `timeout_ms` bounds every page fetch, its retries
//! included, sequential, prefetched or replayed after a re-login, by running it on a
//! detached worker in a `Slot`; a black-holed
//! request surfaces as ConnectionTimedOut. The worker cannot be interrupted, so it
//! is abandoned (bounded and counted) and frees itself if the server ever
//! answers. That is why a slot reads only its own arena snapshot and the
//! gpa-allocated client (never the run arena or the HttpSource), and a state CAS
//! decides whether consumer or worker frees it. `prefetch` (page/offset modes)
//! keeps N such slots in flight, which helps slow APIs that compute pages in
//! parallel and hurts servers that serialise requests per session.
//!
//! TLS: std's verifier walks the server's chain strictly in presentation order,
//! so `repairBundle` re-anchors a misordered chain; BASALT_CA_BUNDLE (a PEM file)
//! is the manual override for chains it cannot fix, such as a missing
//! intermediate. Responses decompress straight into the caller's allocating
//! writer (`decompress_direct`): through std's fixed window buffer the flate
//! decoder could emit up to 258 bytes past the limit and abort the process.
//!
//! Pages are read as rows by `format/json/read.zig`, as a JSON file is: the schema from
//! the first page's rows, later pages coerced to it, missing keys null and new keys
//! dropped. Each page is fetched whole into the per-batch arena, so memory stays
//! flat across pages.

const std = @import("std");
const objstore = @import("../store/objstore.zig");
const types = @import("../lang/types.zig");
const ast = @import("../lang/ast.zig");
const Batch = @import("../exec/batch.zig").Batch;
const driver = @import("../connect/driver.zig");
const fjson = @import("../format/json/read.zig");
const jt = @import("../exec/json.zig");
const eval = @import("../exec/eval.zig");
const ChainCapture = @import("tls_client.zig").ChainCapture;
const init = @import("tls_client.zig").init;
const obs = @import("../runtime/obs.zig");
const percent = @import("percent.zig");
const http_fake = @import("../testing/http_fake.zig");

/// The most one response body may decode to, a page or a login reply.
const max_page_bytes = 256 << 20;

/// The longest a server's `Retry-After` stretches one wait between attempts.
const max_retry_after_ms = 60_000;

/// Adds to `bundle` each certificate the server presents that verifies against one
/// already there, repeating to a fixpoint so order doesn't matter. Trusts nothing
/// new that does not chain to an existing root; true if the bundle grew.
pub fn repairBundle(gpa: std.mem.Allocator, bundle: *std.crypto.Certificate.Bundle, host: []const u8, port: u16) bool {
    var cap = ChainCapture{};
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

/// Captures the chain a server presents over an unverified handshake. SNI and the
/// hostname are still sent and checked; only the CA path is not verified.
fn harvestChain(gpa: std.mem.Allocator, host: []const u8, port: u16, cap: *ChainCapture) !void {
    const stream = try std.net.tcpConnectToHost(gpa, host, port);
    defer stream.close();
    var in_buf: [std.crypto.tls.max_ciphertext_record_len]u8 = undefined;
    var out_buf: [std.crypto.tls.max_ciphertext_record_len]u8 = undefined;
    var sr = stream.reader(&in_buf);
    var sw = stream.writer(&out_buf);
    var wbuf: [std.crypto.tls.max_ciphertext_record_len]u8 = undefined;
    var rbuf: [std.crypto.tls.max_ciphertext_record_len * 2]u8 = undefined;
    _ = try init(sr.interface(), &sw.interface, .{
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

pub const ConnConfig = struct {
    base_url: []const u8 = "",
    auth: AuthKind = .none,
    token: []const u8 = "",
    user: []const u8 = "",
    password: []const u8 = "",
    header_name: []const u8 = "",
    header_value: []const u8 = "",
    login_url: []const u8 = "",
    scope: []const u8 = "",
    body: []const KV = &.{},
    token_path: []const u8 = "",
    token_header: []const u8 = "Authorization",
    token_prefix: ?[]const u8 = null,
};

/// Builds a ConnConfig from resolved connection attrs. `body_<field>` attrs pass
/// into the login JSON object; an unknown key is an error that `errmsg` names.
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
    if (cc.auth == .bearer and cc.token.len == 0) {
        errmsg.* = "auth 'bearer' needs a `token`";
        return error.BadHttpConn;
    }
    return cc;
}

pub const AuthState = struct {
    arena: std.mem.Allocator,
    cc: ConnConfig,
    header: ?std.http.Header = null,
    logger: ?*obs.Logger = null,

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

        const res = try objstore.exchange(arena, client, .{
            .method = .POST,
            .url = self.cc.login_url,
            .payload = payload,
            .content_type = content_type,
            .max_body = max_page_bytes,
        });
        const code = res.code;
        if (code != 200) {
            const b = res.body;
            var ub: [512]u8 = undefined;
            obs.logOr(self.logger, .err, "login http {d} from {s}: {f}", .{ code, redactUrl(&ub, self.cc.login_url), std.zig.fmtString(b[0..@min(b.len, 300)]) });
            return statusError(code);
        }
        const body = std.mem.trim(u8, res.body, " \t\r\n");
        if (body.len == 0) return error.LoginTokenMissing;
        try jt.validate(arena, body);
        const path = if (self.cc.token_path.len > 0)
            self.cc.token_path
        else if (is_oauth) "access_token" else ".";
        const raw = if (std.mem.eql(u8, path, ".")) body else ((try fjson.field(arena, body, path)) orelse return error.LoginTokenMissing);
        if (raw[0] != '"') return error.LoginTokenMissing;
        const tok = try jt.decodeString(arena, raw[1 .. raw.len - 1]);
        if (!lineSafe(tok)) return eval.explain(error.BadHeaderValue, "the login response's token holds a line break or NUL, which would inject into every request's headers");
        const prefix = self.cc.token_prefix orelse (if (is_oauth) "Bearer " else "");
        self.header = .{
            .name = self.cc.token_header,
            .value = try std.fmt.allocPrint(arena, "{s}{s}", .{ prefix, tok }),
        };
    }
};

/// `url` as logs and messages show it: without a `user:pass@` and with each query
/// value replaced by `...`, as either may hold a credential.
pub fn redactUrl(buf: []u8, url: []const u8) []const u8 {
    var w = std.Io.Writer.fixed(buf);
    var rest = url;
    if (std.mem.indexOf(u8, rest, "://")) |at| {
        w.writeAll(rest[0 .. at + 3]) catch return "(url)";
        rest = rest[at + 3 ..];
        const host_end = std.mem.indexOfAny(u8, rest, "/?#") orelse rest.len;
        if (std.mem.lastIndexOfScalar(u8, rest[0..host_end], '@')) |amp| rest = rest[amp + 1 ..];
    }
    const q = std.mem.indexOfScalar(u8, rest, '?') orelse {
        w.writeAll(rest) catch return "(url)";
        return w.buffered();
    };
    w.writeAll(rest[0 .. q + 1]) catch return "(url)";
    var it = std.mem.splitScalar(u8, rest[q + 1 ..], '&');
    var first = true;
    while (it.next()) |kv| {
        if (!first) w.writeByte('&') catch return "(url)";
        first = false;
        const eq = std.mem.indexOfScalar(u8, kv, '=') orelse kv.len;
        w.writeAll(kv[0..eq]) catch return "(url)";
        if (eq < kv.len) w.writeAll("=...") catch return "(url)";
    }
    return w.buffered();
}

/// Whether a server-sent value can go into a header or request line as is: a CR,
/// LF or NUL in it would end the line and start one the server chose.
fn lineSafe(s: []const u8) bool {
    return std.mem.indexOfAny(u8, s, "\r\n\x00") == null;
}

pub fn appendForm(buf: *std.array_list.Managed(u8), key: []const u8, val: []const u8) !void {
    try buf.append('&');
    try buf.appendSlice(key);
    try buf.append('=');
    try formEncode(buf, val);
}

pub const formEncode = percent.encode;

pub fn joinUrl(arena: std.mem.Allocator, base: []const u8, path: []const u8) ![]const u8 {
    if (path.len == 0) return base;
    if (std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://")) return path;
    const b = std.mem.trimRight(u8, base, "/");
    const sep: []const u8 = if (path.len > 0 and path[0] == '/') "" else "/";
    return std.fmt.allocPrint(arena, "{s}{s}{s}", .{ b, sep, path });
}

/// `path` with each param appended as `key=value`, both percent-encoded, after a
/// `?` or, when the path already carries a query string, an `&`.
pub fn withQuery(arena: std.mem.Allocator, path: []const u8, params: []const KV) ![]const u8 {
    if (params.len == 0) return path;
    var buf = std.array_list.Managed(u8).init(arena);
    try buf.appendSlice(path);
    var sep: u8 = if (std.mem.indexOfScalar(u8, path, '?') != null) '&' else '?';
    for (params) |p| {
        try buf.append(sep);
        try formEncode(&buf, p.key);
        try buf.append('=');
        try formEncode(&buf, p.value);
        sep = '&';
    }
    return buf.toOwnedSlice();
}

/// Encodes the one character that breaks URI parsing but appears constantly in
/// hand-written OData filters: the space. Everything else is the author's job.
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

pub const decompress_direct: []u8 = &.{};

pub fn initClient(gpa: std.mem.Allocator) std.http.Client {
    var client = std.http.Client{ .allocator = gpa };
    const path = std.process.getEnvVarOwned(gpa, "BASALT_CA_BUNDLE") catch return client;
    defer gpa.free(path);
    client.ca_bundle.rescan(gpa) catch return client;
    client.ca_bundle.addCertsFromFilePath(gpa, std.fs.cwd(), path) catch return client;
    client.next_https_rescan_certs = false;
    return client;
}

pub const Mode = enum { none, page, offset, cursor };

pub const Options = struct {
    bearer: ?[]const u8 = null,
    bearer_env: ?[]const u8 = null,
    auth: ?[]const u8 = null,
    auth_env: ?[]const u8 = null,
    user_env: ?[]const u8 = null,
    pass_env: ?[]const u8 = null,
    header: ?[]const u8 = null,
    items: ?[]const u8 = null,
    method: enum { get, post } = .get,
    body: ?[]const u8 = null,
    body_type: []const u8 = "form",
    paginate: Mode = .none,
    page_param: []const u8 = "page",
    start_page: i64 = 1,
    start_offset: i64 = 0,
    size_param: ?[]const u8 = null,
    page_size: i64 = 100,
    cursor_param: []const u8 = "cursor",
    cursor_field: []const u8 = "next",
    max_pages: i64 = 10_000,
    prefetch: i64 = 1,
    retries: i64 = 2,
    retry_base_ms: i64 = 500,
    timeout_ms: i64 = 300_000,
    progress_ms: i64 = 30_000,
    retry_statuses: ?[]const u8 = null,
    logger: ?*obs.Logger = null,
    total_field: ?[]const u8 = null,
    stop_short: bool = false,
};

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
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    client: *std.http.Client,
    zombies: usize = 0,
    base_url: []const u8,
    opts: Options,
    headers: []const std.http.Header,
    schema: *types.Schema,
    first: ?Batch,
    page_no: i64,
    next_url: ?[]const u8 = null,
    pages_fetched: i64 = 0,
    done: bool = false,
    repaired: bool = false,
    auth: ?*AuthState = null,
    slots: std.array_list.Managed(*Slot) = undefined,
    pages_issued: i64 = 0,
    issue_done: bool = false,
    auth_gen: u32 = 0,
    total_pages: ?i64 = null,
    logger: ?*obs.Logger = null,
    rows_done: u64 = 0,
    pages_done: i64 = 0,
    last_progress_ms: i64 = 0,

    const SlotState = enum(u8) { running, done, abandoned };

    const Slot = struct {
        state: std.atomic.Value(u8) = std.atomic.Value(u8).init(@intFromEnum(SlotState.running)),
        gpa: std.mem.Allocator,
        snap: std.heap.ArenaAllocator,
        client: *std.http.Client,
        method_post: bool,
        content_type: ?[]const u8,
        user_agent: []const u8,
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

    pub fn openConn(arena: std.mem.Allocator, gpa: std.mem.Allocator, cc: ConnConfig, path: []const u8, opts: Options) !*HttpSource {
        if (std.mem.indexOfAny(u8, path, "\r\n") != null) return error.UrlContainsLineBreak;
        const url = try encodeSpaces(arena, try joinUrl(arena, cc.base_url, path));
        const headers = try buildHeaders(arena, opts);
        const self = try arena.create(HttpSource);
        const client = try gpa.create(std.http.Client);
        client.* = initClient(gpa);
        self.* = .{
            .arena = arena,
            .gpa = gpa,
            .client = client,
            .base_url = url,
            .auth = null,
            .opts = opts,
            .headers = headers,
            .schema = undefined,
            .first = null,
            .page_no = if (opts.paginate == .offset) opts.start_offset else opts.start_page,
        };
        self.logger = opts.logger;
        self.slots = std.array_list.Managed(*Slot).init(gpa);
        errdefer if (self.zombies == 0) {
            client.deinit();
            gpa.destroy(client);
        };
        if (cc.auth != .none) {
            var rcc = cc;
            rcc.login_url = try joinUrl(arena, cc.base_url, cc.login_url);
            const a = try arena.create(AuthState);
            a.* = .{ .arena = arena, .cc = rcc, .logger = opts.logger };
            self.auth = a;
        }

        const first_req = try self.pageReq(arena);
        const page = try self.fetchParsed(arena, first_req);
        const items = page.items;
        const schema = try arena.create(types.Schema);
        schema.* = try fjson.schemaOf(arena, items, (fjson.Options{}).sample);
        self.schema = schema;
        self.first = try fjson.batchOf(arena, self.schema, items);
        self.noteProgress(items.len);
        if (opts.total_field) |tf| {
            if (try fjson.field(arena, page.root, tf)) |tv| {
                if (jt.number(tv) == .int) self.total_pages = jt.number(tv).int;
            }
        }
        self.advance(arena, page.root, items.len) catch |e| return e;
        return self;
    }

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
            obs.logOr(self.logger, .info, "http: pages {d}/{d}, rows {d}", .{ self.pages_done, t, self.rows_done });
        } else {
            obs.logOr(self.logger, .info, "http: pages {d}, rows {d}", .{ self.pages_done, self.rows_done });
        }
    }

    const Page = struct { root: []const u8, items: []const []const u8 };

    fn fetchParsed(self: *HttpSource, arena: std.mem.Allocator, req: PageReq) !Page {
        const body = try self.fetchPage(arena, req);
        const trimmed = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "null")) return .{ .root = "null", .items = &.{} };
        try jt.validate(arena, trimmed);
        return .{ .root = trimmed, .items = try itemsOf(arena, trimmed, self.opts.items) };
    }

    fn advance(self: *HttpSource, arena: std.mem.Allocator, root: []const u8, n_items: usize) !void {
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
                const cur = (if (std.mem.eql(u8, root, "null")) null else try fjson.field(arena, root, self.opts.cursor_field)) orelse {
                    self.done = true;
                    return;
                };
                if (cur[0] != '"') {
                    self.done = true;
                    return;
                }
                const tok = try jt.decodeString(arena, cur[1 .. cur.len - 1]);
                if (tok.len == 0) {
                    self.done = true;
                    return;
                }
                if (std.mem.startsWith(u8, tok, "http://") or std.mem.startsWith(u8, tok, "https://")) {
                    if (!lineSafe(tok)) return eval.explain(error.BadHeaderValue, "the response's next-page URL holds a line break or NUL, which would inject into the request");
                    if (!objstore.sameOrigin(self.base_url, tok)) return eval.explain(error.HttpCrossOriginPage, "the response's next-page URL is on another host or scheme than the first page's, and this read's headers and login go only to its own");
                    self.next_url = try self.arena.dupe(u8, tok);
                } else {
                    self.next_url = try withParam(self.arena, self.base_url, self.opts.cursor_param, tok);
                }
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
        if (driver.aborting()) return error.Aborted;
        return self.settle(arena, try self.spawnFetch(req), false);
    }

    /// The one fetch path, sequential or prefetched: a slot's body once its worker has
    /// run the retries under the page deadline. A 401 re-logs in and a TLS failure
    /// repairs the CA bundle, each once, and is replayed as a fresh slot under its own
    /// deadline and retries; 401 goes to re-login before `retry_statuses` applies.
    fn settle(self: *HttpSource, arena: std.mem.Allocator, slot: *Slot, replay: bool) ![]const u8 {
        var red_buf: [512]u8 = undefined;
        const shown = redactUrl(&red_buf, slot.url);
        self.awaitSlot(slot) catch |e| {
            obs.logOr(self.logger, .warn, "page fetch abandoned ({s}): {s}", .{ @errorName(e), shown });
            return e;
        };
        defer freeSlot(slot);
        if (slot.err) |e| {
            if (e == error.TlsInitializationFailed and !replay and self.alone() and self.tryRepair(slot.url)) return self.replayOf(arena, slot);
            obs.logOr(self.logger, .err, "page fetch failed ({s}): {s}", .{ @errorName(e), shown });
            return mapTransport(e);
        }
        if (slot.code == 401 and !replay) {
            const a = self.auth orelse return self.listedFallback(401, self.raiseStatus(401, slot.url, slot.body));
            if (slot.gen >= self.auth_gen) {
                if (!a.refresh(self.client)) return self.listedFallback(401, self.raiseStatus(401, slot.url, slot.body));
                self.auth_gen += 1;
            }
            return self.replayOf(arena, slot);
        }
        if (slot.code == 204) return "";
        if (slot.code != 200) return self.raiseStatus(slot.code, slot.url, slot.body);
        return arena.dupe(u8, slot.body);
    }

    /// No other worker can be using the client, so its CA bundle may grow.
    fn alone(self: *HttpSource) bool {
        return self.slots.items.len == 0 and self.zombies == 0;
    }

    fn replayOf(self: *HttpSource, arena: std.mem.Allocator, slot: *Slot) ![]const u8 {
        return self.settle(arena, try self.spawnFetch(.{ .url = slot.url, .body = slot.req_body }), true);
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

    fn issueSlot(self: *HttpSource) !void {
        if (self.total_pages) |t| {
            if (1 + self.pages_issued >= t) {
                self.issue_done = true;
                return;
            }
        }
        if (self.pages_issued + 1 >= self.opts.max_pages) self.issue_done = true;
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

    /// Page strings are built in a scratch arena and duped into the slot's own;
    /// building them in the run arena would grow it by ~2x URL bytes per page.
    fn spawnSlot(self: *HttpSource) !*Slot {
        var scratch = std.heap.ArenaAllocator.init(self.gpa);
        defer scratch.deinit();
        const req = try self.pageReq(scratch.allocator());
        return self.spawnFetch(req);
    }

    fn spawnFetch(self: *HttpSource, req: PageReq) !*Slot {
        const slot = try self.gpa.create(Slot);
        slot.* = .{
            .gpa = self.gpa,
            .snap = std.heap.ArenaAllocator.init(self.gpa),
            .client = self.client,
            .method_post = self.opts.method == .post,
            .content_type = self.contentType(),
            .user_agent = userAgentOf(self.opts),
            .url = undefined,
            .gen = self.auth_gen,
            .retries = self.opts.retries,
            .retry_base_ms = self.opts.retry_base_ms,
        };
        errdefer freeSlot(slot);
        const sa = slot.snap.allocator();
        slot.user_agent = try sa.dupe(u8, slot.user_agent);
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

    /// One request of a slot. A 429, a 5xx or a listed status other than 401 is worth
    /// another attempt; every status is kept for the consumer, a transport error is not.
    /// An abandoned slot stops retrying, as nobody waits for it.
    fn attempt(slot: *Slot) !objstore.Verdict {
        if (slot.state.load(.acquire) == @intFromEnum(SlotState.abandoned)) return error.Aborted;
        slot.code = 0;
        const res = try objstore.exchange(slot.snap.allocator(), slot.client, .{
            .method = if (slot.method_post) .POST else .GET,
            .url = slot.url,
            .headers = slot.hdrs[0..slot.nh],
            .payload = slot.req_body,
            .content_type = slot.content_type,
            .user_agent = slot.user_agent,
            .max_body = max_page_bytes,
        });
        slot.code = res.code;
        slot.body = res.body;
        const again = res.code == 429 or res.code >= 500 or (res.code != 401 and statusListed(slot.retry_statuses, res.code));
        if (!again) return .done;
        const after: ?u64 = if (res.retry_after_ms) |ms| @min(ms, max_retry_after_ms) else null;
        return .{ .failed = .{ .code = res.code, .err = error.HttpServerBusy, .retry_after_ms = after, .retriable = true } };
    }

    /// The retries `retries` and `retry_base_ms` ask for, unbounded in total sleep: the
    /// page deadline bounds them.
    fn workerMain(slot: *Slot) void {
        var prng = objstore.jitterPrng();
        objstore.retry(.{
            .rand = prng.random(),
            .max_attempts = @intCast(@max(slot.retries, 0) + 1),
            .base_ms = @intCast(@max(slot.retry_base_ms, 1)),
            .max_shift = 6,
            .budget_ms = std.math.maxInt(u64),
        }, slot, attempt) catch |e| {
            if (slot.code == 0) slot.err = e;
        };
        if (slot.state.cmpxchgStrong(
            @intFromEnum(SlotState.running),
            @intFromEnum(SlotState.done),
            .acq_rel,
            .acquire,
        ) != null) {
            freeSlot(slot);
        }
    }

    /// Waits under the page deadline, polling so Ctrl+C interrupts within ~25ms. On
    /// timeout or abort the slot is abandoned to its worker and counted as a zombie.
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
                return;
            }
            std.Thread.sleep(25 * std.time.ns_per_ms);
        }
    }

    /// Discards in-flight slots no longer wanted after one short shared grace window,
    /// rather than waiting a full page deadline per slot.
    fn drainSlots(self: *HttpSource) void {
        const grace_ms: i64 = if (self.opts.timeout_ms > 0) @min(self.opts.timeout_ms, 5_000) else 5_000;
        const deadline = std.time.milliTimestamp() +| grace_ms;
        for (self.slots.items) |slot| {
            if (self.awaitSlotUntil(slot, deadline)) |_| freeSlot(slot) else |_| {}
        }
        self.slots.clearRetainingCapacity();
    }

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
        return self.consumeBody(arena, try self.settle(arena, self.slots.orderedRemove(0), false));
    }

    fn consumeBody(self: *HttpSource, arena: std.mem.Allocator, body: []const u8) !?Batch {
        const trimmed = std.mem.trim(u8, body, " \t\r\n");
        if (trimmed.len == 0 or std.mem.eql(u8, trimmed, "null")) return self.finishEmpty();
        try jt.validate(arena, trimmed);
        const items = try itemsOf(arena, trimmed, self.opts.items);
        if (items.len == 0) return self.finishEmpty();
        self.noteProgress(items.len);
        const batch = try fjson.batchOf(arena, self.schema, items);
        if (self.opts.stop_short and items.len < self.opts.page_size) {
            self.issue_done = true;
            self.drainSlots();
            self.done = true;
        }
        return batch;
    }

    fn raiseStatus(self: *HttpSource, code: u16, url: []const u8, body: []const u8) anyerror {
        var ub: [512]u8 = undefined;
        const shown = redactUrl(&ub, url);
        obs.logOr(self.logger, .warn, "http {d} from {s}: {f}", .{ code, shown, std.zig.fmtString(body[0..@min(body.len, 300)]) });
        const e = if (code == 401)
            error.HttpUnauthorized
        else if (statusListed(self.opts.retry_statuses, code))
            error.HttpServerBusy
        else
            statusError(code);
        var buf: [400]u8 = undefined;
        return eval.explain(e, std.fmt.bufPrint(&buf, "http {d} from {s}", .{ code, shown }) catch shown);
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
        return try fjson.batchOf(arena, self.schema, page.items);
    }

    pub fn close(self: *HttpSource) void {
        self.drainSlots();
        self.slots.deinit();
        if (self.zombies == 0) {
            self.client.deinit();
            self.gpa.destroy(self.client);
        } else {
            obs.logOr(self.logger, .warn, "http: {d} timed-out request(s) abandoned; client leaked intentionally", .{self.zombies});
        }
    }

    pub fn source(self: *HttpSource) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }
};

pub const user_agent = "basalt/" ++ @import("build_options").version;

pub fn userAgentOf(opts: Options) []const u8 {
    const h = opts.header orelse return user_agent;
    const colon = std.mem.indexOfScalar(u8, h, ':') orelse return user_agent;
    if (!std.ascii.eqlIgnoreCase(std.mem.trim(u8, h[0..colon], " "), "user-agent")) return user_agent;
    return std.mem.trim(u8, h[colon + 1 ..], " ");
}

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
        const name = std.mem.trim(u8, h[0..colon], " ");
        if (!std.ascii.eqlIgnoreCase(name, "user-agent")) {
            try hdrs.append(.{
                .name = name,
                .value = std.mem.trim(u8, h[colon + 1 ..], " "),
            });
        }
    }
    try hdrs.append(.{ .name = "Accept", .value = "application/json" });
    return hdrs.toOwnedSlice();
}

fn envOpt(arena: std.mem.Allocator, name_opt: ?[]const u8) !?[]const u8 {
    const name = name_opt orelse return null;
    return std.process.getEnvVarOwned(arena, name) catch return error.MissingAuthEnv;
}

/// A validated page's rows: the array (or one object) at `items_path`, or the page
/// itself, each a row's text.
fn itemsOf(arena: std.mem.Allocator, doc: []const u8, items_path: ?[]const u8) ![]const []const u8 {
    if (items_path) |p| {
        const v = (try fjson.field(arena, doc, p)) orelse return error.ItemsFieldMissing;
        return (try fjson.rowsOf(arena, v, null)) orelse error.ItemsFieldNotArray;
    }
    return (try fjson.rowsOf(arena, doc, null)) orelse error.ExpectedJsonArrayOrObject;
}

fn appendParam(arena: std.mem.Allocator, base: []const u8, key: []const u8, val: []const u8) ![]const u8 {
    const sep: []const u8 = if (base.len == 0) "" else "&";
    return std.fmt.allocPrint(arena, "{s}{s}{s}={s}", .{ base, sep, key, val });
}

/// `base` with `key=val` appended, `val` percent-encoded: a cursor token may hold
/// `&`, `+` or spaces.
fn withParam(arena: std.mem.Allocator, base: []const u8, key: []const u8, val: []const u8) ![]const u8 {
    var buf = std.array_list.Managed(u8).init(arena);
    try buf.appendSlice(base);
    try buf.append(if (std.mem.indexOfScalar(u8, base, '?') != null) '&' else '?');
    try buf.appendSlice(key);
    try buf.append('=');
    try formEncode(&buf, val);
    return buf.toOwnedSlice();
}

/// Maps a non-200 status to a named error, distinct per fix: 429/5xx are worth a
/// control-plane retry (exit 75), 401 is an expired token, 400 a rejected request.
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

fn mapTransport(e: anyerror) anyerror {
    return if (objstore.transient(e)) error.HttpTransportFailed else e;
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

const TestServer = struct {
    listener: std.net.Server,
    responses: []const []const u8,
    statuses: ?[]const []const u8 = null,
    route_div: ?i64 = null,
    expected: ?usize = null,
    captured: [8][2048]u8 = undefined,
    captured_len: [8]usize = .{0} ** 8,
    requests: std.atomic.Value(usize) = .init(0),
    stop: std.atomic.Value(bool) = .init(false),

    fn start(responses: []const []const u8) !*TestServer {
        const self = try std.testing.allocator.create(TestServer);
        self.* = .{ .listener = try http_fake.listen(128), .responses = responses };
        return self;
    }
    fn port(self: *TestServer) u16 {
        return self.listener.listen_address.getPort();
    }
    fn run(self: *TestServer) void {
        self.serve() catch {};
    }
    /// Stops the server thread even when the client made fewer requests than it
    /// expected, so a regression fails the test instead of hanging it in `accept`.
    fn finish(self: *TestServer, th: std.Thread) void {
        self.stop.store(true, .seq_cst);
        http_fake.wake(&self.listener);
        th.join();
    }
    fn serve(self: *TestServer) !void {
        const total = self.expected orelse self.responses.len;
        var i: usize = 0;
        while (i < total) : (i += 1) {
            const conn = try self.listener.accept();
            defer conn.stream.close();
            if (self.stop.load(.seq_cst)) return;
            var req_buf: [2048]u8 = undefined;
            const req_len = try readRequest(conn.stream, &req_buf);
            _ = self.requests.fetchAdd(1, .seq_cst);
            if (i < self.captured.len) {
                @memcpy(self.captured[i][0..req_len], req_buf[0..req_len]);
                self.captured_len[i] = req_len;
            }
            var body: []const u8 = undefined;
            var status: []const u8 = "200 OK";
            if (self.route_div) |div| {
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

/// One whole request: the headers, then as many body bytes as `Content-Length` says,
/// however the client's writes were split.
fn readRequest(stream: std.net.Stream, buf: []u8) !usize {
    var n: usize = 0;
    while (n < buf.len) {
        const got = try stream.read(buf[n..]);
        if (got == 0) break;
        n += got;
        const end = std.mem.indexOf(u8, buf[0..n], "\r\n\r\n") orelse continue;
        if (n >= end + 4 + http_fake.contentLength(buf[0..end])) break;
    }
    return n;
}

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
    defer srv.finish(th);

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
    defer srv.finish(th);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .items = "data", .paginate = .page });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
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
    defer srv.finish(th);

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
    defer srv.finish(th);

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
    defer srv.finish(th);

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
    defer srv.finish(th);

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
    defer srv.finish(th);

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

test "http source: prefetch fetches the pages ahead and stops on empty" {
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
    srv.expected = 5;
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    var joined = false;
    defer if (!joined) srv.finish(th);

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
    srv.finish(th);
    joined = true;
    var seen2 = false;
    var seen4 = false;
    for (0..srv.captured.len) |i| {
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

    const srv = try TestServer.start(&.{
        \\[{"id":1},{"id":2}]
        ,
        \\[{"id":3}]
        ,
        "[]",
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer srv.finish(th);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .paginate = .offset,
        .page_param = "$skip",
        .size_param = "$top",
        .page_size = 2,
        .stop_short = true,
        .timeout_ms = 2000,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
    try std.testing.expectEqual(@as(usize, 2), srv.requests.load(.seq_cst));
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
    defer srv.finish(th);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .paginate = .page });
    defer s.close();
    _ = (try s.next(a)).?;

    driver.requestAbort();
    defer driver.resetAbort();
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
    defer srv.finish(th);

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
    try std.testing.expect(std.mem.indexOf(u8, r1, "/rest?") == null);
}

test "http source: total_field bounds the page count exactly" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"totalCount":2,"itens":[{"id":1},{"id":2}]}
        ,
        \\{"totalCount":2,"itens":[{"id":3}]}
        ,
        \\{"totalCount":2,"itens":[]}
    });
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer srv.finish(th);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/rest", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{
        .method = .post,
        .body = "cmd=get",
        .items = "itens",
        .paginate = .page,
        .total_field = "totalCount",
        .timeout_ms = 2000,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 3), try drain(s, a));
    try std.testing.expectEqual(@as(usize, 2), srv.requests.load(.seq_cst));
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
    defer srv.finish(th);

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
    defer srv.finish(th);

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

var hang_released = std.atomic.Value(bool).init(false);

fn serveOneThenHang(listener: *std.net.Server) void {
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
    var waited: usize = 0;
    while (!hang_released.load(.seq_cst) and waited < 500) : (waited += 1) std.Thread.sleep(20 * std.time.ns_per_ms);
    conn.stream.close();
}

test "a black-holed page request times out instead of hanging the run" {
    var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var listener = try http_fake.listen(128);
    defer listener.deinit();
    hang_released.store(false, .seq_cst);
    const th = try std.Thread.spawn(.{}, serveOneThenHang, .{&listener});
    defer th.join();
    defer hang_released.store(true, .seq_cst);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{listener.listen_address.getPort()});
    const s = try HttpSource.open(a, std.heap.page_allocator, url, .{
        .paginate = .page,
        .prefetch = 2,
        .retries = 0,
        .timeout_ms = 400,
    });
    defer s.close();
    _ = (try s.next(a)).?;
    const started = std.time.milliTimestamp();
    try std.testing.expectError(error.ConnectionTimedOut, s.next(a));
    const waited = std.time.milliTimestamp() - started;
    try std.testing.expect(waited >= 350 and waited < 5_000);
}

fn serveThenHang(listener: *std.net.Server, replies: []const []const u8) void {
    for (replies) |r| {
        const conn = listener.accept() catch return;
        defer conn.stream.close();
        var rb: [2048]u8 = undefined;
        _ = conn.stream.read(&rb) catch return;
        conn.stream.writeAll(r) catch return;
    }
    const conn = listener.accept() catch return;
    var rb: [2048]u8 = undefined;
    _ = conn.stream.read(&rb) catch return;
    var waited: usize = 0;
    while (!hang_released.load(.seq_cst) and waited < 500) : (waited += 1) std.Thread.sleep(20 * std.time.ns_per_ms);
    conn.stream.close();
}

test "a page replayed after a re-login times out like any page" {
    var ar = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var listener = try http_fake.listen(128);
    defer listener.deinit();
    hang_released.store(false, .seq_cst);
    const replies = [_][]const u8{
        "HTTP/1.1 200 OK\r\ncontent-length: 7\r\nconnection: close\r\n\r\n\"tok-A\"",
        "HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        "HTTP/1.1 200 OK\r\ncontent-length: 7\r\nconnection: close\r\n\r\n\"tok-B\"",
    };
    const th = try std.Thread.spawn(.{}, serveThenHang, .{ &listener, @as([]const []const u8, &replies) });
    defer th.join();
    defer hang_released.store(true, .seq_cst);

    const base = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}", .{listener.listen_address.getPort()});
    const started = std.time.milliTimestamp();
    try std.testing.expectError(error.ConnectionTimedOut, HttpSource.openConn(a, std.heap.page_allocator, .{
        .base_url = base,
        .auth = .login_json,
        .login_url = "/login",
    }, "/items", .{ .retries = 0, .timeout_ms = 400 }));
    try std.testing.expect(std.time.milliTimestamp() - started < 5_000);
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

test "withQuery encodes keys and values and picks the separator" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("/c", try withQuery(a, "/c", &.{}));
    try std.testing.expectEqualStrings(
        "/c?region=S%C3%A3o%20Paulo&fields=name%2Ccca2",
        try withQuery(a, "/c", &.{ .{ .key = "region", .value = "São Paulo" }, .{ .key = "fields", .value = "name,cca2" } }),
    );
    try std.testing.expectEqualStrings("/c?a=1&q=x%26y", try withQuery(a, "/c?a=1", &.{.{ .key = "q", .value = "x&y" }}));
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
    defer srv.finish(th);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .items = "data", .paginate = .cursor });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[1][0..srv.captured_len[1]], "GET /items?cursor=t1") != null);
}

test "http source: a cursor token is URL-encoded; a later page's failure names its status and URL" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const srv = try TestServer.start(&.{
        \\{"next":"c 2&x+y","data":[{"id":1}]}
        ,
        \\{"err":"bad"}
    });
    srv.statuses = &.{ "200 OK", "400 Bad Request" };
    defer srv.deinit();
    const th = try std.Thread.spawn(.{}, TestServer.run, .{srv});
    defer srv.finish(th);

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/items", .{srv.port()});
    const s = try HttpSource.open(a, std.testing.allocator, url, .{ .items = "data", .paginate = .cursor });
    defer s.close();
    try std.testing.expectError(error.HttpBadRequest, drain(s, a));
    try std.testing.expect(std.mem.indexOf(u8, srv.captured[1][0..srv.captured_len[1]], "GET /items?cursor=c%202%26x%2By ") != null);
    const why = eval.takeFailure(error.HttpBadRequest).?;
    try std.testing.expect(std.mem.startsWith(u8, why, "http 400 from http://127.0.0.1:"));
    try std.testing.expect(std.mem.endsWith(u8, why, "/items?cursor=..."));
}

test "connFromKvs: bearer auth without a token is an error" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var msg: []const u8 = "";
    try std.testing.expectError(error.BadHttpConn, connFromKvs(ar.allocator(), &.{
        .{ .key = "base_url", .value = "http://x" },
        .{ .key = "auth", .value = "bearer" },
    }, &msg));
    try std.testing.expect(std.mem.indexOf(u8, msg, "token") != null);
}

test "http source: a 404 on open is HttpNotFound" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var srv: http_fake.FakeServer = undefined;
    try srv.start(&.{http_fake.FakeServer.reply("404 Not Found", "", "")});
    defer srv.stop();
    try std.testing.expectError(error.HttpNotFound, HttpSource.open(a, std.testing.allocator, try srv.url(a, "/x"), .{}));
}

test "statusError names each status family distinctly" {
    try std.testing.expectEqual(error.HttpBadRequest, statusError(400));
    try std.testing.expectEqual(error.HttpUnauthorized, statusError(401));
    try std.testing.expectEqual(error.HttpForbidden, statusError(403));
    try std.testing.expectEqual(error.HttpNotFound, statusError(404));
    try std.testing.expectEqual(error.HttpRequestFailed, statusError(418));
    try std.testing.expectEqual(error.HttpServerBusy, statusError(429));
    try std.testing.expectEqual(error.HttpServerBusy, statusError(500));
    try std.testing.expectEqual(error.HttpServerBusy, statusError(503));
}

test "a page field walks dotted paths; null and missing are both absent" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const root =
        \\{"meta":{"next":"tok","total":3,"gone":null},"data":[1]}
    ;
    try std.testing.expectEqualStrings("\"tok\"", (try fjson.field(a, root, "meta.next")).?);
    try std.testing.expectEqualStrings("3", (try fjson.field(a, root, "meta.total")).?);
    try std.testing.expect((try fjson.field(a, root, "meta.missing")) == null);
    try std.testing.expect((try fjson.field(a, root, "meta.gone")) == null);
    try std.testing.expect((try fjson.field(a, root, "data.next")) == null);
}

test "itemsOf: bare array, dotted path, and named errors" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    try std.testing.expectEqual(@as(usize, 2), (try itemsOf(a, "[{\"id\":1},{\"id\":2}]", null)).len);
    const wrapped =
        \\{"result":{"rows":[{"id":1}]}}
    ;
    try std.testing.expectEqual(@as(usize, 1), (try itemsOf(a, wrapped, "result.rows")).len);
    try std.testing.expectError(error.ItemsFieldMissing, itemsOf(a, wrapped, "result.nope"));
    try std.testing.expectEqual(@as(usize, 1), (try itemsOf(a, wrapped, "result")).len);
    try std.testing.expectError(error.ItemsFieldNotArray, itemsOf(a, "{\"n\":5}", "n"));
    try std.testing.expectEqual(@as(usize, 1), (try itemsOf(a, "{\"id\":9}", null)).len);
    try std.testing.expectError(error.ExpectedJsonArrayOrObject, itemsOf(a, "5", null));
}

test "withParam and appendParam pick the right separator" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("http://x/i?page=2", try withParam(a, "http://x/i", "page", "2"));
    try std.testing.expectEqualStrings("http://x/i?a=1&page=2", try withParam(a, "http://x/i?a=1", "page", "2"));
    try std.testing.expectEqualStrings("page=1", try appendParam(a, "", "page", "1"));
    try std.testing.expectEqualStrings("cmd=get&page=1", try appendParam(a, "cmd=get", "page", "1"));
}

test "a server-sent token or next URL that would break a header line is refused" {
    try std.testing.expect(lineSafe("Bearer abc.def"));
    for ([_][]const u8{ "abc\r\nX-Evil: 1", "abc\nX: 1", "abc\rx", "a\x00b" }) |v| try std.testing.expect(!lineSafe(v));
}

test "a URL in a log or message keeps no login and no query values" {
    var buf: [256]u8 = undefined;
    try std.testing.expectEqualStrings("https://api.example.com/v1/x?api_key=...&page=...", redactUrl(&buf, "https://user:secret@api.example.com/v1/x?api_key=abc&page=2"));
    try std.testing.expectEqualStrings("http://h/p", redactUrl(&buf, "http://h/p"));
    try std.testing.expectEqualStrings("http://h/p?flag", redactUrl(&buf, "http://h/p?flag"));
}
