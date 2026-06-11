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
    paginate: Mode = .none,
    page_param: []const u8 = "page", // page number (page mode) or offset (offset mode) param
    start_page: i64 = 1,
    start_offset: i64 = 0,
    size_param: ?[]const u8 = null,
    page_size: i64 = 100,
    cursor_param: []const u8 = "cursor",
    cursor_field: []const u8 = "next",
    max_pages: i64 = 10_000,
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
        }
    }
    return o;
}

pub const HttpSource = struct {
    arena: std.mem.Allocator, // run arena: self, schema, headers, first page
    client: std.http.Client,
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

    pub fn open(arena: std.mem.Allocator, url: []const u8, opts: Options) !*HttpSource {
        const self = try arena.create(HttpSource);
        self.* = .{
            .arena = arena,
            .client = initClient(arena),
            .base_url = url,
            .opts = opts,
            .headers = try buildHeaders(arena, opts),
            .schema = undefined,
            .first = null,
            .page_no = if (opts.paginate == .offset) opts.start_offset else opts.start_page,
        };
        errdefer self.client.deinit();

        const first_url = try self.pageUrl(arena);
        const page = try self.fetchParsed(arena, first_url);
        const items = page.items;
        self.schema = try request.inferSchema(arena, items);
        self.first = try request.batchFromJson(arena, self.schema, items);
        self.advance(arena, page.root, items.len) catch |e| return e;
        return self;
    }

    const Page = struct { root: json.Value, items: []const json.Value };

    /// Fetch one page and extract its rows. An empty body or a bare JSON `null`
    /// (end-of-dataset markers in the wild, alongside http 204) yields no items.
    fn fetchParsed(self: *HttpSource, arena: std.mem.Allocator, url: []const u8) !Page {
        const body = try self.fetchPage(arena, url);
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
                if (n_items == 0 or self.pages_fetched >= self.opts.max_pages) self.done = true;
            },
            .offset => {
                self.page_no += self.opts.page_size;
                if (n_items == 0 or self.pages_fetched >= self.opts.max_pages) self.done = true;
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

    fn pageUrl(self: *HttpSource, arena: std.mem.Allocator) ![]const u8 {
        switch (self.opts.paginate) {
            .none => return self.base_url,
            .page, .offset => {
                var buf: [32]u8 = undefined;
                const n = try std.fmt.bufPrint(&buf, "{d}", .{self.page_no});
                var url = try withParam(arena, self.base_url, self.opts.page_param, n);
                if (self.opts.size_param) |sp| {
                    const sz = try std.fmt.bufPrint(&buf, "{d}", .{self.opts.page_size});
                    url = try withParam(arena, url, sp, sz);
                }
                return url;
            },
            .cursor => return self.next_url orelse self.base_url,
        }
    }

    fn fetchPage(self: *HttpSource, arena: std.mem.Allocator, url: []const u8) ![]const u8 {
        return self.fetchPageRaw(arena, url) catch |e| switch (e) {
            // Likely a misordered server chain: rebuild trust from the chain
            // itself (repairBundle) and retry once.
            error.TlsInitializationFailed => {
                if (!self.tryRepair(url)) return e;
                return self.fetchPageRaw(arena, url);
            },
            else => e,
        };
    }

    fn tryRepair(self: *HttpSource, url: []const u8) bool {
        if (self.repaired) return false;
        self.repaired = true;
        const uri = std.Uri.parse(url) catch return false;
        const h = uriHost(uri) orelse return false;
        if (!repairBundle(self.arena, &self.client.ca_bundle, h, uri.port orelse 443)) return false;
        self.client.next_https_rescan_certs = false;
        return true;
    }

    fn fetchPageRaw(self: *HttpSource, arena: std.mem.Allocator, url: []const u8) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        const res = try self.client.fetch(.{
            .method = .GET,
            .location = .{ .url = url },
            .extra_headers = self.headers,
            .response_writer = &aw.writer,
        });
        const code = @intFromEnum(res.status);
        // 204 = no content: some OData servers (e.g. Ivanti) signal "past the
        // end of the dataset" this way instead of an empty `value` array.
        if (code == 204) return "";
        if (code != 200) {
            const b = aw.writer.buffered();
            std.debug.print("http {d} from {s}: {s}\n", .{ code, url, b[0..@min(b.len, 300)] });
            return statusError(code);
        }
        return aw.writer.buffered();
    }

    pub fn next(self: *HttpSource, arena: std.mem.Allocator) !?Batch {
        if (self.first) |b| {
            self.first = null;
            return b;
        }
        if (self.done) return null;
        const url = try self.pageUrl(arena);
        const page = try self.fetchParsed(arena, url);
        try self.advance(arena, page.root, page.items.len);
        if (page.items.len == 0) return null;
        return try request.batchFromJson(arena, self.schema, page.items);
    }

    pub fn close(self: *HttpSource) void {
        self.client.deinit();
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
        for (self.responses, 0..) |body, i| {
            const conn = try self.listener.accept();
            defer conn.stream.close();
            if (i < self.captured.len) {
                self.captured_len[i] = try conn.stream.read(&self.captured[i]);
            } else {
                var sink: [2048]u8 = undefined;
                _ = try conn.stream.read(&sink);
            }
            var wb: [256]u8 = undefined;
            const status = if (self.statuses) |st| st[i] else "200 OK";
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
    const s = try HttpSource.open(a, url, .{ .bearer = "sek" });
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
    const s = try HttpSource.open(a, url, .{ .items = "data", .paginate = .page });
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
    const s = try HttpSource.open(a, url, .{
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
    const s = try HttpSource.open(a, url, .{
        .items = "value",
        .paginate = .offset,
        .page_param = "$skip",
        .size_param = "$top",
        .page_size = 100,
    });
    defer s.close();
    try std.testing.expectEqual(@as(usize, 2), try drain(s, a));
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
    const s = try HttpSource.open(a, url, .{ .items = "data", .paginate = .cursor });
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
    try std.testing.expectError(error.HttpNotFound, HttpSource.open(a, url, .{}));
}

fn serve404Once(listener: *std.net.Server) void {
    const conn = listener.accept() catch return;
    defer conn.stream.close();
    var rb: [2048]u8 = undefined;
    _ = conn.stream.read(&rb) catch return;
    conn.stream.writeAll("HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n") catch return;
}
