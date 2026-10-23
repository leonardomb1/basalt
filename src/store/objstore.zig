//! The object-store layer under `azure.zig` and `s3.zig`: everything the two
//! providers share, and the interface consumers (csv, pqdecode, pqwrite) use so
//! they never branch on the URL scheme themselves.
//!
//! Shared pieces: the XML tag scraping both REST APIs need, the error-body parser
//! (`<Code>`/`<Message>`, shown as `Code: Message (HTTP nnn)` instead of a bare
//! status), the retry policy, the `std.Io.Writer` drain adapter for a streaming
//! upload, and the paginated listing loop. Signing stays per provider: Shared Key
//! and SigV4 have nothing in common past "put a header on the request", and query
//! parameter names and encoding stay with each provider's page function because
//! they are signed and the services canonicalise a query string differently.
//!
//! Retry: a transport failure or a 408/429/500/502/503/504, at most five attempts
//! and about 30 s of sleep, exponential backoff with jitter so parallel lanes
//! throttled at the same instant do not retry in lockstep, stretched to any
//! `Retry-After` the service sent. Any other 4xx fails at once, and so does a run
//! that is aborting. Requests that go through it are safe to replay: GET, HEAD and
//! listings read, Put Block and UploadPart are keyed by block id / part number,
//! Put Block List is a full replace. Replaying CreateMultipartUpload after a lost
//! answer starts a second upload and orphans the first, which, like any dropped
//! writer's, is invisible and costs only storage. S3's CompleteMultipartUpload is
//! not idempotent: a replay
//! after a lost response may meet NoSuchUpload, which s3.zig resolves by checking
//! the object it should have made. A `Verdict.again` (the attempt fixed the cause
//! itself, e.g. created the missing container) spends an attempt but no backoff.
//! `exchange` is the one request path, because `std.http.Client.fetch` does not
//! surface the response headers (ETag, Retry-After) the retry and S3 need.
//!
//! The interface is a vtable rather than a comptime generic because the provider
//! is chosen at run time from a URL string: a reader over `az://…` and one over
//! `s3://…` are the same code path holding a different `Object`. A provider is
//! one `Provider` const naming its scheme and five functions; adding one means
//! writing that const and appending it to `providers`. An object `Writer` left
//! without `finish` publishes nothing, since staged blocks and uncompleted uploads
//! are invisible to readers; its `abort` only frees its client and request
//! memory, while an SFTP one removes its visible `.part` file. A read of a prefix
//! that lists nothing raises the provider's `empty_prefix` error, as it is almost
//! always a typo.

const std = @import("std");
const azure = @import("azure.zig");
const xml = @import("../format/xml/token.zig");
const driver = @import("../connect/driver.zig");
const http_client = @import("../net/http_client.zig");
const percent = @import("../net/percent.zig");
const FakeServer = @import("../testing/http_fake.zig").FakeServer;
const s3 = @import("s3.zig");
const sftp = @import("sftp.zig");
const smb = @import("smb.zig");

/// An error reply's code and the first line of its message, as written: a code is
/// compared, and `describe` decodes the message for display.
pub fn parseError(body: []const u8) ?struct { code: []const u8, message: []const u8 } {
    const code = xml.firstRawText(body, "Code") orelse return null;
    const msg = xml.firstRawText(body, "Message") orelse "";
    return .{ .code = code, .message = std.mem.sliceTo(msg, '\n') };
}

pub fn describe(arena: std.mem.Allocator, code: u16, body: []const u8) ![]const u8 {
    if (parseError(body)) |e| {
        var msg = std.array_list.Managed(u8).init(arena);
        xml.decodeInto(&msg, e.message, false) catch try msg.appendSlice(e.message);
        return std.fmt.allocPrint(arena, "{s}: {s} (HTTP {d})", .{ e.code, msg.items, code });
    }
    return std.fmt.allocPrint(arena, "HTTP {d}", .{code});
}

pub fn retriable(code: u16) bool {
    return switch (code) {
        408, 429, 500, 502, 503, 504 => true,
        else => false,
    };
}

/// A request that died on the wire: the socket set every connector shares, plus a
/// server that closed the connection instead of answering.
pub fn transient(e: anyerror) bool {
    return driver.transientNet(e) or e == error.HttpConnectionClosing;
}

pub const max_attempts = 5;
pub const budget_ms: u64 = 30_000;

/// 500 ms doubling per attempt up to 8 s, plus up to 50% jitter.
pub fn backoffMs(attempt: usize, rand: std.Random) u64 {
    const base = @as(u64, 500) << @intCast(@min(attempt, 4));
    return base + rand.uintLessThan(u64, base / 2 + 1);
}

/// `Retry-After` in delta-seconds; the HTTP-date form is left to the backoff.
pub fn retryAfterMs(value: []const u8) ?u64 {
    const secs = std.fmt.parseInt(u64, std.mem.trim(u8, value, " \t"), 10) catch return null;
    return std.math.mul(u64, secs, std.time.ms_per_s) catch budget_ms;
}

pub fn jitterPrng() std.Random.DefaultPrng {
    return std.Random.DefaultPrng.init(@bitCast(std.time.milliTimestamp()));
}

pub const Verdict = union(enum) {
    done,
    again,
    failed: struct { code: u16, err: anyerror, retry_after_ms: ?u64 = null },
};

pub const Sleep = *const fn (ns: u64) void;

pub const Policy = struct {
    rand: std.Random,
    sleep: Sleep = std.Thread.sleep,
};

/// Runs `attemptFn(ctx)` until it reports `.done`, giving up on the first failure
/// that is not transient, after `max_attempts`, once `budget_ms` of sleep is spent,
/// or as soon as the run is aborting.
pub fn retry(policy: Policy, ctx: anytype, comptime attemptFn: anytype) anyerror!void {
    var slept: u64 = 0;
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        var wait = backoffMs(attempt, policy.rand);
        const err = if (attemptFn(ctx)) |v| switch (v) {
            .done => return,
            .again => continue,
            .failed => |f| blk: {
                if (!retriable(f.code)) return f.err;
                wait = @max(wait, f.retry_after_ms orelse 0);
                break :blk f.err;
            },
        } else |e| blk: {
            if (!transient(e)) return e;
            break :blk e;
        };
        if (attempt + 1 >= max_attempts or slept >= budget_ms or driver.aborting()) return err;
        wait = @min(wait, budget_ms - slept);
        if (!pause(policy.sleep, wait)) return err;
        slept += wait;
    }
}

/// Sleeps `ms` in short slices; false as soon as the run starts aborting.
fn pause(sleep: Sleep, ms: u64) bool {
    var left = ms;
    while (left > 0) {
        const step: u64 = @min(left, 100);
        sleep(step * std.time.ns_per_ms);
        left -= step;
        if (driver.aborting()) return false;
    }
    return true;
}

pub const Response = struct {
    code: u16,
    body: []const u8,
    etag: []const u8 = "",
    retry_after_ms: ?u64 = null,
    content_length: ?u64 = null,

    pub fn failed(self: Response, err: anyerror) Verdict {
        return .{ .failed = .{ .code = self.code, .err = err, .retry_after_ms = self.retry_after_ms } };
    }
};

pub const Call = struct {
    method: std.http.Method,
    url: []const u8,
    headers: []const std.http.Header = &.{},
    payload: ?[]const u8 = null,
    content_type: ?[]const u8 = null,
    identity: bool = false,
    user_agent: ?[]const u8 = null,
    max_body: ?usize = null,
};

/// One request, its body read whole into `arena` and decoded per its
/// Content-Encoding; `identity` asks for none, as a byte range of a compressed body
/// cannot be decoded. A body that decodes past `max_body` bytes is
/// `HttpBodyTooLarge`, so a compressed bomb stops at the cap. A bodiless request follows up to three redirects, here rather
/// than in std, which keeps `extra_headers` on a redirect to another host: a hop to
/// another scheme, host or port drops `headers`, so a token or key never leaves its
/// origin. Header values are copied out before the body is read, which invalidates
/// the head's strings. A request that fails before its response head arrives would
/// leave its connection pooled (std keeps a `.ready` one), so the replay would write
/// into the dead socket; a failure always drops it.
pub fn exchange(arena: std.mem.Allocator, client: *std.http.Client, call: Call) !Response {
    var hop = call;
    var hops: u8 = 0;
    while (true) {
        var location: ?[]const u8 = null;
        const out = try exchangeOnce(arena, client, hop, &location);
        const loc = location orelse return out;
        if (hop.payload != null or hops == 3) return out;
        hops += 1;
        const next = try resolveLocation(arena, hop.url, loc);
        if (!sameOrigin(hop.url, next)) hop.headers = &.{};
        hop.url = next;
    }
}

fn isRedirect(code: u16) bool {
    return switch (code) {
        301, 302, 303, 307, 308 => true,
        else => false,
    };
}

/// `url`'s `scheme://host[:port]`, the part a redirect must keep for headers to follow.
fn origin(url: []const u8) []const u8 {
    const at = std.mem.indexOf(u8, url, "://") orelse return url;
    const rest = url[at + 3 ..];
    const end = std.mem.indexOfAny(u8, rest, "/?#") orelse rest.len;
    return url[0 .. at + 3 + end];
}

/// Whether two absolute URLs share scheme, host and port.
pub fn sameOrigin(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(origin(a), origin(b));
}

/// A `Location` against the URL that sent it: absolute, scheme-relative, rooted or
/// relative to the sender's folder.
fn resolveLocation(arena: std.mem.Allocator, base: []const u8, loc: []const u8) ![]const u8 {
    if (std.mem.indexOf(u8, loc, "://") != null) return loc;
    const o = origin(base);
    if (std.mem.startsWith(u8, loc, "//")) return std.mem.concat(arena, u8, &.{ o[0 .. std.mem.indexOf(u8, o, "://").? + 1], loc });
    if (std.mem.startsWith(u8, loc, "/")) return std.mem.concat(arena, u8, &.{ o, loc });
    const path_end = std.mem.indexOfAny(u8, base[o.len..], "?#") orelse base.len - o.len;
    const path = base[o.len..][0..path_end];
    const dir = path[0..(std.mem.lastIndexOfScalar(u8, path, '/') orelse 0)];
    return std.mem.concat(arena, u8, &.{ o, dir, "/", loc });
}

fn exchangeOnce(arena: std.mem.Allocator, client: *std.http.Client, call: Call, location: *?[]const u8) !Response {
    const uri = try std.Uri.parse(call.url);
    var req = try client.request(call.method, uri, .{
        .redirect_behavior = .unhandled,
        .extra_headers = call.headers,
        .headers = .{
            .user_agent = if (call.user_agent) |ua| .{ .override = ua } else .default,
            .content_type = if (call.content_type) |ct| .{ .override = ct } else .default,
            .accept_encoding = if (call.identity) .{ .override = "identity" } else .default,
        },
    });
    defer req.deinit();
    errdefer if (req.connection) |c| {
        c.closing = true;
    };
    if (call.payload) |p| {
        req.transfer_encoding = .{ .content_length = p.len };
        var body = try req.sendBodyUnflushed(&.{});
        try body.writer.writeAll(p);
        try body.end();
        try req.connection.?.flush();
    } else {
        try req.sendBodiless();
    }

    var resp = try req.receiveHead(&.{});
    var out = Response{ .code = @intFromEnum(resp.head.status), .body = "", .content_length = resp.head.content_length };
    var it = resp.head.iterateHeaders();
    while (it.next()) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "etag")) out.etag = try arena.dupe(u8, h.value);
        if (std.ascii.eqlIgnoreCase(h.name, "retry-after")) out.retry_after_ms = retryAfterMs(h.value);
        if (std.ascii.eqlIgnoreCase(h.name, "location") and isRedirect(out.code)) location.* = try arena.dupe(u8, h.value);
    }
    if (location.* != null) {
        if (req.connection) |c| c.closing = true;
        return out;
    }
    if (call.method == .HEAD) return out;
    if (resp.head.content_encoding == .compress) return error.UnsupportedCompressionMethod;
    var aw = std.Io.Writer.Allocating.init(arena);
    var transfer_buf: [64]u8 = undefined;
    var decompress: std.http.Decompress = undefined;
    const reader = resp.readerDecompressing(&transfer_buf, &decompress, http_client.decompress_direct);
    var left: usize = call.max_body orelse std.math.maxInt(usize);
    while (true) {
        const n = reader.stream(&aw.writer, .limited(left +| 1)) catch |e| switch (e) {
            error.EndOfStream => break,
            error.ReadFailed => return resp.bodyErr().?,
            error.WriteFailed => return error.OutOfMemory,
        };
        if (n > left) return error.HttpBodyTooLarge;
        left -= n;
    }
    out.body = aw.writer.buffered();
    return out;
}

/// What an upload writer holds beside its open-time arena, which keeps only what
/// lasts the whole upload (block ids, part ETags): the part being gathered, the
/// HTTP client (owned when the writer made it), and the arena of the request in
/// flight, emptied before the next part, so memory stays flat however large the
/// object grows.
pub const Upload = struct {
    client: *std.http.Client,
    owns_client: bool = false,
    req: std.heap.ArenaAllocator = .init(std.heap.page_allocator),
    part: []u8 = &.{},
    part_len: usize = 0,
    err_buf: [256]u8 = undefined,
    released: bool = false,

    /// Over `client`, or over a client of its own when null, freed by `release`.
    pub fn init(arena: std.mem.Allocator, client: ?*std.http.Client, part_size: usize) !Upload {
        const part = try arena.alloc(u8, part_size);
        if (client) |c| return .{ .client = c, .part = part };
        const c = try arena.create(std.http.Client);
        c.* = http_client.initClient(std.heap.smp_allocator);
        return .{ .client = c, .owns_client = true, .part = part };
    }

    /// Copies `bytes` in, handing each full part to `ship(w, part)`, so every part
    /// but the last is exactly the buffer's size however the writer machinery chunks.
    pub fn stage(self: *Upload, bytes: []const u8, w: anytype, comptime ship: anytype) !void {
        var rest = bytes;
        while (rest.len > 0) {
            const n = @min(self.part.len - self.part_len, rest.len);
            @memcpy(self.part[self.part_len..][0..n], rest[0..n]);
            self.part_len += n;
            rest = rest[n..];
            if (self.part_len == self.part.len) {
                try ship(w, self.part);
                self.part_len = 0;
            }
        }
    }

    /// The part gathered so far.
    pub fn pending(self: *Upload) []const u8 {
        return self.part[0..self.part_len];
    }

    /// The request arena, emptied of the last request's allocations.
    pub fn fresh(self: *Upload) std.mem.Allocator {
        _ = self.req.reset(.{ .retain_with_limit = 64 * 1024 });
        return self.req.allocator();
    }

    /// The request arena as it stands, for the steps of the request in flight.
    pub fn alloc(self: *Upload) std.mem.Allocator {
        return self.req.allocator();
    }

    /// `describe`'s text, kept in the writer so it outlives the request.
    pub fn keepError(self: *Upload, code: u16, body: []const u8) []const u8 {
        const msg = describe(self.req.allocator(), code, body) catch return "";
        const n = @min(msg.len, self.err_buf.len);
        @memcpy(self.err_buf[0..n], msg[0..n]);
        return self.err_buf[0..n];
    }

    /// Once only: `abort` may follow a `finish` that failed.
    pub fn release(self: *Upload) void {
        if (self.released) return;
        self.released = true;
        self.req.deinit();
        if (self.owns_client) self.client.deinit();
    }
};

pub fn drain(comptime T: type) *const fn (*std.Io.Writer, []const []const u8, usize) std.Io.Writer.Error!usize {
    return struct {
        fn f(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *T = @fieldParentPtr("interface", w);
            var total: usize = 0;
            total += try self.put(w.buffered());
            for (data[0 .. data.len - 1]) |d| total += try self.put(d);
            const last = data[data.len - 1];
            for (0..splat) |_| total += try self.put(last);
            return w.consume(total);
        }
    }.f;
}

/// A front-to-back reader over `[start, end)` of a remote `File`, fetched a whole
/// `chunk_size` at a time and handed out in the sizes asked. `File` supplies `open`,
/// `size`, `close` and `readChunk(off, buf)`, which marks the file unhealthy as its
/// protocol needs.
pub fn ChunkStream(comptime File: type, comptime chunk_size: usize) type {
    return struct {
        const Self = @This();

        file: *File,
        at: u64 = 0,
        end: u64,
        owns_file: bool,
        chunk: []u8,
        pos: usize = 0,
        len: usize = 0,
        interface: std.Io.Reader,

        pub fn open(arena: std.mem.Allocator, url: []const u8) !*Self {
            const f = try File.open(arena, url);
            errdefer f.close();
            return make(arena, f, 0, f.size, true);
        }

        /// `len` bytes of an open file from `off`: a zip member inside a remote archive.
        pub fn window(arena: std.mem.Allocator, f: *File, off: u64, len: u64) !*Self {
            return make(arena, f, off, off + len, false);
        }

        fn make(arena: std.mem.Allocator, f: *File, start: u64, end: u64, owns: bool) !*Self {
            const self = try arena.create(Self);
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

        pub fn close(self: *Self) void {
            if (self.owns_file) self.file.close();
        }

        fn streamFn(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
            const self: *Self = @fieldParentPtr("interface", r);
            if (self.pos == self.len) {
                if (self.at >= self.end) return error.EndOfStream;
                const want: usize = @intCast(@min(@as(u64, self.chunk.len), self.end - self.at));
                const n = self.file.readChunk(self.at, self.chunk[0..want]) catch return error.ReadFailed;
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
}

pub const ListSpec = struct {
    item_tag: []const u8,
    next_tag: []const u8,
};

/// Collects every item across a paginated listing. `pageFn(ctx, marker)` issues
/// one request (`marker` empty for the first page) and returns the XML body or the provider's error.
pub fn listPages(arena: std.mem.Allocator, spec: ListSpec, ctx: anytype, comptime pageFn: anytype) ![][]const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    var marker: []const u8 = "";
    while (true) {
        const body: []const u8 = try pageFn(ctx, marker);
        try out.appendSlice(try xml.elementTexts(arena, body, spec.item_tag));
        const next = try xml.elementTexts(arena, body, spec.next_tag);
        if (next.len == 0 or next[0].len == 0) break;
        marker = next[0];
    }
    return out.toOwnedSlice();
}

pub const Provider = struct {
    scheme: []const u8,
    empty_prefix: anyerror,
    vtable: *const VTable,

    pub const VTable = struct {
        parse: *const fn (arena: std.mem.Allocator, url: []const u8) anyerror!Object,
        list_prefix: *const fn (arena: std.mem.Allocator, client: *std.http.Client, url: []const u8) anyerror![]const []const u8,
        request_headers: *const fn (ptr: *const anyopaque, arena: std.mem.Allocator, method: []const u8, range: []const u8) anyerror![]const std.http.Header,
        status_to_error: *const fn (code: u16, body: []const u8) anyerror,
        open_writer: *const fn (ptr: *const anyopaque, arena: std.mem.Allocator, client: ?*std.http.Client, content_type: []const u8) anyerror!Writer,
        put_if_absent: *const fn (ptr: *const anyopaque, arena: std.mem.Allocator, client: *std.http.Client, bytes: []const u8, content_type: []const u8, why: *[]const u8) anyerror!bool,
    };
};

pub const Object = struct {
    provider: *const Provider,
    url: []const u8,
    ptr: *const anyopaque,

    pub fn requestHeaders(self: Object, arena: std.mem.Allocator, method: []const u8, range: []const u8) ![]const std.http.Header {
        return self.provider.vtable.request_headers(self.ptr, arena, method, range);
    }

    pub fn getHeaders(self: Object, arena: std.mem.Allocator) ![]const std.http.Header {
        return self.requestHeaders(arena, "GET", "");
    }

    pub fn statusToError(self: Object, code: u16, body: []const u8) anyerror {
        return self.provider.vtable.status_to_error(code, body);
    }

    /// A null `client` has the writer make its own, freed on `finish` or `abort`.
    pub fn openWriter(self: Object, arena: std.mem.Allocator, client: ?*std.http.Client, content_type: []const u8) !Writer {
        return self.provider.vtable.open_writer(self.ptr, arena, client, content_type);
    }

    /// Writes `bytes` as the object only if none is there, in one request: true
    /// when it made it, false when one existed. `why` gets the store's message on
    /// failure.
    pub fn putIfAbsent(self: Object, arena: std.mem.Allocator, client: *std.http.Client, bytes: []const u8, content_type: []const u8, why: *[]const u8) !bool {
        return self.provider.vtable.put_if_absent(self.ptr, arena, client, bytes, content_type, why);
    }
};

pub const Writer = struct {
    io: *std.Io.Writer,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        finish: *const fn (*anyopaque) anyerror!void,
        last_status: *const fn (*anyopaque) ?anyerror,
        last_error: *const fn (*anyopaque) []const u8,
        abort: ?*const fn (*anyopaque) void = null,
    };

    pub fn finish(self: Writer) !void {
        return self.vtable.finish(self.ptr);
    }

    pub fn abort(self: Writer) void {
        if (self.vtable.abort) |f| f(self.ptr);
    }

    /// Recovers the error the upload actually hit. `std.Io.Writer` only knows
    /// `WriteFailed`, so the status recorded at the failure is put back here.
    pub fn specific(self: Writer, e: anyerror) anyerror {
        if (e != error.WriteFailed) return e;
        return self.vtable.last_status(self.ptr) orelse e;
    }

    pub fn lastError(self: Writer) []const u8 {
        return self.vtable.last_error(self.ptr);
    }
};

/// Whether `path` is written whole on another host: SFTP, SMB or an object store,
/// none of which can be appended to.
pub fn isRemote(path: []const u8) bool {
    return sftp.isUrl(path) or smb.isUrl(path) or isUrl(path);
}

/// The writer for a remote `path` (see `isRemote`), or null for a local one.
pub fn openRemote(arena: std.mem.Allocator, path: []const u8, content_type: []const u8) !?Writer {
    if (sftp.isUrl(path)) return writer(try sftp.Upload.open(arena, path));
    if (smb.isUrl(path)) return writer(try smb.Upload.open(arena, path));
    if (!isUrl(path)) return null;
    return try (try parse(arena, path)).openWriter(arena, null, content_type);
}

pub fn writer(w: anytype) Writer {
    const T = @TypeOf(w.*);
    const Adapter = struct {
        fn finish(p: *anyopaque) anyerror!void {
            const self: *T = @ptrCast(@alignCast(p));
            return self.finish();
        }
        fn lastStatus(p: *anyopaque) ?anyerror {
            const self: *T = @ptrCast(@alignCast(p));
            return self.last_status;
        }
        fn lastError(p: *anyopaque) []const u8 {
            const self: *T = @ptrCast(@alignCast(p));
            return self.last_error;
        }
        fn abort(p: *anyopaque) void {
            const self: *T = @ptrCast(@alignCast(p));
            self.abort();
        }
        const vt = Writer.VTable{ .finish = finish, .last_status = lastStatus, .last_error = lastError, .abort = if (@hasDecl(T, "abort")) abort else null };
    };
    return .{ .io = &w.interface, .ptr = w, .vtable = &Adapter.vt };
}

pub fn cast(comptime T: type, ptr: *const anyopaque) *const T {
    return @ptrCast(@alignCast(ptr));
}

pub const providers = [_]*const Provider{ &azure.provider, &s3.provider };

pub fn find(url: []const u8) ?*const Provider {
    for (providers) |p| if (std.mem.startsWith(u8, url, p.scheme)) return p;
    return null;
}

pub const EncodeSlash = enum { keep_slash, encode_slash };

/// URI encoding as both providers sign it (SigV4's, and Shared Key's encoded
/// path): unreserved characters pass, the rest become uppercase %XX. A path keeps
/// '/'; a query value encodes it too.
pub fn uriEncode(arena: std.mem.Allocator, s: []const u8, slash: EncodeSlash) ![]const u8 {
    var aw = std.Io.Writer.Allocating.init(arena);
    switch (slash) {
        inline else => |sl| std.Uri.Component.percentEncode(&aw.writer, s, struct {
            fn keep(c: u8) bool {
                return percent.isUnreserved(c) or (sl == .keep_slash and c == '/');
            }
        }.keep) catch return error.OutOfMemory,
    }
    return aw.toOwnedSlice();
}

pub fn isUrl(url: []const u8) bool {
    return find(url) != null;
}

pub const Error = error{NotObjectUrl};

pub fn parse(arena: std.mem.Allocator, url: []const u8) !Object {
    const p = find(url) orelse return Error.NotObjectUrl;
    return p.vtable.parse(arena, url);
}

pub fn listPrefix(arena: std.mem.Allocator, client: *std.http.Client, url: []const u8) ![]const []const u8 {
    const p = find(url) orelse return Error.NotObjectUrl;
    const urls = try p.vtable.list_prefix(arena, client, url);
    if (urls.len == 0) return p.empty_prefix;
    return urls;
}

test "parseError pulls the code and first message line out of a fault body" {
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

test "a listing page gives every item, entities decoded, and nothing else" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = try xml.elementTexts(a, "<Blobs><Blob><Name>p/a.csv</Name></Blob><Blob><Name>p/b.csv</Name></Blob></Blobs><NextMarker/>", "Name");
    try std.testing.expectEqual(@as(usize, 2), names.len);
    try std.testing.expectEqualStrings("p/a.csv", names[0]);
    try std.testing.expectEqualStrings("p/b.csv", names[1]);

    const keys = try xml.elementTexts(a,
        \\<ListBucketResult><Name>lake</Name><Prefix>p/</Prefix><KeyCount>2</KeyCount>
        \\<Contents><Key>p/a.csv</Key><Size>10</Size></Contents>
        \\<Contents><Key>p/b.csv</Key><Size>20</Size></Contents>
        \\</ListBucketResult>
    , "Key");
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("p/b.csv", keys[1]);

    try std.testing.expectEqual(@as(usize, 1), (try xml.elementTexts(a, "<Key>a</Key><Key>b", "Key")).len);
    try std.testing.expectEqual(@as(usize, 0), (try xml.elementTexts(a, "", "Key")).len);
    const amp = try xml.elementTexts(a, "<ListBucketResult><Contents><Key>p/a&amp;b &lt;1&gt;.csv</Key></Contents><Contents><Key><![CDATA[p/c&d.csv]]></Key></Contents></ListBucketResult>", "Key");
    try std.testing.expectEqualStrings("p/a&b <1>.csv", amp[0]);
    try std.testing.expectEqualStrings("p/c&d.csv", amp[1]);
}

test "retry policy: transport failures, 408, 429 and 5xx gateways retry; other 4xx do not" {
    for ([_]u16{ 408, 429, 500, 502, 503, 504 }) |c| try std.testing.expect(retriable(c));
    for ([_]u16{ 200, 201, 206, 400, 401, 403, 404, 409, 412, 501 }) |c| try std.testing.expect(!retriable(c));
    try std.testing.expect(transient(error.ConnectionResetByPeer) and transient(error.HttpConnectionClosing));
    try std.testing.expect(transient(error.ConnectionRefused) and transient(error.EndOfStream));
    try std.testing.expect(!transient(error.S3AuthFailed) and !transient(error.OutOfMemory));
}

test "backoff doubles from 500 ms with up to 50% jitter, capped at 8 s; Retry-After reads delta-seconds" {
    for (0..5) |i| {
        const base = @as(u64, 500) << @intCast(i);
        var above_base = false;
        for (0..64) |seed| {
            var prng = std.Random.DefaultPrng.init(seed);
            const ms = backoffMs(i, prng.random());
            try std.testing.expect(ms >= base and ms <= base + base / 2);
            if (ms > base) above_base = true;
        }
        try std.testing.expect(above_base);
    }
    for ([_]usize{ 5, 6, 12, 40 }) |attempt| {
        for (0..64) |seed| {
            var prng = std.Random.DefaultPrng.init(seed);
            const ms = backoffMs(attempt, prng.random());
            try std.testing.expect(ms >= 8000 and ms <= 12000);
        }
    }
    try std.testing.expectEqual(@as(?u64, 7000), retryAfterMs("7"));
    try std.testing.expectEqual(@as(?u64, 0), retryAfterMs(" 0 "));
    try std.testing.expectEqual(@as(?u64, null), retryAfterMs("Wed, 21 Oct 2015 07:28:00 GMT"));
}

const Scripted = struct {
    codes: []const u16,
    calls: usize = 0,
    retry_after_ms: ?u64 = null,

    var slept_ms: u64 = 0;

    /// 0 fixes the cause itself (`.again`), 1 is a reset connection, 2 a local
    /// non-transient failure, 200 success, anything else that status.
    fn attempt(self: *Scripted) !Verdict {
        const code = self.codes[@min(self.calls, self.codes.len - 1)];
        self.calls += 1;
        return switch (code) {
            0 => .again,
            1 => error.ConnectionResetByPeer,
            2 => error.OutOfMemory,
            200 => .done,
            else => .{ .failed = .{ .code = code, .err = error.Scripted, .retry_after_ms = self.retry_after_ms } },
        };
    }
    fn noSleep(ns: u64) void {
        slept_ms += ns / std.time.ns_per_ms;
    }
    fn abortingSleep(ns: u64) void {
        noSleep(ns);
        driver.requestAbort();
    }
};

test "retry gives up after max_attempts and only ever retries transient failures" {
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep };

    var throttled = Scripted{ .codes = &.{503} };
    try std.testing.expectError(error.Scripted, retry(policy, &throttled, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, max_attempts), throttled.calls);

    var flaky = Scripted{ .codes = &.{ 500, 429, 1, 502, 504, 200 } };
    try std.testing.expectError(error.Scripted, retry(policy, &flaky, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, max_attempts), flaky.calls);

    var recovers = Scripted{ .codes = &.{ 1, 408, 200 } };
    try retry(policy, &recovers, Scripted.attempt);
    try std.testing.expectEqual(@as(usize, 3), recovers.calls);

    var denied = Scripted{ .codes = &.{ 403, 200 } };
    try std.testing.expectError(error.Scripted, retry(policy, &denied, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, 1), denied.calls);

    var broken = Scripted{ .codes = &.{ 2, 200 } };
    try std.testing.expectError(error.OutOfMemory, retry(policy, &broken, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, 1), broken.calls);

    var created = Scripted{ .codes = &.{ 0, 200 } };
    try retry(policy, &created, Scripted.attempt);
    try std.testing.expectEqual(@as(usize, 2), created.calls);
}

test "retry stretches the wait to Retry-After but never sleeps past the budget" {
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep };

    Scripted.slept_ms = 0;
    var told = Scripted{ .codes = &.{ 503, 200 }, .retry_after_ms = 4000 };
    try retry(policy, &told, Scripted.attempt);
    try std.testing.expectEqual(@as(u64, 4000), Scripted.slept_ms);

    Scripted.slept_ms = 0;
    var greedy = Scripted{ .codes = &.{503}, .retry_after_ms = 3600 * 1000 };
    try std.testing.expectError(error.Scripted, retry(policy, &greedy, Scripted.attempt));
    try std.testing.expectEqual(budget_ms, Scripted.slept_ms);
    try std.testing.expectEqual(@as(usize, 2), greedy.calls);

    Scripted.slept_ms = 0;
    var plain = Scripted{ .codes = &.{503} };
    try std.testing.expectError(error.Scripted, retry(policy, &plain, Scripted.attempt));
    try std.testing.expect(Scripted.slept_ms >= 500 + 1000 + 2000 + 4000 and Scripted.slept_ms <= budget_ms);
}

test "retry stops as soon as the run is aborting" {
    defer driver.resetAbort();
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.abortingSleep };

    Scripted.slept_ms = 0;
    var throttled = Scripted{ .codes = &.{503} };
    try std.testing.expectError(error.Scripted, retry(policy, &throttled, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, 1), throttled.calls);
    try std.testing.expect(Scripted.slept_ms <= 100);

    var again = Scripted{ .codes = &.{503} };
    try std.testing.expectError(error.Scripted, retry(policy, &again, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, 1), again.calls);
}

test "exchange surfaces ETag, Retry-After and the body, and a reset is a transient failure" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var srv: FakeServer = undefined;
    try srv.start(&.{
        null,
        FakeServer.reply("503 Slow Down", "Retry-After: 3\r\n", ""),
        FakeServer.reply("200 OK", "ETag: \"abc\"\r\n", "hello"),
    });
    defer srv.stop();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    const url = try srv.url(a, "/k");

    if (exchange(a, &client, .{ .method = .GET, .url = url })) |_| {
        return error.TestExpectedError;
    } else |e| try std.testing.expect(transient(e));
    const busy = try exchange(a, &client, .{ .method = .GET, .url = url });
    try std.testing.expectEqual(@as(u16, 503), busy.code);
    try std.testing.expectEqual(@as(?u64, 3000), busy.retry_after_ms);
    const ok = try exchange(a, &client, .{ .method = .PUT, .url = url, .payload = "body!" });
    try std.testing.expectEqual(@as(u16, 200), ok.code);
    try std.testing.expectEqualStrings("\"abc\"", ok.etag);
    try std.testing.expectEqualStrings("hello", ok.body);
    try std.testing.expectEqualStrings("PUT /k HTTP/1.1", srv.line(2));
}

test "listPages follows the continuation marker and stops on the page without one" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const Pages = struct {
        markers: std.array_list.Managed([]const u8),
        fn page(self: *@This(), marker: []const u8) ![]const u8 {
            try self.markers.append(marker);
            if (marker.len == 0)
                return "<Blobs><Blob><Name>p/a.csv</Name></Blob><Blob><Name>p/b.csv</Name></Blob></Blobs><NextMarker>2!abc</NextMarker>";
            if (std.mem.eql(u8, marker, "2!abc"))
                return "<Blobs><Blob><Name>p/c.csv</Name></Blob></Blobs><NextMarker />";
            return error.UnexpectedMarker;
        }
    };
    var pages = Pages{ .markers = std.array_list.Managed([]const u8).init(a) };
    const names = try listPages(a, .{ .item_tag = "Name", .next_tag = "NextMarker" }, &pages, Pages.page);
    try std.testing.expectEqual(@as(usize, 3), names.len);
    try std.testing.expectEqualStrings("p/a.csv", names[0]);
    try std.testing.expectEqualStrings("p/c.csv", names[2]);
    try std.testing.expectEqual(@as(usize, 2), pages.markers.items.len);
    try std.testing.expectEqualStrings("", pages.markers.items[0]);
    try std.testing.expectEqualStrings("2!abc", pages.markers.items[1]);

    var bad = Pages{ .markers = std.array_list.Managed([]const u8).init(a) };
    const Broken = struct {
        fn page(_: *Pages, _: []const u8) ![]const u8 {
            return error.S3AuthFailed;
        }
    };
    try std.testing.expectError(error.S3AuthFailed, listPages(a, .{ .item_tag = "Key", .next_tag = "NextContinuationToken" }, &bad, Broken.page));
}

test "find dispatches on the scheme alone" {
    try std.testing.expect(find("az://a/c/f.csv") == &azure.provider);
    try std.testing.expect(find("s3://b/k.csv") == &s3.provider);
    try std.testing.expect(find("https://x/y.csv") == null);
    try std.testing.expect(find("/tmp/f.csv") == null);
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectError(Error.NotObjectUrl, parse(ar.allocator(), "/tmp/f.csv"));
}

test "a redirect resolves against its sender, and headers follow only within one origin" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    try std.testing.expectEqualStrings("https://h.io/x/b?q=1", try resolveLocation(a, "https://h.io/x/a?z=2", "b?q=1"));
    try std.testing.expectEqualStrings("https://h.io/b", try resolveLocation(a, "https://h.io/x/a", "/b"));
    try std.testing.expectEqualStrings("https://cdn.io/b", try resolveLocation(a, "https://h.io/x/a", "//cdn.io/b"));
    try std.testing.expectEqualStrings("http://e.io/", try resolveLocation(a, "https://h.io/x/a", "http://e.io/"));
    try std.testing.expect(sameOrigin("https://H.io/a", "https://h.io/b?c"));
    try std.testing.expect(!sameOrigin("https://h.io/a", "http://h.io/a"));
    try std.testing.expect(!sameOrigin("https://h.io/a", "https://h.io:8443/a"));
    try std.testing.expect(!sameOrigin("https://h.io/a", "https://evil.io/a"));
}

test "a redirect to another host drops the request's headers; one within the origin keeps them" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var other: FakeServer = undefined;
    try other.start(&.{FakeServer.reply("200 OK", "", "far")});
    defer other.stop();
    const away = try std.fmt.allocPrint(a, "HTTP/1.1 302 Found\r\nlocation: {s}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n", .{try other.url(a, "/x")});
    var home: FakeServer = undefined;
    try home.start(&.{ FakeServer.reply("302 Found", "location: /next\r\n", ""), FakeServer.reply("200 OK", "", "near"), away });
    defer home.stop();
    var client = http_client.initClient(std.testing.allocator);
    defer client.deinit();
    const auth = [_]std.http.Header{.{ .name = "Authorization", .value = "Bearer s3cret" }};
    const near = try exchange(a, &client, .{ .method = .GET, .url = try home.url(a, "/a"), .headers = &auth });
    try std.testing.expectEqualStrings("near", near.body);
    try std.testing.expect(std.mem.indexOf(u8, home.headOf(1), "s3cret") != null);
    const far = try exchange(a, &client, .{ .method = .GET, .url = try home.url(a, "/b"), .headers = &auth });
    try std.testing.expectEqualStrings("far", far.body);
    try std.testing.expect(std.mem.indexOf(u8, other.headOf(0), "s3cret") == null);
}

test "exchange stops a body at max_body" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var srv: FakeServer = undefined;
    try srv.start(&.{
        FakeServer.reply("200 OK", "", "x" ** 100),
        FakeServer.reply("200 OK", "", "x" ** 100),
    });
    defer srv.stop();
    var client = std.http.Client{ .allocator = std.testing.allocator };
    defer client.deinit();
    const url = try srv.url(a, "/k");

    try std.testing.expectError(error.HttpBodyTooLarge, exchange(a, &client, .{ .method = .GET, .url = url, .max_body = 99 }));
    const ok = try exchange(a, &client, .{ .method = .GET, .url = url, .max_body = 100 });
    try std.testing.expectEqual(@as(usize, 100), ok.body.len);
}
