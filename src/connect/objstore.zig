//! The object-store layer under `azure.zig` and `s3.zig`: everything the two
//! providers share, and the interface consumers (csv, pqdecode, pqwrite) use so
//! they never branch on the URL scheme themselves.
//!
//! Shared pieces: the XML tag scraping both REST APIs need, the error-body
//! parser, the retry policy (429/500/503, five attempts, jittered exponential
//! backoff), the `std.Io.Writer` drain adapter for a streaming upload, and the
//! paginated listing loop. Signing stays per provider — Shared Key and SigV4
//! have nothing in common past "put a header on the request".
//!
//! The interface is a vtable rather than a comptime generic because the
//! provider is chosen at run time from a URL string: a reader over
//! `az://…` and one over `s3://…` are the same code path holding a different
//! `Object`, so the choice has to be a value, not a type parameter. A provider
//! is one `Provider` const naming its scheme and five functions; adding one
//! means writing that const and appending it to `providers`.

const std = @import("std");
const azure = @import("azure.zig");
const s3 = @import("s3.zig");

/// Civil date from a day count since the epoch (Howard Hinnant's algorithm).
/// Duplicated from `exec/eval.zig` rather than imported so this module depends
/// on nothing but std — it is the one piece reachable before any pipeline exists.
pub fn civilFromDays(z0: i64) struct { y: i64, m: u32, d: u32 } {
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

/// The text of the first `<name>…</name>` element, or null when absent.
pub fn extractTag(xml: []const u8, name: []const u8) ?[]const u8 {
    var open_buf: [64]u8 = undefined;
    var close_buf: [64]u8 = undefined;
    const open = std.fmt.bufPrint(&open_buf, "<{s}>", .{name}) catch return null;
    const close = std.fmt.bufPrint(&close_buf, "</{s}>", .{name}) catch return null;
    const s = std.mem.indexOf(u8, xml, open) orelse return null;
    const from = s + open.len;
    const e = std.mem.indexOfPos(u8, xml, from, close) orelse return null;
    return xml[from..e];
}

/// The text of every `<name>…</name>` element, in document order. A flat
/// listing (no delimiter) carries item names in exactly one element kind, so a
/// plain tag scan cannot pick up anything else.
pub fn collectTag(arena: std.mem.Allocator, xml: []const u8, name: []const u8) ![][]const u8 {
    var open_buf: [64]u8 = undefined;
    var close_buf: [64]u8 = undefined;
    const open = try std.fmt.bufPrint(&open_buf, "<{s}>", .{name});
    const close = try std.fmt.bufPrint(&close_buf, "</{s}>", .{name});
    var out = std.array_list.Managed([]const u8).init(arena);
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, xml, pos, open)) |s| {
        const from = s + open.len;
        const e = std.mem.indexOfPos(u8, xml, from, close) orelse break;
        try out.append(try arena.dupe(u8, xml[from..e]));
        pos = e + close.len;
    }
    return out.toOwnedSlice();
}

/// Pulls `<Code>` and `<Message>` out of an error body. Every failure response
/// from either service carries them; without this a caller sees only a bare
/// status and the cause (expired key? clock skew? wrong container?) is guesswork.
pub fn parseError(body: []const u8) ?struct { code: []const u8, message: []const u8 } {
    const code = extractTag(body, "Code") orelse return null;
    const msg = extractTag(body, "Message") orelse "";
    // Azure's message carries RequestId/Time on following lines; the first is enough.
    return .{ .code = code, .message = std.mem.sliceTo(msg, '\n') };
}

/// `Code: Message (HTTP nnn)` — what a user needs to see instead of a status.
pub fn describe(arena: std.mem.Allocator, code: u16, body: []const u8) ![]const u8 {
    if (parseError(body)) |e|
        return std.fmt.allocPrint(arena, "{s}: {s} (HTTP {d})", .{ e.code, e.message, code });
    return std.fmt.allocPrint(arena, "HTTP {d}", .{code});
}

/// Both services throttle with 429/503 under load and return 500 on transient
/// internal faults. Every request the providers make is idempotent — Put Block
/// and UploadPart are keyed by block id / part number, the commit is a full
/// replace, GET is a read — so retrying is always safe.
pub fn retriable(code: u16) bool {
    return code == 429 or code == 500 or code == 503;
}

pub const max_attempts = 5;

/// Exponential backoff with jitter. Jitter matters here specifically: N parallel
/// lanes throttled at the same instant would otherwise retry in lockstep and
/// re-throttle each other.
///
/// Both services send `Retry-After` on 503, which would beat guessing — but
/// `std.http.Client.fetch` does not surface response headers, and dropping to
/// the lower-level request API for one hint is not worth it until throttling is
/// observed in practice.
pub fn backoffMs(attempt: usize, rand: std.Random) u64 {
    const base = @as(u64, 200) << @intCast(@min(attempt, 5));
    return base + rand.uintLessThan(u64, base / 2 + 1);
}

/// The PRNG behind a writer's backoff jitter. Jitter only needs to decorrelate
/// lanes, not be unpredictable.
pub fn jitterPrng() std.Random.DefaultPrng {
    return std.Random.DefaultPrng.init(@bitCast(std.time.milliTimestamp()));
}

/// What one attempt of a retried request decided.
pub const Verdict = union(enum) {
    done,
    /// Go straight round again, no backoff: the attempt fixed the cause itself
    /// (created the missing container) rather than hitting a transient fault.
    again,
    /// Failed with this status. `err` is what the caller gets once the policy
    /// gives up; recording it here keeps the provider's typed error instead of
    /// collapsing everything to one retry failure.
    failed: struct { code: u16, err: anyerror },
};

pub const Policy = struct {
    rand: std.Random,
    /// Injectable so a give-up test does not really wait out the backoff.
    sleep: *const fn (ns: u64) void = std.Thread.sleep,
};

/// Runs `attemptFn(ctx)` until it reports `.done`, giving up after
/// `max_attempts` or on the first non-transient status. `.again` spends an
/// attempt but no backoff.
pub fn retry(policy: Policy, ctx: anytype, comptime attemptFn: anytype) anyerror!void {
    var attempt: usize = 0;
    while (true) : (attempt += 1) {
        switch (try attemptFn(ctx)) {
            .done => return,
            .again => {},
            .failed => |f| {
                if (!retriable(f.code) or attempt + 1 >= max_attempts) return f.err;
                policy.sleep(backoffMs(attempt, policy.rand) * std.time.ns_per_ms);
            },
        }
    }
}

/// `std.Io.Writer.VTable.drain` for a streaming uploader `T` with an
/// `interface: std.Io.Writer` field and `put(*T, []const u8) Error!usize`.
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

/// Where a listing page keeps its item names and the marker for the next page.
pub const ListSpec = struct {
    item_tag: []const u8,
    /// Absent or empty on the last page.
    next_tag: []const u8,
};

/// Collects every item across a paginated listing. `pageFn(ctx, marker)`
/// issues one request — `marker` is empty for the first page — and returns the
/// XML body of a successful response, or the provider's error. Query parameter
/// names and encoding stay with the page function: they are signed, and the two
/// services canonicalise a query string differently.
pub fn listPages(arena: std.mem.Allocator, spec: ListSpec, ctx: anytype, comptime pageFn: anytype) ![][]const u8 {
    var out = std.array_list.Managed([]const u8).init(arena);
    var marker: []const u8 = "";
    while (true) {
        const body: []const u8 = try pageFn(ctx, marker);
        try out.appendSlice(try collectTag(arena, body, spec.item_tag));
        const next = extractTag(body, spec.next_tag) orelse "";
        if (next.len == 0) break;
        marker = try arena.dupe(u8, next);
    }
    return out.toOwnedSlice();
}

// --- interface ----------------------------------------------------------------

/// One URL scheme and the operations consumers need from it.
pub const Provider = struct {
    /// `az://`, `s3://`.
    scheme: []const u8,
    /// Raised for a prefix read that lists nothing: the overwhelmingly common
    /// cause is a mistyped prefix in an otherwise full lake.
    empty_prefix: anyerror,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Parses an object URL against the endpoint configured in the environment.
        parse: *const fn (arena: std.mem.Allocator, url: []const u8) anyerror!Object,
        /// Every object under a prefix URL, as full `<scheme>…` URLs in listing order.
        list_prefix: *const fn (arena: std.mem.Allocator, client: *std.http.Client, url: []const u8) anyerror![]const []const u8,
        /// Signed headers for `method` on the object. `range` is `bytes=a-b` or
        /// empty; it participates in the signature, so it cannot be added later.
        request_headers: *const fn (ptr: *const anyopaque, arena: std.mem.Allocator, method: []const u8, range: []const u8) anyerror![]const std.http.Header,
        /// Maps a failure status and body onto the provider's typed error.
        status_to_error: *const fn (code: u16, body: []const u8) anyerror,
        /// A streaming writer that publishes the object on `finish`.
        open_writer: *const fn (ptr: *const anyopaque, arena: std.mem.Allocator, client: *std.http.Client, content_type: []const u8) anyerror!Writer,
    };
};

/// A parsed object URL bound to its provider.
pub const Object = struct {
    provider: *const Provider,
    /// Absolute request URL, endpoint style already applied.
    url: []const u8,
    /// The provider's parsed form (`azure.Blob`, `s3.Obj`), arena-owned.
    ptr: *const anyopaque,

    pub fn requestHeaders(self: Object, arena: std.mem.Allocator, method: []const u8, range: []const u8) ![]const std.http.Header {
        return self.provider.vtable.request_headers(self.ptr, arena, method, range);
    }

    /// Signed headers for a GET of the whole object.
    pub fn getHeaders(self: Object, arena: std.mem.Allocator) ![]const std.http.Header {
        return self.requestHeaders(arena, "GET", "");
    }

    pub fn statusToError(self: Object, code: u16, body: []const u8) anyerror {
        return self.provider.vtable.status_to_error(code, body);
    }

    pub fn openWriter(self: Object, arena: std.mem.Allocator, client: *std.http.Client, content_type: []const u8) !Writer {
        return self.provider.vtable.open_writer(self.ptr, arena, client, content_type);
    }
};

/// A streaming upload. Rows go to `io`; `finish` commits them into a readable
/// object. Dropping it without `finish` is the abort: staged blocks and
/// uncompleted uploads are invisible to readers.
pub const Writer = struct {
    io: *std.Io.Writer,
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        finish: *const fn (*anyopaque) anyerror!void,
        last_status: *const fn (*anyopaque) ?anyerror,
        last_error: *const fn (*anyopaque) []const u8,
    };

    pub fn finish(self: Writer) !void {
        return self.vtable.finish(self.ptr);
    }

    /// Recovers the error the upload actually hit. Staging runs under
    /// `std.Io.Writer`, whose error set is just `WriteFailed`, so the status
    /// recorded at the point of failure is put back here — otherwise a 403 and a
    /// missing container are the same word to the caller.
    pub fn specific(self: Writer, e: anyerror) anyerror {
        if (e != error.WriteFailed) return e;
        return self.vtable.last_status(self.ptr) orelse e;
    }

    /// The provider's `Code: Message (HTTP nnn)` from the last failure.
    pub fn lastError(self: Writer) []const u8 {
        return self.vtable.last_error(self.ptr);
    }
};

/// Wraps a provider writer — anything with `interface`, `last_error`,
/// `last_status` and `finish` — as a `Writer`.
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
        const vt = Writer.VTable{ .finish = finish, .last_status = lastStatus, .last_error = lastError };
    };
    return .{ .io = &w.interface, .ptr = w, .vtable = &Adapter.vt };
}

/// Casts an `Object.ptr` back to the provider's parsed form.
pub fn cast(comptime T: type, ptr: *const anyopaque) *const T {
    return @ptrCast(@alignCast(ptr));
}

pub const providers = [_]*const Provider{ &azure.provider, &s3.provider };

/// The provider owning `url`'s scheme, or null for a local path or plain http(s).
pub fn find(url: []const u8) ?*const Provider {
    for (providers) |p| if (std.mem.startsWith(u8, url, p.scheme)) return p;
    return null;
}

pub fn isUrl(url: []const u8) bool {
    return find(url) != null;
}

/// A trailing slash means "every object under this prefix", not one object.
pub fn isPrefix(url: []const u8) bool {
    return isUrl(url) and std.mem.endsWith(u8, url, "/");
}

pub const Error = error{NotObjectUrl};

pub fn parse(arena: std.mem.Allocator, url: []const u8) !Object {
    const p = find(url) orelse return Error.NotObjectUrl;
    return p.vtable.parse(arena, url);
}

/// Every object under a prefix URL, or the provider's empty-prefix error.
pub fn listPrefix(arena: std.mem.Allocator, client: *std.http.Client, url: []const u8) ![]const []const u8 {
    const p = find(url) orelse return Error.NotObjectUrl;
    const urls = try p.vtable.list_prefix(arena, client, url);
    if (urls.len == 0) return p.empty_prefix;
    return urls;
}

// --- tests --------------------------------------------------------------------

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

test "collectTag reads every item from a listing page and nothing else" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const names = try collectTag(a, "<Blobs><Blob><Name>p/a.csv</Name></Blob><Blob><Name>p/b.csv</Name></Blob></Blobs><NextMarker/>", "Name");
    try std.testing.expectEqual(@as(usize, 2), names.len);
    try std.testing.expectEqualStrings("p/a.csv", names[0]);
    try std.testing.expectEqualStrings("p/b.csv", names[1]);

    const keys = try collectTag(a,
        \\<ListBucketResult><Name>lake</Name><Prefix>p/</Prefix><KeyCount>2</KeyCount>
        \\<Contents><Key>p/a.csv</Key><Size>10</Size></Contents>
        \\<Contents><Key>p/b.csv</Key><Size>20</Size></Contents>
        \\</ListBucketResult>
    , "Key");
    try std.testing.expectEqual(@as(usize, 2), keys.len);
    try std.testing.expectEqualStrings("p/b.csv", keys[1]);

    // An unterminated element ends the scan rather than yielding garbage.
    try std.testing.expectEqual(@as(usize, 1), (try collectTag(a, "<Key>a</Key><Key>b", "Key")).len);
    try std.testing.expectEqual(@as(usize, 0), (try collectTag(a, "", "Key")).len);
}

test "retry policy: only transient statuses, and jitter never collapses to zero spread" {
    try std.testing.expect(retriable(429) and retriable(500) and retriable(503));
    try std.testing.expect(!retriable(403) and !retriable(404) and !retriable(201) and !retriable(200));

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

const Scripted = struct {
    codes: []const u16,
    calls: usize = 0,
    fn attempt(self: *Scripted) !Verdict {
        const code = self.codes[@min(self.calls, self.codes.len - 1)];
        self.calls += 1;
        if (code == 0) return .again;
        if (code == 200) return .done;
        return .{ .failed = .{ .code = code, .err = error.Scripted } };
    }
    fn noSleep(_: u64) void {}
};

test "retry gives up after max_attempts and only ever retries transient statuses" {
    var prng = std.Random.DefaultPrng.init(0);
    const policy = Policy{ .rand = prng.random(), .sleep = Scripted.noSleep };

    // Throttled forever: every attempt is spent, then the recorded error surfaces.
    var throttled = Scripted{ .codes = &.{503} };
    try std.testing.expectError(error.Scripted, retry(policy, &throttled, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, max_attempts), throttled.calls);

    // Transient then success: stops as soon as an attempt is done.
    var flaky = Scripted{ .codes = &.{ 500, 429, 200 } };
    try retry(policy, &flaky, Scripted.attempt);
    try std.testing.expectEqual(@as(usize, 3), flaky.calls);

    // A 403 is not worth a second request.
    var denied = Scripted{ .codes = &.{ 403, 200 } };
    try std.testing.expectError(error.Scripted, retry(policy, &denied, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, 1), denied.calls);

    // `.again` (container just created) goes straight round without backoff.
    var created = Scripted{ .codes = &.{ 0, 200 } };
    try retry(policy, &created, Scripted.attempt);
    try std.testing.expectEqual(@as(usize, 2), created.calls);
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

    // A provider error on any page aborts the listing.
    var bad = Pages{ .markers = std.array_list.Managed([]const u8).init(a) };
    const Broken = struct {
        fn page(_: *Pages, _: []const u8) ![]const u8 {
            return error.S3AuthFailed;
        }
    };
    try std.testing.expectError(error.S3AuthFailed, listPages(a, .{ .item_tag = "Key", .next_tag = "NextContinuationToken" }, &bad, Broken.page));
}

test "find and isPrefix dispatch on the scheme alone" {
    try std.testing.expect(find("az://a/c/f.csv") == &azure.provider);
    try std.testing.expect(find("s3://b/k.csv") == &s3.provider);
    try std.testing.expect(find("https://x/y.csv") == null);
    try std.testing.expect(find("/tmp/f.csv") == null);
    try std.testing.expect(isPrefix("az://a/c/") and isPrefix("s3://b/dir/"));
    try std.testing.expect(!isPrefix("s3://b/f.csv") and !isPrefix("/tmp/dir/"));
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectError(Error.NotObjectUrl, parse(ar.allocator(), "/tmp/f.csv"));
}
