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
//! Retry: 429/500/503, five attempts, exponential backoff with jitter so parallel
//! lanes throttled at the same instant do not retry in lockstep. Every request is
//! idempotent (Put Block and UploadPart are keyed by block id / part number, the
//! commit is a full replace, GET is a read), so retrying is always safe. A
//! `Verdict.again` (the attempt fixed the cause itself, e.g. created the missing
//! container) spends an attempt but no backoff. `Retry-After` is not honoured
//! because `std.http.Client.fetch` does not surface response headers.
//!
//! The interface is a vtable rather than a comptime generic because the provider
//! is chosen at run time from a URL string: a reader over `az://…` and one over
//! `s3://…` are the same code path holding a different `Object`. A provider is
//! one `Provider` const naming its scheme and five functions; adding one means
//! writing that const and appending it to `providers`. A `Writer` dropped without
//! `finish` is the abort, since staged blocks and uncompleted uploads are
//! invisible to readers; `abort` exists only for destinations where an unfinished
//! write is visible (an SFTP `.part` file). A read of a prefix that lists nothing
//! raises the provider's `empty_prefix` error, as it is almost always a typo.

const std = @import("std");
const azure = @import("azure.zig");
const s3 = @import("s3.zig");

/// Civil date from a day count since the epoch (Howard Hinnant's algorithm).
/// Duplicated from `exec/eval.zig` so this module depends on nothing but std.
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

/// The text of every `<name>…</name>` element, in document order. A flat listing
/// carries item names in exactly one element kind, so a plain tag scan is enough.
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

pub fn parseError(body: []const u8) ?struct { code: []const u8, message: []const u8 } {
    const code = extractTag(body, "Code") orelse return null;
    const msg = extractTag(body, "Message") orelse "";
    return .{ .code = code, .message = std.mem.sliceTo(msg, '\n') };
}

pub fn describe(arena: std.mem.Allocator, code: u16, body: []const u8) ![]const u8 {
    if (parseError(body)) |e|
        return std.fmt.allocPrint(arena, "{s}: {s} (HTTP {d})", .{ e.code, e.message, code });
    return std.fmt.allocPrint(arena, "HTTP {d}", .{code});
}

pub fn retriable(code: u16) bool {
    return code == 429 or code == 500 or code == 503;
}

pub const max_attempts = 5;

pub fn backoffMs(attempt: usize, rand: std.Random) u64 {
    const base = @as(u64, 200) << @intCast(@min(attempt, 5));
    return base + rand.uintLessThan(u64, base / 2 + 1);
}

pub fn jitterPrng() std.Random.DefaultPrng {
    return std.Random.DefaultPrng.init(@bitCast(std.time.milliTimestamp()));
}

pub const Verdict = union(enum) {
    done,
    again,
    failed: struct { code: u16, err: anyerror },
};

pub const Policy = struct {
    rand: std.Random,
    sleep: *const fn (ns: u64) void = std.Thread.sleep,
};

/// Runs `attemptFn(ctx)` until it reports `.done`, giving up after
/// `max_attempts` or on the first non-transient status.
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
        try out.appendSlice(try collectTag(arena, body, spec.item_tag));
        const next = extractTag(body, spec.next_tag) orelse "";
        if (next.len == 0) break;
        marker = try arena.dupe(u8, next);
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
        open_writer: *const fn (ptr: *const anyopaque, arena: std.mem.Allocator, client: *std.http.Client, content_type: []const u8) anyerror!Writer,
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

    pub fn openWriter(self: Object, arena: std.mem.Allocator, client: *std.http.Client, content_type: []const u8) !Writer {
        return self.provider.vtable.open_writer(self.ptr, arena, client, content_type);
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

pub fn isUrl(url: []const u8) bool {
    return find(url) != null;
}

pub fn isPrefix(url: []const u8) bool {
    return isUrl(url) and std.mem.endsWith(u8, url, "/");
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

    try std.testing.expectEqual(@as(usize, 1), (try collectTag(a, "<Key>a</Key><Key>b", "Key")).len);
    try std.testing.expectEqual(@as(usize, 0), (try collectTag(a, "", "Key")).len);
}

test "retry policy: only transient statuses retry; backoff doubles from 200 ms with up to 50% jitter, capped at 6.4 s" {
    try std.testing.expect(retriable(429) and retriable(500) and retriable(503));
    try std.testing.expect(!retriable(403) and !retriable(404) and !retriable(201) and !retriable(200));

    for (0..5) |i| {
        const base = @as(u64, 200) << @intCast(i);
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
            try std.testing.expect(ms >= 6400 and ms <= 9600);
        }
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

    var throttled = Scripted{ .codes = &.{503} };
    try std.testing.expectError(error.Scripted, retry(policy, &throttled, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, max_attempts), throttled.calls);

    var flaky = Scripted{ .codes = &.{ 500, 429, 200 } };
    try retry(policy, &flaky, Scripted.attempt);
    try std.testing.expectEqual(@as(usize, 3), flaky.calls);

    var denied = Scripted{ .codes = &.{ 403, 200 } };
    try std.testing.expectError(error.Scripted, retry(policy, &denied, Scripted.attempt));
    try std.testing.expectEqual(@as(usize, 1), denied.calls);

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
