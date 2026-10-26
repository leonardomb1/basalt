//! A file's bytes read by range, wherever it is: in memory, a local file, or ranged
//! reads over HTTP, an object store, SFTP or SMB. A range past the end is
//! `error.ByteRangeOutOfFile`. Parquet reads its footer and column chunks this way,
//! and `read_bytes` any range a query names.

const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const scheme = @import("../store/scheme.zig");
const http_fake = @import("../testing/http_fake.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const std = @import("std");
const testing = std.testing;

/// Whether `path` is read over HTTP: a URL or an object-store path.
pub fn isRemote(path: []const u8) bool {
    return scheme.of(path).has(.http_ranged);
}

/// Whether `len` bytes at `off` lie within `total`, without the sum that a huge
/// offset from a file's own header would wrap.
pub fn fits(off: u64, len: u64, total: u64) bool {
    return off <= total and len <= total - off;
}

pub const Bytes = union(enum) {
    memory: []const u8,
    file: struct { f: std.fs.File, size: u64 },
    remote: *Remote,
    sftp: *sftp.File,
    smb: *smb.File,

    pub fn open(arena: std.mem.Allocator, path: []const u8) !Bytes {
        return openSized(arena, path, null);
    }

    /// `open`, with the size already known from an earlier open: a remote file is
    /// then not asked for it again.
    pub fn openSized(arena: std.mem.Allocator, path: []const u8, known: ?u64) !Bytes {
        if (sftp.isUrl(path)) return .{ .sftp = try sftp.File.open(arena, path) };
        if (smb.isUrl(path)) return .{ .smb = try smb.File.open(arena, path) };
        if (isRemote(path)) return .{ .remote = try Remote.openSized(arena, path, known) };
        const f = try std.fs.cwd().openFile(path, .{});
        errdefer f.close();
        return .{ .file = .{ .f = f, .size = (try f.stat()).size } };
    }

    /// Whether each read is a request over a network, so a reader should ask for
    /// few large ranges rather than many small ones.
    pub fn coalesces(self: Bytes) bool {
        return switch (self) {
            .memory, .file => false,
            .remote, .sftp, .smb => true,
        };
    }

    /// Whether ranges are read as ranges: false once a server answered a range
    /// request with the whole file, which a second reader would download again.
    pub fn rangesHonoured(self: Bytes) bool {
        return switch (self) {
            .remote => |r| r.whole == null,
            else => true,
        };
    }

    pub fn size(self: Bytes) u64 {
        return switch (self) {
            .memory => |m| m.len,
            .file => |x| x.size,
            .remote => |r| r.total,
            .sftp => |f| f.size,
            .smb => |f| f.size,
        };
    }

    /// Owned by `arena` for file and remote sources, borrowed for memory; read-only.
    pub fn range(self: Bytes, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        switch (self) {
            .memory => |m| {
                if (!fits(off, len, m.len)) return error.ByteRangeOutOfFile;
                return m[@intCast(off)..][0..len];
            },
            .file => |x| {
                if (!fits(off, len, x.size)) return error.ByteRangeOutOfFile;
                const buf = try arena.alloc(u8, len);
                const n = try x.f.preadAll(buf, off);
                if (n != len) return error.ByteRangeOutOfFile;
                return buf;
            },
            .remote => |r| {
                if (!fits(off, len, r.total)) return error.ByteRangeOutOfFile;
                return r.read(arena, off, len);
            },
            .sftp => |f| {
                if (!fits(off, len, f.size)) return error.ByteRangeOutOfFile;
                return f.read(arena, off, len) catch |e| switch (e) {
                    error.EndOfStream => error.ByteRangeOutOfFile,
                    else => e,
                };
            },
            .smb => |f| {
                if (!fits(off, len, f.size)) return error.ByteRangeOutOfFile;
                return f.read(arena, off, len) catch |e| switch (e) {
                    error.EndOfStream => error.ByteRangeOutOfFile,
                    else => e,
                };
            },
        }
    }

    pub fn close(self: Bytes) void {
        switch (self) {
            .memory => {},
            .file => |x| x.f.close(),
            .remote => |r| r.client.deinit(),
            .sftp => |f| f.close(),
            .smb => |f| f.close(),
        }
    }
};

pub const Remote = struct {
    arena: std.mem.Allocator,
    client: *std.http.Client,
    url: []const u8,
    object: ?objstore.Object = null,
    total: u64,
    whole: ?[]const u8 = null,
    repaired: bool = false,
    sleep: objstore.Sleep = std.Thread.sleep,

    /// Sizes the object by HEAD; without a length it fetches the whole object once.
    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Remote {
        return openSized(arena, path, null);
    }

    /// `open`, taking the size when it is `known` rather than asking for it.
    pub fn openSized(arena: std.mem.Allocator, path: []const u8, known: ?u64) !*Remote {
        const client = try arena.create(std.http.Client);
        client.* = http_client.initClient(arena);
        const self = try arena.create(Remote);
        self.* = .{ .arena = arena, .client = client, .url = path, .total = 0 };
        if (objstore.isUrl(path)) {
            const o = try objstore.parse(arena, path);
            self.object = o;
            self.url = o.url;
        }

        if (known) |n| {
            self.total = n;
            return self;
        }
        if (self.contentLength(arena)) |n| {
            self.total = n;
        } else |e| {
            if (objstore.transient(e)) return e;
            const body = try self.fetchWhole(arena);
            self.whole = body;
            self.total = body.len;
        }
        return self;
    }

    /// Slices `whole` once a 200 supplied it, never past its real length, which may
    /// disagree with HEAD's.
    pub fn read(self: *Remote, arena: std.mem.Allocator, off: u64, len: usize) ![]const u8 {
        if (len == 0) return "";
        if (self.whole) |w| {
            if (!fits(off, len, w.len)) return error.ByteRangeOutOfFile;
            return w[@intCast(off)..][0..len];
        }

        const hdr = try std.fmt.allocPrint(arena, "bytes={d}-{d}", .{ off, off + len - 1 });
        const res = try self.send(arena, .GET, hdr);
        switch (res.code) {
            206 => {
                if (res.body.len != len) return error.ByteRangeOutOfFile;
                return res.body;
            },
            200 => {
                const kept = try self.arena.dupe(u8, res.body);
                self.whole = kept;
                if (!fits(off, len, kept.len)) return error.ByteRangeOutOfFile;
                return kept[@intCast(off)..][0..len];
            },
            else => return self.statusError(res.code, res.body),
        }
    }

    fn fetchWhole(self: *Remote, arena: std.mem.Allocator) ![]const u8 {
        const res = try self.send(arena, .GET, "");
        if (res.code != 200) return self.statusError(res.code, res.body);
        return res.body;
    }

    /// A failed TLS handshake is retried once after repairing the CA bundle.
    fn send(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) !objstore.Response {
        return self.retrying(arena, method, range_hdr) catch |e| switch (e) {
            error.TlsInitializationFailed => {
                if (!http_client.repairOnce(self.client, &self.repaired, self.url)) return e;
                return self.retrying(arena, method, range_hdr);
            },
            else => e,
        };
    }

    /// Replays a transport failure or transient status under the object-store retry
    /// policy, signing each attempt afresh; any other status is the caller's to read.
    /// Each attempt has its own buffer, so a failed one's partial body is never
    /// prepended to the next.
    fn retrying(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) !objstore.Response {
        var out: objstore.Response = undefined;
        var prng = objstore.jitterPrng();
        const Ctx = struct { r: *Remote, arena: std.mem.Allocator, method: std.http.Method, range_hdr: []const u8, out: *objstore.Response };
        try objstore.retry(.{ .rand = prng.random(), .sleep = self.sleep }, Ctx{
            .r = self,
            .arena = arena,
            .method = method,
            .range_hdr = range_hdr,
            .out = &out,
        }, struct {
            fn attempt(c: Ctx) !objstore.Verdict {
                const extra = try c.r.headers(c.arena, c.method, c.range_hdr);
                const resp = try objstore.exchange(c.arena, c.r.client, .{ .method = c.method, .url = c.r.url, .headers = extra, .identity = true });
                c.out.* = resp;
                if (objstore.retriable(resp.code)) return resp.failed(c.r.statusError(resp.code, resp.body));
                return .done;
            }
        }.attempt);
        return out;
    }

    pub fn headers(
        self: *Remote,
        arena: std.mem.Allocator,
        method: std.http.Method,
        range_hdr: []const u8,
    ) ![]const std.http.Header {
        if (self.object) |o| {
            const verb = if (method == .HEAD) "HEAD" else "GET";
            return o.requestHeaders(arena, verb, range_hdr);
        }
        if (range_hdr.len == 0) return &.{};
        return arena.dupe(std.http.Header, &.{.{ .name = "Range", .value = range_hdr }});
    }

    fn contentLength(self: *Remote, arena: std.mem.Allocator) !u64 {
        const r = try self.send(arena, .HEAD, "");
        if (r.code != 200) return error.HeadUnsupported;
        return r.content_length orelse error.HeadUnsupported;
    }

    pub fn statusError(self: *Remote, code: u16, body: []const u8) anyerror {
        if (self.object) |o| return o.statusToError(code, body);
        return http_client.statusError(code);
    }
};

test "remote paths are recognised, local ones left alone" {
    try testing.expect(isRemote("https://host/a.parquet"));
    try testing.expect(isRemote("http://host/a.parquet"));
    try testing.expect(isRemote("az://acct/ctr/a.parquet"));
    try testing.expect(!isRemote("/data/a.parquet"));
    try testing.expect(!isRemote("a.parquet"));
}

test "an HTTP:// URL in capitals is fetched, not opened as a local file" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var srv: http_fake.RangeServer = undefined;
    try srv.start("0123456789", false);
    defer srv.stop();
    const lower = try srv.url(a, "/x.parquet");
    const url = try std.mem.concat(a, u8, &.{ "HTTP", lower["http".len..] });
    try testing.expect(isRemote(url));
    const r = try Remote.open(a, url);
    try testing.expectEqual(@as(u64, 10), r.total);
    try testing.expectEqualStrings("3456", try r.read(a, 3, 4));
}

test "a Bytes range refuses to read past the end" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const src = Bytes{ .memory = "0123456789" };
    try testing.expectEqualStrings("234", try src.range(ar.allocator(), 2, 3));
    try testing.expectError(error.ByteRangeOutOfFile, src.range(ar.allocator(), 8, 5));
    try testing.expectEqual(@as(u64, 10), src.size());
}

test "a remote whole-body read refuses to slice past the body it was given" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var client = http_client.initClient(a);
    defer client.deinit();
    var r = Remote{
        .arena = a,
        .client = &client,
        .url = "http://example/x.parquet",
        .total = 100000,
        .whole = "0123456789",
    };
    try testing.expectEqualStrings("234", try r.read(a, 2, 3));
    try testing.expectError(error.ByteRangeOutOfFile, r.read(a, 99992, 8));
    try testing.expectError(error.ByteRangeOutOfFile, r.read(a, 8, 5));
}

fn noSleep(_: u64) void {}

test "a ranged read replays a 503, a reset and a 504 before the 206" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const reply = http_fake.FakeServer.reply;
    var srv: http_fake.FakeServer = undefined;
    try srv.start(&.{
        reply("503 Service Unavailable", "", ""),
        null,
        reply("504 Gateway Timeout", "", ""),
        reply("206 Partial Content", "content-range: bytes 2-5/100\r\n", "2345"),
        reply("404 Not Found", "", ""),
    });
    defer srv.stop();
    var client = std.http.Client{ .allocator = testing.allocator };
    defer client.deinit();
    var r = Remote{ .arena = a, .client = &client, .url = try srv.url(a, "/x.parquet"), .total = 100, .sleep = noSleep };
    try testing.expectEqualStrings("2345", try r.read(a, 2, 4));
    try testing.expectEqual(@as(usize, 4), srv.requests());
    try testing.expectError(error.HttpNotFound, r.read(a, 2, 4));
    try testing.expectEqual(@as(usize, 5), srv.requests());
}

test "a ranged read replays a 408 and a 507, fails a 501 at once, and a 408 that outlasts the retries is busy" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const reply = http_fake.FakeServer.reply;
    var srv: http_fake.FakeServer = undefined;
    try srv.start(&.{
        reply("408 Request Timeout", "", ""),
        reply("507 Insufficient Storage", "", ""),
        reply("206 Partial Content", "content-range: bytes 2-5/100\r\n", "2345"),
        reply("501 Not Implemented", "", ""),
        reply("408 Request Timeout", "", ""),
        reply("408 Request Timeout", "", ""),
        reply("408 Request Timeout", "", ""),
        reply("408 Request Timeout", "", ""),
        reply("408 Request Timeout", "", ""),
    });
    defer srv.stop();
    var client = std.http.Client{ .allocator = testing.allocator };
    defer client.deinit();
    var r = Remote{ .arena = a, .client = &client, .url = try srv.url(a, "/x.parquet"), .total = 100, .sleep = noSleep };
    try testing.expectEqualStrings("2345", try r.read(a, 2, 4));
    try testing.expectEqual(@as(usize, 3), srv.requests());
    try testing.expectError(error.HttpRequestFailed, r.read(a, 2, 4));
    try testing.expectEqual(@as(usize, 4), srv.requests());
    try testing.expectError(error.HttpServerBusy, r.read(a, 2, 4));
    try testing.expectEqual(@as(usize, 9), srv.requests());
}

test "a range whose offset plus length would wrap is out of the file, never a slice past it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const max = std.math.maxInt(u64);
    try std.testing.expect(fits(2, 3, 5));
    try std.testing.expect(!fits(2, 4, 5));
    try std.testing.expect(!fits(max, 2, 5));
    try std.testing.expect(!fits(4, max, 5));
    const b = Bytes{ .memory = "hello" };
    try std.testing.expectError(error.ByteRangeOutOfFile, b.range(ar.allocator(), max - 1, 4));
    try std.testing.expectEqualStrings("llo", try b.range(ar.allocator(), 2, 3));
}
