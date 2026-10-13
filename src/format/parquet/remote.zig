//! A Parquet file's bytes: in memory, a local file, or ranged reads over HTTP, an
//! object store, SFTP or SMB.

const Error = @import("read.zig").Error;
const http_client = @import("../../net/http_client.zig");
const isRemote = @import("folder.zig").isRemote;
const objstore = @import("../../store/objstore.zig");
const sftp = @import("../../store/sftp.zig");
const smb = @import("../../store/smb.zig");
const std = @import("std");
const testing = std.testing;

pub const Bytes = union(enum) {
    memory: []const u8,
    file: struct { f: std.fs.File, size: u64 },
    remote: *Remote,
    sftp: *sftp.File,
    smb: *smb.File,

    pub fn open(arena: std.mem.Allocator, path: []const u8) !Bytes {
        if (sftp.isUrl(path)) return .{ .sftp = try sftp.File.open(arena, path) };
        if (smb.isUrl(path)) return .{ .smb = try smb.File.open(arena, path) };
        if (isRemote(path)) return .{ .remote = try Remote.open(arena, path) };
        const f = try std.fs.cwd().openFile(path, .{});
        errdefer f.close();
        return .{ .file = .{ .f = f, .size = (try f.stat()).size } };
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
                if (off + len > m.len) return Error.CorruptParquetPage;
                return m[@intCast(off)..][0..len];
            },
            .file => |x| {
                if (off + len > x.size) return Error.CorruptParquetPage;
                const buf = try arena.alloc(u8, len);
                const n = try x.f.preadAll(buf, off);
                if (n != len) return Error.CorruptParquetPage;
                return buf;
            },
            .remote => |r| {
                if (off + len > r.total) return Error.CorruptParquetPage;
                return r.read(arena, off, len);
            },
            .sftp => |f| {
                if (off + len > f.size) return Error.CorruptParquetPage;
                return f.read(arena, off, len) catch |e| switch (e) {
                    error.EndOfStream => Error.CorruptParquetPage,
                    else => e,
                };
            },
            .smb => |f| {
                if (off + len > f.size) return Error.CorruptParquetPage;
                return f.read(arena, off, len) catch |e| switch (e) {
                    error.EndOfStream => Error.CorruptParquetPage,
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
        const client = try arena.create(std.http.Client);
        client.* = http_client.initClient(arena);
        const self = try arena.create(Remote);
        self.* = .{ .arena = arena, .client = client, .url = path, .total = 0 };
        if (objstore.isUrl(path)) {
            const o = try objstore.parse(arena, path);
            self.object = o;
            self.url = o.url;
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
            if (off + len > w.len) return Error.CorruptParquetPage;
            return w[@intCast(off)..][0..len];
        }

        const hdr = try std.fmt.allocPrint(arena, "bytes={d}-{d}", .{ off, off + len - 1 });
        const res = try self.send(arena, .GET, hdr);
        switch (res.code) {
            206 => {
                if (res.body.len != len) return Error.CorruptParquetPage;
                return res.body;
            },
            200 => {
                const kept = try self.arena.dupe(u8, res.body);
                self.whole = kept;
                if (off + len > kept.len) return Error.CorruptParquetPage;
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
                if (!self.repair()) return e;
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
                const resp = try objstore.exchange(c.arena, c.r.client, .{ .method = c.method, .url = c.r.url, .headers = extra });
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

    fn repair(self: *Remote) bool {
        if (self.repaired) return false;
        self.repaired = true;
        const uri = std.Uri.parse(self.url) catch return false;
        const h = http_client.uriHost(uri) orelse return false;
        if (!http_client.repairBundle(self.client.allocator, &self.client.ca_bundle, h, uri.port orelse 443)) return false;
        self.client.next_https_rescan_certs = false;
        return true;
    }
};

test "a Bytes range refuses to read past the end" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const src = Bytes{ .memory = "0123456789" };
    try testing.expectEqualStrings("234", try src.range(ar.allocator(), 2, 3));
    try testing.expectError(Error.CorruptParquetPage, src.range(ar.allocator(), 8, 5));
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
    try testing.expectError(Error.CorruptParquetPage, r.read(a, 99992, 8));
    try testing.expectError(Error.CorruptParquetPage, r.read(a, 8, 5));
}

fn noSleep(_: u64) void {}

test "a ranged read replays a 503, a reset and a 504 before the 206" {
    var ar = std.heap.ArenaAllocator.init(testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const reply = objstore.FakeServer.reply;
    var srv: objstore.FakeServer = undefined;
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
