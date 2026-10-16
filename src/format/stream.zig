//! Opens any path a reader takes as one stream of decoded bytes: a local file, an
//! `http(s)://` URL, an object (`s3://`, `az://`, signed and retried through
//! `objstore.retry`), `sftp://`, `smb://`, a zip member (`data.zip :: member.csv`,
//! or a bare `.zip` of one member) or the process's stdin. A format reader parses
//! `Stream.reader` and never learns where its bytes came from.
//!
//! Compression is chosen by suffix (`.gz`, `.gzip`, `.zst`, `.zstd`) of the name,
//! a zip member's own name rather than the archive's, and decoded through
//! `std.http.Decompress`, the same path HTTP `Content-Encoding` uses. No `.xz`:
//! std's decoder still has the old reader shape and the files are rare.
//! Concatenated gzip members (`pigz`, `bgzip`, appends) read as one stream, where
//! std stops after the first. `reopen` moves to the next file of a folder in the
//! same buffers, so a folder of any length reads in constant memory.

const std = @import("std");
const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const zipsrc = @import("compress/zip.zig");

pub const LINE_BUF = 64 * 1024;

pub const Codec = enum { none, gzip, zstd };

pub fn splitCodec(path: []const u8) struct { codec: Codec, rest: []const u8 } {
    const bare = path[0 .. std.mem.indexOfAny(u8, path, "?#") orelse path.len];
    if (std.ascii.endsWithIgnoreCase(bare, ".gz")) return .{ .codec = .gzip, .rest = bare[0 .. bare.len - 3] };
    if (std.ascii.endsWithIgnoreCase(bare, ".gzip")) return .{ .codec = .gzip, .rest = bare[0 .. bare.len - 5] };
    if (std.ascii.endsWithIgnoreCase(bare, ".zst")) return .{ .codec = .zstd, .rest = bare[0 .. bare.len - 4] };
    if (std.ascii.endsWithIgnoreCase(bare, ".zstd")) return .{ .codec = .zstd, .rest = bare[0 .. bare.len - 5] };
    return .{ .codec = .none, .rest = bare };
}

pub const ArchiveRef = struct { archive: []const u8, member: ?[]const u8 };

pub fn splitArchive(path: []const u8) ?ArchiveRef {
    const at = std.mem.indexOf(u8, path, "::") orelse {
        if (isArchive(path)) return .{ .archive = path, .member = null };
        return null;
    };
    const archive = std.mem.trim(u8, path[0..at], " \t");
    const member = std.mem.trim(u8, path[at + 2 ..], " \t");
    if (archive.len == 0) return null;
    return .{ .archive = archive, .member = if (member.len == 0) null else member };
}

pub fn isArchive(path: []const u8) bool {
    const bare = path[0 .. std.mem.indexOfAny(u8, path, "?#") orelse path.len];
    return std.ascii.endsWithIgnoreCase(bare, ".zip");
}

pub fn dataName(path: []const u8) []const u8 {
    const inner = if (splitArchive(path)) |a| (a.member orelse a.archive) else path;
    return splitCodec(inner).rest;
}

/// Whether `path` is fetched over HTTP: an `http(s)://` URL or an object-store one.
pub fn isUrl(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://") or objstore.isRemote(path);
}

/// Not `ContentEncoding.minBufferCapacity()`, which gives zstd only its window:
/// `zstd.Decompress` needs the window plus `block_size_max`, or fails the stream
/// with `OutputBufferUndersize`, as a real 2.6 MB `.csv.zst` did.
pub fn codecBuffer(codec: Codec) usize {
    return switch (codec) {
        .none => 0,
        .gzip => std.compress.flate.max_window_len,
        .zstd => std.compress.zstd.default_window_len + std.compress.zstd.block_size_max,
    };
}

pub const Gunzip = struct {
    src: *std.Io.Reader,
    dec: std.compress.flate.Decompress,
    window: []u8,
    interface: std.Io.Reader,

    pub fn init(self: *Gunzip, src: *std.Io.Reader, window: []u8, buf: []u8) void {
        self.* = .{
            .src = src,
            .dec = .init(src, .gzip, window),
            .window = window,
            .interface = .{ .vtable = &.{ .stream = streamFn }, .buffer = buf, .seek = 0, .end = 0 },
        };
    }

    fn streamFn(r: *std.Io.Reader, w: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *Gunzip = @fieldParentPtr("interface", r);
        while (true) {
            return self.dec.reader.stream(w, limit) catch |e| switch (e) {
                error.EndOfStream => {
                    _ = self.src.peekByte() catch |e2| return switch (e2) {
                        error.EndOfStream => error.EndOfStream,
                        error.ReadFailed => error.ReadFailed,
                    };
                    self.dec = .init(self.src, .gzip, self.window);
                    continue;
                },
                else => |x| return x,
            };
        }
    }
};

pub const Stream = struct {
    reader: *std.Io.Reader,
    name: []const u8,
    codec: Codec = .none,
    arena: std.mem.Allocator,
    backend: Backend,
    read_buf: [LINE_BUF]u8 = undefined,
    codec_state: std.http.Decompress = undefined,
    gunzip: Gunzip = undefined,
    codec_bufs: ?CodecBufs = null,

    const CodecBufs = struct { window: []u8, buf: []u8 };

    const Backend = union(enum) {
        file: FileBackend,
        borrowed: FileBackend,
        http: *HttpFetch,
        member: *zipsrc.Member,
        sftp: *sftp.Stream,
        smb: *smb.Stream,
    };
    const FileBackend = struct {
        file: std.fs.File,
        fr: std.fs.File.Reader,
    };
    const HttpFetch = struct {
        client: std.http.Client,
        req: std.http.Client.Request,
        response: std.http.Client.Response,
        decompress: std.http.Decompress = undefined,
        redirect_buf: [8 * 1024]u8 = undefined,
        transfer_buf: [LINE_BUF]u8 = undefined,
    };

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*Stream {
        const self = try arena.create(Stream);
        self.* = .{ .arena = arena, .reader = undefined, .name = path, .backend = undefined };
        try self.attach(path);
        try self.decode();
        return self;
    }

    /// The process's stdin, read as it arrives and left open on `close`.
    pub fn openStdin(arena: std.mem.Allocator) !*Stream {
        return openBorrowed(arena, std.fs.File.stdin(), "stdin");
    }

    fn openBorrowed(arena: std.mem.Allocator, file: std.fs.File, name: []const u8) !*Stream {
        const self = try arena.create(Stream);
        self.* = .{ .arena = arena, .reader = undefined, .name = name, .backend = .{ .borrowed = .{ .file = file, .fr = undefined } } };
        self.backend.borrowed.fr = file.readerStreaming(&self.read_buf);
        self.reader = &self.backend.borrowed.fr.interface;
        return self;
    }

    /// Closes the current file and opens `path` in its place, in the same buffers.
    /// The current one stays open until `path` has opened, so `close` after a failure
    /// still closes exactly one.
    pub fn reopen(self: *Stream, path: []const u8) !void {
        const old = self.backend;
        try self.attach(path);
        closeBackend(old);
        try self.decode();
    }

    pub fn close(self: *Stream) void {
        closeBackend(self.backend);
    }

    fn closeBackend(backend: Backend) void {
        switch (backend) {
            .file => |f| f.file.close(),
            .borrowed => {},
            .http => |hf| {
                hf.req.deinit();
                hf.client.deinit();
            },
            .member => |m| m.close(),
            .sftp => |st| st.close(),
            .smb => |st| st.close(),
        }
    }

    fn attach(self: *Stream, path: []const u8) !void {
        if (splitArchive(path)) |ar| {
            const m = try zipsrc.openMember(self.arena, ar.archive, ar.member);
            self.backend = .{ .member = m };
            self.reader = m.reader;
            self.name = m.name;
        } else if (sftp.isUrl(path)) {
            const st = try sftp.Stream.open(self.arena, path);
            self.backend = .{ .sftp = st };
            self.reader = &st.interface;
            self.name = path;
        } else if (smb.isUrl(path)) {
            const st = try smb.Stream.open(self.arena, path);
            self.backend = .{ .smb = st };
            self.reader = &st.interface;
            self.name = path;
        } else if (isUrl(path)) {
            const hf = try self.openHttp(path);
            self.backend = .{ .http = hf };
            self.name = path;
        } else {
            const file = try std.fs.cwd().openFile(path, .{});
            self.backend = .{ .file = .{ .file = file, .fr = undefined } };
            self.backend.file.fr = file.reader(&self.read_buf);
            self.reader = &self.backend.file.fr.interface;
            self.name = path;
        }
    }

    fn decode(self: *Stream) !void {
        self.codec = splitCodec(self.name).codec;
        switch (self.codec) {
            .none => {},
            .gzip => {
                const b = self.codec_bufs orelse blk: {
                    const nb = CodecBufs{ .window = try self.arena.alloc(u8, codecBuffer(.gzip)), .buf = try self.arena.alloc(u8, LINE_BUF) };
                    self.codec_bufs = nb;
                    break :blk nb;
                };
                self.gunzip.init(self.reader, b.window, b.buf);
                self.reader = &self.gunzip.interface;
            },
            .zstd => {
                const cbuf = try self.arena.alloc(u8, codecBuffer(.zstd));
                self.reader = std.http.Decompress.init(&self.codec_state, self.reader, cbuf, .zstd);
            },
        }
    }

    /// GETs `url`, signed when it names an object store, and reads its body decoded
    /// per its Content-Encoding. A failed TLS handshake is retried once after
    /// repairing the CA bundle for that host.
    fn openHttp(self: *Stream, url: []const u8) !*HttpFetch {
        const arena = self.arena;
        const hf = try arena.create(HttpFetch);
        hf.* = .{ .client = http_client.initClient(arena), .req = undefined, .response = undefined };
        errdefer hf.client.deinit();
        var req_url = url;
        var extra: []const std.http.Header = &.{};
        if (objstore.isUrl(url)) {
            const obj = try objstore.parse(arena, url);
            req_url = obj.url;
            extra = try obj.getHeaders(arena);
        }
        const uri = std.Uri.parse(req_url) catch return error.InvalidUrl;
        var prng = objstore.jitterPrng();
        var opening: HttpOpen = .{ .arena = arena, .hf = hf, .uri = uri, .extra = extra };
        try objstore.retry(.{ .rand = prng.random() }, &opening, HttpOpen.attempt);
        errdefer hf.req.deinit();
        const ce = hf.response.head.content_encoding;
        if (ce == .compress) return error.UnsupportedCompressionMethod;
        const win = switch (ce) {
            .zstd => codecBuffer(.zstd),
            .gzip, .deflate => codecBuffer(.gzip),
            .compress, .identity => 0,
        };
        const dbuf: []u8 = if (win > 0) try arena.alloc(u8, win) else &.{};
        self.reader = hf.response.readerDecompressing(&hf.transfer_buf, &hf.decompress, dbuf);
        return hf;
    }

    const HttpOpen = struct {
        arena: std.mem.Allocator,
        hf: *HttpFetch,
        uri: std.Uri,
        extra: []const std.http.Header,

        /// One try at a GET's response head, retried by `objstore.retry` until a 200
        /// arrives; a failed try drops its connection, so the next one dials afresh.
        fn attempt(self: *HttpOpen) !objstore.Verdict {
            const hf = self.hf;
            startHttp(hf, self.uri, self.extra) catch |e| switch (e) {
                error.TlsInitializationFailed => {
                    const h = http_client.uriHost(self.uri) orelse return e;
                    if (!http_client.repairBundle(self.arena, &hf.client.ca_bundle, h, self.uri.port orelse 443)) return e;
                    hf.client.next_https_rescan_certs = false;
                    try startHttp(hf, self.uri, self.extra);
                },
                else => return e,
            };
            const code = @intFromEnum(hf.response.head.status);
            if (code == 200) return .done;
            if (hf.req.connection) |c| c.closing = true;
            hf.req.deinit();
            return .{ .failed = .{ .code = code, .err = http_client.statusError(code) } };
        }
    };

    fn startHttp(hf: *HttpFetch, uri: std.Uri, extra: []const std.http.Header) !void {
        hf.req = try hf.client.request(.GET, uri, .{ .extra_headers = extra });
        errdefer {
            if (hf.req.connection) |c| c.closing = true;
            hf.req.deinit();
        }
        try hf.req.sendBodiless();
        hf.response = try hf.req.receiveHead(&hf.redirect_buf);
    }
};

/// Drops the pages wholly inside `bytes`, which must lie in a file mapping, from the
/// process, so what was read once stops counting as resident; touched again, they
/// fault back in from the file (anonymous memory would come back zeroed).
pub fn release(bytes: []const u8) void {
    const page = std.heap.page_size_min;
    const lo = std.mem.alignForward(usize, @intFromPtr(bytes.ptr), page);
    const hi = std.mem.alignBackward(usize, @intFromPtr(bytes.ptr) + bytes.len, page);
    if (hi <= lo) return;
    const at: [*]align(page) u8 = @ptrFromInt(lo);
    std.posix.madvise(at, hi - lo, std.posix.MADV.DONTNEED) catch {};
}

test "splitCodec / splitArchive / dataName walk the container chain" {
    try std.testing.expectEqual(Codec.gzip, splitCodec("a/orders.csv.gz").codec);
    try std.testing.expectEqualStrings("a/orders.csv", splitCodec("a/orders.csv.gz").rest);
    try std.testing.expectEqual(Codec.zstd, splitCodec("x.csv.ZST").codec);
    try std.testing.expectEqual(Codec.none, splitCodec("x.csv").codec);
    try std.testing.expectEqual(Codec.gzip, splitCodec("https://h/x.csv.gz?sig=1").codec);

    try std.testing.expect(splitArchive("plain.csv") == null);
    const one = splitArchive("inf.zip").?;
    try std.testing.expectEqualStrings("inf.zip", one.archive);
    try std.testing.expect(one.member == null);
    const two = splitArchive("d/inf.zip :: inner/data.csv").?;
    try std.testing.expectEqualStrings("d/inf.zip", two.archive);
    try std.testing.expectEqualStrings("inner/data.csv", two.member.?);
    try std.testing.expectEqualStrings("a.csv", splitArchive("i.zip::a.csv").?.member.?);

    try std.testing.expectEqualStrings("inner/data.csv", dataName("d/inf.zip :: inner/data.csv"));
    try std.testing.expectEqualStrings("rows.csv", dataName("rows.csv.gz"));
    try std.testing.expectEqualStrings("m.csv", dataName("a.zip :: m.csv.gz"));
    try std.testing.expect(splitArchive("s3://bucket/key.csv") == null);
}

test "Stream.open reads a plain file as is and a .gz file decoded, and reopen moves between them" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "rows.csv", .data = "id,name\n1,alpha\n2,beta\n3,gamma\n" });
    try tmp.dir.writeFile(.{ .sub_path = "rows.csv.gz", .data = @embedFile("testdata/rows.csv.gz") });
    const plain = try tmp.dir.realpathAlloc(a, "rows.csv");
    const gz = try tmp.dir.realpathAlloc(a, "rows.csv.gz");

    const s = try Stream.open(a, plain);
    defer s.close();
    try std.testing.expectEqual(Codec.none, s.codec);
    try std.testing.expectEqualStrings(plain, s.name);
    const raw = try s.reader.allocRemaining(a, .unlimited);
    try std.testing.expectEqualStrings("id,name\n1,alpha\n2,beta\n3,gamma\n", raw);

    try s.reopen(gz);
    try std.testing.expectEqual(Codec.gzip, s.codec);
    const unzipped = try s.reader.allocRemaining(a, .unlimited);
    try std.testing.expect(std.mem.indexOf(u8, unzipped, "gamma") != null);

    const g = try Stream.open(a, gz);
    defer g.close();
    try std.testing.expectEqualStrings(unzipped, try g.reader.allocRemaining(a, .unlimited));
}

test "Stream.open keeps the error of a missing file" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    try std.testing.expectError(error.FileNotFound, Stream.open(ar.allocator(), "/nonexistent/basalt/none.csv"));
}

test "openStdin reads a borrowed fd and leaves it open on close" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    _ = &Stream.openStdin;
    const fds = try std.posix.pipe();
    const rd: std.fs.File = .{ .handle = fds[0] };
    defer rd.close();
    {
        const wr: std.fs.File = .{ .handle = fds[1] };
        defer wr.close();
        try wr.writeAll("a,b\n1,2\n");
    }
    const s = try Stream.openBorrowed(a, rd, "stdin");
    try std.testing.expectEqualStrings("a,b\n1,2\n", try s.reader.allocRemaining(a, .unlimited));
    s.close();
    var one: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try rd.read(&one));
}
