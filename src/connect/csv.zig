//! Minimal CSV source and sink. The source reads a header row into an all-string
//! schema (empty field = null) and produces batches of string columns; the sink
//! writes a header then serializes each batch. RFC-ish quoting: fields containing
//! the delimiter, a quote or a newline are double-quoted with `""` escaping.
//!
//! The delimiter and the source encoding are a `Dialect`, because the CSV a
//! government statistics office publishes is usually neither comma-separated nor
//! UTF-8: the Brazilian securities regulator ships `;` and ISO-8859-1, and so
//! does most of the EU.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");
const httpx = @import("http.zig");
const azure = @import("azure.zig");
const s3 = @import("s3.zig");
const zipsrc = @import("zipsrc.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;

const BATCH_ROWS = 1024;
/// Reader/writer buffer size; also the max CSV line length (a line longer than
/// this yields `error.StreamTooLong`).
const LINE_BUF = 64 * 1024;

/// How a file's bytes map to text. Only single-byte encodings are here: a
/// multi-byte one would break the byte-range chunking the parallel reader depends
/// on, and would need a decoder that spans chunk boundaries.
pub const Encoding = enum {
    utf8,
    /// ISO-8859-1. Byte `b` is codepoint U+00`b`, so decoding is a table-free
    /// widening and no byte sequence can contain a delimiter or a newline.
    latin1,
    /// Windows-1252. Latin-1 except for 0x80–0x9F, where it puts the curly
    /// quotes, the dashes and the euro sign. Files labelled latin-1 are very
    /// often really this, and decoding one as latin-1 turns a quote into a
    /// control character rather than failing.
    cp1252,

    pub fn parse(s: []const u8) ?Encoding {
        const norm = struct {
            fn eq(a: []const u8, b: []const u8) bool {
                var i: usize = 0;
                var j: usize = 0;
                while (true) {
                    while (i < a.len and (a[i] == '-' or a[i] == '_')) i += 1;
                    while (j < b.len and (b[j] == '-' or b[j] == '_')) j += 1;
                    if (i == a.len or j == b.len) return i == a.len and j == b.len;
                    if (std.ascii.toLower(a[i]) != std.ascii.toLower(b[j])) return false;
                    i += 1;
                    j += 1;
                }
            }
        };
        for ([_]struct { name: []const u8, enc: Encoding }{
            .{ .name = "utf8", .enc = .utf8 },
            .{ .name = "latin1", .enc = .latin1 },
            .{ .name = "iso88591", .enc = .latin1 },
            .{ .name = "cp1252", .enc = .cp1252 },
            .{ .name = "windows1252", .enc = .cp1252 },
        }) |c| if (norm.eq(s, c.name)) return c.enc;
        return null;
    }
};

/// The 0x80–0x9F block of Windows-1252 as codepoints; 0 marks the five slots that
/// are undefined, which decode to U+FFFD rather than being invented.
const cp1252_high = [32]u21{
    0x20AC, 0, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
    0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0, 0x017D, 0,
    0, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0, 0x017E, 0x0178,
};

/// A single-stream compression suffix. Only the ones Zig's std decompresses into a
/// `*std.Io.Reader`: gzip and zstd via `std.http.Decompress`, which is the same
/// mechanism this reader already uses for HTTP `Content-Encoding`. `.xz` is absent
/// deliberately — std's xz decoder is still the old `GenericReader` shape and would
/// need an adapter, and `.csv.xz` is vanishingly rare in published data.
pub const Codec = enum { none, gzip, zstd };

/// Splits a trailing compression suffix off a path: `orders.csv.gz` is a gzip
/// wrapping `orders.csv`, and it is the inner name that picks the format.
pub fn splitCodec(path: []const u8) struct { codec: Codec, rest: []const u8 } {
    const bare = path[0 .. std.mem.indexOfAny(u8, path, "?#") orelse path.len];
    if (std.ascii.endsWithIgnoreCase(bare, ".gz")) return .{ .codec = .gzip, .rest = bare[0 .. bare.len - 3] };
    if (std.ascii.endsWithIgnoreCase(bare, ".gzip")) return .{ .codec = .gzip, .rest = bare[0 .. bare.len - 5] };
    if (std.ascii.endsWithIgnoreCase(bare, ".zst")) return .{ .codec = .zstd, .rest = bare[0 .. bare.len - 4] };
    if (std.ascii.endsWithIgnoreCase(bare, ".zstd")) return .{ .codec = .zstd, .rest = bare[0 .. bare.len - 5] };
    return .{ .codec = .none, .rest = bare };
}

/// A path naming a file inside an archive: `inf.zip :: inf_diario.csv`.
///
/// `::` is ClickHouse's separator for the same idea, and following it means the
/// member's own name carries the extension that picks the reader. Whitespace around
/// it is optional.
pub const ArchiveRef = struct { archive: []const u8, member: ?[]const u8 };

pub fn splitArchive(path: []const u8) ?ArchiveRef {
    const at = std.mem.indexOf(u8, path, "::") orelse {
        // A bare `.zip` is still an archive; it just has to hold one member.
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

/// The name whose extension decides the format: the member inside an archive, with
/// any compression suffix taken off.
pub fn dataName(path: []const u8) []const u8 {
    const inner = if (splitArchive(path)) |a| (a.member orelse a.archive) else path;
    return splitCodec(inner).rest;
}

/// Buffer a decompressor needs, per codec.
///
/// Not `std.http.ContentEncoding.minBufferCapacity()`: that returns
/// `zstd.default_window_len` alone, while `zstd.Decompress` asserts capacity for
/// the window *plus* `block_size_max` and otherwise fails the stream with
/// `OutputBufferUndersize`. A real 2.6 MB `.csv.zst` failed exactly that way.
fn codecBuffer(codec: Codec) usize {
    return switch (codec) {
        .none => 0,
        .gzip => std.compress.flate.max_window_len,
        .zstd => std.compress.zstd.default_window_len + std.compress.zstd.block_size_max,
    };
}

pub const Dialect = struct {
    delim: u8 = ',',
    encoding: Encoding = .utf8,
};

/// Re-encode one field as UTF-8. Returns the input untouched when the encoding is
/// already UTF-8 or when every byte is ASCII — which is true of most fields even
/// in a latin-1 file (the CVM registry's dates, CNPJs and codes are all ASCII), so
/// the ordinary field costs one scan and no allocation.
fn decodeField(arena: std.mem.Allocator, enc: Encoding, s: []const u8) ![]const u8 {
    if (enc == .utf8) return s;
    var high = false;
    for (s) |c| if (c >= 0x80) {
        high = true;
        break;
    };
    if (!high) return s;

    var out = try std.array_list.Managed(u8).initCapacity(arena, s.len + 8);
    var buf: [4]u8 = undefined;
    for (s) |c| {
        if (c < 0x80) {
            out.appendAssumeCapacity(c);
            continue;
        }
        const cp: u21 = switch (enc) {
            .utf8 => unreachable,
            .latin1 => c,
            .cp1252 => blk: {
                if (c > 0x9F) break :blk c;
                const m = cp1252_high[c - 0x80];
                break :blk if (m == 0) 0xFFFD else m;
            },
        };
        const n = std.unicode.utf8Encode(cp, &buf) catch unreachable;
        try out.appendSlice(buf[0..n]);
    }
    return out.toOwnedSlice();
}

pub const CsvReader = struct {
    arena: std.mem.Allocator,
    dialect: Dialect = .{},
    backend: Backend,
    read_buf: [LINE_BUF]u8 = undefined,
    /// Line source, independent of where bytes come from: points at the file
    /// reader's interface or the HTTP response body reader.
    rdr: *std.Io.Reader = undefined,
    schema: types.Schema,
    pending: []const []const u8 = &.{},
    pending_i: usize = 0,
    stream_eof: bool = false,
    done: bool = false,
    /// Remaining `az://`/`s3://` object URLs of a prefix read, in listing order.
    /// Empty for a single object.
    rest_urls: []const []const u8 = &.{},
    /// Header line of the first blob; later blobs must match it, or the read
    /// would silently splice mismatched columns together.
    header_line: []const u8 = "",
    /// Scratch for a record that spans physical lines (quoted newline). Reused,
    /// so it costs one allocation of the longest such record, not one per row.
    join_buf: std.array_list.Managed(u8) = undefined,
    /// Set when the path carried a compression suffix; holds the decompressor the
    /// line reader pulls through. Must not move once `rdr` points into it.
    codec_state: std.http.Decompress = undefined,

    const Backend = union(enum) {
        file: FileBackend,
        http: *HttpFetch,
        /// A member of a local zip, already inflating.
        member: *zipsrc.Member,
    };
    const FileBackend = struct {
        file: std.fs.File,
        fr: std.fs.File.Reader,
    };
    /// The live HTTP request whose body the reader streams from. Separate
    /// allocation: Request/Response hold internal pointers, so they are built
    /// in place here and never moved.
    const HttpFetch = struct {
        client: std.http.Client,
        req: std.http.Client.Request,
        response: std.http.Client.Response,
        decompress: std.http.Decompress = undefined,
        redirect_buf: [8 * 1024]u8 = undefined,
        transfer_buf: [LINE_BUF]u8 = undefined,
    };

    pub fn isUrl(path: []const u8) bool {
        return std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://") or
            azure.isUrl(path) or s3.isUrl(path);
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8, dialect: Dialect) !*CsvReader {
        const self = try arena.create(CsvReader);
        self.* = .{
            .arena = arena,
            .dialect = dialect,
            .backend = undefined,
            .schema = undefined,
            .done = false,
            .join_buf = std.array_list.Managed(u8).init(arena),
        };
        var first = path;
        if (azure.isPrefix(path)) {
            const p = try azure.parsePrefix(path);
            const client = try arena.create(std.http.Client);
            client.* = httpx.initClient(arena);
            defer client.deinit();
            const names = try azure.listPrefix(arena, client, p.account, p.container, p.prefix, azure.endpointFromEnv(arena));
            if (names.len == 0) return azure.Error.AzureEmptyPrefix;
            const urls = try arena.alloc([]const u8, names.len);
            for (names, urls) |n, *u| u.* = try std.fmt.allocPrint(arena, "az://{s}/{s}/{s}", .{ p.account, p.container, n });
            first = urls[0];
            self.rest_urls = urls[1..];
        } else if (s3.isPrefix(path)) {
            const p = try s3.parsePrefix(path);
            const client = try arena.create(std.http.Client);
            client.* = httpx.initClient(arena);
            defer client.deinit();
            const names = try s3.listPrefix(arena, client, p.bucket, p.prefix, s3.endpointFromEnv(arena));
            if (names.len == 0) return s3.Error.S3EmptyPrefix;
            const urls = try arena.alloc([]const u8, names.len);
            for (names, urls) |n, *u| u.* = try std.fmt.allocPrint(arena, "s3://{s}/{s}", .{ p.bucket, n });
            first = urls[0];
            self.rest_urls = urls[1..];
        }

        if (splitArchive(first)) |ar| {
            // Local archives only for now; a remote one needs its central directory
            // fetched from the tail first.
            if (isUrl(ar.archive)) return error.ArchiveUrlUnsupported;
            const m = try zipsrc.openMember(arena, ar.archive, ar.member);
            self.backend = .{ .member = m };
            self.rdr = m.reader;
        } else if (isUrl(first)) {
            const hf = try arena.create(HttpFetch);
            hf.* = .{ .client = httpx.initClient(arena), .req = undefined, .response = undefined };
            errdefer hf.client.deinit();
            // az:// and s3:// resolve to a real endpoint and carry a signature;
            // plain http(s) URLs go out unsigned as before.
            var req_url = first;
            var extra: []const std.http.Header = &.{};
            if (azure.isUrl(first)) {
                const blob = try azure.parseUrl(arena, first, azure.endpointFromEnv(arena));
                req_url = blob.url;
                extra = try azure.getHeaders(arena, blob, "");
            } else if (s3.isUrl(first)) {
                const obj = try s3.parseUrl(arena, first, s3.endpointFromEnv(arena));
                req_url = obj.url;
                extra = try s3.getHeaders(arena, obj, "");
            }
            const uri = std.Uri.parse(req_url) catch return error.InvalidUrl;
            startHttp(hf, uri, extra) catch |e| switch (e) {
                error.TlsInitializationFailed => {
                    const h = httpx.uriHost(uri) orelse return e;
                    if (!httpx.repairBundle(arena, &hf.client.ca_bundle, h, uri.port orelse 443)) return e;
                    hf.client.next_https_rescan_certs = false;
                    try startHttp(hf, uri, extra);
                },
                else => return e,
            };
            errdefer hf.req.deinit();
            const code = @intFromEnum(hf.response.head.status);
            if (code != 200) return httpx.statusError(code);
            self.backend = .{ .http = hf };
            const ce = hf.response.head.content_encoding;
            if (ce == .compress) return error.UnsupportedCompressionMethod;
            // Sized here rather than from `minBufferCapacity`, which under-sizes
            // zstd — see `codecBuffer`.
            const win = switch (ce) {
                .zstd => codecBuffer(.zstd),
                .gzip, .deflate => codecBuffer(.gzip),
                .compress, .identity => 0,
            };
            const dbuf: []u8 = if (win > 0) try arena.alloc(u8, win) else &.{};
            self.rdr = hf.response.readerDecompressing(&hf.transfer_buf, &hf.decompress, dbuf);
        } else {
            self.backend = .{ .file = .{ .file = try std.fs.cwd().openFile(path, .{}), .fr = undefined } };
            self.backend.file.fr = self.backend.file.file.reader(&self.read_buf);
            self.rdr = &self.backend.file.fr.interface;
        }

        // A compression suffix on the name wraps whatever the bytes came from —
        // file, HTTP body or archive member — in the same decompressor the HTTP
        // content-encoding path uses.
        const codec = splitCodec(if (splitArchive(first)) |ar| (ar.member orelse ar.archive) else first).codec;
        if (codec != .none) {
            const ce: std.http.ContentEncoding = switch (codec) {
                .none => .identity,
                .gzip => .gzip,
                .zstd => .zstd,
            };
            const cbuf = try arena.alloc(u8, codecBuffer(codec));
            self.rdr = std.http.Decompress.init(&self.codec_state, self.rdr, cbuf, ce);
        }

        const header = (try self.readLine()) orelse return error.EmptyCsv;
        self.header_line = try arena.dupe(u8, std.mem.trim(u8, header, " \t\r"));
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var it = std.mem.splitScalar(u8, header, dialect.delim);
        while (it.next()) |name| {
            try fields.append(.{
                .name = try decodeField(arena, dialect.encoding, try arena.dupe(u8, std.mem.trim(u8, name, " \t"))),
                .ty = types.Type.init(.string).asNullable(),
            });
        }

        var sniff = try TypeSniffer.init(arena, fields.items.len);
        var pending = std.array_list.Managed([]const u8).init(arena);
        while (pending.items.len < SAMPLE_ROWS) {
            const line = (try self.readLine()) orelse {
                // A prefix read keeps sniffing into the next blob; stopping here
                // would both truncate the read and infer types from one file.
                if (try self.advance()) continue;
                self.stream_eof = true;
                break;
            };
            if (line.len == 0) continue;
            const own = try arena.dupe(u8, line);
            sniff.feed(own);
            try pending.append(own);
        }
        for (fields.items, 0..) |*f, j| f.ty = sniff.resolve(j);

        self.pending = try pending.toOwnedSlice();
        self.schema = .{ .fields = try fields.toOwnedSlice() };
        return self;
    }

    pub fn next(self: *CsvReader, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const ncols = self.schema.fields.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders, self.schema.fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, BATCH_ROWS);

        var rows: usize = 0;
        while (rows < BATCH_ROWS) {
            var line: []const u8 = undefined;
            if (self.pending_i < self.pending.len) {
                line = self.pending[self.pending_i];
                self.pending_i += 1;
            } else {
                if (self.stream_eof) {
                    if (!(try self.advance())) {
                        self.done = true;
                        break;
                    }
                    self.stream_eof = false;
                }
                line = (try self.readLine()) orelse blk: {
                    // End of this blob — a prefix read continues into the next.
                    if (try self.advance()) break :blk (try self.readLine()) orelse {
                        self.done = true;
                        break;
                    };
                    self.done = true;
                    break;
                };
            }
            if (line.len == 0) continue;
            try splitInto(arena, line, builders, self.dialect);
            rows += 1;
        }
        if (rows == 0) return null;

        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
    }

    /// Opens the next blob of a prefix read and discards its header, which must
    /// match the first blob's. Returns false when the listing is exhausted.
    fn advance(self: *CsvReader) !bool {
        if (self.rest_urls.len == 0) return false;
        const url = self.rest_urls[0];
        self.rest_urls = self.rest_urls[1..];

        switch (self.backend) {
            .http => |hf| hf.req.deinit(),
            // `rest_urls` is only ever populated by an object-prefix listing, so a
            // file or archive read never reaches here.
            .file, .member => {},
        }
        const hf = try self.arena.create(HttpFetch);
        hf.* = .{ .client = httpx.initClient(self.arena), .req = undefined, .response = undefined };
        var req_url: []const u8 = undefined;
        var extra: []const std.http.Header = undefined;
        if (s3.isUrl(url)) {
            const obj = try s3.parseUrl(self.arena, url, s3.endpointFromEnv(self.arena));
            req_url = obj.url;
            extra = try s3.getHeaders(self.arena, obj, "");
        } else {
            const blob = try azure.parseUrl(self.arena, url, azure.endpointFromEnv(self.arena));
            req_url = blob.url;
            extra = try azure.getHeaders(self.arena, blob, "");
        }
        const uri = std.Uri.parse(req_url) catch return error.InvalidUrl;
        try startHttp(hf, uri, extra);
        const code = @intFromEnum(hf.response.head.status);
        if (code != 200) return httpx.statusError(code);
        self.backend = .{ .http = hf };
        const ce = hf.response.head.content_encoding;
        if (ce == .compress) return error.UnsupportedCompressionMethod;
        const win = ce.minBufferCapacity();
        const dbuf: []u8 = if (win > 0) try self.arena.alloc(u8, win) else &.{};
        self.rdr = hf.response.readerDecompressing(&hf.transfer_buf, &hf.decompress, dbuf);

        const hdr = (try self.readLine()) orelse return error.EmptyCsv;
        if (!std.mem.eql(u8, std.mem.trim(u8, hdr, " \t\r"), self.header_line)) return error.CsvHeaderMismatch;
        return true;
    }

    pub fn close(self: *CsvReader) void {
        switch (self.backend) {
            .file => |f| f.file.close(),
            .http => |hf| {
                hf.req.deinit();
                hf.client.deinit();
            },
            .member => |m| m.close(),
        }
    }

    pub fn source(self: *CsvReader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    fn startHttp(hf: *HttpFetch, uri: std.Uri, extra: []const std.http.Header) !void {
        hf.req = try hf.client.request(.GET, uri, .{ .extra_headers = extra });
        errdefer hf.req.deinit();
        try hf.req.sendBodiless();
        hf.response = try hf.req.receiveHead(&hf.redirect_buf);
    }

    /// One CSV record, which may span several physical lines: a newline inside a
    /// quoted field belongs to the value. The common case (balanced quotes) is
    /// the untouched zero-copy path; only a continued record is joined, into a
    /// buffer reused across records so a long file does not grow the arena.
    fn readLine(self: *CsvReader) !?[]const u8 {
        const first = (try self.rdr.takeDelimiter('\n')) orelse return null;
        var s: []const u8 = first;
        if (s.len > 0 and s[s.len - 1] == '\r') s = s[0 .. s.len - 1];
        if (!quotesOpen(s, self.dialect.delim)) return s;

        self.join_buf.clearRetainingCapacity();
        try self.join_buf.appendSlice(s);
        while (quotesOpen(self.join_buf.items, self.dialect.delim)) {
            const more = (try self.rdr.takeDelimiter('\n')) orelse break;
            var m: []const u8 = more;
            if (m.len > 0 and m[m.len - 1] == '\r') m = m[0 .. m.len - 1];
            try self.join_buf.append('\n');
            try self.join_buf.appendSlice(m);
        }
        return self.join_buf.items;
    }
};

/// A local CSV file mapped into memory once, so N worker threads can parse disjoint
/// newline-aligned byte ranges in parallel (the parse, not the read, is the CSV
/// bottleneck). The mapping is shared read-only; each thread builds a `CsvSliceReader`
/// over its chunk. Only for local files — not URLs.
pub const MappedCsv = struct {
    data: []align(std.heap.page_size_min) const u8,
    body: []const u8,
    schema: types.Schema,
    file: std.fs.File,
    /// True when some quoted field contains a newline. Chunk boundaries are
    /// picked by seeking a newline from a byte offset, and whether that newline
    /// is inside quotes cannot be known without scanning from the start of the
    /// file — so callers must not split this file; they fall back to serial.
    quoted_newlines: bool = false,
    dialect: Dialect = .{},

    pub fn open(arena: std.mem.Allocator, path: []const u8, dialect: Dialect) !*MappedCsv {
        // Chunking picks boundaries by seeking a newline from a byte offset, which
        // needs the plain bytes on disk. A compressed stream has no such mapping
        // from offset to row, and an archive member is not the file itself — both
        // belong on the serial reader, and the callers fall back on this error.
        if (splitCodec(path).codec != .none or splitArchive(path) != null) return error.NotMappable;
        const self = try arena.create(MappedCsv);
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();
        const size = (try file.stat()).size;
        if (size == 0) return error.EmptyCsv;
        const data = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(data);

        const nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.EmptyCsv;
        var header = data[0..nl];
        if (header.len > 0 and header[header.len - 1] == '\r') header = header[0 .. header.len - 1];
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var it = std.mem.splitScalar(u8, header, dialect.delim);
        while (it.next()) |name| try fields.append(.{
            .name = try decodeField(arena, dialect.encoding, try arena.dupe(u8, std.mem.trim(u8, name, " \t"))),
            .ty = types.Type.init(.string).asNullable(),
        });

        const body = data[nl + 1 ..];
        var sniff = try TypeSniffer.init(arena, fields.items.len);
        var fed: usize = 0;
        var pos: usize = 0;
        while (fed < SAMPLE_ROWS and pos < body.len) {
            const rec = scanRecord(body, pos, dialect.delim);
            const line = rec.line;
            pos = rec.next;
            if (line.len == 0) continue;
            sniff.feed(line);
            fed += 1;
        }
        for (fields.items, 0..) |*f, j| f.ty = sniff.resolve(j);

        self.* = .{
            .data = data,
            .body = body,
            .schema = .{ .fields = try fields.toOwnedSlice() },
            .file = file,
            .dialect = dialect,
            .quoted_newlines = hasQuotedNewline(body, dialect.delim),
        };
        return self;
    }

    /// The i-th of `n` newline-aligned chunks of the body (whole lines only; a line
    /// belongs to the chunk that contains its first byte). May be empty.
    pub fn chunk(self: *const MappedCsv, i: usize, n: usize) []const u8 {
        const lo = self.lineStart(self.body.len * i / n);
        const hi = self.lineStart(self.body.len * (i + 1) / n);
        return self.body[lo..hi];
    }

    /// Does any quoted field hold a newline? Skipped entirely when the file has
    /// no quote at all (the common case), so the scan costs one `memchr`.
    fn hasQuotedNewline(body: []const u8, delim: u8) bool {
        if (std.mem.indexOfScalar(u8, body, '"') == null) return false;
        var in_q = false;
        var at_field = true;
        var i: usize = 0;
        while (i < body.len) : (i += 1) {
            const c = body[i];
            if (c == '"') {
                if (in_q) {
                    if (i + 1 < body.len and body[i + 1] == '"') {
                        i += 1;
                        continue;
                    }
                    in_q = false;
                } else if (at_field) {
                    in_q = true;
                }
                at_field = false;
            } else if (c == '\n' and in_q) {
                return true;
            } else if (c == delim and !in_q) {
                at_field = true;
            } else if (c == '\n') {
                at_field = true;
            } else at_field = false;
        }
        return false;
    }

    /// Smallest line-start offset >= `raw` (0, or just past a '\n').
    fn lineStart(self: *const MappedCsv, raw: usize) usize {
        if (raw == 0) return 0;
        if (raw >= self.body.len) return self.body.len;
        var p = raw;
        while (p < self.body.len and self.body[p] != '\n') p += 1;
        return if (p < self.body.len) p + 1 else self.body.len;
    }

    pub fn close(self: *MappedCsv) void {
        std.posix.munmap(self.data);
        self.file.close();
    }
};

/// A `driver.Source` over an in-memory byte slice of whole CSV lines (one chunk of a
/// `MappedCsv`). Shares the parent's schema; copies field bytes into the pull arena.
pub const CsvSliceReader = struct {
    data: []const u8,
    pos: usize = 0,
    schema: *const types.Schema,
    dialect: Dialect = .{},

    pub fn next(self: *CsvSliceReader, arena: std.mem.Allocator) !?Batch {
        if (self.pos >= self.data.len) return null;
        const ncols = self.schema.fields.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders, self.schema.fields) |*b, f| b.* = try column.Builder.initCapacity(arena, f.ty, BATCH_ROWS);

        var rows: usize = 0;
        while (rows < BATCH_ROWS and self.pos < self.data.len) {
            const rec = scanRecord(self.data, self.pos, self.dialect.delim);
            const line = rec.line;
            self.pos = rec.next;
            if (line.len == 0) continue;
            try splitInto(arena, line, builders, self.dialect);
            rows += 1;
        }
        if (rows == 0) return null;
        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = self.schema, .columns = cols, .len = rows };
    }

    pub fn source(self: *CsvSliceReader) driver.Source {
        return .{ .ptr = self, .vtable = &slice_vtable };
    }
};

const slice_vtable = driver.Source.VTable{
    .schema = sliceSchema,
    .next = sliceNext,
    .close = sliceClose,
};
fn sliceSchema(ptr: *anyopaque) types.Schema {
    const self: *CsvSliceReader = @ptrCast(@alignCast(ptr));
    return self.schema.*;
}
fn sliceNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *CsvSliceReader = @ptrCast(@alignCast(ptr));
    return self.next(arena);
}
fn sliceClose(_: *anyopaque) void {}

/// Rows sampled for type inference. Both CSV readers sniff the same first
/// SAMPLE_ROWS lines with the same rules, so the serial and mapped-parallel
/// paths always agree on a file's schema.
pub const SAMPLE_ROWS = 1024;

/// Column type inference over sampled lines: int ⊂ float ⊂ string. Quoted cells
/// force string (quotes mark text), empty cells only mark nullability, and a
/// leading zero / '+' sign disqualifies int ("007" must round-trip verbatim).
const TypeSniffer = struct {
    const ColState = struct { seen: bool = false, all_int: bool = true, all_float: bool = true };
    cols: []ColState,

    fn init(arena: std.mem.Allocator, ncols: usize) !TypeSniffer {
        const cols = try arena.alloc(ColState, ncols);
        for (cols) |*c| c.* = .{};
        return .{ .cols = cols };
    }

    fn feed(self: *TypeSniffer, line: []const u8) void {
        var i: usize = 0;
        for (self.cols) |*c| {
            if (i < line.len and line[i] == '"') {
                c.seen = true;
                c.all_int = false;
                c.all_float = false;
                i += 1;
                while (i < line.len) {
                    if (line[i] == '"') {
                        if (i + 1 < line.len and line[i + 1] == '"') {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            } else {
                const start = i;
                while (i < line.len and line[i] != ',') i += 1;
                const raw = line[start..i];
                if (raw.len > 0) {
                    c.seen = true;
                    if (raw[0] == '+' or (raw.len > 1 and (raw[0] == '0' or (raw[0] == '-' and raw[1] == '0')) and std.mem.indexOfScalar(u8, raw, '.') == null)) {
                        c.all_int = false;
                        c.all_float = false;
                    } else {
                        if (c.all_int) _ = std.fmt.parseInt(i64, raw, 10) catch {
                            c.all_int = false;
                        };
                        if (c.all_float) _ = std.fmt.parseFloat(f64, raw) catch {
                            c.all_float = false;
                        };
                    }
                }
            }
            if (i < line.len and line[i] == ',') i += 1;
        }
    }

    fn resolve(self: *const TypeSniffer, j: usize) types.Type {
        const c = self.cols[j];
        const k: types.TypeKind = if (!c.seen or !c.all_float) .string else if (c.all_int) .int else .float;
        return types.Type.init(k).asNullable();
    }
};

/// Append one decoded cell per the builder's column type. Unquoted empty is
/// null; quoted "" is an empty string. A cell beyond the sample that no longer
/// parses as the inferred type is a hard error rather than silent corruption.
fn appendCell(b: *column.Builder, raw: []const u8, quoted: bool) !void {
    if (raw.len == 0) return b.append(if (quoted and b.ty.kind == .string) Value{ .string = raw } else .null);
    switch (b.ty.kind) {
        .int => try b.append(.{ .int = std.fmt.parseInt(i64, raw, 10) catch return error.CsvTypeMismatch }),
        .float => try b.append(.{ .float = std.fmt.parseFloat(f64, raw) catch return error.CsvTypeMismatch }),
        else => try b.append(.{ .string = raw }),
    }
}

/// One CSV record starting at `data[start]`, plus where the next one begins.
///
/// A `\n` inside a quoted field is part of the value (RFC 4180) — `splitInto`
/// has always honored quotes when cutting FIELDS, but records used to be cut on
/// the first raw newline, which split such a row in half. basalt's own CSV
/// writer quotes embedded newlines, so it emitted files it could not read back.
fn scanRecord(data: []const u8, start: usize, delim: u8) struct { line: []const u8, next: usize } {
    var i = start;
    var in_q = false;
    var at_field = true;
    while (i < data.len) : (i += 1) {
        const c = data[i];
        if (c == '"') {
            // Only a quote at the START of a field opens one — `splitInto` cuts
            // fields by that same rule. A bare `"` in the middle (`12" pipe`) is
            // data, and treating it as an opener swallowed every following row.
            if (in_q) {
                if (i + 1 < data.len and data[i + 1] == '"') {
                    i += 1;
                    continue;
                }
                in_q = false;
            } else if (at_field) {
                in_q = true;
            }
            at_field = false;
        } else if (c == delim and !in_q) {
            at_field = true;
        } else if (c == '\n' and !in_q) {
            var end = i;
            if (end > start and data[end - 1] == '\r') end -= 1;
            return .{ .line = data[start..end], .next = i + 1 };
        } else {
            at_field = false;
        }
    }
    var end = data.len;
    if (end > start and data[end - 1] == '\r') end -= 1;
    return .{ .line = data[start..end], .next = data.len };
}

/// Whether `line` leaves a quoted field open — i.e. the record continues on the
/// next physical line. `""` contributes two, so plain parity is the quote state.
fn quotesOpen(line: []const u8, delim: u8) bool {
    var in_q = false;
    var at_field = true;
    var i: usize = 0;
    while (i < line.len) : (i += 1) {
        const c = line[i];
        if (c == '"') {
            if (in_q) {
                if (i + 1 < line.len and line[i + 1] == '"') {
                    i += 1;
                    continue;
                }
                in_q = false;
            } else if (at_field) {
                in_q = true;
            }
            at_field = false;
        } else if (c == delim and !in_q) {
            at_field = true;
        } else at_field = false;
    }
    return in_q;
}

fn splitInto(arena: std.mem.Allocator, line: []const u8, builders: []column.Builder, d: Dialect) !void {
    var i: usize = 0;
    var col: usize = 0;
    while (col < builders.len) : (col += 1) {
        if (i < line.len and line[i] == '"') {
            i += 1;
            var buf = std.array_list.Managed(u8).init(arena);
            while (i < line.len) {
                if (line[i] == '"') {
                    if (i + 1 < line.len and line[i + 1] == '"') {
                        try buf.append('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                try buf.append(line[i]);
                i += 1;
            }
            // Text between the closing quote and the delimiter, as in the CVM
            // registry's `"1" é calculado de acordo com…`: a field that opens
            // quoted and then continues unquoted. RFC 4180 leaves it undefined and
            // it is plainly a publishing mistake, but ending the field at the quote
            // made the remainder look like the next column — one such row shifted
            // its last 19 values by one and dropped the final one, silently. Keep
            // reading to the delimiter, which is where the field visibly ends.
            while (i < line.len and line[i] != d.delim) : (i += 1) try buf.append(line[i]);
            const raw = try buf.toOwnedSlice();
            try appendCell(&builders[col], try decodeField(arena, d.encoding, raw), true);
            if (i < line.len and line[i] == d.delim) i += 1;
        } else {
            const start = i;
            while (i < line.len and line[i] != d.delim) i += 1;
            try appendCell(&builders[col], try decodeField(arena, d.encoding, line[start..i]), false);
            if (i < line.len and line[i] == d.delim) i += 1;
        }
    }
}

const source_vtable = driver.Source.VTable{
    .schema = srcSchema,
    .next = srcNext,
    .close = srcClose,
};
fn srcSchema(ptr: *anyopaque) types.Schema {
    const self: *CsvReader = @ptrCast(@alignCast(ptr));
    return self.schema;
}
fn srcNext(ptr: *anyopaque, arena: std.mem.Allocator) anyerror!?Batch {
    const self: *CsvReader = @ptrCast(@alignCast(ptr));
    return self.next(arena);
}
fn srcClose(ptr: *anyopaque) void {
    const self: *CsvReader = @ptrCast(@alignCast(ptr));
    self.close();
}

pub const CsvWriter = struct {
    backend: Backend,
    dialect: Dialect = .{},
    write_buf: [LINE_BUF]u8 = undefined,
    fw: std.fs.File.Writer = undefined,

    /// A local file, or a block blob staged over HTTP. Both expose a plain
    /// `*std.Io.Writer`, so row formatting below is identical either way.
    const Backend = union(enum) {
        file: std.fs.File,
        blob: struct { client: *std.http.Client, w: *azure.BlockBlobWriter },
        s3obj: struct { client: *std.http.Client, w: *s3.MultipartWriter },
    };

    fn out(self: *CsvWriter) *std.Io.Writer {
        return switch (self.backend) {
            .file => &self.fw.interface,
            .blob => |b| &b.w.interface,
            .s3obj => |b| &b.w.interface,
        };
    }

    /// Recovers the error a blob destination actually hit. Staging a block runs
    /// under `std.Io.Writer`, whose error set is just `WriteFailed`, so the Azure
    /// code recorded at the point of failure is put back here — otherwise a 403
    /// and a missing container are the same word to the caller.
    fn specific(self: *CsvWriter, e: anyerror) anyerror {
        if (e != error.WriteFailed) return e;
        return switch (self.backend) {
            .file => e,
            .blob => |b| b.w.last_status orelse e,
            .s3obj => |b| b.w.last_status orelse e,
        };
    }

    /// `.append` opens the file without truncating and resumes at its end,
    /// emitting the header only when there was nothing there — appending to a
    /// populated CSV must not splice a second header into the rows. A block blob
    /// is committed whole rather than extended, so it takes `.truncate` only.
    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, mode: driver.FileMode, dialect: Dialect) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        var header = true;
        if (azure.isUrl(path)) {
            if (mode == .append) return error.AppendNotSupported;
            const client = try arena.create(std.http.Client);
            client.* = httpx.initClient(arena);
            const blob = try azure.parseUrl(arena, path, azure.endpointFromEnv(arena));
            self.* = .{ .backend = .{ .blob = .{
                .client = client,
                .w = try azure.BlockBlobWriter.init(arena, client, blob, "text/csv"),
            } } };
        } else if (s3.isUrl(path)) {
            if (mode == .append) return error.AppendNotSupported;
            const client = try arena.create(std.http.Client);
            client.* = httpx.initClient(arena);
            const obj = try s3.parseUrl(arena, path, s3.endpointFromEnv(arena));
            self.* = .{ .backend = .{ .s3obj = .{
                .client = client,
                .w = try s3.MultipartWriter.init(arena, client, obj, "text/csv"),
            } } };
        } else {
            self.* = .{ .backend = .{ .file = try std.fs.cwd().createFile(path, .{ .truncate = mode == .truncate }) } };
            self.fw = self.backend.file.writer(&self.write_buf);
            if (mode == .append) {
                const end = try self.backend.file.getEndPos();
                try self.fw.seekTo(end);
                header = end == 0;
            }
        }

        // Set before the header is written, which is the writer's first output.
        self.dialect = dialect;
        if (header) {
            const w = self.out();
            for (schema.fields, 0..) |f, i| {
                if (i > 0) try w.writeByte(dialect.delim);
                try writeField(w, f.name, dialect.delim);
            }
            try w.writeByte('\n');
        }
        return self;
    }

    pub fn writeBatch(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) !void {
        self.writeRows(arena, batch) catch |e| return self.specific(e);
    }

    fn writeRows(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) !void {
        const w = self.out();
        const d = self.dialect.delim;
        // Whether a number or timestamp could need quoting under this dialect —
        // constant for the whole run, so it is decided once rather than per field.
        const quote_scalars = scalarCanNeedQuote(d);
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            for (batch.columns, 0..) |*col, i| {
                if (i > 0) try w.writeByte(d);
                switch (col.ty.kind) {
                    // Text columns already hold their bytes: read straight out of
                    // the Arrow buffers rather than boxing into a `Value` only for
                    // `valueToString` to switch back out and hand back the slice.
                    .string, .bytes => if (col.validity.get(r)) try writeField(w, col.data.bytes.at(r), d),
                    else => {
                        if (!col.validity.get(r)) continue;
                        const v = col.getValue(r);
                        // The common case: a number or timestamp cannot contain this
                        // delimiter, a quote or a newline, so it needs no quoting and
                        // no scan to discover that — it goes straight into the output
                        // buffer. `valueToString` would allocate a string per field,
                        // which on a 6M-row move is tens of millions of allocations
                        // whose only purpose is to be copied once and dropped.
                        if (quote_scalars) {
                            try writeField(w, try eval.valueToString(arena, v), d);
                        } else {
                            try eval.writeValue(w, v);
                        }
                    },
                }
            }
            try w.writeByte('\n');
        }
    }

    pub fn close(self: *CsvWriter) !void {
        switch (self.backend) {
            .file => |f| {
                try self.fw.interface.flush();
                f.close();
            },
            // Committing the block list is what makes the blob appear.
            .blob => |b| b.w.finish() catch |e| return self.specific(e),
            // Same for the multipart completion (or the single PUT).
            .s3obj => |b| b.w.finish() catch |e| return self.specific(e),
        }
    }

    /// Failure path. For a file, rows already flushed stay on disk (a CSV has no
    /// transaction to roll back). For a blob, skipping the block-list commit is
    /// the rollback: staged blocks never become a readable object, and Azure
    /// discards them after a week — so a failed run leaves nothing behind.
    pub fn abort(self: *CsvWriter) void {
        switch (self.backend) {
            .file => |f| f.close(),
            .blob => {},
            // An uncompleted multipart upload is invisible to readers; unlike
            // Azure, S3 only reaps it where the bucket has a lifecycle rule.
            .s3obj => {},
        }
    }

    pub fn sink(self: *CsvWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }
};

const sink_vtable = driver.Sink.VTable{
    .writeBatch = sinkWrite,
    .close = sinkClose,
    .abort = sinkAbort,
};
fn sinkWrite(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror!void {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    return self.writeBatch(arena, b);
}
fn sinkClose(ptr: *anyopaque) anyerror!void {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    return self.close();
}
fn sinkAbort(ptr: *anyopaque) void {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    self.abort();
}

/// Could a number or timestamp rendering ever need CSV quoting under this delimiter?
///
/// `valueToString`/`writeValue` render non-text values using only digits and
/// `+-.:eE ` (the space appears in a timestamp), so with an ordinary delimiter — `,`
/// `;` tab `|` — the answer is no and the quote scan can be skipped entirely. A
/// pathological delimiter like `.` or `-` would collide, and those take the same
/// quoted path text does.
fn scalarCanNeedQuote(delim: u8) bool {
    return switch (delim) {
        '0'...'9', '+', '-', '.', ':', 'e', 'E', ' ', '"', '\n', '\r' => true,
        else => false,
    };
}

fn writeField(w: anytype, s: []const u8, delim: u8) !void {
    if (needsQuote(s, delim)) {
        try w.writeByte('"');
        for (s) |c| {
            if (c == '"') try w.writeByte('"');
            try w.writeByte(c);
        }
        try w.writeByte('"');
    } else {
        try w.writeAll(s);
    }
}

/// An empty value must be quoted. Only the writer knows it is a string rather
/// than a null — unquoted empty is how the reader spells null — so emitting it
/// bare turned `""` into NULL on the next read.
fn needsQuote(s: []const u8, delim: u8) bool {
    if (s.len == 0) return true;
    for (s) |c| {
        if (c == delim or c == '"' or c == '\n' or c == '\r') return true;
    }
    return false;
}

/// Accept one connection, swallow the request, write a canned HTTP response.
fn serveOnce(listener: *std.net.Server, status_line: []const u8, body: []const u8) void {
    serveOnceInner(listener, status_line, body) catch {};
}
fn serveOnceInner(listener: *std.net.Server, status_line: []const u8, body: []const u8) !void {
    const conn = try listener.accept();
    defer conn.stream.close();
    var rb: [4096]u8 = undefined;
    _ = try conn.stream.read(&rb);
    var wb: [512]u8 = undefined;
    const head = try std.fmt.bufPrint(
        &wb,
        "HTTP/1.1 {s}\r\ncontent-length: {d}\r\nconnection: close\r\n\r\n",
        .{ status_line, body.len },
    );
    try conn.stream.writeAll(head);
    try conn.stream.writeAll(body);
}

test "CsvReader streams a CSV over http" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var listener = try addr.listen(.{ .reuse_address = true });
    defer listener.deinit();
    const th = try std.Thread.spawn(.{}, serveOnce, .{ &listener, "200 OK", "id,name\n1,alpha\n2,beta\n" });
    defer th.join();

    const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/data.csv", .{listener.listen_address.getPort()});
    const r = try CsvReader.open(a, url, .{});
    defer r.close();
    try std.testing.expectEqual(@as(usize, 2), r.schema.fields.len);
    try std.testing.expectEqualStrings("id", r.schema.fields[0].name);
    try std.testing.expectEqualStrings("name", r.schema.fields[1].name);

    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expectEqual(@as(i64, 1), b.columns[0].getValue(0).int);
    try std.testing.expectEqualStrings("alpha", b.columns[1].getValue(0).string);
    try std.testing.expectEqualStrings("beta", b.columns[1].getValue(1).string);
    try std.testing.expect((try r.next(a)) == null);
}

test "CsvReader maps http status: 4xx permanent, 5xx transient" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    {
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        var listener = try addr.listen(.{ .reuse_address = true });
        defer listener.deinit();
        const th = try std.Thread.spawn(.{}, serveOnce, .{ &listener, "404 Not Found", "nope" });
        defer th.join();
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/missing.csv", .{listener.listen_address.getPort()});
        try std.testing.expectError(error.HttpNotFound, CsvReader.open(a, url, .{}));
    }
    {
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        var listener = try addr.listen(.{ .reuse_address = true });
        defer listener.deinit();
        const th = try std.Thread.spawn(.{}, serveOnce, .{ &listener, "503 Service Unavailable", "busy" });
        defer th.join();
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/data.csv", .{listener.listen_address.getPort()});
        try std.testing.expectError(error.HttpServerBusy, CsvReader.open(a, url, .{}));
    }
}

/// Parse `data` (whole CSV lines, no header) as `ncols` string columns and
/// return the one resulting batch — the pure-parsing entry shared by every
/// reader backend (`splitInto` under a `CsvSliceReader`).
fn parseSlice(a: std.mem.Allocator, schema: *const types.Schema, data: []const u8) !Batch {
    var r = CsvSliceReader{ .data = data, .schema = schema };
    return (try r.next(a)).?;
}

fn stringSchema(a: std.mem.Allocator, names: []const []const u8) !types.Schema {
    const fields = try a.alloc(types.Schema.Field, names.len);
    for (names, 0..) |n, i| fields[i] = .{ .name = n, .ty = types.Type.init(.string).asNullable() };
    return .{ .fields = fields };
}

test "csv parsing: quoted fields, escaped quotes, empty fields" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b", "c" });

    const b = try parseSlice(a, &schema, "\"x,y\",\"say \"\"hi\"\"\",\n1,2,3\n");
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expectEqualStrings("x,y", b.columns[0].getValue(0).string);
    try std.testing.expectEqualStrings("say \"hi\"", b.columns[1].getValue(0).string);
    try std.testing.expect(b.columns[2].getValue(0).isNull());
    try std.testing.expectEqualStrings("3", b.columns[2].getValue(1).string);
}

test "csv parsing: CRLF endings, blank lines, ragged rows" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b", "c" });

    const b = try parseSlice(a, &schema, "1,2,3\r\n\r\n4,5\r\n6,7,8,NINE\n");
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqualStrings("3", b.columns[2].getValue(0).string);
    try std.testing.expect(b.columns[2].getValue(1).isNull());
    try std.testing.expectEqualStrings("6", b.columns[0].getValue(2).string);
    try std.testing.expectEqualStrings("8", b.columns[2].getValue(2).string);
}

test "csv parsing: leading/trailing empty fields and last line without newline" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b", "c" });

    const b = try parseSlice(a, &schema, ",mid,\nx,y,z");
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expect(b.columns[0].getValue(0).isNull());
    try std.testing.expectEqualStrings("mid", b.columns[1].getValue(0).string);
    try std.testing.expect(b.columns[2].getValue(0).isNull());
    try std.testing.expectEqualStrings("z", b.columns[2].getValue(1).string);
}

test "writeField quotes exactly the fields that need it, doubling quotes" {
    var buf = std.array_list.Managed(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeField(buf.writer(), "plain", ',');
    try buf.append('|');
    try writeField(buf.writer(), "a,b", ',');
    try buf.append('|');
    try writeField(buf.writer(), "say \"hi\"", ',');
    try buf.append('|');
    try writeField(buf.writer(), "line\nbreak", ',');
    try buf.append('|');
    try writeField(buf.writer(), "", ',');
    try std.testing.expectEqualStrings("plain|\"a,b\"|\"say \"\"hi\"\"\"|\"line\nbreak\"|\"\"", buf.items);
}

test "an empty string survives a write/read round-trip and stays distinct from null" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b" });

    // The writer emits nothing for a null and `""` for an empty string; reading
    // that back has to reproduce exactly that distinction. Emitting the empty
    // string bare made every one of them come back as NULL.
    var line = std.array_list.Managed(u8).init(a);
    try writeField(line.writer(), "", ',');
    try line.append(',');
    try line.append('\n');
    const b = try parseSlice(a, &schema, line.items);
    try std.testing.expect(!b.columns[0].getValue(0).isNull());
    try std.testing.expectEqualStrings("", b.columns[0].getValue(0).string);
    try std.testing.expect(b.columns[1].getValue(0).isNull());
}

test "csv write/parse round-trip preserves quoted values" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b" });

    var line = std.array_list.Managed(u8).init(a);
    try writeField(line.writer(), "O'Neil, \"Jr\"", ',');
    try line.append(',');
    try writeField(line.writer(), "plain", ',');
    try line.append('\n');
    const b = try parseSlice(a, &schema, line.items);
    try std.testing.expectEqualStrings("O'Neil, \"Jr\"", b.columns[0].getValue(0).string);
    try std.testing.expectEqualStrings("plain", b.columns[1].getValue(0).string);
}

test "TypeSniffer: int/float promotion, leading zeros and quoted cells force string, empties only mark nulls" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var s = try TypeSniffer.init(ar.allocator(), 6);
    s.feed("1,1.5,abc,007,\"9\",");
    s.feed("-2,2,x,12,3,");
    try std.testing.expectEqual(types.TypeKind.int, s.resolve(0).kind);
    try std.testing.expectEqual(types.TypeKind.float, s.resolve(1).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(2).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(3).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(4).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(5).kind);
    try std.testing.expect(s.resolve(0).nullable);
}

test "serial and mapped readers infer the same schema; mismatch past the sample errors" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var data = std.array_list.Managed(u8).init(a);
    try data.appendSlice("id,name\n");
    for (0..SAMPLE_ROWS + 10) |i| try data.writer().print("{d},n{d}\n", .{ i + 1, i + 1 });
    try tmp.dir.writeFile(.{ .sub_path = "ok.csv", .data = data.items });
    try data.appendSlice("oops,tail\n");
    try tmp.dir.writeFile(.{ .sub_path = "bad.csv", .data = data.items });
    const base = try tmp.dir.realpathAlloc(a, ".");

    const ok_path = try std.fs.path.join(a, &.{ base, "ok.csv" });
    const r = try CsvReader.open(a, ok_path, .{});
    defer r.close();
    const m = try MappedCsv.open(a, ok_path, .{});
    defer m.close();
    try std.testing.expectEqual(types.TypeKind.int, r.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.int, m.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.string, r.schema.fields[1].ty.kind);
    try std.testing.expectEqual(types.TypeKind.string, m.schema.fields[1].ty.kind);

    const bad_path = try std.fs.path.join(a, &.{ base, "bad.csv" });
    const rb = try CsvReader.open(a, bad_path, .{});
    defer rb.close();
    var err: ?anyerror = null;
    while (rb.next(a) catch |e| blk: {
        err = e;
        break :blk null;
    }) |_| {}
    try std.testing.expectEqual(@as(?anyerror, error.CsvTypeMismatch), err);
}

test "MappedCsv chunks are newline-aligned, disjoint, and covering" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const body = "1,alpha\n22,bb\n333,c\n4444,dddd\n5,e\n";
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id,name\n" ++ body });
    const path = try tmp.dir.realpathAlloc(a, "t.csv");

    const m = try MappedCsv.open(a, path, .{});
    defer m.close();
    try std.testing.expectEqual(@as(usize, 2), m.schema.fields.len);
    try std.testing.expectEqualStrings("name", m.schema.fields[1].name);
    try std.testing.expectEqualStrings(body, m.body);

    const n = 3;
    var reassembled = std.array_list.Managed(u8).init(a);
    for (0..n) |i| {
        const c = m.chunk(i, n);
        if (c.len > 0) try std.testing.expectEqual(@as(u8, '\n'), c[c.len - 1]);
        try reassembled.appendSlice(c);
    }
    try std.testing.expectEqualStrings(body, reassembled.items);

    var total: usize = 0;
    for (0..16) |i| total += m.chunk(i, 16).len;
    try std.testing.expectEqual(body.len, total);
}

test "CsvSliceReader over a MappedCsv chunk parses only its rows" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id\n1\n2\n3\n4\n" });
    const path = try tmp.dir.realpathAlloc(a, "t.csv");
    const m = try MappedCsv.open(a, path, .{});
    defer m.close();

    var rows: usize = 0;
    for (0..2) |i| {
        var r = CsvSliceReader{ .data = m.chunk(i, 2), .schema = &m.schema };
        while (try r.next(a)) |b| rows += b.len;
    }
    try std.testing.expectEqual(@as(usize, 4), rows);
}

test "splitCodec / splitArchive / dataName walk the container chain" {
    try std.testing.expectEqual(Codec.gzip, splitCodec("a/orders.csv.gz").codec);
    try std.testing.expectEqualStrings("a/orders.csv", splitCodec("a/orders.csv.gz").rest);
    try std.testing.expectEqual(Codec.zstd, splitCodec("x.csv.ZST").codec);
    try std.testing.expectEqual(Codec.none, splitCodec("x.csv").codec);
    // A query string is not part of the name.
    try std.testing.expectEqual(Codec.gzip, splitCodec("https://h/x.csv.gz?sig=1").codec);

    try std.testing.expect(splitArchive("plain.csv") == null);
    const one = splitArchive("inf.zip").?;
    try std.testing.expectEqualStrings("inf.zip", one.archive);
    try std.testing.expect(one.member == null);
    const two = splitArchive("d/inf.zip :: inner/data.csv").?;
    try std.testing.expectEqualStrings("d/inf.zip", two.archive);
    try std.testing.expectEqualStrings("inner/data.csv", two.member.?);
    // No spaces required.
    try std.testing.expectEqualStrings("a.csv", splitArchive("i.zip::a.csv").?.member.?);

    // The innermost name is the one that carries the format.
    try std.testing.expectEqualStrings("inner/data.csv", dataName("d/inf.zip :: inner/data.csv"));
    try std.testing.expectEqualStrings("rows.csv", dataName("rows.csv.gz"));
    try std.testing.expectEqualStrings("m.csv", dataName("a.zip :: m.csv.gz"));
    // An s3 URL is not mistaken for an archive reference despite its colons.
    try std.testing.expect(splitArchive("s3://bucket/key.csv") == null);
}

const fx_gz = @embedFile("testdata/rows.csv.gz");

test "csv reader: a gzip stream decompresses and types normally" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "rows.csv.gz", .data = fx_gz });
    const path = try tmp.dir.realpathAlloc(a, "rows.csv.gz");

    const r = try CsvReader.open(a, path, .{});
    defer r.close();
    try std.testing.expectEqual(@as(usize, 2), r.schema.fields.len);
    // Type sniffing runs on the decompressed bytes, so `id` is still an int.
    try std.testing.expectEqual(types.TypeKind.int, r.schema.fields[0].ty.kind);
    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqualStrings("gamma", b.columns[1].data.bytes.at(2));
}

const fx_zst = @embedFile("testdata/rows.csv.zst");

test "csv reader: a zstd stream decompresses" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "rows.csv.zst", .data = fx_zst });
    const path = try tmp.dir.realpathAlloc(a, "rows.csv.zst");

    // Regression: sized from `ContentEncoding.minBufferCapacity()` this failed with
    // `OutputBufferUndersize`, because zstd needs the window plus a max block.
    const r = try CsvReader.open(a, path, .{});
    defer r.close();
    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqualStrings("gamma", b.columns[1].data.bytes.at(2));
}

test "csv reader: a zip member reads, and MappedCsv refuses both containers" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = @embedFile("testdata/two_members.zip") });
    try tmp.dir.writeFile(.{ .sub_path = "rows.csv.gz", .data = fx_gz });
    const zpath = try tmp.dir.realpathAlloc(a, "t.zip");
    const gpath = try tmp.dir.realpathAlloc(a, "rows.csv.gz");

    const ref = try std.fmt.allocPrint(a, "{s} :: a.csv", .{zpath});
    const r = try CsvReader.open(a, ref, .{});
    defer r.close();
    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 2), b.len);

    // Neither can be cut on byte offsets, so the parallel paths must fall back.
    try std.testing.expectError(error.NotMappable, MappedCsv.open(a, ref, .{}));
    try std.testing.expectError(error.NotMappable, MappedCsv.open(a, gpath, .{}));
    try std.testing.expectError(error.NotMappable, MappedCsv.open(a, zpath, .{}));
}

test "Encoding.parse accepts the spellings the wild uses" {
    try std.testing.expectEqual(Encoding.latin1, Encoding.parse("latin1").?);
    try std.testing.expectEqual(Encoding.latin1, Encoding.parse("ISO-8859-1").?);
    try std.testing.expectEqual(Encoding.latin1, Encoding.parse("iso_8859_1").?);
    try std.testing.expectEqual(Encoding.cp1252, Encoding.parse("cp1252").?);
    try std.testing.expectEqual(Encoding.cp1252, Encoding.parse("Windows-1252").?);
    try std.testing.expectEqual(Encoding.utf8, Encoding.parse("UTF8").?);
    try std.testing.expect(Encoding.parse("latin9") == null);
    try std.testing.expect(Encoding.parse("") == null);
}

test "decodeField: latin-1 and cp1252 widen to UTF-8, ASCII is passed through" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // Pure ASCII returns the very same slice — the no-allocation fast path.
    const ascii = "2020-10-27";
    try std.testing.expect((try decodeField(a, .latin1, ascii)).ptr == ascii.ptr);
    // And UTF-8 is never touched whatever the bytes are.
    try std.testing.expect((try decodeField(a, .utf8, "\xC7\xC3")).len == 2);

    // LIQUIDA<C7><C3>O in latin-1 is LIQUIDAÇÃO.
    try std.testing.expectEqualStrings("LIQUIDAÇÃO", try decodeField(a, .latin1, "LIQUIDA\xC7\xC3O"));
    // 0x93/0x94 are undefined in latin-1 (C1 controls) but the curly quotes in
    // cp1252, which is what a Windows-authored file actually means by them.
    try std.testing.expectEqualStrings("“hi”", try decodeField(a, .cp1252, "\x93hi\x94"));
    try std.testing.expectEqualStrings("€", try decodeField(a, .cp1252, "\x80"));
    // An undefined cp1252 slot is replacement, not an invented codepoint.
    try std.testing.expectEqualStrings("\u{FFFD}", try decodeField(a, .cp1252, "\x81"));
}

test "csv dialect: a semicolon latin-1 file reads as UTF-8 columns" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Shaped like the CVM fund registry: `;` separated, ISO-8859-1, CRLF.
    try tmp.dir.writeFile(.{
        .sub_path = "cad.csv",
        .data = "SIT;DENOM\r\nLIQUIDA\xC7\xC3O;A\xC7\xD5ES\r\nCANCELADA;PLAIN\r\n",
    });
    const path = try tmp.dir.realpathAlloc(a, "cad.csv");

    const r = try CsvReader.open(a, path, .{ .delim = ';', .encoding = .latin1 });
    defer r.close();
    try std.testing.expectEqual(@as(usize, 2), r.schema.fields.len);
    try std.testing.expectEqualStrings("SIT", r.schema.fields[0].name);
    try std.testing.expectEqualStrings("DENOM", r.schema.fields[1].name);

    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expectEqualStrings("LIQUIDAÇÃO", b.columns[0].data.bytes.at(0));
    try std.testing.expectEqualStrings("AÇÕES", b.columns[1].data.bytes.at(0));
    try std.testing.expectEqualStrings("CANCELADA", b.columns[0].data.bytes.at(1));
}

test "csv dialect: the same file read with the default dialect is one column" {
    // Not a bug to fix, just the reason the option had to exist: a `;` file holds
    // no commas, so the comma reader sees one field per row.
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "cad.csv", .data = "SIT;DENOM\nX;Y\n" });
    const path = try tmp.dir.realpathAlloc(a, "cad.csv");
    const r = try CsvReader.open(a, path, .{});
    defer r.close();
    try std.testing.expectEqual(@as(usize, 1), r.schema.fields.len);
}

test "csv: text after a closing quote stays in the field" {
    // From the CVM registry: `"1" é calculado de acordo…`, a field that opens
    // quoted and continues unquoted. Ending it at the quote made the remainder
    // look like the next column, shifting every later value of the row.
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "q.csv", .data = "a;b;c\n1;\"2\" and more;3\n" });
    const path = try tmp.dir.realpathAlloc(a, "q.csv");
    const r = try CsvReader.open(a, path, .{ .delim = ';' });
    defer r.close();
    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 1), b.len);
    try std.testing.expectEqualStrings("2 and more", b.columns[1].data.bytes.at(0));
    // The row's last column must still be its own value, not shifted.
    try std.testing.expectEqualStrings("3", b.columns[2].data.bytes.at(0));
}

test "csv writer: the delimiter carries to the header, rows and quoting" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ base, "o.csv" });

    var names = [_][]const u8{ "x", "y" };
    const schema = try stringSchema(a, &names);
    const w = try CsvWriter.open(a, path, schema, .truncate, .{ .delim = ';' });
    // A value holding the delimiter must be quoted; one holding a comma must not.
    const b = try parseSlice(a, &schema, "a;b,c\n");
    try w.writeBatch(a, b);
    try w.close();

    const got = try tmp.dir.readFileAlloc(a, "o.csv", 1 << 16);
    try std.testing.expectEqualStrings("x;y\n\"a;b\";c\n", got);
}

/// A schema with one field per (name, kind) pair, all nullable — for exercising the
/// writer's typed paths, which `stringSchema` cannot reach.
fn typedSchema(a: std.mem.Allocator, names: []const []const u8, kinds: []const types.TypeKind) !types.Schema {
    const fields = try a.alloc(types.Schema.Field, names.len);
    for (names, kinds, 0..) |n, k, i| fields[i] = .{ .name = n, .ty = types.Type.init(k).asNullable() };
    return .{ .fields = fields };
}

test "csv writer: typed columns render without quoting, and nulls stay empty" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ base, "o.csv" });

    // Numbers take the no-allocation path that formats straight into the output
    // buffer. This pins the exact bytes it produces, including a null (bare empty,
    // which is how the reader spells null) and a negative value. The kinds are the
    // ones a CSV source can actually yield — `TypeSniffer` resolves int, float or
    // string — while `eval`'s equivalence test covers date/time/decimal rendering.
    var names = [_][]const u8{ "i", "f", "s" };
    var kinds = [_]types.TypeKind{ .int, .float, .string };
    const schema = try typedSchema(a, &names, &kinds);
    const w = try CsvWriter.open(a, path, schema, .truncate, .{});
    const b = try parseSlice(a, &schema, "1,2.5,ok\n-7,,\n");
    try w.writeBatch(a, b);
    try w.close();

    const got = try tmp.dir.readFileAlloc(a, "o.csv", 1 << 16);
    try std.testing.expectEqualStrings("i,f,s\n1,2.5,ok\n-7,,\n", got);
}

test "csv writer: a delimiter that collides with a number still round-trips" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    // The fast path skips the quote scan because an ordinary delimiter cannot occur
    // in a number. These two can: `.` inside a float, `-` in front of a negative.
    // Getting this wrong writes `2.5` as two fields under `delim = '.'` — a silently
    // corrupt file that still parses.
    const cases = [_]struct { delim: u8, kinds: [2]types.TypeKind, csv: []const u8, want: []const u8 }{
        .{ .delim = '.', .kinds = .{ .float, .int }, .csv = "2.5,7\n", .want = "f.i\n\"2.5\".7\n" },
        .{ .delim = '-', .kinds = .{ .int, .int }, .csv = "-3,7\n", .want = "f-i\n\"-3\"-7\n" },
    };

    for (cases) |c| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const base = try tmp.dir.realpathAlloc(a, ".");
        const path = try std.fs.path.join(a, &.{ base, "o.csv" });

        var names = [_][]const u8{ "f", "i" };
        var kinds = c.kinds;
        const schema = try typedSchema(a, &names, &kinds);
        // The input is comma-separated; only the *output* dialect is exotic.
        var in_names = [_][]const u8{ "f", "i" };
        const in_schema = try typedSchema(a, &in_names, &kinds);
        const b = try parseSlice(a, &in_schema, c.csv);

        const w = try CsvWriter.open(a, path, schema, .truncate, .{ .delim = c.delim });
        try w.writeBatch(a, b);
        try w.close();

        const got = try tmp.dir.readFileAlloc(a, "o.csv", 1 << 16);
        try std.testing.expectEqualStrings(c.want, got);

        // And a reader on the same dialect must see two fields, not three — the
        // quoting has to survive the round trip, or the file is silently corrupt.
        var rd = CsvSliceReader{
            .data = got[std.mem.indexOfScalar(u8, got, '\n').? + 1 ..],
            .schema = &schema,
            .dialect = .{ .delim = c.delim },
        };
        const back = (try rd.next(a)).?;
        try std.testing.expectEqual(@as(usize, 1), back.len);
        try std.testing.expectEqual(@as(usize, 2), back.columns.len);
        try std.testing.expectEqual(@as(i64, 7), back.columns[1].getValue(0).int);
    }
}

test "scalarCanNeedQuote: only a delimiter a number could contain forces the scan" {
    // The ordinary delimiters, where the fast path applies.
    for ([_]u8{ ',', ';', '\t', '|', '#', '^' }) |d| try std.testing.expect(!scalarCanNeedQuote(d));
    // Every character a number, decimal, date, time or timestamp rendering can emit.
    for ([_]u8{ '0', '9', '+', '-', '.', ':', 'e', 'E', ' ', '"', '\n', '\r' }) |d| {
        try std.testing.expect(scalarCanNeedQuote(d));
    }
}

test "scanRecord: a newline inside a quoted field stays in the value" {
    // RFC 4180 allows it and basalt's own writer emits it, so cutting records on
    // the first raw newline turned one row into several — silently.
    const data = "1,\"line A\nline B\"\n2,plain\n";
    const r1 = scanRecord(data, 0, ',');
    try std.testing.expectEqualStrings("1,\"line A\nline B\"", r1.line);
    const r2 = scanRecord(data, r1.next, ',');
    try std.testing.expectEqualStrings("2,plain", r2.line);
    try std.testing.expectEqual(data.len, r2.next);

    // An escaped `""` inside a quoted field does not end the quote.
    const esc = "a,\"he said \"\"hi\"\"\nand left\"\n";
    try std.testing.expectEqualStrings("a,\"he said \"\"hi\"\"\nand left\"", scanRecord(esc, 0, ',').line);

    // CRLF is still trimmed, and an unterminated final record still returns.
    try std.testing.expectEqualStrings("x,y", scanRecord("x,y\r\n", 0, ',').line);
    try std.testing.expectEqualStrings("x,y", scanRecord("x,y", 0, ',').line);
}

test "quotesOpen / hasQuotedNewline drive the continuation and split decisions" {
    try std.testing.expect(quotesOpen("1,\"line A", ','));
    try std.testing.expect(!quotesOpen("1,\"line A\"", ','));
    try std.testing.expect(!quotesOpen("plain,row", ','));
    // `""` is an escaped quote: two marks, so the field is still closed.
    try std.testing.expect(!quotesOpen("a,\"he said \"\"hi\"\"\"", ','));

    try std.testing.expect(MappedCsv.hasQuotedNewline("1,\"a\nb\"\n", ','));
    try std.testing.expect(!MappedCsv.hasQuotedNewline("1,\"a b\"\n2,c\n", ','));
    try std.testing.expect(!MappedCsv.hasQuotedNewline("1,a\n2,b\n", ','));
}
