//! Minimal CSV source and sink. The source reads a header row and produces
//! batches of columns typed by sniffing; the sink writes a header then
//! serializes each batch. RFC 4180 quoting: fields containing the delimiter, a
//! quote or a newline are double-quoted with `""` escaping, and a newline inside
//! a quoted field belongs to the value. Only a quote at the start of a field
//! opens one; a mid-field `"` (`12" pipe`) is data, since treating it as an
//! opener once swallowed every following row. Unquoted empty is null and `""`
//! is an empty string. A physical line is at most 64 KiB (`StreamTooLong`).
//!
//! The delimiter and the source encoding are a `Dialect`, because the CSV a
//! statistics office or regulator publishes is often `;`-separated latin-1.
//! Only single-byte encodings exist, since a multi-byte one would break the
//! parallel reader's byte-range chunking. Byte `b` of latin-1 is U+00`b`, so no
//! byte sequence can hold a delimiter or newline. Files labelled latin-1 are
//! often really cp1252, whose 0x80–0x9F block holds curly quotes, dashes and the
//! euro sign (its five undefined slots decode to U+FFFD).
//!
//! Compression is chosen by suffix, and the inner name picks the format: gzip and
//! zstd through `std.http.Decompress`, the same path HTTP `Content-Encoding`
//! uses. No `.xz`: std's decoder still has the old reader shape and the files are
//! rare. Concatenated gzip members (`pigz`, `bgzip`, appends) read as one stream,
//! where std stops after the first. `data.zip :: member.csv` names a member of an
//! archive (spaces optional); a bare `.zip` must hold exactly one member.
//!
//! Types are sniffed from the first `SAMPLE_ROWS` lines by the same rules in the
//! serial and parallel readers, so both agree on a file's schema: int ⊂ float ⊂
//! string, and a column of only `YYYY-MM-DD` cells is a DATE. A quoted cell is
//! text, and a leading zero or `+` rules out int so "007" round-trips. A later
//! cell that no longer parses as the inferred type is an error, not corruption.
//! When a query reads only some columns, the rest are cut past, never parsed.
//!
//! A prefix read or a folder is one CSV: every later file must repeat the first
//! one's header, or the read would splice mismatched columns, and sniffing goes
//! on into the next file. `MappedCsv` maps a local, uncompressed, non-archive
//! file once so workers parse disjoint newline-aligned chunks in parallel (the
//! parse, not the read, is the bottleneck). A file with a newline inside a quoted
//! field cannot be split, as no offset tells whether a newline is quoted, and
//! falls back to the serial reader, as compressed and archived files do.
//!
//! The sink appends to a `.gz` by adding a gzip member. On failure a local file
//! keeps the rows already flushed (a CSV has no transaction); an object is never
//! committed, so its staged blocks or multipart upload stay invisible (`az://`
//! reaps them after a week, `s3://` only with a lifecycle rule), and an SFTP
//! `.part` is removed. Committing on close is what makes the object appear.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const Batch = @import("../exec/batch.zig").Batch;
const Value = @import("../exec/value.zig").Value;
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const http_client = @import("../net/http_client.zig");
const objstore = @import("../store/objstore.zig");
const sftp = @import("../store/sftp.zig");
const smb = @import("../store/smb.zig");
const zipsrc = @import("zipsrc.zig");
const folder = @import("../connect/folder.zig");
const deflate = @import("deflate.zig");

const BATCH_ROWS = 1024;
const LINE_BUF = 64 * 1024;

pub const Encoding = enum {
    utf8,
    latin1,
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

const cp1252_high = [32]u21{
    0x20AC, 0,      0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
    0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0,      0x017D, 0,
    0,      0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
    0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0,      0x017E, 0x0178,
};

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

/// Not `ContentEncoding.minBufferCapacity()`, which gives zstd only its window:
/// `zstd.Decompress` needs the window plus `block_size_max`, or fails the stream
/// with `OutputBufferUndersize`, as a real 2.6 MB `.csv.zst` did.
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

/// Returns the input untouched when already UTF-8 or all ASCII, as most fields of a
/// latin-1 file are, so the ordinary field costs one scan and no allocation.
/// The column names of a header line, every one a nullable string until sniffed. A
/// quoted name loses its quotes (`""` is one quote) and may hold the delimiter; an
/// unquoted one is trimmed of spaces and tabs.
fn headerFields(arena: std.mem.Allocator, header: []const u8, d: Dialect) ![]types.Schema.Field {
    var fields = std.array_list.Managed(types.Schema.Field).init(arena);
    var i: usize = 0;
    while (true) {
        while (i < header.len and (header[i] == ' ' or header[i] == '\t')) i += 1;
        var name = std.array_list.Managed(u8).init(arena);
        if (i < header.len and header[i] == '"') {
            i += 1;
            while (i < header.len) : (i += 1) {
                if (header[i] != '"') {
                    try name.append(header[i]);
                } else if (i + 1 < header.len and header[i + 1] == '"') {
                    try name.append('"');
                    i += 1;
                } else {
                    i += 1;
                    break;
                }
            }
        }
        const end = std.mem.indexOfScalarPos(u8, header, i, d.delim) orelse header.len;
        try name.appendSlice(std.mem.trim(u8, header[i..end], " \t"));
        try fields.append(.{
            .name = try decodeField(arena, d.encoding, name.items),
            .ty = types.Type.init(.string).asNullable(),
        });
        if (end >= header.len) break;
        i = end + 1;
    }
    return fields.toOwnedSlice();
}

fn decodeField(arena: std.mem.Allocator, enc: Encoding, s: []const u8) ![]const u8 {
    if (enc == .utf8) return s;
    var high = false;
    for (s) |c| if (c >= 0x80) {
        high = true;
        break;
    };
    if (!high) return s;

    var out = try std.array_list.Managed(u8).initCapacity(arena, s.len * 3);
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
        out.appendSliceAssumeCapacity(buf[0..n]);
    }
    return out.items;
}

const Gunzip = struct {
    src: *std.Io.Reader,
    dec: std.compress.flate.Decompress,
    window: []u8,
    interface: std.Io.Reader,

    fn init(self: *Gunzip, src: *std.Io.Reader, window: []u8, buf: []u8) void {
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

pub const CsvReader = struct {
    arena: std.mem.Allocator,
    dialect: Dialect = .{},
    backend: Backend,
    read_buf: [LINE_BUF]u8 = undefined,
    rdr: *std.Io.Reader = undefined,
    schema: types.Schema,
    pending: []const []const u8 = &.{},
    pending_i: usize = 0,
    stream_eof: bool = false,
    done: bool = false,
    rest_urls: []const []const u8 = &.{},
    header_line: []const u8 = "",
    join_buf: std.array_list.Managed(u8) = undefined,
    codec_state: std.http.Decompress = undefined,
    gunzip: Gunzip = undefined,
    codec_bufs: ?CodecBufs = null,
    slot: ?[]const i16 = null,

    const CodecBufs = struct { window: []u8, buf: []u8 };

    const Backend = union(enum) {
        file: FileBackend,
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

    pub fn project(self: *CsvReader, names: []const []const u8) !void {
        const p = (try Projection.of(self.arena, self.schema, names)) orelse return;
        self.slot = p.slot;
        self.schema = p.schema;
    }

    fn decodeAs(self: *CsvReader, codec: Codec) !void {
        switch (codec) {
            .none => {},
            .gzip => {
                const b = self.codec_bufs orelse blk: {
                    const nb = CodecBufs{ .window = try self.arena.alloc(u8, codecBuffer(.gzip)), .buf = try self.arena.alloc(u8, LINE_BUF) };
                    self.codec_bufs = nb;
                    break :blk nb;
                };
                self.gunzip.init(self.rdr, b.window, b.buf);
                self.rdr = &self.gunzip.interface;
            },
            .zstd => {
                const cbuf = try self.arena.alloc(u8, codecBuffer(.zstd));
                self.rdr = std.http.Decompress.init(&self.codec_state, self.rdr, cbuf, .zstd);
            },
        }
    }

    pub fn isUrl(path: []const u8) bool {
        return std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://") or
            objstore.isUrl(path) or sftp.isUrl(path) or smb.isUrl(path);
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
        if (folder.isFolder(path)) {
            const all = try folder.list(arena, path);
            if (all.len == 0) return error.EmptyFolder;
            const urls = try folder.only(arena, all, .csv);
            if (urls.len == 0) return error.NoCsvInFolder;
            return openList(arena, urls, dialect);
        }
        return self.openFirst(path);
    }

    pub fn openList(arena: std.mem.Allocator, files: []const []const u8, dialect: Dialect) !*CsvReader {
        if (files.len == 0) return error.EmptyFolder;
        const self = try arena.create(CsvReader);
        self.* = .{
            .arena = arena,
            .dialect = dialect,
            .backend = undefined,
            .schema = undefined,
            .done = false,
            .join_buf = std.array_list.Managed(u8).init(arena),
            .rest_urls = files[1..],
        };
        return self.openFirst(files[0]);
    }

    fn openFirst(self: *CsvReader, first: []const u8) !*CsvReader {
        const arena = self.arena;

        if (splitArchive(first)) |ar| {
            const m = try zipsrc.openMember(arena, ar.archive, ar.member);
            self.backend = .{ .member = m };
            self.rdr = m.reader;
        } else if (sftp.isUrl(first)) {
            const st = try sftp.Stream.open(arena, first);
            self.backend = .{ .sftp = st };
            self.rdr = &st.interface;
        } else if (smb.isUrl(first)) {
            const st = try smb.Stream.open(arena, first);
            self.backend = .{ .smb = st };
            self.rdr = &st.interface;
        } else if (isUrl(first)) {
            const hf = try arena.create(HttpFetch);
            hf.* = .{ .client = http_client.initClient(arena), .req = undefined, .response = undefined };
            errdefer hf.client.deinit();
            var req_url = first;
            var extra: []const std.http.Header = &.{};
            if (objstore.isUrl(first)) {
                const obj = try objstore.parse(arena, first);
                req_url = obj.url;
                extra = try obj.getHeaders(arena);
            }
            const uri = std.Uri.parse(req_url) catch return error.InvalidUrl;
            startHttp(hf, uri, extra) catch |e| switch (e) {
                error.TlsInitializationFailed => {
                    const h = http_client.uriHost(uri) orelse return e;
                    if (!http_client.repairBundle(arena, &hf.client.ca_bundle, h, uri.port orelse 443)) return e;
                    hf.client.next_https_rescan_certs = false;
                    try startHttp(hf, uri, extra);
                },
                else => return e,
            };
            errdefer hf.req.deinit();
            const code = @intFromEnum(hf.response.head.status);
            if (code != 200) return http_client.statusError(code);
            self.backend = .{ .http = hf };
            const ce = hf.response.head.content_encoding;
            if (ce == .compress) return error.UnsupportedCompressionMethod;
            const win = switch (ce) {
                .zstd => codecBuffer(.zstd),
                .gzip, .deflate => codecBuffer(.gzip),
                .compress, .identity => 0,
            };
            const dbuf: []u8 = if (win > 0) try arena.alloc(u8, win) else &.{};
            self.rdr = hf.response.readerDecompressing(&hf.transfer_buf, &hf.decompress, dbuf);
        } else {
            self.backend = .{ .file = .{ .file = try std.fs.cwd().openFile(first, .{}), .fr = undefined } };
            self.backend.file.fr = self.backend.file.file.reader(&self.read_buf);
            self.rdr = &self.backend.file.fr.interface;
        }

        try self.decodeAs(splitCodec(if (splitArchive(first)) |ar| (ar.member orelse ar.archive) else first).codec);

        const header = stripBom((try self.readLine()) orelse return error.EmptyCsv);
        self.header_line = try arena.dupe(u8, std.mem.trim(u8, header, " \t\r"));
        var fields = std.array_list.Managed(types.Schema.Field).fromOwnedSlice(arena, try headerFields(arena, header, self.dialect));

        var sniff = try TypeSniffer.init(arena, fields.items.len, self.dialect.delim);
        var pending = std.array_list.Managed([]const u8).init(arena);
        while (pending.items.len < SAMPLE_ROWS) {
            const line = (try self.readLine()) orelse {
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
                    if (try self.advance()) break :blk (try self.readLine()) orelse {
                        self.done = true;
                        break;
                    };
                    self.done = true;
                    break;
                };
            }
            if (line.len == 0) continue;
            try splitInto(arena, line, builders, self.dialect, self.slot);
            rows += 1;
        }
        if (rows == 0) return null;

        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
    }

    fn advance(self: *CsvReader) !bool {
        if (self.rest_urls.len == 0) return false;
        const url = self.rest_urls[0];
        self.rest_urls = self.rest_urls[1..];

        switch (self.backend) {
            .http => |hf| hf.req.deinit(),
            .sftp => |st| {
                st.close();
                const st2 = try sftp.Stream.open(self.arena, url);
                self.backend = .{ .sftp = st2 };
                self.rdr = &st2.interface;
                try self.decodeAs(splitCodec(url).codec);
                const hdr = (try self.readLine()) orelse return error.EmptyCsv;
                if (!std.mem.eql(u8, std.mem.trim(u8, stripBom(hdr), " \t\r"), self.header_line)) return error.CsvHeaderMismatch;
                return true;
            },
            .smb => |st| {
                st.close();
                const st2 = try smb.Stream.open(self.arena, url);
                self.backend = .{ .smb = st2 };
                self.rdr = &st2.interface;
                try self.decodeAs(splitCodec(url).codec);
                const hdr = (try self.readLine()) orelse return error.EmptyCsv;
                if (!std.mem.eql(u8, std.mem.trim(u8, stripBom(hdr), " \t\r"), self.header_line)) return error.CsvHeaderMismatch;
                return true;
            },
            .file => |*f| {
                f.file.close();
                f.file = try std.fs.cwd().openFile(url, .{});
                f.fr = f.file.reader(&self.read_buf);
                self.rdr = &f.fr.interface;
                try self.decodeAs(splitCodec(url).codec);
                const hdr = (try self.readLine()) orelse return error.EmptyCsv;
                if (!std.mem.eql(u8, std.mem.trim(u8, stripBom(hdr), " \t\r"), self.header_line)) return error.CsvHeaderMismatch;
                return true;
            },
            .member => {},
        }
        const hf = try self.arena.create(HttpFetch);
        hf.* = .{ .client = http_client.initClient(self.arena), .req = undefined, .response = undefined };
        const obj = try objstore.parse(self.arena, url);
        const extra = try obj.getHeaders(self.arena);
        const uri = std.Uri.parse(obj.url) catch return error.InvalidUrl;
        try startHttp(hf, uri, extra);
        const code = @intFromEnum(hf.response.head.status);
        if (code != 200) return http_client.statusError(code);
        self.backend = .{ .http = hf };
        const ce = hf.response.head.content_encoding;
        if (ce == .compress) return error.UnsupportedCompressionMethod;
        const win = ce.minBufferCapacity();
        const dbuf: []u8 = if (win > 0) try self.arena.alloc(u8, win) else &.{};
        self.rdr = hf.response.readerDecompressing(&hf.transfer_buf, &hf.decompress, dbuf);

        try self.decodeAs(splitCodec(url).codec);
        const hdr = (try self.readLine()) orelse return error.EmptyCsv;
        if (!std.mem.eql(u8, std.mem.trim(u8, stripBom(hdr), " \t\r"), self.header_line)) return error.CsvHeaderMismatch;
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
            .sftp => |st| st.close(),
            .smb => |st| st.close(),
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

    /// One record, which may span physical lines. Balanced quotes take the zero-copy
    /// path; only a continued record is joined, into a buffer reused across records.
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

/// A header without the UTF-8 byte-order mark Excel writes before "CSV UTF-8" files,
/// which would otherwise stick to the first column's name.
fn stripBom(line: []const u8) []const u8 {
    return if (std.mem.startsWith(u8, line, "\xef\xbb\xbf")) line[3..] else line;
}

pub const MappedCsv = struct {
    data: []align(std.heap.page_size_min) const u8,
    body: []const u8,
    schema: types.Schema,
    file: std.fs.File,
    quoted_newlines: bool = false,
    dialect: Dialect = .{},
    slot: ?[]const i16 = null,

    pub fn project(self: *MappedCsv, arena: std.mem.Allocator, names: []const []const u8) !void {
        const p = (try Projection.of(arena, self.schema, names)) orelse return;
        self.slot = p.slot;
        self.schema = p.schema;
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8, dialect: Dialect) !*MappedCsv {
        if (splitCodec(path).codec != .none or splitArchive(path) != null) return error.NotMappable;
        const self = try arena.create(MappedCsv);
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();
        const size = (try file.stat()).size;
        if (size == 0) return error.EmptyCsv;
        const data = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(data);

        const nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.EmptyCsv;
        var header = stripBom(data[0..nl]);
        if (header.len > 0 and header[header.len - 1] == '\r') header = header[0 .. header.len - 1];
        var fields = std.array_list.Managed(types.Schema.Field).fromOwnedSlice(arena, try headerFields(arena, header, dialect));

        const body = data[nl + 1 ..];
        var sniff = try TypeSniffer.init(arena, fields.items.len, dialect.delim);
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

    /// The i-th of `n` newline-aligned chunks of the body; a line belongs to the
    /// chunk holding its first byte. May be empty.
    pub fn chunk(self: *const MappedCsv, i: usize, n: usize) []const u8 {
        const lo = self.lineStart(self.body.len * i / n);
        const hi = self.lineStart(self.body.len * (i + 1) / n);
        return self.body[lo..hi];
    }

    /// Skipped when the file has no quote at all, so the common case costs one `memchr`.
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

pub const CsvSliceReader = struct {
    data: []const u8,
    pos: usize = 0,
    schema: *const types.Schema,
    dialect: Dialect = .{},
    slot: ?[]const i16 = null,

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
            try splitInto(arena, line, builders, self.dialect, self.slot);
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

pub const SAMPLE_ROWS = 1024;

const TypeSniffer = struct {
    const ColState = struct { seen: bool = false, all_int: bool = true, all_float: bool = true, all_date: bool = true };
    cols: []ColState,
    delim: u8,

    fn init(arena: std.mem.Allocator, ncols: usize, delim: u8) !TypeSniffer {
        const cols = try arena.alloc(ColState, ncols);
        for (cols) |*c| c.* = .{};
        return .{ .cols = cols, .delim = delim };
    }

    fn feed(self: *TypeSniffer, line: []const u8) void {
        var i: usize = 0;
        for (self.cols) |*c| {
            if (i < line.len and line[i] == '"') {
                c.seen = true;
                c.all_int = false;
                c.all_float = false;
                c.all_date = false;
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
                while (i < line.len and line[i] != self.delim) i += 1;
                const raw = line[start..i];
                if (raw.len > 0) {
                    c.seen = true;
                    if (c.all_date and eval.parseIsoDate(raw) == null) c.all_date = false;
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
            if (i < line.len and line[i] == self.delim) i += 1;
        }
    }

    fn resolve(self: *const TypeSniffer, j: usize) types.Type {
        const c = self.cols[j];
        const k: types.TypeKind = if (!c.seen) .string else if (c.all_int) .int else if (c.all_float) .float else if (c.all_date) .date else .string;
        return types.Type.init(k).asNullable();
    }
};

fn appendCell(b: *column.Builder, raw: []const u8, quoted: bool) !void {
    if (raw.len == 0) return b.append(if (quoted and b.ty.kind == .string) Value{ .string = raw } else .null);
    switch (b.ty.kind) {
        .int => try b.appendInt(parseIntFast(raw) orelse (std.fmt.parseInt(i64, raw, 10) catch return error.CsvTypeMismatch)),
        .float => try b.appendFloat(parseFloatFast(raw) orelse (std.fmt.parseFloat(f64, raw) catch return error.CsvTypeMismatch)),
        .date => try b.append(.{ .date = @intCast(eval.parseIsoDate(raw) orelse return error.CsvTypeMismatch) }),
        else => try b.appendStr(raw),
    }
}

/// `[-]digits`, at most 18 of them so no i64 overflow: what a CSV cell needs of
/// `std.fmt.parseInt`, without its per-call base, underscore and sign handling.
fn parseIntFast(s: []const u8) ?i64 {
    if (s.len == 0 or s.len > 19) return null;
    var i: usize = 0;
    const neg = s[0] == '-';
    if (neg) i = 1;
    if (s.len - i == 0 or s.len - i > 18) return null;
    var v: i64 = 0;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c < '0' or c > '9') return null;
        v = v * 10 + (c - '0');
    }
    return if (neg) -v else v;
}

test "parseIntFast agrees with std and declines what it cannot prove" {
    for ([_][]const u8{ "0", "7", "-7", "123456789012345678", "-123456789012345678", "007" }) |s| {
        try std.testing.expectEqual(try std.fmt.parseInt(i64, s, 10), parseIntFast(s).?);
    }
    for ([_][]const u8{ "", "-", "+1", "1_000", "0x10", "1234567890123456789", "1.0", " 1" }) |s| {
        try std.testing.expect(parseIntFast(s) == null);
    }
}

/// `[-]digits[.digits]` with at most 15 significant digits, as an integer mantissa
/// over a power of ten: both exact doubles, so one division rounds correctly
/// (Clinger's fast path) and matches `std.fmt.parseFloat`. Anything else is null.
fn parseFloatFast(s: []const u8) ?f64 {
    if (s.len == 0 or s.len > 18) return null;
    var i: usize = 0;
    const neg = s[0] == '-';
    if (neg) i = 1;
    var mant: u64 = 0;
    var digits: usize = 0;
    var frac: usize = 0;
    var seen_dot = false;
    while (i < s.len) : (i += 1) {
        const c = s[i];
        if (c == '.') {
            if (seen_dot) return null;
            seen_dot = true;
            continue;
        }
        if (c < '0' or c > '9') return null;
        mant = mant * 10 + (c - '0');
        digits += 1;
        if (seen_dot) frac += 1;
    }
    if (digits == 0 or digits > 15) return null;
    const pow10 = [_]f64{ 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22 };
    if (frac >= pow10.len) return null;
    const v = @as(f64, @floatFromInt(mant)) / pow10[frac];
    return if (neg) -v else v;
}

test "parseFloatFast agrees with std on plain decimals and declines the rest" {
    for ([_][]const u8{ "0", "1", "-1", "3.25", "-0.001", "123456.789", "99.90", "0.1", "1234567890.12345" }) |s| {
        try std.testing.expectEqual(try std.fmt.parseFloat(f64, s), parseFloatFast(s).?);
    }
    for ([_][]const u8{ "", "-", ".", "1e5", "+1", "1.2.3", "abc", "1234567890123456", "nan" }) |s| {
        try std.testing.expect(parseFloatFast(s) == null);
    }
}

/// One record from `data[start]` and where the next begins. Records were once cut
/// on the first raw newline, so basalt could not read back its own quoted newlines.
/// A quoteless line ends at the next newline, found by two SIMD scans.
fn scanRecord(data: []const u8, start: usize, delim: u8) struct { line: []const u8, next: usize } {
    if (std.mem.indexOfScalarPos(u8, data, start, '\n')) |nl| {
        if (std.mem.indexOfScalar(u8, data[start..nl], '"') == null) {
            var end = nl;
            if (end > start and data[end - 1] == '\r') end -= 1;
            return .{ .line = data[start..end], .next = nl + 1 };
        }
    }
    var i = start;
    var in_q = false;
    var at_field = true;
    while (i < data.len) : (i += 1) {
        const c = data[i];
        if (c == '"') {
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

/// Whether a quoted field is left open. `""` counts two, so parity is the state;
/// a quoteless line is one SIMD scan (a byte walk capped the parse at ~75 MB/s).
fn quotesOpen(line: []const u8, delim: u8) bool {
    const first_q = std.mem.indexOfScalar(u8, line, '"') orelse return false;
    var in_q = false;
    var at_field = first_q == 0 or line[first_q - 1] == delim;
    var i: usize = first_q;
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

pub const Projection = struct {
    slot: []const i16,
    schema: types.Schema,

    /// Null when every column is wanted. A query naming none (a `COUNT(*)`) keeps the
    /// first, as a batch still needs a column to count.
    pub fn of(arena: std.mem.Allocator, full: types.Schema, names: []const []const u8) !?Projection {
        const slot = try arena.alloc(i16, full.fields.len);
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        for (full.fields, slot) |f, *sl| {
            sl.* = -1;
            for (names) |n| if (std.mem.eql(u8, n, f.name)) {
                sl.* = @intCast(fields.items.len);
                try fields.append(f);
                break;
            };
        }
        if (fields.items.len == full.fields.len) return null;
        if (fields.items.len == 0 and full.fields.len > 0) {
            slot[0] = 0;
            try fields.append(full.fields[0]);
        }
        return .{ .slot = slot, .schema = .{ .fields = try fields.toOwnedSlice() } };
    }
};

/// Missing trailing fields are null; extra ones are ignored. A quoted field with no
/// `""` is a slice of the line. Text after a closing quote stays in the field up to
/// the delimiter: ending at the quote once silently shifted a row's values by one.
fn splitInto(arena: std.mem.Allocator, line: []const u8, builders: []column.Builder, d: Dialect, slot: ?[]const i16) !void {
    const ncols = if (slot) |sl| sl.len else builders.len;
    if (std.mem.indexOfScalar(u8, line, '"') == null) return splitPlain(arena, line, builders, d, slot, ncols);
    var i: usize = 0;
    var col: usize = 0;
    while (col < ncols) : (col += 1) {
        const dest: ?*column.Builder = if (slot) |sl| (if (sl[col] >= 0) &builders[@intCast(sl[col])] else null) else &builders[col];
        if (i < line.len and line[i] == '"') {
            i += 1;
            const start = i;
            var buf: ?std.array_list.Managed(u8) = null;
            while (i < line.len) {
                const q = std.mem.indexOfScalarPos(u8, line, i, '"') orelse line.len;
                if (buf) |*bb| try bb.appendSlice(line[i..q]);
                i = q;
                if (i >= line.len) break;
                if (i + 1 < line.len and line[i + 1] == '"') {
                    if (buf == null and dest != null) {
                        buf = std.array_list.Managed(u8).init(arena);
                        try buf.?.appendSlice(line[start..i]);
                    }
                    if (buf) |*bb| try bb.append('"');
                    i += 2;
                    continue;
                }
                break;
            }
            const end = i;
            if (i < line.len) i += 1;
            const tail_end = std.mem.indexOfScalarPos(u8, line, i, d.delim) orelse line.len;
            if (tail_end > i and dest != null) {
                if (buf == null) {
                    buf = std.array_list.Managed(u8).init(arena);
                    try buf.?.appendSlice(line[start..end]);
                }
                try buf.?.appendSlice(line[i..tail_end]);
            }
            i = tail_end;
            if (dest) |bld| {
                const raw = if (buf) |*bb| try bb.toOwnedSlice() else line[start..end];
                try putCell(arena, bld, raw, true, d.encoding);
            }
            if (i < line.len and line[i] == d.delim) i += 1;
        } else {
            const start = i;
            i = std.mem.indexOfScalarPos(u8, line, i, d.delim) orelse line.len;
            if (dest) |bld| try putCell(arena, bld, line[start..i], false, d.encoding);
            if (i < line.len and line[i] == d.delim) i += 1;
        }
    }
}

/// A quoteless line, its delimiters found 32 bytes at a time as a walked bitmask.
fn splitPlain(arena: std.mem.Allocator, line: []const u8, builders: []column.Builder, d: Dialect, slot: ?[]const i16, ncols: usize) !void {
    const V = 32;
    const Vec = @Vector(V, u8);
    const dv: Vec = @splat(d.delim);
    var col: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    scan: {
        while (i + V <= line.len) : (i += V) {
            var m: u32 = @bitCast(@as(Vec, line[i..][0..V].*) == dv);
            while (m != 0) : (m &= m - 1) {
                const p = i + @ctz(m);
                try putField(arena, builders, slot, col, line[start..p], d.encoding);
                start = p + 1;
                col += 1;
                if (col == ncols) break :scan;
            }
        }
        while (i < line.len) : (i += 1) if (line[i] == d.delim) {
            try putField(arena, builders, slot, col, line[start..i], d.encoding);
            start = i + 1;
            col += 1;
            if (col == ncols) break :scan;
        };
        try putField(arena, builders, slot, col, line[start..], d.encoding);
        col += 1;
        while (col < ncols) : (col += 1) try putField(arena, builders, slot, col, "", d.encoding);
    }
}

inline fn putField(arena: std.mem.Allocator, builders: []column.Builder, slot: ?[]const i16, col: usize, raw: []const u8, enc: Encoding) !void {
    if (slot) |sl| {
        if (sl[col] < 0) return;
        return putCell(arena, &builders[@intCast(sl[col])], raw, false, enc);
    }
    return putCell(arena, &builders[col], raw, false, enc);
}

inline fn putCell(arena: std.mem.Allocator, b: *column.Builder, raw: []const u8, quoted: bool, enc: Encoding) !void {
    return appendCell(b, if (enc == .utf8) raw else try decodeField(arena, enc, raw), quoted);
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
    owns_file: bool = true,
    gz: ?*deflate.Gzip = null,

    const Backend = union(enum) {
        file: std.fs.File,
        object: objstore.Writer,
    };

    fn out(self: *CsvWriter) *std.Io.Writer {
        if (self.gz) |g| return &g.interface;
        return self.raw();
    }

    fn raw(self: *CsvWriter) *std.Io.Writer {
        return switch (self.backend) {
            .file => &self.fw.interface,
            .object => |o| o.io,
        };
    }

    /// Recovers the error an object destination actually hit (see
    /// `objstore.Writer.specific`); a file's error is already the real one.
    fn specific(self: *CsvWriter, e: anyerror) anyerror {
        return switch (self.backend) {
            .file => e,
            .object => |o| o.specific(e),
        };
    }

    /// `.append` resumes at the file's end and writes the header only to an empty file,
    /// so a second header never lands among the rows. Objects, SFTP and SMB are written
    /// whole (through a `.part`), so they cannot append.
    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, mode: driver.FileMode, dialect: Dialect) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        var header = true;
        if (sftp.isUrl(path)) {
            if (mode == .append) return error.AppendNotSupported;
            self.* = .{ .backend = .{ .object = objstore.writer(try sftp.Upload.open(arena, path)) } };
        } else if (smb.isUrl(path)) {
            if (mode == .append) return error.AppendNotSupported;
            self.* = .{ .backend = .{ .object = objstore.writer(try smb.Upload.open(arena, path)) } };
        } else if (objstore.isUrl(path)) {
            if (mode == .append) return error.AppendNotSupported;
            const client = try arena.create(std.http.Client);
            client.* = http_client.initClient(arena);
            const obj = try objstore.parse(arena, path);
            self.* = .{ .backend = .{ .object = try obj.openWriter(arena, client, "text/csv") } };
        } else {
            self.* = .{ .backend = .{ .file = try std.fs.cwd().createFile(path, .{ .truncate = mode == .truncate }) } };
            self.fw = self.backend.file.writer(&self.write_buf);
            if (mode == .append) {
                const end = try self.backend.file.getEndPos();
                try self.fw.seekTo(end);
                header = end == 0;
            }
        }

        switch (splitCodec(path).codec) {
            .none => {},
            .gzip => self.gz = try deflate.Gzip.init(std.heap.page_allocator, self.raw()),
            .zstd => return error.ZstdWriteUnsupported,
        }

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

    pub fn openStdout(arena: std.mem.Allocator, schema: types.Schema, dialect: Dialect) !*CsvWriter {
        return openBorrowed(arena, std.fs.File.stdout(), schema, dialect);
    }

    /// Writes to a file someone else owns: left open on `close`, and written from
    /// where it stands, so a second writer carries on after the first.
    pub fn openBorrowed(arena: std.mem.Allocator, file: std.fs.File, schema: types.Schema, dialect: Dialect) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        self.* = .{ .backend = .{ .file = file }, .owns_file = false, .dialect = dialect };
        self.fw = self.backend.file.writerStreaming(&self.write_buf);
        const w = self.out();
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try w.writeByte(dialect.delim);
            try writeField(w, f.name, dialect.delim);
        }
        try w.writeByte('\n');
        return self;
    }

    pub fn writeBatch(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) !void {
        self.writeRows(arena, batch) catch |e| return self.specific(e);
    }

    fn writeRows(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) !void {
        return self.renderRows(self.out(), arena, batch);
    }

    /// Split from `writeRows` so a parallel lane renders into its own buffer and holds
    /// the shared sink's lock only for the append (`driver.Sink.VTable.renderBatch`).
    fn renderRows(self: *CsvWriter, w: *std.Io.Writer, arena: std.mem.Allocator, batch: Batch) !void {
        const d = self.dialect.delim;
        const quote_scalars = scalarCanNeedQuote(d);
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            for (batch.columns, 0..) |*col, i| {
                if (i > 0) try w.writeByte(d);
                switch (col.ty.kind) {
                    .string, .bytes => if (col.validity.get(r)) try writeField(w, col.data.bytes.at(r), d),
                    else => {
                        if (!col.validity.get(r)) continue;
                        const v = col.getValue(r);
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

    pub fn renderBatch(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.ensureUnusedCapacity(batch.len * 64 + 64);
        self.renderRows(&aw.writer, arena, batch) catch |e| return self.specific(e);
        return aw.writer.buffered();
    }

    pub fn writeRendered(self: *CsvWriter, bytes: []const u8) !void {
        self.out().writeAll(bytes) catch |e| return self.specific(e);
    }

    pub fn close(self: *CsvWriter) !void {
        if (self.gz) |g| {
            defer {
                g.deinit(std.heap.page_allocator);
                self.gz = null;
            }
            g.finish() catch |e| return self.specific(e);
        }
        switch (self.backend) {
            .file => |f| {
                try self.fw.interface.flush();
                if (self.owns_file) f.close();
            },
            .object => |o| o.finish() catch |e| return self.specific(e),
        }
    }

    pub fn abort(self: *CsvWriter) void {
        if (self.gz) |g| {
            g.deinit(std.heap.page_allocator);
            self.gz = null;
        }
        switch (self.backend) {
            .file => |f| if (self.owns_file) f.close(),
            .object => |o| o.abort(),
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
    .renderBatch = sinkRender,
    .writeRendered = sinkWriteRendered,
};
fn sinkRender(ptr: *anyopaque, arena: std.mem.Allocator, b: Batch) anyerror![]const u8 {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    return self.renderBatch(arena, b);
}
fn sinkWriteRendered(ptr: *anyopaque, bytes: []const u8) anyerror!void {
    const self: *CsvWriter = @ptrCast(@alignCast(ptr));
    return self.writeRendered(bytes);
}
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

/// Numbers and timestamps render with only digits and `+-.:eE `, so under an
/// ordinary delimiter they skip the quote scan; a `.` or `-` delimiter cannot.
fn scalarCanNeedQuote(delim: u8) bool {
    return switch (delim) {
        '0'...'9', '+', '-', '.', ':', 'e', 'E', ' ', '"', '\n', '\r' => true,
        else => false,
    };
}

pub fn writeField(w: anytype, s: []const u8, delim: u8) !void {
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

/// An empty string is quoted, since unquoted empty is how the reader spells null;
/// emitting it bare once turned `""` into NULL on the next read.
fn needsQuote(s: []const u8, delim: u8) bool {
    if (s.len == 0) return true;
    for (s) |c| {
        if (c == delim or c == '"' or c == '\n' or c == '\r') return true;
    }
    return false;
}

/// Wakes a `serveOnce` still blocked in `accept` because the reader never connected,
/// so a regression fails the test instead of hanging the suite at the join.
fn releaseAccept(listener: *std.net.Server) void {
    if (std.net.tcpConnectToAddress(listener.listen_address)) |c| c.close() else |_| {}
}

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
    defer releaseAccept(&listener);

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
        defer releaseAccept(&listener);
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/missing.csv", .{listener.listen_address.getPort()});
        try std.testing.expectError(error.HttpNotFound, CsvReader.open(a, url, .{}));
    }
    {
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        var listener = try addr.listen(.{ .reuse_address = true });
        defer listener.deinit();
        const th = try std.Thread.spawn(.{}, serveOnce, .{ &listener, "503 Service Unavailable", "busy" });
        defer th.join();
        defer releaseAccept(&listener);
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/data.csv", .{listener.listen_address.getPort()});
        try std.testing.expectError(error.HttpServerBusy, CsvReader.open(a, url, .{}));
    }
}

pub fn parseSlice(a: std.mem.Allocator, schema: *const types.Schema, data: []const u8) !Batch {
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

test "quoted values and an empty string survive a write/read round-trip, the empty string distinct from null" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b" });

    var line = std.array_list.Managed(u8).init(a);
    try writeField(line.writer(), "", ',');
    try line.append(',');
    try line.append('\n');
    try writeField(line.writer(), "O'Neil, \"Jr\"", ',');
    try line.append(',');
    try writeField(line.writer(), "plain", ',');
    try line.append('\n');
    const b = try parseSlice(a, &schema, line.items);
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expect(!b.columns[0].getValue(0).isNull());
    try std.testing.expectEqualStrings("", b.columns[0].getValue(0).string);
    try std.testing.expect(b.columns[1].getValue(0).isNull());
    try std.testing.expectEqualStrings("O'Neil, \"Jr\"", b.columns[0].getValue(1).string);
    try std.testing.expectEqualStrings("plain", b.columns[1].getValue(1).string);
}

test "TypeSniffer: int/float promotion, leading zeros and quoted cells force string, empties only mark nulls" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var s = try TypeSniffer.init(ar.allocator(), 7, ',');
    s.feed("1,1.5,abc,007,\"9\",,5");
    s.feed("-2,2,x,12,3,,");
    s.feed("3,2,y,1,4,,6");
    try std.testing.expectEqual(types.TypeKind.int, s.resolve(0).kind);
    try std.testing.expectEqual(types.TypeKind.float, s.resolve(1).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(2).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(3).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(4).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(5).kind);
    try std.testing.expectEqual(types.TypeKind.int, s.resolve(6).kind);
}

test "TypeSniffer: cells split on the dialect's delimiter, not always a comma" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var s = try TypeSniffer.init(ar.allocator(), 4, ';');
    s.feed("2026-07-01;1140469.50;1;1,5");
    s.feed("2026-07-02;0.00;12;2,25");
    try std.testing.expectEqual(types.TypeKind.date, s.resolve(0).kind);
    try std.testing.expectEqual(types.TypeKind.float, s.resolve(1).kind);
    try std.testing.expectEqual(types.TypeKind.int, s.resolve(2).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(3).kind);
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

test "a UTF-8 byte-order mark, as Excel writes it, is not part of the first column's name" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "a.csv", .data = "\xef\xbb\xbfid,name\n1,x\n" });
    try tmp.dir.writeFile(.{ .sub_path = "b.csv", .data = "\xef\xbb\xbfid,name\n2,y\n" });
    const a_path = try tmp.dir.realpathAlloc(a, "a.csv");
    const b_path = try tmp.dir.realpathAlloc(a, "b.csv");

    const m = try MappedCsv.open(a, a_path, .{});
    defer m.close();
    try std.testing.expectEqualStrings("id", m.schema.fields[0].name);
    try std.testing.expectEqualStrings("1,x\n", m.body);

    const r = try CsvReader.openList(a, &.{ a_path, b_path }, .{});
    defer r.close();
    try std.testing.expectEqualStrings("id", r.schema.fields[0].name);
    var rows: usize = 0;
    while (try r.next(a)) |b| rows += b.len;
    try std.testing.expectEqual(@as(usize, 2), rows);
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

    const want = [_][]const i64{ &.{ 1, 2, 3 }, &.{4} };
    for (0..2) |i| {
        var r = CsvSliceReader{ .data = m.chunk(i, 2), .schema = &m.schema };
        var ids = std.array_list.Managed(i64).init(a);
        while (try r.next(a)) |b| {
            for (0..b.len) |j| try ids.append(b.columns[0].getValue(j).int);
        }
        try std.testing.expectEqualSlices(i64, want[i], ids.items);
    }
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

    const ascii = "2020-10-27";
    try std.testing.expect((try decodeField(a, .latin1, ascii)).ptr == ascii.ptr);
    try std.testing.expect((try decodeField(a, .utf8, "\xC7\xC3")).len == 2);

    try std.testing.expectEqualStrings("LIQUIDAÇÃO", try decodeField(a, .latin1, "LIQUIDA\xC7\xC3O"));
    try std.testing.expectEqualStrings("“hi”", try decodeField(a, .cp1252, "\x93hi\x94"));
    try std.testing.expectEqualStrings("€", try decodeField(a, .cp1252, "\x80"));
    try std.testing.expectEqualStrings("\u{FFFD}", try decodeField(a, .cp1252, "\x81"));
    try std.testing.expectEqualStrings("ção éáíóúâêô x", try decodeField(a, .latin1, "\xe7\xe3o \xe9\xe1\xed\xf3\xfa\xe2\xea\xf4 x"));
    try std.testing.expectEqualStrings("€€€€€€€€€€ ok", try decodeField(a, .cp1252, "\x80" ** 10 ++ " ok"));
}

test "csv dialect: a semicolon latin-1 file reads as UTF-8 columns" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
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
    const b = try parseSlice(a, &schema, "a;b,c\n");
    try w.writeBatch(a, b);
    try w.close();

    const got = try tmp.dir.readFileAlloc(a, "o.csv", 1 << 16);
    try std.testing.expectEqualStrings("x;y\n\"a;b\";c\n", got);
}

test "csv writer on a borrowed file: header and rows only, tsv quotes a tab, the file stays open" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile("o.tsv", .{ .read = true });
    defer file.close();

    var names = [_][]const u8{ "x", "y" };
    const schema = try stringSchema(a, &names);
    const w = try CsvWriter.openBorrowed(a, file, schema, .{ .delim = '\t' });
    try w.writeBatch(a, try parseSlice(a, &schema, "plain,\"has\ttab\"\n\"a,b\",\n"));
    try w.close();

    try file.writeAll("# still writable\n");
    const got = try tmp.dir.readFileAlloc(a, "o.tsv", 1 << 16);
    try std.testing.expectEqualStrings("x\ty\nplain\t\"has\ttab\"\na,b\t\n# still writable\n", got);
}

test "two borrowed-file writers in a row append: a second SELECT must not overwrite the first" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile("out.csv", .{});
    defer file.close();

    var first = [_][]const u8{"a"};
    var second = [_][]const u8{"b"};
    const s1 = try stringSchema(a, &first);
    const s2 = try stringSchema(a, &second);
    const w1 = try CsvWriter.openBorrowed(a, file, s1, .{});
    try w1.writeBatch(a, try parseSlice(a, &s1, "1\n"));
    try w1.close();
    const w2 = try CsvWriter.openBorrowed(a, file, s2, .{});
    try w2.writeBatch(a, try parseSlice(a, &s2, "2\n"));
    try w2.close();

    const got = try tmp.dir.readFileAlloc(a, "out.csv", 1 << 16);
    try std.testing.expectEqualStrings("a\n1\nb\n2\n", got);
}

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

    const cases = [_]struct { delim: u8, kinds: [2]types.TypeKind, csv: []const u8, want: []const u8, first: Value }{
        .{ .delim = '.', .kinds = .{ .float, .int }, .csv = "2.5,7\n", .want = "f.i\n\"2.5\".7\n", .first = .{ .float = 2.5 } },
        .{ .delim = '-', .kinds = .{ .int, .int }, .csv = "-3,7\n", .want = "f-i\n\"-3\"-7\n", .first = .{ .int = -3 } },
    };

    for (cases) |c| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const base = try tmp.dir.realpathAlloc(a, ".");
        const path = try std.fs.path.join(a, &.{ base, "o.csv" });

        var names = [_][]const u8{ "f", "i" };
        var kinds = c.kinds;
        const schema = try typedSchema(a, &names, &kinds);
        var in_names = [_][]const u8{ "f", "i" };
        const in_schema = try typedSchema(a, &in_names, &kinds);
        const b = try parseSlice(a, &in_schema, c.csv);

        const w = try CsvWriter.open(a, path, schema, .truncate, .{ .delim = c.delim });
        try w.writeBatch(a, b);
        try w.close();

        const got = try tmp.dir.readFileAlloc(a, "o.csv", 1 << 16);
        try std.testing.expectEqualStrings(c.want, got);

        var rd = CsvSliceReader{
            .data = got[std.mem.indexOfScalar(u8, got, '\n').? + 1 ..],
            .schema = &schema,
            .dialect = .{ .delim = c.delim },
        };
        const back = (try rd.next(a)).?;
        try std.testing.expectEqual(@as(usize, 1), back.len);
        try std.testing.expectEqual(@as(usize, 2), back.columns.len);
        try std.testing.expectEqual(@as(i64, 7), back.columns[1].getValue(0).int);
        try std.testing.expectEqualDeep(c.first, back.columns[0].getValue(0));
    }
}

test "csv writer: renderBatch produces exactly what writeBatch writes" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");

    var names = [_][]const u8{ "i", "f", "s" };
    var kinds = [_]types.TypeKind{ .int, .float, .string };
    const schema = try typedSchema(a, &names, &kinds);
    const rows = "1,2.5,ok\n-7,,\n0,-0.125,\"has,comma\"\n";

    const direct = try std.fs.path.join(a, &.{ base, "direct.csv" });
    const wd = try CsvWriter.open(a, direct, schema, .truncate, .{});
    try wd.writeBatch(a, try parseSlice(a, &schema, rows));
    try wd.close();

    const staged = try std.fs.path.join(a, &.{ base, "staged.csv" });
    const ws = try CsvWriter.open(a, staged, schema, .truncate, .{});
    const bytes = try ws.renderBatch(a, try parseSlice(a, &schema, rows));
    try ws.writeRendered(bytes);
    try ws.close();

    const got_direct = try tmp.dir.readFileAlloc(a, "direct.csv", 1 << 16);
    const got_staged = try tmp.dir.readFileAlloc(a, "staged.csv", 1 << 16);
    try std.testing.expectEqualStrings(got_direct, got_staged);
    try std.testing.expectEqualStrings("1,2.5,ok\n-7,,\n0,-0.125,\"has,comma\"\n", bytes);

    try std.testing.expect(ws.sink().canRender());
}

test "scalarCanNeedQuote: only a delimiter a number could contain forces the scan" {
    for ([_]u8{ ',', ';', '\t', '|', '#', '^' }) |d| try std.testing.expect(!scalarCanNeedQuote(d));
    for ([_]u8{ '0', '9', '+', '-', '.', ':', 'e', 'E', ' ', '"', '\n', '\r' }) |d| {
        try std.testing.expect(scalarCanNeedQuote(d));
    }
}

test "scanRecord: a newline inside a quoted field stays in the value" {
    const data = "1,\"line A\nline B\"\n2,plain\n";
    const r1 = scanRecord(data, 0, ',');
    try std.testing.expectEqualStrings("1,\"line A\nline B\"", r1.line);
    const r2 = scanRecord(data, r1.next, ',');
    try std.testing.expectEqualStrings("2,plain", r2.line);
    try std.testing.expectEqual(data.len, r2.next);

    const esc = "a,\"he said \"\"hi\"\"\nand left\"\n";
    try std.testing.expectEqualStrings("a,\"he said \"\"hi\"\"\nand left\"", scanRecord(esc, 0, ',').line);

    try std.testing.expectEqualStrings("x,y", scanRecord("x,y\r\n", 0, ',').line);
    try std.testing.expectEqualStrings("x,y", scanRecord("x,y", 0, ',').line);
}

test "quotesOpen / hasQuotedNewline drive the continuation and split decisions" {
    try std.testing.expect(quotesOpen("1,\"line A", ','));
    try std.testing.expect(!quotesOpen("1,\"line A\"", ','));
    try std.testing.expect(!quotesOpen("plain,row", ','));
    try std.testing.expect(!quotesOpen("a,\"he said \"\"hi\"\"\"", ','));

    try std.testing.expect(MappedCsv.hasQuotedNewline("1,\"a\nb\"\n", ','));
    try std.testing.expect(!MappedCsv.hasQuotedNewline("1,\"a b\"\n2,c\n", ','));
    try std.testing.expect(!MappedCsv.hasQuotedNewline("1,a\n2,b\n", ','));
}

test "CsvReader reads every member of a multi-member .csv.gz, as pigz and an append write it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file: std.Io.Writer.Allocating = .init(a);
    for ([_][]const u8{ "id,name\n1,alpha\n", "2,beta\n" }) |part| {
        const gz = try deflate.Gzip.init(std.testing.allocator, &file.writer);
        defer gz.deinit(std.testing.allocator);
        try gz.interface.writeAll(part);
        try gz.finish();
    }
    try tmp.dir.writeFile(.{ .sub_path = "two.csv.gz", .data = file.written() });
    const path = try tmp.dir.realpathAlloc(a, "two.csv.gz");
    const r = try CsvReader.open(a, path, .{});
    defer r.close();
    var ids = std.array_list.Managed(i64).init(a);
    var names = std.array_list.Managed([]const u8).init(a);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| {
            try ids.append(b.columns[0].getValue(i).int);
            try names.append(b.columns[1].getValue(i).string);
        }
    }
    try std.testing.expectEqualSlices(i64, &.{ 1, 2 }, ids.items);
    try std.testing.expectEqual(@as(usize, 2), names.items.len);
    try std.testing.expectEqualStrings("alpha", names.items[0]);
    try std.testing.expectEqualStrings("beta", names.items[1]);
}

test "splitInto: delimiter bitmask across chunk edges, missing and extra fields, a projection" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const S = types.Type.init(.string).asNullable();
    const I = types.Type.init(.int).asNullable();

    var bs = [_]column.Builder{ column.Builder.init(a, I), column.Builder.init(a, S), column.Builder.init(a, S), column.Builder.init(a, I) };
    try splitInto(a, "12345678901,abcdefghijklmnopqrstu,vwxyzABCDEFGHIJKLMNOP,42,extra,more", &bs, .{}, null);
    try splitInto(a, "7,x", &bs, .{}, null);
    try splitInto(a, "8,\"q,\"\"t\"\"\",,9", &bs, .{}, null);
    const c0 = try bs[0].finish();
    const c1 = try bs[1].finish();
    const c2 = try bs[2].finish();
    const c3 = try bs[3].finish();
    try std.testing.expectEqual(@as(i64, 12345678901), c0.getValue(0).int);
    try std.testing.expectEqualStrings("abcdefghijklmnopqrstu", c1.getValue(0).string);
    try std.testing.expectEqualStrings("vwxyzABCDEFGHIJKLMNOP", c2.getValue(0).string);
    try std.testing.expectEqual(@as(i64, 42), c3.getValue(0).int);
    try std.testing.expect(c2.getValue(1).isNull() and c3.getValue(1).isNull());
    try std.testing.expectEqualStrings("q,\"t\"", c1.getValue(2).string);
    try std.testing.expect(c2.getValue(2).isNull());
    try std.testing.expectEqual(@as(i64, 9), c3.getValue(2).int);

    const full = types.Schema{ .fields = &.{ .{ .name = "a", .ty = I }, .{ .name = "b", .ty = S }, .{ .name = "c", .ty = S }, .{ .name = "d", .ty = I } } };
    const p = (try Projection.of(a, full, &.{ "d", "b" })).?;
    try std.testing.expectEqualStrings("b", p.schema.fields[0].name);
    var ps = [_]column.Builder{ column.Builder.init(a, S), column.Builder.init(a, I) };
    try splitInto(a, "not-an-int,bee,\"skipped \"\"x\"\"\",5", &ps, .{}, p.slot);
    try splitInto(a, "x,b2,c,6", &ps, .{}, p.slot);
    const pb = try ps[0].finish();
    const pd = try ps[1].finish();
    try std.testing.expectEqualStrings("bee", pb.getValue(0).string);
    try std.testing.expectEqual(@as(i64, 6), pd.getValue(1).int);
    try std.testing.expect((try Projection.of(a, full, &.{ "a", "b", "c", "d" })) == null);
    try std.testing.expectEqualStrings("a", (try Projection.of(a, full, &.{"zz"})).?.schema.fields[0].name);
}

test "a header's quoted names lose their quotes and may hold the delimiter" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const fields = try headerFields(ar.allocator(), "\"Valor Total\",\"a,b\", c ,\"x\"\"y\",", .{});
    try std.testing.expectEqual(@as(usize, 5), fields.len);
    for ([_][]const u8{ "Valor Total", "a,b", "c", "x\"y", "" }, fields) |want, f| try std.testing.expectEqualStrings(want, f.name);
}
