//! Minimal CSV source and sink. The source reads a header row and produces
//! batches of columns typed by sniffing; the sink writes a header then
//! serializes each batch. RFC 4180 quoting: fields containing the delimiter, a
//! quote or a newline are double-quoted with `""` escaping, and a newline inside
//! a quoted field belongs to the value. Only a quote at the start of a field
//! opens one; a mid-field `"` (`12" pipe`) is data, since treating it as an
//! opener once swallowed every following row. Unquoted empty is null and `""`
//! is an empty string. Lines end in `\n`, `\r\n` or, when the stream starts so, a
//! bare `\r`; a line longer than the read buffer is gathered, and a record past
//! `MAX_RECORD` is refused (no CSV, or a quote left open) to keep memory bounded. A
//! row with more or fewer fields than the header is an error naming its file and line.
//!
//! The delimiter and the source encoding are a `Dialect`, because the CSV a
//! statistics office or regulator publishes is often `;`-separated latin-1.
//! Only single-byte encodings exist, since a multi-byte one would break the
//! parallel reader's byte-range chunking. Byte `b` of latin-1 is U+00`b`, so no
//! byte sequence can hold a delimiter or newline. Files labelled latin-1 are
//! often really cp1252, whose 0x80–0x9F block holds curly quotes, dashes and the
//! euro sign (its five undefined slots decode to U+FFFD).
//!
//! Where the bytes come from (a file, a URL, an object, SFTP, SMB, a zip member)
//! and their gzip or zstd decoding is `stream.zig`'s; the reader parses its
//! `Stream.reader` and moves to a folder's next file with `Stream.reopen`.
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
const celltext = @import("../exec/celltext.zig");
const Batch = @import("../exec/batch.zig").Batch;
const Value = @import("../exec/value.zig").Value;
const eval = @import("../exec/eval.zig");
const driver = @import("../connect/driver.zig");
const objstore = @import("../store/objstore.zig");
const Output = @import("output.zig").Output;
const texttype = @import("texttype.zig");
const stream = @import("stream.zig");
const Stream = stream.Stream;
const json = @import("json/read.zig");
const release = stream.release;
const Gunzip = stream.Gunzip;
const codecBuffer = stream.codecBuffer;
const folder = @import("../connect/folder.zig");
const deflate = @import("compress/deflate.zig");

const BATCH_ROWS = 1024;
const LINE_BUF = stream.LINE_BUF;
const MAX_RECORD = 64 * 1024 * 1024;

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

pub const Codec = stream.Codec;
pub const ArchiveRef = stream.ArchiveRef;
pub const splitCodec = stream.splitCodec;
pub const splitArchive = stream.splitArchive;
pub const isArchive = stream.isArchive;
pub const dataName = stream.dataName;

pub const Dialect = struct {
    delim: u8 = ',',
    encoding: Encoding = .utf8,
    text: []const []const u8 = &.{},
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
    if (enc == .utf8) return utf8Repair(arena, s);
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

pub const CsvReader = struct {
    arena: std.mem.Allocator,
    dialect: Dialect = .{},
    stream: *Stream,
    rdr: *std.Io.Reader = undefined,
    schema: types.Schema,
    pending: []const Pending = &.{},
    pending_i: usize = 0,
    stream_eof: bool = false,
    done: bool = false,
    rest_urls: []const []const u8 = &.{},
    header_line: []const u8 = "",
    join_buf: std.array_list.Managed(u8) = undefined,
    long_buf: std.array_list.Managed(u8) = undefined,
    path: []const u8 = "",
    eol: u8 = '\n',
    line_no: u64 = 0,
    rec_line: u64 = 0,
    slot: ?[]const i16 = null,

    const Pending = struct { text: []const u8, line_no: u64, path: []const u8 };

    pub fn project(self: *CsvReader, names: []const []const u8) !void {
        const p = (try Projection.of(self.arena, self.schema, names)) orelse return;
        self.slot = p.slot;
        self.schema = p.schema;
    }

    /// Starts reading `path` from its newly opened stream: its line ending and line count.
    fn begin(self: *CsvReader, path: []const u8) void {
        self.path = path;
        self.line_no = 0;
        self.rdr = self.stream.reader;
        self.eol = sniffEol(self.rdr);
    }

    pub const isUrl = stream.isUrl;

    pub fn open(arena: std.mem.Allocator, path: []const u8, dialect: Dialect) !*CsvReader {
        const self = try arena.create(CsvReader);
        self.* = .{
            .arena = arena,
            .dialect = dialect,
            .stream = undefined,
            .schema = undefined,
            .done = false,
            .join_buf = std.array_list.Managed(u8).init(arena),
            .long_buf = std.array_list.Managed(u8).init(arena),
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
            .stream = undefined,
            .schema = undefined,
            .done = false,
            .join_buf = std.array_list.Managed(u8).init(arena),
            .long_buf = std.array_list.Managed(u8).init(arena),
            .rest_urls = files[1..],
        };
        return self.openFirst(files[0]);
    }

    /// A reader of `s`, a stream opened elsewhere (stdin), named by `s.name`.
    pub fn openStream(arena: std.mem.Allocator, s: *Stream, dialect: Dialect) !*CsvReader {
        const self = try arena.create(CsvReader);
        self.* = .{
            .arena = arena,
            .dialect = dialect,
            .stream = s,
            .schema = undefined,
            .done = false,
            .join_buf = std.array_list.Managed(u8).init(arena),
            .long_buf = std.array_list.Managed(u8).init(arena),
        };
        return self.start(s.name);
    }

    fn openFirst(self: *CsvReader, first: []const u8) !*CsvReader {
        self.stream = try Stream.open(self.arena, first);
        return self.start(first);
    }

    /// The header and the type sample of the stream just opened, its first file `first`.
    fn start(self: *CsvReader, first: []const u8) !*CsvReader {
        const arena = self.arena;
        self.begin(first);

        const header = stripBom((try self.readLine()) orelse return error.EmptyCsv);
        self.header_line = try arena.dupe(u8, std.mem.trim(u8, header, " \t\r"));
        var fields = std.array_list.Managed(types.Schema.Field).fromOwnedSlice(arena, try headerFields(arena, header, self.dialect));

        var sniff = try TypeSniffer.init(arena, fields.items.len, self.dialect.delim);
        var pending = std.array_list.Managed(Pending).init(arena);
        while (pending.items.len <= SAMPLE_ROWS) {
            const line = (try self.readLine()) orelse {
                if (try self.advance()) continue;
                self.stream_eof = true;
                break;
            };
            if (line.len == 0) continue;
            const own = try arena.dupe(u8, line);
            if (pending.items.len < SAMPLE_ROWS) sniff.feed(own);
            try pending.append(.{ .text = own, .line_no = self.rec_line, .path = self.path });
        }
        sniff.whole = self.stream_eof;
        texttype.settle(fields.items, sniff.cols, sniff.whole, self.dialect.text);

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
            var at: Pending = .{ .text = "", .line_no = 0, .path = "" };
            if (self.pending_i < self.pending.len) {
                at = self.pending[self.pending_i];
                line = at.text;
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
                at = .{ .text = line, .line_no = self.rec_line, .path = self.path };
            }
            if (line.len == 0) continue;
            splitInto(arena, line, builders, self.dialect, self.slot) catch |e|
                return rowFailure(e, at.path, at.line_no, line, self.schema, self.dialect, self.slot);
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

        try self.stream.reopen(url);
        try self.nextHeader(url);
        return true;
    }

    fn nextHeader(self: *CsvReader, url: []const u8) !void {
        self.begin(url);
        const hdr = (try self.readLine()) orelse return error.EmptyCsv;
        const got = std.mem.trim(u8, stripBom(hdr), " \t\r");
        if (!std.mem.eql(u8, got, self.header_line))
            return explainf(error.CsvHeaderMismatch, "`{s}` has the header `{s}`, not the first file's `{s}` — a folder read takes CSVs of one layout", .{ url, clip(got), clip(self.header_line) });
    }

    pub fn close(self: *CsvReader) void {
        self.stream.close();
    }

    pub fn source(self: *CsvReader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    /// One record, which may span physical lines. Balanced quotes take the zero-copy
    /// path; only a continued record is joined, into a buffer reused across records,
    /// and only its new line is scanned for quotes. A `\r` is cut only where the record
    /// ends, so a quoted CRLF keeps it.
    fn readLine(self: *CsvReader) !?[]const u8 {
        self.rec_line = self.line_no + 1;
        const first = (try self.takePhysical()) orelse return null;
        if (!quotesOpen(first, self.dialect.delim, false)) return trimCr(first);

        self.join_buf.clearRetainingCapacity();
        try self.join_buf.appendSlice(first);
        var quoted = true;
        while (quoted) {
            const more = (try self.takePhysical()) orelse
                return explainf(error.CsvUnterminatedQuote, "`{s}`: the quote opened on line {d} is never closed, so every line after it would read as one field", .{ self.path, self.rec_line });
            quoted = quotesOpen(more, self.dialect.delim, true);
            if (self.join_buf.items.len + more.len >= MAX_RECORD)
                return explainf(error.CsvRecordTooLong, "`{s}`: the record from line {d} runs past {d} MiB without closing its quote — is a quote left open?", .{ self.path, self.rec_line, MAX_RECORD >> 20 });
            try self.join_buf.append(self.eol);
            try self.join_buf.appendSlice(more);
        }
        return trimCr(self.join_buf.items);
    }

    /// A line past the read buffer is gathered into `long_buf`, up to `MAX_RECORD`; a
    /// truncated or corrupt compressed stream is named with where it broke.
    fn takePhysical(self: *CsvReader) !?[]const u8 {
        const line = self.rdr.takeDelimiter(self.eol) catch |e| switch (e) {
            error.StreamTooLong => return try self.takeLong(),
            error.ReadFailed => return self.readFailed(),
        };
        if (line != null) self.line_no += 1;
        return line;
    }

    fn takeLong(self: *CsvReader) ![]const u8 {
        self.long_buf.clearRetainingCapacity();
        while (true) {
            const part = self.rdr.takeDelimiter(self.eol) catch |e| switch (e) {
                error.StreamTooLong => {
                    const held = self.rdr.buffered();
                    if (self.long_buf.items.len + held.len >= MAX_RECORD)
                        return explainf(error.CsvRecordTooLong, "`{s}` line {d} is longer than {d} MiB — is it a CSV?", .{ self.path, self.line_no + 1, MAX_RECORD >> 20 });
                    try self.long_buf.appendSlice(held);
                    self.rdr.toss(held.len);
                    continue;
                },
                error.ReadFailed => return self.readFailed(),
            };
            if (part) |p| try self.long_buf.appendSlice(p);
            self.line_no += 1;
            return self.long_buf.items;
        }
    }

    fn readFailed(self: *CsvReader) anyerror {
        if (self.stream.codec == .none) return error.ReadFailed;
        return explainf(error.ReadFailed, "`{s}`: the {s} stream is truncated or corrupt (it broke after line {d})", .{ self.path, @tagName(self.stream.codec), self.line_no });
    }
};

fn trimCr(s: []const u8) []const u8 {
    return if (s.len > 0 and s[s.len - 1] == '\r') s[0 .. s.len - 1] else s;
}

/// `\r` when the start of the stream ends its lines with a bare carriage return
/// (classic Mac OS, some exports), else `\n`.
fn sniffEol(r: *std.Io.Reader) u8 {
    const head = r.peekGreedy(1) catch return '\n';
    if (std.mem.indexOfScalar(u8, head, '\n') != null) return '\n';
    const cr = std.mem.indexOfScalar(u8, head, '\r') orelse return '\n';
    return if (cr + 1 < head.len) '\r' else '\n';
}

fn explainf(e: anyerror, comptime fmt: []const u8, args: anytype) anyerror {
    var buf: [480]u8 = undefined;
    return eval.explain(e, std.fmt.bufPrint(&buf, fmt, args) catch blk: {
        @memcpy(buf[buf.len - 3 ..], "...");
        break :blk buf[0..];
    });
}

fn clip(s: []const u8) []const u8 {
    return if (s.len <= 80) s else s[0..80];
}

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
    path: []const u8 = "",
    quoted_newlines: bool = false,
    quote_free: bool = false,
    ndjson: ?json.Options = null,
    dialect: Dialect = .{},
    slot: ?[]const i16 = null,

    pub fn project(self: *MappedCsv, arena: std.mem.Allocator, names: []const []const u8) !void {
        if (self.ndjson != null) {
            var keep = std.array_list.Managed(types.Schema.Field).init(arena);
            for (self.schema.fields) |f| for (names) |n| {
                if (std.mem.eql(u8, f.name, n)) {
                    try keep.append(f);
                    break;
                }
            };
            if (keep.items.len > 0) self.schema = .{ .fields = keep.items };
            return;
        }
        const p = (try Projection.of(arena, self.schema, names)) orelse return;
        self.slot = p.slot;
        self.schema = p.schema;
    }

    /// A local NDJSON file mapped for lanes, its columns from its sampled rows. Every
    /// newline ends a row, as a JSON string cannot hold a raw one, so it cuts where a
    /// CSV does. A top-level array or an `items` path has no row per line and is
    /// `NotMappable`, read serially instead.
    pub fn openNdjson(arena: std.mem.Allocator, path: []const u8, opts: json.Options) !*MappedCsv {
        if (splitCodec(path).codec != .none or splitArchive(path) != null or opts.items != null) return error.NotMappable;
        const self = try arena.create(MappedCsv);
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();
        const size = (try file.stat()).size;
        if (size == 0) return error.NotMappable;
        const data = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(data);
        const first = std.mem.indexOfNone(u8, data, " \t\r\n") orelse return error.NotMappable;
        if (data[first] == '[' and opts.shape != .ndjson) return error.NotMappable;
        var sample = opts;
        sample.shape = .ndjson;
        const r = json.Reader.openMapped(arena, arena, data, sample) catch return error.NotMappable;
        self.* = .{ .data = data, .body = data, .schema = r.schema, .file = file, .path = try arena.dupe(u8, path), .quote_free = true, .ndjson = sample };
        return self;
    }

    /// Lane `i` of `n`'s rows as a source, read from `scratch`: a CSV chunk through
    /// `reader`, an NDJSON one in place through `json.Reader`.
    pub fn lane(self: *const MappedCsv, scratch: std.mem.Allocator, i: usize, n: usize, schema: *const types.Schema) !driver.Source {
        if (self.ndjson) |o| {
            var opts = o;
            opts.schema = schema.*;
            const r = try json.Reader.openMapped(scratch, scratch, self.chunk(i, n), opts);
            return r.source();
        }
        const rd = try scratch.create(CsvSliceReader);
        rd.* = self.reader(i, n, schema);
        return rd.source();
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

        if (std.mem.indexOfScalar(u8, data, '\n') == null) return error.NotMappable;
        const head = scanRecord(data, 0, dialect.delim);
        if (data[head.next - 1] != '\n') return error.EmptyCsv;
        var fields = std.array_list.Managed(types.Schema.Field).fromOwnedSlice(arena, try headerFields(arena, stripBom(head.line), dialect));

        const body = data[head.next..];
        var sniff = try TypeSniffer.init(arena, fields.items.len, dialect.delim);
        var fed: usize = 0;
        var pos: usize = 0;
        while (pos < body.len) {
            const rec = scanRecord(body, pos, dialect.delim);
            if (rec.line.len > 0) {
                if (fed == SAMPLE_ROWS) break;
                sniff.feed(rec.line);
                fed += 1;
            }
            pos = rec.next;
        }
        sniff.whole = pos >= body.len;
        texttype.settle(fields.items, sniff.cols, sniff.whole, dialect.text);

        self.* = .{
            .data = data,
            .body = body,
            .schema = .{ .fields = try fields.toOwnedSlice() },
            .file = file,
            .path = try arena.dupe(u8, path),
            .dialect = dialect,
        };
        const quotes = scanQuotes(body, dialect.delim);
        self.quoted_newlines = quotes == .quoted_newlines;
        self.quote_free = quotes == .none;
        return self;
    }

    /// A reader of the i-th of `n` chunks, its failures naming this file and line. In a
    /// quote-free file it releases the pages it has read as it goes; in one with quotes,
    /// whose pages the open-time scan already mapped, releasing them cost a third of
    /// the read, so they stay.
    pub fn reader(self: *const MappedCsv, i: usize, n: usize, schema: *const types.Schema) CsvSliceReader {
        return .{ .data = self.chunk(i, n), .schema = schema, .dialect = self.dialect, .slot = self.slot, .path = self.path, .whole = self.data, .release = self.quote_free };
    }

    /// The i-th of `n` newline-aligned chunks of the body; a line belongs to the
    /// chunk holding its first byte. May be empty.
    pub fn chunk(self: *const MappedCsv, i: usize, n: usize) []const u8 {
        const lo = self.lineStart(self.body.len * i / n);
        const hi = self.lineStart(self.body.len * (i + 1) / n);
        return self.body[lo..hi];
    }

    /// Whether the body has a quote, and then whether a quoted field holds a newline.
    /// A quote-free file costs one `memchr`, which releases the pages behind it as it
    /// goes, so checking it never holds all of it resident; a file with quotes is
    /// walked record by record.
    fn scanQuotes(body: []const u8, delim: u8) enum { none, quotes, quoted_newlines } {
        var at: usize = 0;
        const quoted = while (at < body.len) {
            const end = @min(body.len, at + scan_window);
            if (std.mem.indexOfScalar(u8, body[at..end], '"') != null) break true;
            release(body[at..end]);
            at = end;
        } else false;
        if (!quoted) return .none;
        var pos: usize = 0;
        while (pos < body.len) {
            const rec = scanRecord(body, pos, delim);
            if (std.mem.indexOfScalar(u8, rec.line, '\n') != null) return .quoted_newlines;
            pos = rec.next;
        }
        return .quotes;
    }

    const scan_window = 4 << 20;

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

/// A chunk of a mapped file, read a batch at a time; with `release`, every
/// `release_every` bytes the pages already read are released, so a lane holds a few
/// MiB of its chunk resident, not all of it.
pub const CsvSliceReader = struct {
    data: []const u8,
    pos: usize = 0,
    release: bool = false,
    released: usize = 0,
    schema: *const types.Schema,
    dialect: Dialect = .{},
    slot: ?[]const i16 = null,
    path: []const u8 = "",
    whole: []const u8 = "",

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
            splitInto(arena, line, builders, self.dialect, self.slot) catch |e|
                return rowFailure(e, self.path, self.lineOf(line), line, self.schema.*, self.dialect, self.slot);
            rows += 1;
        }
        if (rows == 0) return null;
        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        if (self.release and self.pos - self.released >= release_every) {
            release(self.data[self.released..self.pos]);
            self.released = self.pos;
        }
        return Batch{ .schema = self.schema, .columns = cols, .len = rows };
    }

    const release_every = 4 << 20;

    pub fn source(self: *CsvSliceReader) driver.Source {
        return .{ .ptr = self, .vtable = &slice_vtable };
    }

    pub fn close(_: *CsvSliceReader) void {}

    /// Counted only for an error message: a split file has no quoted newline, so its
    /// newlines are its line breaks.
    fn lineOf(self: *const CsvSliceReader, line: []const u8) u64 {
        const base = if (self.whole.len > 0) self.whole else self.data;
        const off = @intFromPtr(line.ptr) -| @intFromPtr(base.ptr);
        return std.mem.count(u8, base[0..@min(off, base.len)], "\n") + 1;
    }
};

const slice_vtable = driver.sourceVTable(CsvSliceReader);

pub const SAMPLE_ROWS = 1024;

/// A CSV sample's columns, each line split on the dialect's delimiter into
/// `texttype.Column`s.
const TypeSniffer = struct {
    cols: []texttype.Column,
    delim: u8,
    whole: bool = false,

    fn init(arena: std.mem.Allocator, ncols: usize, delim: u8) !TypeSniffer {
        const cols = try arena.alloc(texttype.Column, ncols);
        for (cols) |*c| c.* = .{};
        return .{ .cols = cols, .delim = delim };
    }

    fn feed(self: *TypeSniffer, line: []const u8) void {
        var i: usize = 0;
        for (self.cols) |*c| {
            if (i > line.len) break;
            const quoted = i < line.len and line[i] == '"';
            c.note(nextField(line, &i, self.delim), quoted);
        }
    }

    fn resolve(self: *const TypeSniffer, j: usize) types.Type {
        return self.cols[j].resolve(self.whole);
    }
};

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

/// Whether a quoted field is left open at the end of `line`, which starts inside one
/// when `in_quote` (a continuation line), so a long record is scanned once, not once
/// per line. `""` counts two, so parity is the state; a quoteless line is one SIMD
/// scan (a byte walk capped the parse at ~75 MB/s).
fn quotesOpen(line: []const u8, delim: u8, in_quote: bool) bool {
    const first_q = std.mem.indexOfScalar(u8, line, '"') orelse return in_quote;
    var in_q = in_quote;
    var at_field = !in_quote and (first_q == 0 or line[first_q - 1] == delim);
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

/// A row with more or fewer fields than the header is `CsvRaggedRow`: padding or
/// cutting it once loaded a shifted row without a word. A quoted field with no
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
        } else {
            const start = i;
            i = std.mem.indexOfScalarPos(u8, line, i, d.delim) orelse line.len;
            if (dest) |bld| try putCell(arena, bld, line[start..i], false, d.encoding);
        }
        if ((i < line.len) != (col + 1 < ncols)) return error.CsvRaggedRow;
        i += 1;
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
    while (i + V <= line.len) : (i += V) {
        var m: u32 = @bitCast(@as(Vec, line[i..][0..V].*) == dv);
        while (m != 0) : (m &= m - 1) {
            const p = i + @ctz(m);
            if (col + 1 == ncols) return error.CsvRaggedRow;
            try putField(arena, builders, slot, col, line[start..p], d.encoding);
            start = p + 1;
            col += 1;
        }
    }
    while (i < line.len) : (i += 1) if (line[i] == d.delim) {
        if (col + 1 == ncols) return error.CsvRaggedRow;
        try putField(arena, builders, slot, col, line[start..i], d.encoding);
        start = i + 1;
        col += 1;
    };
    if (col + 1 != ncols) return error.CsvRaggedRow;
    try putField(arena, builders, slot, col, line[start..], d.encoding);
}

/// The file, line and field behind a row `splitInto` refused, found by walking the
/// row again; only a failing read pays for it.
fn rowFailure(e: anyerror, path: []const u8, line_no: u64, line: []const u8, schema: types.Schema, d: Dialect, slot: ?[]const i16) anyerror {
    const ncols = if (slot) |sl| sl.len else schema.fields.len;
    var where_buf: [std.fs.max_path_bytes + 32]u8 = undefined;
    const where = (if (path.len > 0)
        std.fmt.bufPrint(&where_buf, "`{s}` line {d}", .{ path, line_no })
    else
        std.fmt.bufPrint(&where_buf, "line {d}", .{line_no})) catch "a row";
    var i: usize = 0;
    var n: usize = 0;
    while (i <= line.len) : (n += 1) {
        const raw = nextField(line, &i, d.delim);
        if (e != error.TextTypeMismatch or n >= ncols) continue;
        const f = if (slot) |sl| (if (sl[n] >= 0) schema.fields[@intCast(sl[n])] else continue) else schema.fields[n];
        if (texttype.cellParses(f.ty.kind, raw)) continue;
        const want: []const u8 = switch (f.ty.kind) {
            .int => "an INT",
            .float => "a FLOAT",
            .date => "a DATE",
            else => @tagName(f.ty.kind),
        };
        return explainf(e, "{s}, column `{s}`: '{s}' is not {s}, the type sniffed from the first {d} rows — to read the column as written, use CAST({s} AS STRING) (or the type it holds) everywhere the query reads it", .{ where, f.name, clip(raw), want, SAMPLE_ROWS, f.name });
    }
    if (e == error.CsvRaggedRow)
        return explainf(e, "{s} has {d} fields, the header {d} — a row must have one field per column", .{ where, n, ncols });
    return e;
}

/// One field's text from `line[i.*]` (a quoted one without its quotes, `""` kept),
/// moving `i` past its delimiter; past the end once the last field is taken.
fn nextField(line: []const u8, i: *usize, delim: u8) []const u8 {
    const start = i.*;
    var j = start;
    var lo = start;
    var hi: ?usize = null;
    if (j < line.len and line[j] == '"') {
        lo = j + 1;
        j += 1;
        while (j < line.len) : (j += 1) {
            if (line[j] != '"') continue;
            if (j + 1 < line.len and line[j + 1] == '"') {
                j += 1;
                continue;
            }
            hi = j;
            break;
        }
    }
    const end = std.mem.indexOfScalarPos(u8, line, @min(j, line.len), delim) orelse line.len;
    i.* = end + 1;
    return line[lo..(hi orelse end)];
}

inline fn putField(arena: std.mem.Allocator, builders: []column.Builder, slot: ?[]const i16, col: usize, raw: []const u8, enc: Encoding) !void {
    if (slot) |sl| {
        if (sl[col] < 0) return;
        return putCell(arena, &builders[@intCast(sl[col])], raw, false, enc);
    }
    return putCell(arena, &builders[col], raw, false, enc);
}

inline fn putCell(arena: std.mem.Allocator, b: *column.Builder, raw: []const u8, quoted: bool, enc: Encoding) !void {
    if (enc != .utf8) return texttype.appendCell(b, try decodeField(arena, enc, raw), quoted);
    return texttype.appendCell(b, if (b.ty.kind == .string) try utf8Repair(arena, raw) else raw, quoted);
}

/// `s` when it is valid UTF-8, else a copy with each invalid byte as U+FFFD, as
/// pyarrow reads one: a text value basalt holds is always UTF-8, so a Parquet or
/// JSON file written from a mislabelled CSV stays readable everywhere.
fn utf8Repair(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    if (std.unicode.utf8ValidateSlice(s)) return s;
    var out = try std.array_list.Managed(u8).initCapacity(arena, s.len + 8);
    var runs = celltext.Utf8Runs{ .s = s };
    while (runs.next()) |t| try out.appendSlice(t);
    return out.items;
}

test "utf8Repair keeps valid text and replaces each invalid byte" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const ok = "café ção";
    try std.testing.expect((try utf8Repair(a, ok)).ptr == ok.ptr);
    try std.testing.expectEqualStrings("caf\u{FFFD}", try utf8Repair(a, "caf\xe9"));
    try std.testing.expectEqualStrings("\u{FFFD}(", try utf8Repair(a, "\xc3\x28"));
    try std.testing.expectEqualStrings("\u{FFFD}x", try utf8Repair(a, "\xffx"));
    try std.testing.expectEqualStrings("a\u{FFFD}\u{FFFD}", try utf8Repair(a, "a\xe2\x82"));
}

const source_vtable = driver.sourceVTable(CsvReader);

pub const CsvWriter = struct {
    out: *Output,
    dialect: Dialect = .{},

    /// `.append` resumes at the file's end and writes the header only to an empty file,
    /// so a second header never lands among the rows; a file with rows must already
    /// have this header, and one missing its last newline gets it first, or the first
    /// new row would join the last old one. Objects, SFTP and SMB are written whole
    /// (through a `.part`), so they cannot append; a local file is written whole too
    /// unless appended to.
    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema, mode: driver.FileMode, dialect: Dialect) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        self.* = .{ .out = try Output.open(arena, path, "text/csv", mode), .dialect = dialect };
        errdefer self.out.abort();
        var header = true;
        var newline = false;
        if (mode == .append and self.out.end > 0) {
            if (try self.out.lastByte(arena, path)) |last| {
                header = false;
                newline = last != '\n';
                try checkAppendHeader(arena, path, try appendHeader(arena, self.out.localFile().?, splitCodec(path).codec), schema, dialect);
            }
        }
        try self.out.compress(path);
        if (newline) try self.out.writer().writeByte('\n');
        if (header) try writeHeader(self.out.writer(), schema, dialect);
        return self;
    }

    fn writeHeader(w: *std.Io.Writer, schema: types.Schema, dialect: Dialect) !void {
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try w.writeByte(dialect.delim);
            try writeField(w, f.name, dialect.delim);
        }
        try w.writeByte('\n');
    }

    /// The header record of the file an append extends, read from its start; a `.gz`
    /// is read through its decoder.
    fn appendHeader(arena: std.mem.Allocator, file: std.fs.File, codec: Codec) ![]const u8 {
        const buf = try arena.alloc(u8, LINE_BUF);
        var fr = file.reader(buf);
        var gz: Gunzip = undefined;
        var r: *std.Io.Reader = &fr.interface;
        if (codec == .gzip) {
            gz.init(r, try arena.alloc(u8, codecBuffer(.gzip)), try arena.alloc(u8, LINE_BUF));
            r = &gz.interface;
        }
        var head = std.array_list.Managed(u8).init(arena);
        var in_q = false;
        while (true) {
            const got = r.peekGreedy(1) catch |e| switch (e) {
                error.EndOfStream => return head.items,
                else => |x| return x,
            };
            for (got, 0..) |c, k| {
                if (c == '"') in_q = !in_q;
                if (c == '\n' and !in_q) {
                    try head.appendSlice(got[0..k]);
                    return head.items;
                }
            }
            try head.appendSlice(got);
            r.toss(got.len);
        }
    }

    fn checkAppendHeader(arena: std.mem.Allocator, path: []const u8, line: []const u8, schema: types.Schema, d: Dialect) !void {
        const have_line = trimCr(stripBom(line));
        const have = try headerFields(arena, have_line, d);
        const same = have.len == schema.fields.len and for (have, schema.fields) |h, f| {
            if (!std.mem.eql(u8, h.name, f.name)) break false;
        } else true;
        if (same) return;
        var want = std.array_list.Managed(u8).init(arena);
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try want.append(d.delim);
            try want.appendSlice(f.name);
        }
        return explainf(error.AppendHeaderMismatch, "`{s}` has the header `{s}`, but this APPEND writes `{s}` — appended rows must have the file's columns, in its order", .{ path, clip(have_line), clip(want.items) });
    }

    pub fn openStdout(arena: std.mem.Allocator, schema: types.Schema, dialect: Dialect) !*CsvWriter {
        return openBorrowed(arena, std.fs.File.stdout(), schema, dialect);
    }

    /// Writes to a file someone else owns (`Output.openBorrowed`).
    pub fn openBorrowed(arena: std.mem.Allocator, file: std.fs.File, schema: types.Schema, dialect: Dialect) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        self.* = .{ .out = try Output.openBorrowed(arena, file), .dialect = dialect };
        try writeHeader(self.out.writer(), schema, dialect);
        return self;
    }

    pub fn writeBatch(self: *CsvWriter, _: std.mem.Allocator, batch: Batch) !void {
        self.renderRows(self.out.writer(), batch) catch |e| return self.out.specific(e);
    }

    /// Split from `writeBatch` so a parallel lane renders into its own buffer and holds
    /// the shared sink's lock only for the append (`driver.Sink.VTable.renderBatch`).
    fn renderRows(self: *CsvWriter, w: *std.Io.Writer, batch: Batch) !void {
        const d = self.dialect.delim;
        if (scalarCanNeedQuote(d)) return renderCells(w, batch, CsvText(true){ .delim = d });
        return renderCells(w, batch, CsvText(false){ .delim = d });
    }

    pub fn renderBatch(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) ![]const u8 {
        var aw = std.Io.Writer.Allocating.init(arena);
        try aw.ensureUnusedCapacity(batch.len * 64 + 64);
        self.renderRows(&aw.writer, batch) catch |e| return self.out.specific(e);
        return aw.writer.buffered();
    }

    pub fn writeRendered(self: *CsvWriter, bytes: []const u8) !void {
        self.out.writer().writeAll(bytes) catch |e| return self.out.specific(e);
    }

    pub fn close(self: *CsvWriter) !void {
        return self.out.finish();
    }

    pub fn abort(self: *CsvWriter) void {
        self.out.abort();
    }

    pub fn sink(self: *CsvWriter) driver.Sink {
        return .{ .ptr = self, .vtable = &sink_vtable };
    }
};

const sink_vtable = driver.sinkVTable(CsvWriter);

/// Numbers and timestamps render with only digits and `+-.:eE `, so under an
/// ordinary delimiter they skip the quote scan; a `.` or `-` delimiter cannot.
fn renderCells(w: *std.Io.Writer, batch: Batch, p: anytype) !void {
    for (0..batch.len) |r| {
        for (batch.columns, 0..) |*col, i| {
            if (i > 0) try w.writeByte(p.delim);
            try celltext.write(w, col, r, p);
        }
        try w.writeByte('\n');
    }
}

/// The `celltext.write` policy for CSV: null is empty and a field is quoted as
/// `writeField` decides; with a delimiter a number, date or boolean can hold,
/// every field's text goes through that decision.
fn CsvText(comptime quote_scalars: bool) type {
    return struct {
        delim: u8,

        pub const null_text = "";
        pub const bool_true = "true";
        pub const bool_false = "false";
        pub const escape_scalars = quote_scalars;

        pub fn string(self: @This(), w: *std.Io.Writer, s: []const u8) !void {
            return writeField(w, s, self.delim);
        }
    };
}

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

test "CsvReader reads a list of http CSVs, each decoded per its Content-Encoding" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();

    const addr = try std.net.Address.parseIp("127.0.0.1", 0);
    var listener = try addr.listen(.{ .reuse_address = true });
    defer listener.deinit();
    const gz = "200 OK\r\ncontent-encoding: gzip";
    const th1 = try std.Thread.spawn(.{}, serveOnce, .{ &listener, gz, fx_gz });
    defer th1.join();
    defer releaseAccept(&listener);
    const th2 = try std.Thread.spawn(.{}, serveOnce, .{ &listener, gz, fx_gz });
    defer th2.join();
    defer releaseAccept(&listener);

    const port = listener.listen_address.getPort();
    const urls = [_][]const u8{
        try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/a.csv", .{port}),
        try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/b.csv", .{port}),
    };
    const r = try CsvReader.openList(a, &urls, .{});
    defer r.close();
    var rows: usize = 0;
    while (try r.next(a)) |b| rows += b.len;
    try std.testing.expectEqual(@as(usize, 6), rows);
}

test "CsvReader fails at once on a 4xx" {
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
}

test "CsvReader retries a 503, a reset and a 502 before the 200" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const reply = objstore.FakeServer.reply;
    var srv: objstore.FakeServer = undefined;
    try srv.start(&.{
        reply("503 Service Unavailable", "", ""),
        null,
        reply("502 Bad Gateway", "", ""),
        reply("200 OK", "", "id,name\n1,a\n2,b\n"),
    });
    defer srv.stop();
    const r = try CsvReader.open(a, try srv.url(a, "/data.csv"), .{});
    defer r.close();
    var rows: usize = 0;
    while (try r.next(a)) |b| rows += b.len;
    try std.testing.expectEqual(@as(usize, 2), rows);
    try std.testing.expectEqual(@as(usize, 4), srv.requests());
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

test "csv parsing: CRLF endings, blank lines, and a ragged row is an error naming its line" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b", "c" });

    const b = try parseSlice(a, &schema, "1,2,3\r\n\r\n4,5,\r\n6,7,8\n");
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqualStrings("3", b.columns[2].getValue(0).string);
    try std.testing.expect(b.columns[2].getValue(1).isNull());
    try std.testing.expectEqualStrings("8", b.columns[2].getValue(2).string);

    try std.testing.expectError(error.CsvRaggedRow, parseSlice(a, &schema, "1,2,3\n4,5\n"));
    try std.testing.expectEqualStrings("line 2 has 2 fields, the header 3 — a row must have one field per column", eval.takeFailure(error.CsvRaggedRow).?);
    try std.testing.expectError(error.CsvRaggedRow, parseSlice(a, &schema, "1,2,3,4\n"));
    try std.testing.expectError(error.CsvRaggedRow, parseSlice(a, &schema, "1,\"2,x\",3,\n"));
    try std.testing.expectError(error.CsvRaggedRow, parseSlice(a, &schema, "1,\"2\"\n"));
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

test "TypeSniffer: a column with no value is of unknown type only when the sample read the whole file" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var s = try TypeSniffer.init(ar.allocator(), 2, ',');
    s.feed("1,");
    try std.testing.expect(!s.resolve(1).unknown);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(1).kind);
    s.whole = true;
    try std.testing.expect(s.resolve(1).unknown);
    try std.testing.expect(!s.resolve(0).unknown);
}

test "TypeSniffer: a text column keeps its sampled numbers' most decimal places for SUM" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var s = try TypeSniffer.init(ar.allocator(), 3, ',');
    s.feed("\"1.5\",\"7\",\"1e3\"");
    s.feed("\"-2.125\",\"007\",\"a.b\"");
    try std.testing.expectEqual(@as(u8, 3), s.resolve(0).scale);
    try std.testing.expectEqual(@as(u8, 0), s.resolve(1).scale);
    try std.testing.expectEqual(@as(u8, 0), s.resolve(2).scale);
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

test "TypeSniffer: text after a closing quote stays in its field, as the loader keeps it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    var s = try TypeSniffer.init(ar.allocator(), 3, ',');
    s.feed("\"x\"y,1,z");
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(0).kind);
    try std.testing.expectEqual(types.TypeKind.int, s.resolve(1).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(2).kind);
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
    try std.testing.expectEqual(@as(?anyerror, error.TextTypeMismatch), err);
    const why = eval.takeFailure(error.TextTypeMismatch).?;
    try std.testing.expect(std.mem.indexOf(u8, why, "bad.csv` line 1036, column `id`: 'oops' is not an INT, the type sniffed from the first 1024 rows") != null);
    try std.testing.expect(std.mem.endsWith(u8, why, "use CAST(id AS STRING) (or the type it holds) everywhere the query reads it"));

    const mb = try MappedCsv.open(a, bad_path, .{});
    defer mb.close();
    var sr = mb.reader(0, 1, &mb.schema);
    err = null;
    while (sr.next(a) catch |e| blk: {
        err = e;
        break :blk null;
    }) |_| {}
    try std.testing.expectEqual(@as(?anyerror, error.TextTypeMismatch), err);
    try std.testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.TextTypeMismatch).?, "bad.csv` line 1036, column `id`: 'oops' is not an INT, the type sniffed from the first 1024 rows") != null);
}

test "serial and mapped readers agree whether the sample read the whole file" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ base, "e.csv" });

    for ([_]usize{ SAMPLE_ROWS - 1, SAMPLE_ROWS, SAMPLE_ROWS + 1 }) |rows| {
        for ([_][]const u8{ "", "\n", "\n\n" }) |tail| {
            var data = std.array_list.Managed(u8).init(a);
            try data.appendSlice("id,e");
            for (0..rows) |i| try data.writer().print("\n{d},", .{i + 1});
            try data.appendSlice(tail);
            try tmp.dir.writeFile(.{ .sub_path = "e.csv", .data = data.items });

            const r = try CsvReader.open(a, path, .{});
            defer r.close();
            const m = try MappedCsv.open(a, path, .{});
            defer m.close();
            try std.testing.expectEqual(rows <= SAMPLE_ROWS, r.schema.fields[1].ty.unknown);
            try std.testing.expectEqual(rows <= SAMPLE_ROWS, m.schema.fields[1].ty.unknown);
            var n: usize = 0;
            while (try r.next(a)) |b| n += b.len;
            try std.testing.expectEqual(rows, n);
        }
    }
}

test "a cell past the sample that only lenient parsers accept is named with its file, line and column" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(a, ".");
    const path = try std.fs.path.join(a, &.{ base, "lax.csv" });

    const cases = [_]struct { row: []const u8, want: []const u8 }{
        .{ .row = "1_000,1.5\n", .want = "lax.csv` line 1026, column `i`: '1_000' is not an INT, the type sniffed from the first 1024 rows" },
        .{ .row = "7,0x1p3\n", .want = "lax.csv` line 1026, column `f`: '0x1p3' is not a FLOAT, the type sniffed from the first 1024 rows" },
        .{ .row = "7,1e400\n", .want = "lax.csv` line 1026, column `f`: '1e400' is not a FLOAT, the type sniffed from the first 1024 rows" },
    };
    for (cases) |c| {
        var data = std.array_list.Managed(u8).init(a);
        try data.appendSlice("i,f\n");
        for (0..SAMPLE_ROWS) |i| try data.writer().print("{d},{d}.5\n", .{ i, i });
        try data.appendSlice(c.row);
        try tmp.dir.writeFile(.{ .sub_path = "lax.csv", .data = data.items });

        const r = try CsvReader.open(a, path, .{});
        defer r.close();
        var err: ?anyerror = null;
        while (r.next(a) catch |e| blk: {
            err = e;
            break :blk null;
        }) |_| {}
        try std.testing.expectEqual(@as(?anyerror, error.TextTypeMismatch), err);
        try std.testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.TextTypeMismatch).?, c.want) != null);

        const m = try MappedCsv.open(a, path, .{});
        defer m.close();
        var sr = m.reader(0, 1, &m.schema);
        err = null;
        while (sr.next(a) catch |e| blk: {
            err = e;
            break :blk null;
        }) |_| {}
        try std.testing.expectEqual(@as(?anyerror, error.TextTypeMismatch), err);
        try std.testing.expect(std.mem.indexOf(u8, eval.takeFailure(error.TextTypeMismatch).?, c.want) != null);
    }
}

fn readAllRows(a: std.mem.Allocator, path: []const u8) ![]const []const u8 {
    const r = try CsvReader.open(a, path, .{});
    defer r.close();
    var out = std.array_list.Managed([]const u8).init(a);
    for (r.schema.fields) |f| try out.append(f.name);
    while (try r.next(a)) |b| {
        for (0..b.len) |i| for (b.columns) |c| try out.append(try eval.valueToString(a, c.getValue(i)));
    }
    return out.items;
}

test "csv reader: a quoted CRLF keeps its CR, bare-CR files read, no line is too long" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const long = try a.alloc(u8, 3 * LINE_BUF);
    @memset(long, 'x');
    try tmp.dir.writeFile(.{ .sub_path = "crlf.csv", .data = "id,v\r\n1,\"a\r\nb\"\r\n2,c\r\n" });
    try tmp.dir.writeFile(.{ .sub_path = "cr.csv", .data = "id,n\r1,x\r2,\"y\rz\"\r" });
    try tmp.dir.writeFile(.{ .sub_path = "long.csv", .data = try std.mem.concat(a, u8, &.{ "a,b\n1,", long, "\n2,\"", long, "\n", long, "\"\n" }) });

    const crlf = try readAllRows(a, try tmp.dir.realpathAlloc(a, "crlf.csv"));
    try std.testing.expectEqualStrings("a\r\nb", crlf[3]);
    try std.testing.expectEqualStrings("c", crlf[5]);

    const cr = try readAllRows(a, try tmp.dir.realpathAlloc(a, "cr.csv"));
    try std.testing.expectEqual(@as(usize, 6), cr.len);
    try std.testing.expectEqualStrings("n", cr[1]);
    try std.testing.expectEqualStrings("y\rz", cr[5]);

    const lg = try readAllRows(a, try tmp.dir.realpathAlloc(a, "long.csv"));
    try std.testing.expectEqual(@as(usize, 6), lg.len);
    try std.testing.expectEqual(long.len, lg[3].len);
    try std.testing.expectEqual(2 * long.len + 1, lg[5].len);
}

test "a quoted header name may hold a newline, in the serial and the mapped reader" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "h.csv", .data = "\"multi\nline\",b\n1,2\n" });
    const path = try tmp.dir.realpathAlloc(a, "h.csv");
    const rows = try readAllRows(a, path);
    try std.testing.expectEqualStrings("multi\nline", rows[0]);
    try std.testing.expectEqualStrings("2", rows[3]);
    const m = try MappedCsv.open(a, path, .{});
    defer m.close();
    try std.testing.expectEqualStrings("multi\nline", m.schema.fields[0].name);
    var sr = m.reader(0, 1, &m.schema);
    const b = (try sr.next(a)).?;
    try std.testing.expectEqual(@as(i64, 2), b.columns[1].getValue(0).int);
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

test "csv reader: a lone zip member's own name picks its codec" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(.{ .sub_path = "zg.zip", .data = @embedFile("testdata/gz_member.zip") });
    const rows = try readAllRows(a, try tmp.dir.realpathAlloc(a, "zg.zip"));
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqualStrings("x", rows[0]);
    try std.testing.expectEqualStrings("2", rows[2]);
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
    try std.testing.expectEqualStrings("\u{FFFD}\u{FFFD}", try decodeField(a, .utf8, "\xC7\xC3"));

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
    try std.testing.expectEqual(@as(i64, 3), b.columns[2].getValue(0).int);
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

test "csv writer: an append ends a last line first, keeps the header it finds, writes one where none is" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const schema = try typedSchema(a, &.{"a"}, &.{.int});
    const batch = try parseSlice(a, &schema, "2\n");
    const append = struct {
        fn run(al: std.mem.Allocator, path: []const u8, s: types.Schema, b: Batch) !void {
            const w = try CsvWriter.open(al, path, s, .append, .{});
            try w.writeBatch(al, b);
            try w.close();
        }
    }.run;

    try tmp.dir.writeFile(.{ .sub_path = "ap.csv", .data = "a\n1" });
    const ap = try tmp.dir.realpathAlloc(a, "ap.csv");
    try append(a, ap, schema, batch);
    try std.testing.expectEqualStrings("a\n1\n2\n", try tmp.dir.readFileAlloc(a, "ap.csv", 1024));

    const gz_path = try std.fs.path.join(a, &.{ try tmp.dir.realpathAlloc(a, "."), "ap.csv.gz" });
    const gz_bytes = [_]u8{ 0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    try tmp.dir.writeFile(.{ .sub_path = "ap.csv.gz", .data = &gz_bytes });
    try append(a, gz_path, schema, batch);
    try append(a, gz_path, schema, batch);
    const r = try CsvReader.open(a, gz_path, .{});
    defer r.close();
    try std.testing.expectEqualStrings("a", r.schema.fields[0].name);
    try std.testing.expectEqual(@as(usize, 2), (try r.next(a)).?.len);

    try tmp.dir.writeFile(.{ .sub_path = "m.csv", .data = "b,a\nx,1\n" });
    try std.testing.expectError(error.AppendHeaderMismatch, append(a, try tmp.dir.realpathAlloc(a, "m.csv"), schema, batch));
    try std.testing.expect(std.mem.endsWith(u8, eval.takeFailure(error.AppendHeaderMismatch).?, "m.csv` has the header `b,a`, but this APPEND writes `a` — appended rows must have the file's columns, in its order"));
    try std.testing.expectEqualStrings("b,a\nx,1\n", try tmp.dir.readFileAlloc(a, "m.csv", 1024));
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

fn goldenCsv(a: std.mem.Allocator, d: u8) ![]const []const u8 {
    const batches = try celltext.goldenBatches(a);
    const out = try a.alloc([]const u8, batches.len);
    var w = CsvWriter{ .out = try Output.openBorrowed(a, std.fs.File.stdout()), .dialect = .{ .delim = d } };
    for (batches, out) |batch, *o| o.* = try w.renderBatch(a, batch);
    return out;
}

test "csv writer: rows of every kind, for every kind of delimiter" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const tiny = celltext.golden_tiny;
    const want = [_]struct { u8, [11][]const u8 }{
        .{ ',', .{
            "true\nfalse\n\n",
            "0\n-1\n-9223372036854775808\n9223372036854775807\n\n",
            "0.1\n0\n-0\n" ++ tiny ++ "\n1000000000000000000000\n-2.25\nnan\ninf\n-inf\n\n",
            "123.45\n-0.0007\n5\n0.12345\n-18446744073709.551615\n0.00\n\n",
            "\"\"\nplain\ntab\there\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\n",
            "00:00:00\n01:02:03.000001\n23:59:59.999999\n\n",
            "1970-01-01 00:00:00\n2024-01-01 00:00:00.123456\n1969-12-31 23:59:59.999999\n0001-01-01 00:00:00\n\n",
            "\n\n",
            "1,a b,2.50\n,,\n-2,\"\",-0.1\n",
        } },
        .{ '\t', .{
            "true\nfalse\n\n",
            "0\n-1\n-9223372036854775808\n9223372036854775807\n\n",
            "0.1\n0\n-0\n" ++ tiny ++ "\n1000000000000000000000\n-2.25\nnan\ninf\n-inf\n\n",
            "123.45\n-0.0007\n5\n0.12345\n-18446744073709.551615\n0.00\n\n",
            "\"\"\nplain\n\"tab\there\"\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\na,b;c:d.e-1 0\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\n",
            "00:00:00\n01:02:03.000001\n23:59:59.999999\n\n",
            "1970-01-01 00:00:00\n2024-01-01 00:00:00.123456\n1969-12-31 23:59:59.999999\n0001-01-01 00:00:00\n\n",
            "\n\n",
            "1\ta b\t2.50\n\t\t\n-2\t\"\"\t-0.1\n",
        } },
        .{ '.', .{
            "true\nfalse\n\n",
            "0\n-1\n-9223372036854775808\n9223372036854775807\n\n",
            "\"0.1\"\n0\n-0\n\"" ++ tiny ++ "\"\n1000000000000000000000\n\"-2.25\"\nnan\ninf\n-inf\n\n",
            "\"123.45\"\n\"-0.0007\"\n5\n\"0.12345\"\n\"-18446744073709.551615\"\n\"0.00\"\n\n",
            "\"\"\nplain\ntab\there\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\n",
            "00:00:00\n\"01:02:03.000001\"\n\"23:59:59.999999\"\n\n",
            "1970-01-01 00:00:00\n\"2024-01-01 00:00:00.123456\"\n\"1969-12-31 23:59:59.999999\"\n0001-01-01 00:00:00\n\n",
            "\"\"\n\n",
            "1.a b.\"2.50\"\n..\n-2.\"\".\"-0.1\"\n",
        } },
        .{ '-', .{
            "true\nfalse\n\n",
            "0\n\"-1\"\n\"-9223372036854775808\"\n9223372036854775807\n\n",
            "0.1\n0\n\"-0\"\n" ++ tiny ++ "\n1000000000000000000000\n\"-2.25\"\nnan\ninf\n\"-inf\"\n\n",
            "123.45\n\"-0.0007\"\n5\n0.12345\n\"-18446744073709.551615\"\n0.00\n\n",
            "\"\"\nplain\ntab\there\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "\"1970-01-01\"\n\"2024-01-01\"\n\"0000-01-01\"\n\"-5877641-06-24\"\n\n",
            "00:00:00\n01:02:03.000001\n23:59:59.999999\n\n",
            "\"1970-01-01 00:00:00\"\n\"2024-01-01 00:00:00.123456\"\n\"1969-12-31 23:59:59.999999\"\n\"0001-01-01 00:00:00\"\n\n",
            "\"\"\n\n",
            "1-a b-2.50\n--\n\"-2\"-\"\"-\"-0.1\"\n",
        } },
        .{ ':', .{
            "true\nfalse\n\n",
            "0\n-1\n-9223372036854775808\n9223372036854775807\n\n",
            "0.1\n0\n-0\n" ++ tiny ++ "\n1000000000000000000000\n-2.25\nnan\ninf\n-inf\n\n",
            "123.45\n-0.0007\n5\n0.12345\n-18446744073709.551615\n0.00\n\n",
            "\"\"\nplain\ntab\there\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\n",
            "\"00:00:00\"\n\"01:02:03.000001\"\n\"23:59:59.999999\"\n\n",
            "\"1970-01-01 00:00:00\"\n\"2024-01-01 00:00:00.123456\"\n\"1969-12-31 23:59:59.999999\"\n\"0001-01-01 00:00:00\"\n\n",
            "\"\"\n\n",
            "1:a b:2.50\n::\n-2:\"\":-0.1\n",
        } },
        .{ 'e', .{
            "\"true\"\n\"false\"\n\n",
            "0\n-1\n-9223372036854775808\n9223372036854775807\n\n",
            "0.1\n0\n-0\n" ++ tiny ++ "\n1000000000000000000000\n-2.25\nnan\ninf\n-inf\n\n",
            "123.45\n-0.0007\n5\n0.12345\n-18446744073709.551615\n0.00\n\n",
            "\"\"\nplain\n\"tab\there\"\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\n\"O'Brien\"\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\n\"sixteen bytes and more\x01\"\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\n",
            "00:00:00\n01:02:03.000001\n23:59:59.999999\n\n",
            "1970-01-01 00:00:00\n2024-01-01 00:00:00.123456\n1969-12-31 23:59:59.999999\n0001-01-01 00:00:00\n\n",
            "\"\"\n\n",
            "1ea be2.50\nee\n-2e\"\"e-0.1\n",
        } },
        .{ '0', .{
            "true\nfalse\n\n",
            "\"0\"\n-1\n\"-9223372036854775808\"\n\"9223372036854775807\"\n\n",
            "\"0.1\"\n\"0\"\n\"-0\"\n\"" ++ tiny ++ "\"\n\"1000000000000000000000\"\n-2.25\nnan\ninf\n-inf\n\n",
            "123.45\n\"-0.0007\"\n5\n\"0.12345\"\n\"-18446744073709.551615\"\n\"0.00\"\n\n",
            "\"\"\nplain\ntab\there\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\\N \nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\xc3\xb3 \xf0\x9f\x98\x80\n\x00nul\nsixteen bytes and more\x01\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "\"1970-01-01\"\n\"2024-01-01\"\n\"0000-01-01\"\n\"-5877641-06-24\"\n\n",
            "\"00:00:00\"\n\"01:02:03.000001\"\n23:59:59.999999\n\n",
            "\"1970-01-01 00:00:00\"\n\"2024-01-01 00:00:00.123456\"\n1969-12-31 23:59:59.999999\n\"0001-01-01 00:00:00\"\n\n",
            "\"\"\n\n",
            "10a b0\"2.50\"\n00\n-20\"\"0\"-0.1\"\n",
        } },
        .{ ' ', .{
            "true\nfalse\n\n",
            "0\n-1\n-9223372036854775808\n9223372036854775807\n\n",
            "0.1\n0\n-0\n" ++ tiny ++ "\n1000000000000000000000\n-2.25\nnan\ninf\n-inf\n\n",
            "123.45\n-0.0007\n5\n0.12345\n-18446744073709.551615\n0.00\n\n",
            "\"\"\nplain\ntab\there\n\"line\nbreak\"\n\"cr\rlf\r\n\"\n\"say \"\"hi\"\"\"\nO'Brien\nback\\slash\\\na\\Nb\n\"\\N \"\nNULL\na\x01b\x02c\n\"a,b;c:d.e-1 0\"\n\"\xc3\xb3 \xf0\x9f\x98\x80\"\n\x00nul\n\"sixteen bytes and more\x01\"\n\n",
            "\"\"\nplain\n\"\x01\x02 \\ \t\n\"\"\"\n\xc3\xb3\n\n",
            "1970-01-01\n2024-01-01\n0000-01-01\n-5877641-06-24\n\n",
            "00:00:00\n01:02:03.000001\n23:59:59.999999\n\n",
            "\"1970-01-01 00:00:00\"\n\"2024-01-01 00:00:00.123456\"\n\"1969-12-31 23:59:59.999999\"\n\"0001-01-01 00:00:00\"\n\n",
            "\"\"\n\n",
            "1 \"a b\" 2.50\n  \n-2 \"\" -0.1\n",
        } },
    };
    for (want) |x| try celltext.expectGolden(&x[1], try goldenCsv(a, x[0]));
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

test "quotesOpen / scanQuotes drive the continuation and split decisions" {
    try std.testing.expect(quotesOpen("1,\"line A", ',', false));
    try std.testing.expect(!quotesOpen("1,\"line A\"", ',', false));
    try std.testing.expect(!quotesOpen("plain,row", ',', false));
    try std.testing.expect(!quotesOpen("a,\"he said \"\"hi\"\"\"", ',', false));
    try std.testing.expect(quotesOpen("still inside", ',', true));
    try std.testing.expect(!quotesOpen("end\",2", ',', true));
    try std.testing.expect(quotesOpen("end\",\"again", ',', true));
    try std.testing.expect(quotesOpen("a \"\"quoted\"\" word", ',', true));

    try std.testing.expect(MappedCsv.scanQuotes("1,\"a\nb\"\n", ',') == .quoted_newlines);
    try std.testing.expect(MappedCsv.scanQuotes("1,\"a b\"\n2,c\n", ',') == .quotes);
    try std.testing.expect(MappedCsv.scanQuotes("1,a\n2,b\n", ',') == .none);
}

test "CsvReader reads every member of a multi-member .csv.gz, as pigz and an append write it" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var file: std.Io.Writer.Allocating = .init(a);
    for ([_][]const u8{ "id,name\n1,alpha\n", "2,beta\n" }) |part| {
        const gz = try deflate.Deflate.init(std.testing.allocator, &file.writer, .{});
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

test "splitInto: delimiter bitmask across chunk edges, missing and extra fields refused, a projection" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const S = types.Type.init(.string).asNullable();
    const I = types.Type.init(.int).asNullable();

    var bs = [_]column.Builder{ column.Builder.init(a, I), column.Builder.init(a, S), column.Builder.init(a, S), column.Builder.init(a, I) };
    try splitInto(a, "12345678901,abcdefghijklmnopqrstu,vwxyzABCDEFGHIJKLMNOP,42", &bs, .{}, null);
    try splitInto(a, "7,x,,", &bs, .{}, null);
    try splitInto(a, "8,\"q,\"\"t\"\"\",,9", &bs, .{}, null);
    var extra = [_]column.Builder{ column.Builder.init(a, I), column.Builder.init(a, S), column.Builder.init(a, S), column.Builder.init(a, I) };
    try std.testing.expectError(error.CsvRaggedRow, splitInto(a, "12345678901,abcdefghijklmnopqrstu,vwxyzABCDEFGHIJKLMNOP,42,extra,more", &extra, .{}, null));
    try std.testing.expectError(error.CsvRaggedRow, splitInto(a, "7,x", &extra, .{}, null));
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
