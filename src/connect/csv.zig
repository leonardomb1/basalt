//! Minimal CSV source and sink. The source reads a header row into an all-string
//! schema (empty field = null) and produces batches of string columns; the sink
//! writes a header then serializes each batch. RFC-ish quoting: fields containing
//! comma/quote/newline are double-quoted with `""` escaping.

const std = @import("std");
const types = @import("../lang/types.zig");
const column = @import("../exec/column.zig");
const batchmod = @import("../exec/batch.zig");
const valuemod = @import("../exec/value.zig");
const eval = @import("../exec/eval.zig");
const driver = @import("driver.zig");
const httpx = @import("http.zig");

const Batch = batchmod.Batch;
const Value = valuemod.Value;

const BATCH_ROWS = 1024;
/// Reader/writer buffer size; also the max CSV line length (a line longer than
/// this yields `error.StreamTooLong`).
const LINE_BUF = 64 * 1024;

pub const CsvReader = struct {
    arena: std.mem.Allocator,
    backend: Backend,
    read_buf: [LINE_BUF]u8 = undefined,
    /// Line source, independent of where bytes come from: points at the file
    /// reader's interface or the HTTP response body reader.
    rdr: *std.Io.Reader = undefined,
    schema: types.Schema,
    pending: []const []const u8 = &.{}, // sampled lines, replayed before the stream
    pending_i: usize = 0,
    stream_eof: bool = false, // sampling consumed the whole stream; never read rdr again
    done: bool = false,

    const Backend = union(enum) {
        file: FileBackend,
        http: *HttpFetch,
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
        return std.mem.startsWith(u8, path, "http://") or std.mem.startsWith(u8, path, "https://");
    }

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*CsvReader {
        const self = try arena.create(CsvReader);
        self.* = .{
            .arena = arena,
            .backend = undefined,
            .schema = undefined,
            .done = false,
        };
        if (isUrl(path)) {
            const hf = try arena.create(HttpFetch);
            hf.* = .{ .client = httpx.initClient(arena), .req = undefined, .response = undefined };
            errdefer hf.client.deinit();
            const uri = std.Uri.parse(path) catch return error.InvalidUrl;
            startHttp(hf, uri) catch |e| switch (e) {
                // Likely a misordered server chain: rebuild trust from the chain
                // itself and retry once (see http.zig repairBundle).
                error.TlsInitializationFailed => {
                    const h = httpx.uriHost(uri) orelse return e;
                    if (!httpx.repairBundle(arena, &hf.client.ca_bundle, h, uri.port orelse 443)) return e;
                    hf.client.next_https_rescan_certs = false;
                    try startHttp(hf, uri);
                },
                else => return e,
            };
            errdefer hf.req.deinit();
            const code = @intFromEnum(hf.response.head.status);
            if (code != 200) return httpx.statusError(code);
            self.backend = .{ .http = hf };
            // Servers may force gzip/zstd regardless of what we asked for (GitHub
            // does); route through the decompressing reader, which is a passthrough
            // for identity. The window buffer is sized per negotiated encoding.
            const ce = hf.response.head.content_encoding;
            if (ce == .compress) return error.UnsupportedCompressionMethod;
            const win = ce.minBufferCapacity();
            const dbuf: []u8 = if (win > 0) try arena.alloc(u8, win) else &.{};
            self.rdr = hf.response.readerDecompressing(&hf.transfer_buf, &hf.decompress, dbuf);
        } else {
            self.backend = .{ .file = .{ .file = try std.fs.cwd().openFile(path, .{}), .fr = undefined } };
            self.backend.file.fr = self.backend.file.file.reader(&self.read_buf);
            self.rdr = &self.backend.file.fr.interface;
        }

        const header = (try self.readLine()) orelse return error.EmptyCsv;
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var it = std.mem.splitScalar(u8, header, ',');
        while (it.next()) |name| {
            try fields.append(.{
                .name = try arena.dupe(u8, std.mem.trim(u8, name, " \t")),
                .ty = types.Type.init(.string).asNullable(),
            });
        }

        // Sample the first SAMPLE_ROWS lines for type inference; they are kept
        // (arena-duped, bounded) and replayed by `next` before streaming resumes.
        var sniff = try TypeSniffer.init(arena, fields.items.len);
        var pending = std.array_list.Managed([]const u8).init(arena);
        while (pending.items.len < SAMPLE_ROWS) {
            const line = (try self.readLine()) orelse {
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
        for (builders, self.schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);

        var rows: usize = 0;
        while (rows < BATCH_ROWS) {
            var line: []const u8 = undefined;
            if (self.pending_i < self.pending.len) {
                line = self.pending[self.pending_i];
                self.pending_i += 1;
            } else {
                if (self.stream_eof) {
                    self.done = true;
                    break;
                }
                line = (try self.readLine()) orelse {
                    self.done = true;
                    break;
                };
            }
            if (line.len == 0) continue;
            try splitInto(arena, line, builders);
            rows += 1;
        }
        if (rows == 0) return null;

        const cols = try arena.alloc(column.Column, ncols);
        for (builders, 0..) |*b, i| cols[i] = try b.finish();
        return Batch{ .schema = &self.schema, .columns = cols, .len = rows };
    }

    pub fn close(self: *CsvReader) void {
        switch (self.backend) {
            .file => |f| f.file.close(),
            .http => |hf| {
                hf.req.deinit();
                hf.client.deinit();
            },
        }
    }

    pub fn source(self: *CsvReader) driver.Source {
        return .{ .ptr = self, .vtable = &source_vtable };
    }

    fn startHttp(hf: *HttpFetch, uri: std.Uri) !void {
        hf.req = try hf.client.request(.GET, uri, .{});
        errdefer hf.req.deinit();
        try hf.req.sendBodiless();
        hf.response = try hf.req.receiveHead(&hf.redirect_buf);
    }

    fn readLine(self: *CsvReader) !?[]const u8 {
        // Returns a slice into the reader's buffer (invalidated on the next read);
        // safe because `column.Builder.append` dupes string values into the arena.
        const line = (try self.rdr.takeDelimiter('\n')) orelse return null;
        var s: []const u8 = line;
        if (s.len > 0 and s[s.len - 1] == '\r') s = s[0 .. s.len - 1];
        return s;
    }
};

/// A local CSV file mapped into memory once, so N worker threads can parse disjoint
/// newline-aligned byte ranges in parallel (the parse, not the read, is the CSV
/// bottleneck). The mapping is shared read-only; each thread builds a `CsvSliceReader`
/// over its chunk. Only for local files — not URLs.
pub const MappedCsv = struct {
    data: []align(std.heap.page_size_min) const u8,
    body: []const u8, // bytes after the header line
    schema: types.Schema,
    file: std.fs.File,

    pub fn open(arena: std.mem.Allocator, path: []const u8) !*MappedCsv {
        const self = try arena.create(MappedCsv);
        const file = try std.fs.cwd().openFile(path, .{});
        errdefer file.close();
        const size = (try file.stat()).size;
        if (size == 0) return error.EmptyCsv;
        const data = try std.posix.mmap(null, size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
        errdefer std.posix.munmap(data);

        // Header → schema; body starts after the first newline. Types come from
        // the same SAMPLE_ROWS sniff the serial reader does, so both paths always
        // infer identical schemas for a file.
        const nl = std.mem.indexOfScalar(u8, data, '\n') orelse return error.EmptyCsv;
        var header = data[0..nl];
        if (header.len > 0 and header[header.len - 1] == '\r') header = header[0 .. header.len - 1];
        var fields = std.array_list.Managed(types.Schema.Field).init(arena);
        var it = std.mem.splitScalar(u8, header, ',');
        while (it.next()) |name| try fields.append(.{
            .name = try arena.dupe(u8, std.mem.trim(u8, name, " \t")),
            .ty = types.Type.init(.string).asNullable(),
        });

        const body = data[nl + 1 ..];
        var sniff = try TypeSniffer.init(arena, fields.items.len);
        var fed: usize = 0;
        var pos: usize = 0;
        while (fed < SAMPLE_ROWS and pos < body.len) {
            const k = std.mem.indexOfScalar(u8, body[pos..], '\n');
            var line = if (k) |j| body[pos .. pos + j] else body[pos..];
            pos += (k orelse line.len) + 1;
            if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
            if (line.len == 0) continue;
            sniff.feed(line);
            fed += 1;
        }
        for (fields.items, 0..) |*f, j| f.ty = sniff.resolve(j);

        self.* = .{ .data = data, .body = body, .schema = .{ .fields = try fields.toOwnedSlice() }, .file = file };
        return self;
    }

    /// The i-th of `n` newline-aligned chunks of the body (whole lines only; a line
    /// belongs to the chunk that contains its first byte). May be empty.
    pub fn chunk(self: *const MappedCsv, i: usize, n: usize) []const u8 {
        const lo = self.lineStart(self.body.len * i / n);
        const hi = self.lineStart(self.body.len * (i + 1) / n);
        return self.body[lo..hi];
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

    pub fn next(self: *CsvSliceReader, arena: std.mem.Allocator) !?Batch {
        if (self.pos >= self.data.len) return null;
        const ncols = self.schema.fields.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders, self.schema.fields) |*b, f| b.* = column.Builder.init(arena, f.ty);

        var rows: usize = 0;
        while (rows < BATCH_ROWS and self.pos < self.data.len) {
            const rest = self.data[self.pos..];
            const nl = std.mem.indexOfScalar(u8, rest, '\n');
            var line = if (nl) |k| rest[0..k] else rest;
            self.pos += (nl orelse rest.len) + 1;
            if (line.len > 0 and line[line.len - 1] == '\r') line = line[0 .. line.len - 1];
            if (line.len == 0) continue;
            try splitInto(arena, line, builders);
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
                        c.all_float = false; // "007", "+5", "-012": text that numeric round-tripping would rewrite
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
// ponytail: bare error name, no line/column context — add when it bites.
fn appendCell(b: *column.Builder, raw: []const u8, quoted: bool) !void {
    if (raw.len == 0) return b.append(if (quoted and b.ty.kind == .string) Value{ .string = raw } else .null);
    switch (b.ty.kind) {
        .int => try b.append(.{ .int = std.fmt.parseInt(i64, raw, 10) catch return error.CsvTypeMismatch }),
        .float => try b.append(.{ .float = std.fmt.parseFloat(f64, raw) catch return error.CsvTypeMismatch }),
        else => try b.append(.{ .string = raw }),
    }
}

fn splitInto(arena: std.mem.Allocator, line: []const u8, builders: []column.Builder) !void {
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
            try appendCell(&builders[col], try buf.toOwnedSlice(), true);
            if (i < line.len and line[i] == ',') i += 1;
        } else {
            const start = i;
            while (i < line.len and line[i] != ',') i += 1;
            try appendCell(&builders[col], line[start..i], false);
            if (i < line.len and line[i] == ',') i += 1;
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
    file: std.fs.File,
    write_buf: [LINE_BUF]u8 = undefined,
    fw: std.fs.File.Writer = undefined,

    pub fn open(arena: std.mem.Allocator, path: []const u8, schema: types.Schema) !*CsvWriter {
        const self = try arena.create(CsvWriter);
        self.* = .{ .file = try std.fs.cwd().createFile(path, .{}) };
        self.fw = self.file.writer(&self.write_buf);

        const w = &self.fw.interface;
        for (schema.fields, 0..) |f, i| {
            if (i > 0) try w.writeByte(',');
            try writeField(w, f.name);
        }
        try w.writeByte('\n');
        return self;
    }

    pub fn writeBatch(self: *CsvWriter, arena: std.mem.Allocator, batch: Batch) !void {
        const w = &self.fw.interface;
        var r: usize = 0;
        while (r < batch.len) : (r += 1) {
            for (batch.columns, 0..) |*col, i| {
                if (i > 0) try w.writeByte(',');
                const v = col.getValue(r);
                if (!v.isNull()) try writeField(w, try eval.valueToString(arena, v));
            }
            try w.writeByte('\n');
        }
    }

    pub fn close(self: *CsvWriter) !void {
        try self.fw.interface.flush();
        self.file.close();
    }

    /// Failure path: drop the unflushed write buffer and close the file. Rows
    /// already flushed stay in the file (a CSV has no transaction to roll back).
    pub fn abort(self: *CsvWriter) void {
        self.file.close();
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

fn writeField(w: anytype, s: []const u8) !void {
    if (needsQuote(s)) {
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

fn needsQuote(s: []const u8) bool {
    for (s) |c| {
        if (c == ',' or c == '"' or c == '\n' or c == '\r') return true;
    }
    return false;
}

// --- tests ---------------------------------------------------------------

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
    const r = try CsvReader.open(a, url);
    defer r.close();
    try std.testing.expectEqual(@as(usize, 2), r.schema.fields.len);
    try std.testing.expectEqualStrings("id", r.schema.fields[0].name);
    try std.testing.expectEqualStrings("name", r.schema.fields[1].name);

    const b = (try r.next(a)).?;
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expectEqual(@as(i64, 1), b.columns[0].getValue(0).int); // numeric column: inferred int
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
        try std.testing.expectError(error.HttpNotFound, CsvReader.open(a, url));
    }
    {
        const addr = try std.net.Address.parseIp("127.0.0.1", 0);
        var listener = try addr.listen(.{ .reuse_address = true });
        defer listener.deinit();
        const th = try std.Thread.spawn(.{}, serveOnce, .{ &listener, "503 Service Unavailable", "busy" });
        defer th.join();
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}/data.csv", .{listener.listen_address.getPort()});
        try std.testing.expectError(error.HttpServerBusy, CsvReader.open(a, url));
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

    // quoted field with embedded delimiter; "" escape; empty field -> null
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

    // \r stripped; blank lines skipped; short row pads with null; long row drops extras
    const b = try parseSlice(a, &schema, "1,2,3\r\n\r\n4,5\r\n6,7,8,NINE\n");
    try std.testing.expectEqual(@as(usize, 3), b.len);
    try std.testing.expectEqualStrings("3", b.columns[2].getValue(0).string); // no trailing \r
    try std.testing.expect(b.columns[2].getValue(1).isNull()); // short row -> null
    try std.testing.expectEqualStrings("6", b.columns[0].getValue(2).string);
    try std.testing.expectEqualStrings("8", b.columns[2].getValue(2).string); // "NINE" dropped
}

test "csv parsing: leading/trailing empty fields and last line without newline" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b", "c" });

    const b = try parseSlice(a, &schema, ",mid,\nx,y,z"); // no trailing \n
    try std.testing.expectEqual(@as(usize, 2), b.len);
    try std.testing.expect(b.columns[0].getValue(0).isNull());
    try std.testing.expectEqualStrings("mid", b.columns[1].getValue(0).string);
    try std.testing.expect(b.columns[2].getValue(0).isNull());
    try std.testing.expectEqualStrings("z", b.columns[2].getValue(1).string);
}

test "writeField quotes exactly the fields that need it, doubling quotes" {
    var buf = std.array_list.Managed(u8).init(std.testing.allocator);
    defer buf.deinit();
    try writeField(buf.writer(), "plain");
    try buf.append('|');
    try writeField(buf.writer(), "a,b");
    try buf.append('|');
    try writeField(buf.writer(), "say \"hi\"");
    try buf.append('|');
    try writeField(buf.writer(), "line\nbreak");
    try std.testing.expectEqualStrings("plain|\"a,b\"|\"say \"\"hi\"\"\"|\"line\nbreak\"", buf.items);
}

test "csv write/parse round-trip preserves quoted values" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    const schema = try stringSchema(a, &.{ "a", "b" });

    // serialize one row through writeField, then parse it back with splitInto
    var line = std.array_list.Managed(u8).init(a);
    try writeField(line.writer(), "O'Neil, \"Jr\"");
    try line.append(',');
    try writeField(line.writer(), "plain");
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
    try std.testing.expectEqual(types.TypeKind.float, s.resolve(1).kind); // int promoted by 1.5
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(2).kind);
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(3).kind); // "007" must survive verbatim
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(4).kind); // quoted = text
    try std.testing.expectEqual(types.TypeKind.string, s.resolve(5).kind); // all-empty column
    try std.testing.expect(s.resolve(0).nullable);
}

test "serial and mapped readers infer the same schema; mismatch past the sample errors" {
    const gpa = std.testing.allocator;
    var ar = std.heap.ArenaAllocator.init(gpa);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    // > SAMPLE_ROWS int rows, then text in the int column past the sample.
    var data = std.array_list.Managed(u8).init(a);
    try data.appendSlice("id,name\n");
    for (0..SAMPLE_ROWS + 10) |i| try data.writer().print("{d},n{d}\n", .{ i + 1, i + 1 });
    try tmp.dir.writeFile(.{ .sub_path = "ok.csv", .data = data.items });
    try data.appendSlice("oops,tail\n");
    try tmp.dir.writeFile(.{ .sub_path = "bad.csv", .data = data.items });
    const base = try tmp.dir.realpathAlloc(a, ".");

    const ok_path = try std.fs.path.join(a, &.{ base, "ok.csv" });
    const r = try CsvReader.open(a, ok_path);
    defer r.close();
    const m = try MappedCsv.open(a, ok_path);
    defer m.close();
    try std.testing.expectEqual(types.TypeKind.int, r.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.int, m.schema.fields[0].ty.kind);
    try std.testing.expectEqual(types.TypeKind.string, r.schema.fields[1].ty.kind);
    try std.testing.expectEqual(types.TypeKind.string, m.schema.fields[1].ty.kind);

    const bad_path = try std.fs.path.join(a, &.{ base, "bad.csv" });
    const rb = try CsvReader.open(a, bad_path);
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
    // Uneven line lengths so naive byte splits would land mid-line.
    const body = "1,alpha\n22,bb\n333,c\n4444,dddd\n5,e\n";
    try tmp.dir.writeFile(.{ .sub_path = "t.csv", .data = "id,name\n" ++ body });
    const path = try tmp.dir.realpathAlloc(a, "t.csv");

    const m = try MappedCsv.open(a, path);
    defer m.close();
    try std.testing.expectEqual(@as(usize, 2), m.schema.fields.len);
    try std.testing.expectEqualStrings("name", m.schema.fields[1].name);
    try std.testing.expectEqualStrings(body, m.body);

    const n = 3;
    var reassembled = std.array_list.Managed(u8).init(a);
    for (0..n) |i| {
        const c = m.chunk(i, n);
        // whole lines only: each non-empty chunk ends exactly at a newline
        if (c.len > 0) try std.testing.expectEqual(@as(u8, '\n'), c[c.len - 1]);
        try reassembled.appendSlice(c);
    }
    // disjoint + covering: concatenating the chunks reproduces the body exactly
    try std.testing.expectEqualStrings(body, reassembled.items);

    // more chunks than lines: still covering, extras are empty
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
    const m = try MappedCsv.open(a, path);
    defer m.close();

    var rows: usize = 0;
    for (0..2) |i| {
        var r = CsvSliceReader{ .data = m.chunk(i, 2), .schema = &m.schema };
        while (try r.next(a)) |b| rows += b.len;
    }
    try std.testing.expectEqual(@as(usize, 4), rows);
}
