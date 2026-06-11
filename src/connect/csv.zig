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
        self.schema = .{ .fields = try fields.toOwnedSlice() };
        return self;
    }

    pub fn next(self: *CsvReader, arena: std.mem.Allocator) !?Batch {
        if (self.done) return null;
        const ncols = self.schema.fields.len;
        const builders = try arena.alloc(column.Builder, ncols);
        for (builders) |*b| b.* = column.Builder.init(arena, types.Type.init(.string).asNullable());

        var rows: usize = 0;
        while (rows < BATCH_ROWS) {
            const line = (try self.readLine()) orelse {
                self.done = true;
                break;
            };
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
            try builders[col].append(.{ .string = try buf.toOwnedSlice() });
            if (i < line.len and line[i] == ',') i += 1;
        } else {
            const start = i;
            while (i < line.len and line[i] != ',') i += 1;
            const raw = line[start..i];
            try builders[col].append(if (raw.len == 0) .null else Value{ .string = raw });
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
    try std.testing.expectEqualStrings("1", b.columns[0].getValue(0).string);
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
