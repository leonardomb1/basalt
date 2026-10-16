//! A zip archive written front to back, the write side of `zip.zig`, for
//! `.xlsx` workbooks: nothing is seeked back to, so the archive streams to any
//! `Output` (a local `.part`, an object upload, SFTP, SMB).
//!
//! A member known whole is stored with its CRC and sizes in its local header. A
//! streamed member is deflated (`deflate.zig`, raw, at the fast level: twice the
//! speed for a fifth more bytes) behind general-purpose bit 3:
//! its local header carries zeros and a data descriptor after the data gives the
//! CRC and sizes, learned by counting what passes. The central directory closes
//! the archive. No zip64: a member or offset past 4 GiB is refused as soon as it
//! is reached (`error.ZipTooLarge`). Times are the DOS epoch, 1980-01-01.

const std = @import("std");
const zip = std.zip;
const deflate = @import("deflate.zig");

const dos_date: u16 = (1 << 5) | 1;
const max32 = std.math.maxInt(u32);
const bit3: u16 = 1 << 3;

const Count = struct {
    n: u64 = 0,

    pub fn update(self: *Count, b: []const u8) void {
        self.n += b.len;
    }
};

const Tally = struct {
    crc: std.hash.Crc32 = .init(),
    n: u64 = 0,

    pub fn update(self: *Tally, b: []const u8) void {
        self.crc.update(b);
        self.n += b.len;
    }
};

const Entry = struct {
    name: []const u8,
    method: zip.CompressionMethod,
    flags: u16,
    crc: u32,
    csize: u32,
    usize: u32,
    offset: u32,
};

pub const Writer = struct {
    arena: std.mem.Allocator,
    counter: std.Io.Writer.Hashed(Count),
    entries: std.array_list.Managed(Entry),
    def: ?*deflate.Deflate = null,
    member: std.Io.Writer.Hashed(Tally) = undefined,
    member_buf: []u8,
    open_at: ?u64 = null,
    open_name: []const u8 = "",

    /// Lives in `arena` and must not move: the member writer points into it.
    pub fn init(arena: std.mem.Allocator, out: *std.Io.Writer) !*Writer {
        const self = try arena.create(Writer);
        self.* = .{
            .arena = arena,
            .counter = .initHasher(out, .{}, &.{}),
            .entries = .init(arena),
            .member_buf = try arena.alloc(u8, 64 * 1024),
        };
        return self;
    }

    pub fn deinit(self: *Writer) void {
        if (self.def) |d| d.deinit(std.heap.page_allocator);
        self.def = null;
    }

    fn pos(self: *const Writer) u64 {
        return self.counter.hasher.n;
    }

    fn offset32(self: *const Writer) !u32 {
        const p = self.pos();
        if (p > max32) return error.ZipTooLarge;
        return @intCast(p);
    }

    fn localHeader(self: *Writer, e: Entry) !void {
        const w = &self.counter.writer;
        try w.writeStruct(zip.LocalFileHeader{
            .signature = zip.local_file_header_sig,
            .version_needed_to_extract = 20,
            .flags = @bitCast(e.flags),
            .compression_method = e.method,
            .last_modification_time = 0,
            .last_modification_date = dos_date,
            .crc32 = e.crc,
            .compressed_size = e.csize,
            .uncompressed_size = e.usize,
            .filename_len = @intCast(e.name.len),
            .extra_len = 0,
        }, .little);
        try w.writeAll(e.name);
    }

    /// One member written whole, uncompressed.
    pub fn addStored(self: *Writer, name: []const u8, bytes: []const u8) !void {
        std.debug.assert(self.open_at == null);
        if (bytes.len > max32) return error.ZipTooLarge;
        const e = Entry{
            .name = try self.arena.dupe(u8, name),
            .method = .store,
            .flags = 0,
            .crc = std.hash.Crc32.hash(bytes),
            .csize = @intCast(bytes.len),
            .usize = @intCast(bytes.len),
            .offset = try self.offset32(),
        };
        try self.localHeader(e);
        try self.counter.writer.writeAll(bytes);
        try self.entries.append(e);
    }

    /// Opens a deflated member and returns where its bytes go, until `end`.
    pub fn begin(self: *Writer, name: []const u8) !*std.Io.Writer {
        std.debug.assert(self.open_at == null);
        const at = self.pos();
        self.open_name = try self.arena.dupe(u8, name);
        _ = try self.offset32();
        try self.localHeader(.{ .name = self.open_name, .method = .deflate, .flags = bit3, .crc = 0, .csize = 0, .usize = 0, .offset = 0 });
        if (self.def) |d| try d.reset() else self.def = try deflate.Deflate.init(std.heap.page_allocator, &self.counter.writer, .{ .container = .raw, .level = .fast });
        self.member = .initHasher(&self.def.?.interface, .{}, self.member_buf);
        self.open_at = at;
        return &self.member.writer;
    }

    /// Bytes written to the open member so far, uncompressed.
    pub fn memberSize(self: *const Writer) u64 {
        return self.member.hasher.n + self.member.writer.end;
    }

    pub fn end(self: *Writer) !void {
        const at = self.open_at.?;
        const hdr = @sizeOf(zip.LocalFileHeader) + self.open_name.len;
        try self.member.writer.flush();
        try self.def.?.finish();
        const csize = self.pos() - at - hdr;
        const usize_ = self.member.hasher.n;
        if (csize > max32 or usize_ > max32) return error.ZipTooLarge;
        const crc = self.member.hasher.crc.final();
        const w = &self.counter.writer;
        try w.writeAll(&.{ 'P', 'K', 7, 8 });
        try w.writeInt(u32, crc, .little);
        try w.writeInt(u32, @intCast(csize), .little);
        try w.writeInt(u32, @intCast(usize_), .little);
        try self.entries.append(.{ .name = self.open_name, .method = .deflate, .flags = bit3, .crc = crc, .csize = @intCast(csize), .usize = @intCast(usize_), .offset = @intCast(at) });
        self.open_at = null;
    }

    /// Writes the central directory and end record; flushing the output is left to its owner.
    pub fn finish(self: *Writer) !void {
        std.debug.assert(self.open_at == null);
        const cd_off = try self.offset32();
        const w = &self.counter.writer;
        for (self.entries.items) |e| {
            try w.writeStruct(zip.CentralDirectoryFileHeader{
                .signature = zip.central_file_header_sig,
                .version_made_by = 20,
                .version_needed_to_extract = 20,
                .flags = @bitCast(e.flags),
                .compression_method = e.method,
                .last_modification_time = 0,
                .last_modification_date = dos_date,
                .crc32 = e.crc,
                .compressed_size = e.csize,
                .uncompressed_size = e.usize,
                .filename_len = @intCast(e.name.len),
                .extra_len = 0,
                .comment_len = 0,
                .disk_number = 0,
                .internal_file_attributes = 0,
                .external_file_attributes = 0,
                .local_file_header_offset = e.offset,
            }, .little);
            try w.writeAll(e.name);
        }
        const cd_end = try self.offset32();
        const n: u16 = @intCast(self.entries.items.len);
        try w.writeStruct(zip.EndRecord{
            .signature = zip.end_record_sig,
            .disk_number = 0,
            .central_directory_disk_number = 0,
            .record_count_disk = n,
            .record_count_total = n,
            .central_directory_size = cd_end - cd_off,
            .central_directory_offset = cd_off,
            .comment_len = 0,
        }, .little);
    }
};

test "zipwrite: stored and streamed members read back through zip.zig" {
    var ar = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer ar.deinit();
    const a = ar.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var aw = std.Io.Writer.Allocating.init(a);
    const z = try Writer.init(a, &aw.writer);
    defer z.deinit();
    try z.addStored("a.txt", "hello");
    const m = try z.begin("dir/b.txt");
    for (0..5000) |i| try m.print("row {d}\n", .{i});
    try z.end();
    try z.addStored("empty", "");
    const m2 = try z.begin("c.txt");
    try m2.writeAll("tail");
    try z.end();
    try z.finish();

    try tmp.dir.writeFile(.{ .sub_path = "t.zip", .data = aw.written() });
    const path = try tmp.dir.realpathAlloc(a, "t.zip");
    const zr = @import("zip.zig");
    const names = try zr.names(a, path);
    try std.testing.expectEqual(@as(usize, 4), names.len);

    const ma = try zr.openMember(a, path, "a.txt");
    defer ma.close();
    try std.testing.expectEqualStrings("hello", try ma.reader.allocRemaining(a, .limited(1 << 20)));
    const mb = try zr.openMember(a, path, "dir/b.txt");
    defer mb.close();
    const got = try mb.reader.allocRemaining(a, .limited(1 << 20));
    try std.testing.expect(std.mem.startsWith(u8, got, "row 0\nrow 1\n"));
    try std.testing.expect(std.mem.endsWith(u8, got, "row 4999\n"));
    const mc = try zr.openMember(a, path, "c.txt");
    defer mc.close();
    try std.testing.expectEqualStrings("tail", try mc.reader.allocRemaining(a, .limited(1 << 20)));
}
